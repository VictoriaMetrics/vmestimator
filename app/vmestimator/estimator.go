package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/axiomhq/hyperloglog"
	"github.com/dgryski/go-metro"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

type estimator struct {
	groupBy          []string
	groupByKeysLabel string
	groupSize        *groupSize

	buckets []*estimatorBucket

	metricsSet  *metrics.Set
	insertTotal *metrics.Counter

	stopCh chan struct{}
}

func newEstimator(cfg EstimatorConfig) (*estimator, error) {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute * 5
	}
	if cfg.GroupLimit <= 0 {
		cfg.GroupLimit = 10000
	}
	if cfg.Buckets <= 0 {
		cfg.Buckets = min(64, 2*cgroup.AvailableCPUs())
	}
	if cfg.HLLPrecision == 0 {
		cfg.HLLPrecision = 14
	}
	if cfg.HLLSparse == nil {
		cfg.HLLSparse = new(true)
	}

	metricPrefix := fmt.Sprintf("cardinality_estimate{interval=%q", cfg.Interval)
	if len(cfg.Labels) > 0 {
		keys := make([]string, 0, len(cfg.Labels))
		for k := range cfg.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			metricPrefix += fmt.Sprintf(",%s=%q", k, cfg.Labels[k])
		}
	}

	groupByKeysLabel := "__global__"
	if len(cfg.GroupBy) > 0 {
		groupByKeysLabel = strings.Join(cfg.GroupBy, `,`)
	}

	e := &estimator{
		groupBy:          cfg.GroupBy,
		groupByKeysLabel: groupByKeysLabel,
		groupSize: &groupSize{
			limit:          int64(cfg.GroupLimit),
			bucketSizes:    make([]int64, cfg.Buckets),
			rejectSketches: make([]*hyperloglog.Sketch, cfg.Buckets),
		},
		buckets:    make([]*estimatorBucket, cfg.Buckets),
		metricsSet: metrics.NewSet(),
		stopCh:     make(chan struct{}),
	}

	e.insertTotal = e.metricsSet.NewCounter(
		fmt.Sprintf(`vmestimator_estimator_insert_total{group_by_keys=%q}`, e.groupByKeysLabel),
	)
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_rejected_size{group_by_keys=%q,interval=%q}`, e.groupByKeysLabel, cfg.Interval), func() float64 {
		return float64(e.groupSize.totalRejected())
	})

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			idx:              i,
			groupSize:        e.groupSize,
			groupBy:          cfg.GroupBy,
			extraLabels:      cfg.Labels,
			interval:         cfg.Interval,
			metricPrefix:     metricPrefix,
			groupByKeysLabel: groupByKeysLabel,

			precision: cfg.HLLPrecision,
			sparse:    *cfg.HLLSparse,
		}

		if len(cfg.GroupBy) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[string]groupSketch)
			eb.prevGroups = make(map[string]groupSketch)
		}

		e.buckets[i] = eb
	}

	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_limit{group_by_keys=%q,interval=%q}`, e.groupByKeysLabel, cfg.Interval), func() float64 {
		return float64(e.groupSize.limit)
	})
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_size{group_by_keys=%q,interval=%q}`, e.groupByKeysLabel, cfg.Interval), func() float64 {
		return float64(e.groupSize.totalSize())
	})

	go e.runRotation(cfg.Interval)

	metrics.RegisterSet(e.metricsSet)

	return e, nil
}

func (e *estimator) stop() {
	close(e.stopCh)
	e.metricsSet.UnregisterAllMetrics()
}

var groupValuesPool = sync.Pool{}

func getGroupValuesKeySlice() *[]byte {
	v0 := groupValuesPool.Get()
	if v0 == nil {
		v := make([]byte, 128)
		return &v
	}

	return v0.(*[]byte)
}

func putGroupValuesSlice(key *[]byte) {
	if key == nil {
		return
	}

	*key = (*key)[:0]
	groupValuesPool.Put(key)
}

func (e *estimator) insertMany(tss []protoparser.TimeSerie) {
	bucketsNum := uint64(len(e.buckets))

	groupValuesKeyP := getGroupValuesKeySlice()
	groupValuesKey := *groupValuesKeyP
	defer func() {
		*groupValuesKeyP = groupValuesKey
		putGroupValuesSlice(groupValuesKeyP)
	}()

	groupValues := make([]string, len(e.groupBy))

	var cnt int
	for _, ts := range tss {
		if len(e.groupBy) == 0 {
			i := int(ts.Fingerprint % bucketsNum)
			e.buckets[i].insert(ts, "", nil)
			cnt++
			continue
		}

		groupValuesKey = groupValuesKey[:0]
		clear(groupValues)
		var hasNames bool
		for i, labelName := range e.groupBy {
			if i > 0 {
				groupValuesKey = append(groupValuesKey, ',')
			}

			for _, l := range ts.GroupLabels {
				if l.Name == labelName {
					hasNames = true

					groupValuesKey = append(groupValuesKey, l.Value...)
					groupValues[i] = l.Value
					break
				}
			}
		}

		// time series does not contribute to this groupBy
		if !hasNames {
			continue
		}

		i := int(hash(groupValuesKey) % bucketsNum)
		e.buckets[i].insert(ts, bytesutil.ToUnsafeString(groupValuesKey), groupValues)
		cnt++
	}

	e.insertTotal.Add(cnt)
}

func (e *estimator) reset() {
	for _, b := range e.buckets {
		b.reset()
	}
}

func (e *estimator) writeMetrics(w io.Writer) {
	eb0 := e.buckets[0]

	if len(e.groupBy) == 0 {
		formatBuf := make([]byte, 0, 1024)
		resSK := eb0.newSketch()
		for _, eb := range e.buckets {
			eb.writeNoGroupMetric(resSK)
		}

		formatBuf = appendGlobalMetric(formatBuf, eb0.metricPrefix)
		formatBuf = strconv.AppendUint(formatBuf, resSK.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		if _, err := w.Write(formatBuf); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
		return
	}

	formatBuf := make([]byte, 0, 16384)
	formatBuf = appendGroupByKeysAndValuesPrefix(formatBuf, eb0.metricPrefix, eb0.groupByKeysLabel)

	prefixLen := len(formatBuf)
	resSK := eb0.newSketch()
	for _, eb := range e.buckets {
		formatBuf = eb.writeGroupMetrics(w, resSK, formatBuf[:prefixLen])
	}

	formatBuf = formatBuf[:0]
	formatBuf = appendGroupMetric(formatBuf, eb0.metricPrefix, eb0.groupByKeysLabel)
	formatBuf = strconv.AppendInt(formatBuf, eb0.groupSize.totalSize(), 10)
	formatBuf = append(formatBuf, "\n"...)
	if _, err := w.Write(formatBuf); err != nil {
		logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
	}

	formatBuf = formatBuf[:0]
	formatBuf = appendGroupLimitMetric(formatBuf, eb0.groupByKeysLabel, eb0.interval)
	formatBuf = strconv.AppendInt(formatBuf, eb0.groupSize.limit, 10)
	formatBuf = append(formatBuf, "\n"...)
	if _, err := w.Write(formatBuf); err != nil {
		logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
	}
}

func (e *estimator) runRotation(interval time.Duration) {
	// Divide the rotation interval evenly among buckets so each bucket rotates
	// at a different time, reducing the sawtooth effect.
	bucketInterval := interval / 2 / time.Duration(len(e.buckets))
	period := int64(bucketInterval)
	bucketIdx := 0
	for {
		// Align next tick to a fixed grid of bucketInterval since Unix epoch,
		// so rotations happen at the same absolute times regardless of startup time.
		now := time.Now().UnixNano()
		waitNs := period - now%period
		if waitNs == period {
			waitNs = 0
		}
		t := time.NewTimer(time.Duration(waitNs))
		select {
		case <-t.C:
			e.buckets[bucketIdx].rotate()
			bucketIdx = (bucketIdx + 1) % len(e.buckets)
		case <-e.stopCh:
			t.Stop()
			return
		}
	}
}

func (e *estimator) writeSnapshot(enc *gob.Encoder) error {
	if len(e.groupBy) == 0 {
		s := newSnapshot()
		if err := enc.Encode(convertNoGroupToSnapshot(e, s)); err != nil {
			return fmt.Errorf("encode snapshot: %w", err)
		}

		return nil
	}

	eb0 := e.buckets[0]

	formatBuf := make([]byte, 0, 16384)
	formatBuf = appendGroupByKeysAndValuesPrefix(formatBuf, eb0.metricPrefix, eb0.groupByKeysLabel)

	s := newSnapshot()
	for _, eb := range e.buckets {
		s.reset()
		if err := enc.Encode(convertGroupBucketToSnapshot(eb, s, formatBuf)); err != nil {
			return fmt.Errorf("encode snapshot: %w", err)
		}
	}

	return nil
}

type estimatorBucket struct {
	mu sync.Mutex

	idx              int
	groupBy          []string
	extraLabels      map[string]string
	interval         time.Duration
	metricPrefix     string
	groupByKeysLabel string
	precision        uint8
	sparse           bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  *groupSize
	groups     map[string]groupSketch
	prevGroups map[string]groupSketch
}

func (eb *estimatorBucket) String() string {
	return fmt.Sprintf(
		"interval: %s; group_by: %v; extra_labels: %v", eb.interval, eb.groupBy, eb.extraLabels)
}

func (eb *estimatorBucket) reset() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.prevSketch.Reset()
		eb.sketch.Reset()
		return
	}

	eb.groups = make(map[string]groupSketch)
	eb.prevGroups = make(map[string]groupSketch)

	eb.groupSize.rotateLocked(eb.idx, 0)
}

func (eb *estimatorBucket) rotate() {
	if len(eb.groupBy) == 0 {
		eb.mu.Lock()
		eb.prevSketch = eb.sketch
		eb.sketch = eb.newSketch()
		eb.mu.Unlock()
		return
	}

	eb.mu.Lock()
	eb.prevGroups = eb.groups
	eb.groups = make(map[string]groupSketch, len(eb.groups))
	eb.groupSize.rotateLocked(eb.idx, int64(len(eb.prevGroups)))
	eb.mu.Unlock()
}

func (eb *estimatorBucket) insert(ts protoparser.TimeSerie, groupValuesKey string, groupValues []string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.sketch.InsertHash(ts.Fingerprint)
		return
	}

	gsk, ok := eb.groups[groupValuesKey]
	if !ok {
		prevGSK, ok := eb.prevGroups[groupValuesKey]
		if !ok {
			if !eb.groupSize.allowInsertLocked(eb.idx, groupValuesKey) {
				return
			}
		}

		groupValueLabels := prevGSK.groupValueLabels
		if len(groupValueLabels) == 0 {
			formatBuf := make([]byte, 0, 1024)
			formatBuf = strconv.AppendQuote(formatBuf, groupValuesKey)
			for i := range groupValues {
				formatBuf = append(formatBuf, ',')
				if eb.groupBy[i] == `__name__` {
					formatBuf = append(formatBuf, `by__name__`...)
				} else {
					formatBuf = append(formatBuf, `by_`...)
					formatBuf = append(formatBuf, eb.groupBy[i]...)
				}
				formatBuf = append(formatBuf, '=')
				formatBuf = strconv.AppendQuote(formatBuf, groupValues[i])
			}
			formatBuf = append(formatBuf, `} `...)

			groupValueLabels = bytesutil.ToUnsafeString(formatBuf)
		}

		gsk = groupSketch{
			groupValueLabels: groupValueLabels,

			Sketch: eb.newSketch(),
		}

		eb.groups[strings.Clone(groupValuesKey)] = gsk
	}
	gsk.InsertHash(ts.Fingerprint)
}

func (eb *estimatorBucket) writeNoGroupMetric(res *hyperloglog.Sketch) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.mergeSketches(eb.sketch, eb.prevSketch, res)
}

func (eb *estimatorBucket) writeGroupMetrics(w io.Writer, res *hyperloglog.Sketch, formatBuf []byte) []byte {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	prefixLen := len(formatBuf)

	for valuesKey, gsk := range eb.groups {
		res.Reset()
		formatBuf = append(formatBuf[:prefixLen], gsk.groupValueLabels...)

		eb.mergeSketches(gsk.Sketch, eb.prevGroups[valuesKey].Sketch, res)
		formatBuf = strconv.AppendUint(formatBuf, res.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		if _, err := w.Write(formatBuf); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
	}

	for valuesKey := range eb.prevGroups {
		if _, ok := eb.groups[valuesKey]; ok {
			continue
		}

		res.Reset()
		formatBuf = formatBuf[:prefixLen]

		gsk := eb.prevGroups[valuesKey]
		formatBuf = append(formatBuf, gsk.groupValueLabels...)

		eb.mergeSketches(nil, eb.prevGroups[valuesKey].Sketch, res)
		formatBuf = strconv.AppendUint(formatBuf, res.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		if _, err := w.Write(formatBuf); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
	}

	return formatBuf[:prefixLen]
}

func (eb *estimatorBucket) mergeSketches(cur, prev, res *hyperloglog.Sketch) {
	if err := res.Merge(cur); err != nil {
		panic(err)
	}
	if prev != nil {
		if err := res.Merge(prev); err != nil {
			panic(err)
		}
	}
}

func (eb *estimatorBucket) newSketch() *hyperloglog.Sketch {
	return mustNewSketch(eb.precision, eb.sparse)
}

type groupSketch struct {
	groupValueLabels string

	*hyperloglog.Sketch
}

type groupSize struct {
	limit int64
	size  atomic.Int64

	bucketSizes []int64

	rejectMu       sync.Mutex
	rejectSketches []*hyperloglog.Sketch
}

// allowInsertLocked must be called under estimatorBucket lock
func (gs *groupSize) allowInsertLocked(bucketIdx int, groupValuesKey string) bool {
	if gs.size.Load() >= gs.limit {
		gs.rejectMu.Lock()
		sk := gs.rejectSketches[bucketIdx]
		if sk == nil {
			sk = mustNewGroupRejectSketch()
			gs.rejectSketches[bucketIdx] = sk
		}
		sk.InsertHash(hash([]byte(groupValuesKey)))
		gs.rejectMu.Unlock()
		return false
	}

	gs.bucketSizes[bucketIdx]++
	gs.size.Add(1)
	return true
}

// rotateLocked must be called under estimatorBucket lock
func (gs *groupSize) rotateLocked(bucketIdx int, size int64) {
	if diff := gs.bucketSizes[bucketIdx] - size; diff > 0 {
		gs.bucketSizes[bucketIdx] -= diff
		gs.size.Add(-diff)
	}

	gs.rejectMu.Lock()
	gs.rejectSketches[bucketIdx] = nil
	gs.rejectMu.Unlock()
}

func (gs *groupSize) totalSize() int64 {
	size := gs.size.Load()
	if size >= int64(float64(gs.limit)*0.8) {
		var rejectSize uint64
		gs.rejectMu.Lock()
		for _, sk := range gs.rejectSketches {
			if sk == nil {
				continue
			}
			rejectSize += sk.Estimate()
		}
		gs.rejectMu.Unlock()

		size += int64(rejectSize)
	}

	return size
}

func (gs *groupSize) totalRejected() uint64 {
	var rejectSize uint64
	gs.rejectMu.Lock()
	for _, sk := range gs.rejectSketches {
		if sk == nil {
			continue
		}
		rejectSize += sk.Estimate()
	}
	gs.rejectMu.Unlock()

	return rejectSize
}

func mustNewGroupRejectSketch() *hyperloglog.Sketch {
	return mustNewSketch(10, true)
}

func mustNewSketch(precision uint8, sparse bool) *hyperloglog.Sketch {
	sk, err := hyperloglog.NewSketch(precision, sparse)
	if err != nil {
		panic(fmt.Sprintf("cannot create HLL sketch with precision=%d and sparse=%v: %s", precision, sparse, err))
	}

	return sk
}

func hash(v []byte) uint64 {
	return metro.Hash64(v, 1337)
}

// appendGlobalMetric produces:
// 'cardinality_estimate{interval="5m",group_by_keys="__global__"} '
func appendGlobalMetric(buf []byte, metricPrefix string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,group_by_keys="__global__"} `...)
	return buf
}

// appendGroupMetric produces:
// 'cardinality_estimate{interval="5m",group_by_keys="__group__",group_by_values="fooKey,barKey"} '
func appendGroupMetric(buf []byte, metricPrefix, groupByKeysLabel string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,group_by_keys="__group__",group_by_values="`...)
	buf = append(buf, groupByKeysLabel...)
	buf = append(buf, `"} `...)
	return buf
}

// appendGroupLimitMetric produces:
// 'vmestimator_estimator_group_limit{group_by_keys="fooKey,barKey",interval="5m"} '
func appendGroupLimitMetric(buf []byte, groupByKeysLabel string, interval time.Duration) []byte {
	buf = buf[:0]
	buf = append(buf, `vmestimator_estimator_group_limit{interval="`...)
	buf = append(buf, interval.String()...)
	buf = append(buf, `",group_by_keys="__group__",group_by_values="`...)
	buf = append(buf, groupByKeysLabel...)
	buf = append(buf, `"} `...)
	return buf
}

// appendGroupByKeysAndValuesPrefix produces:
// 'cardinality_estimate{interval="5m",group_by_keys="fooKey,barKey",group_by_values='
func appendGroupByKeysAndValuesPrefix(buf []byte, metricPrefix, groupByKeysLabel string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,group_by_keys="`...)
	buf = append(buf, groupByKeysLabel...)
	buf = append(buf, `",group_by_values=`...)
	return buf
}
