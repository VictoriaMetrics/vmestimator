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

	"github.com/axiomhq/hyperloglog"
	"github.com/dgryski/go-metro"
	"github.com/valyala/fastrand"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

const labelKeyword = "__label__"

type estimator struct {
	compiledFilter compiledFilter

	groupBy          []string
	groupByKeysLabel string
	groupSize        *groupSize

	hasLabelKeyword bool

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
		buckets := min(64, 2*cgroup.AvailableCPUs())
		cfg.Buckets = max(4, buckets)
	}
	if cfg.HLLPrecision == 0 {
		cfg.HLLPrecision = 14
	}
	if cfg.HLLSparse == nil {
		cfg.HLLSparse = new(true)
	}

	metricPrefix := fmt.Sprintf("cardinality_estimate{interval=%q,filter=%q", cfg.Interval, cfg.Filter)
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
		for _, k := range cfg.GroupBy {
			if k == `__global__` || k == `__group__` {
				return nil, fmt.Errorf("group by %s is not allowed. __global__, __group__ are reserved keywords", k)
			}
		}

		groupByKeysLabel = strings.Join(cfg.GroupBy, `,`)
	}

	cf, err := compileFilters(cfg.Filter)
	if err != nil {
		return nil, fmt.Errorf("cannot compile filters for estimator: %w", err)
	}
	if len(cfg.GroupBy) > 0 {
		for i, k := range cfg.GroupBy[:len(cfg.GroupBy)-1] {
			if k == labelKeyword {
				panic(fmt.Sprintf("BUG: %s must be the last element of groupBy, got index %d in %v", labelKeyword, i, cfg.GroupBy))
			}
		}
	}

	e := &estimator{
		compiledFilter:   cf,
		groupBy:          cfg.GroupBy,
		groupByKeysLabel: groupByKeysLabel,
		hasLabelKeyword:  len(cfg.GroupBy) > 0 && cfg.GroupBy[len(cfg.GroupBy)-1] == labelKeyword,
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
		fmt.Sprintf(`vmestimator_estimator_insert_total{group_by_keys=%q,interval=%q,filter=%q}`, e.groupByKeysLabel, cfg.Interval, cfg.Filter),
	)
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_rejected_size{group_by_keys=%q,interval=%q,filter=%q}`, e.groupByKeysLabel, cfg.Interval, cfg.Filter), func() float64 {
		return float64(e.groupSize.totalRejected())
	})

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			idx:              i,
			filter:           cfg.Filter,
			groupSize:        e.groupSize,
			groupBy:          cfg.GroupBy,
			extraLabels:      cfg.Labels,
			interval:         cfg.Interval,
			metricPrefix:     metricPrefix,
			groupByKeysLabel: groupByKeysLabel,

			precision:       cfg.HLLPrecision,
			sparse:          *cfg.HLLSparse,
			hasLabelKeyword: e.hasLabelKeyword,
		}

		if len(cfg.GroupBy) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[uint64]groupSketch)
			eb.prevGroups = make(map[uint64]groupSketch)
		}

		e.buckets[i] = eb
	}

	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_limit{group_by_keys=%q,interval=%q,filter=%q}`, e.groupByKeysLabel, cfg.Interval, cfg.Filter), func() float64 {
		return float64(e.groupSize.limit)
	})
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_size{group_by_keys=%q,interval=%q,filter=%q}`, e.groupByKeysLabel, cfg.Interval, cfg.Filter), func() float64 {
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
		v := make([]byte, 0, 128)
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

	if len(e.groupBy) == 0 {
		var cnt int
		tssLen := uint32(len(tss))
		start := fastrand.Uint32n(tssLen)
		for j := uint32(0); j < tssLen; j++ {
			i := (start + j) % tssLen

			ts := tss[i]
			if !e.compiledFilter.match(ts.Labels) {
				continue
			}
			bi := int(ts.Fingerprint % bucketsNum)
			e.buckets[bi].insert(ts.Fingerprint, 0, nil)
			cnt++
		}
		e.insertTotal.Add(cnt)
		return
	}

	var cnt int
	groupValuesKeyP := getGroupValuesKeySlice()
	groupValuesKey := *groupValuesKeyP
	defer func() {
		*groupValuesKeyP = groupValuesKey
		putGroupValuesSlice(groupValuesKeyP)
	}()

	groupValues := make([]string, len(e.groupBy))

	// When __label__ is present it is always the last element; iterate only the explicit keys.
	groupByKeys := e.groupBy
	if e.hasLabelKeyword {
		groupByKeys = e.groupBy[:len(e.groupBy)-1]
	}

	tssLen := uint32(len(tss))
	start := fastrand.Uint32n(tssLen)
	for j := uint32(0); j < tssLen; j++ {
		i := (start + j) % tssLen

		ts := tss[i]
		if !e.compiledFilter.match(ts.Labels) {
			continue
		}

		groupValuesKey = groupValuesKey[:0]
		clear(groupValues)
		// hasNames starts true when there are no explicit keys (pure __label__ mode).
		hasNames := len(groupByKeys) == 0
		for i, labelName := range groupByKeys {
			if i > 0 {
				groupValuesKey = append(groupValuesKey, ',')
			}

			for _, l := range ts.Labels {
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

		groupValuesKeyHash := hash(groupValuesKey)
		if !e.hasLabelKeyword {
			bi := int(groupValuesKeyHash % bucketsNum)
			e.buckets[bi].insert(ts.Fingerprint, groupValuesKeyHash, groupValues)
			cnt++
			continue
		}

		// __label__ expansion: one insert per label in the series.
		explicitKeyLen := len(groupValuesKey)
		lastIdx := len(e.groupBy) - 1
		for _, label := range ts.Labels {
			if label.Fingerprint == 0 {
				panic(fmt.Sprintf("BUG: label %q has zero Fingerprint; group_by contains %s", label.Name, labelKeyword))
			}
			groupValuesKey = groupValuesKey[:explicitKeyLen]
			if explicitKeyLen > 0 {
				groupValuesKey = append(groupValuesKey, ',')
			}
			groupValuesKey = append(groupValuesKey, label.Name...)
			groupValues[lastIdx] = label.Name
			groupValuesKeyHash := hash(groupValuesKey)
			bi := int(groupValuesKeyHash % bucketsNum)
			e.buckets[bi].insert(label.Fingerprint, groupValuesKeyHash, groupValues)
			cnt++
		}
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
	formatBuf = appendGroupLimitMetric(formatBuf, eb0.groupByKeysLabel, eb0.interval, eb0.filter)
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
	filter           string
	groupBy          []string
	extraLabels      map[string]string
	interval         time.Duration
	metricPrefix     string
	groupByKeysLabel string
	precision        uint8
	sparse           bool
	hasLabelKeyword  bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  *groupSize
	groups     map[uint64]groupSketch
	prevGroups map[uint64]groupSketch
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

	eb.groups = make(map[uint64]groupSketch)
	eb.prevGroups = make(map[uint64]groupSketch)

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
	eb.groups = make(map[uint64]groupSketch, len(eb.groups))
	eb.groupSize.rotateLocked(eb.idx, int64(len(eb.prevGroups)))
	eb.mu.Unlock()
}

func (eb *estimatorBucket) insert(fp uint64, groupValuesKey uint64, groupValues []string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.sketch.InsertHash(fp)
		return
	}

	gsk, ok := eb.groups[groupValuesKey]
	if !ok {
		var groupValueLabels string
		if prevGSK, ok := eb.prevGroups[groupValuesKey]; !ok {
			if !eb.groupSize.allowInsertLocked(eb.idx, groupValuesKey) {
				return
			}

			tmpBufP := getGroupValuesKeySlice()
			tmpBuf := *tmpBufP
			defer func() {
				*tmpBufP = tmpBuf
				putGroupValuesSlice(tmpBufP)
			}()

			for i, v := range groupValues {
				if i > 0 {
					tmpBuf = append(tmpBuf, ',')
				}
				tmpBuf = append(tmpBuf, v...)
			}

			formatBuf := make([]byte, 0, 1024)
			formatBuf = strconv.AppendQuote(formatBuf, bytesutil.ToUnsafeString(tmpBuf))
			for i := range groupValues {
				formatBuf = append(formatBuf, ',')
				switch eb.groupBy[i] {
				case `__name__`:
					formatBuf = append(formatBuf, `by__name__`...)
				case labelKeyword:
					formatBuf = append(formatBuf, `by__label__`...)
				default:
					formatBuf = append(formatBuf, `by_`...)
					formatBuf = append(formatBuf, eb.groupBy[i]...)
				}
				formatBuf = append(formatBuf, '=')
				formatBuf = strconv.AppendQuote(formatBuf, groupValues[i])
			}
			formatBuf = append(formatBuf, `} `...)

			groupValueLabels = bytesutil.ToUnsafeString(formatBuf)
		} else {
			groupValueLabels = prevGSK.groupValueLabels
		}

		gsk = groupSketch{
			groupValueLabels: groupValueLabels,
			Sketch:           eb.newSketch(),
		}

		eb.groups[groupValuesKey] = gsk
	}
	gsk.InsertHash(fp)
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
func (gs *groupSize) allowInsertLocked(bucketIdx int, groupValuesKey uint64) bool {
	if gs.size.Load() >= gs.limit {
		gs.rejectMu.Lock()
		sk := gs.rejectSketches[bucketIdx]
		if sk == nil {
			sk = mustNewGroupRejectSketch()
			gs.rejectSketches[bucketIdx] = sk
		}
		sk.InsertHash(groupValuesKey)
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
// 'vmestimator_estimator_group_limit{interval="5m",filter="",group_by_keys="__group__",group_by_values="fooKey,barKey"} '
func appendGroupLimitMetric(buf []byte, groupByKeysLabel string, interval time.Duration, filter string) []byte {
	buf = buf[:0]
	buf = append(buf, `vmestimator_estimator_group_limit{interval="`...)
	buf = append(buf, interval.String()...)
	buf = append(buf, `",filter=`...)
	buf = strconv.AppendQuote(buf, filter)
	buf = append(buf, `,group_by_keys="__group__",group_by_values="`...)
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
