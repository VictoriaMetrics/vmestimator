package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/metrics"
	"github.com/axiomhq/hyperloglog"
	"github.com/dgryski/go-metro"

	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

type estimator struct {
	groupBy          []string
	groupByKeysLabel string

	groupLimit              int64
	groupSize               atomic.Int64
	groupRejectedMu         sync.Mutex
	groupRejectedSketch     *hyperloglog.Sketch
	groupRejectedSketchPrev *hyperloglog.Sketch

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
		cfg.Buckets = min(20, cgroup.AvailableCPUs())
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
		groupBy:                 cfg.GroupBy,
		groupByKeysLabel:        groupByKeysLabel,
		groupLimit:              int64(cfg.GroupLimit),
		groupRejectedSketch:     mustNewGroupRejectSketch(),
		groupRejectedSketchPrev: mustNewGroupRejectSketch(),
		buckets:                 make([]*estimatorBucket, cfg.Buckets),
		metricsSet:              metrics.NewSet(),
		stopCh:                  make(chan struct{}),
	}

	e.insertTotal = e.metricsSet.NewCounter(
		fmt.Sprintf(`cestorage_estimator_insert_total{group_by_keys=%q}`, e.groupByKeysLabel),
	)
	e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_estimator_group_rejected_size{group_by_keys=%q}`, e.groupByKeysLabel), func() float64 {
		e.groupRejectedMu.Lock()
		defer e.groupRejectedMu.Unlock()
		return float64(e.groupRejectedSketch.Estimate())
	})

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			groupBy:             cfg.GroupBy,
			extraLabels:         cfg.Labels,
			interval:            cfg.Interval,
			metricPrefix:        metricPrefix,
			groupByKeysLabel:    groupByKeysLabel,
			groupLimit:          int64(cfg.GroupLimit),
			groupSize:           &e.groupSize,
			groupRejectedMu:     &e.groupRejectedMu,
			groupRejectedSketch: e.groupRejectedSketch,

			precision: cfg.HLLPrecision,
			sparse:    *cfg.HLLSparse,
		}

		if len(cfg.GroupBy) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[string]*hyperloglog.Sketch)
			eb.prevGroups = make(map[string]*hyperloglog.Sketch)

			e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_estimator_group_size{group_by_keys=%q,bucket="%d"}`, eb.groupByKeysLabel, i), func() float64 {
				return float64(eb.groupSize.Load())
			})
			e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_estimator_group_limit{group_by_keys=%q,bucket="%d"}`, eb.groupByKeysLabel, i), func() float64 {
				return float64(eb.groupLimit)
			})
		}

		go e.runRotation(cfg.Interval)

		e.buckets[i] = eb
	}

	metrics.RegisterSet(e.metricsSet)

	return e, nil
}

func (e *estimator) stop() {
	close(e.stopCh)
	e.metricsSet.UnregisterAllMetrics()
}

var groupValuesPool = sync.Pool{}

func getGroupValuesSlice() []byte {
	v0 := groupValuesPool.Get()
	if v0 == nil {
		return nil
	}

	return v0.([]byte)
}

func putGroupValuesSlice(key []byte) {
	key = key[:0]
	groupValuesPool.Put(key)
}

func (e *estimator) insertMany(tss []protoparser.TimeSerie) {
	bucketsNum := uint64(len(e.buckets))

	groupValues := getGroupValuesSlice()
	defer putGroupValuesSlice(groupValues)

	var cnt int
	for _, ts := range tss {
		groupValues = groupValues[:0]

		if len(e.groupBy) == 0 {
			i := int(ts.Fingerprint % bucketsNum)
			e.buckets[i].insert(ts, "")
			continue
		}

		var hasNames bool
		for i, labelName := range e.groupBy {
			if i > 0 {
				groupValues = append(groupValues, ',')
			}

			for _, l := range ts.GroupLabels {
				if l.Name == labelName {
					hasNames = true

					groupValues = append(groupValues, l.Value...)
					break
				}
			}
		}

		// time series does not contribute to this groupBy
		if !hasNames {
			continue
		}

		i := int(hash(groupValues) % bucketsNum)
		e.buckets[i].insert(ts, b2s(groupValues))
		cnt++
	}

	e.insertTotal.Add(cnt)
}

func (e *estimator) reset() {
	e.groupSize.Store(0)
	for _, b := range e.buckets {
		b.reset()
	}

	e.groupRejectedMu.Lock()
	e.groupRejectedSketch.Reset()
	e.groupRejectedMu.Unlock()
}

func (e *estimator) writeMetrics(w io.Writer) {
	formatBuf := make([]byte, 0, 1024)
	eb0 := e.buckets[0]

	if len(e.groupBy) == 0 {
		resSK := eb0.newSketch()
		for _, eb := range e.buckets {
			eb.writeNoGroupMetric(resSK)
		}

		formatBuf = append(formatBuf, eb0.metricPrefix...)
		formatBuf = append(formatBuf, `,group_by_keys="__global__"} `...)
		formatBuf = strconv.AppendUint(formatBuf, resSK.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		w.Write(formatBuf)
		return
	}

	for _, b := range e.buckets {
		b.writeGroupMetrics(w, formatBuf, eb0.groupByKeysLabel)
	}

	groupSize := e.groupSize.Load()
	if groupSize >= int64(float64(e.groupLimit)*0.8) {
		e.groupRejectedMu.Lock()
		res := mustNewGroupRejectSketch()
		e.groupRejectedSketch.Merge(res)
		e.groupRejectedSketchPrev.Merge(res)
		e.groupRejectedMu.Unlock()

		groupSize += int64(res.Estimate())
	}

	formatBuf = formatBuf[:0]
	formatBuf = append(formatBuf, eb0.metricPrefix...)
	formatBuf = append(formatBuf, `,group_by_keys="__group__",group_by_values="`...)
	formatBuf = append(formatBuf, eb0.groupByKeysLabel...)
	formatBuf = append(formatBuf, `"} `...)
	formatBuf = strconv.AppendInt(formatBuf, groupSize, 10)
	formatBuf = append(formatBuf, "\n"...)
	w.Write(formatBuf)
}

func (e *estimator) runRotation(interval time.Duration) {
	t := time.NewTicker(interval / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.rotate()
		case <-e.stopCh:
			return
		}
	}
}

func (e *estimator) rotate() {
	e.groupSize.Store(0)

	var wg sync.WaitGroup
	for i := range e.buckets {
		wg.Go(e.buckets[i].rotate)
	}
	wg.Wait()

	e.groupRejectedMu.Lock()
	prevSK := e.groupRejectedSketchPrev
	prevSK.Reset()
	e.groupRejectedSketchPrev = e.groupRejectedSketch
	e.groupRejectedSketch = prevSK
	e.groupRejectedMu.Unlock()
}

type estimatorBucket struct {
	mu sync.Mutex

	groupBy          []string
	groupLimit       int64
	extraLabels      map[string]string
	interval         time.Duration
	metricPrefix     string
	groupByKeysLabel string
	precision        uint8
	sparse           bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  *atomic.Int64
	groups     map[string]*hyperloglog.Sketch
	prevGroups map[string]*hyperloglog.Sketch

	groupRejectedMu     *sync.Mutex
	groupRejectedSketch *hyperloglog.Sketch
}

func (eb *estimatorBucket) String() string {
	return fmt.Sprintf(
		"interval: %s; group_by: %v; extra_labels: %v", eb.interval, eb.groupBy, eb.extraLabels)
}

func (eb *estimatorBucket) reset() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.sketch.Reset()
		return
	}

	for k := range eb.groups {
		delete(eb.groups, k)
	}
	for k := range eb.prevGroups {
		delete(eb.prevGroups, k)
	}
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
	eb.groups = make(map[string]*hyperloglog.Sketch, len(eb.groups))
	eb.mu.Unlock()

	eb.groupSize.Add(int64(len(eb.prevGroups)))
}

func (eb *estimatorBucket) insert(ts protoparser.TimeSerie, groupValues string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.sketch.InsertHash(ts.Fingerprint)
		return
	}

	sk := eb.groups[groupValues]
	if sk == nil {
		if eb.prevGroups[groupValues] == nil {
			groupSize := eb.groupSize.Load()
			if groupSize+1 > eb.groupLimit {
				eb.groupRejectedMu.Lock()
				eb.groupRejectedSketch.InsertHash(hash([]byte(groupValues)))
				eb.groupRejectedMu.Unlock()
				return
			}

			eb.groupSize.Add(1)
		}

		sk = eb.newSketch()
		eb.groups[strings.Clone(groupValues)] = sk
	}
	sk.InsertHash(ts.Fingerprint)
}

func (eb *estimatorBucket) writeNoGroupMetric(res *hyperloglog.Sketch) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.estimateSketch(eb.sketch, eb.prevSketch, res)
	return
}

func (eb *estimatorBucket) writeGroupMetrics(w io.Writer, formatBuf []byte, groupByKey string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	res := eb.newSketch()
	for groupByVal := range eb.groups {
		res.Reset()
		formatBuf = formatBuf[:0]

		eb.estimateSketch(eb.groups[groupByVal], eb.prevGroups[groupByVal], res)
		formatBuf = append(formatBuf, eb.metricPrefix...)
		formatBuf = append(formatBuf, `,group_by_keys="`...)
		formatBuf = append(formatBuf, groupByKey...)
		formatBuf = append(formatBuf, `",group_by_values="`...)
		formatBuf = append(formatBuf, groupByVal...)
		formatBuf = append(formatBuf, `"} `...)
		formatBuf = strconv.AppendUint(formatBuf, res.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		w.Write(formatBuf)
	}

	for groupByVal := range eb.prevGroups {
		if _, ok := eb.groups[groupByVal]; ok {
			continue
		}

		res.Reset()
		formatBuf = formatBuf[:0]

		eb.estimateSketch(nil, eb.prevGroups[groupByVal], res)
		formatBuf = append(formatBuf, eb.metricPrefix...)
		formatBuf = append(formatBuf, `,group_by_keys="`...)
		formatBuf = append(formatBuf, groupByKey...)
		formatBuf = append(formatBuf, `",group_by_values="`...)
		formatBuf = append(formatBuf, groupByVal...)
		formatBuf = append(formatBuf, `"} `...)
		formatBuf = strconv.AppendUint(formatBuf, res.Estimate(), 10)
		formatBuf = append(formatBuf, "\n"...)
		w.Write(formatBuf)
	}
}

func (eb *estimatorBucket) ensureKeySet(res map[string]*hyperloglog.Sketch, key string) {
	if _, ok := res[key]; !ok {
		res[key] = eb.newSketch()
	}
}

func (eb *estimatorBucket) estimateSketch(cur, prev, res *hyperloglog.Sketch) {
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

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
