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
	groupBy    []string
	buckets    []*estimatorBucket
	metricsSet *metrics.Set

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

	e := &estimator{
		groupBy:    cfg.GroupBy,
		buckets:    make([]*estimatorBucket, cfg.Buckets),
		metricsSet: metrics.NewSet(),
		stopCh:     make(chan struct{}),
	}

	groupKeyLabel := strings.Join(cfg.GroupBy, `,`)
	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			groupBy:       cfg.GroupBy,
			extraLabels:   cfg.Labels,
			interval:      cfg.Interval,
			metricPrefix:  metricPrefix,
			groupKeyLabel: groupKeyLabel,
			groupLimit:    int64(cfg.GroupLimit),

			precision: 14,
			sparse:    true,
		}

		if len(cfg.GroupBy) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[string]*hyperloglog.Sketch)
			eb.prevGroups = make(map[string]*hyperloglog.Sketch)

			e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_group_estimator_size{groupBy=%q,bucket="%d"}`, eb.groupKeyLabel, i), func() float64 {
				return float64(eb.groupSize.Load())
			})
			e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_group_estimator_rejected_size{groupBy=%q,bucket="%d"}`, eb.groupKeyLabel, i), func() float64 {
				eb.groupRejectedSketchMu.Lock()
				defer eb.groupRejectedSketchMu.Unlock()

				return float64(eb.groupRejectedSketch.Estimate())
			})
			e.metricsSet.NewGauge(fmt.Sprintf(`cestorage_group_limit{groupBy=%q,bucket="%d"}`, eb.groupKeyLabel, i), func() float64 {
				return float64(eb.groupLimit)
			})
		}

		go e.runRotation(cfg.Interval)

		e.buckets[i] = eb
	}

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
	}
}

func (e *estimator) reset() {
	for _, b := range e.buckets {
		b.reset()
	}
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

	groupSize := int64(0)
	for _, b := range e.buckets {
		groupSize += b.writeGroupMetrics(w, formatBuf, eb0.groupKeyLabel)
	}

	formatBuf = formatBuf[:0]
	formatBuf = append(formatBuf, eb0.metricPrefix...)
	formatBuf = append(formatBuf, `,group_by_keys="__group__",group_by_values="`...)
	formatBuf = append(formatBuf, eb0.groupKeyLabel...)
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
	var wg sync.WaitGroup
	for i := range e.buckets {
		wg.Go(e.buckets[i].rotate)
	}
	wg.Wait()
}

type estimatorBucket struct {
	mu sync.Mutex

	groupBy       []string
	groupLimit    int64
	extraLabels   map[string]string
	interval      time.Duration
	metricPrefix  string
	groupKeyLabel string
	precision     uint8
	sparse        bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  atomic.Int64
	groups     map[string]*hyperloglog.Sketch
	prevGroups map[string]*hyperloglog.Sketch

	groupRejectedSketchMu sync.Mutex
	groupRejectedSketch   *hyperloglog.Sketch
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

	eb.groupSize.Store(int64(len(eb.prevGroups)))

	eb.groupRejectedSketchMu.Lock()
	if eb.groupRejectedSketch != nil {
		eb.groupRejectedSketch.Reset()
	}
	eb.groupRejectedSketchMu.Unlock()
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
				eb.groupRejectedSketchMu.Lock()
				if eb.groupRejectedSketch == nil {
					eb.groupRejectedSketch = mustNewGroupRejectSketch()
				}
				eb.groupRejectedSketch.InsertHash(hash([]byte(groupValues)))
				eb.groupRejectedSketchMu.Unlock()
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

func (eb *estimatorBucket) writeGroupMetrics(w io.Writer, formatBuf []byte, groupByKey string) int64 {
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

	return eb.groupSize.Load()
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
	return mustNewSketch(14, true)
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
