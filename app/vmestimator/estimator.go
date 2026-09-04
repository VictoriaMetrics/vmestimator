package main

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
	"github.com/dgryski/go-metro"
	"github.com/valyala/fastrand"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

const labelKeyword = "__label__"

type estimator struct {
	groupBy        []string
	groupSize      *groupSize
	compiledFilter compiledFilter

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

	if len(cfg.GroupBy) > 5 {
		return nil, fmt.Errorf("group by must not be bigger than 5 elements; got %d", len(cfg.GroupBy))
	}

	if len(cfg.GroupBy) > 0 {
		for _, k := range cfg.GroupBy {
			if k == `__global__` || k == `__group__` {
				return nil, fmt.Errorf("group by %s is not allowed. __global__, __group__ are reserved keywords", k)
			}
		}
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
		groupBy:         cfg.GroupBy,
		hasLabelKeyword: len(cfg.GroupBy) > 0 && cfg.GroupBy[len(cfg.GroupBy)-1] == labelKeyword,
		compiledFilter:  cf,
		groupSize: &groupSize{
			limit:          int64(cfg.GroupLimit),
			bucketSizes:    make([]int64, cfg.Buckets),
			rejectSketches: make([]*hyperloglog.Sketch, cfg.Buckets),
		},
		buckets:    make([]*estimatorBucket, cfg.Buckets),
		metricsSet: metrics.NewSet(),
		stopCh:     make(chan struct{}),
	}

	groupByKeysLabel := appendGroupByKeysLabel(make([]byte, 0, 128), `group_by_values`, cfg.GroupBy)
	e.insertTotal = e.metricsSet.NewCounter(
		fmt.Sprintf(`vmestimator_estimator_insert_total{%s,interval=%q}`, groupByKeysLabel, cfg.Interval),
	)
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_rejected_size{group_by_keys=%q,interval=%q}`, groupByKeysLabel, cfg.Interval), func() float64 {
		return float64(e.groupSize.totalRejected())
	})

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			idx:       i,
			groupSize: e.groupSize,
			groupBy:   cfg.GroupBy,
			interval:  cfg.Interval,
			labels:    cfg.Labels,
			filter:    cfg.Filter,

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

	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_limit{%s,interval=%q}`, groupByKeysLabel, cfg.Interval), func() float64 {
		return float64(e.groupSize.limit)
	})
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_size{%s,interval=%q}`, groupByKeysLabel, cfg.Interval), func() float64 {
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

var formatBufPool = sync.Pool{}

func getFormatBuf() *[]byte {
	v0 := formatBufPool.Get()
	if v0 == nil {
		v := make([]byte, 0, 1024)
		return &v
	}

	return v0.(*[]byte)
}

func putFormatBuf(key *[]byte) {
	if key == nil {
		return
	}

	*key = (*key)[:0]
	formatBufPool.Put(key)
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
	// When __label__ is present it is always the last element; iterate only the explicit keys.
	groupByKeys := e.groupBy
	groupValues := make([]string, len(e.groupBy))
	if e.hasLabelKeyword {
		groupByKeys = e.groupBy[:len(e.groupBy)-1]
	}

	d := getDigest()
	defer putDigest(d)

	tssLen := uint32(len(tss))
	start := fastrand.Uint32n(tssLen)
	for j := uint32(0); j < tssLen; j++ {
		i := (start + j) % tssLen

		ts := tss[i]
		if !e.compiledFilter.match(ts.Labels) {
			continue
		}

		d.Reset()
		clear(groupValues)

		// hasNames starts true when there are no explicit keys (pure __label__ mode).
		hasNames := len(groupByKeys) == 0
		for i, labelName := range groupByKeys {
			for _, l := range ts.Labels {
				if l.Name == labelName {
					hasNames = true

					_, _ = d.WriteString("\u0000")
					_, _ = d.WriteString(l.Value)
					groupValues[i] = l.Value
					break
				}
			}
		}

		// time series does not contribute to this groupBy
		if !hasNames {
			continue
		}

		if !e.hasLabelKeyword {
			key := d.Sum64()
			bi := int(key % bucketsNum)
			e.buckets[bi].insert(ts.Fingerprint, key, groupValues)
			cnt++
			continue
		}

		// __label__ expansion: one insert per label in the series.
		labelIdx := len(e.groupBy) - 1
		for _, label := range ts.Labels {
			ld := *d
			if len(groupByKeys) > 0 {
				_, _ = ld.WriteString("\u0000")
			}

			if label.Fingerprint == 0 {
				panic(fmt.Sprintf("BUG: label %q has zero Fingerprint; group_by contains %s", label.Name, labelKeyword))
			}
			_, _ = ld.WriteString(label.Name)
			groupValues[labelIdx] = label.Name

			key := ld.Sum64()
			bi := int(key % bucketsNum)
			e.buckets[bi].insert(label.Fingerprint, key, groupValues)
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

// toSnapshot calls cb with a snapshot of the estimator's current state.
// For group estimators, cb may be called multiple times — once per batch of up to 1000 groups.
// The snapshot s is only valid for the duration of the cb call; it is reset and reused after cb returns.
// If cb returns an error, toSnapshot aborts and returns that error.
func (e *estimator) toSnapshot(cb func(s *snapshot) error) error {
	s := newSnapshot()
	if len(e.groupBy) == 0 {
		eb0 := e.buckets[0]
		resSK := eb0.newSketch()
		for _, eb := range e.buckets {
			eb.mu.Lock()
			eb.mergeSketches(eb.sketch, eb.prevSketch, resSK)
			eb.mu.Unlock()
		}
		s.Sketches[0] = SnapshotSketch{Sketch: resSK}
		s.Interval = eb0.interval
		s.Filter = eb0.filter
		s.Labels = eb0.labels
		s.GroupBy = nil
		return cb(s)
	}

	const batchSize = 1000

	eb0 := e.buckets[0]
	s.GroupLimit = eb0.groupSize.limit
	s.GroupBy = eb0.groupBy
	s.Interval = eb0.interval
	s.Filter = eb0.filter
	s.Labels = eb0.labels

	skp := newSketchesPool(eb0.precision, min(batchSize, eb0.groupSize.avgBucketSize()))
	keys := make([]uint64, 0, batchSize)

	for _, eb := range e.buckets {
		eb.mu.Lock()
		groups := eb.groups
		prevGroups := eb.prevGroups
		keys = keys[:0]
		for k := range groups {
			keys = append(keys, k)
		}
		for k := range prevGroups {
			if _, ok := groups[k]; !ok {
				keys = append(keys, k)
			}
		}
		eb.mu.Unlock()

		for i := 0; i < len(keys); i += batchSize {
			end := min(i+batchSize, len(keys))
			batch := keys[i:end]

			eb.mu.Lock()
			for _, key := range batch {
				var resSK *hyperloglog.Sketch
				var values []string
				gsk := groups[key]
				if gsk.Sketch != nil {
					resSK = skp.getForMerge(gsk.Sketch)
					values = gsk.values
				}

				prevGSK := prevGroups[key]
				if prevGSK.Sketch != nil && resSK == nil {
					resSK = skp.getForMerge(prevGSK.Sketch)
					values = prevGSK.values
				}

				eb.mergeSketches(gsk.Sketch, prevGSK.Sketch, resSK)

				s.Sketches[key] = SnapshotSketch{
					Values: values,
					Sketch: resSK,
				}
			}
			eb.mu.Unlock()

			if err := cb(s); err != nil {
				return err
			}

			for k, ssk := range s.Sketches {
				skp.put(ssk.Sketch)
				delete(s.Sketches, k)
			}
		}
	}

	// Always emit a final metadata-only snapshot so the decoder receives
	// GroupBy/GroupLimit/GroupRejectSize even when there are no groups.
	// GroupRejectSize is included here (not in per-batch snapshots) so that
	// merging on the decoder side accumulates it exactly once.
	s.GroupRejectSize = int64(eb0.groupSize.totalRejected())
	return cb(s)
}

func (e *estimator) writeMetrics(w io.Writer) {
	var dropped uint64
	if err := e.toSnapshot(func(s *snapshot) error {
		d, err := s.writeCardinalityEstimates(w)
		dropped += d
		return err
	}); err != nil {
		logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
	}

	if len(e.groupBy) > 0 {
		eb0 := e.buckets[0]
		s := &snapshot{
			GroupBy:    eb0.groupBy,
			Interval:   eb0.interval,
			Filter:     eb0.filter,
			Labels:     eb0.labels,
			GroupLimit: eb0.groupSize.limit,
		}
		if dropped > 0 {
			if err := s.writeDroppedMetric(w, dropped); err != nil {
				logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
			}
		}
		if err := s.writeGroupSizeAndLimit(w, eb0.groupSize.totalSize()); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
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

type estimatorBucket struct {
	mu sync.Mutex

	idx             int
	groupBy         []string
	interval        time.Duration
	filter          string
	precision       uint8
	sparse          bool
	labels          map[string]string
	hasLabelKeyword bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  *groupSize
	groups     map[uint64]groupSketch
	prevGroups map[uint64]groupSketch
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

	if len(eb.groupBy) == 0 {
		eb.sketch.InsertHash(fp)
		eb.mu.Unlock()
		return
	}

	gsk, ok := eb.groups[groupValuesKey]
	if !ok {
		var values []string
		if prevGSK, ok := eb.prevGroups[groupValuesKey]; ok {
			values = prevGSK.values
		} else {
			if !eb.groupSize.allowInsertLocked(eb.idx, groupValuesKey) {
				eb.mu.Unlock()
				return
			}
		}

		if values == nil {
			values = make([]string, len(groupValues))
			for i, v := range groupValues {
				values[i] = strings.Clone(v)
			}
		}
		gsk = groupSketch{
			values: values,
			Sketch: eb.newSketch(),
		}
		eb.groups[groupValuesKey] = gsk
	}
	gsk.InsertHash(fp)
	eb.mu.Unlock()
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
	*hyperloglog.Sketch

	values []string
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

func (gs *groupSize) avgBucketSize() int {
	return int(gs.size.Load()) / len(gs.bucketSizes)
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

func getDigest() *xxhash.Digest {
	d := digestPool.Get()
	if d == nil {
		return xxhash.New()
	}
	return d.(*xxhash.Digest)
}

func putDigest(d *xxhash.Digest) {
	d.Reset()
	digestPool.Put(d)
}

var digestPool sync.Pool
