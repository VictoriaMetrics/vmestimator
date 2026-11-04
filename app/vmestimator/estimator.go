package main

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
	"github.com/dgryski/go-metro"
	"github.com/valyala/fastrand"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

const labelKeyword = "__label__"

type estimator struct {
	// id uniquely identifies the estimator by its labels, groupBy, interval and filter.
	// It is passed to snapshots so they can be merged across vmestimator instances.
	id string

	// cfg is the config the estimator was created from.
	cfg EstimatorConfig

	groupBy        []string
	groupSize      *groupSize
	compiledFilter compiledFilter

	hasLabelKeyword bool

	minCardinality int64

	buckets []*estimatorBucket

	metricsSet  *metrics.Set
	insertTotal *metrics.Counter

	stopCh chan struct{}
}

func newEstimator(cfg EstimatorConfig) (*estimator, error) {
	cfg.applyDefaults()

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
		id:              estimatorID(cfg.Labels, cfg.GroupBy, cfg.Interval, cfg.Filter),
		cfg:             cfg,
		groupBy:         cfg.GroupBy,
		hasLabelKeyword: len(cfg.GroupBy) > 0 && cfg.GroupBy[len(cfg.GroupBy)-1] == labelKeyword,
		compiledFilter:  cf,
		minCardinality:  cfg.MinCardinality,
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
			idx:            i,
			groupSize:      e.groupSize,
			groupBy:        cfg.GroupBy,
			interval:       cfg.Interval,
			calculateChurn: cfg.CalculateChurn,
			labels:         cfg.Labels,
			filter:         cfg.Filter,

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

func estimatorID(labels map[string]string, groupBy []string, interval time.Duration, filter string) string {
	groupByKey := `__global__`
	if len(groupBy) > 0 {
		groupByKey = strings.Join(groupBy, ",")
	}

	var labelsKey string
	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(labels[k])
			b.WriteByte('\x00')
		}
		labelsKey = b.String()
	}

	return fmt.Sprintf("\u0000labels=%s\u0000group_by=%s\u0000interval=%v\u0000filter=%v", labelsKey, groupByKey, interval, filter)
}

func (e *estimator) stop() {
	close(e.stopCh)
	metrics.UnregisterSet(e.metricsSet, true)
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
			e.buckets[bi].insert(ts.Fingerprint)
			cnt++
		}
		e.insertTotal.Add(cnt)
		return
	}

	// When __label__ is present it is always the last element; iterate only the explicit keys.
	groupByKeys := e.groupBy
	if e.hasLabelKeyword {
		groupByKeys = e.groupBy[:len(e.groupBy)-1]
	}

	d := getDigest()
	defer putDigest(d)

	// Inserts are collected first and then applied per bucket, so every bucket is locked
	// at most once per insertMany call instead of once per series (or per label for __label__).
	b := getGroupInsertBatch()
	defer putGroupInsertBatch(b)
	items := b.items[:0]

	tssLen := uint32(len(tss))
	start := fastrand.Uint32n(tssLen)
	for j := uint32(0); j < tssLen; j++ {
		i := (start + j) % tssLen

		ts := &tss[i]
		if !e.compiledFilter.match(ts.Labels) {
			continue
		}

		d.Reset()

		// hasNames starts true when there are no explicit keys (pure __label__ mode).
		hasNames := len(groupByKeys) == 0
		for _, labelName := range groupByKeys {
			for _, l := range ts.Labels {
				if l.Name == labelName {
					hasNames = true

					_, _ = d.WriteString("\u0000")
					_, _ = d.WriteString(l.Value)
					break
				}
			}
		}

		// time series does not contribute to this groupBy
		if !hasNames {
			continue
		}

		if !e.hasLabelKeyword {
			items = append(items, groupInsert{fp: ts.Fingerprint, key: d.Sum64(), tsIdx: i, labelIdx: -1})
			continue
		}

		// __label__ expansion: one insert per label in the series.
		if len(groupByKeys) > 0 {
			_, _ = d.WriteString("\u0000")
		}
		for li, label := range ts.Labels {
			if label.Fingerprint == 0 {
				// Label fingerprints are missing for requests parsed before a config reload
				// enabled them via protoparser.SetFingerprintLabels; skip such labels.
				continue
			}

			ld := *d
			_, _ = ld.WriteString(label.Name)
			items = append(items, groupInsert{fp: label.Fingerprint, key: ld.Sum64(), tsIdx: i, labelIdx: int32(li)})
		}
	}
	b.items = items

	e.insertGroups(b, tss)
	e.insertTotal.Add(len(items))
}

// groupInsert is a pending insert of fp into the group identified by key.
// Group values are materialized only when a new group is created,
// so only the position of the series (and of the expanded label for __label__) is kept.
type groupInsert struct {
	fp       uint64
	key      uint64
	tsIdx    uint32
	labelIdx int32
}

type groupInsertBatch struct {
	items   []groupInsert
	sorted  []groupInsert
	offsets []int
}

var groupInsertBatchPool sync.Pool

func getGroupInsertBatch() *groupInsertBatch {
	v := groupInsertBatchPool.Get()
	if v == nil {
		return &groupInsertBatch{}
	}
	return v.(*groupInsertBatch)
}

func putGroupInsertBatch(b *groupInsertBatch) {
	b.items = b.items[:0]
	b.sorted = b.sorted[:0]
	groupInsertBatchPool.Put(b)
}

// insertGroups groups b.items by bucket and inserts every group under a single bucket lock.
func (e *estimator) insertGroups(b *groupInsertBatch, tss []protoparser.TimeSerie) {
	if len(b.items) == 0 {
		return
	}
	bucketsNum := uint64(len(e.buckets))

	// Counting sort by bucket index.
	offsets := slices.Grow(b.offsets[:0], len(e.buckets))[:len(e.buckets)]
	clear(offsets)
	for _, it := range b.items {
		offsets[it.key%bucketsNum]++
	}
	var sum int
	for bi, n := range offsets {
		offsets[bi] = sum
		sum += n
	}
	sorted := slices.Grow(b.sorted[:0], len(b.items))[:len(b.items)]
	for _, it := range b.items {
		bi := it.key % bucketsNum
		sorted[offsets[bi]] = it
		offsets[bi]++
	}
	// offsets[bi] now points to the end of bucket bi items.
	b.offsets = offsets
	b.sorted = sorted

	// Start from a random bucket, so concurrent callers do not convoy on the same bucket locks.
	start := int(fastrand.Uint32n(uint32(len(e.buckets))))
	for j := range e.buckets {
		bi := (start + j) % len(e.buckets)
		from := 0
		if bi > 0 {
			from = offsets[bi-1]
		}
		if from == offsets[bi] {
			continue
		}
		e.buckets[bi].insertGroups(sorted[from:offsets[bi]], tss)
	}
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
		s.ID = e.id
		s.Interval = eb0.interval
		s.CalculateChurn = eb0.calculateChurn
		s.Filter = eb0.filter
		s.Labels = eb0.labels
		s.GroupBy = nil
		s.MinCardinality = e.minCardinality
		return cb(s)
	}

	const batchSize = 1000

	eb0 := e.buckets[0]
	s.ID = e.id
	s.GroupLimit = eb0.groupSize.limit
	s.GroupBy = eb0.groupBy
	s.Interval = eb0.interval
	s.CalculateChurn = eb0.calculateChurn
	s.Filter = eb0.filter
	s.Labels = eb0.labels
	s.MinCardinality = e.minCardinality

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
			GroupBy:        eb0.groupBy,
			Interval:       eb0.interval,
			Filter:         eb0.filter,
			Labels:         eb0.labels,
			GroupLimit:     eb0.groupSize.limit,
			MinCardinality: e.minCardinality,
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
	calculateChurn  bool
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

func (eb *estimatorBucket) insert(fp uint64) {
	eb.mu.Lock()
	eb.sketch.InsertHash(fp)
	eb.mu.Unlock()
}

// insertGroups inserts items into the group sketches of eb under a single lock acquisition.
// tss must be the slice items were collected from.
func (eb *estimatorBucket) insertGroups(items []groupInsert, tss []protoparser.TimeSerie) {
	eb.mu.Lock()
	for _, it := range items {
		gsk, ok := eb.groups[it.key]
		if !ok {
			var values []string
			if prevGSK, ok := eb.prevGroups[it.key]; ok {
				values = prevGSK.values
			} else if !eb.groupSize.allowInsertLocked(eb.idx, it.key) {
				continue
			}

			if values == nil {
				values = eb.newGroupValues(&tss[it.tsIdx], it.labelIdx)
			}
			gsk = groupSketch{
				values: values,
				Sketch: eb.newSketch(),
			}
			eb.groups[it.key] = gsk
		}
		gsk.InsertHash(it.fp)
	}
	eb.mu.Unlock()
}

// newGroupValues returns a copy of the groupBy values of ts.
// labelIdx is the index of the label in ts.Labels expanded by __label__; it is ignored without __label__.
func (eb *estimatorBucket) newGroupValues(ts *protoparser.TimeSerie, labelIdx int32) []string {
	values := make([]string, len(eb.groupBy))
	keys := eb.groupBy
	if eb.hasLabelKeyword {
		keys = keys[:len(keys)-1]
		values[len(keys)] = strings.Clone(ts.Labels[labelIdx].Name)
	}
	for i, labelName := range keys {
		for _, l := range ts.Labels {
			if l.Name == labelName {
				values[i] = strings.Clone(l.Value)
				break
			}
		}
	}
	return values
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
