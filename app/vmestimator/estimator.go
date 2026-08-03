package main

import (
	"encoding/gob"
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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

type estimator struct {
	groupBy   []string
	groupSize *groupSize

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

	e := &estimator{
		groupBy: cfg.GroupBy,
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
		fmt.Sprintf(`vmestimator_estimator_insert_total{%s,interval=%q}`, appendGroupByKeysLabel(nil, cfg.GroupBy), cfg.Interval),
	)
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_rejected_size{group_by_keys=%q,interval=%q}`, appendGroupByKeysLabel(nil, cfg.GroupBy), cfg.Interval), func() float64 {
		return float64(e.groupSize.totalRejected())
	})

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			idx:       i,
			groupSize: e.groupSize,
			groupBy:   cfg.GroupBy,
			interval:  cfg.Interval,
			labels:    cfg.Labels,

			precision: cfg.HLLPrecision,
			sparse:    *cfg.HLLSparse,
		}

		if len(cfg.GroupBy) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[values]*hyperloglog.Sketch)
			eb.prevGroups = make(map[values]*hyperloglog.Sketch)
		}

		e.buckets[i] = eb
	}

	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_limit{%s,interval=%q}`, appendGroupByKeysLabel(nil, cfg.GroupBy), cfg.Interval), func() float64 {
		return float64(e.groupSize.limit)
	})
	e.metricsSet.NewGauge(fmt.Sprintf(`vmestimator_estimator_group_size{%s,interval=%q}`, appendGroupByKeysLabel(nil, cfg.GroupBy), cfg.Interval), func() float64 {
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
		tssLen := uint32(len(tss))
		start := fastrand.Uint32n(tssLen)
		for j := uint32(0); j < tssLen; j++ {
			i := (start + j) % tssLen

			ts := tss[i]
			bi := int(ts.Fingerprint % bucketsNum)
			e.buckets[bi].insert(ts, values{})
		}
		e.insertTotal.Add(len(tss))
		return
	}

	var cnt int
	tssLen := uint32(len(tss))
	start := fastrand.Uint32n(tssLen)
	for j := uint32(0); j < tssLen; j++ {
		i := (start + j) % tssLen

		ts := tss[i]

		groupValuesKey := values{
			Cap: len(e.groupBy),
		}
		var hasNames bool
		for i, labelName := range e.groupBy {
			for _, l := range ts.Labels {
				if l.Name == labelName {
					hasNames = true

					groupValuesKey.Arr[i] = l.Value
					break
				}
			}
		}

		// time series does not contribute to this groupBy
		if !hasNames {
			continue
		}

		bi := int(groupValuesKey.Hash() % bucketsNum)
		e.buckets[bi].insert(ts, groupValuesKey)
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

	s := newSnapshot()
	if len(e.groupBy) == 0 {
		s = convertGlobalEstimatorToSnapshot(e, s)
		if err := s.writeCardinalityEstimates(w); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
		return
	}

	skp := newSketchesPool(eb0.precision, eb0.groupSize.avgBucketSize())
	for _, eb := range e.buckets {
		s.reset()
		convertEstimatorBucketToSnapshot(eb, s, skp)
		if err := s.writeCardinalityEstimates(w); err != nil {
			logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
		}
		for _, sk := range s.Sketches {
			skp.put(sk)
		}
	}
	if err := s.writeGroupSizeAndLimit(w); err != nil {
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
		if err := enc.Encode(convertGlobalEstimatorToSnapshot(e, s)); err != nil {
			return fmt.Errorf("encode snapshot: %w", err)
		}

		return nil
	}

	eb0 := e.buckets[0]

	skp := newSketchesPool(eb0.precision, eb0.groupSize.avgBucketSize())
	s := newSnapshot()
	for _, eb := range e.buckets {
		s.reset()
		convertEstimatorBucketToSnapshot(eb, s, skp)
		if err := enc.Encode(s); err != nil {
			return fmt.Errorf("encode snapshot: %w", err)
		}
		for _, sk := range s.Sketches {
			skp.put(sk)
		}
	}

	return nil
}

type estimatorBucket struct {
	mu sync.Mutex

	idx       int
	groupBy   []string
	interval  time.Duration
	precision uint8
	sparse    bool
	labels    map[string]string

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch

	groupSize  *groupSize
	groups     map[values]*hyperloglog.Sketch
	prevGroups map[values]*hyperloglog.Sketch
}

func (eb *estimatorBucket) reset() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.prevSketch.Reset()
		eb.sketch.Reset()
		return
	}

	eb.groups = make(map[values]*hyperloglog.Sketch)
	eb.prevGroups = make(map[values]*hyperloglog.Sketch)

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
	eb.groups = make(map[values]*hyperloglog.Sketch, len(eb.groups))
	eb.groupSize.rotateLocked(eb.idx, int64(len(eb.prevGroups)))
	eb.mu.Unlock()
}

func (eb *estimatorBucket) insert(ts protoparser.TimeSerie, groupValuesKey values) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.sketch.InsertHash(ts.Fingerprint)
		return
	}

	gsk, ok := eb.groups[groupValuesKey]
	if !ok {
		if _, ok := eb.prevGroups[groupValuesKey]; !ok {
			if !eb.groupSize.allowInsertLocked(eb.idx, groupValuesKey) {
				return
			}
		}

		gsk = eb.newSketch()
		eb.groups[groupValuesKey.Clone()] = gsk
	}
	gsk.InsertHash(ts.Fingerprint)
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
func (gs *groupSize) allowInsertLocked(bucketIdx int, groupValuesKey values) bool {
	if gs.size.Load() >= gs.limit {
		gs.rejectMu.Lock()
		sk := gs.rejectSketches[bucketIdx]
		if sk == nil {
			sk = mustNewGroupRejectSketch()
			gs.rejectSketches[bucketIdx] = sk
		}
		sk.InsertHash(groupValuesKey.Hash())
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

type values struct {
	Cap int
	Arr [10]string

	hash uint64
}

func (vs values) Hash() uint64 {
	if vs.hash > 0 {
		return vs.hash
	}

	h := getHasher()
	defer putHasher(h)

	for i := 0; i < vs.Cap; i++ {
		_, _ = h.Write(bytesutil.ToUnsafeBytes(vs.Arr[i]))
	}
	vs.hash = h.Sum64()
	return vs.hash
}

func (vs values) Clone() values {
	ck := values{
		Cap: vs.Cap,
	}

	for i := 0; i < vs.Cap; i++ {
		ck.Arr[i] = strings.Clone(vs.Arr[i])
	}

	return ck
}

func getHasher() *xxhash.Digest {
	v := hasherPool.Get()
	if v == nil {
		return xxhash.New()
	}
	return v.(*xxhash.Digest)
}

func putHasher(h *xxhash.Digest) {
	h.Reset()
	hasherPool.Put(h)
}

var hasherPool sync.Pool
