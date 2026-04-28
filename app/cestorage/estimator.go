package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/dgryski/go-metro"

	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

type estimator struct {
	groupBy []string

	buckets []*estimatorBucket
}

func newEstimator(cfg EstimatorConfig) (*estimator, error) {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute * 5
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
		groupBy: cfg.Group,
		buckets: make([]*estimatorBucket, 20),
	}

	for i := 0; i < len(e.buckets); i++ {
		eb := &estimatorBucket{
			groupBy:      cfg.Group,
			extraLabels:  cfg.Labels,
			interval:     cfg.Interval,
			metricPrefix: metricPrefix,
			precision:    14,
			sparse:       true,

			stopCh: make(chan struct{}),
		}

		if len(cfg.Group) == 0 {
			eb.sketch = eb.newSketch()
		} else {
			eb.groups = make(map[string]*hyperloglog.Sketch)
			eb.prevGroups = make(map[string]*hyperloglog.Sketch)
		}

		go eb.runRotation()

		e.buckets[i] = eb
	}

	return e, nil
}

func (e *estimator) stop() {
	for _, b := range e.buckets {
		b.stop()
	}
}

var keyPool = sync.Pool{}

func getKeySlice() []byte {
	v0 := keyPool.Get()
	if v0 == nil {
		return nil
	}

	return v0.([]byte)
}

func putKeySlice(key []byte) {
	key = key[:0]
	keyPool.Put(key)
}

func (e *estimator) insertMany(tss []protoparser.TimeSerie) {
	bucketsNum := uint64(len(e.buckets))

	key := getKeySlice()
	defer putKeySlice(key)

	for _, ts := range tss {
		key = key[:0]

		if len(e.groupBy) == 0 {
			i := int(ts.Fingerprint % bucketsNum)
			e.buckets[i].insert(ts, "")
			continue
		}

		var hasNonEmptyLabel bool
		for i, labelName := range e.groupBy {
			if i > 0 {
				key = append(key, ',')
			}

			for _, l := range ts.GroupLabels {
				if l.Name == labelName {
					if l.Value != "" {
						hasNonEmptyLabel = true
					}

					key = append(key, l.Value...)
					break
				}
			}
		}
		// time series does not contribute to this group
		if !hasNonEmptyLabel {
			continue
		}

		i := int(hash(key) % bucketsNum)
		e.buckets[i].insert(ts, string(key))
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

	keys := 0
	groupByKey := strings.Join(eb0.groupBy, ",")
	for _, b := range e.buckets {
		keys += b.writeGroupMetrics(w, formatBuf, groupByKey)
	}

	formatBuf = formatBuf[:0]
	formatBuf = append(formatBuf, eb0.metricPrefix...)
	formatBuf = append(formatBuf, `,group_by_keys="__group__",group_by_values="`...)
	formatBuf = append(formatBuf, groupByKey...)
	formatBuf = append(formatBuf, `"} `...)
	formatBuf = strconv.AppendInt(formatBuf, int64(keys), 10)
	formatBuf = append(formatBuf, "\n"...)
	w.Write(formatBuf)
}

//type groupsMap = haxmap.Map[string, *sketch]
//
//func newGroupsMap() *groupsMap {
//	return haxmap.New[string, *sketch]()
//}

type estimatorBucket struct {
	mu sync.Mutex

	groupBy      []string
	extraLabels  map[string]string
	interval     time.Duration
	metricPrefix string
	precision    uint8
	sparse       bool

	sketch     *hyperloglog.Sketch
	prevSketch *hyperloglog.Sketch
	groups     map[string]*hyperloglog.Sketch
	prevGroups map[string]*hyperloglog.Sketch

	stopCh chan struct{}
}

func (eb *estimatorBucket) String() string {
	return fmt.Sprintf(
		"interval: %s; group_by: %v; extra_labels: %v", eb.interval, eb.groupBy, eb.extraLabels)
}

// Stop stops the background rotation goroutine, if any.
func (eb *estimatorBucket) stop() {
	close(eb.stopCh)
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

// runRotation resets the sketches on every tick until stopCh is closed.
func (eb *estimatorBucket) runRotation() {
	t := time.NewTicker(eb.interval / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			eb.rotate()
		case <-eb.stopCh:
			return
		}
	}
}

func (eb *estimatorBucket) rotate() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if len(eb.groupBy) == 0 {
		eb.prevSketch = eb.sketch
		eb.sketch = eb.newSketch()
		return
	}

	eb.prevGroups = eb.groups
	eb.groups = make(map[string]*hyperloglog.Sketch, len(eb.groups))
}

func (eb *estimatorBucket) insert(ts protoparser.TimeSerie, groupKey string) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if groupKey == "" {
		eb.sketch.InsertHash(ts.Fingerprint)
		return
	}

	sk := eb.groups[groupKey]
	if sk == nil {
		sk = eb.newSketch()
		eb.groups[groupKey] = sk
	}
	sk.InsertHash(ts.Fingerprint)
}

func (eb *estimatorBucket) writeNoGroupMetric(res *hyperloglog.Sketch) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.estimateSketch(eb.sketch, eb.prevSketch, res)
	return
}

func (eb *estimatorBucket) writeGroupMetrics(w io.Writer, formatBuf []byte, groupByKey string) int {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	res := eb.newSketch()
	keys := len(eb.groups)
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

		keys++
	}

	return keys
}

func (eb *estimatorBucket) ensureKeySet(res map[string]*hyperloglog.Sketch, key string) {
	if _, ok := res[key]; !ok {
		res[key] = eb.newSketch()
	}
}

func (eb *estimatorBucket) estimateSketch(cur, prev, res *hyperloglog.Sketch) {
	if cur != nil {
		if err := res.Merge(cur); err != nil {
			panic(err)
		}
	}
	if prev != nil {
		if err := res.Merge(prev); err != nil {
			panic(err)
		}
	}
}

func (eb *estimatorBucket) newSketch() *hyperloglog.Sketch {
	sk, err := hyperloglog.NewSketch(eb.precision, eb.sparse)
	if err != nil {
		panic(fmt.Sprintf("cannot create HLL sketch with precision=%d and sparse=%v: %s", eb.precision, eb.sparse, err))
	}

	return sk
}

func hash(v []byte) uint64 {
	return metro.Hash64(v, 1337)
}
