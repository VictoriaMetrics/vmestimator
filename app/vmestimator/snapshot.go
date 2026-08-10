package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/axiomhq/hyperloglog"
)

type snapshots struct {
	mu sync.Mutex
	m  map[string]*snapshot
}

func newSnapshots() *snapshots {
	return &snapshots{m: make(map[string]*snapshot)}
}

func (ss *snapshots) add(newS *snapshot) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	k := newS.key()
	if s, found := ss.m[k]; found {
		s.merge(newS)
		return
	}

	s := newSnapshot()
	s.merge(newS)
	ss.m[k] = s
}

func (ss *snapshots) writeMetrics(w io.Writer) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, s := range ss.m {
		if err := s.writeMetrics(w); err != nil {
			return err
		}
	}

	return nil
}

type snapshot struct {
	Interval time.Duration
	Labels   map[string]string
	Filter   string

	GroupBy         []string
	GroupLimit      int64
	GroupRejectSize int64

	// prom string metric => hll
	Sketches map[uint64]SnapshotSketch
}

type SnapshotSketch struct {
	Values []string
	Sketch *hyperloglog.Sketch
}

func newSnapshot() *snapshot {
	return &snapshot{
		Sketches: make(map[uint64]SnapshotSketch),
	}
}

// decodeSnapshot reads a stream of gob-encoded EstimatorMerge objects from the response and merges them into the provided estimatorMerge object.
func decodeSnapshots(r io.Reader, cb func(s *snapshot)) error {
	d := gob.NewDecoder(r)
	s := newSnapshot()
	for {
		s.reset()
		if err := d.Decode(s); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		cb(s)
	}
}

func (s *snapshot) merge(other *snapshot) {
	if s.GroupBy != nil && !slices.Equal(s.GroupBy, other.GroupBy) {
		logger.Panicf("BUG: merge snapshots must have the same groupBy; s: %v; other: %v", s.GroupBy, other.GroupBy)
	}
	if s.Interval != 0 && s.Interval != other.Interval {
		logger.Panicf("BUG: merge snapshots must have the same interval; s: %s; other: %s", s.Interval, other.Interval)
	}
	if s.Filter != "" && s.Filter != other.Filter {
		logger.Panicf("BUG: merge snapshots must have the same filter; s: %s; other: %s", s.Filter, other.Filter)
	}

	for key, otherSSK := range other.Sketches {
		if existingSSK, ok := s.Sketches[key]; ok {
			_ = existingSSK.Sketch.Merge(otherSSK.Sketch)
		} else {
			s.Sketches[key] = SnapshotSketch{
				Values: append([]string{}, otherSSK.Values...),
				Sketch: otherSSK.Sketch.Clone(),
			}
		}
	}

	s.Interval = other.Interval
	s.Filter = other.Filter
	for k, v := range other.Labels {
		if s.Labels == nil {
			s.Labels = make(map[string]string)
		}
		s.Labels[k] = v
	}

	s.GroupBy = append(s.GroupBy[:0], other.GroupBy...)
	s.GroupLimit = other.GroupLimit
	s.GroupRejectSize += other.GroupRejectSize
}

func (s *snapshot) writeMetrics(w io.Writer) error {
	if err := s.writeCardinalityEstimates(w); err != nil {
		return err
	}
	if len(s.GroupBy) > 0 {
		if err := s.writeGroupSizeAndLimit(w, int64(len(s.Sketches))+s.GroupRejectSize); err != nil {
			return err
		}
	}
	return nil
}

// writeCardinalityEstimates writes metrics to w.
// w must be a buffered writer.
func (s *snapshot) writeCardinalityEstimates(w io.Writer) error {
	tmpBufP := getFormatBuf()
	tmpBuf := *tmpBufP
	defer func() {
		*tmpBufP = tmpBuf
		putFormatBuf(tmpBufP)
	}()

	metricPrefixB := appendCardinalityEstimateMetricPrefix(make([]byte, 0, 128), s.Labels, s.Interval, s.Filter)
	metricPrefix := bytesutil.ToUnsafeString(metricPrefixB)

	if len(s.GroupBy) == 0 {
		tmpBuf = tmpBuf[:0]
		tmpBuf = appendCardinalityEstimateGlobalMetric(tmpBuf, metricPrefix)
		tmpBuf = strconv.AppendUint(tmpBuf, s.Sketches[0].Sketch.Estimate(), 10)
		tmpBuf = append(tmpBuf, "\n"...)
		if _, err := w.Write(tmpBuf); err != nil {
			return err
		}
		return nil
	}

	groupByKeysLabelB := appendGroupByKeysLabel(make([]byte, 0, 128), `group_by_keys`, s.GroupBy)
	groupByKeysLabel := bytesutil.ToUnsafeString(groupByKeysLabelB)

	for _, ssk := range s.Sketches {
		tmpBuf = tmpBuf[:0]
		tmpBuf = appendCardinalityEstimateGroupMetrics(tmpBuf, metricPrefix, groupByKeysLabel, s.GroupBy, ssk.Values)
		tmpBuf = strconv.AppendUint(tmpBuf, ssk.Sketch.Estimate(), 10)
		tmpBuf = append(tmpBuf, "\n"...)
		if _, err := w.Write(tmpBuf); err != nil {
			return err
		}
	}

	return nil
}

// writeGroupSizeAndLimit writes metrics to w.
// w must be a buffered writer.
func (s *snapshot) writeGroupSizeAndLimit(w io.Writer, groupSize int64) error {
	tmpBufP := getFormatBuf()
	tmpBuf := *tmpBufP
	defer func() {
		*tmpBufP = tmpBuf
		putFormatBuf(tmpBufP)
	}()

	metricPrefixB := appendCardinalityEstimateMetricPrefix(make([]byte, 0, 128), s.Labels, s.Interval, s.Filter)
	metricPrefix := bytesutil.ToUnsafeString(metricPrefixB)

	tmpBuf = tmpBuf[:0]
	tmpBuf = appendCardinalityEstimateGroupSizeMetric(tmpBuf, metricPrefix, s.GroupBy)
	tmpBuf = strconv.AppendInt(tmpBuf, groupSize, 10)
	tmpBuf = append(tmpBuf, "\n"...)
	if _, err := w.Write(tmpBuf); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	tmpBuf = tmpBuf[:0]
	tmpBuf = appendGroupLimitMetric(tmpBuf, s.GroupBy, s.Interval, s.Filter)
	tmpBuf = strconv.AppendInt(tmpBuf, s.GroupLimit, 10)
	tmpBuf = append(tmpBuf, "\n"...)
	if _, err := w.Write(tmpBuf); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

func (s *snapshot) key() string {
	groupByKey := `__global__`
	if len(s.GroupBy) > 0 {
		groupByKey = strings.Join(s.GroupBy, ",")
	}

	var labelsKey string
	if len(s.Labels) > 0 {
		keys := make([]string, 0, len(s.Labels))
		for k := range s.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(s.Labels[k])
			b.WriteByte('\x00')
		}
		labelsKey = b.String()
	}

	return fmt.Sprintf("\u0000labels=%s\u0000group_by=%s\u0000interval=%v\u0000filter=%v", labelsKey, groupByKey, s.Interval, s.Filter)
}

func (s *snapshot) reset() {
	s.GroupLimit = 0
	s.GroupRejectSize = 0
	s.Interval = 0
	s.Filter = ""
	s.GroupBy = s.GroupBy[:0]
	clear(s.Labels)
	clear(s.Sketches)
}

// appendCardinalityEstimateGlobalMetric produces:
// 'cardinality_estimate{interval="5m",group_by_keys="__global__"} '
func appendCardinalityEstimateGlobalMetric(buf []byte, metricPrefix string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,group_by_keys="__global__"} `...)
	return buf
}

// appendCardinalityEstimateGroupSizeMetric produces:
// 'cardinality_estimate{interval="5m",group_by_keys="__group__",group_by_values="fooKey,barKey"} '
func appendCardinalityEstimateGroupSizeMetric(buf []byte, metricPrefix string, keys []string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,group_by_keys="__group__",`...)
	buf = appendGroupByKeysLabel(buf, `group_by_values`, keys)
	buf = append(buf, `} `...)
	return buf
}

// appendGroupLimitMetric produces:
// 'vmestimator_estimator_group_limit{group_by_keys="fooKey,barKey",interval="5m"} '
func appendGroupLimitMetric(buf []byte, keys []string, interval time.Duration, filter string) []byte {
	buf = buf[:0]
	buf = append(buf, `vmestimator_estimator_group_limit{interval=`...)
	buf = strconv.AppendQuote(buf, interval.String())
	buf = append(buf, `,filter=`...)
	buf = strconv.AppendQuote(buf, filter)
	buf = append(buf, `,group_by_keys="__group__",`...)
	buf = appendGroupByKeysLabel(buf, `group_by_values`, keys)
	buf = append(buf, `} `...)
	return buf
}

// appendCardinalityEstimateGroupMetrics produces:
// 'cardinality_estimate{interval="5m",group_by_keys="fooKey,barKey",group_by_values='fooVal,BarVal',by_fooKey="fooVal",by_barKey="barVal"} '
func appendCardinalityEstimateGroupMetrics(buf []byte, metricPrefix, groupByKeysLabel string, keys, values []string) []byte {
	buf = append(buf, metricPrefix...)
	buf = append(buf, `,`...)
	buf = append(buf, groupByKeysLabel...)
	buf = append(buf, `,`...)

	tmpBufP := getFormatBuf()
	tmpBuf := *tmpBufP
	defer func() {
		*tmpBufP = tmpBuf
		putFormatBuf(tmpBufP)
	}()
	for i := 0; i < len(values); i++ {
		if i > 0 {
			tmpBuf = append(tmpBuf, ',')
		}

		tmpBuf = append(tmpBuf, []byte(values[i])...)
	}
	buf = append(buf, `group_by_values=`...)
	buf = strconv.AppendQuote(buf, bytesutil.ToUnsafeString(tmpBuf))

	for i := 0; i < len(values); i++ {
		buf = append(buf, ',')
		switch keys[i] {
		case `__name__`:
			buf = append(buf, `by__name__`...)
		case `__label__`:
			buf = append(buf, `by__label__`...)
		default:
			buf = append(buf, `by_`...)
			buf = append(buf, keys[i]...)
		}
		buf = append(buf, '=')
		buf = strconv.AppendQuote(buf, values[i])
	}

	buf = append(buf, `} `...)

	return buf
}

// appendCardinalityEstimateGroupMetrics produces:
// 'group_by_keys="fooKey,barKey"'
func appendGroupByKeysLabel(buf []byte, labelName string, keys []string) []byte {
	tmpBufP := getFormatBuf()
	tmpBuf := *tmpBufP
	defer func() {
		*tmpBufP = tmpBuf
		putFormatBuf(tmpBufP)
	}()

	for i := range keys {
		if i > 0 {
			tmpBuf = append(tmpBuf, ',')
		}

		tmpBuf = append(tmpBuf, keys[i]...)
	}
	buf = append(buf, labelName...)
	buf = append(buf, '=')
	buf = strconv.AppendQuote(buf, bytesutil.ToUnsafeString(tmpBuf))
	return buf
}

func appendCardinalityEstimateMetricPrefix(buf []byte, labels map[string]string, interval time.Duration, filter string) []byte {
	buf = append(buf, `cardinality_estimate{interval=`...)
	buf = strconv.AppendQuote(buf, interval.String())
	buf = append(buf, ",filter="...)
	buf = strconv.AppendQuote(buf, filter)

	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf = append(buf, ',')
			buf = append(buf, k...)
			buf = append(buf, '=')
			buf = strconv.AppendQuote(buf, labels[k])
		}
	}

	return buf
}

type sketchesPool struct {
	precision uint8
	sparseSKs []*hyperloglog.Sketch
	denseSKs  []*hyperloglog.Sketch
}

func newSketchesPool(precision uint8, size int) *sketchesPool {
	p := &sketchesPool{
		precision: precision,
		sparseSKs: make([]*hyperloglog.Sketch, 0, size),
	}
	for i := 0; i < size; i++ {
		p.sparseSKs = append(p.sparseSKs, mustNewSketch(precision, true))
	}
	return p
}

func (p *sketchesPool) getSparse() *hyperloglog.Sketch {
	n := len(p.sparseSKs)
	if n == 0 {
		return mustNewSketch(p.precision, true)
	}
	sk := p.sparseSKs[n-1]
	p.sparseSKs = p.sparseSKs[:n-1]
	return sk
}

func (p *sketchesPool) getDense() *hyperloglog.Sketch {
	n := len(p.denseSKs)
	if n == 0 {
		return mustNewSketch(p.precision, false)
	}
	sk := p.denseSKs[n-1]
	p.denseSKs = p.denseSKs[:n-1]
	return sk
}

func (p *sketchesPool) getForMerge(other *hyperloglog.Sketch) *hyperloglog.Sketch {
	if other.Sparse() {
		return p.getSparse()
	}

	return p.getDense()
}

func (p *sketchesPool) put(sk *hyperloglog.Sketch) {
	sk.Reset()
	if sk.Sparse() {
		p.sparseSKs = append(p.sparseSKs, sk)
		return
	}

	p.denseSKs = append(p.denseSKs, sk)
}
