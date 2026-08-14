package main

import (
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/axiomhq/hyperloglog"
)

// derivedInfo holds the state for an estimator that derives its cardinality
// estimates from a more granular "base" estimator instead of maintaining its
// own HLL sketches.
//
// At insert time, the base estimator calls insertRejected for fingerprints
// that were dropped due to group_limit. The derived estimator stores these
// in per-bucket reject HLLs keyed by the parent group values (the subset of
// the base group values that corresponds to the derived group_by keys).
//
// At snapshot time, the derived estimator sums the base's per-group
// estimates by parent group, adds the reject HLL estimates, and emits the
// result as cardinality_estimate metrics.
type derivedInfo struct {
	base *estimator

	// parentToBase maps derived group_by key indices to base group_by key
	// indices. For example, if the base groups by [__name__, region, task_name]
	// and the derived groups by [__name__, region], then parentToBase = [0, 1].
	parentToBase []int

	precision uint8

	rejectMu       sync.Mutex
	rejectSketches []map[uint64]*hyperloglog.Sketch
	rejectValues   []map[uint64][]string
}

func newDerivedInfo(base, derived *estimator, baseCfg EstimatorConfig) (*derivedInfo, error) {
	parentToBase, err := buildParentToBase(derived.groupBy, base.groupBy)
	if err != nil {
		return nil, err
	}

	nBuckets := len(base.buckets)
	return &derivedInfo{
		base:           base,
		parentToBase:   parentToBase,
		precision:      baseCfg.HLLPrecision,
		rejectSketches: make([]map[uint64]*hyperloglog.Sketch, nBuckets),
		rejectValues:   make([]map[uint64][]string, nBuckets),
	}, nil
}

// buildParentToBase validates that every key in parentGroupBy exists in
// baseGroupBy and returns the index mapping.
func buildParentToBase(parentGroupBy, baseGroupBy []string) ([]int, error) {
	for _, k := range parentGroupBy {
		if k == labelKeyword {
			return nil, fmt.Errorf("__label__ is not supported in derived estimators")
		}
	}
	for _, k := range baseGroupBy {
		if k == labelKeyword {
			return nil, fmt.Errorf("__label__ is not supported in base estimators with derived streams")
		}
	}

	parentToBase := make([]int, len(parentGroupBy))
	for i, parentKey := range parentGroupBy {
		found := false
		for j, baseKey := range baseGroupBy {
			if parentKey == baseKey {
				parentToBase[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("group_by key %q in derived stream not found in base stream", parentKey)
		}
	}
	return parentToBase, nil
}

// insertRejected is called by the base estimator when a child group is
// rejected due to group_limit. The fingerprint is inserted into a per-bucket
// reject HLL keyed by the parent group values, so it is still counted in the
// derived estimate.
func (di *derivedInfo) insertRejected(bucketIdx int, baseGroupValues []string, fp uint64) {
	parentValues := di.extractParentValues(baseGroupValues)
	parentKey := hashGroupValues(parentValues)

	di.rejectMu.Lock()
	defer di.rejectMu.Unlock()

	sketches := di.rejectSketches[bucketIdx]
	if sketches == nil {
		sketches = make(map[uint64]*hyperloglog.Sketch)
		di.rejectSketches[bucketIdx] = sketches
	}
	sk := sketches[parentKey]
	if sk == nil {
		sk = mustNewSketch(di.precision, true)
		sketches[parentKey] = sk
	}
	sk.InsertHash(fp)

	values := di.rejectValues[bucketIdx]
	if values == nil {
		values = make(map[uint64][]string)
		di.rejectValues[bucketIdx] = values
	}
	if _, ok := values[parentKey]; !ok {
		values[parentKey] = append([]string{}, parentValues...)
	}
}

// extractParentValues picks the subset of baseGroupValues that corresponds
// to the derived group_by keys, using the parentToBase index mapping.
func (di *derivedInfo) extractParentValues(baseGroupValues []string) []string {
	if len(di.parentToBase) == 0 {
		return nil
	}
	parentValues := make([]string, len(di.parentToBase))
	for i, baseIdx := range di.parentToBase {
		parentValues[i] = baseGroupValues[baseIdx]
	}
	return parentValues
}

// hashGroupValues computes a uint64 hash of group values using the same
// digest format as insertMany: "\u0000" + val1 + "\u0000" + val2 + ...
func hashGroupValues(values []string) uint64 {
	d := getDigest()
	defer putDigest(d)
	for _, v := range values {
		_, _ = d.WriteString("\u0000")
		_, _ = d.WriteString(v)
	}
	return d.Sum64()
}

// parentSum accumulates the summed estimate for a single parent group.
type parentSum struct {
	values []string
	sum    uint64
}

// writeDerivedMetrics derives cardinality estimates from the base estimator
// and writes them as cardinality_estimate metrics.
func (e *estimator) writeDerivedMetrics(w io.Writer) {
	di := e.derived
	base := di.base

	// Accumulate summed estimates by parent group key.
	sums := make(map[uint64]*parentSum)

	// 1. Sum base estimator's per-group estimates by parent group.
	err := base.toSnapshot(func(s *snapshot) error {
		for _, ssk := range s.Sketches {
			parentValues := di.extractParentValues(ssk.Values)
			parentKey := hashGroupValues(parentValues)

			ps, ok := sums[parentKey]
			if !ok {
				ps = &parentSum{values: append([]string{}, parentValues...)}
				sums[parentKey] = ps
			}
			ps.sum += ssk.Sketch.Estimate()
		}
		return nil
	})
	if err != nil {
		logger.Errorf("derived estimator: base toSnapshot failed: %s", err)
	}

	// 2. Add reject sketch estimates for rejected child groups.
	di.rejectMu.Lock()
	for bucketIdx, sketches := range di.rejectSketches {
		values := di.rejectValues[bucketIdx]
		for parentKey, sk := range sketches {
			estimate := sk.Estimate()
			if estimate == 0 {
				continue
			}
			ps, ok := sums[parentKey]
			if !ok {
				ps = &parentSum{values: append([]string{}, values[parentKey]...)}
				sums[parentKey] = ps
			}
			ps.sum += estimate
		}
	}

	// 3. Reset reject sketches — estimates have been consumed.
	for i := range di.rejectSketches {
		di.rejectSketches[i] = nil
		di.rejectValues[i] = nil
	}
	di.rejectMu.Unlock()

	// 4. Emit metrics.
	e.emitDerivedCardinality(w, sums)
}

// emitDerivedCardinality writes cardinality_estimate metrics from the
// accumulated parent group sums, respecting the minCardinality filter.
func (e *estimator) emitDerivedCardinality(w io.Writer, sums map[uint64]*parentSum) {
	tmpBufP := getFormatBuf()
	tmpBuf := *tmpBufP
	defer func() {
		*tmpBufP = tmpBuf
		putFormatBuf(tmpBufP)
	}()

	eb0 := e.buckets[0]
	metricPrefixB := appendCardinalityEstimateMetricPrefix(make([]byte, 0, 128), eb0.labels, eb0.interval, eb0.filter)
	metricPrefix := bytesutil.ToUnsafeString(metricPrefixB)

	if len(e.groupBy) == 0 {
		// Global estimate: sum all parent groups into one value.
		var total uint64
		for _, ps := range sums {
			total += ps.sum
		}
		if total < uint64(*cardinalityMetricsMinCardinality) {
			return
		}
		tmpBuf = tmpBuf[:0]
		tmpBuf = appendCardinalityEstimateGlobalMetric(tmpBuf, metricPrefix)
		tmpBuf = strconv.AppendUint(tmpBuf, total, 10)
		tmpBuf = append(tmpBuf, "\n"...)
		_, _ = w.Write(tmpBuf)
		return
	}

	// Grouped estimates.
	groupByKeysLabelB := appendGroupByKeysLabel(make([]byte, 0, 128), `group_by_keys`, e.groupBy)
	groupByKeysLabel := bytesutil.ToUnsafeString(groupByKeysLabelB)

	for _, ps := range sums {
		if ps.sum < uint64(*cardinalityMetricsMinCardinality) {
			continue
		}
		tmpBuf = tmpBuf[:0]
		tmpBuf = appendCardinalityEstimateGroupMetrics(tmpBuf, metricPrefix, groupByKeysLabel, e.groupBy, ps.values)
		tmpBuf = strconv.AppendUint(tmpBuf, ps.sum, 10)
		tmpBuf = append(tmpBuf, "\n"...)
		if _, err := w.Write(tmpBuf); err != nil {
			logger.Errorf("derived estimator: write failed: %s", err)
			return
		}
	}
}
