package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/axiomhq/hyperloglog"
)

type churnKey struct {
	snapshotKey string
	groupKey    uint64
}

type churnEntry struct {
	// metadata for writing metrics
	interval      time.Duration
	churnInterval time.Duration
	filter        string
	labels        map[string]string
	groupBy       []string
	values        []string // group label values for this groupKey

	prev *churnSketch
	curr *churnSketch
}

type churnSketch struct {
	addedAt time.Time
	sketch  *hyperloglog.Sketch
}

// churnSnapshots holds prev and curr sketch baselines per (snapshotKey, groupKey).
// When prev.addedAt < now-churnInterval, prev is reset to curr.
type churnSnapshots struct {
	entries map[churnKey]*churnEntry
}

func newChurnSnapshots() *churnSnapshots {
	return &churnSnapshots{entries: make(map[churnKey]*churnEntry)}
}

// update accepts a single snapshot and upserts one churnEntry per group sketch.
// Panics if s.ChurnInterval is zero.
func (cs *churnSnapshots) update(s *snapshot, now time.Time) {
	if s.ChurnInterval == 0 {
		logger.Panicf("BUG: update called with snapshot with zero ChurnInterval")
	}

	snapKey := s.key()
	for groupKey, ssk := range s.Sketches {
		k := churnKey{snapshotKey: snapKey, groupKey: groupKey}

		e, ok := cs.entries[k]
		if !ok {
			e = &churnEntry{
				interval:      s.Interval,
				churnInterval: s.ChurnInterval,
				filter:        s.Filter,
				groupBy:       append([]string{}, s.GroupBy...),
				values:        append([]string{}, ssk.Values...),
			}
			if len(s.Labels) > 0 {
				e.labels = make(map[string]string, len(s.Labels))
				for lk, lv := range s.Labels {
					e.labels[lk] = lv
				}
			}
			cs.entries[k] = e
		}

		newSK := &churnSketch{
			addedAt: now,
			sketch:  ssk.Sketch.Clone(),
		}

		if e.curr == nil {
			// First update: create an empty prev so rotation logic works normally,
			// but churnRate will report 0 until prev has real data.
			empty := ssk.Sketch.Clone()
			empty.Reset()
			e.prev = &churnSketch{addedAt: now, sketch: empty}
			e.curr = newSK
		} else if e.prev.addedAt.Before(now.Add(-s.ChurnInterval)) {
			e.prev = &churnSketch{addedAt: now, sketch: e.curr.sketch}
			e.curr = newSK
		} else {
			e.curr = newSK
		}
	}
}

// cleanup removes entries whose curr sketch has not been updated within their 2*interval.
func (cs *churnSnapshots) cleanup(now time.Time) {
	for k, e := range cs.entries {
		if e.curr.addedAt.Before(now.Add(-e.interval * 2)) {
			delete(cs.entries, k)
		}
	}
}

// writeMetrics writes cardinality_churn_rate metrics for all entries.
// Churn rate is in [0, 1]. Reports 0 when prev has no data yet.
func (cs *churnSnapshots) writeMetrics(w io.Writer) error {
	buf := make([]byte, 0, 256)
	for _, e := range cs.entries {
		metricPrefixB := appendChurnMetricPrefix(make([]byte, 0, 128), e.labels, e.interval, e.churnInterval, e.filter)
		metricPrefix := bytesutil.ToUnsafeString(metricPrefixB)

		rate := churnRate(e)
		buf = buf[:0]
		if len(e.groupBy) == 0 {
			buf = append(buf, metricPrefix...)
			buf = append(buf, `,group_by_keys="__global__"} `...)
		} else {
			groupByKeysLabelB := appendGroupByKeysLabel(make([]byte, 0, 128), `group_by_keys`, e.groupBy)
			groupByKeysLabel := bytesutil.ToUnsafeString(groupByKeysLabelB)
			buf = appendCardinalityEstimateGroupMetrics(buf, metricPrefix, groupByKeysLabel, e.groupBy, e.values)
		}
		buf = strconv.AppendFloat(buf, rate, 'f', 4, 64)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}
	return nil
}

// churnRate computes the churn rate for a churnEntry.
// Returns 0 when prev has no data yet.
func churnRate(e *churnEntry) float64 {
	psk := e.prev.sketch
	csk := e.curr.sketch
	if psk.Estimate() == 0 {
		return 0
	}
	union := csk.Clone()
	if err := union.Merge(psk); err != nil {
		return 0
	}
	unionEst := union.Estimate()
	if unionEst == 0 {
		return 0
	}
	churn := float64(2*unionEst) - float64(psk.Estimate()) - float64(csk.Estimate())
	rate := churn / float64(unionEst)
	if rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
}

// appendChurnMetricPrefix produces:
// 'cardinality_churn_ratio{interval="5m",churn_interval="15m",filter=""'
func appendChurnMetricPrefix(buf []byte, labels map[string]string, interval, churnInterval time.Duration, filter string) []byte {
	buf = append(buf, `cardinality_churn_ratio{interval=`...)
	buf = strconv.AppendQuote(buf, interval.String())
	buf = append(buf, `,churn_interval=`...)
	buf = strconv.AppendQuote(buf, churnInterval.String())
	buf = append(buf, `,filter=`...)
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
