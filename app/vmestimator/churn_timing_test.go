package main

import (
	"bytes"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type churnTestGroup struct {
	key    uint64
	values []string
	series []string
}

func newGlobalChurnSnapshot(churnInterval time.Duration, series ...string) *snapshot {
	sk := mustNewSketch(14, true)
	for _, s := range series {
		sk.Insert([]byte(s))
	}
	return &snapshot{
		Interval:      churnInterval,
		ChurnInterval: churnInterval,
		Sketches:      map[uint64]SnapshotSketch{0: {Sketch: sk}},
	}
}

func newGroupedChurnSnapshot(churnInterval time.Duration, groupBy []string, groups ...churnTestGroup) *snapshot {
	s := &snapshot{
		Interval:      churnInterval,
		ChurnInterval: churnInterval,
		GroupBy:       groupBy,
		Sketches:      make(map[uint64]SnapshotSketch, len(groups)),
	}
	for _, g := range groups {
		sk := mustNewSketch(14, true)
		for _, series := range g.series {
			sk.Insert([]byte(series))
		}
		s.Sketches[g.key] = SnapshotSketch{Sketch: sk, Values: g.values}
	}
	return s
}

func writeChurnMetrics(t *testing.T, cs *churnSnapshots) string {
	t.Helper()
	var buf bytes.Buffer
	if err := cs.writeMetrics(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func assertChurnMetrics(t *testing.T, act, exp string) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(act, "\n"), "\n")
	sort.Strings(lines)
	got := strings.Join(lines, "\n")
	if got != exp {
		t.Fatalf("unexpected metrics:\ngot:  %s\nwant: %s", got, exp)
	}
}

// --- group tests ---

func TestChurnSnapshots_Group_Empty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		assertChurnMetrics(t, writeChurnMetrics(t, cs), "")
	})
}

func TestChurnSnapshots_Group_OneInserted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		cs.update(newGroupedChurnSnapshot(
			time.Minute,
			[]string{"job"},
			churnTestGroup{
				key:    1,
				values: []string{"api"},
				series: []string{"s1", "s2", "s3"},
			},
		), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 0.0000`,
		)
	})
}

func TestChurnSnapshots_Group_TwoInserted_ZeroChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		snap := func(series ...string) *snapshot {
			return newGroupedChurnSnapshot(
				interval,
				[]string{"job"},
				churnTestGroup{key: 1, values: []string{"api"}, series: series},
			)
		}

		cs.update(snap("s1", "s2", "s3"), time.Now())
		time.Sleep(interval + time.Second)

		cs.update(snap("s1", "s2", "s3"), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 0.0000`,
		)
	})
}

func TestChurnSnapshots_Group_TwoInserted_FiftyPercentChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		snap := func(series ...string) *snapshot {
			return newGroupedChurnSnapshot(
				interval,
				[]string{"job"},
				churnTestGroup{key: 1, values: []string{"api"}, series: series},
			)
		}

		cs.update(snap("s1", "s2", "s3"), time.Now())
		time.Sleep(interval + time.Second)

		cs.update(snap("s2", "s3", "s4"), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 0.5000`,
		)
	})
}

func TestChurnSnapshots_Group_TwoInserted_HundredPercentChurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		snap := func(series ...string) *snapshot {
			return newGroupedChurnSnapshot(
				interval,
				[]string{"job"},
				churnTestGroup{key: 1, values: []string{"api"}, series: series},
			)
		}

		cs.update(snap("s1", "s2", "s3"), time.Now())
		time.Sleep(interval + time.Second)

		cs.update(snap("s4", "s5", "s6"), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 1.0000`,
		)
	})
}

func TestChurnSnapshots_Group_ThreeInserted_PrevReplaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		snap := func(series ...string) *snapshot {
			return newGroupedChurnSnapshot(
				interval,
				[]string{"job"},
				churnTestGroup{key: 1, values: []string{"api"}, series: series},
			)
		}

		// First insert: A={s1,s2,s3}. curr=A, prev=empty.
		cs.update(snap("s1", "s2", "s3"), time.Now())
		time.Sleep(interval + time.Second)

		// Second insert: B={s4,s5,s6}. prev rotates to A, curr=B. Churn=1 (disjoint).
		cs.update(snap("s4", "s5", "s6"), time.Now())
		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 1.0000`,
		)
		time.Sleep(interval + time.Second)

		// Third insert: C=B={s4,s5,s6}. prev rotates to B, curr=C. Churn=0 (identical).
		// If prev had NOT rotated (still A), Churn = 1.
		cs.update(snap("s4", "s5", "s6"), time.Now())
		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 0.0000`,
		)
	})
}

// --- several groups ---

func TestChurnSnapshots_SeveralGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		snap := func(apiSeries, workerSeries []string) *snapshot {
			return newGroupedChurnSnapshot(
				interval,
				[]string{"job"},
				churnTestGroup{key: 1, values: []string{"api"}, series: apiSeries},
				churnTestGroup{key: 2, values: []string{"worker"}, series: workerSeries},
			)
		}

		// A: api={s1,s2,s3}, worker={s1,s2,s3}
		cs.update(snap([]string{"s1", "s2", "s3"}, []string{"s1", "s2", "s3"}), time.Now())
		time.Sleep(interval + time.Second)

		// B: api={s1,s2,s3} (no churn), worker={s4,s5,s6} (100% churn)
		cs.update(snap([]string{"s1", "s2", "s3"}, []string{"s4", "s5", "s6"}), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="api",by_job="api"} 0.0000
cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="job",group_by_values="worker",by_job="worker"} 1.0000`,
		)
	})
}

// --- cleanup ---

func TestChurnSnapshots_Cleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute
		expMetric := `cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="__global__"} 0.0000`

		cs.update(newGlobalChurnSnapshot(interval, "s1", "s2"), time.Now())

		// Metric present at start.
		cs.cleanup(time.Now())
		assertChurnMetrics(t, writeChurnMetrics(t, cs), expMetric)

		// Metric present just before 2*interval.
		time.Sleep(2*interval - time.Millisecond)
		cs.cleanup(time.Now())
		assertChurnMetrics(t, writeChurnMetrics(t, cs), expMetric)

		// Metric disappears just after 2*interval.
		time.Sleep(2 * time.Millisecond)
		cs.cleanup(time.Now())
		assertChurnMetrics(t, writeChurnMetrics(t, cs), "")
	})
}

// --- global ---

func TestChurnSnapshots_Global(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newChurnSnapshots()
		interval := time.Minute

		cs.update(newGlobalChurnSnapshot(interval, "s1", "s2", "s3"), time.Now())
		time.Sleep(interval + time.Second)

		cs.update(newGlobalChurnSnapshot(interval, "s2", "s3", "s4"), time.Now())

		assertChurnMetrics(
			t,
			writeChurnMetrics(t, cs),
			`cardinality_churn_ratio{interval="1m0s",churn_interval="1m0s",filter="",group_by_keys="__global__"} 0.5000`,
		)
	})
}
