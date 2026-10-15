package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
	"github.com/cespare/xxhash/v2"
)

func TestReadConfigHash(t *testing.T) {
	f := func(data string) uint64 {
		t.Helper()
		path := filepath.Join(t.TempDir(), "streams.yaml")
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := readConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg.hash
	}

	data := "streams:\n  - interval: 15m\n"
	if got, want := f(data), xxhash.Sum64String(data); got != want {
		t.Fatalf("config hash = %x; want %x", got, want)
	}
	if f(data) == f("streams:\n  - interval: 30m\n") {
		t.Fatal("different config contents have identical hashes")
	}
}

func TestInvalidateCardinalityCache(t *testing.T) {
	keep := &estimator{id: "keep"}
	remove := &estimator{id: "remove"}

	globalChurnSnapshots = &churnSnapshots{
		entries: map[churnKey]*churnEntry{
			{snapshotID: keep.id}:   {},
			{snapshotID: remove.id}: {},
		},
	}
	cardinalityMetricsCacheAt = time.Now()

	invalidateCardinalityCache([]*estimator{keep})

	if len(globalChurnSnapshots.entries) != 1 {
		t.Fatalf("churn entries = %d; want 1", len(globalChurnSnapshots.entries))
	}
	if _, ok := globalChurnSnapshots.entries[churnKey{snapshotID: keep.id}]; !ok {
		t.Fatal("reused estimator churn state was removed")
	}
	if !cardinalityMetricsCacheAt.IsZero() {
		t.Fatal("cardinality cache was not invalidated")
	}
}

// initEstimators writes data to a temporary config file and loads it into the active estimators.
// It returns the config path. Active estimators are reset on test cleanup.
func initEstimators(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "streams.yaml")
	resetEstimators()
	t.Cleanup(resetEstimators)
	if _, err := applyConfig(path, data, nil); err != nil {
		t.Fatal(err)
	}
	return path
}

// applyConfig writes data to path and loads it into the active estimators.
func applyConfig(path, data string, dedup *deduplicator) (bool, error) {
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		return false, err
	}
	cfg, err := readConfig(path)
	if err != nil {
		return false, err
	}
	return loadConfig(cfg, dedup)
}

func resetEstimators() {
	for _, e := range getEstimators() {
		e.stop()
	}
	activeEstimators.Store([]*estimator(nil))
}

func TestLoadConfig(t *testing.T) {
	data := "streams:\n  - interval: 15m\n"
	path := initEstimators(t, data)
	es := getEstimators()

	// unchanged config keeps estimators
	changed, err := applyConfig(path, data, nil)
	if err != nil || changed {
		t.Fatalf("unexpected result: changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 1 || got[0] != es[0] {
		t.Fatalf("estimators must not be replaced")
	}

	// invalid config keeps previous estimators
	changed, err = applyConfig(path, "streams:\n  - interval: 15m\n    hll_precision: 99\n", nil)
	if err == nil || changed {
		t.Fatalf("expecting error; got changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 1 || got[0] != es[0] {
		t.Fatalf("estimators must not be replaced on error")
	}

	// valid change swaps estimators
	changed, err = applyConfig(path, "streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n", nil)
	if err != nil || !changed {
		t.Fatalf("expecting reload; got changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 2 {
		t.Fatalf("unexpected estimators count: %d", len(got))
	}
	if got := getEstimators(); got[0] != es[0] {
		t.Fatal("unchanged estimator must be reused")
	}
}

func TestLoadConfigIgnoresEstimatorOrder(t *testing.T) {
	path := initEstimators(t, "streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n")
	es := getEstimators()

	changed, err := applyConfig(path, "streams:\n  - interval: 15m\n    group_by: [job]\n  - interval: 15m\n", nil)
	if err != nil || changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); got[0] != es[0] || got[1] != es[1] {
		t.Fatal("estimators were replaced or reordered")
	}
}

func TestLoadConfigKeepsEstimatorState(t *testing.T) {
	path := initEstimators(t, "streams:\n  - interval: 15m\n")
	es := getEstimators()

	es[0].insertMany([]protoparser.TimeSerie{{Fingerprint: 1}})
	if changed, err := applyConfig(path, "streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n", nil); err != nil || !changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}

	var estimate uint64
	if err := getEstimators()[0].toSnapshot(func(s *snapshot) error {
		estimate = s.Sketches[0].Sketch.Estimate()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if estimate == 0 {
		t.Fatal("reused estimator state was reset")
	}
}

func TestLoadConfigDropsRemovedEstimators(t *testing.T) {
	path := initEstimators(t, "streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n")
	es := getEstimators()

	if changed, err := applyConfig(path, "streams:\n  - interval: 15m\n", nil); err != nil || !changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 1 || got[0] != es[0] {
		t.Fatalf("remaining estimators = %v; want first estimator only", got)
	}
	select {
	case <-es[1].stopCh:
	default:
		t.Fatal("removed estimator was not stopped")
	}
}

func TestLoadConfigResetsDeduplicator(t *testing.T) {
	path := initEstimators(t, "streams:\n  - interval: 15m\n")

	dedup := newDeduplicator(10_000, time.Hour)
	defer dedup.Stop()
	tss := []protoparser.TimeSerie{{Fingerprint: 1}}
	dedup.filter(tss, nil)
	if got := dedup.filter(tss, nil); len(got) != 0 {
		t.Fatalf("deduplicator did not filter duplicate: %v", got)
	}

	changed, err := applyConfig(path, "streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n", dedup)
	if err != nil || !changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}
	if got := dedup.filter(tss, nil); len(got) != 1 {
		t.Fatalf("deduplicator retained previous config: %v", got)
	}
}
