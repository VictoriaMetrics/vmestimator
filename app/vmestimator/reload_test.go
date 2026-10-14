package main

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func TestConfigHash(t *testing.T) {
	data := []byte("streams:\n  - interval: 15m\n")
	if got, want := configHash(data), sha256.Sum256(data); got != want {
		t.Fatalf("configHash() = %x; want %x", got, want)
	}
	if configHash(data) == configHash([]byte("streams:\n  - interval: 30m\n")) {
		t.Fatal("different config contents have identical hashes")
	}
}

func TestUpdateConfigReloadStatus(t *testing.T) {
	oldSuccess := configSuccess.Load()
	oldTimestamp := configTimestamp.Load()
	t.Cleanup(func() {
		configSuccess.Store(oldSuccess)
		configTimestamp.Store(oldTimestamp)
	})

	configSuccess.Store(0)
	configTimestamp.Store(0)
	updateConfigReloadStatus(false)
	if got := configSuccess.Load(); got != 0 {
		t.Fatalf("success after unchanged config = %d; want 0", got)
	}
	if got := configTimestamp.Load(); got != 0 {
		t.Fatalf("timestamp after unchanged config = %d; want 0", got)
	}

	updateConfigReloadStatus(true)
	if got := configSuccess.Load(); got != 1 {
		t.Fatalf("success after changed config = %d; want 1", got)
	}
	if got := configTimestamp.Load(); got == 0 {
		t.Fatal("timestamp after changed config = 0; want current timestamp")
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

func TestReloadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("streams:\n  - interval: 15m\n")
	data, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := configHash(data)
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	defer func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	}()

	// unchanged file keeps estimators
	hash, changed, err := reloadConfig(path, hash, nil)
	if err != nil || changed {
		t.Fatalf("unexpected result: changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 1 || got[0] != es[0] {
		t.Fatalf("estimators must not be replaced")
	}

	// invalid file keeps previous config
	write("streams:\n  - interval: 15m\n    hll_precision: 99\n")
	hash, changed, err = reloadConfig(path, hash, nil)
	if err == nil || changed {
		t.Fatalf("expecting error; got changed=%v err=%v", changed, err)
	}
	if got := getEstimators(); len(got) != 1 || got[0] != es[0] {
		t.Fatalf("estimators must not be replaced on error")
	}

	// valid change swaps estimators
	write("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n")
	_, changed, err = reloadConfig(path, hash, nil)
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

func TestReloadConfigIgnoresEstimatorOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	data := []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	newData := []byte("streams:\n  - interval: 15m\n    group_by: [job]\n  - interval: 15m\n")
	if err := os.WriteFile(path, newData, 0o600); err != nil {
		t.Fatal(err)
	}
	hash, changed, err := reloadConfig(path, configHash(data), nil)
	if err != nil || changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}
	if hash != configHash(newData) {
		t.Fatal("config hash was not updated")
	}
	if got := getEstimators(); got[0] != es[0] || got[1] != es[1] {
		t.Fatal("estimators were replaced or reordered")
	}
}

func TestReloadConfigKeepsEstimatorState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	data := []byte("streams:\n  - interval: 15m\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	es[0].insertMany([]protoparser.TimeSerie{{Fingerprint: 1}})
	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := reloadConfig(path, configHash(data), nil); err != nil || !changed {
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

func TestReloadConfigDropsRemovedEstimators(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	data := []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := reloadConfig(path, configHash(data), nil); err != nil || !changed {
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

func TestReloadConfigResetsDeduplicator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	dedup := newDeduplicator(10_000, time.Hour)
	defer dedup.Stop()
	tss := []protoparser.TimeSerie{{Fingerprint: 1}}
	dedup.filter(tss, nil)
	if got := dedup.filter(tss, nil); len(got) != 0 {
		t.Fatalf("deduplicator did not filter duplicate: %v", got)
	}

	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, changed, err := reloadConfig(path, configHash(data), dedup)
	if err != nil || !changed {
		t.Fatalf("reload result: changed=%v err=%v", changed, err)
	}
	if got := dedup.filter(tss, nil); len(got) != 1 {
		t.Fatalf("deduplicator retained previous config: %v", got)
	}
}

func TestReloadConfigReleasesLockBeforeCacheReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cardinalityCacheMu.Lock()
	locked := true
	defer func() {
		if locked {
			cardinalityCacheMu.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, _, err := reloadConfig(path, configHash(data), nil)
		done <- err
	}()

	deadline := time.After(time.Second)
	for len(getEstimators()) != 2 {
		select {
		case <-deadline:
			t.Fatal("reload did not swap estimators")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	writeDone := make(chan struct{})
	go withEstimators(func([]*estimator) {
		close(writeDone)
	})
	select {
	case <-writeDone:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("reload held estimator lock during cache reset")
	}

	cardinalityCacheMu.Unlock()
	locked = false
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Check that reload blocks until active estimators are released.
func TestWithEstimatorsBlocksReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.yaml")
	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	es, err := newEstimators(path, data)
	if err != nil {
		t.Fatal(err)
	}
	activeEstimators.Store(&es)
	t.Cleanup(func() {
		for _, e := range getEstimators() {
			e.stop()
		}
	})

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	go withEstimators(func([]*estimator) {
		close(entered)
		<-release
	})
	<-entered

	if err := os.WriteFile(path, []byte("streams:\n  - interval: 15m\n  - interval: 15m\n    group_by: [job]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := reloadConfig(path, configHash(data), nil)
		done <- err
	}()

	deadline := time.After(time.Second)
	for activeEstimatorsMu.TryRLock() {
		activeEstimatorsMu.RUnlock()
		select {
		case <-deadline:
			t.Fatal("reload did not wait for active ingestion")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	select {
	case err := <-done:
		t.Fatalf("reload completed during ingestion: %v", err)
	default:
	}
	close(release)
	released = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
