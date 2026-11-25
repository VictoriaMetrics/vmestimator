package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func TestReadConfigMode(t *testing.T) {
	f := func(cfg string, wantErr bool) {
		t.Helper()

		path := filepath.Join(t.TempDir(), "streams.yaml")
		if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
			t.Fatalf("cannot write config: %s", err)
		}
		_, err := readConfig(path)
		if (err != nil) != wantErr {
			t.Fatalf("unexpected error: got %v; want error: %v", err, wantErr)
		}
	}

	// explicit modes
	f(`
streams:
- mode: gauge
- group_by: [job]
  mode: counter
`, false)

	// gauge and counter streams with the same labels, group_by, interval and filter produce different series
	f(`
streams:
- group_by: [job]
  interval: 24h
- group_by: [job]
  interval: 24h
  mode: counter
`, false)

	// explicit gauge mode equals the default one
	f(`
streams:
- group_by: [job]
- group_by: [job]
  mode: gauge
`, true)

	// same counter streams
	f(`
streams:
- mode: counter
- mode: counter
  min_cardinality: 10
`, true)

	// unknown mode
	f(`
streams:
- mode: histogram
`, true)

	// churn is not supported in counter mode
	f(`
streams:
- mode: counter
  calculate_churn: true
`, true)

	// mode is a reserved label for counter streams only
	f(`
streams:
- labels: {mode: "a"}
  mode: counter
`, true)
	f(`
streams:
- labels: {mode: "a"}
`, false)
}

func TestCounterModeWait(t *testing.T) {
	f := func(now string, interval, want time.Duration) {
		t.Helper()

		ts, err := time.Parse(time.RFC3339Nano, now)
		if err != nil {
			t.Fatalf("cannot parse %q: %s", now, err)
		}
		if got := counterResetWait(ts, interval); got != want {
			t.Fatalf("unexpected wait for now=%s, interval=%s; got %s; want %s", now, interval, got, want)
		}
	}

	// 24h counters reset at 00:00 UTC
	f("2026-11-25T13:30:00Z", 24*time.Hour, 10*time.Hour+30*time.Minute)
	f("2026-11-25T23:59:59.5Z", 24*time.Hour, 500*time.Millisecond)
	// at a boundary the next reset is a whole interval away
	f("2026-11-25T00:00:00Z", 24*time.Hour, 24*time.Hour)
	// timezone of now doesn't matter
	f("2026-11-25T05:30:00-08:00", 24*time.Hour, 10*time.Hour+30*time.Minute)
	// 1h counters reset at the top of the hour
	f("2026-11-25T13:45:00Z", time.Hour, 15*time.Minute)
	// 5m counters
	f("2026-11-25T13:47:00Z", 5*time.Minute, 3*time.Minute)
}

func TestCounterModeEstimate(t *testing.T) {
	insert := func(e *estimator, from, to int, name string) {
		t.Helper()

		var tss []protoparser.TimeSerie
		fpBuf := make([]byte, 8)
		for i := from; i < to; i++ {
			binary.LittleEndian.PutUint64(fpBuf, uint64(i))
			tss = append(tss, protoparser.TimeSerie{
				Fingerprint: hash(fpBuf),
				Labels:      []protoparser.Label{{Name: "__name__", Value: name}},
			})
		}
		e.insertMany(tss)
	}
	resetBuckets := func(e *estimator) {
		for _, eb := range e.buckets {
			eb.reset()
		}
	}
	write := func(e *estimator) string {
		buf := bytes.NewBuffer(nil)
		e.writeMetrics(buf)
		return buf.String()
	}

	// global stream
	e, err := newEstimator(EstimatorConfig{Interval: 24 * time.Hour, Buckets: 4, Mode: modeCounter})
	if err != nil {
		t.Fatalf("cannot create estimator: %s", err)
	}
	defer e.stop()

	insert(e, 0, 100, "foo")
	assertMetricsSame(t, "", `cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__global__"} 100`, write(e))

	// the estimate only grows between resets: already seen series are not counted again
	insert(e, 50, 150, "foo")
	assertMetricsSame(t, "", `cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__global__"} 150`, write(e))

	// at the boundary all buckets reset at once, with no previous generation kept
	resetBuckets(e)
	assertMetricsSame(t, "", `cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__global__"} 0`, write(e))
	insert(e, 100, 110, "foo")
	assertMetricsSame(t, "", `cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__global__"} 10`, write(e))

	// group stream
	ge, err := newEstimator(EstimatorConfig{Interval: 24 * time.Hour, Buckets: 4, GroupBy: []string{"__name__"}, GroupLimit: 100, MinCardinality: -1, Mode: modeCounter})
	if err != nil {
		t.Fatalf("cannot create estimator: %s", err)
	}
	defer ge.stop()

	insert(ge, 0, 20, "foo")
	insert(ge, 0, 30, "bar")
	assertMetricsSame(t, "", `
cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__name__",group_by_values="foo",by__name__="foo"} 20
cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__name__",group_by_values="bar",by__name__="bar"} 30
cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__group__",group_by_values="__name__"} 2
vmestimator_estimator_group_limit{interval="24h0m0s",filter="",mode="counter",group_by_keys="__group__",group_by_values="__name__"} 100
`, write(ge))

	// groups not seen after the boundary are gone, and their slots are released for the group limit
	resetBuckets(ge)
	insert(ge, 0, 5, "foo")
	assertMetricsSame(t, "", `
cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__name__",group_by_values="foo",by__name__="foo"} 5
cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__group__",group_by_values="__name__"} 1
vmestimator_estimator_group_limit{interval="24h0m0s",filter="",mode="counter",group_by_keys="__group__",group_by_values="__name__"} 100
`, write(ge))
	if size := ge.groupSize.totalSize(); size != 1 {
		t.Fatalf("unexpected group size after the boundary; got %d; want 1", size)
	}
}

func TestCounterModeEstimatorID(t *testing.T) {
	gauge := estimatorID(nil, []string{"__name__"}, 24*time.Hour, "", "")
	counter := estimatorID(nil, []string{"__name__"}, 24*time.Hour, "", modeCounter)
	if gauge == counter {
		t.Fatalf("gauge and counter streams must have different ids; got %q", gauge)
	}
	// gauge ids must not change, so snapshots from older vmestimator versions are merged with them
	if want := "\u0000labels=\u0000group_by=__name__\u0000interval=24h0m0s\u0000filter="; gauge != want {
		t.Fatalf("unexpected gauge id; got %q; want %q", gauge, want)
	}
}

func TestCounterModeSnapshotTransfer(t *testing.T) {
	e, err := newEstimator(EstimatorConfig{Interval: 24 * time.Hour, Buckets: 4, Mode: modeCounter})
	if err != nil {
		t.Fatalf("cannot create estimator: %s", err)
	}
	defer e.stop()

	var tss []protoparser.TimeSerie
	fpBuf := make([]byte, 8)
	for i := 0; i < 42; i++ {
		binary.LittleEndian.PutUint64(fpBuf, uint64(i))
		tss = append(tss, protoparser.TimeSerie{Fingerprint: hash(fpBuf)})
	}
	e.insertMany(tss)

	// a storage node sends gob-encoded snapshots, the select node decodes and merges them
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := e.toSnapshot(func(s *snapshot) error { return enc.Encode(s) }); err != nil {
		t.Fatalf("cannot encode snapshot: %s", err)
	}
	ss := newSnapshots()
	if err := decodeSnapshots(&data, ss.add); err != nil {
		t.Fatalf("cannot decode snapshots: %s", err)
	}
	buf := bytes.NewBuffer(nil)
	if err := ss.writeMetrics(buf); err != nil {
		t.Fatalf("cannot write metrics: %s", err)
	}
	assertMetricsSame(t, "", `cardinality_estimate{interval="24h0m0s",filter="",mode="counter",group_by_keys="__global__"} 42`, buf.String())

	// snapshots from older vmestimator versions have no Mode field and are decoded as gauges
	type oldSnapshot struct {
		ID       string
		Interval time.Duration
		Sketches map[uint64]SnapshotSketch
	}
	var oldData bytes.Buffer
	sk := e.buckets[0].newSketch()
	sk.InsertHash(1)
	if err := gob.NewEncoder(&oldData).Encode(oldSnapshot{Interval: time.Hour, Sketches: map[uint64]SnapshotSketch{0: {Sketch: sk}}}); err != nil {
		t.Fatalf("cannot encode old snapshot: %s", err)
	}
	ss = newSnapshots()
	if err := decodeSnapshots(&oldData, ss.add); err != nil {
		t.Fatalf("cannot decode old snapshot: %s", err)
	}
	buf.Reset()
	if err := ss.writeMetrics(buf); err != nil {
		t.Fatalf("cannot write metrics: %s", err)
	}
	if got := buf.String(); strings.Contains(got, "mode") {
		t.Fatalf("old snapshots must be gauges; got %q", got)
	}
}

func TestCounterModeRotation(t *testing.T) {
	// The reset goroutine resets the estimate at the next epoch-aligned boundary.
	e, err := newEstimator(EstimatorConfig{Interval: time.Second, Buckets: 4, Mode: modeCounter})
	if err != nil {
		t.Fatalf("cannot create estimator: %s", err)
	}
	defer e.stop()

	fpBuf := make([]byte, 8)
	var tss []protoparser.TimeSerie
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint64(fpBuf, uint64(i))
		tss = append(tss, protoparser.TimeSerie{Fingerprint: hash(fpBuf)})
	}
	e.insertMany(tss)

	time.Sleep(counterResetWait(time.Now(), time.Second) + 100*time.Millisecond)

	buf := bytes.NewBuffer(nil)
	e.writeMetrics(buf)
	assertMetricsSame(t, "", `cardinality_estimate{interval="1s",filter="",mode="counter",group_by_keys="__global__"} 0`, buf.String())
}
