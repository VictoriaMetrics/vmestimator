package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func TestDeduplicatorBloomFilter(t *testing.T) {
	bf := newDeduplicatorBloomFilter(10_000)

	// Not added yet.
	if bf.contains(1) {
		t.Fatal("contains(1): expected false before any add")
	}

	// First add: item is new.
	if !bf.add(1) {
		t.Fatal("add(1): expected true (new item)")
	}
	if !bf.contains(1) {
		t.Fatal("contains(1): expected true after add")
	}

	// Second add: already present.
	if bf.add(1) {
		t.Fatal("add(1): expected false (already present)")
	}

	// Unrelated hash is unaffected.
	if bf.contains(2) {
		t.Fatal("contains(2): expected false (never added)")
	}

	// Many items: first insertion is new, second is not.
	for i := uint64(2); i <= 1000; i++ {
		if !bf.add(i) {
			t.Fatalf("add(%d): expected true on first insert", i)
		}
	}
	for i := uint64(2); i <= 1000; i++ {
		if bf.add(i) {
			t.Fatalf("add(%d): expected false on second insert", i)
		}
	}
}

func TestDeduplicator_filter(t *testing.T) {
	d := newDeduplicator(10_000, time.Hour)
	defer d.Stop()

	makeTSS := func(fps ...uint64) []protoparser.TimeSerie {
		tss := make([]protoparser.TimeSerie, len(fps))
		for i, fp := range fps {
			tss[i] = protoparser.TimeSerie{Fingerprint: fp}
		}
		return tss
	}

	f := func(src, exp []protoparser.TimeSerie) {
		t.Helper()
		dst := d.filter(src, nil)
		if !reflect.DeepEqual(dst, exp) {
			t.Fatalf("filter: expected %v, got %v", exp, dst)
		}
	}

	// fp 1,2,3 go to buckets 1,2,3 respectively (fp%10).
	tss := makeTSS(1, 2, 3)

	// First pass: all unique, all pass through.
	f(tss, makeTSS(1, 2, 3))

	// Second pass: all already seen, all filtered.
	f(tss, nil)

	// Mixed: known + new. Only the new one (fp=4) passes.
	f(makeTSS(1, 4), makeTSS(4))

	// Clear bucket 1 (holds fp=1). fp=1 passes through again; fp=2 still filtered.
	d.bfs[1].Store(newDeduplicatorBloomFilter(d.bucketMaxSize))
	f(makeTSS(1, 2), makeTSS(1))

	// Clear all buckets: every series is new again.
	for i := range d.bfs {
		d.bfs[i].Store(newDeduplicatorBloomFilter(d.bucketMaxSize))
	}
	f(tss, makeTSS(1, 2, 3))
}

func TestDeduplicator_filterPassthrough(t *testing.T) {
	d := newDeduplicator(10_000, time.Hour)
	defer d.Stop()

	makeTSS := func(fps ...uint64) []protoparser.TimeSerie {
		tss := make([]protoparser.TimeSerie, len(fps))
		for i, fp := range fps {
			tss[i] = protoparser.TimeSerie{Fingerprint: fp}
		}
		return tss
	}

	f := func(src, exp []protoparser.TimeSerie) {
		t.Helper()
		dst := d.filter(src, nil)
		if !reflect.DeepEqual(dst, exp) {
			t.Fatalf("filter: expected %v, got %v", exp, dst)
		}
	}

	// Build a batch that spans both sides of the 500 threshold.
	// fp%1000: 1,101,201,301,401 < 500 (pass); 501,601,701,801,901 >= 500 (no pass).
	tss := makeTSS(1, 101, 201, 301, 401, 501, 601, 701, 801, 901)

	// Seed BF so all series are "already seen".
	d.filter(tss, nil)
	f(tss, nil)

	// Full passthrough: all series pass regardless of BF state.
	d.passthroughPer1000.Store(1000)
	f(tss, tss)

	// 50% passthrough: only series with fp%1000 < 500 pass through.
	d.passthroughPer1000.Store(500)
	exp := makeTSS(1, 101, 201, 301, 401)
	f(tss, exp)

	// Passthrough decision is deterministic: repeated calls yield the same result.
	f(tss, exp)
}
