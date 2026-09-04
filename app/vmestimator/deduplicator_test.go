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

func TestDeduplicator_filterSaturated(t *testing.T) {
	// Use minimum valid maxSize so bucketMaxSize=1_000, keeping the test fast.
	d := newDeduplicator(10_000, time.Hour)
	defer d.Stop()

	// Saturate bucket 1 (fp%10 == 1) by inserting bucketMaxSize unique series.
	for i := uint64(0); i < uint64(d.bucketMaxSize); i++ {
		fp := i*10 + 1
		d.filter([]protoparser.TimeSerie{{Fingerprint: fp}}, nil)
	}

	bf1 := d.bfs[1].Load()
	if got := bf1.size.Load(); got != int64(d.bucketMaxSize) {
		t.Fatalf("expected bucket 1 saturated at size=%d, got %d", d.bucketMaxSize, got)
	}

	// Pick a new fp that maps to bucket 1 and is outside the seeded range.
	newFP := uint64(d.bucketMaxSize)*10 + 1
	if bf1.contains(newFP) {
		t.Fatalf("newFP=%d is a false positive in the bloom filter; choose a different value", newFP)
	}

	newTSS := []protoparser.TimeSerie{{Fingerprint: newFP}}

	// Saturated bucket: unseen series passes through but is NOT added to the BF.
	got := d.filter(newTSS, nil)
	if !reflect.DeepEqual(got, newTSS) {
		t.Fatalf("saturated: expected pass-through, got %v", got)
	}
	if bf1.size.Load() != int64(d.bucketMaxSize) {
		t.Fatal("saturated: series was incorrectly added to the bloom filter")
	}

	// Second call: same series passes through again because it was never recorded.
	got = d.filter(newTSS, nil)
	if !reflect.DeepEqual(got, newTSS) {
		t.Fatalf("saturated: expected pass-through on second call, got %v", got)
	}
}
