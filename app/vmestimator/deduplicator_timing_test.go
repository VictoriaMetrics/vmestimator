package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func BenchmarkDeduplicator_filter(b *testing.B) {
	d := newDeduplicator(100_000, time.Minute)
	defer d.Stop()

	const seriesCount = 10_000
	tss := make([]protoparser.TimeSerie, seriesCount)
	for i := range tss {
		tss[i].Fingerprint = hash([]byte("foo"+strconv.Itoa(i)))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		dst := d.filter(tss, tss[:0])
		_ = dst
	}
}
