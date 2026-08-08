package main

import (
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func pregenerateLabels(n int) []string {
	if n == 0 {
		return nil
	}
	labels := make([]string, n)
	for i := range labels {
		labels[i] = fmt.Sprintf("%d", i)
	}
	return labels
}

// generateSeries returns numSeries pre-built TimeSeries.
// When groupsNum > 0 each series gets a "groupLabel" cycling through groupsNum values.
func generateSeries(numSeries, groupsNum int) []protoparser.TimeSerie {
	groupLabels := pregenerateLabels(groupsNum)
	series := make([]protoparser.TimeSerie, numSeries)
	var buf []byte
	for i := range series {
		var labels []protoparser.Label
		if groupsNum > 0 {
			labels = []protoparser.Label{{Name: "groupLabel", Value: groupLabels[i%groupsNum]}}
		}
		buf = strconv.AppendInt(append(buf[:0], "foobarbaz"...), int64(i), 10)
		series[i] = protoparser.TimeSerie{
			Labels:      labels,
			Fingerprint: hash(buf),
		}
	}
	return series
}

func BenchmarkEstimator_WriteMetrics(b *testing.B) {
	b.Run("NoGroup/NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		e.insertMany(generateSeries(5_000, 0))

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("NoGroup/WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		series := generateSeries(5_000, 0)
		e.insertMany(series)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		e.insertMany(series)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("Group100/NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		e.insertMany(generateSeries(5_000, 100))

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("Group100/WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		series := generateSeries(5_000, 100)
		e.insertMany(series)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		e.insertMany(series)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("Group10k/NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		e.insertMany(generateSeries(50_000, 10_000))

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("Group10k/WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		series := generateSeries(50_000, 10_000)
		e.insertMany(series)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		e.insertMany(series)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})
}

func BenchmarkEstimator_InsertManyParallel(b *testing.B) {
	b.Run("NoGroup", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{Fingerprint: i}})
				i++
			}
		})
	})

	b.Run("Group100", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		groupLabels := pregenerateLabels(100)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: groupLabels[i%100]}},
					Fingerprint: i,
				}})
				i++
			}
		})
	})

	b.Run("Group10k", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		groupLabels := pregenerateLabels(10_000)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: groupLabels[i%10_000]}},
					Fingerprint: i,
				}})
				i++
			}
		})
	})

	b.Run("Group100k", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		groupLabels := pregenerateLabels(100_000)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: groupLabels[i%100_000]}},
					Fingerprint: i,
				}})
				i++
			}
		})
	})
}

// BenchmarkEstimator_InsertRotateCycle benchmarks the insert→rotate→insert cycle
// in two HLL regimes (sparse/normal) and two grouping modes (no-group/grouped).
//   - Sparse: 1 000 series per interval (sketch stays in sparse mode)
//   - Normal: 30 000 series per interval (sketch converts to dense mode)
func BenchmarkEstimator_InsertRotateCycle(b *testing.B) {
	b.Run("NoGroup/SparseHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		series := generateSeries(1_000, 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.insertMany(series)
			for i := range e.buckets {
				e.buckets[i].rotate()
			}
		}
	})

	b.Run("NoGroup/NormalHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		series := generateSeries(30_000, 0)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.insertMany(series)
			for i := range e.buckets {
				e.buckets[i].rotate()
			}
		}
	})

	b.Run("Group100/SparseHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		series := generateSeries(1_000, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.insertMany(series)
			for i := range e.buckets {
				e.buckets[i].rotate()
			}
		}
	})

	b.Run("Group100/NormalHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:  []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()

		series := generateSeries(30_000, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.insertMany(series)
			for i := range e.buckets {
				e.buckets[i].rotate()
			}
		}
	})
}
