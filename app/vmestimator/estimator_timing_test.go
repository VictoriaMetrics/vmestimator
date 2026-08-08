package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func BenchmarkEstimator_WriteMetrics(b *testing.B) {
	b.Run("NoGroup/NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		insertSeriesIntoEstimator(e, 5_000, 0)

		s := newSnapshot()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.reset()
			convertGlobalEstimatorToSnapshot(e, s)
		}
	})

	b.Run("NoGroup/WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.stop()
		insertSeriesIntoEstimator(e, 5_000, 0)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		insertSeriesIntoEstimator(e, 5_000, 0)

		s := newSnapshot()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.reset()
			convertGlobalEstimatorToSnapshot(e, s)
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
		insertSeriesIntoEstimator(e, 5_000, 100)

		s := newSnapshot()
		skp := newSketchesPool(e.buckets[0].precision, e.groupSize.avgBucketSize())

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, eb := range e.buckets {
				s.reset()
				convertEstimatorBucketToSnapshot(eb, s, skp)
				for _, ssk := range s.Sketches {
					skp.put(ssk.Sketch)
				}
			}
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
		insertSeriesIntoEstimator(e, 5_000, 100)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		insertSeriesIntoEstimator(e, 5_000, 100)

		s := newSnapshot()
		skp := newSketchesPool(e.buckets[0].precision, e.groupSize.avgBucketSize())

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, eb := range e.buckets {
				s.reset()
				convertEstimatorBucketToSnapshot(eb, s, skp)
				for _, ssk := range s.Sketches {
					skp.put(ssk.Sketch)
				}
			}
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
		insertSeriesIntoEstimator(e, 50_000, 10_000)

		s := newSnapshot()
		skp := newSketchesPool(e.buckets[0].precision, e.groupSize.avgBucketSize())

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, eb := range e.buckets {
				s.reset()
				convertEstimatorBucketToSnapshot(eb, s, skp)
				for _, ssk := range s.Sketches {
					skp.put(ssk.Sketch)
				}
			}
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
		insertSeriesIntoEstimator(e, 50_000, 10_000)
		for _, eb := range e.buckets {
			eb.rotate()
		}
		insertSeriesIntoEstimator(e, 50_000, 10_000)

		s := newSnapshot()
		skp := newSketchesPool(e.buckets[0].precision, e.groupSize.avgBucketSize())

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, eb := range e.buckets {
				s.reset()
				convertEstimatorBucketToSnapshot(eb, s, skp)
				for _, ssk := range s.Sketches {
					skp.put(ssk.Sketch)
				}
			}
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

		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: fmt.Sprintf("%d", i%100)}},
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

		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: fmt.Sprintf("%d", i%10_000)}},
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

		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			var i uint64
			for pb.Next() {
				e.insertMany([]protoparser.TimeSerie{{
					Labels:      []protoparser.Label{{Name: "groupLabel", Value: fmt.Sprintf("%d", i%100_000)}},
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

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			insertSeriesIntoEstimator(e, 1_000, 0)
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

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			insertSeriesIntoEstimator(e, 30_000, 0)
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

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			insertSeriesIntoEstimator(e, 1_000, 100)
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

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			insertSeriesIntoEstimator(e, 30_000, 100)
			for i := range e.buckets {
				e.buckets[i].rotate()
			}
		}
	})
}

// insertSeriesIntoEstimator inserts numSeries time series into e.
// When groupsNum > 0 each series gets a "groupLabel" cycling through groupsNum values.
func insertSeriesIntoEstimator(e *estimator, numSeries, groupsNum int) {
	for i := 0; i < numSeries; i++ {
		var labels []protoparser.Label
		if groupsNum > 0 {
			labels = append(labels, protoparser.Label{
				Name:  "groupLabel",
				Value: fmt.Sprintf("%d", i%groupsNum),
			})
		}
		e.insertMany([]protoparser.TimeSerie{
			{
				Labels:      labels,
				Fingerprint: hash([]byte(fmt.Sprintf("foobarbaz%d", i))),
			},
		})
	}
}
