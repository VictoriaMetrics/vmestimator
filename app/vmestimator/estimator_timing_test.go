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
	f := func(sparse bool) {
		benchName := "Sparse/"
		if !sparse {
			benchName = "Normal/"
		}

		b.Run(benchName+"NoGroup", func(b *testing.B) {
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

		b.Run(benchName+"Group100", func(b *testing.B) {
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

		b.Run(benchName+"Group10k", func(b *testing.B) {
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

		b.Run(benchName+"Group100k", func(b *testing.B) {
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

	f(true)
	f(false)
}

// BenchmarkEstimator_InsertRotateCycle benchmarks the insert→rotate→insert cycle
// in two HLL regimes (sparse/normal) and two grouping modes (no-group/grouped).
//   - Sparse: 1 000 series per interval (sketch stays in sparse mode)
//   - Normal: 30 000 series per interval (sketch converts to dense mode)
func BenchmarkEstimator_InsertRotateCycle(b *testing.B) {
	b.Run("NoGroup/SparseHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			Interval:  time.Hour,
			HLLSparse: new(true),
		})
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
		e, err := newEstimator(EstimatorConfig{
			Interval:  time.Hour,
			HLLSparse: new(false),
		})
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
			GroupBy:   []string{"groupLabel"},
			Interval:  time.Hour,
			HLLSparse: new(true),
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

	b.Run("Group100/NormalHLL", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			GroupBy:   []string{"groupLabel"},
			Interval:  time.Hour,
			HLLSparse: new(false),
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

// BenchmarkEstimator_InsertManyParallelLabelKeyword mimics production __label__ expansion:
// concurrent workers insert 100-series chunks where every series carries the same set of label names.
func BenchmarkEstimator_InsertManyParallelLabelKeyword(b *testing.B) {
	f := func(name string, groupBy []string) {
		b.Run(name, func(b *testing.B) {
			e, err := newEstimator(EstimatorConfig{
				GroupBy:  groupBy,
				Interval: time.Hour,
			})
			if err != nil {
				b.Fatalf("newEstimator: %v", err)
			}
			defer e.stop()

			const chunkSize = 100
			const labelsPerSeries = 15
			const chunksNum = 64
			chunks := make([][]protoparser.TimeSerie, chunksNum)
			var buf []byte
			for c := range chunks {
				chunk := make([]protoparser.TimeSerie, chunkSize)
				for i := range chunk {
					n := c*chunkSize + i
					labels := make([]protoparser.Label, 0, labelsPerSeries)
					labels = append(labels, protoparser.Label{Name: "job", Value: fmt.Sprintf("job_%d", n%10)})
					for l := 1; l < labelsPerSeries; l++ {
						v := fmt.Sprintf("value_%d_%d", l, n)
						buf = append(buf[:0], v...)
						labels = append(labels, protoparser.Label{Name: fmt.Sprintf("label_%d", l), Value: v, Fingerprint: hash(buf)})
					}
					labels[0].Fingerprint = hash([]byte(labels[0].Value))
					buf = strconv.AppendInt(append(buf[:0], "series"...), int64(n), 10)
					chunk[i] = protoparser.TimeSerie{Labels: labels, Fingerprint: hash(buf)}
				}
				chunks[c] = chunk
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(chunkSize)
			b.RunParallel(func(pb *testing.PB) {
				var i int
				for pb.Next() {
					e.insertMany(chunks[i%chunksNum])
					i++
				}
			})
		})
	}

	f("LabelOnly", []string{labelKeyword})
	f("JobLabel", []string{"job", labelKeyword})
	f("Job", []string{"job"})
}
