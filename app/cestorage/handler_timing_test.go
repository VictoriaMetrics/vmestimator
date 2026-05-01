package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/makasim/cestimator/app/cestorage/protoparser"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func BenchmarkParse_EstimatorGlobal(b *testing.B) {
	data := buildSnappyEncodedWriteRequest(5_000, 3, 5, 1)
	e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
	if err != nil {
		b.Fatalf("newEstimator: %v", err)
	}
	defer e.stop()
	estimators := []*estimator{e}
	groupLabels := make([]string, 0)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := protoparser.Parse(bytes.NewReader(data), groupLabels, func(tss []protoparser.TimeSerie) {
				for _, est := range estimators {
					est.insertMany(tss)
				}
			})
			if err != nil {
				b.Errorf("stream.Parse: %v", err)
			}
		}
	})
}

func BenchmarkParse_EstimatorGroup(b *testing.B) {
	data := buildSnappyEncodedWriteRequest(5_000, 3, 5, 100)
	e, err := newEstimator(EstimatorConfig{
		GroupBy:  []string{"groupLabel"},
		Interval: time.Hour,
	})
	if err != nil {
		b.Fatalf("newEstimator: %v", err)
	}
	defer e.stop()
	estimators := []*estimator{e}
	groupLabels := []string{"groupLabel"}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := protoparser.Parse(bytes.NewReader(data), groupLabels, func(tss []protoparser.TimeSerie) {
				for _, est := range estimators {
					est.insertMany(tss)
				}
			})
			if err != nil {
				b.Errorf("stream.Parse: %v", err)
			}
		}
	})
}

// buildSnappyEncodedWriteRequest builds a snappy-encoded protobuf WriteRequest
// with numSeries time series, each having numLabels labels of labelSize bytes each.
func buildSnappyEncodedWriteRequest(numSeries, numLabels, labelSize, groupsNum int) []byte {
	labelValue := strings.Repeat("x", labelSize)

	tss := make([]prompb.TimeSeries, numSeries)
	for i := range tss {
		labels := make([]prompb.Label, numLabels)
		for j := range labels {
			labels[j] = prompb.Label{
				Name:  fmt.Sprintf("label%02d", j),
				Value: fmt.Sprintf("val%05d_%s", i, labelValue),
			}
		}
		labels = append(labels, prompb.Label{
			Name:  "groupLabel",
			Value: fmt.Sprintf("%d", i%groupsNum),
		})

		tss[i] = prompb.TimeSeries{
			Labels:  labels,
			Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
		}
	}

	wr := &prompb.WriteRequest{Timeseries: tss}
	pbData := wr.MarshalProtobuf(nil)
	return snappy.Encode(nil, pbData)
}
