package protoparser

import (
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/golang/snappy"
)

func TestParseRequestBody(t *testing.T) {
	originalFPLabels := fpLabelsGlobal.Load()
	t.Cleanup(func() {
		fpLabelsGlobal.Store(originalFPLabels)
	})

	f := func(fpLabels bool, input []prompb.TimeSeries, exp []TimeSerie) {
		t.Helper()
		pbData := (&prompb.WriteRequest{Timeseries: input}).MarshalProtobuf(nil)
		data := snappy.Encode(nil, pbData)

		SetFingerprintLabels(fpLabels)

		var act []TimeSerie
		if err := parseRequestBody(data, func(batch []TimeSerie) {
			for _, ts := range batch {
				labels := make([]Label, len(ts.Labels))
				copy(labels, ts.Labels)
				act = append(act, TimeSerie{Labels: labels, Fingerprint: ts.Fingerprint})
			}
		}); err != nil {
			t.Fatalf("parseRequestBody: unexpected error: %s", err)
		}
		if !reflect.DeepEqual(exp, act) {
			t.Fatalf("unexpected time series\nexpected: %v\ngot:      %v", exp, act)
		}
	}

	// regular series pass through unchanged
	f(false,
		[]prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "job", Value: "frontend"},
					{Name: "foo", Value: "fooVal"},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "job", Value: "backend"},
					{Name: "bar", Value: "barVal"},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
		},
		[]TimeSerie{
			{
				Labels: []Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "job", Value: "frontend"},
					{Name: "foo", Value: "fooVal"},
				},
				Fingerprint: 16911267577736150059,
			},
			{
				Labels: []Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "job", Value: "backend"},
					{Name: "bar", Value: "barVal"},
				},
				Fingerprint: 11849420350211768702,
			},
		},
	)

	// label fingerprints are computed when labelFP=true
	f(true,
		[]prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "job", Value: "api"},
					{Name: "foo", Value: "fooVal"},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "api"},
					{Name: "bar", Value: "barVal"},
				},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
		},
		[]TimeSerie{
			{
				Labels: []Label{
					{Name: "__name__", Value: "requests_total", Fingerprint: 17112259355248401197},
					{Name: "job", Value: "api", Fingerprint: 5617089093593724463},
					{Name: "foo", Value: "fooVal", Fingerprint: 18265805195628551540},
				},
				Fingerprint: 11009355061427964546,
			},
			{
				Labels: []Label{
					{Name: "__name__", Value: "up", Fingerprint: 2571373012096355983},
					{Name: "job", Value: "api", Fingerprint: 5617089093593724463},
					{Name: "bar", Value: "barVal", Fingerprint: 3223421134637281647},
				},
				Fingerprint: 14808323381317661148,
			},
		},
	)
}
