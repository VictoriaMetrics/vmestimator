package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func TestDerivedEstimatorNoRejects(t *testing.T) {
	baseCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__", "job", "region"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}
	derivedCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}
	independentCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}

	base, err := newEstimator(baseCfg)
	if err != nil {
		t.Fatalf("newEstimator base: %v", err)
	}
	defer base.stop()

	derived, err := newEstimator(derivedCfg)
	if err != nil {
		t.Fatalf("newEstimator derived: %v", err)
	}
	defer derived.stop()

	independent, err := newEstimator(independentCfg)
	if err != nil {
		t.Fatalf("newEstimator independent: %v", err)
	}
	defer independent.stop()

	di, err := newDerivedInfo(base, derived, baseCfg)
	if err != nil {
		t.Fatalf("newDerivedInfo: %v", err)
	}
	derived.derived = di
	base.baseFor = append(base.baseFor, derived)
	for _, b := range base.buckets {
		b.baseFor = base.baseFor
	}

	metricNames := []string{"http_requests_total", "grpc_duration_seconds", "go_goroutines"}
	jobs := []string{"api", "worker", "scheduler"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}

	var tss []protoparser.TimeSerie
	for _, name := range metricNames {
		for _, job := range jobs {
			for _, region := range regions {
				for i := 0; i < 50; i++ {
					tss = append(tss, protoparser.TimeSerie{
						Labels: []protoparser.Label{
							{Name: "__name__", Value: name},
							{Name: "job", Value: job},
							{Name: "region", Value: region},
							{Name: "instance", Value: fmt.Sprintf("host-%d", i)},
						},
						Fingerprint: hash([]byte(fmt.Sprintf("%s/%s/%s/%d", name, job, region, i))),
					})
				}
			}
		}
	}

	base.insertMany(tss)
	independent.insertMany(tss)

	baseBuf := bytes.NewBuffer(nil)
	derived.writeMetrics(baseBuf)
	derivedOut := baseBuf.String()

	indepBuf := bytes.NewBuffer(nil)
	independent.writeMetrics(indepBuf)
	indepOut := indepBuf.String()

	for _, name := range metricNames {
		derivedRe := regexp.MustCompile(fmt.Sprintf(`by__name__="%s"\}\s*(\d+)`, regexp.QuoteMeta(name)))
		indepRe := regexp.MustCompile(fmt.Sprintf(`by__name__="%s"\}\s*(\d+)`, regexp.QuoteMeta(name)))

		derivedMatch := derivedRe.FindStringSubmatch(derivedOut)
		indepMatch := indepRe.FindStringSubmatch(indepOut)

		if derivedMatch == nil {
			t.Fatalf("derived output missing %s\noutput:\n%s", name, derivedOut)
		}
		if indepMatch == nil {
			t.Fatalf("independent output missing %s\noutput:\n%s", name, indepOut)
		}

		derivedEst, _ := strconv.ParseUint(derivedMatch[1], 10, 64)
		indepEst, _ := strconv.ParseUint(indepMatch[1], 10, 64)

		if derivedEst != indepEst {
			t.Errorf("estimate mismatch for %s: derived=%d independent=%d", name, derivedEst, indepEst)
		}
	}
}

func TestDerivedEstimatorWithRejects(t *testing.T) {
	baseCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__", "job"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   3,
	}
	derivedCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}
	independentCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}

	base, err := newEstimator(baseCfg)
	if err != nil {
		t.Fatalf("newEstimator base: %v", err)
	}
	defer base.stop()

	derived, err := newEstimator(derivedCfg)
	if err != nil {
		t.Fatalf("newEstimator derived: %v", err)
	}
	defer derived.stop()

	independent, err := newEstimator(independentCfg)
	if err != nil {
		t.Fatalf("newEstimator independent: %v", err)
	}
	defer independent.stop()

	di, err := newDerivedInfo(base, derived, baseCfg)
	if err != nil {
		t.Fatalf("newDerivedInfo: %v", err)
	}
	derived.derived = di
	base.baseFor = append(base.baseFor, derived)
	for _, b := range base.buckets {
		b.baseFor = base.baseFor
	}

	metricNames := []string{"metric_a", "metric_b"}
	jobs := []string{"job1", "job2", "job3", "job4", "job5"}

	var tss []protoparser.TimeSerie
	for _, name := range metricNames {
		for _, job := range jobs {
			for i := 0; i < 100; i++ {
				tss = append(tss, protoparser.TimeSerie{
					Labels: []protoparser.Label{
						{Name: "__name__", Value: name},
						{Name: "job", Value: job},
						{Name: "instance", Value: fmt.Sprintf("host-%d", i)},
					},
					Fingerprint: hash([]byte(fmt.Sprintf("%s/%s/%d", name, job, i))),
				})
			}
		}
	}

	base.insertMany(tss)
	independent.insertMany(tss)

	baseBuf := bytes.NewBuffer(nil)
	derived.writeMetrics(baseBuf)
	derivedOut := baseBuf.String()

	indepBuf := bytes.NewBuffer(nil)
	independent.writeMetrics(indepBuf)
	indepOut := indepBuf.String()

	for _, name := range metricNames {
		derivedRe := regexp.MustCompile(fmt.Sprintf(`by__name__="%s"\}\s*(\d+)`, regexp.QuoteMeta(name)))
		indepRe := regexp.MustCompile(fmt.Sprintf(`by__name__="%s"\}\s*(\d+)`, regexp.QuoteMeta(name)))

		derivedMatch := derivedRe.FindStringSubmatch(derivedOut)
		indepMatch := indepRe.FindStringSubmatch(indepOut)

		if derivedMatch == nil {
			t.Fatalf("derived output missing %s\noutput:\n%s", name, derivedOut)
		}
		if indepMatch == nil {
			t.Fatalf("independent output missing %s\noutput:\n%s", name, indepOut)
		}

		derivedEst, _ := strconv.ParseUint(derivedMatch[1], 10, 64)
		indepEst, _ := strconv.ParseUint(indepMatch[1], 10, 64)

		t.Logf("estimate for %s: derived=%d independent=%d", name, derivedEst, indepEst)

		if derivedEst < indepEst {
			t.Errorf("derived estimate for %s (%d) should not be less than independent (%d) — rejected fingerprints not captured",
				name, derivedEst, indepEst)
		}
	}
}

func TestDerivedEstimatorGlobal(t *testing.T) {
	baseCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      []string{"__name__", "job"},
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}
	derivedCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      nil,
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}
	independentCfg := EstimatorConfig{
		Interval:     time.Minute * 5,
		GroupBy:      nil,
		Buckets:      4,
		HLLPrecision: 14,
		GroupLimit:   100000,
	}

	base, err := newEstimator(baseCfg)
	if err != nil {
		t.Fatalf("newEstimator base: %v", err)
	}
	defer base.stop()

	derived, err := newEstimator(derivedCfg)
	if err != nil {
		t.Fatalf("newEstimator derived: %v", err)
	}
	defer derived.stop()

	independent, err := newEstimator(independentCfg)
	if err != nil {
		t.Fatalf("newEstimator independent: %v", err)
	}
	defer independent.stop()

	di, err := newDerivedInfo(base, derived, baseCfg)
	if err != nil {
		t.Fatalf("newDerivedInfo: %v", err)
	}
	derived.derived = di
	base.baseFor = append(base.baseFor, derived)
	for _, b := range base.buckets {
		b.baseFor = base.baseFor
	}

	var tss []protoparser.TimeSerie
	for _, name := range []string{"metric_a", "metric_b", "metric_c"} {
		for _, job := range []string{"job1", "job2"} {
			for i := 0; i < 100; i++ {
				tss = append(tss, protoparser.TimeSerie{
					Labels: []protoparser.Label{
						{Name: "__name__", Value: name},
						{Name: "job", Value: job},
						{Name: "instance", Value: fmt.Sprintf("h%d", i)},
					},
					Fingerprint: hash([]byte(fmt.Sprintf("%s/%s/%d", name, job, i))),
				})
			}
		}
	}

	base.insertMany(tss)
	independent.insertMany(tss)

	baseBuf := bytes.NewBuffer(nil)
	derived.writeMetrics(baseBuf)
	derivedOut := baseBuf.String()

	indepBuf := bytes.NewBuffer(nil)
	independent.writeMetrics(indepBuf)
	indepOut := indepBuf.String()

	derivedRe := regexp.MustCompile(`group_by_keys="__global__"\}\s*(\d+)`)
	indepRe := regexp.MustCompile(`group_by_keys="__global__"\}\s*(\d+)`)

	derivedMatch := derivedRe.FindStringSubmatch(derivedOut)
	indepMatch := indepRe.FindStringSubmatch(indepOut)

	if derivedMatch == nil {
		t.Fatalf("derived output missing global estimate\noutput:\n%s", derivedOut)
	}
	if indepMatch == nil {
		t.Fatalf("independent output missing global estimate\noutput:\n%s", indepOut)
	}

	derivedEst, _ := strconv.ParseUint(derivedMatch[1], 10, 64)
	indepEst, _ := strconv.ParseUint(indepMatch[1], 10, 64)

	t.Logf("global estimate: derived=%d independent=%d", derivedEst, indepEst)

	if derivedEst != indepEst {
		t.Errorf("global estimate mismatch: derived=%d independent=%d", derivedEst, indepEst)
	}
}

func TestBuildParentToBase(t *testing.T) {
	tests := []struct {
		name        string
		parentGroup []string
		baseGroup   []string
		want        []int
		wantErr     bool
	}{
		{
			name:        "subset",
			parentGroup: []string{"__name__"},
			baseGroup:   []string{"__name__", "job", "region"},
			want:        []int{0},
		},
		{
			name:        "two keys",
			parentGroup: []string{"__name__", "region"},
			baseGroup:   []string{"__name__", "job", "region"},
			want:        []int{0, 2},
		},
		{
			name:        "all keys",
			parentGroup: []string{"__name__", "job", "region"},
			baseGroup:   []string{"__name__", "job", "region"},
			want:        []int{0, 1, 2},
		},
		{
			name:        "empty parent (global)",
			parentGroup: nil,
			baseGroup:   []string{"__name__", "job"},
			want:        nil,
		},
		{
			name:        "not a subset",
			parentGroup: []string{"__name__", "missing_key"},
			baseGroup:   []string{"__name__", "job"},
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildParentToBase(tc.parentGroup, tc.baseGroup)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestDerivedEstimatorConfigValidation(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "valid derivation",
			yaml: `streams:
  - interval: 5m
    group_by: [__name__, job]
    hll_precision: 14
  - interval: 5m
    group_by: [__name__]
    hll_precision: 14
    derive_from: 0
`,
		},
		{
			name: "interval mismatch",
			yaml: `streams:
  - interval: 5m
    group_by: [__name__, job]
    hll_precision: 14
  - interval: 10m
    group_by: [__name__]
    hll_precision: 14
    derive_from: 0
`,
			wantErr: "same interval",
		},
		{
			name: "filter mismatch",
			yaml: `streams:
  - interval: 5m
    filter: '{__name__=~"foo.*"}'
    group_by: [__name__, job]
    hll_precision: 14
  - interval: 5m
    group_by: [__name__]
    hll_precision: 14
    derive_from: 0
`,
			wantErr: "same filter",
		},
		{
			name: "group_by not subset",
			yaml: `streams:
  - interval: 5m
    group_by: [__name__]
    hll_precision: 14
  - interval: 5m
    group_by: [job]
    hll_precision: 14
    derive_from: 0
`,
			wantErr: "not found in base stream",
		},
		{
			name: "derive from global (no group_by)",
			yaml: `streams:
  - interval: 5m
    hll_precision: 14
  - interval: 5m
    group_by: [__name__]
    hll_precision: 14
    derive_from: 0
`,
			wantErr: "no group_by",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := tmpDir + "/config.yaml"
			if err := writeFile(path, tc.yaml); err != nil {
				t.Fatalf("writeFile: %v", err)
			}
			es, err := loadConfig(path)
			if tc.wantErr != "" {
				if err == nil {
					if es != nil {
						for _, e := range es {
							e.stop()
						}
					}
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, e := range es {
				e.stop()
			}
		})
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
