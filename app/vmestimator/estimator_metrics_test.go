package main

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestEstimatorOperationalMetricsUniqueAcrossStreams verifies that estimators
// sharing the same group_by but configured with different intervals do not
// expose duplicate series on the metrics endpoint.
// See https://github.com/VictoriaMetrics/vmestimator/issues/20
func TestEstimatorOperationalMetricsUniqueAcrossStreams(t *testing.T) {
	e1, err := newEstimator(EstimatorConfig{
		Interval: 15 * time.Minute,
		GroupBy:  []string{"job"},
		Buckets:  2,
	})
	if err != nil {
		t.Fatalf("failed to create first estimator: %v", err)
	}
	defer e1.stop()

	e2, err := newEstimator(EstimatorConfig{
		Interval: 30 * time.Minute,
		GroupBy:  []string{"job"},
		Buckets:  2,
	})
	if err != nil {
		t.Fatalf("failed to create second estimator: %v", err)
	}
	defer e2.stop()

	var bb bytes.Buffer
	e1.metricsSet.WritePrometheus(&bb)
	e2.metricsSet.WritePrometheus(&bb)

	seen := make(map[string]struct{})
	sc := bufio.NewScanner(&bb)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series := line
		if n := strings.LastIndexByte(line, ' '); n >= 0 {
			series = line[:n]
		}
		if _, ok := seen[series]; ok {
			t.Fatalf("duplicate series exposed by streams sharing the same group_by: %s", series)
		}
		seen[series] = struct{}{}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("failed to scan metrics: %v", err)
	}

	for _, series := range []string{
		`vmestimator_estimator_insert_total{group_by_keys="job",interval="15m0s"}`,
		`vmestimator_estimator_insert_total{group_by_keys="job",interval="30m0s"}`,
	} {
		if _, ok := seen[series]; !ok {
			t.Fatalf("expected series %s is not exposed", series)
		}
	}
}
