package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
)

var globalChurnSnapshots = newChurnSnapshots()

var (
	cardinalityMetricsWrites        = metrics.NewCounter(`vmestimator_write_cardinality_metrics_total`)
	cardinalityMetricsWriteDuration = metrics.NewFloatCounter(`vmestimator_write_cardinality_metrics_duration_seconds_total`)
	cardinalityMetricsWriteBytes    = metrics.NewCounter(`vmestimator_write_cardinality_metrics_size_bytes_total`)

	cardinalityCacheMu         sync.Mutex
	cardinalityMetricsCacheAt  time.Time
	cardinalityMetricsCache    []byte
	cardinalityMetricsCacheTTL = flag.Duration("cardinalityMetrics.cacheTTL", time.Second*30, "Duration for caching cardinality metrics response")
	cardinalityMetricsExposeAt = flag.String(`cardinalityMetrics.exposeAt`, `/metrics`, "HTTP path for exposing cardinality metrics. "+
		"If set to the default /metrics, cardinality metrics are merged with regular metrics and exposed together. "+
		"If set to a different path, only cardinality metrics are exposed at that endpoint. "+
		"If set to an empty value, cardinality metrics are not exposed via HTTP at all.")
	cardinalityMetricsMinCardinality = flag.Uint64("cardinalityMetrics.minCardinality", 0, "Minimum cardinality estimate to expose. "+
		"Estimates below this threshold are suppressed to reduce metric volume and noise. "+
		"Set to 0 to expose all estimates (default 0).")
)

func writeCardinalityMetrics(w io.Writer, es []*estimator, storageNodeURLs []string) {
	startTime := time.Now()

	cardinalityCacheMu.Lock()
	if time.Since(cardinalityMetricsCacheAt) >= *cardinalityMetricsCacheTTL || *cardinalityMetricsCacheTTL == 0 {
		now := time.Now()
		plain := bytes.NewBuffer(cardinalityMetricsCache[:0])
		for _, e := range es {
			var dropped uint64
			if err := e.toSnapshot(func(s *snapshot) error {
				if s.ChurnInterval > 0 {
					globalChurnSnapshots.update(s, now)
				}
				d, err := s.writeCardinalityEstimates(plain)
				dropped += d
				return err
			}); err != nil {
				logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
			}
			if len(e.groupBy) > 0 {
				eb0 := e.buckets[0]
				s := &snapshot{
					GroupBy:    eb0.groupBy,
					Interval:   eb0.interval,
					Filter:     eb0.filter,
					Labels:     eb0.labels,
					GroupLimit: eb0.groupSize.limit,
				}
				if dropped > 0 {
					if err := s.writeDroppedMetric(plain, dropped); err != nil {
						logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
					}
				}
				if err := s.writeGroupSizeAndLimit(plain, eb0.groupSize.totalSize()); err != nil {
					logger.Errorf("writing metrics failed: %s; written cardinality metrics might be incomplete or invalid", err)
				}
			}
		}
		if len(storageNodeURLs) > 0 {
			ss := newSnapshots()
			var wg sync.WaitGroup
			for _, nodeURL := range storageNodeURLs {
				wg.Add(1)
				go func(url string) {
					defer wg.Done()
					if err := fetchAndMergeSnapshots(url, ss.add); err != nil {
						logger.Errorf("fetch snapshots from %s: %s", url, err)
					}
				}(nodeURL)
			}
			wg.Wait()

			for _, s := range ss.m {
				if s.ChurnInterval > 0 {
					globalChurnSnapshots.update(s, now)
				}
			}

			if err := ss.writeMetrics(plain); err != nil {
				logger.Errorf("write cardinality metrics: %s", err)
			}
		}
		if err := globalChurnSnapshots.writeMetrics(plain); err != nil {
			logger.Errorf("write churn metrics: %s", err)
		}

		cardinalityMetricsCache = plain.Bytes()
		cardinalityMetricsCacheAt = time.Now()
	}
	cm := make([]byte, len(cardinalityMetricsCache))
	copy(cm, cardinalityMetricsCache)
	cardinalityCacheMu.Unlock()

	if _, err := w.Write(cm); err != nil {
		logger.Warnf("writing cardinality metrics: %s", err)
	}

	cardinalityMetricsWrites.Inc()
	cardinalityMetricsWriteDuration.Add(time.Since(startTime).Seconds())
	cardinalityMetricsWriteBytes.Add(len(cm))
}

func fetchAndMergeSnapshots(storageNodeURL string, cb func(s *snapshot)) error {
	url := fmt.Sprintf("%s/clusternative/snapshot", storageNodeURL)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return fmt.Errorf("http get %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, url)
	}

	return decodeSnapshots(resp.Body, cb)
}
