package main

import (
	"bytes"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/metrics"
	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

var prometheusWriteRequests = metrics.NewCounter(`cestorage_http_requests_total{path="/api/v1/write", protocol="promremotewrite"}`)
var rowsInserted = metrics.NewCounter(`cestorage_rows_inserted_total{type="promremotewrite"}`)

var (
	cardinalityMetricsRequests      = metrics.NewCounter(`cestorage_http_requests_total{path="/cardinality/metrics"}`)
	cardinalityMetricsDurationTotal = metrics.NewFloatCounter(`cestorage_http_request_duration_seconds_total{path="/cardinality/metrics"}`)
	cardinalityMetricsBytesTotal    = metrics.NewCounter(`cestorage_http_response_size_bytes_total{path="/cardinality/metrics"}`)
)

var (
	cardinalityCacheMu  sync.Mutex
	cardinalityCacheAt  time.Time
	cardinalityCache    []byte
	cardinalityCacheTTL = flag.Duration("cardinalityCacheTTL", time.Minute, "Duration for caching cardinality metrics response")
)

func requestHandler(estimators []*estimator) httpserver.RequestHandler {
	groupLabelsMap := make(map[string]struct{})
	for _, e := range estimators {
		for _, l := range e.groupBy {
			groupLabelsMap[l] = struct{}{}
		}
	}

	groupLabels := make([]string, 0, len(groupLabelsMap))
	for k := range groupLabelsMap {
		groupLabels = append(groupLabels, k)
	}

	return func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/api/v1/write":
			prometheusWriteRequests.Inc()
			handleRemoteWrite(w, r, groupLabels, estimators)
			return true
		case "/cardinality/metrics":
			handleCardinalityMetrics(w, r, estimators)
			return true
		case "/cardinality/reset":
			handleCardinalityReset(w, r, estimators)
			return true
		}
		return false
	}
}

func handleCardinalityReset(w http.ResponseWriter, _ *http.Request, estimators []*estimator) {
	for _, e := range estimators {
		e.reset()
	}
	w.WriteHeader(http.StatusOK)
}

func handleCardinalityMetrics(w http.ResponseWriter, _ *http.Request, estimators []*estimator) {
	startTime := time.Now()

	cardinalityCacheMu.Lock()
	if time.Since(cardinalityCacheAt) >= *cardinalityCacheTTL {
		plain := bytes.NewBuffer(cardinalityCache[:0])
		for _, e := range estimators {
			e.writeMetrics(plain)
		}
		cardinalityCache = plain.Bytes()
		cardinalityCacheAt = time.Now()
	}
	responseData := cardinalityCache
	cardinalityCacheMu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseData)

	cardinalityMetricsRequests.Inc()
	cardinalityMetricsDurationTotal.Add(time.Since(startTime).Seconds())
	cardinalityMetricsBytesTotal.Add(len(responseData))
}

func handleRemoteWrite(w http.ResponseWriter, r *http.Request, groupLabels []string, estimators []*estimator) {
	err := protoparser.Parse(r.Body, groupLabels, func(tss []protoparser.TimeSerie) {
		//var wg sync.WaitGroup
		for _, e := range estimators {
			//wg.Go(func() {
				e.insertMany(tss)
			//})
		}
		//wg.Wait()
		rowsInserted.Add(len(tss))
	})
	if err != nil {
		httpserver.Errorf(w, r, "error parsing remote write request: %s", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
