package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
	"github.com/valyala/fastrand"
)

var (
	httpListenAddrs = flagutil.NewArrayString("httpListenAddr", "TCP address to listen for incoming HTTP requests")
	configPath      = flag.String("config", "", "Path to YAML configuration file. "+
		"Must be set unless -storageNode is specified. See https://github.com/VictoriaMetrics/vmestimator/blob/main/streams.yaml for config example")
	storageNodes = flagutil.NewArrayString("storageNode", "HTTP URLs of remote vmestimator nodes to query for cardinality snapshots, e.g. http://vmestimator-2:8490")

	prometheusWriteRequests = metrics.NewCounter(`vmestimator_http_requests_total{path="/api/v1/write", protocol="promremotewrite"}`)
)

func main() {
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	es, err := loadConfig(*configPath)
	if err != nil {
		logger.Fatalf("cannot load config: %v", err)
	}

	for _, e := range es {
		for _, l := range e.groupBy {
			if l == labelKeyword {
				protoparser.SetFingerprintLabels(true)
			}
		}
	}

	startWorkers()
	defer stopWorkers()

	var dedup *deduplicator
	if *deduplicationInterval > 0 {
		maxSize := resolveDeduplicatorMaxSize()
		if maxSize <= 0 {
			logger.Fatalf("deduplicator maxSize must be greater than 0; set -deduplication.maxSize or -deduplication.maxSizeBytes")
		}
		dedup = newDeduplicator(maxSize, *deduplicationInterval)
		defer dedup.Stop()
		logger.Infof("deduplicator enabled: interval=%s maxSize=%d sizeBytes=%d", *deduplicationInterval, maxSize, maxSize*bitsPerItem/8)
	}

	if *cardinalityMetricsExposeAt == `/metrics` {
		metrics.RegisterMetricsWriter(func(w io.Writer) {
			writeCardinalityMetrics(w, es, *storageNodes)
		})
	}

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":8490"}
	}

	logger.Infof("starting vmestimator at %q", listenAddrs)
	startTime := time.Now()

	go httpserver.Serve(listenAddrs, func(w http.ResponseWriter, r *http.Request) bool {
		cmPath := *cardinalityMetricsExposeAt
		if cmPath != "/metrics" && cmPath != "" && r.URL.Path == cmPath {
			w.WriteHeader(http.StatusOK)
			writeCardinalityMetrics(w, es, *storageNodes)
			return true
		}

		path, _ := strings.CutPrefix(r.URL.Path, `/cardinality`)
		switch path {
		case "/api/v1/write":
			prometheusWriteRequests.Inc()
			err := protoparser.Parse(r.Body, func(tss []protoparser.TimeSerie) {
				if dedup != nil {
					tss = dedup.filter(tss, tss[:0])
				}
				const chunkSize = 100
				esLen := uint32(len(es))
				wg := &sync.WaitGroup{}
			loop:
				for start := 0; start < len(tss); start += chunkSize {
					end := start + chunkSize
					if end > len(tss) {
						end = len(tss)
					}
					tssChunk := tss[start:end]

					esStart := fastrand.Uint32n(esLen)

					for j := uint32(0); j < esLen; j++ {
						idx := (esStart + j) % esLen
						wg.Add(1)
						select {
						case workersCh <- workerReq{e: es[idx], wg: wg, tss: tssChunk}:
						case <-r.Context().Done():
							wg.Done()
							break loop
						}
					}
				}
				wg.Wait()
			})
			if err != nil {
				httpserver.Errorf(w, r, "error parsing remote write request: %s", err)
				return true
			}
			w.WriteHeader(http.StatusNoContent)
			return true
		case "/clusternative/query", "/clusternative/snapshot":
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Transfer-Encoding", "chunked")
			w.WriteHeader(http.StatusOK)

			bw := bufio.NewWriterSize(w, 64*1024)
			enc := gob.NewEncoder(bw)
			for _, e := range es {
				if err := e.toSnapshot(func(s *snapshot) error {
					return enc.Encode(s)
				}); err != nil {
					logger.Errorf("write snapshot binary: %s", err)
				}
			}
			if err := bw.Flush(); err != nil {
				logger.Errorf("flush snapshot binary: %s", err)
			}

			return true
		case "/reset":
			for _, e := range es {
				e.reset()
			}
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	}, httpserver.ServeOptions{})

	logger.Infof("started vmestimator in %.3f seconds", time.Since(startTime).Seconds())

	pushmetrics.Init()
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s", sig)
	pushmetrics.Stop()

	logger.Infof("gracefully shutting down webservice at %q", listenAddrs)
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Errorf("cannot stop http server: %s", err)
	}
	for _, e := range es {
		e.stop()
	}
	logger.Infof("shutting down vmestimator")
}
