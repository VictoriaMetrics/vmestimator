package main

import (
	"flag"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

var (
	workers   = flag.Int(`workers`, max(1, cgroup.AvailableCPUs()*2), `The number of workers for processing time series insertions concurrently. Each worker handles insertMany calls for a single estimator. Defaults to 2x the number of available CPUs.`)
	workersCh chan workerReq
	workersWG sync.WaitGroup
)

func startWorkers() {
	if *workers < 1 {
		logger.Fatalf("BUG: -workers must be at least 1, got %d", *workers)
	}

	workersCh = make(chan workerReq, *workers*2)

	for i := 0; i < *workers; i++ {
		workersWG.Go(func() {
			for req := range workersCh {
				e, wg, tss := req.e, req.wg, req.tss

				e.insertMany(tss)
				wg.Done()
			}
		})
	}
}

func stopWorkers() {
	close(workersCh)
	workersWG.Wait()
	workersCh = nil
}

type workerReq struct {
	e   *estimator
	wg  *sync.WaitGroup
	tss []protoparser.TimeSerie
}
