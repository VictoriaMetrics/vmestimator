package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

var (
	reloadAuthKey = flagutil.NewPassword("reloadAuthKey", "Auth key for /-/reload http endpoint. It must be passed via authKey query arg. It overrides -httpAuth.*")

	configCheckInterval = flag.Duration("configCheckInterval", 0, "Interval for checking for changes in '-config' file. "+
		"By default, the checking is disabled. Send SIGHUP signal or request /-/reload in order to force config check for changes")

	configReloads      = metrics.NewCounter(`vmestimator_config_reloads_total`)
	configReloadErrors = metrics.NewCounter(`vmestimator_config_reloads_errors_total`)

	configSuccess   atomic.Uint64
	configTimestamp atomic.Uint64

	_ = metrics.NewGauge(`vmestimator_config_last_reload_successful`, func() float64 {
		return float64(configSuccess.Load())
	})
	_ = metrics.NewGauge(`vmestimator_config_last_reload_success_timestamp_seconds`, func() float64 {
		return float64(configTimestamp.Load())
	})
)

var activeEstimators atomic.Pointer[[]*estimator]

var activeEstimatorsMu sync.RWMutex

func getEstimators() []*estimator {
	es := activeEstimators.Load()
	if es == nil {
		return nil
	}
	return *es
}

func withEstimators(f func(es []*estimator)) {
	activeEstimatorsMu.RLock()
	defer activeEstimatorsMu.RUnlock()
	f(getEstimators())
}

// enableFingerprintLabels turns on label fingerprinting if any estimator groups by __label__.
// It is never turned off, since in-flight requests may still feed estimators needing it.
func enableFingerprintLabels(es []*estimator) {
	for _, e := range es {
		if e.hasLabelKeyword {
			protoparser.SetFingerprintLabels(true)
			return
		}
	}
}

func configHash(data []byte) [sha256.Size]byte {
	return sha256.Sum256(data)
}

func updateConfigReloadStatus(changed bool) {
	if !changed {
		return
	}
	configSuccess.Store(1)
	configTimestamp.Store(uint64(time.Now().Unix()))
}

// reloadConfig applies the config at path if its hash differs from prevHash.
// The previous config stays active on error.
func reloadConfig(path string, prevHash [sha256.Size]byte, dedup *deduplicator) (hash [sha256.Size]byte, changed bool, err error) {
	data, err := readConfig(path)
	if err != nil {
		return prevHash, false, err
	}
	hash = configHash(data)
	if hash == prevHash {
		return prevHash, false, nil
	}

	cfgs, err := parseEstimatorConfigs(path, data)
	if err != nil {
		return prevHash, false, err
	}

	old := getEstimators()
	oldByID := make(map[string]*estimator, len(old))
	for _, e := range old {
		oldByID[e.id] = e
	}
	ids := make(map[string]struct{}, len(cfgs))
	for _, cfg := range cfgs {
		ids[estimatorConfigID(cfg)] = struct{}{}
	}
	// Exit early if the estimator IDs haven't changed - order changes are ignored.
	if sameEstimatorIDs(oldByID, ids) {
		return hash, false, nil
	}

	es := make([]*estimator, 0, len(cfgs))
	created := make([]*estimator, 0, len(cfgs))
	for _, cfg := range cfgs {
		id := estimatorConfigID(cfg)
		if e := oldByID[id]; e != nil {
			es = append(es, e)
			delete(oldByID, id)
			continue
		}
		e, err := newEstimator(cfg)
		if err != nil {
			for _, e := range created {
				e.stop()
			}
			return prevHash, false, fmt.Errorf("cannot create estimator: %w", err)
		}
		es = append(es, e)
		created = append(created, e)
	}

	enableFingerprintLabels(es)
	activeEstimatorsMu.Lock()
	if dedup != nil {
		dedup.Reset()
	}
	activeEstimators.Store(&es)
	for _, e := range oldByID {
		e.stop()
	}
	activeEstimatorsMu.Unlock()

	invalidateCardinalityCache(es)

	return hash, true, nil
}

func sameEstimatorIDs(es map[string]*estimator, ids map[string]struct{}) bool {
	if len(es) != len(ids) {
		return false
	}
	for id := range ids {
		if es[id] == nil {
			return false
		}
	}
	return true
}

// startReloader reloads config on every sighupCh event and every -configCheckInterval.
// The returned function stops the reloader and waits for it to exit.
func startReloader(path string, hash [sha256.Size]byte, sighupCh <-chan os.Signal, dedup *deduplicator) (stop func()) {
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)

		var tickerCh <-chan time.Time
		if *configCheckInterval > 0 {
			ticker := time.NewTicker(*configCheckInterval)
			defer ticker.Stop()
			tickerCh = ticker.C
		}

		for {
			select {
			case <-stopCh:
				return
			case <-sighupCh:
				logger.Infof("SIGHUP received; reloading config from %q", path)
			case <-tickerCh:
			}

			newHash, changed, err := reloadConfig(path, hash, dedup)
			if err != nil {
				configReloadErrors.Inc()
				configSuccess.Store(0)
				logger.Errorf("cannot reload config from %q: %s; continuing with the previous config", path, err)
				continue
			}

			hash = newHash
			if !changed {
				continue
			}

			configReloads.Inc()
			updateConfigReloadStatus(changed)
			logger.Infof("reloaded config from %q", path)
		}
	}()

	return func() {
		close(stopCh)
		<-doneCh
	}
}
