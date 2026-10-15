package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
	"github.com/cespare/xxhash/v2"
	"gopkg.in/yaml.v2"
)

var (
	reloadAuthKey = flagutil.NewPassword("reloadAuthKey", "Auth key for /-/reload http endpoint. It must be passed via authKey query arg. It overrides -httpAuth.*")

	configCheckInterval = flag.Duration("configCheckInterval", 0, "Interval for checking for changes in '-config' file. "+
		"By default, the checking is disabled. Send SIGHUP signal or request /-/reload in order to force config check for changes")

	configReloads      = metrics.NewCounter(`vmestimator_config_reloads_total`)
	configReloadErrors = metrics.NewCounter(`vmestimator_config_reloads_errors_total`)
	configSuccess      = metrics.NewGauge(`vmestimator_config_last_reload_successful`, nil)
	configTimestamp    = metrics.NewCounter(`vmestimator_config_last_reload_success_timestamp_seconds`)
)

type Config struct {
	hash uint64

	Streams []EstimatorConfig `yaml:"streams"`
}

// prepare normalizes config settings and validates them.
func (cfg *Config) prepare() error {
	for i, stream := range cfg.Streams {
		sort.Strings(stream.GroupBy)
		if stream.HLLPrecision != 0 && (stream.HLLPrecision < 4 || stream.HLLPrecision > 18) {
			return fmt.Errorf("invalid precision %d: must be in range [4, 18]", stream.HLLPrecision)
		}
		count := 0
		newGroupBy := make([]string, 0, len(stream.GroupBy))
		for _, g := range stream.GroupBy {
			if g == labelKeyword {
				count++
				continue
			}
			newGroupBy = append(newGroupBy, g)
		}
		if count > 1 {
			return fmt.Errorf("__label__ may appear at most once in group_by")
		}
		if count == 1 {
			newGroupBy = append(newGroupBy, labelKeyword)
		}
		stream.GroupBy = newGroupBy
		stream.applyDefaults()
		cfg.Streams[i] = stream
	}

	reservedLabels := map[string]bool{
		"interval":        true,
		"filter":          true,
		"group_by_keys":   true,
		"group_by_values": true,
	}
	ids := make(map[string]int, len(cfg.Streams))
	for i, ec := range cfg.Streams {
		for k := range ec.Labels {
			if reservedLabels[k] {
				return fmt.Errorf("static label name %q is reserved and cannot be used in labels", k)
			}
			if strings.HasPrefix(k, "by_") {
				return fmt.Errorf("static label name %q is reserved: labels starting with \"by_\" are reserved and cannot be used in labels", k)
			}
		}
		// Streams with the same labels, group_by, interval and filter produce identical time series.
		id := estimatorID(ec.Labels, ec.GroupBy, ec.Interval, ec.Filter)
		if j, ok := ids[id]; ok {
			return fmt.Errorf("streams #%d and #%d have the same labels, group_by, interval and filter; such streams produce identical time series", j+1, i+1)
		}
		ids[id] = i
	}

	return nil
}

type EstimatorConfig struct {
	Filter         string            `yaml:"filter"`
	GroupBy        []string          `yaml:"group_by"`
	GroupLimit     int               `yaml:"group_limit"`
	Labels         map[string]string `yaml:"labels"`
	Interval       time.Duration     `yaml:"interval"`
	CalculateChurn bool              `yaml:"calculate_churn"`
	Buckets        int               `yaml:"buckets"`
	HLLPrecision   uint8             `yaml:"hll_precision"`
	HLLSparse      *bool             `yaml:"hll_sparse"`
	MinCardinality int64             `yaml:"min_cardinality"`
}

func (ec *EstimatorConfig) applyDefaults() {
	if ec.Interval == 0 {
		ec.Interval = 5 * time.Minute
	}
	if ec.GroupLimit <= 0 {
		ec.GroupLimit = 10000
	}
	if ec.Buckets <= 0 {
		buckets := min(64, 2*cgroup.AvailableCPUs())
		ec.Buckets = max(4, buckets)
	}
	if ec.HLLPrecision == 0 {
		ec.HLLPrecision = 14
	}
	if ec.HLLSparse == nil {
		ec.HLLSparse = new(true)
	}
}

// readConfig reads, parses and prepares the config file at path.
// An empty config is returned if path is empty and -storageNode is set.
func readConfig(path string) (*Config, error) {
	if path == "" && len(*storageNodes) > 0 {
		return &Config{}, nil
	}
	if path == "" {
		return nil, fmt.Errorf("either -config or -storageNode must be specified; see https://github.com/VictoriaMetrics/vmestimator/blob/main/streams.yaml for config example")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file %q: %w", path, err)
	}
	if err := cfg.prepare(); err != nil {
		return nil, fmt.Errorf("invalid config file %q: %w", path, err)
	}
	cfg.hash = xxhash.Sum64(data)
	return &cfg, nil
}

// loadConfig applies cfg to the active estimators.
//
// Active estimators with the same id as a configured stream are reused with their state,
// missing ones are created and the ones absent in the config are stopped.
// It returns false if the set of estimators didn't change; stream order is ignored.
// Active estimators are left untouched if an error is returned.
func loadConfig(cfg *Config, dedup *deduplicator) (bool, error) {
	old := getEstimators()
	removed := make(map[string]*estimator, len(old))
	for _, e := range old {
		removed[e.id] = e
	}
	es := make([]*estimator, 0, len(cfg.Streams))
	var created []*estimator
	for _, ec := range cfg.Streams {
		id := estimatorID(ec.Labels, ec.GroupBy, ec.Interval, ec.Filter)
		if e := removed[id]; e != nil {
			es = append(es, e)
			delete(removed, id)
			continue
		}
		e, err := newEstimator(ec)
		if err != nil {
			for _, e := range created {
				e.stop()
			}
			return false, fmt.Errorf("cannot create estimator: %w", err)
		}
		es = append(es, e)
		created = append(created, e)
	}
	if len(created) == 0 && len(removed) == 0 {
		return false, nil
	}

	// Label fingerprinting is never turned off, since in-flight requests may still feed estimators needing it.
	for _, e := range created {
		if e.hasLabelKeyword {
			protoparser.SetFingerprintLabels(true)
			break
		}
	}
	if dedup != nil {
		// New estimators must see series the deduplicator already passed to the previous ones.
		dedup.Reset()
	}
	// In-flight requests may still insert into removed estimators; this is harmless.
	estimators.Store(es)
	for _, e := range removed {
		e.stop()
	}

	invalidateCardinalityCache(es)

	return true, nil
}

// startReloader re-reads the config at path on every sighupCh event and every -configCheckInterval,
// and loads it if its contents differ from cfg. The previous config stays active on error.
// The returned function stops the reloader and waits for it to exit.
func startReloader(path string, cfg *Config, sighupCh <-chan os.Signal, dedup *deduplicator) (stop func()) {
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

			newCfg, err := readConfig(path)
			if err != nil {
				configReloadErrors.Inc()
				configSuccess.Set(0)
				logger.Errorf("cannot read config from %q: %s; continuing with the previous config", path, err)
				continue
			}
			if newCfg.hash == cfg.hash {
				configSuccess.Set(1)
				continue
			}
			changed, err := loadConfig(newCfg, dedup)
			if err != nil {
				configReloadErrors.Inc()
				configSuccess.Set(0)
				logger.Errorf("cannot load config from %q: %s; continuing with the previous config", path, err)
				continue
			}
			configSuccess.Set(1)
			cfg = newCfg
			if !changed {
				continue
			}

			configReloads.Inc()
			configTimestamp.Set(fasttime.UnixTimestamp())
			logger.Infof("reloaded config from %q", path)
		}
	}()

	return func() {
		close(stopCh)
		<-doneCh
	}
}

type compiledFilter []labelFilter

// labelFilter is a compiled label filter for fast matching.
type labelFilter struct {
	label      string
	value      string
	isNegative bool
	isRegexp   bool
	re         *regexp.Regexp // non-nil when isRegexp is true
}

func compileFilters(filter string) (compiledFilter, error) {
	if filter == "" {
		return nil, nil
	}
	expr, err := metricsql.Parse(filter)
	if err != nil {
		return nil, fmt.Errorf("cannot parse filter %q: %w", filter, err)
	}
	me, ok := expr.(*metricsql.MetricExpr)
	if !ok {
		return nil, fmt.Errorf("filter %q must be a metric selector, got %T", filter, expr)
	}
	if len(me.LabelFilterss) == 0 {
		return nil, nil
	}
	if len(me.LabelFilterss) > 1 {
		return nil, fmt.Errorf("filter %q must not contain OR groups; got %d groups", filter, len(me.LabelFilterss))
	}
	lfs := me.LabelFilterss[0]
	result := make([]labelFilter, 0, len(lfs))
	for _, lf := range lfs {
		f := labelFilter{
			label:      lf.Label,
			value:      lf.Value,
			isNegative: lf.IsNegative,
			isRegexp:   lf.IsRegexp,
		}
		if lf.IsRegexp {
			re, err := metricsql.CompileRegexpAnchored(lf.Value)
			if err != nil {
				return nil, fmt.Errorf("cannot compile regexp %q in filter %q: %w", lf.Value, filter, err)
			}
			f.re = re
		}
		result = append(result, f)
	}
	return result, nil
}

// matchesFilters returns true if all filters are satisfied by labels.
// It returns true immediately when filters is empty (fast path).
func (cf compiledFilter) match(labels []protoparser.Label) bool {
	if len(cf) == 0 {
		return true
	}
	for _, f := range cf {
		val := ""
		for _, l := range labels {
			if l.Name == f.label {
				val = l.Value
				break
			}
		}
		var matched bool
		if f.isRegexp {
			matched = f.re.MatchString(val)
		} else {
			matched = val == f.value
		}
		if f.isNegative {
			matched = !matched
		}
		if !matched {
			return false
		}
	}
	return true
}
