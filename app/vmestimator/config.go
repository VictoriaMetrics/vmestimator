package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metricsql"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Streams []EstimatorConfig `yaml:"streams"`
}

type EstimatorConfig struct {
	Filter       string            `yaml:"filter"`
	GroupBy      []string          `yaml:"group_by"`
	GroupLimit   int               `yaml:"group_limit"`
	Labels       map[string]string `yaml:"labels"`
	Interval     time.Duration     `yaml:"interval"`
	Buckets      int               `yaml:"buckets"`
	HLLPrecision uint8             `yaml:"hll_precision"`
	HLLSparse    *bool             `yaml:"hll_sparse"`
}

// labelFilter is a compiled label filter for fast matching.
type labelFilter struct {
	label      string
	value      string
	isNegative bool
	isRegexp   bool
	re         *regexp.Regexp // non-nil when isRegexp is true
}

func compileFilters(filter string) ([]labelFilter, error) {
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
	// Use the first group of label filters (OR-groups are not supported).
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
			re, err := regexp.Compile("^(?:" + lf.Value + ")$")
			if err != nil {
				return nil, fmt.Errorf("cannot compile regexp %q in filter %q: %w", lf.Value, filter, err)
			}
			f.re = re
		}
		result = append(result, f)
	}
	return result, nil
}

func loadConfig(path string) ([]*estimator, error) {
	if path == "" && len(*storageNodes) > 0 {
		return nil, nil
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
	for _, stream := range cfg.Streams {
		sort.Strings(stream.GroupBy)
		if stream.HLLPrecision != 0 && (stream.HLLPrecision < 4 || stream.HLLPrecision > 18) {
			return nil, fmt.Errorf("invalid precision %d: must be in range [4, 18]", stream.HLLPrecision)
		}
	}

	es := make([]*estimator, 0, len(cfg.Streams))
	for _, ec := range cfg.Streams {
		e, err := newEstimator(ec)
		if err != nil {
			logger.Fatalf("cannot create estimator: %v", err)
		}
		es = append(es, e)
	}

	return es, nil
}
