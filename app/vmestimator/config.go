package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
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

	// MinCardinality, when non-nil, overrides the global
	// -cardinalityMetrics.minCardinality flag for this stream only.
	// When nil, the global flag value is used.
	MinCardinality *uint64 `yaml:"min_cardinality"`
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
	for i, stream := range cfg.Streams {
		sort.Strings(stream.GroupBy)
		if stream.HLLPrecision != 0 && (stream.HLLPrecision < 4 || stream.HLLPrecision > 18) {
			return nil, fmt.Errorf("invalid precision %d: must be in range [4, 18]", stream.HLLPrecision)
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
			return nil, fmt.Errorf("__label__ may appear at most once in group_by")
		}
		if count == 1 {
			newGroupBy = append(newGroupBy, labelKeyword)
		}
		stream.GroupBy = newGroupBy
		cfg.Streams[i] = stream
	}

	reservedLabels := map[string]bool{
		"interval":        true,
		"filter":          true,
		"group_by_keys":   true,
		"group_by_values": true,
	}
	es := make([]*estimator, 0, len(cfg.Streams))
	for _, ec := range cfg.Streams {
		for k := range ec.Labels {
			if reservedLabels[k] {
				return nil, fmt.Errorf("static label name %q is reserved and cannot be used in labels", k)
			}
			if strings.HasPrefix(k, "by_") {
				return nil, fmt.Errorf("static label name %q is reserved: labels starting with \"by_\" are reserved and cannot be used in labels", k)
			}
		}
		e, err := newEstimator(ec)
		if err != nil {
			logger.Fatalf("cannot create estimator: %v", err)
		}
		es = append(es, e)
	}

	return es, nil
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
