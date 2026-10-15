package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

func TestLoadConfigUniqueIDs(t *testing.T) {
	f := func(cfg string, wantErr bool) {
		t.Helper()

		path := filepath.Join(t.TempDir(), "streams.yaml")
		if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
			t.Fatalf("cannot write config: %s", err)
		}
		es, err := loadConfig(path)
		for _, e := range es {
			e.stop()
		}
		if (err != nil) != wantErr {
			t.Fatalf("unexpected error: got %v; want error: %v", err, wantErr)
		}
	}

	// different group_by
	f(`
streams:
- group_by: [job]
- group_by: [instance]
`, false)

	// different labels
	f(`
streams:
- labels: {a: "1"}
- labels: {a: "2"}
`, false)

	// same id, only non-id fields differ
	f(`
streams:
- group_by: [job]
  group_limit: 10
- group_by: [job]
  group_limit: 20
`, true)

	// same id, group_by order differs
	f(`
streams:
- group_by: [job, instance]
- group_by: [instance, job]
`, true)

	// same id, default interval equals explicit one
	f(`
streams:
- filter: '{job="a"}'
- filter: '{job="a"}'
  interval: 5m
  min_cardinality: 10
`, true)
}

func TestCompiledFilterMatch(t *testing.T) {
	labels := func(pairs ...string) []protoparser.Label {
		ls := make([]protoparser.Label, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			ls = append(ls, protoparser.Label{Name: pairs[i], Value: pairs[i+1]})
		}
		return ls
	}

	f := func(filter string, ls []protoparser.Label, want bool) {
		t.Helper()
		cf, err := compileFilters(filter)
		if err != nil {
			t.Fatalf("compileFilters(%q): %v", filter, err)
		}
		if got := cf.match(ls); got != want {
			t.Fatalf("matchesFilters(%q): expected %v, got %v", filter, want, got)
		}
	}

	// fast path: no filters always match
	f("", labels(), true)
	f("", labels("job", "foo"), true)

	// equality
	f(`{job="foo"}`, labels("job", "foo"), true)
	f(`{job="foo"}`, labels("job", "bar"), false)
	// absent label has empty value
	f(`{job="foo"}`, labels("env", "prod"), false)
	f(`{job=""}`, labels("env", "prod"), true)

	// negative equality
	f(`{job!="foo"}`, labels("job", "bar"), true)
	f(`{job!="foo"}`, labels("job", "foo"), false)
	// absent label (empty) does not equal "foo" → matches negation
	f(`{job!="foo"}`, labels("env", "prod"), true)
	// absent label (empty) equals "" → negation fails
	f(`{job!=""}`, labels("env", "prod"), false)

	// regexp
	f(`{job=~"api|worker"}`, labels("job", "api"), true)
	f(`{job=~"api|worker"}`, labels("job", "worker"), true)
	f(`{job=~"api|worker"}`, labels("job", "db"), false)
	// partial match is not allowed (anchored)
	f(`{job=~"api"}`, labels("job", "api-v2"), false)

	// negative regexp
	f(`{job!~"api|worker"}`, labels("job", "db"), true)
	f(`{job!~"api|worker"}`, labels("job", "api"), false)
	// absent label has empty value; empty does not match "api|worker" → negation passes
	f(`{job!~"api|worker"}`, labels("env", "prod"), true)

	// multiple filters: all must match
	f(`{job="api",env="prod"}`, labels("job", "api", "env", "prod"), true)
	f(`{job="api",env="prod"}`, labels("job", "api", "env", "dev"), false)
	f(`{job="api",env!~"dev|staging"}`, labels("job", "api", "env", "prod"), true)
	f(`{job="api",env!~"dev|staging"}`, labels("job", "api", "env", "dev"), false)
}

func TestCompileFilters(t *testing.T) {
	f := func(filter string, wantErr bool, want []labelFilter) {
		t.Helper()
		got, err := compileFilters(filter)
		if wantErr {
			if err == nil {
				t.Fatalf("expected error for filter %q, got nil", filter)
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error for filter %q: %v", filter, err)
		}
		if len(got) != len(want) {
			t.Fatalf("filter %q: expected %d filters, got %d: %+v", filter, len(want), len(got), got)
		}
		for i, gf := range got {
			wf := want[i]
			if gf.label != wf.label || gf.value != wf.value || gf.isNegative != wf.isNegative || gf.isRegexp != wf.isRegexp {
				t.Fatalf("filter %q [%d]: expected %+v, got %+v", filter, i, wf, gf)
			}
			if gf.isRegexp && gf.re == nil {
				t.Fatalf("filter %q [%d]: compiled regexp is nil", filter, i)
			}
		}
	}

	// empty → no filters
	f("", false, nil)
	// equality
	f(`{job="foo"}`, false, []labelFilter{{label: "job", value: "foo"}})
	// negative equality
	f(`{job!="foo"}`, false, []labelFilter{{label: "job", value: "foo", isNegative: true}})
	// regexp
	f(`{job=~"foo|bar"}`, false, []labelFilter{{label: "job", value: "foo|bar", isRegexp: true}})
	// negative regexp
	f(`{job!~"foo|bar"}`, false, []labelFilter{{label: "job", value: "foo|bar", isRegexp: true, isNegative: true}})
	// multiple filters
	f(`{job="api",env!~"dev|staging"}`, false, []labelFilter{
		{label: "job", value: "api"},
		{label: "env", value: "dev|staging", isRegexp: true, isNegative: true},
	})
	// invalid regexp → error
	f(`{job=~"[invalid"}`, true, nil)
	// invalid selector → error
	f(`not valid selector(`, true, nil)
	// OR groups → error
	f(`{job="api"} or {job="worker"}`, true, nil)
}
