# cestorage

`cestorage` is a cardinality estimator that receives Prometheus remote write streams
and exposes approximate time series cardinality as metrics (TODO: support remote write).

It is useful for tracking how many unique time series are flowing through across all metrics, metric name, or broken down by specific labels.

## Goal

The goal of this project is to evaluate whether cardinality estimation can provide practical value within VictoriaMetrics and justify its inclusion in the core system (see [proposal](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10848)).

## How it works

Running:
```
go run ./app/cestorage/... -config=streams.yaml -httpListenAddr=:8490
```

Configuration:

```yaml
streams:
  # Track total cardinality with no grouping.
  - interval: '1h'

  # Track cardinality grouped by metric name.
  - interval: '1h'
    group_by: ["__name__"]

  # Track cardinality grouped by job label.
  - interval: '1m'
    group_by: ["job"]

  # Track cardinality grouped by tenant info
  - group_by: ["vm_account_id", "vm_project_id"]

  # Track cardinality of jobs, with extra labels on the output metrics.
  - group_by: ["job"]
    labels:
      region: 'eu-central-1'
      env: 'production'
```

Fields:
- `group_by` (optional): list of label names to split cardinality by; each distinct combination gets its own estimate
- `group_limit` (optional): maximum number of distinct groups to track; excess groups are counted in a rejected sketch but not individually; defaults to `10000`
- `buckets` (optional): number of internal shards for parallel ingestion; defaults to `min(20, availableCPUs)`
- `labels` (optional): extra labels attached to all output metrics for this estimator
- `interval` (optional): how often to rotate (reset) counters; defaults to `5m`
- `hll_precision` (optional): HyperLogLog precision, must be in range `[4, 18]`; higher values yield more accurate estimates at the cost of more memory; defaults to `14`
- `hll_sparse` (optional): whether to use sparse HyperLogLog representation, which reduces memory for low-cardinality groups; defaults to `true`

Cardinality generator:

```
go run ./app/cegen/main.go -cardI=100 -cardY=20 -template="foo{instance=\"127.0.0.[cardI]\",job=\"ametric[cardY]\"}"
```


## Metrics

By default, cardinality estimates are merged with regular metrics and exposed at `/metrics`.

This behavior is controlled by the `-cardinalityMetrics.exposeAt` flag:
- `-cardinalityMetrics.exposeAt=/metrics` (default): cardinality metrics merged with regular metrics at `/metrics`
- `-cardinalityMetrics.exposeAt=/cardinality/metrics`: only cardinality metrics exposed at that path
- `-cardinalityMetrics.exposeAt=`: cardinality metrics not exposed via HTTP

All metrics include `interval`, `group_by_keys`, and `group_by_values` labels. Extra labels from the `labels` config field are inserted between `interval` and `group_by_keys` (sorted alphabetically).

**Without grouping** (`group_by_keys` is `__global__` and `group_by_values` is not set):
```
cardinality_estimate{interval="1h0m0s",group_by_keys="__global__"} 142300
```

**With grouping** — one summary line (total distinct group count) plus one line per distinct label value combination:
```
cardinality_estimate{interval="5m0s",group_by_keys="__group__",group_by_values="instance,job"} 2
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host1:9090,prometheus"} 312
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host2:9100,node"} 87
```

**With extra labels:**
```
cardinality_estimate{interval="5m0s",env="production",region="eu-central-1",group_by_keys="job",group_by_values="prometheus"} 312
```

## Operational metrics

When grouping is enabled, cestorage exposes per-bucket operational metrics at `/metrics`:

- `cestorage_group_estimator_size{groupBy, bucket}` — number of active groups in this bucket after the last rotation
- `cestorage_group_estimator_rejected_size{groupBy, bucket}` — estimated number of distinct group values rejected since the last rotation because `group_limit` was reached
- `cestorage_group_limit{groupBy, bucket}` — configured `group_limit` for this bucket

## Dashboard

There are Grafana dashboards available in `dashboards` directory:

<img width="1512" height="862" alt="Screenshot 2026-04-23 at 09 47 38" src="https://github.com/user-attachments/assets/2bd6a930-1eb5-40ef-8006-8196c1c12397" />


## Benchmarks

```
$ go test ./... -run=none -bench=.
?       github.com/makasim/cestimator/app/cegen [no test files]
goos: darwin
goarch: arm64
pkg: github.com/makasim/cestimator/app/cestorage
cpu: Apple M1 Pro
BenchmarkEstimator_WriteMetrics/NoGroup/NoPrev-10                 937376              1265 ns/op            1504 B/op         12 allocs/op
BenchmarkEstimator_WriteMetrics/NoGroup/WithPrev-10               625159              1843 ns/op            1504 B/op         12 allocs/op
BenchmarkEstimator_WriteMetrics/Group100/NoPrev-10                 56973             21076 ns/op            3745 B/op         81 allocs/op
BenchmarkEstimator_WriteMetrics/Group100/WithPrev-10               43438             27834 ns/op            3745 B/op         81 allocs/op
BenchmarkEstimator_WriteMetrics/Group10k/NoPrev-10                   807           1530942 ns/op            3106 B/op         71 allocs/op
BenchmarkEstimator_WriteMetrics/Group10k/WithPrev-10                 580           2060489 ns/op            3107 B/op         71 allocs/op
BenchmarkEstimator_InsertManyParallel/NoGroup-10                15398458                78.11 ns/op            0 B/op          0 allocs/op
BenchmarkEstimator_InsertManyParallel/Group100-10               14786208                82.26 ns/op           15 B/op          1 allocs/op
BenchmarkEstimator_InsertManyParallel/Group10k-10               13931193                84.10 ns/op           24 B/op          2 allocs/op
BenchmarkEstimator_InsertManyParallel/Group100k-10               7087110               174.6 ns/op            24 B/op          3 allocs/op
BenchmarkParse_EstimatorGlobal-10                                   2656            476446 ns/op           18224 B/op         26 allocs/op
BenchmarkParse_EstimatorGroup-10                                    4430            259190 ns/op             129 B/op          6 allocs/op
PASS
ok      github.com/makasim/cestimator/app/cestorage     17.104s
goos: darwin
goarch: arm64
pkg: github.com/makasim/cestimator/app/cestorage/protoparser
cpu: Apple M1 Pro
BenchmarkStreamParse-10               96          12052191 ns/op         162.92 MB/s      225972 B/op          6 allocs/op
PASS
ok      github.com/makasim/cestimator/app/cestorage/protoparser 1.482s
```