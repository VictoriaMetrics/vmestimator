# vmestimator

`vmestimator` measures metrics cardinality across arbitrary label dimensions and exposes the results as metrics.

# Why measure cardinality

Imagine you're scraping metrics from dozens of Prometheus targets.
One day, a team deploys a new version of their service with a `trace_id` or `user_id` label. 
Overnight, that job's cardinality explodes from 500 to 500,000 time series.
Suddenly, VictoriaMetrics consumes 100x more memory and disk. 
Ingestion slows down, storage struggles to keep up, and in the worst case becomes unavailable. 

By the time someone gets paged, the damage is already done: indexes are bloated, caches are oversized, and observability across the entire system is affected.

`vmestimator` continuously tracks cardinality and exposes the results as metrics. 
This allows you to alert on cardinality spikes within minutes and identify the offending job directly from the alert. 
Instead of discovering the problem after it impacts your infrastructure, you can react before it becomes an outage.

Per-job cardinality tracking is the most actionable use case, but it's far from the only one. 
`vmestimator` can measure cardinality across arbitrary label dimensions,
enabling use cases such as tenant-level cardinality monitoring, usage analysis, and more granular billing.

## Design

We recommend deploying `vmestimator` close to the metrics source, ideally alongside `vmagent` instances that scrape targets. 
Each `vmagent` mirrors all ingested metrics into the estimator.

To reduce overhead, persistent queueing and metadata ingestion can be disabled for the estimator remote write path. 
It is safe to send metrics from multiple independent `vmagent` instances into a single `vmestimator`.

Example configuration:
```bash
/path/to/vmagent \
  -remoteWrite.url=http://vmsingle:8428/api/v1/write \
  -remoteWrite.url=http://vmestimator:8490/cardinality/api/v1/write \
  -remoteWrite.disableOnDiskQueue=false,true \
  -remoteWrite.disableMetadata=false,true
```

The next step is to expose cardinality estimates as metrics. 
For this, `vmagent` should scrape the `vmestimator` `/metrics` endpoint and forward those metrics to a `vmsingle` instance (or another VictoriaMetrics storage).

TODO: image simple setup

This setup is straightforward and introduces minimal overhead. 
The main drawback is that cardinality data shares the same storage backend as the rest of your observability stack. 
If that storage becomes unavailable, you also lose visibility into cardinality—precisely when it may be most needed. 

To mitigate this, we recommend running a separate `vmsingle` instance dedicated to scraping and storing VictoriaMetrics-related monitoring signals only. 
This pattern is commonly referred to as a monitor-of-monitors (MoM) setup. 
In this architecture, `vmestimator` metrics are isolated from production observability storage, 
ensuring cardinality visibility remains available even during incidents affecting the primary monitoring system.

The resulting topology looks like this:

TODO: image setup with mom

## Configuration

Running:
```
go run ./app/vmestimator/... -config=streams.yaml -httpListenAddr=:8490
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
- `buckets` (optional): number of internal shards for parallel ingestion; defaults to `min(64, 2*availableCPUs)`
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

This behavior is controlled by the following flags:
- `-cardinalityMetrics.cacheTTL` (default `30s`): how long to cache the cardinality metrics response before recomputing it

The HTTP endpoint is controlled by the `-cardinalityMetrics.exposeAt` flag:
- `-cardinalityMetrics.exposeAt=/metrics` (default): cardinality metrics merged with regular metrics at `/metrics`
- `-cardinalityMetrics.exposeAt=/cardinality/metrics`: only cardinality metrics exposed at that path
- `-cardinalityMetrics.exposeAt=`: cardinality metrics not exposed via HTTP

All metrics include `interval`, `group_by_keys`, and `group_by_values` labels. Extra labels from the `labels` config field are inserted between `interval` and `group_by_keys` (sorted alphabetically).

**Without grouping** (`group_by_keys` is `__global__` and `group_by_values` is not set):
```
cardinality_estimate{interval="1h0m0s",group_by_keys="__global__"} 142300
```

**With grouping** — one summary line (total distinct group count) plus one line per distinct label value combination. Each per-group line also includes individual `by_{key}="{val}"` labels for each group key:
```
cardinality_estimate{interval="5m0s",group_by_keys="__group__",group_by_values="instance,job"} 2
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host1:9090,prometheus",by_instance="host1:9090",by_job="prometheus"} 312
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host2:9100,node",by_instance="host2:9100",by_job="node"} 87
```

**With extra labels:**
```
cardinality_estimate{interval="5m0s",env="production",region="eu-central-1",group_by_keys="job",group_by_values="prometheus",by_job="prometheus"} 312
```

## Operational metrics

When grouping is enabled, vmestimator exposes per-bucket operational metrics at `/metrics`:

- `vmestimator_estimator_group_size{group_by_keys, bucket}` — number of active groups in this bucket after the last rotation
- `vmestimator_estimator_group_rejected_size{group_by_keys}` — estimated number of distinct group values rejected since the last rotation because `group_limit` was reached
- `vmestimator_estimator_group_limit{group_by_keys, bucket}` — configured `group_limit` for this bucket

## Cluster

`vmestimator` can be run as a cluster for high availability or when CPU per instance becomes a limiting factor.

In this mode instances are split into two roles: **storages** that receive writes, and **selectors** that read from storages and expose the merged result.

**Storage nodes** — receive Prometheus remote write and serve snapshots:
```
vmestimator -config=streams.yaml -httpListenAddr=:8491 -cardinalityMetrics.exposeAt=/cardinality/metrics
vmestimator -config=streams.yaml -httpListenAddr=:8492 -cardinalityMetrics.exposeAt=/cardinality/metrics
vmestimator -config=streams.yaml -httpListenAddr=:8493 -cardinalityMetrics.exposeAt=/cardinality/metrics
```

Setting `-cardinalityMetrics.exposeAt=/cardinality/metrics` keeps cardinality estimates off the default `/metrics` path. This way `/metrics` on a storage node returns only its own operational metrics, while `/cardinality/metrics` gives you the storage's local cardinality estimates if you need to inspect or debug a specific node.

**Selector nodes** — query all storage nodes, merge HyperLogLog sketches, and expose consolidated cardinality estimates:
```
vmestimator -storageNode=http://vmestimator-storage-1:8491 \
            -storageNode=http://vmestimator-storage-2:8492 \
            -storageNode=http://vmestimator-storage-3:8493 \
            -httpListenAddr=:8490
```

When `-storageNode` flags are provided and no `-config` is specified, the selector runs without local estimators and only merges remote data.

## Dashboard

There are Grafana dashboards available in `dashboards` directory:

<img width="1512" height="862" alt="Screenshot 2026-04-23 at 09 47 38" src="https://github.com/user-attachments/assets/2bd6a930-1eb5-40ef-8006-8196c1c12397" />


## Benchmarks

```
$ go test ./... -run=none -bench=.
?       github.com/makasim/vmestimator/app/cegen [no test files]
goos: darwin
goarch: arm64
pkg: github.com/makasim/vmestimator/app/vmestimator
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
ok      github.com/makasim/vmestimator/app/vmestimator     17.104s
goos: darwin
goarch: arm64
pkg: github.com/makasim/vmestimator/app/vmestimator/protoparser
cpu: Apple M1 Pro
BenchmarkStreamParse-10               96          12052191 ns/op         162.92 MB/s      225972 B/op          6 allocs/op
PASS
ok      github.com/makasim/vmestimator/app/vmestimator/protoparser 1.482s
```