# vmestimator

`vmestimator` measures metrics cardinality across arbitrary label dimensions and exposes the results as metrics.

# Why measure ?

Consider a setup where metrics are scraped from dozens of Prometheus targets.
One day, a team deploys a new version of their service with a `trace_id` or `user_id` label. 
Overnight, that job's cardinality explodes from 500 to 500,000 time series.
Suddenly, VictoriaMetrics consumes 100x more memory and disk. 
Ingestion slows down, storage struggles to keep up, and in the worst case becomes unavailable. 

By the time someone gets paged, the damage is already done: indexes are bloated, caches are oversized, and observability across the entire system is affected.

`vmestimator` continuously tracks cardinality and exposes the estimation results as [metrics](https://github.com/VictoriaMetrics/vmestimator/blob/main/README.md#cardinality-metrics).
This allows alerting on cardinality spikes within minutes and identifying the offending job directly from the alert.
Instead of discovering the problem after it impacts the infrastructure, it becomes possible to react before it turns into an outage.

Per-job cardinality tracking is the most actionable use case, but it’s not the only one.
`vmestimator` can measure cardinality across arbitrary label dimensions, 
enabling use cases such as per-tenant usage analysis, long-term trend tracking, and capacity planning.

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
For this, `vmagent` should scrape the estimator `/metrics` endpoint and forward those metrics to a `vmsingle` instance (or another VictoriaMetrics storage).

<img width="2413" height="1189" alt="image" src="https://github.com/user-attachments/assets/e52d9210-b6f9-457b-8d8f-1d6ff6ba1416" />


This setup is straightforward and introduces minimal overhead. 
The main drawback is that cardinality data shares the same storage with production metrics. 
If that storage becomes unavailable, the visibility into cardinality is lost precisely when it may be most needed. 

To mitigate this, we recommend running a separate `vmsingle` instance dedicated to scraping and storing VictoriaMetrics-related monitoring signals only. 
This pattern is commonly referred to as a monitoring-of-monitoring (MoM) setup. 
In this architecture, `vmestimator` metrics are isolated from production observability storage, 
ensuring cardinality visibility remains available even during incidents affecting the primary monitoring system.

The resulting topology looks like this:
<img width="2413" height="1189" alt="image" src="https://github.com/user-attachments/assets/e2ca4a69-e931-47a1-9d91-99749382d4a9" />

## Configuration

To run vmestimator a `streams.yaml` config has to be provided:

```bash
/path/to/vmestimator -config=streams.yaml # -httpListenAddr=:8490
```

Config reference:
```yaml
streams:
  -
    # The measurement window: how long unique series are retained before the HLL sketch resets.
    # Increases are always reflected immediately. Interval only controls how fast the estimate
    # drops after previously seen series disappear.
    #
    # Short interval (e.g. '1m'): estimate clears quickly, so a resolved spike is visible fast.
    #   Use for alerting on transient cardinality bursts.
    # Long interval (e.g. '24h'): unique values accumulate across the full window.
    #   Use for measuring peak or cumulative cardinality over a day.
    #
    # default: 5m
    interval: '5m'

    # Label names used to split the cardinality estimate into per-combination groups.
    # Each distinct combination of values for these labels gets its own estimate metric.
    # Omit entirely for a single global estimate across all series.
    # Examples:
    #  - ["job"]
    #  - ["__name__"] 
    #  - ["vm_account_id","vm_project_id"]
    #
    # default: none (single global estimate)
    group_by: ['job']

    # Maximum number of distinct groups (HLL sketches) to track.
    # Once the limit is reached, excess groups are counted in a single shared "rejected" sketch
    # rather than getting their own entry. Acts as a memory cap and a safeguard against OOM
    # when the group_by label values grow unboundedly.
    # Memory upper bound per stream: 
    #   group_limit * 2^hll_precision bytes. 
    #
    # default: 10000
    group_limit: 10000

    # Number of shards used to reduce lock contention during parallel ingestion.
    # Slightly increases memory for global streams (no group_by); negligible otherwise.
    # Leave at the default unless you have profiled lock contention or have a specific reason to change it.
    #
    # default: min(64, 2*availableCPUs)
    buckets: 64

    # HyperLogLog precision p, in range [4..18].
    # Determines the number of registers m = 2^p and the relative error 1.04 / sqrt(m):
    #   p=14 → m=16 384, error ~0.81%, memory ~16 KB per sketch  (default, suits most cases)
    #   p=18 → m=262 144, error ~0.20%, memory ~256 KB per sketch (billing-grade accuracy)
    #   p=10 → m=1 024,   error ~3.25%, memory ~1 KB per sketch   (thousands of groups, memory-tight)
    # See more in https://research.google.com/pubs/archive/40671.pdf
    #
    # default: 14
    hll_precision: 14

    # Whether to use the sparse HyperLogLog representation for low-cardinality groups.
    # Sparse mode uses far less memory until a group's cardinality reaches ~2^(p-1),
    # at which point it automatically promotes to the dense representation.
    # See more in # See more in https://research.google.com/pubs/archive/40671.pdf
    #
    # default: true
    hll_sparse: true

    # Static labels attached to every output metric produced by this stream entry.
    # Useful when multiple vmestimator instances feed the same storage and you need
    # to distinguish their estimates in dashboards and alerts.
    labels:
      env: 'production'
      region: 'eu-central-1'
```

## Cardinality Metrics

Cardinality estimates are exposed as the `cardinality_estimate` metric.
All metrics include `interval`, `group_by_keys`, `group_by_values`, and any static labels defined in the stream config.

For global estimates (no `group_by` configured), `group_by_keys` is `__global__` and `group_by_values` is omitted:
```
cardinality_estimate{interval="1h0m0s",group_by_keys="__global__"} 142300
```

For grouped estimates, one summary line shows the total number of distinct groups `group_by_keys="__group__"`, followed by one line per distinct label value combination.
Each per-group line also includes individual `by_{key}="{val}"` labels:
```
cardinality_estimate{interval="5m0s",group_by_keys="__group__",group_by_values="instance,job"} 2
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host1:9090,prometheus",by_instance="host1:9090",by_job="prometheus"} 312
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host2:9100,node",by_instance="host2:9100",by_job="node"} 87
```

Note: the total distinct group count in the summary line may exceed the number of per-group lines when `group_limit` is reached 
and excess groups are counted in a single shared "rejected" sketch rather than getting their own entry.

By default, cardinality estimates are merged with the estimator's operational metrics and exposed at `/metrics`.
This is controlled by the `-cardinalityMetrics.exposeAt` flag:
- `-cardinalityMetrics.exposeAt=/metrics` (default): cardinality metrics merged with operational metrics at `/metrics`
- `-cardinalityMetrics.exposeAt=/cardinality/metrics`: cardinality metrics exposed at separate path
- `-cardinalityMetrics.exposeAt=`: cardinality metrics not exposed via HTTP

Computing cardinality estimates is expensive, so results are cached. 
Cache duration is controlled by `-cardinalityMetrics.cacheTTL` (default: `30s`). 
Set to `0` to disable caching entirely.

## Use cases

TODO

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


