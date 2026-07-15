---
build:
  list: never
  publishResources: false
  render: never
sitemap:
  disable: true
---

The following `tip` changes can be tested by building vmestimator from the latest commits according to the following docs:

* [How to build vmestimator](https://docs.victoriametrics.com/victoriametrics/vmestimator/#how-to-build-from-sources)

Metrics of the latest version of vmestimator cluster are available for viewing at our
[sandbox](https://play-grafana.victoriametrics.com/d/mkv22l4).

## tip

* FEATURE: [alerts](https://github.com/VictoriaMetrics/vmestimator/blob/main/deployment/docker/rules/alerts-cardinality.yml): add `GlobalChurnTooHigh` alert to detect when global series churn rate exceeds 10%. See [#13](https://github.com/VictoriaMetrics/vmestimator/pull/13).
* FEATURE: [alerts](https://github.com/VictoriaMetrics/vmestimator/blob/main/deployment/docker/rules/alerts-cardinality.yml): update `JobChurnTooHigh` alert (previously `JobTooHighChurnRate`) to also detect rapid cardinality growth. The threshold was raised from 10% to 20% and the `for` window extended from 15m to 30m to reduce false positives. See [#13](https://github.com/VictoriaMetrics/vmestimator/pull/13).
* FEATURE: [alerts](https://github.com/VictoriaMetrics/vmestimator/blob/main/deployment/docker/rules/alerts-cardinality.yml): replace the static-threshold `JobTooHighCardinality` alert with adaptive 3-sigma anomaly detection alerts `GlobalCardinalityTooHigh` and `JobCardinalityTooHigh`. See [#17](https://github.com/VictoriaMetrics/vmestimator/pull/17).

* BUGFIX: [vmestimator](https://docs.victoriametrics.com/victoriametrics/vmestimator/): add `interval` label to `vmestimator_estimator_insert_total` metric in order to avoid exposing duplicate series when multiple streams share the same `group_by` but use different intervals. The insert-rate panel in the bundled Grafana dashboard now deduplicates per-interval series with `max without(interval)` before summing the remaining series, so the rate is not multiplied by the number of per-interval streams. See [#20](https://github.com/VictoriaMetrics/vmestimator/issues/20).
* BUGFIX: [vmestimator](https://docs.victoriametrics.com/victoriametrics/vmestimator/): fix inaccurate cardinality estimates when the number of unique series was in the upper ~10% of the sparse mode. Estimates in this region could be off due to premature transition to a less precise (dense) counting mode. See [hyperloglog#d44d606f](https://github.com/makasim/hyperloglog/commit/d44d606f7e8bdd78d2b56c27fc6fe82f3981d4a6).

## [v0.1.7](https://github.com/VictoriaMetrics/vmestimator/releases/tag/v0.1.7)

Released at 2026-07-09

* FEATURE: [vmestimator](https://docs.victoriametrics.com/victoriametrics/vmestimator/): randomize estimator iteration order to reduce lock contention. See [#15](https://github.com/VictoriaMetrics/vmestimator/pull/15).

## [v0.1.6](https://github.com/VictoriaMetrics/vmestimator/releases/tag/v0.1.6)

Released at 2026-07-07

* FEATURE: [vmestimator](https://docs.victoriametrics.com/victoriametrics/vmestimator/): initial release