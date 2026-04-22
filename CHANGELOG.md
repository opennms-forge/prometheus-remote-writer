# Changelog

All notable changes to this project are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (BREAKING)

- **`labels.rename` targets are now validated at startup.** Entries whose
  `to` value collides with a default-allowlist label name or a reserved prefix
  cause the plugin to refuse to start with an actionable error message.
  Duplicate rename targets across entries (`a -> cluster, b -> cluster`) and
  duplicate `from` keys (`a -> cluster, a -> tenant` — the second silently
  overwrote the first before) are likewise rejected. Multiple rename errors
  are accumulated into one startup error so operators fix-once, restart-once.
  Previously these configs were accepted and silently clobbered the colliding
  default at flush time — a data-quality incident with no visibility surface.

  Reserved exact targets: `__name__`, `resourceId`, `node`, `foreign_source`,
  `foreign_id`, `node_label`, `location`, `resource_type`, `resource_instance`,
  `if_name`, `if_descr`, `if_speed`, `onms_instance_id`.
  Reserved prefixes: `onms_cat_*`, `onms_meta_*`.

  The `onms_instance_id` name is reserved unconditionally, even when
  `instance.id` is unset, so a subsequent hot-reload enabling the knob cannot
  unmask an already-broken rename.

  Error message shape:

  ```
  labels.rename target 'foreign_source' collides with the default label
  'foreign_source'. The plugin already emits this label; renaming onto it
  would silently clobber the default value. Pick a different 'to' name.
  ```

  Migration: pick a non-reserved `to` name. Pre-upgrade, scan your cfg
  (swap the etc path for your install's actual location):

  ```bash
  grep -E '^[[:space:]]*labels\.rename' /opt/opennms/etc/org.opennms.plugins.tss.prometheusremotewriter.cfg
  ```

### Added

- **`instance.id` config and `onms_instance_id` label** — stamps every
  outbound sample with an operator-supplied identifier so multiple OpenNMS
  instances writing into the same Prometheus-compatible backend can be
  distinguished and aggregated in PromQL. Works against every
  Prometheus-compatible backend (Prometheus, Mimir, Cortex,
  VictoriaMetrics, Thanos). Orthogonal to `tenant.org-id` (backend tenant
  isolation); use either, both, or neither depending on deployment shape.
  See the new README section "Identifying samples from multiple OpenNMS
  instances" for the decision table.
- **Startup WARN when `instance.id` is unset** — one-shot, informational.
  Fires once per bundle lifecycle; not repeated by hot-reload cycles.
  Single-instance deployments can ignore or silence by setting the knob.

### Upgrade notes

- Purely additive. Deployments that do not set `instance.id` emit
  identical labels to v0.1 and gain exactly one new `WARN` line on
  startup.
- Deployments already using `tenant.org-id` alone continue to work
  unchanged; setting `instance.id` is an additive migration that unlocks
  cross-instance PromQL without disturbing tenant isolation.

## [0.1.0] — 2026-04-22

First release. Implements the OpenNMS `TimeSeriesStorage` SPI against any
Prometheus-compatible Remote Write v1 endpoint.

### Highlights

- **Backend-agnostic Remote Write v1** — ships against Prometheus, Grafana
  Mimir, VictoriaMetrics, Cortex, and Thanos Receive. Snappy + protobuf on
  write, the Prometheus HTTP query API on read.
- **OpenNMS-native label model** — resource context surfaced as first-class
  Prometheus labels (`node`, `node_label`, `location`, `resource_type`,
  `if_name`, `if_speed`, `onms_cat_*`, …) with operator-configurable
  include/exclude/rename policy.
- **Production-grade operation** — Basic/Bearer/`X-Scope-OrgID` auth, custom
  TLS CA bundles, bounded in-memory queue with batched flush and 5xx
  exponential backoff, backpressure-aware enqueue, Dropwizard self-metrics,
  and graceful drain on shutdown.

### Compatibility

- OpenNMS Horizon 35
- Java 17
- OpenNMS Integration API v2.0 (Apache 2.0)
- Prometheus Remote Write v1 (`prom/prometheus`, Grafana Mimir,
  VictoriaMetrics, Cortex, Thanos Receive)

### Known deviations

- **Partition-lossy read path.** Prometheus does not model the OpenNMS
  intrinsic/meta/external tag partition. On read, every non-intrinsic label
  is attached as a meta tag; the original partition is not preserved.
- **`shouldDeleteMetrics` and partition-equality tests** from the OpenNMS
  TSS compliance suite are skipped — see `PrometheusComplianceIT` for
  per-test rationale.

### License

Apache License 2.0. Clean-room implementation against public specifications;
no code lifted from the AGPL `opennms-cortex-tss-plugin`. Reference sources
are the Prometheus Remote Write spec, the Prometheus HTTP query API docs,
the upstream Prometheus protobuf definitions, and the Apache-2.0 Prometheus
Go sanitization rules.

### Full changes

- Time Series Storage plugin that writes samples via Prometheus Remote Write v1
  (snappy + protobuf) and reads via the Prometheus HTTP query API.
- Opinionated default label allowlist surfacing OpenNMS resource context as
  native Prometheus labels: `__name__`, `resourceId`, `node`, `node_label`,
  `foreign_source`, `foreign_id`, `location`, `resource_type`,
  `resource_instance`, `if_name`, `if_descr`, `if_speed`, and one
  `onms_cat_<name>` label per surveillance category.
- `resourceId` parsing: kept raw and parsed into structured
  `node` / `resource_type` / `resource_instance` labels; parse failure falls
  back to raw-only emission.
- `if_speed` normalization: `ifHighSpeed × 1_000_000` preferred, falling back
  to `ifSpeed` when zero or absent. Produces a single bits-per-second label.
- Operator-configurable label policy: `labels.include` (add non-default
  source tags), `labels.exclude` (remove defaults), `labels.rename`
  (`from -> to`).
- OpenNMS metadata passthrough — **off by default**. Opt in with
  `metadata.enabled = true` and `metadata.include` globs. Built-in credential
  denylist (`*password*`, `*secret*`, `*token*`, `*key*`, `snmp-community`)
  is always applied even when the operator's globs would match. Labels
  namespaced under `onms_meta_<context>_<key>`.
- Authentication: Basic, Bearer (mutually exclusive), and `X-Scope-OrgID`
  multi-tenant header.
- TLS: JDK truststore by default; custom PEM CA bundle via `tls.ca-file`;
  insecure skip-verify available behind an explicit opt-in with hourly
  WARN log.
- Write pipeline: bounded in-memory queue, batch flush on size or interval,
  5xx exponential-backoff retry, 4xx drop with body captured to WARN,
  transport errors treated as 5xx for accounting.
- Backpressure: queue overflow throws `StorageException` so OpenNMS sees the
  failure and increments `samples_dropped_queue_full_total`.
- Read path: `findMetrics` → `GET /api/v1/series`, `getTimeSeriesData` →
  `GET /api/v1/query_range`. Step derivation from request range when
  `getStep()` is unset; clamped to Prometheus's 11 000 points-per-query
  ceiling.
- Delete path: `delete(Metric)` is a no-op (Remote Write has no delete
  semantic) that counts via `delete_noop_total` and logs a rate-limited
  WARN once per minute.
- Self-metrics via a Dropwizard `MetricRegistry`; `opennms:prometheus-writer-stats`
  Karaf shell command prints a stable name/value table.
- Graceful shutdown: stops accepting new enqueues, drains the queue within
  `shutdown.grace-period-ms`, then terminates in-flight HTTP calls. Residual
  queue depth is logged at WARN.
- Karaf feature `prometheus-remote-writer` shipping a pre-populated
  `etc/org.opennms.plugins.tss.prometheusremotewriter.cfg` on install.

[Unreleased]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/releases/tag/v0.1.0
