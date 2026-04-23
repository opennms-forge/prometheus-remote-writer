# Changelog

All notable changes to this project are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] — TBD

### Breaking changes

#### `labels.rename` targets validated at startup

Entries whose `to` value collides with a default-allowlist label name or a
reserved prefix cause the plugin to refuse to start with an actionable error
message. Duplicate rename targets across entries (`a -> cluster, b -> cluster`)
and duplicate `from` keys (`a -> cluster, a -> tenant` — the second silently
overwrote the first before) are likewise rejected. Multiple rename errors are
accumulated into one startup error so operators fix-once, restart-once.
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

#### `labels.include = *` no longer re-emits consumed default source keys

Operators running `labels.include = *` on v0.1 received five duplicate labels
whose snake-cased source-key forms did not collide with a default label name,
and therefore slipped past the v0.1 `putIfAbsent`-based dedup. These
duplicates are no longer emitted in v0.2.

Migration table:

| Deprecated label | Use instead                | Why                                                                                                                   |
|---               |---                         |---                                                                                                                    |
| `name`           | `__name__`                 | Prometheus-native metric name.                                                                                        |
| `resource_id`    | `resourceId`               | Raw OpenNMS resource identifier.                                                                                      |
| `if_high_speed`  | `if_speed`                 | Normalized bits-per-second: `ifHighSpeed × 10⁶` when non-zero, else `ifSpeed`.                                        |
| `node_id`        | `node`                     | FS-qualified identity when available, else the numeric dbId. Keep the old name via `labels.rename = node -> node_id`. |
| `categories`     | `onms_cat_<name>` per cat  | Per-category expansion, not a single comma-separated label.                                                           |

The six other snake-cased source-key forms (`foreign_source`, `foreign_id`,
`node_label`, `location`, `if_name`, `if_descr`) collided on label name with a
default and were already single-valued in v0.1 via `putIfAbsent` — they are
unchanged in v0.2.

Deployments running narrow `labels.include` patterns (e.g. `sys*, asset*`)
are unaffected. Pre-upgrade, scan your dashboards and alert rules for the
five deprecated label names.

### Added

- **Slash-path `resourceId` grammars** — `ResourceIdParser` now recognises
  `snmp/fs/<foreignSource>/<foreignId>/<group>/<instance…>` and
  `snmp/<dbNodeId>/<group>/<instance…>` in addition to the existing bracketed
  form. Self-monitor, JMX, and legacy-path samples now acquire `node`,
  `resource_type`, and `resource_instance` labels automatically. The instance
  segment is greedy, so MBean object names with embedded separators stay
  intact. Bracketed-form matching is unchanged — no previously parseable
  resourceId changes shape.
- **`instance.id` config and `onms_instance_id` label** — stamps every
  outbound sample with an operator-supplied identifier so multiple OpenNMS
  instances writing into the same Prometheus-compatible backend can be
  distinguished and aggregated in PromQL. Works against every
  Prometheus-compatible backend (Prometheus, Mimir, Cortex, VictoriaMetrics,
  Thanos). Orthogonal to `tenant.org-id` (backend tenant isolation); use
  either, both, or neither depending on deployment shape. See the README
  section "Identifying samples from multiple OpenNMS instances" for the
  decision table.
- **Startup WARN when `instance.id` is unset** — one-shot, informational.
  Fires once per bundle lifecycle; not repeated by hot-reload cycles.
  Single-instance deployments can ignore or silence by setting the knob.
- **README: "Label enrichment is two-sided"** — documents the OpenNMS-side
  `org.opennms.timeseries.tin.metatags.tag.*` prerequisite with a minimal
  four-property example, a pointer at the sandbox comprehensive example, the
  `exposeCategories` opt-in flag, and the node-record-must-exist caveat.

### Known limitations

These are deliberate omissions for v0.2, surfaced so operators can plan around
them:

- **Remote Write v2** — stable since Prometheus 2.50 but not yet universally
  deployed across Mimir / VictoriaMetrics / Cortex / Thanos Receive. Adopting
  v2 before universal backend support would fork the compatibility matrix.
  Workaround: v1 (what this plugin ships) is universally supported. Tracked
  for a future release.
- **Durable on-disk write buffer (WAL)** — v0.2 still uses a bounded
  in-memory queue and drops samples on overflow, counted via
  `samples_dropped_queue_full_total`. Workaround: alert on that counter and
  size `queue.capacity` for your peak ingest rate. Tracked for a future
  release.
- **Per-tenant routing / multi-destination fan-out** — one `write.url` and
  one `tenant.org-id` per plugin instance. Workaround: run one OpenNMS
  instance per destination. Tracked for a future release.

### Upgrade notes

- **`instance.id` path is purely additive.** Deployments that do not set
  `instance.id` emit identical labels to v0.1 and gain exactly one new `WARN`
  line on startup. Deployments using `tenant.org-id` alone continue unchanged;
  setting `instance.id` is an additive migration that unlocks cross-instance
  PromQL without disturbing tenant isolation.
- **Slash-path enrichment is additive.** Self-monitor, JMX, and legacy-path
  samples that previously emitted only `{resourceId="snmp/…"}` now also emit
  `node`, `resource_type`, and `resource_instance`. No previously parseable
  resourceId changes shape.
- **`labels.include = *` deployments**: audit dashboards and alert rules for
  the five deprecated label names above; migrate before upgrade.

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

[Unreleased]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/releases/tag/v0.1.0
