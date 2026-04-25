# Changelog

All notable changes to this project are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-04-25

### Added

- **Write-Ahead Log (WAL)** — optional, opt-in via `wal.enabled=false`
  (default). When enabled, the plugin durably persists every mapped
  sample to disk before ack'ing `store()`; the WAL replaces the
  in-memory `ArrayBlockingQueue` as source of truth, so samples survive
  process restart AND extended endpoint outages. Six new config keys:
  - `wal.enabled` (bool, default `false`)
  - `wal.path` (default `${karaf.data}/prometheus-remote-writer/wal`)
  - `wal.max-size-bytes` (default 512 MB — total footprint cap)
  - `wal.segment-size-bytes` (default 64 MB — per-segment rotation)
  - `wal.fsync` (`always | batch | never`, default `batch` — fsync at
    flush-interval boundary)
  - `wal.overflow` (`backpressure | drop-oldest`, default `backpressure`
    — matches v0.4 queue-full semantics)

  On-disk format: length-prefixed protobuf `WalEntry` frames with
  CRC32C; rotating segment files (`00000000000000000000.seg`, ...);
  companion `.idx` jsonl per segment; `checkpoint.json` tracks the
  last offset confirmed shipped. Crash recovery scans the newest
  segment, truncates any torn tail, and replays from the checkpoint.
  See README "Write-Ahead Log" for operator guidance.

- **Eight new metrics** (wal.enabled=true only), surfaced via
  `opennms:prometheus-writer-stats`:
  - Counters: `wal_bytes_written_total`,
    `wal_bytes_checkpointed_total` (bytes that moved past the
    durable checkpoint), `wal_replay_samples_total` (one-shot
    at startup), `wal_batches_dropped_4xx_total`,
    `samples_dropped_wal_full_total`,
    `wal_frames_dropped_corrupted_total` (sealed-segment
    skips due to bit rot / torn frames)
  - Gauges: `wal_disk_usage_bytes`, `wal_segments_active`

- **`samples_unparseable_resource_id_total` counter** — increments once per
  sample whose `resourceId` tag failed all parser grammars (bracketed,
  slash-FS, slash-DB) or was absent. Surfaced via
  `opennms:prometheus-writer-stats` alongside the existing `samples_*_total`
  counters. Lets operators see the v0.4 "catch-all `job=opennms`" bucket
  size without grepping logs — a non-zero rate signals config drift, a new
  resourceId shape, or unexpected upstream input.

### Changed

- **`shutdown.grace-period-ms` semantic shift when `wal.enabled=true`** —
  the knob now bounds the wait for the flusher loop to exit cleanly,
  rather than a data-drain window. (Note: `Thread.interrupt()` does
  not cancel an in-flight OkHttp call, so a worker blocked on a dead
  TCP connection may continue past the grace window until
  `http.read-timeout-ms`; the HTTP client is shut down right after
  the grace, which is what actually breaks the call.) WAL durability
  means no sample is lost at shutdown regardless of grace value;
  anything unshipped replays on next start. Operators who set a large
  grace value (e.g., 60_000) for drain-safety under v0.4 can safely
  reduce it. The `wal.enabled=false` path (default) retains the v0.4
  drain-or-lose semantics exactly.
- **`queue.capacity` ignored when `wal.enabled=true`** — WARN logged
  at startup if the operator explicitly sets it. The WAL replaces the
  in-memory queue; size via `wal.max-size-bytes` instead.

### Tests

- **Blueprint-wiring regression test** — reflectively cross-checks every
  `<property name="X">` in `OSGI-INF/blueprint/blueprint.xml` against the
  corresponding `set*` method on `PrometheusRemoteWriterConfig`, and
  both-directions against every `<cm:property>` default. Catches the
  v0.3-style silent-no-op where `labels.copy` had a Java setter but no
  Blueprint binding — the kind of bug no existing test would see.
- **IT storage-rebuild isolation** — five `PrometheusRemoteWriteIT` tests
  that recreate `PrometheusRemoteWriterStorage` with a custom config now
  use a local `override` variable with try/finally stop. Earlier pattern
  (field reassignment between stop/start) would leak a started storage on
  an assertion failure and leak a stopped reference on a constructor or
  `start()` throw.

## Internal milestone: default `job` + `instance` labels (folded into 0.2.0)

### Added

- **`job` default label** — derived from the sample's `resourceId` shape:
  bracketed or slash-DB SNMP patterns → `"snmp"`, slash-FS groups named
  `jmx-*` or `opennms-jvm` → `"jmx"`, unparseable or absent resourceIds →
  `"opennms"` (catch-all). Emitted unconditionally so `{job=~".+"}` covers
  every sample.
- **`instance` default label** — mirror of `node` with the same derivation
  precedence (FS-qualified external tags > parsed slash-FS > parsed slash-DB
  > external `nodeId`). Emitted iff `node` is emitted. `node` is kept
  alongside — dashboards filtering on `{node="X"}` continue working
  unchanged.
- **`job.name` configuration key** — when set to a non-empty value,
  overrides the per-sample `job` derivation with a fleet-wide constant.
  Default: unset (use derivation).

  Together, these enable the Prom-idiomatic `{job="X", instance="Y"}`
  scoping idiom for cross-source dashboards that combine OpenNMS data
  with node-exporter / OTel / other Prometheus data sources in a shared
  backend. See README "Cross-source filtering" for the semantics and the
  deliberate `instance = subject` stance.

### Changed (BREAKING)

- **`instance` and `job` added to the reserved-target list.**
  `labels.rename = X -> instance` / `X -> job` and `labels.copy = Y -> instance`
  / `Y -> job` are now rejected at startup with the existing "collides with
  the default label" error. Operators who previously wrote
  `labels.copy = node -> instance` as a workaround for the missing default
  can remove the directive (the default emission covers it) — the config
  parser would reject it anyway.

### Changed

- **Internal**: `PrometheusRemoteWriterConfig.labelsRenameMap()` and
  `labelsCopyMap()` now cache their parsed result, returning the same `Map`
  instance across repeated calls within one config-string lifecycle. Setters
  invalidate. `validate()` no longer re-parses the same string three times
  per invocation. The returned maps continue to be unmodifiable (as before);
  the behavior change is strictly tighter — same content, stable identity.

### Fixed

- `labels.copy` is now actually wired up from ConfigAdmin — v0.3 introduced
  the Java setter and the parser + validator, but the Blueprint property
  binding and the default placeholder entry were both missing. Operators
  who tried `labels.copy = ...` in their `.cfg` on v0.3 saw a silent no-op.
  v0.4 adds the Blueprint `<cm:property>` entry and the `<property name="labelsCopy">`
  binding alongside `instance.id` and the new `job.name` knob. Also picks up
  an earlier docs oversight: the README Configuration block was missing
  `instance.id` entirely (has been since v0.1); now includes both `instance.id`
  and `job.name` under a "Source identity" subsection.

### Tests

- Pinned the `instance.id` WARN-suppression emission count — a refactor that
  kept the one-shot gate correct but moved `LOG.warn` outside the CAS-success
  branch would have passed the existing boolean-gate assertions and silently
  re-fired on every bundle activation. New counter +
  `getInstanceIdWarnCountForTesting()` accessor + three scenarios (first
  start, silent re-start, hot-reload with instance.id flipped on).
- Pinned the harder one-pass invariant of `labels.copy`: the stage reads
  source values from its input map, not from the accumulating output, so a
  chained directive whose source was just clobbered by an earlier directive
  still sees the ORIGINAL value.

### Upgrade notes

- **Dashboards using `{node="X"}` are unaffected.** `node` is still a default
  label with the same value semantics; `instance` is an additional emission.
- **Dashboards built around `{job=..., instance=...}` work out of the box.**
  No need for per-deployment `labels.copy = node -> instance` — it's a
  default emit now.
- **Operators with `labels.copy = node -> instance` in `.cfg`** must remove
  it: v0.4 rejects the directive at startup because `instance` is reserved.
  Once removed, the default emission carries the same value.
- **Wire cost**: +30-50 bytes per sample for the two new labels (`job` is
  short low-cardinality; `instance` shares cardinality and values with
  `node`). For deployments pushing 1000 samples/sec, typical <1% wire
  traffic increase. Opt out with `labels.exclude = instance, job` if
  neither label is useful for your backend.
- **Mixed backends**: operators combining OpenNMS data with node-exporter
  will see `instance` carry different value shapes across sources
  (node-exporter: `host:port`; OpenNMS: `<foreignSource>:<foreignId>` or
  numeric dbId). This is by design — `instance` means "the measured
  device" from each source's perspective; `job` is the primary cross-source
  scoping filter. Cross-source value-correlation bridges (for queries that
  need the SAME value to identify "the same box" across sources) remain
  the operator's responsibility via backend `relabel_config`.
- **Operators who configured `labels.copy` via Karaf `config:edit` on v0.3**
  (rather than via the `.cfg` file, where the binding was missing on v0.3):
  verify your `labels.copy` value is still set after the v0.4 bundle
  activates. v0.4 adds the previously-missing `<cm:property name="labels.copy">`
  default to the Blueprint descriptor; Aries Blueprint's merge behavior
  between an empty default and an existing ConfigAdmin dictionary may reset
  the value in edge cases. Re-apply via `config:edit` or the `.cfg` file if
  so.

## Internal milestone: `labels.copy` primitive (folded into 0.2.0)

### Added

- **`labels.copy` primitive** — a new `labels.*` configuration key that emits
  the value of an existing label under an additional name, leaving both
  present on the wire. Solves dashboard-portability scenarios
  (`labels.copy = node -> instance` to bridge OpenNMS-native and
  Prometheus-idiomatic vocabulary), migration-period dual emission
  (emit old + new name for a release cycle, then drop), and integration with
  systems that hardcode label names (multi-tenant Mimir's `tenant` label,
  vendor alert bundles, Grafana folder conventions).

  Config syntax mirrors `labels.rename` — a comma-separated list of
  `from -> to` pairs — with one difference: the same `from` key may appear
  with multiple targets (`labels.copy = node -> instance, node -> host`)
  to emit all named copies.

  Pipeline position: between `labels.include` and `labels.rename`
  (`defaults → exclude → include → copy → rename → metadata`). Copy is
  one-pass (snapshots source values at stage entry; `copy = a -> b, b -> c`
  does NOT produce `c`) and operates on pre-rename names.

  Validation: copy targets must not collide with default-allowlist labels,
  reserved prefixes (`onms_cat_*`, `onms_meta_*`), another copy target, or a
  `labels.rename` target. Unknown copy sources are logged once at startup
  and treated as per-sample no-ops thereafter.

- **`rename`/`copy` target-validation sharing** — the reserved-name and
  reserved-prefix checks are now centralised in a single helper. A new
  collision rule added to one primitive applies uniformly to the other.

### Known limitations

- **`labels.copy` is the last mapping primitive.** Further transformations —
  regex extraction, case conversion, conditional emission, value rewriting —
  belong on the backend via Prometheus / Mimir `relabel_config`, not in this
  plugin. This is a deliberate scope commitment: the plugin is a simple
  remote-write sender, and the ecosystem has better tools for the heavier
  transformations downstream.
- **Glob support on the copy source** (e.g., `onms_cat_* -> cat_*`) is not
  supported in v0.3. Worth reconsidering in a future release if real demand
  materialises; adds validation complexity that v0.3 does not need.

### Upgrade notes

- **Purely additive.** Deployments that do not set `labels.copy` see no
  behavior change. The internal refactor that shares validation code between
  `labels.rename` and `labels.copy` is operator-invisible.
- **Wire cost.** Each copy directive adds ~20-50 bytes per sample. Typical
  deployments configuring one or two copies see <1% write-traffic increase.
- **Cardinality shift.** Turning a copy on once changes series identity
  (Prometheus identifies series by the full label-value set), replacing the
  pre-copy series with post-copy series of the same count. No ongoing
  cardinality growth.

## Internal milestone: label enrichment v0.2 (folded into 0.2.0)

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
| `node_id`        | `node`                     | FS-qualified identity when available, else parsed from the `resourceId`, else the numeric dbId. To keep the `node_id` label name for existing dashboards, use `labels.rename = node -> node_id` — but note the *value* is now FS-qualified (`<fs>:<fid>`) when available, not the raw numeric dbId v0.1 emitted under `node_id`. Dashboards that assumed a purely numeric value must be updated. |
| `categories`     | `onms_cat_<name>` per cat  | Per-category expansion, not a single comma-separated label.                                                           |

The six other snake-cased source-key forms (`foreign_source`, `foreign_id`,
`node_label`, `location`, `if_name`, `if_descr`) collided on label name with a
default and were already single-valued in v0.1 via `putIfAbsent` — they are
unchanged in v0.2.

This also supersedes an undocumented v0.1 quirk where
`labels.rename = foreign_source -> foreign_source_raw` combined with
`labels.include = *` produced both `foreign_source_raw` (renamed default) and
`foreign_source` (re-surfaced from the source tag). In v0.2 only
`foreign_source_raw` is emitted — the source key is consumed and the include
pass skips it. Operators who want the original value preserved under a
different label name use `labels.rename` alone.

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

[Unreleased]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/labmonkeys-space/prometheus-remote-writer/releases/tag/v0.1.0
