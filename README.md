# OpenNMS Prometheus Remote Writer

An OpenNMS Horizon plugin that pushes performance data to any Prometheus-compatible
Remote Write endpoint — Prometheus itself, Cortex, Grafana Mimir, VictoriaMetrics,
Thanos Receive — and surfaces OpenNMS resource context as native Prometheus labels.

Query your OpenNMS time-series data with PromQL directly, from Grafana's native
Prometheus data source. No OpenNMS Plugin for Grafana required, no round-trip to
the OpenNMS REST API at query time.

## Status

**v0.1 — under development.** Targets OpenNMS Horizon 36 on Java 21.

## Why this plugin exists

OpenNMS ships a Prometheus integration today via
[`opennms-cortex-tss-plugin`](https://github.com/OpenNMS/opennms-cortex-tss-plugin).
That plugin writes numeric samples to a Prometheus backend but keeps OpenNMS
resource context (node label, foreign source, categories, asset record, interface
descriptors) in a separate OpenNMS key-value store. To turn those opaque
`resourceId` labels back into human-readable resources, you need the
[OpenNMS Plugin for Grafana](https://github.com/OpenNMS/grafana-plugin), which
round-trips to the OpenNMS REST API at query time.

`prometheus-remote-writer` fixes that at the write path. Resource context is
pushed to the backend as first-class Prometheus labels, so PromQL — on any
vanilla Grafana Prometheus data source — works end-to-end with no OpenNMS
query-time dependency.

## License

Apache License 2.0. See [LICENSE](LICENSE).

This plugin is a **clean-room implementation**. It targets
`opennms-integration-api` v2.0, which is Apache 2.0, and is written from public
specifications — not derived from the AGPL-3.0 `opennms-cortex-tss-plugin`.
Reference sources consulted during implementation:

- [Prometheus Remote Write specification](https://prometheus.io/docs/specs/prw/remote_write_spec/)
- [Prometheus HTTP query API](https://prometheus.io/docs/prometheus/latest/querying/api/)
- Prometheus protobuf definitions (`prompb/remote.proto`, `prompb/types.proto`, Apache 2.0 upstream)
- Sanitization rules from `prometheus/common` (Apache 2.0 upstream)

No code has been lifted from `opennms-cortex-tss-plugin`.

## Default label set

For every sample, the plugin emits the following Prometheus labels when the
corresponding source data is available:

| Label | Source | Notes |
|---|---|---|
| `__name__` | intrinsic `name` | Sanitized to Prom's metric-name grammar. |
| `resourceId` | intrinsic `resourceId` | Raw, lossless. |
| `node` | derived | `"<foreignSource>:<foreignId>"` when both are set; numeric dbId otherwise. |
| `node_label` | external `nodeLabel` | Human-readable. Mutable — disable via config if churn is a concern. |
| `foreign_source` | external | Stable. |
| `foreign_id` | external | Stable. |
| `location` | external | OpenNMS monitoring location. |
| `resource_type` | parsed from `resourceId` | e.g. `interfaceSnmp`, `hrStorageIndex`, `nodeSnmp`. |
| `resource_instance` | parsed from `resourceId` | e.g. `en0`, `1`, or empty for node-level. |
| `if_name` | external `ifName` | |
| `if_descr` | external `ifDescr` | |
| `if_speed` | derived | Bits-per-second: `ifHighSpeed × 1_000_000` when non-zero, else `ifSpeed`. |
| `onms_cat_<name>` | surveillance categories | One label per category, value `"true"`. |

**Deliberately excluded by default** — available via `labels.include` if you want
them: `if_alias` (user-editable, churns), `sys_descr`, `sys_object_id`, asset
record fields, OpenNMS metadata (see below).

## OpenNMS metadata — opt-in only

OpenNMS metadata is an open KV store scoped per-node, per-interface, and
per-service. Operators put arbitrary things in there, **including credentials**
like `requisition:snmp-community`, `jdbc:password`, and API tokens. Emitting
that as Prometheus labels would publish those credentials to every dashboard
and alert built on top of the backend.

This plugin ships with:

- `metadata.enabled = false` by default. You opt in explicitly.
- A built-in denylist that blocks `*password*`, `*secret*`, `*token*`, `*key*`,
  and `snmp-community` regardless of operator `include` globs.
- Labels namespaced under `onms_meta_<context>_<key>` so they never collide
  with the default label set.

See [operator knobs](#configuration) for the full knob list.

## Building from source

The build uses Apache Maven Wrapper (`mvnw`) so you don't need a global Maven
install — just JDK 21. A `Makefile` fronts the common targets; run `make help`
to see everything available.

```bash
make build        # compile, run unit tests, install locally
make test         # unit tests only
make verify       # unit + integration tests (needs Docker for testcontainers)
make kar          # build assembly/kar/target/*.kar
make clean
```

Under the hood the Makefile calls `./mvnw`, which in turn bootstraps the Maven
version pinned in `.mvn/wrapper/maven-wrapper.properties`.

## Quick start

> Once v0.1 is released. Placeholder until then.

```bash
# Install on a running OpenNMS Horizon 36 instance
karaf@root()> feature:repo-add mvn:org.opennms.plugins/prometheus-remote-writer-features/0.1.0/xml/features
karaf@root()> feature:install prometheus-remote-writer
```

Minimum config at `etc/org.opennms.plugins.tss.prometheus-remote-writer.cfg`:

```properties
write.url = https://mimir.example.com/api/v1/push
read.url  = https://mimir.example.com/prometheus
```

Then in `etc/opennms.properties.d/timeseries.properties`:

```properties
org.opennms.timeseries.strategy = integration
```

## Configuration

All knobs live in `etc/org.opennms.plugins.tss.prometheus-remote-writer.cfg`
and take effect on the next flush cycle — no OpenNMS restart required.

```properties
# Endpoint
write.url                     = https://<backend>/api/v1/push
read.url                      = https://<backend>/prometheus

# Authentication — Basic and Bearer are mutually exclusive; the plugin
# refuses to start if both are configured.
auth.basic.username           =
auth.basic.password           =
auth.bearer.token             =

# Multi-tenant header (Cortex, Mimir)
tenant.org-id                 =

# TLS
tls.ca-file                   =
tls.insecure-skip-verify      = false

# Write pipeline
queue.capacity                = 10000
batch.size                    = 1000
flush.interval-ms             = 1000
retry.max-attempts            = 5
retry.initial-backoff-ms      = 250
retry.max-backoff-ms          = 10000

# HTTP
http.connect-timeout-ms       = 5000
http.read-timeout-ms          = 30000
http.write-timeout-ms         = 30000
http.max-connections          = 16

# Shutdown
shutdown.grace-period-ms      = 10000

# Read path
max-series-lookback-seconds   = 7776000   # 90 days

# Label policy
labels.include                =
labels.exclude                =
labels.rename                 =
metric.prefix                 =

# Metadata passthrough (OFF by default)
metadata.enabled              = false
metadata.include              =
metadata.exclude              =
metadata.label-prefix         = onms_meta_
metadata.case                 = preserve
```

### Operator tuning

**Cardinality controls.** The default label allowlist is deliberately narrow.
If you observe unexpected label churn (rising series count on a stable set of
resources), the usual culprits are:

- `node_label` — mutable; any rename creates a new series. Drop it with
  `labels.exclude = node_label` if renames happen regularly.
- `if_descr` — vendor-generated; firmware upgrades can change it. Same fix:
  `labels.exclude = if_descr`.
- Large `onms_cat_*` fan-out — nodes with many surveillance categories add
  one label each per series.

If you deliberately want more context, opt in with
`labels.include = ifAlias, sysDescr, sysObjectId` or similar. Globs work
(`labels.include = asset_*`). Each operator addition multiplies your series
space, so prefer narrow globs.

**Metadata gating.** `metadata.enabled = true` opts into the OpenNMS metadata
passthrough. The built-in denylist (`*password*`, `*secret*`, `*token*`,
`*key*`, `snmp-community`) is always applied; `metadata.exclude` extends it.
Leave metadata disabled unless you have an explicit use case — the
credential-leak risk is real.

**TLS.** Default is JDK truststore verification. To trust a private CA, point
`tls.ca-file` at a PEM bundle. `tls.insecure-skip-verify = true` is
development-only: on each use the plugin emits a WARN log, repeated hourly.

**Auth.** Pick one of Basic or Bearer, or neither. `X-Scope-OrgID` is
independent and can be combined with either.

**Backpressure.** When the queue fills (`queue.capacity`, default 10 000),
`store()` throws `StorageException` and OpenNMS sees the failure. If you hit
this under normal load, the flusher isn't keeping up — raise
`http.max-connections`, drop `flush.interval-ms`, raise `batch.size`, or look
at backend ingest latency. v0.1 has no on-disk buffer; samples are lost if
the backend is unavailable for longer than `queue.capacity / sample-rate`.

## Self-metrics

Exposed via a Dropwizard registry and printed by the Karaf shell command
`opennms:prometheus-writer-stats`:

- `samples_written_total`
- `samples_dropped_4xx_total`
- `samples_dropped_5xx_total`
- `samples_dropped_queue_full_total`
- `samples_dropped_nonfinite_total`
- `delete_noop_total`
- `metadata_denylist_blocked_total`
- `queue_depth` (gauge)
- `http_bytes_written_total`
- `http_writes_successful_total`
- `http_writes_failed_total`

## Backend compatibility

Targets the [Prometheus Remote Write v1](https://prometheus.io/docs/specs/prw/remote_write_spec/)
protocol. Tested against (planned, end of v0.1 cycle):

- Prometheus
- Grafana Mimir
- VictoriaMetrics
- Cortex
- Thanos Receive

Remote Write v2, native histograms, exemplars, and mTLS client certificates are
deferred to future releases.

## Non-goals for v0.1

These are deliberate omissions, not missing features. Each has a path to a
later release if demand materializes.

- **Remote Write v2** — v1 is universally supported; v2 is stable since
  Prometheus 2.50 but not yet universally deployed across backends. Tracked
  for v0.2.
- **Native histograms and exemplars** — deferred to v0.2; OpenNMS doesn't
  currently surface histogram data through the TSS SPI.
- **mTLS client certificates** — Basic, Bearer, and tenant-id header cover
  the common cases in v0.1. Client-cert auth is a v0.2 candidate.
- **Durable on-disk write buffer (WAL)** — v0.1 uses a bounded in-memory
  queue and drops on overflow. Samples are lost on process restart or
  extended backend outage. Operators are expected to alert on
  `samples_dropped_queue_full_total`. A WAL is tracked for v0.2.
- **Per-tenant routing or multi-destination fan-out** — one write URL, one
  tenant ID per plugin instance. For multi-destination, run multiple OpenNMS
  instances or wait for the fan-out work.
- **Migration tooling from `opennms-cortex-tss-plugin`** — not in scope for
  v0.1. Likely shape: stand both plugins up, dual-write for a period, switch
  queries once the new labels are established, uninstall cortex-tss. No
  in-product tooling.
- **Per-series `delete()`** — Prometheus Remote Write has no delete
  semantic. `delete(Metric)` is a no-op that logs a rate-limited WARN.
  Configure retention at the backend tier (Prometheus
  `--storage.tsdb.retention`, Mimir/VictoriaMetrics compactor).
- **Full OpenNMS TSS compliance-suite pass** — the compliance suite's
  `shouldDeleteMetrics` and whole-`Metric` partition-equality assertions
  conflict with our design. See the design doc §6 and §7 for rationale;
  `PrometheusComplianceIT` skips the conflicting tests with documented
  `@Ignore` reasons.

## Contributing

Code contributions must not be derived from the AGPL-3.0
`opennms-cortex-tss-plugin`. Reference upstream Prometheus sources (Apache 2.0)
and the public specs linked above. PRs will be reviewed for license hygiene.
