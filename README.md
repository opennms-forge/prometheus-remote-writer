# OpenNMS Prometheus Remote Writer

An OpenNMS Horizon plugin that pushes performance data to any Prometheus-compatible
Remote Write endpoint — Prometheus itself, Cortex, Grafana Mimir, VictoriaMetrics,
Thanos Receive — and surfaces OpenNMS resource context as native Prometheus labels.

Query your OpenNMS time-series data with PromQL directly, from Grafana's native
Prometheus data source. No OpenNMS Plugin for Grafana required, no round-trip to
the OpenNMS REST API at query time.

## Status

**v0.1 — under development.** Targets OpenNMS Horizon 35 on Java 17.

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
| `onms_instance_id` | config `instance.id` | Only emitted when `instance.id` is set. See "Identifying samples from multiple OpenNMS instances". |
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

## Identifying samples from multiple OpenNMS instances

Running more than one OpenNMS instance against the same Prometheus-compatible
backend? Two independent knobs exist, and they solve different problems:

| Knob | Header / label | What it does | Works with |
|---|---|---|---|
| `instance.id` | label `onms_instance_id` | Stamps every sample with a stable per-instance identifier. PromQL can filter (`{onms_instance_id="…"}`) or aggregate (`sum by (onms_instance_id) (…)`) across all instances in the shared backend. | Every Prometheus-compatible backend. |
| `tenant.org-id` | header `X-Scope-OrgID` | Partitions storage at the backend tier — each tenant's data is isolated, queried separately. Matches the `opennms-cortex-tss-plugin` `organizationId` behavior. | Grafana Mimir, Cortex, VictoriaMetrics cluster, Thanos Receive. **No-op** against plain Prometheus and single-tenant VictoriaMetrics. |

### When to use which

| Deployment | `instance.id` | `tenant.org-id` |
|---|---|---|
| Single OpenNMS → dedicated backend | not required | not required |
| Multiple OpenNMS → shared Prometheus / single-tenant VM | **required** | n/a (no-op) |
| Multiple OpenNMS → Mimir / Cortex / VM cluster, fleet-wide queries | **required** | optional |
| Multiple OpenNMS → Mimir / Cortex / VM cluster, strict per-instance isolation | optional | **required** |

If you want both fleet-wide PromQL *and* backend-enforced isolation, set both.

### Example

Two OpenNMS instances writing to the same Mimir cluster:

```properties
# opennms.properties.d on instance #1
instance.id    = opennms-us-east
tenant.org-id  = fleet-prod

# opennms.properties.d on instance #2
instance.id    = opennms-us-west
tenant.org-id  = fleet-prod
```

PromQL:

```
# All nodes, either OpenNMS
up{job="opennms"}

# Per-OpenNMS rollup
sum by (onms_instance_id) (rate(ifHCInOctets[5m]))

# Just the west instance
ifHCInOctets{onms_instance_id="opennms-us-west"}
```

### Notes

- `onms_instance_id` is honored by `labels.rename` and `labels.exclude` on the
  same terms as any other default label. If you prefer `cluster` or `tenant` or
  `fleet` as the label name, rename it.
- **Override ordering**: the label pipeline applies `labels.exclude` first, then
  `labels.include`, then `labels.rename`. This matters for `onms_instance_id`
  specifically: `labels.exclude = onms_instance_id` drops it, and `labels.include`
  does *not* resurrect excluded labels — the include pass only surfaces non-default
  source tags. `labels.rename` targets that collide with a default label name (or
  with another rename's target) are rejected at startup with an actionable error —
  see [Reserved rename targets](#reserved-rename-targets) below.
- When `instance.id` is unset, the plugin logs **one** `WARN` at startup
  pointing at the knob. This is informational — single-instance deployments
  can ignore or silence it by setting the value.
- **Validation**: `instance.id` must not contain control characters (`\n`, `\t`,
  `\0`, etc.) and must be ≤ 2048 UTF-8 bytes. Values that violate either rule
  cause the plugin to refuse to start with an actionable error message.
- The plugin cannot detect cross-process uniqueness of `instance.id`. If two
  OpenNMS instances configure the same value, their samples collide again.
  Pick stable, unique identifiers.

#### Reserved rename targets

The plugin rejects `labels.rename` entries whose *target* would silently clobber
an already-emitted label at flush time. Reserved targets:

| Kind | Value | Why |
|---|---|---|
| Exact | `__name__` | Prometheus metric name. |
| Exact | `resourceId` | OpenNMS resource identifier (raw, lossless). |
| Exact | `node` | Derived FS-qualified or numeric node id. |
| Exact | `foreign_source`, `foreign_id` | Requisition identity. |
| Exact | `node_label` | Node's human-readable name. |
| Exact | `location` | OpenNMS monitoring location. |
| Exact | `resource_type`, `resource_instance` | Parsed from `resourceId`. |
| Exact | `if_name`, `if_descr`, `if_speed` | SNMP interface descriptors. |
| Exact | `onms_instance_id` | Multi-instance origin stamp (reserved even when `instance.id` is unset). |
| Prefix | `onms_cat_*` | Per-surveillance-category expansion. |
| Prefix | `onms_meta_*` | Default metadata-passthrough prefix. |

Duplicate rename targets (`foo -> cluster, bar -> cluster`) are also rejected.
The plugin refuses to start with a message naming the offending value:

```
labels.rename target 'foreign_source' collides with the default label
'foreign_source'. The plugin already emits this label; renaming onto it would
silently clobber the default value. Pick a different 'to' name.
```

Pre-upgrade check — scan your cfg for colliding rename targets:

```bash
grep -E '^labels\.rename' /opt/opennms/etc/org.opennms.plugins.tss.prometheusremotewriter.cfg
```

Inspect each `from -> to` pair; any `to` in the table above needs a new name.

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

## Local sandbox (e2e)

The `e2e/` directory contains a Docker Compose stack that stands up an
OpenNMS Horizon core, Grafana, and one Prometheus-compatible backend of your
choice. It's the quickest way to see the plugin work end-to-end on a
laptop, and what task 15.2 / 15.3 use to exercise the real integration
path.

**Prerequisites:** Docker 24+ with Compose v2.

The fastest way to prove the plugin works is `make smoke` — it builds
the KAR, spins each backend up in turn, waits for OpenNMS to push real
samples through the plugin, confirms the backend has ingested series,
and tears the stack down again:

```bash
make smoke                               # all three backends, sequential
make smoke BACKENDS=prometheus           # single backend
make smoke TIMEOUT=900 BACKENDS=mimir    # bump the per-backend deadline
```

For interactive poking, bring a stack up yourself:

```bash
# 1. Build the KAR — the stack mounts assembly/kar/target read-only into
#    the core container's Karaf deploy directory.
make kar

# 2. Pick a backend and start the stack. One compose file per backend —
#    the postgres / core / grafana definitions live in compose.base.yml
#    and each backend file extends them with its own plugin cfg and
#    Grafana datasource bind-mounts.
cd e2e

docker compose -f compose.prometheus.yml      up -d
# or
docker compose -f compose.mimir.yml           up -d
# or
docker compose -f compose.victoriametrics.yml up -d
```

First boot of OpenNMS takes a few minutes while it initialises the
PostgreSQL schema and loads Karaf features. When it settles, these are
the entry points:

| Service | URL | Default credentials |
|---|---|---|
| OpenNMS Web | <http://localhost:8980/opennms/> | `admin` / `admin` |
| OpenNMS Karaf | `ssh -p 8101 admin@localhost` | `admin` / `admin` |
| Grafana | <http://localhost:3000/> — go to **Explore → OpenNMS (<backend>)** | `admin` / `admin` (anonymous Viewer enabled) |

Verify the plugin is running, from the Karaf shell:

```
karaf@root()> bundle:list -s | grep prometheus-remote-writer
karaf@root()> opennms:prometheus-writer-stats
```

The `stats` command prints all plugin counters and gauges — watch
`samples_written_total` tick up as OpenNMS pushes its first samples
through the plugin.

To tear down and reset (use the same file you brought the stack up with):

```bash
docker compose -f compose.prometheus.yml down -v --remove-orphans
```

See [`e2e/README.md`](e2e/README.md) for the full reference — how
iteration works (rebuild + restart loop), querying each backend's API
directly, and what the stack deliberately does not exercise.

## Quick start

> Once v0.1 is released. Placeholder until then.

```bash
# Install on a running OpenNMS Horizon 35 instance
karaf@root()> feature:repo-add mvn:org.opennms.plugins/prometheus-remote-writer-features/0.1.0/xml/features
karaf@root()> feature:install prometheus-remote-writer
```

Minimum config at `etc/org.opennms.plugins.tss.prometheusremotewriter.cfg`:

```properties
write.url = https://mimir.example.com/api/v1/push
read.url  = https://mimir.example.com/prometheus
```

`read.url` is the backend's **Prometheus-compatible root**. The plugin
appends `/api/v1/series` and `/api/v1/query_range` itself. Do NOT include
`/api/v1` in the configured URL. Common shapes:

| Backend | `read.url` |
|---|---|
| Prometheus | `https://prom:9090` |
| Grafana Mimir | `https://mimir/prometheus` |
| VictoriaMetrics | `https://vm:8428` |
| Cortex | `https://cortex/prometheus` |
| Thanos Receive (Query) | `https://thanos-query` |

Then in `etc/opennms.properties.d/timeseries.properties`:

```properties
org.opennms.timeseries.strategy = integration
```

## Configuration

All knobs live in `etc/org.opennms.plugins.tss.prometheusremotewriter.cfg`
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
- `samples_dropped_transport_total` — IOException / socket failures (distinct from 5xx so you can alert separately)
- `samples_dropped_queue_full_total`
- `samples_dropped_nonfinite_total`
- `samples_dropped_duplicate_total` — same-timestamp same-series dedup (last-write-wins)
- `delete_noop_total`
- `metadata_denylist_blocked_total`
- `queue_depth` (gauge)
- `http_in_flight` (gauge) — running + queued HTTP requests at the dispatcher
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
