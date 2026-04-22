# End-to-end sandbox

A self-contained Docker Compose stack for manually exercising the
`prometheus-remote-writer` plugin against real Prometheus-compatible backends.

## Layout

```
e2e/
├── compose.base.yml               # shared: postgres + core + grafana
├── compose.prometheus.yml         # extends base, adds prometheus + per-backend mounts
├── compose.mimir.yml              # extends base, adds mimir + per-backend mounts
├── compose.victoriametrics.yml    # extends base, adds vm + per-backend mounts
├── opennms/
│   ├── opennms.properties.d/
│   │   └── timeseries.properties  # activates TSS integration strategy
│   ├── prometheus.cfg             # plugin cfg for the prometheus backend
│   ├── mimir.cfg                  # plugin cfg for the mimir backend
│   └── victoriametrics.cfg        # plugin cfg for the vm backend
├── grafana/
│   └── datasources/               # one file per backend; the matching
│       ├── prometheus.yml         # compose.<backend>.yml mounts it to
│       ├── mimir.yml              # Grafana's provisioning dir
│       └── victoriametrics.yml
├── prometheus/
│   └── prometheus.yml             # minimal Prom config with remote-write receiver
└── mimir/
    └── mimir.yaml                 # single-binary Mimir config
```

## Prerequisites

- Docker 24+ with Compose v2
- Build the KAR first so the `core` container can pick it up from the
  mounted `../assembly/kar/target`:
  ```bash
  cd .. && make kar
  ```

## Running

One compose file per backend — nothing else to match up. The base
services (postgres, core, grafana) are defined once in
`compose.base.yml`; each backend file `extends:` them and appends the
backend-specific plugin cfg and Grafana datasource mounts.

```bash
# Prometheus
docker compose -f compose.prometheus.yml      up -d

# Grafana Mimir
docker compose -f compose.mimir.yml           up -d

# VictoriaMetrics
docker compose -f compose.victoriametrics.yml up -d
```

First boot of the `core` container can take several minutes while OpenNMS
creates the database and loads features. Watch for:

```
Starting Karaf...
```

Endpoints:

| Service | URL | Default credentials |
|---|---|---|
| OpenNMS Web UI | <http://localhost:8980/opennms/> | `admin` / `admin` |
| OpenNMS Karaf SSH | `ssh -p 8101 admin@localhost` | `admin` / `admin` |
| Grafana | <http://localhost:3000/> | `admin` / `admin` (anonymous Viewer enabled) |
| Prometheus UI | <http://localhost:9090/> (when active) | — |
| Mimir UI | <http://localhost:9009/> (when active) | tenant `e2e` |
| VictoriaMetrics UI | <http://localhost:8428/> (when active) | — |

Grafana auto-provisions a datasource pointing at whichever backend is
active (selected by the compose file you brought up). Open **Explore →
OpenNMS (<backend>)** to run PromQL against the data OpenNMS just wrote.

## Smoke test (automated)

For a non-interactive sanity check against all three backends:

```bash
make smoke                          # all three, sequentially
make smoke BACKENDS=prometheus      # single backend
make smoke BACKENDS="mimir victoriametrics"
```

Each backend is brought up, polled for > 0 ingested series, and torn
down. The target depends on `kar`, so a fresh KAR is built first. The
script bails (and tears down) on the first timeout; pass/fail summary
is printed at the end.

## Verifying the plugin is active

```bash
# Karaf shell (default admin/admin)
ssh -p 8101 admin@localhost

# Inside Karaf:
karaf@root()> feature:list | grep prometheus-remote-writer
karaf@root()> bundle:list | grep prometheus-remote-writer
karaf@root()> opennms:prometheus-writer-stats
```

## Querying the backend

Once OpenNMS has collected a few samples (default interval: 5 minutes on a
fresh provisioning), query the backend directly.

**Prometheus:**
```
curl 'http://localhost:9090/api/v1/series?match%5B%5D={__name__=~".%2B"}' | jq .
curl 'http://localhost:9090/api/v1/query?query=up' | jq .
```

**Mimir:** (requires the `X-Scope-OrgID` header; tenant is `e2e` per the cfg)
```
curl -H 'X-Scope-OrgID: e2e' \
  'http://localhost:9009/prometheus/api/v1/series?match%5B%5D={__name__=~".%2B"}' | jq .
```

**VictoriaMetrics:**
```
curl 'http://localhost:8428/api/v1/series?match%5B%5D={__name__=~".%2B"}' | jq .
```

## Tear down

Use the same `-f` you brought the stack up with:

```bash
docker compose -f compose.prometheus.yml down -v --remove-orphans
```

`-v` removes the named data volumes (postgres, opennms, prometheus, mimir,
vm). Drop `-v` if you want to keep state across restarts.

## Iterating on the plugin

The `../assembly/kar/target` directory is mounted read-only into
`/opt/opennms/deploy/`. A rebuild of the KAR (`make kar` from the repo
root) does **not** auto-reload the plugin — Karaf's hot-deploy watches
file timestamps, but the container sees the mount at a point in time. To
reload a freshly built KAR:

```bash
# From the repo root
make kar

# Restart only the core container (use whichever compose file is active)
docker compose -f compose.prometheus.yml restart core
```

Or, inside the Karaf shell, `feature:uninstall` + `feature:install` cycles
the plugin without restarting the container.

## What's NOT exercised here

- Minion / remote pollers — this is a single-core sandbox
- ActiveMQ / Kafka messaging — not needed for the TSS path
- TLS / auth to the backend — all cleartext on the compose network
- Multi-tenant routing beyond Mimir's default `e2e` tenant
- Dashboards — Grafana is provisioned with a datasource only; you build
  dashboards on top in **Explore** or by dropping JSON under a
  `grafana/dashboards/` provisioning directory
