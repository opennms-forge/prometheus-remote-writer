# Project notes for AI agents

This file captures guidance specific to **`prometheus-remote-writer`**.
General coding / commit / hygiene conventions live in the user-global
`~/.claude/CLAUDE.md` and apply here too — only project-specific norms
are repeated below.

## License hygiene (load-bearing)

This is a **clean-room implementation**. Code MUST NOT be derived from
the AGPL-3.0 [`opennms-cortex-tss-plugin`](https://github.com/OpenNMS/opennms-cortex-tss-plugin).
The targeted dependency `opennms-integration-api` v2.0 is Apache 2.0 —
implementation references are limited to:

- the [Prometheus Remote Write spec](https://prometheus.io/docs/specs/prw/remote_write_spec/)
- the [Prometheus HTTP query API docs](https://prometheus.io/docs/prometheus/latest/querying/api/)
- upstream Prometheus protobuf definitions (`prompb/remote.proto`,
  `prompb/types.proto`, `io.prometheus.write.v2.Request`) — Apache 2.0
- sanitization rules from `prometheus/common` — Apache 2.0

PR reviewers check for license hygiene. If you ever find a snippet that
*could* be from the cortex plugin, flag it explicitly rather than
silently importing the pattern.

## Build entrypoint

The build is fronted by a `Makefile` over the Maven Wrapper (`mvnw`).
CI invokes `make` targets, never raw Maven — keep them in sync.

```bash
make build     # compile, run unit tests, install all modules locally
make test      # unit tests only
make verify    # unit + integration tests (needs Docker)
make kar       # build the KAR
make smoke     # e2e smoke tests against all backends (Docker compose)
make clean
```

## Java source file headers

**Every** `.java` file MUST begin with the standard Apache 2.0 header
codified in `~/.claude/CLAUDE.md` ("Java Source File Headers"). Both
new files and any edited file with a missing/different header.

Year is the file's **creation** year (don't bump on edit). Author line
stays stable across edits.

## End-to-end testing

The `e2e/` directory contains a Docker Compose stack for OpenNMS Horizon
+ a Prometheus-compatible backend. Three backend variants:

```bash
cd e2e && docker compose -f compose.prometheus.yml      up -d
cd e2e && docker compose -f compose.mimir.yml           up -d
cd e2e && docker compose -f compose.victoriametrics.yml up -d
```

Or `make smoke` to run them sequentially in a CI-style harness.
Per the user-global convention, the file is `compose.yml`, not
`docker-compose.yml`.

## OpenSpec workflow

Active changes live under `openspec/changes/<name>/` (gitignored).
Slash-commands `/opsx:explore`, `/opsx:propose`, `/opsx:apply`,
`/opsx:archive` drive the propose-design-spec-tasks-implement-archive
loop. Schema is `spec-driven`: each active change has
`proposal.md`, `design.md`, `specs/**/*.md`, `tasks.md`.

Archived changes live under `openspec/changes/archive/YYYY-MM-DD-<name>/`.
The main spec at `openspec/specs/tss-plugin/spec.md` is updated on
archive when the change has non-empty deltas.

## Code review

`/bmad-code-review` runs three parallel reviewers (Blind Hunter, Edge
Case Hunter, Acceptance Auditor) over a diff. Findings get triaged into
PATCH / DEFER / DISMISS buckets. The skill is at
`.claude/skills/bmad-code-review/`.

## Wire format invariants worth knowing

- The plugin supports **both** Remote Write v1 and v2 wire formats,
  selected via `wire.protocol-version` (default `1`). v2 backends need
  Prometheus 3.0+ recommended — 2.50–2.54 ship an experimental v2
  receiver that can silently drop payloads.
- The **WAL is wire-version-agnostic** — it stores `MappedSample`
  (pre-wire), so flipping `wire.protocol-version` with pending WAL
  contents is safe; the next flush emits in the new version.
- Both v1 and v2 use snappy compression; only headers and payload
  protobuf type differ. See `RemoteWriteHttpClient` for the header
  branch and `RemoteWriteRequestBuilders.forVersion(int)` for the
  payload branch.
