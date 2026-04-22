#!/usr/bin/env bash
# End-to-end smoke test. For each requested backend: bring the compose
# stack up with the locally-built KAR, wait for OpenNMS to start
# writing real samples, confirm the backend has ingested at least one
# series, and tear everything down.
#
# Usage:
#   ./smoke.sh                                # all three backends
#   ./smoke.sh prometheus                     # just one
#   ./smoke.sh mimir victoriametrics          # subset, any order
#
# Requires: docker compose v2, python3, curl. Assumes `make kar` has
# been run from the repo root — the compose stack mounts
# ../assembly/kar/target read-only into /opt/opennms/deploy.
#
# A backend passes if its series count goes above zero within TIMEOUT
# seconds. On failure (or user interrupt) the stack is torn down and
# volumes removed before the script exits non-zero.

set -euo pipefail

cd "$(dirname "$0")"

TIMEOUT="${TIMEOUT:-600}"   # per-backend deadline (seconds)
POLL_EVERY="${POLL_EVERY:-15}"

KAR_DIR="../assembly/kar/target"
if ! ls "$KAR_DIR"/*.kar >/dev/null 2>&1; then
    echo "ERROR: no KAR found under $KAR_DIR. Run 'make kar' first." >&2
    exit 2
fi

# --- Backend query helpers -----------------------------------------------
# Each echoes an integer count of stored series (0 if none yet / query
# not ready). Stderr is suppressed so transient "connection refused"
# during startup doesn't pollute the log.

count_prometheus() {
    curl -sfG 'http://localhost:9090/api/v1/query' \
        --data-urlencode 'query=count({__name__=~".+"})' 2>/dev/null \
        | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
    r = d["data"]["result"]
    print(int(r[0]["value"][1]) if r else 0)
except Exception:
    print(0)'
}

count_mimir() {
    curl -sfG 'http://localhost:9009/prometheus/api/v1/query' \
        -H 'X-Scope-OrgID: e2e' \
        --data-urlencode 'query=count({__name__=~".+"})' 2>/dev/null \
        | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
    r = d["data"]["result"]
    print(int(r[0]["value"][1]) if r else 0)
except Exception:
    print(0)'
}

count_victoriametrics() {
    curl -sfG 'http://localhost:8428/api/v1/query' \
        --data-urlencode 'query=count({__name__=~".+"})' 2>/dev/null \
        | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
    r = d["data"]["result"]
    print(int(r[0]["value"][1]) if r else 0)
except Exception:
    print(0)'
}

# --- One backend ---------------------------------------------------------

run_one() {
    local backend="$1"
    local file="compose.${backend}.yml"

    if [ ! -f "$file" ]; then
        echo "ERROR: $file not found" >&2
        return 2
    fi

    echo
    echo "=== [$backend] starting stack ==="

    # Ensure teardown on any exit path (success, failure, SIGINT).
    # Leave Compose's per-container "Stopped / Removed" output visible so
    # a slow shutdown doesn't look like a hang.
    #
    # INT/TERM are kept separate from RETURN because a bare trap would
    # run teardown and then let the polling loop resume — apparent hang
    # until TIMEOUT elapses. The signal handler tears down and exits 130,
    # clearing RETURN first so teardown doesn't run twice on the way out.
    trap "echo '=== [$backend] tearing down ==='; \
          docker compose -f '$file' down -v --remove-orphans || true" \
        RETURN
    trap "trap - RETURN; \
          echo; echo '=== [$backend] interrupted, tearing down ==='; \
          docker compose -f '$file' down -v --remove-orphans || true; \
          exit 130" \
        INT TERM

    docker compose -f "$file" down -v --remove-orphans >/dev/null 2>&1 || true
    docker compose -f "$file" up -d >/dev/null

    echo "=== [$backend] waiting up to ${TIMEOUT}s for first samples ==="
    local start=$SECONDS
    local count=0
    while (( SECONDS - start < TIMEOUT )); do
        count=$("count_${backend}")
        if (( count > 0 )); then
            echo "=== [$backend] PASS: ${count} series in $((SECONDS - start))s ==="
            return 0
        fi
        sleep "$POLL_EVERY"
    done

    echo "=== [$backend] FAIL: no samples within ${TIMEOUT}s ===" >&2
    echo "--- last 40 lines of core karaf.log ---" >&2
    docker compose -f "$file" exec -T core \
        tail -n 40 /opt/opennms/logs/karaf.log >&2 || true
    return 1
}

# --- Main loop -----------------------------------------------------------

if [ $# -eq 0 ]; then
    set -- prometheus mimir victoriametrics
fi

failed=()
passed=()
for backend in "$@"; do
    if run_one "$backend"; then
        passed+=("$backend")
    else
        failed+=("$backend")
    fi
done

echo
echo "=== SUMMARY ==="
for b in "${passed[@]:-}"; do [ -n "$b" ] && echo "  PASS  $b"; done
for b in "${failed[@]:-}"; do [ -n "$b" ] && echo "  FAIL  $b"; done

[ ${#failed[@]} -eq 0 ]
