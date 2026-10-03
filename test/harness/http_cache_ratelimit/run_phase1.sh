#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Phase 1 end-to-end: start one origin and a caching spiced with
# stale-if-error ON, then run a named fault scenario (a background timeline
# steps the origin fault profile on a shared clock while the load generator
# polls the cache) and score it with the SIE oracle. Exit code is the
# oracle verdict: 0 = PASS, 1 = FAIL, 2 = BLOCKED / PENDING.
#
# Usage:
#   ./run_phase1.sh caching-sie-timeout     # MANDATORY; PASS on prebuilt
#   ./run_phase1.sh caching-sie-refuse      # PASS on prebuilt
#   ./run_phase1.sh caching-sie-503         # BLOCKED on prebuilt (empty-on-503)
#   ./run_phase1.sh caching-sie-expiry      # PENDING (duration SIE not in prebuilt)
#
# Prereqs:
#   - a spiced binary on PATH or at ~/.spice/bin/spiced (prebuilt is fine)
#   - the virtualenv at ../../../.venv or ./.venv (see README)
set -euo pipefail

SCENARIO="${1:-caching-sie-timeout}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

SPICED_BIN="${SPICED_BIN:-$HOME/.spice/bin/spiced}"
# Prefer a repo-root .venv, else a harness-local one.
if [[ -x "$HERE/../../../.venv/bin/python" ]]; then
  PY="${PY:-$HERE/../../../.venv/bin/python}"
else
  PY="${PY:-$HERE/.venv/bin/python}"
fi
ORIGIN_PORT="${ORIGIN_PORT:-9001}"
HTTP_PORT="${HTTP_PORT:-8090}"
METRICS_PORT="${METRICS_PORT:-9090}"
RUN_DIR="${RUN_DIR:-/tmp/http_cache_phase1_run/$SCENARIO}"
ORIGIN_LOG="${ORIGIN_LOG:-$RUN_DIR/origin_p1.jsonl}"

case "$SCENARIO" in
  caching-sie-503|caching-sie-timeout|caching-sie-refuse)
    POD="$HERE/spicepod/spicepod.sie.yaml" ;;
  caching-sie-expiry)
    POD="$HERE/spicepod/spicepod.sie.expiry.yaml" ;;
  *)
    echo "unknown scenario: $SCENARIO"; exit 2 ;;
esac

mkdir -p "$RUN_DIR"
: > "$ORIGIN_LOG"
T0="$($PY -c 'import time; print(time.time())')"

pids=()
cleanup() { for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "[run] scenario=$SCENARIO  spiced=$SPICED_BIN"
echo "[run] starting origin on :$ORIGIN_PORT"
ORIGIN_NAME=p1 ORIGIN_PORT="$ORIGIN_PORT" ORIGIN_BUMP_INTERVAL="${BUMP_INTERVAL_S:-1.0}" \
  HARNESS_T0="$T0" ORIGIN_REQUEST_LOG="$ORIGIN_LOG" \
  "$PY" "$HERE/origin/server.py" >"$RUN_DIR/origin.stdout.log" 2>&1 &
pids+=("$!")

for _ in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$ORIGIN_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "http://127.0.0.1:$ORIGIN_PORT/healthz" >/dev/null || { echo "origin failed to start"; exit 2; }

echo "[run] starting spiced on :$HTTP_PORT (pod $(basename "$POD"))"
"$SPICED_BIN" "$POD" \
  --http "127.0.0.1:$HTTP_PORT" --metrics "127.0.0.1:$METRICS_PORT" \
  >"$RUN_DIR/spiced.log" 2>&1 &
pids+=("$!")

for _ in $(seq 1 100); do
  grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" 2>/dev/null && break
  sleep 0.3
done
grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" || { echo "spiced failed to become ready"; tail -20 "$RUN_DIR/spiced.log"; exit 2; }

# Preflight: the caching_stale_if_error duration form is rejected at load by
# the prebuilt binary, so the dataset never registers. Detect that and
# report the scenario PENDING instead of running a load against a dataset
# that does not exist.
if grep -q "Invalid 'caching_stale_if_error' value" "$RUN_DIR/spiced.log"; then
  echo "[run] BLOCKED / PENDING: this spiced rejects the duration-bounded"
  echo "      caching_stale_if_error window (needs #14126). Scenario $SCENARIO"
  echo "      cannot run on this binary."
  grep "Invalid 'caching_stale_if_error' value" "$RUN_DIR/spiced.log" | tail -1
  cat > "$RUN_DIR/assertions.json" <<EOF
{"scenario": "$SCENARIO", "verdict": "BLOCKED", "exit_code": 2,
 "blocked_reason": "prebuilt spiced rejects a duration caching_stale_if_error value; duration-bounded SIE (#14126) is not in this binary"}
EOF
  exit 2
fi

echo "[run] driving scenario $SCENARIO"
SPICED_SQL_URL="http://127.0.0.1:$HTTP_PORT/v1/sql" \
  ORIGIN_CONTROL_URL="http://127.0.0.1:$ORIGIN_PORT/control" \
  ORIGIN_STATS_URL="http://127.0.0.1:$ORIGIN_PORT/stats" \
  ORIGIN_REQUEST_LOG="$ORIGIN_LOG" \
  OUT_DIR="$RUN_DIR" \
  "$PY" "$HERE/loadgen/run_phase1.py" --scenario "$SCENARIO" --t0 "$T0" \
  && rc=0 || rc=$?

# Snapshot the resolved config for the run directory.
cp "$POD" "$RUN_DIR/spicepod.resolved.yaml" 2>/dev/null || true
"$SPICED_BIN" --version > "$RUN_DIR/spiced.version" 2>&1 || true

echo "[run] done (verdict rc=$rc: 0=PASS 1=FAIL 2=BLOCKED); artifacts in $RUN_DIR"
exit $rc
