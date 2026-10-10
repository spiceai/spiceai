#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Phase 0 end-to-end: start the origin and a caching spiced, wait until
# both are ready, drive the load generator / freshness oracle, then tear
# everything down. Exit code is the oracle verdict.
#
# Prereqs:
#   - a spiced binary on PATH or at ~/.spice/bin/spiced
#   - the origin virtualenv at ./.venv (python -m venv .venv &&
#     ./.venv/bin/pip install -r requirements.txt)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

SPICED_BIN="${SPICED_BIN:-$HOME/.spice/bin/spiced}"
PY="${PY:-$HERE/.venv/bin/python}"
ORIGIN_PORT="${ORIGIN_PORT:-9001}"
HTTP_PORT="${HTTP_PORT:-8090}"
METRICS_PORT="${METRICS_PORT:-9090}"
QPS="${QPS:-10}"
DURATION_S="${DURATION_S:-20}"
RUN_DIR="${RUN_DIR:-/tmp/http_cache_phase0_run}"
ORIGIN_LOG="${ORIGIN_LOG:-/tmp/origin_p1.jsonl}"

mkdir -p "$RUN_DIR"
: > "$ORIGIN_LOG"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "[run] starting origin on :$ORIGIN_PORT"
ORIGIN_NAME=p1 ORIGIN_BUMP_INTERVAL="${BUMP_INTERVAL_S:-1.0}" \
  ORIGIN_REQUEST_LOG="$ORIGIN_LOG" \
  "$PY" -m uvicorn origin.server:app --host 127.0.0.1 --port "$ORIGIN_PORT" \
  --log-level warning >"$RUN_DIR/origin.stdout.log" 2>&1 &
pids+=("$!")

# Wait for the origin health probe.
for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$ORIGIN_PORT/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.2
done
curl -sf "http://127.0.0.1:$ORIGIN_PORT/healthz" >/dev/null || { echo "origin failed to start"; exit 2; }

echo "[run] starting spiced on :$HTTP_PORT"
"$SPICED_BIN" ./spicepod/spicepod.swr.yaml \
  --http "127.0.0.1:$HTTP_PORT" --metrics "127.0.0.1:$METRICS_PORT" \
  >"$RUN_DIR/spiced.log" 2>&1 &
pids+=("$!")

# Wait for the runtime ready line.
for _ in $(seq 1 100); do
  if grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" 2>/dev/null; then break; fi
  sleep 0.3
done
grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" || { echo "spiced failed to become ready"; tail -20 "$RUN_DIR/spiced.log"; exit 2; }

# A short warm-up: the first filtered query primes the cold cache.
curl -s -X POST "http://127.0.0.1:$HTTP_PORT/v1/sql" -H 'Content-Type: text/plain' \
  -d "SELECT version FROM d1 WHERE origin = 'p1'" >/dev/null || true

echo "[run] driving load: qps=$QPS duration=${DURATION_S}s"
: > "$ORIGIN_LOG"  # reset the request log so the oracle counts only run-window fetches
SPICED_SQL_URL="http://127.0.0.1:$HTTP_PORT/v1/sql" \
  ORIGIN_STATS_URL="http://127.0.0.1:$ORIGIN_PORT/stats" \
  ORIGIN_REQUEST_LOG="$ORIGIN_LOG" \
  QPS="$QPS" DURATION_S="$DURATION_S" OUT_DIR="$RUN_DIR" \
  "$PY" "$HERE/loadgen/run_phase0.py" && rc=0 || rc=$?

echo "[run] done (verdict rc=$rc); artifacts in $RUN_DIR"
exit $rc
