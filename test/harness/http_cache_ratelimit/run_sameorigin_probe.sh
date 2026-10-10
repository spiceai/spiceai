#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Same-origin coupling probe: does client-side rate control couple two
# DATASETS that share one upstream ORIGIN (same host:port, different path),
# and leave a dataset on a DIFFERENT origin alone? Starts p1 (:9001, control
# origin) and p2 (:9002, hosts both d2's faulted path and d3's always-healthy
# path), a spiced against spicepod.ratecontrol.sameorigin.yaml, then drives
# loadgen/run_sameorigin_probe.py. Exit code is the probe's verdict: 0 = PASS
# (all three assertions held), 1 = FAIL.
#
# Usage: SPICED_BIN=... ./run_sameorigin_probe.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

SPICED_BIN="${SPICED_BIN:-$HOME/.spice/bin/spiced}"
if [[ -x "$HERE/.venv/bin/python" ]]; then
  PY="${PY:-$HERE/.venv/bin/python}"
else
  PY="${PY:-python3}"
fi

P1_PORT="${P1_PORT:-9001}"
P2_PORT="${P2_PORT:-9002}"
HTTP_PORT="${HTTP_PORT:-8090}"
METRICS_PORT="${METRICS_PORT:-9090}"
RUN_DIR="${RUN_DIR:-/tmp/http_cache_sameorigin_run}"
P1_LOG="$RUN_DIR/origin_p1.jsonl"
P2_LOG="$RUN_DIR/origin_p2.jsonl"
POD="$HERE/spicepod/spicepod.ratecontrol.sameorigin.yaml"

mkdir -p "$RUN_DIR"
: > "$P1_LOG"
: > "$P2_LOG"
T0="$($PY -c 'import time; print(time.time())')"

pids=()
cleanup() { for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "[run] spiced=$SPICED_BIN"
echo "[run] starting origin p1 on :$P1_PORT and p2 on :$P2_PORT (p2 hosts both d2 and d3)"
ORIGIN_NAME=p1 ORIGIN_PORT="$P1_PORT" ORIGIN_BUMP_INTERVAL="${BUMP_INTERVAL_S:-1.0}" \
  HARNESS_T0="$T0" ORIGIN_REQUEST_LOG="$P1_LOG" \
  "$PY" "$HERE/origin/server.py" >"$RUN_DIR/origin_p1.stdout.log" 2>&1 &
pids+=("$!")
ORIGIN_NAME=p2 ORIGIN_PORT="$P2_PORT" ORIGIN_BUMP_INTERVAL="${BUMP_INTERVAL_S:-1.0}" \
  HARNESS_T0="$T0" ORIGIN_REQUEST_LOG="$P2_LOG" \
  "$PY" "$HERE/origin/server.py" >"$RUN_DIR/origin_p2.stdout.log" 2>&1 &
pids+=("$!")

for port in "$P1_PORT" "$P2_PORT"; do
  for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && break
    sleep 0.2
  done
  curl -sf "http://127.0.0.1:$port/healthz" >/dev/null || { echo "origin :$port failed to start"; exit 2; }
done

echo "[run] starting spiced on :$HTTP_PORT metrics :$METRICS_PORT (pod $(basename "$POD"))"
"$SPICED_BIN" "$POD" \
  --http "127.0.0.1:$HTTP_PORT" --metrics "127.0.0.1:$METRICS_PORT" \
  >"$RUN_DIR/spiced.log" 2>&1 &
pids+=("$!")

for _ in $(seq 1 200); do
  grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" 2>/dev/null && break
  sleep 0.3
done
grep -q "Spice runtime is ready" "$RUN_DIR/spiced.log" || { echo "spiced failed to become ready"; tail -30 "$RUN_DIR/spiced.log"; exit 2; }

cp "$POD" "$RUN_DIR/spicepod.resolved.yaml" 2>/dev/null || true
"$SPICED_BIN" --version > "$RUN_DIR/spiced.version" 2>&1 || true

echo "[run] driving same-origin probe"
SPICED_SQL_URL="http://127.0.0.1:$HTTP_PORT/v1/sql" \
  METRICS_ENDPOINT="http://127.0.0.1:$METRICS_PORT/metrics" \
  P1_CONTROL_URL="http://127.0.0.1:$P1_PORT/control" \
  P2_CONTROL_URL="http://127.0.0.1:$P2_PORT/control" \
  OUT_DIR="$RUN_DIR" \
  "$PY" "$HERE/loadgen/run_sameorigin_probe.py" --t0 "$T0" \
  && rc=0 || rc=$?

echo "[run] done (verdict rc=${rc:-?}: 0=PASS 1=FAIL); artifacts in $RUN_DIR"
exit "${rc:-1}"
