#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Phase 2 end-to-end: start two origins (p1 :9001, p2 :9002) and a spiced with
# per-origin adaptive rate control ON, then run a named rate-control scenario
# (a background timeline steps the p2 fault profile on a shared clock while an
# open-loop load generator drives both origins and a scraper samples
# /metrics) and score it with the rate-control oracle. Exit code is the oracle
# verdict: 0 = PASS, 1 = FAIL, 2 = BLOCKED.
#
# IMPORTANT: adaptive_rate_control is gated behind the `rate-control` cargo
# feature, which is NOT in the default feature set. The prebuilt
# ~/.spice/bin/spiced does NOT contain it. Point SPICED_BIN at a binary built
# from the 14136-adaptive-rate-control branch:
#   cargo build --release -p spiced --features "rate-control,duckdb"
# (default below is that branch build's target/release/spiced.)
#
# Usage:
#   ./run_phase2.sh ratecontrol-sre
#   ./run_phase2.sh ratecontrol-cooldown
#   ./run_phase2.sh ratecontrol-ietf-headers
#   ./run_phase2.sh --probe ratecontrol-sre   # start, scrape /metrics once, exit
set -euo pipefail

PROBE=0
if [[ "${1:-}" == "--probe" ]]; then PROBE=1; shift; fi
SCENARIO="${1:-ratecontrol-sre}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Default to the branch build in the main checkout (has the rate-control
# feature); override with SPICED_BIN=... for a different binary.
BRANCH_BUILD="$HOME/Github/spiceai/target/release/spiced"
if [[ -n "${SPICED_BIN:-}" ]]; then
  :
elif [[ -x "$BRANCH_BUILD" ]]; then
  SPICED_BIN="$BRANCH_BUILD"
else
  SPICED_BIN="$HOME/.spice/bin/spiced"
fi

if [[ -x "$HERE/../../../.venv/bin/python" ]]; then
  PY="${PY:-$HERE/../../../.venv/bin/python}"
else
  PY="${PY:-$HERE/.venv/bin/python}"
fi

P1_PORT="${P1_PORT:-9001}"
P2_PORT="${P2_PORT:-9002}"
HTTP_PORT="${HTTP_PORT:-8090}"
METRICS_PORT="${METRICS_PORT:-9090}"
RUN_DIR="${RUN_DIR:-/tmp/http_cache_phase2_run/$SCENARIO}"
P1_LOG="${P1_LOG:-$RUN_DIR/origin_p1.jsonl}"
P2_LOG="${P2_LOG:-$RUN_DIR/origin_p2.jsonl}"

case "$SCENARIO" in
  ratecontrol-sre|ratecontrol-cooldown|ratecontrol-ietf-headers)
    POD="$HERE/spicepod/spicepod.ratecontrol.sre.yaml" ;;
  *) echo "unknown scenario: $SCENARIO"; exit 2 ;;
esac

mkdir -p "$RUN_DIR"
: > "$P1_LOG"
: > "$P2_LOG"
T0="$($PY -c 'import time; print(time.time())')"

pids=()
cleanup() { for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "[run] scenario=$SCENARIO  spiced=$SPICED_BIN"
echo "[run] starting origin p1 on :$P1_PORT and p2 on :$P2_PORT"
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

# Preflight: a dataset that failed to register with rate control (e.g. adaptive
# enabled but no static limit) leaves the feature untestable. Surface an
# InvalidConfiguration at load rather than mislabelling the run.
if grep -Eiq "adaptive_rate_control.*(invalid|no rate limit)|no rate limit is set for this origin" "$RUN_DIR/spiced.log"; then
  echo "[run] WARNING: spiced logged an adaptive rate-control configuration error:"
  grep -Ei "adaptive_rate_control|no rate limit is set for this origin" "$RUN_DIR/spiced.log" | tail -3
fi

# Snapshot config + version for the run directory.
cp "$POD" "$RUN_DIR/spicepod.resolved.yaml" 2>/dev/null || true
"$SPICED_BIN" --version > "$RUN_DIR/spiced.version" 2>&1 || true
( cd "$HOME/Github/spiceai" && git rev-parse HEAD 2>/dev/null ) > "$RUN_DIR/spiced.commit" 2>&1 || true

if [[ "$PROBE" == "1" ]]; then
  echo "[run] PROBE: scraping /metrics once to pin metric names and origin labels"
  # Drive a few queries so the per-origin instruments have been created.
  for _ in $(seq 1 10); do
    curl -s -XPOST "http://127.0.0.1:$HTTP_PORT/v1/sql" -H 'Content-Type: text/plain' \
      --data "SELECT version FROM d2 WHERE origin='p2'" >/dev/null 2>&1 || true
    curl -s -XPOST "http://127.0.0.1:$HTTP_PORT/v1/sql" -H 'Content-Type: text/plain' \
      --data "SELECT version FROM d1 WHERE origin='p1'" >/dev/null 2>&1 || true
  done
  sleep 1
  METRICS_ENDPOINT="http://127.0.0.1:$METRICS_PORT/metrics" \
    "$PY" "$HERE/loadgen/metrics_scraper.py" \
    --endpoint "http://127.0.0.1:$METRICS_PORT/metrics" | tee "$RUN_DIR/metrics_probe.txt"
  echo "[run] raw /metrics adaptive/rate lines:"
  curl -s "http://127.0.0.1:$METRICS_PORT/metrics" | grep -Ei "adaptive_rate_control|rate_control_|rate_limit_retry_after" | tee "$RUN_DIR/metrics_raw_ratecontrol.txt"
  exit 0
fi

echo "[run] driving scenario $SCENARIO"
SPICED_SQL_URL="http://127.0.0.1:$HTTP_PORT/v1/sql" \
  METRICS_ENDPOINT="http://127.0.0.1:$METRICS_PORT/metrics" \
  P1_CONTROL_URL="http://127.0.0.1:$P1_PORT/control" \
  P2_CONTROL_URL="http://127.0.0.1:$P2_PORT/control" \
  P1_STATS_URL="http://127.0.0.1:$P1_PORT/stats" \
  P2_STATS_URL="http://127.0.0.1:$P2_PORT/stats" \
  P1_REQUEST_LOG="$P1_LOG" P2_REQUEST_LOG="$P2_LOG" \
  P1_PORT="$P1_PORT" P2_PORT="$P2_PORT" \
  OUT_DIR="$RUN_DIR" \
  "$PY" "$HERE/loadgen/run_phase2.py" --scenario "$SCENARIO" --t0 "$T0" \
  && rc=0 || rc=$?

echo "[run] done (verdict rc=${rc:-?}: 0=PASS 1=FAIL 2=BLOCKED); artifacts in $RUN_DIR"
exit "${rc:-1}"
