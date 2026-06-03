#!/usr/bin/env bash
#
# Run the CH-benCH HTAP benchmark (Postgres CDC -> accelerator) via testoperator
# and capture time-series telemetry alongside the standard correctness/latency
# report:
#
#   * spiced CPU% + RSS               (ps, 5s)
#   * OLAP query latency              (scrape query_duration_ms from :METRICS, 15s)
#   * OLTP throughput                 (Postgres xact_commit delta, 15s)
#
# All telemetry is written as CSV under OUTDIR. A summary (peaks, latency/
# throughput over time, validation verdict, break signals) is printed at the end.
#
# Usage:
#   scripts/run-chbench-htap.sh [--help]
#
# Configuration (environment variables, with defaults):
#   SF=200                 scale factor (warehouses); terminals default to SF*10
#   DURATION=300           steady-state workload seconds
#   READY=2400             max seconds to wait for spiced to become ready
#   SPICEPOD=test/spicepods/chbench/accelerated/postgres-cayenne[file]-cdc-tuned.yaml
#   SPICED=target/debug/spiced          path to the spiced binary
#   TESTOP=target/debug/testoperator    path to the testoperator binary
#   OUTDIR=/tmp/chbench-<sf>             telemetry output directory
#   METRICS_PORT=9090      spiced Prometheus port (testoperator enables it)
#   PG_CONTAINER=chbench-pg PG_USER=bench PG_DB=chbench   for the OLTP sampler
#                          (set PG_CONTAINER="" to skip OLTP sampling)
#   SPICED_LOG=warn,cayenne::compaction=info,data_components=info,runtime=info
#
# Note: prefix with the sccache-bypass env when the workspace's sccache points
# at an unwritable volume:
#   env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
#       SCCACHE_DIR=$HOME/.cache/sccache cargo build -p spiced -p testoperator ...

set -u

case "${1:-}" in -h|--help) sed -n '2,40p' "$0"; exit 0;; esac

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT" || exit 2

SF="${SF:-200}"
DURATION="${DURATION:-300}"
READY="${READY:-2400}"
TERMINALS="${TERMINALS:-}"      # OLTP terminals; empty = testoperator default (SF*10)
CONCURRENCY="${CONCURRENCY:-}"  # OLAP query threads; empty = testoperator default
SPICEPOD="${SPICEPOD:-test/spicepods/chbench/accelerated/postgres-cayenne[file]-cdc-tuned.yaml}"
SPICED="${SPICED:-target/debug/spiced}"
TESTOP="${TESTOP:-target/debug/testoperator}"
OUTDIR="${OUTDIR:-/tmp/chbench-sf${SF}}"
METRICS_PORT="${METRICS_PORT:-9090}"
PG_CONTAINER="${PG_CONTAINER:-chbench-pg}"
PG_USER="${PG_USER:-bench}"
PG_DB="${PG_DB:-chbench}"
SPICED_LOG="${SPICED_LOG:-warn,cayenne::compaction=info,data_components=info,runtime=info}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/htap.log"
CPU="$OUTDIR/cpu.csv"
QLAT="$OUTDIR/qlat.csv"
OLTP="$OUTDIR/oltp.csv"
rm -f "$LOG" "$CPU" "$QLAT" "$OLTP"
echo "epoch,pct_cpu,rss_mb" > "$CPU"
echo "epoch,q_count,q_sum_ms,qx_count,qx_sum_ms" > "$QLAT"
echo "epoch,xact_commit,tx_per_sec" > "$OLTP"

wait_for_spiced() { for _ in $(seq 1 6000); do pgrep -x spiced >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

# --- spiced CPU% / RSS (5s) ---
( wait_for_spiced || exit 0
  pid=$(pgrep -x spiced | head -1)
  while kill -0 "$pid" 2>/dev/null; do
    read -r pct rss < <(ps -o %cpu=,rss= -p "$pid" 2>/dev/null)
    [ -n "${pct:-}" ] && echo "$(date +%s),$pct,$(( ${rss:-0} / 1024 ))" >> "$CPU"
    sleep 5
  done ) &
CPU_PID=$!

# --- OLAP query latency: cumulative count+sum of the query_duration_ms histograms (15s) ---
( wait_for_spiced || exit 0
  while pgrep -x spiced >/dev/null 2>&1; do
    m=$(curl -s --max-time 5 "http://localhost:${METRICS_PORT}/metrics" 2>/dev/null || true)
    if [ -n "$m" ]; then
      qc=$(echo "$m" | awk '/^query_duration_ms_count([ {]|$)/{s+=$NF} END{print s+0}')
      qs=$(echo "$m" | awk '/^query_duration_ms_sum([ {]|$)/{s+=$NF} END{print s+0}')
      xc=$(echo "$m" | awk '/^query_execution_duration_ms_count([ {]|$)/{s+=$NF} END{print s+0}')
      xs=$(echo "$m" | awk '/^query_execution_duration_ms_sum([ {]|$)/{s+=$NF} END{print s+0}')
      echo "$(date +%s),${qc:-0},${qs:-0},${xc:-0},${xs:-0}" >> "$QLAT"
    fi
    sleep 15
  done ) &
QLAT_PID=$!

# --- OLTP throughput: Postgres committed-transaction rate (15s) ---
OLTP_PID=""
if [ -n "$PG_CONTAINER" ]; then
  ( wait_for_spiced || exit 0
    px=0; pt=0
    while pgrep -x spiced >/dev/null 2>&1; do
      now=$(date +%s)
      x=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A \
            -c "select xact_commit from pg_stat_database where datname='$PG_DB';" 2>/dev/null | tr -d ' ')
      if [ -n "$x" ] && [ "$pt" -gt 0 ]; then
        tps=$(awk -v dx=$((x-px)) -v dt=$((now-pt)) 'BEGIN{print (dt>0)?dx/dt:0}')
        echo "$now,$x,$tps" >> "$OLTP"
      fi
      [ -n "$x" ] && { px=$x; pt=$now; }
      sleep 15
    done ) &
  OLTP_PID=$!
fi

echo "Running CH-benCH HTAP: SF=$SF duration=${DURATION}s ready-wait=${READY}s -> $OUTDIR"
SPICED_LOG="$SPICED_LOG" "$TESTOP" run htap \
  -p "$SPICEPOD" -s "$REPO_ROOT/$SPICED" \
  --query-set chbench --scale-factor "$SF" --validate \
  --ready-wait "$READY" --duration "$DURATION" \
  ${TERMINALS:+--terminals "$TERMINALS"} \
  ${CONCURRENCY:+--concurrency "$CONCURRENCY"} \
  --disable-progress-bars --scrape-spiced-metrics > "$LOG" 2>&1
echo "TESTOP_EXIT=$?"
kill "$CPU_PID" "$QLAT_PID" ${OLTP_PID:+$OLTP_PID} 2>/dev/null

strip() { sed -E 's/\x1b\[[0-9;]*m//g'; }
echo "===== CPU / RSS peaks ====="
awk -F, 'NR>1{if($2>mc)mc=$2; if($3>mr)mr=$3; n++} END{printf "samples=%d peak_cpu=%.0f%% peak_rss_mb=%d\n",n,mc,mr}' "$CPU"
echo "===== OLAP query latency over time (interval avg ms = d(sum)/d(count)) ====="
awk -F, 'NR>1{ if(pc>0 && $2>pc){dq=$2-pc; ds=$3-ps; printf "t=%s done=%d avg=%.0fms\n",$1,dq,(dq>0?ds/dq:0)} pc=$2; ps=$3 }' "$QLAT" | tail -30
echo "===== OLTP throughput over time (tx/sec), peak intervals ====="
sort -t, -k3 -n "$OLTP" 2>/dev/null | tail -8
awk -F, 'NR>1&&$3>m{m=$3}END{printf "peak_tx_per_sec=%.0f\n",m}' "$OLTP" 2>/dev/null
echo "===== break signals ====="
echo "panic=$(grep -ciE 'panic' "$LOG") oom=$(grep -ciE 'out of memory|cannot allocate|Killed' "$LOG") exhausted=$(grep -ci 'ResourcesExhausted' "$LOG")"
echo "===== validation + OLTP report ====="
strip < "$LOG" | grep -iE "verdict|all 7 tables|converged in|tpmc|throughput|transactions|OLTP workload error|match$" | tail -20
echo "DONE -> $OUTDIR"
