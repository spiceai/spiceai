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
#   SPICEPOD=test/spicepods/chbench/accelerated/postgres-cayenne[file]-cdc-tuned-sf100.yaml
#   CDC_MODE=native        set to debezium or debezium-preloaded for v1.11.x spiced
#   SPICED=target/debug/spiced          path to the spiced binary
#   TESTOP=target/debug/testoperator    path to the testoperator binary
#   OUTDIR=/tmp/chbench-<sf>             telemetry output directory
#   METRICS_PORT=9090      spiced Prometheus port (testoperator enables it)
#   METRICS_DUMP=$OUTDIR/metrics.json   JSON dump for scripts/chbench-waterfall.py
#   PG_CONTAINER=chbench-pg  OLTP sampler source; set PG_CONTAINER="" to sample a
#                          NATIVE Postgres via local psql (PG_HOST/PG_PORT/PG_USER/
#                          PG_PASS/PG_DB, default 127.0.0.1:5432 bench/bench/chbench)
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
SKIP_PREPARE="${SKIP_PREPARE:-}"  # non-empty => reuse an already-seeded source
                                  # (testoperator --skip-prepare); SF must match.
SPICEPOD="${SPICEPOD:-test/spicepods/chbench/accelerated/postgres-cayenne[file]-cdc-tuned-sf100.yaml}"
CDC_MODE="${CDC_MODE:-native}"
SPICED="${SPICED:-target/debug/spiced}"
TESTOP="${TESTOP:-target/debug/testoperator}"
OUTDIR="${OUTDIR:-/tmp/chbench-sf${SF}}"
METRICS_PORT="${METRICS_PORT:-9090}"
# OLTP sampler source. With PG_CONTAINER set, samples via `docker exec`. Set
# PG_CONTAINER="" to use a NATIVE Postgres instead (better perf): the sampler
# then runs local `psql` against PG_HOST:PG_PORT. Either way testoperator itself
# connects over TCP to CHBENCH_PG_* (defaults 127.0.0.1:5432 bench/bench/chbench).
PG_CONTAINER="${PG_CONTAINER:-chbench-pg}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-bench}"
PG_PASS="${PG_PASS:-bench}"
PG_DB="${PG_DB:-chbench}"
# Path for the machine-readable metrics dump the waterfall analysis consumes.
METRICS_DUMP="${METRICS_DUMP:-$OUTDIR/metrics.json}"
SPICED_LOG="${SPICED_LOG:-warn,cayenne::compaction=info,data_components=info,runtime=info}"

# The spicepods resolve pg_host from ${env:CHBENCH_PG_HOST}; export it (and the
# rest) so spiced's CDC datasource points at the same Postgres the sampler uses.
# testoperator's own source connection reads these too (defaults 127.0.0.1:5432).
export CHBENCH_PG_HOST="${CHBENCH_PG_HOST:-$PG_HOST}"
export CHBENCH_PG_PORT="${CHBENCH_PG_PORT:-$PG_PORT}"
export CHBENCH_PG_USER="${CHBENCH_PG_USER:-$PG_USER}"
export CHBENCH_PG_PASS="${CHBENCH_PG_PASS:-$PG_PASS}"
export CHBENCH_PG_DB="${CHBENCH_PG_DB:-$PG_DB}"

mkdir -p "$OUTDIR"
LOG="$OUTDIR/htap.log"
CPU="$OUTDIR/cpu.csv"
QLAT="$OUTDIR/qlat.csv"
OLTP="$OUTDIR/oltp.csv"
rm -f "$LOG" "$CPU" "$QLAT" "$OLTP"
echo "epoch,pct_cpu,rss_mb" > "$CPU"
echo "epoch,q_count,q_sum_ms,qx_count,qx_sum_ms" > "$QLAT"
echo "epoch,xact_commit,tx_per_sec" > "$OLTP"

wait_for_spiced() {
  local deadline=$((SECONDS + READY))
  while [ "$SECONDS" -lt "$deadline" ]; do
    pgrep -x spiced >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# --- spiced CPU% / RSS (5s); re-resolve the PID each tick so it survives a
#     spiced restart / PID reuse rather than tracking a stale (or reused) PID. ---
( wait_for_spiced || exit 0
  while pgrep -x spiced >/dev/null 2>&1; do
    pid=$(pgrep -x spiced | head -1)
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
# Sample via `docker exec` when PG_CONTAINER is set, else via native local psql
# (PG_CONTAINER="" + a `psql` on PATH). Empty container AND no psql => skip.
OLTP_PID=""
oltp_xact_query="select xact_commit from pg_stat_database where datname='${PG_DB}';"
if [ -n "$PG_CONTAINER" ]; then
  oltp_sample() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A -c "$oltp_xact_query" 2>/dev/null | tr -d ' '; }
  OLTP_SAMPLE_OK=1
elif command -v psql >/dev/null 2>&1; then
  oltp_sample() { PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -A -c "$oltp_xact_query" 2>/dev/null | tr -d ' '; }
  OLTP_SAMPLE_OK=1
else
  OLTP_SAMPLE_OK=""
fi
if [ -n "$OLTP_SAMPLE_OK" ]; then
  ( wait_for_spiced || exit 0
    px=0; pt=0
    while pgrep -x spiced >/dev/null 2>&1; do
      now=$(date +%s)
      x=$(oltp_sample)
      if [ -n "$x" ] && [ "$pt" -gt 0 ]; then
        tps=$(awk -v dx=$((x-px)) -v dt=$((now-pt)) 'BEGIN{print (dt>0)?dx/dt:0}')
        echo "$now,$x,$tps" >> "$OLTP"
      fi
      [ -n "$x" ] && { px=$x; pt=$now; }
      sleep 15
    done ) &
  OLTP_PID=$!
fi

echo "Running CH-benCH HTAP: SF=$SF duration=${DURATION}s ready-wait=${READY}s cdc-mode=${CDC_MODE} -> $OUTDIR"
# Pipe output to terminal and to file
SPICED_LOG="$SPICED_LOG" "$TESTOP" run htap \
  -p "$SPICEPOD" -s "$REPO_ROOT/$SPICED" \
  --query-set chbench --scale-factor "$SF" --validate \
  --ready-wait "$READY" --duration "$DURATION" \
  --cdc-mode "$CDC_MODE" \
  ${TERMINALS:+--terminals "$TERMINALS"} \
  ${CONCURRENCY:+--concurrency "$CONCURRENCY"} \
  ${SKIP_PREPARE:+--skip-prepare} \
  --metrics-dump "$METRICS_DUMP" \
  --disable-progress-bars --scrape-spiced-metrics 2>&1 | tee "$LOG"
testop_exit=${PIPESTATUS[0]}
echo "TESTOP_EXIT=$testop_exit"
kill "$CPU_PID" "$QLAT_PID" ${OLTP_PID:+$OLTP_PID} 2>/dev/null

strip() { sed -E 's/\x1b\[[0-9;]*m//g'; }
echo "===== CPU / RSS peaks ====="
awk -F, 'NR>1{if($2>mc)mc=$2; if($3>mr)mr=$3; n++} END{printf "samples=%d peak_cpu=%.0f%% peak_rss_mb=%d\n",n,mc,mr}' "$CPU"
echo "===== OLAP query latency over time (interval avg ms = d(sum)/d(count)) ====="
awk -F, 'NR>1{ if(pc>0 && $2>pc){dq=$2-pc; ds=$3-ps; printf "t=%s done=%d avg=%.0fms\n",$1,dq,(dq>0?ds/dq:0)} pc=$2; ps=$3 }' "$QLAT" | tail -30
echo "===== OLTP throughput over time (tx/sec), peak intervals ====="
awk -F, 'NR>1' "$OLTP" 2>/dev/null | sort -t, -k3 -n | tail -8
awk -F, 'NR>1&&$3>m{m=$3}END{printf "peak_tx_per_sec=%.0f\n",m}' "$OLTP" 2>/dev/null
echo "===== break signals ====="
echo "panic=$(grep -ciE 'panic' "$LOG") oom=$(grep -ciE 'out of memory|cannot allocate|Killed' "$LOG") exhausted=$(grep -ci 'ResourcesExhausted' "$LOG")"
echo "===== validation + OLTP report ====="
strip < "$LOG" | grep -iE "verdict|all 7 tables|converged in|tpmc|throughput|transactions|OLTP workload error|match$" | tail -20
echo "===== CDC backpressure waterfall ====="
if [ -f "$METRICS_DUMP" ]; then
  python3 "$REPO_ROOT/scripts/chbench-waterfall.py" "$METRICS_DUMP" 2>&1 || \
    echo "(waterfall failed; inspect $METRICS_DUMP directly)"
else
  echo "(no metrics dump at $METRICS_DUMP — run may have exited early)"
fi
echo "DONE -> $OUTDIR  (metrics dump: $METRICS_DUMP)"
exit "$testop_exit"
