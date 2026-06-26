#!/usr/bin/env bash
# Post-run CH-benCH query diagnostics.
#
# The htap step spawns (and tears down) its own spiced. This script re-launches
# spiced standalone against the data that run left on disk (no re-seed), then for
# each chbench query captures:
#   explain_structure.txt  - EXPLAIN (logical + physical plan shape)        -> debug any plan change
#   explain_analyze.txt     - EXPLAIN ANALYZE (per-operator rows/timings)    -> where time goes (best-effort)
#   decline_map.txt         - the eager-aggregation rule's accept/decline    -> heuristic signal
#                             (push_rows / grouped / join_out per query)
#
# Best-effort: this is a diagnostics artifact, so it never fails the job
# (`continue-on-error` on the step) and tolerates a not-fully-ready spiced.
#
# Env (from the workflow step):
#   SPICED_BIN                path to spiced            (default /usr/local/bin/spiced)
#   SPICEPOD_PATH             the spicepod used for the run               (required)
#   QUERY_DIR                 dir containing qN.sql                       (required)
#   OUT_DIR                   artifact output dir                         (required)
#   SPICED_EAGER_AGGREGATION  '1' (on) or '' (off) — mirrors the run
#   CHBENCH_PG_HOST, ...      the run's PG env (inherited)
set -uo pipefail

SPICED_BIN="${SPICED_BIN:-/usr/local/bin/spiced}"
POD="${SPICEPOD_PATH:?SPICEPOD_PATH required}"
QUERY_DIR="${QUERY_DIR:?QUERY_DIR required}"
OUT_DIR="${OUT_DIR:?OUT_DIR required}"
mkdir -p "$OUT_DIR"
SLOG="$OUT_DIR/spiced_explain.log"
STRUCT="$OUT_DIR/explain_structure.txt"
ANALYZE="$OUT_DIR/explain_analyze.txt"
DECLINE="$OUT_DIR/decline_map.txt"
: > "$STRUCT"; : > "$ANALYZE"; : > "$DECLINE"
HTTP="http://127.0.0.1:8090/v1/sql"

# Turn on the eager-aggregation rule's decision logging so its accept/decline
# lines (with push_rows/grouped/join_out) land in the spiced log. Mirror the
# run's on/off state so the captured plans match what was benchmarked.
export SPICED_LOG="info,eager_aggregation=debug"
echo "capture: SPICED_EAGER_AGGREGATION='${SPICED_EAGER_AGGREGATION:-}'  pod=$POD" | tee "$DECLINE"

nohup "$SPICED_BIN" "$POD" \
  --http 127.0.0.1:8090 --flight 127.0.0.1:50051 --metrics 127.0.0.1:9090 \
  > "$SLOG" 2>&1 &
SPID=$!
trap 'kill "$SPID" 2>/dev/null || true' EXIT

# Wait for spiced to come up against the existing acceleration (bounded ~15 min).
ready=0
for _ in $(seq 1 180); do
  if curl -fsS -m 5 http://127.0.0.1:8090/health >/dev/null 2>&1 \
     && grep -qaE "Dataset load summary.*ready, 0 unhealthy|All datasets are ready" "$SLOG" 2>/dev/null; then
    ready=1; break
  fi
  kill -0 "$SPID" 2>/dev/null || { echo "WARN: spiced exited during startup" | tee -a "$DECLINE"; break; }
  sleep 5
done
[ "$ready" = 1 ] && echo "spiced ready" | tee -a "$DECLINE" \
  || echo "WARN: spiced not fully ready after timeout — capturing best-effort" | tee -a "$DECLINE"

strip() { sed -E 's/\x1b\[[0-9;]*m//g'; }
for n in $(seq 1 22); do
  q="q$n"; f="$QUERY_DIR/$q.sql"; [ -f "$f" ] || continue
  sql="$(cat "$f")"
  off=$(wc -l < "$SLOG" 2>/dev/null || echo 0)

  plan=$(curl -s -m 60 -X POST "$HTTP" -H "Content-Type: text/plain" --data "EXPLAIN $sql" 2>/dev/null)
  { echo "===================== $q ====================="; printf '%s\n\n' "$plan"; } >> "$STRUCT"

  { echo "===================== $q ====================="
    curl -s -m 300 -X POST "$HTTP" -H "Content-Type: text/plain" --data "EXPLAIN ANALYZE $sql" 2>/dev/null
    echo; } >> "$ANALYZE"

  sleep 1  # let the planner's debug lines flush
  fired=$(printf '%s' "$plan" | grep -aoE "__eager_[pm][0-9]" | sort -u | wc -l | tr -d ' ')
  reasons=$(tail -n +"$((off+1))" "$SLOG" 2>/dev/null | strip \
            | grep -a "eager_aggregation:" | sed -E 's/.*eager_aggregation: ?//' | sort | uniq -c)
  { echo "===== $q (eager_pushes=$fired) ====="
    if [ -n "$reasons" ]; then echo "$reasons" | sed 's/^/    /'; else echo "    (no rule log lines)"; fi
  } >> "$DECLINE"
done

echo "" >> "$DECLINE"
echo "fired (eager_pushes>=1): $(grep -cE 'eager_pushes=[1-9]' "$DECLINE" 2>/dev/null)/22" | tee -a "$DECLINE"
echo "capture complete -> $OUT_DIR"
