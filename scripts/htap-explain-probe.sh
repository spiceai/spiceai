#!/usr/bin/env bash
#
# htap-explain-probe.sh — background sampler that captures EXPLAIN VERBOSE +
# EXPLAIN ANALYZE plans for CH-benCH queries against a running spiced, on an
# interval, for HTAP performance troubleshooting.
#
# Queries run through spiced's HTTP SQL API (/v1/sql). EXPLAIN VERBOSE only
# plans (cheap), but EXPLAIN ANALYZE executes the full query, so this probe adds
# analytical load and WILL perturb the run's QPH / latency numbers. It is a
# diagnostic tool, not for clean throughput measurement.
#
# Output: one file per query, per mode, per round — timestamped in the filename:
#   $OUTDIR/<query>_explain_<ts>.txt   (EXPLAIN VERBOSE — plan structure)
#   $OUTDIR/<query>_analyze_<ts>.txt   (EXPLAIN ANALYZE — per-operator metrics)
# e.g. q5_explain_20260702T182233Z.txt, q5_analyze_20260702T182240Z.txt.
# Each round writes fresh files, so the timestamped set shows plan/timing drift
# over the run.
#
# Usage:
#   scripts/htap-explain-probe.sh &        # background alongside the HTAP workload
#   # ... run the workload ...
#   kill %1                                 # or let it self-exit when spiced stops
#
# Configuration (environment variables, with defaults):
#   OUTDIR=/tmp/htap-explain      output directory (explain_ts.txt written here)
#   HTTP_HOST=localhost           spiced HTTP host
#   HTTP_PORT=8090                spiced HTTP SQL port (serves /v1/sql, /v1/ready)
#   CHBENCH_DIR=<repo>/crates/test-framework/src/queries/chbench   query .sql dir
#   EXPLAIN_QUERIES=all           queries to probe: "all" (every q*.sql) or a
#                                 space-separated list like "q5 q10 q18"
#   EXPLAIN_INTERVAL=30           seconds between probe rounds
#   READY_TIMEOUT=1800            max seconds to wait for spiced HTTP to answer
#   CURL_MAX_TIME=300             per-request curl timeout (EXPLAIN ANALYZE is slow)

set -u

case "${1:-}" in -h|--help) sed -n '2,32p' "$0"; exit 0;; esac

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)"

OUTDIR="${OUTDIR:-/tmp/htap-explain}"
HTTP_HOST="${HTTP_HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-8090}"
CHBENCH_DIR="${CHBENCH_DIR:-$REPO_ROOT/crates/test-framework/src/queries/chbench}"
EXPLAIN_QUERIES="${EXPLAIN_QUERIES:-all}"
EXPLAIN_INTERVAL="${EXPLAIN_INTERVAL:-30}"
READY_TIMEOUT="${READY_TIMEOUT:-1800}"
CURL_MAX_TIME="${CURL_MAX_TIME:-300}"

mkdir -p "$OUTDIR"
SQL_URL="http://${HTTP_HOST}:${HTTP_PORT}/v1/sql"
READY_URL="http://${HTTP_HOST}:${HTTP_PORT}/v1/ready"

# Resolve the query list. "all" enumerates q*.sql (version-sorted so q2 < q10).
if [ "$EXPLAIN_QUERIES" = "all" ]; then
  QUERIES=$(cd "$CHBENCH_DIR" 2>/dev/null && ls q*.sql 2>/dev/null | sed 's/\.sql$//' | sort -V)
else
  QUERIES="$EXPLAIN_QUERIES"
fi
if [ -z "$QUERIES" ]; then
  echo "htap-explain-probe: no queries found in $CHBENCH_DIR" >&2
  exit 0
fi

# post_explain <sql-mode> <label> <query-name> <sql-file> — writes one capture to
# a timestamped per-query file, e.g. q5_analyze_20260702T182240Z.txt.
#   <sql-mode>: VERBOSE | ANALYZE  (forms "EXPLAIN <sql-mode> <query>")
#   <label>:    filename tag (explain | analyze)
post_explain() {
  local mode="$1" label="$2" qname="$3" file="$4"
  local ts; ts=$(date -u +%Y%m%dT%H%M%SZ)   # colon-free for filesystem/artifact names
  local out="$OUTDIR/${qname}_${label}_${ts}.txt"
  {
    echo "===== [$ts] $qname (EXPLAIN $mode) ====="
    # Build "EXPLAIN <mode> <sql>" and stream it to curl via stdin:
    #   - strip a trailing ';' (+ whitespace) so it is one clean statement
    #   - --data-binary @- avoids curl's @/< arg interpretation and a large single arg
    #   - Accept: text/plain -> pretty-printed plan table instead of JSON with \n-escaped text
    { printf 'EXPLAIN %s ' "$mode"; sed -E 's/[[:space:]]*;[[:space:]]*$//' "$file"; } \
      | curl -s -m "$CURL_MAX_TIME" -X POST "$SQL_URL" \
          -H "Content-Type: text/plain" \
          -H "Accept: text/plain" \
          --data-binary @-
    echo
  } >> "$out" 2>&1
}

# Wait for spiced HTTP to answer /v1/ready before the first round; spiced may
# still be booting / seeding when this probe is backgrounded.
deadline=$((SECONDS + READY_TIMEOUT))
until curl -s -m 5 -o /dev/null "$READY_URL" 2>/dev/null; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "htap-explain-probe: spiced HTTP not ready after ${READY_TIMEOUT}s; giving up" >&2
    exit 0
  fi
  sleep 2
done

echo "htap-explain-probe: probing [$(echo "$QUERIES" | tr '\n' ' ')] every ${EXPLAIN_INTERVAL}s -> $OUTDIR/<query>_{explain,analyze}_<ts>.txt"

# Probe until spiced exits. The parent harness normally kills this sampler when
# the run ends; the pgrep check is a self-terminating fallback.
round=0
while pgrep -x spiced >/dev/null 2>&1; do
  round=$((round + 1))
  round_start=$SECONDS
  n=0
  for q in $QUERIES; do
    f="$CHBENCH_DIR/$q.sql"
    [ -f "$f" ] || continue
    post_explain "VERBOSE" "explain" "$q" "$f"
    post_explain "ANALYZE" "analyze" "$q" "$f"
    n=$((n + 1))
  done
  echo "htap-explain-probe: round $round captured $n queries in $((SECONDS - round_start))s ($(date -u +%H:%M:%SZ))"
  sleep "$EXPLAIN_INTERVAL"
done
