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
# Output: appends timestamped sections to $OUTDIR/explain_ts.txt.
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
OUT="$OUTDIR/explain_ts.txt"
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

# post_explain <mode> <query-name> <sql-file> — one timestamped section.
post_explain() {
  local mode="$1" qname="$2" file="$3"
  {
    echo "===== [$(date -u +%Y-%m-%dT%H:%M:%SZ)] $qname (EXPLAIN $mode) ====="
    curl -s -m "$CURL_MAX_TIME" -X POST "$SQL_URL" \
      -H "Content-Type: text/plain" \
      --data "EXPLAIN $mode $(cat "$file")"
    echo
  } >> "$OUT" 2>&1
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

echo "htap-explain-probe: probing [$(echo "$QUERIES" | tr '\n' ' ')] every ${EXPLAIN_INTERVAL}s -> $OUT"

# Probe until spiced exits. The parent harness normally kills this sampler when
# the run ends; the pgrep check is a self-terminating fallback.
while pgrep -x spiced >/dev/null 2>&1; do
  for q in $QUERIES; do
    f="$CHBENCH_DIR/$q.sql"
    [ -f "$f" ] || continue
    post_explain "VERBOSE" "$q" "$f"
    post_explain "ANALYZE" "$q" "$f"
  done
  sleep "$EXPLAIN_INTERVAL"
done
