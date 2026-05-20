#!/usr/bin/env bash
#
# Generate TPC-DS SF1 Parquet, start `spiced` against the Cayenne SF1 spicepod,
# and capture EXPLAIN / EXPLAIN ANALYZE plans for a configurable set of queries.
#
# Usage:
#   scripts/tpcds_explain.sh                       # default queries (q21 etc.)
#   scripts/tpcds_explain.sh q9 q21 q72            # specific queries
#   QUERIES_DIR=… SCALE_FACTOR=10 …                # override
#
# Environment overrides:
#   SCALE_FACTOR   default 1
#   SPICEPOD       default test/spicepods/tpcds/sf1/accelerated/file[parquet]-cayenne[file].yaml
#   QUERIES_DIR    default crates/test-framework/src/queries/tpcds
#   DATA_DIR       default data
#   OUT_DIR        default target/tpcds_plans
#   ANALYZE        1 (run EXPLAIN ANALYZE) or 0 (logical EXPLAIN only). Default 1.
#   READY_TIMEOUT  seconds to wait for /v1/ready before giving up. Default 180.

set -euo pipefail

# -- config -------------------------------------------------------------------
SCALE_FACTOR="${SCALE_FACTOR:-1}"
SPICEPOD="${SPICEPOD:-test/spicepods/tpcds/sf${SCALE_FACTOR}/accelerated/file[parquet]-cayenne[file].yaml}"
QUERIES_DIR="${QUERIES_DIR:-crates/test-framework/src/queries/tpcds}"
DATA_DIR="${DATA_DIR:-data}"
OUT_DIR="${OUT_DIR:-target/tpcds_plans}"
ANALYZE="${ANALYZE:-1}"
READY_TIMEOUT="${READY_TIMEOUT:-180}"
HTTP_BIND="${HTTP_BIND:-127.0.0.1:8090}"

DEFAULT_QUERIES=(q9 q21 q72)
QUERIES=("$@")
[[ ${#QUERIES[@]} -eq 0 ]] && QUERIES=("${DEFAULT_QUERIES[@]}")

# -- tool checks --------------------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }
need duckdb
need spiced
need curl

# -- 1. generate SF1 Parquet if missing ---------------------------------------
mkdir -p "$DATA_DIR"
# 24 expected tables in TPC-DS. Skip generation if all parquet files exist.
TPCDS_TABLES=(call_center catalog_page catalog_returns catalog_sales customer
              customer_address customer_demographics date_dim household_demographics
              income_band inventory item promotion reason ship_mode store
              store_returns store_sales time_dim warehouse web_page web_returns
              web_sales web_site)
need_gen=0
for t in "${TPCDS_TABLES[@]}"; do
  [[ -f "$DATA_DIR/$t.parquet" ]] || { need_gen=1; break; }
done

if [[ $need_gen -eq 1 ]]; then
  echo "Generating TPC-DS SF${SCALE_FACTOR} Parquet into $DATA_DIR/ (this can take 1-5 min)..."
  tmpdb=$(mktemp -t tpcds.XXXXXX.duckdb)
  duckdb "$tmpdb" <<SQL
INSTALL tpcds; LOAD tpcds;
CALL dsdgen(sf=${SCALE_FACTOR});
$(for t in "${TPCDS_TABLES[@]}"; do
    echo "COPY ${t} TO '${DATA_DIR}/${t}.parquet' (FORMAT 'parquet', COMPRESSION 'snappy');"
  done)
SQL
  rm -f "$tmpdb"
  echo "Generation complete."
else
  echo "All ${#TPCDS_TABLES[@]} TPC-DS Parquet files already present in $DATA_DIR/, skipping generation."
fi

# -- 2. stage spicepod + start spiced -----------------------------------------
# spiced expects the spicepod file to be named exactly spicepod.yaml. Stage the
# selected pod into a temp dir under that name and point spiced at the dir.
mkdir -p "$OUT_DIR"
# Resolve paths to absolute form because spiced runs from the staged dir.
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"
DATA_DIR_ABS="$(cd "$DATA_DIR" && pwd)"
stage_dir=$(mktemp -d -t tpcds_explain_pod.XXXXXX)
trap 'echo "Cleaning $stage_dir"; rm -rf "$stage_dir"; [[ -n "${SPICED_PID:-}" ]] && { echo "Stopping spiced (pid=$SPICED_PID)"; kill "$SPICED_PID" 2>/dev/null || true; wait "$SPICED_PID" 2>/dev/null || true; }' EXIT
cp "$SPICEPOD" "$stage_dir/spicepod.yaml"
# Make data/ resolvable from the staged dir.
ln -s "$DATA_DIR_ABS" "$stage_dir/data"

spiced_log="$OUT_DIR_ABS/spiced.log"
echo "Starting spiced with $SPICEPOD (staged at $stage_dir); logs -> $spiced_log"
( cd "$stage_dir" && spiced --http "$HTTP_BIND" . >"$spiced_log" 2>&1 ) &
SPICED_PID=$!

# -- 3. wait for readiness ----------------------------------------------------
ready_url="http://${HTTP_BIND}/v1/ready"
echo -n "Waiting for $ready_url ..."
deadline=$((SECONDS + READY_TIMEOUT))
while :; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$ready_url" || echo 000)
  if [[ "$code" == "200" ]]; then echo " ready (${SECONDS}s)"; break; fi
  if (( SECONDS >= deadline )); then
    echo " timeout after ${READY_TIMEOUT}s (last status: $code)"
    echo "--- last 40 lines of $spiced_log ---"
    tail -40 "$spiced_log" >&2 || true
    exit 1
  fi
  sleep 2
done

# -- 4. capture plans ---------------------------------------------------------
sql_url="http://${HTTP_BIND}/v1/sql"
prefix=$([[ "$ANALYZE" == "1" ]] && echo "EXPLAIN ANALYZE" || echo "EXPLAIN")
echo "Capturing plans with: $prefix"

for q in "${QUERIES[@]}"; do
  qfile="$QUERIES_DIR/$q.sql"
  [[ -f "$qfile" ]] || { echo "  skip $q: $qfile not found"; continue; }

  out="$OUT_DIR/${q}.md"
  sql=$(cat "$qfile")
  # Strip a trailing semicolon — EXPLAIN doesn't like it after another statement.
  sql="${sql%;}"
  body="${prefix} ${sql}"

  printf '# %s — %s\n\n' "$q" "$prefix" >"$out"
  printf '```\n' >>"$out"
  curl -s -X POST "$sql_url" \
    -H 'Content-Type: text/plain' \
    --data-binary "$body" >>"$out" \
    && echo "  $q -> $out" \
    || echo "  $q FAILED (see $out)"
  printf '\n```\n' >>"$out"
done

echo "Done. Plans in $OUT_DIR/"
