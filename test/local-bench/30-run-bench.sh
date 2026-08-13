#!/usr/bin/env bash
# Run one of the target benchmarks locally, exactly the way CI runs it
# (testoperator run bench + a spiced binary + the fleet dataset).
#
# Usage:
#   ./30-run-bench.sh <config> [extra testoperator args...]
#
# Configs (the runs we expect the current fixes to change):
#   s3-cayenne          tpcds accelerated/s3[parquet]-cayenne[file]     (eager-agg ROLLUP fix)
#   file-cayenne        tpcds accelerated/file[parquet]-cayenne[file]   (eager-agg ROLLUP fix)
#   postgres-federated  tpcds federated/postgres                        (fleet-seed fix: q17/q25/q85)
#   postgres-arrow      tpcds accelerated/postgres-arrow                (fleet-seed fix)
#   postgres-duckdb     tpcds accelerated/postgres-duckdb[file]         (fleet-seed fix)
#
# Env:
#   SPICED_BIN    path to the spiced binary  (default: <repo>/target/release/spiced)
#   INSTA_UPDATE  snapshot mode: no|always   (default: no - assert against checked-in snapshots)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00-env.sh"

CONFIG="${1:-}"; shift || true

SPICEPOD=""; OVERRIDES=""; READY_WAIT=600
case "${CONFIG}" in
    s3-cayenne)         SPICEPOD="accelerated/s3[parquet]-cayenne[file].yaml" ;;
    file-cayenne)       SPICEPOD="accelerated/file[parquet]-cayenne[file].yaml" ;;
    postgres-federated) SPICEPOD="federated/postgres.yaml";              OVERRIDES=postgresql ;;
    postgres-arrow)     SPICEPOD="accelerated/postgres-arrow.yaml";      OVERRIDES=arrow ;;
    postgres-duckdb)    SPICEPOD="accelerated/postgres-duckdb[file].yaml"; OVERRIDES=duckdb ;;
    *)
        sed -n '3,17p' "${BASH_SOURCE[0]}"
        exit 1
        ;;
esac

SPICED_BIN="${SPICED_BIN:-${REPO_ROOT}/target/release/spiced}"
[[ -x "${SPICED_BIN}" ]] || die "spiced binary not found at ${SPICED_BIN}.
Build it first (release recommended for benchmark timings):
    cargo build --release -p spiced --features postgres,duckdb
or point SPICED_BIN at an existing binary (e.g. ~/.spice/bin/spiced)."

[[ -f "${FLEET_PARQUET_DIR}/store_sales.parquet" ]] || \
    die "fleet parquet missing - run ./10-setup-storage.sh && ./20-populate-data.sh first."

cd "${REPO_ROOT}"

# Env consumed by the spicepods' ${secrets:...} params (spiced inherits it):
#   POSTGRES_HOST / POSTGRES_PASSWORD  - postgres benches   (exported by 00-env.sh)
#   S3_ENDPOINT / S3_KEY / S3_SECRET   - s3 benches         (exported by 00-env.sh)
# INSTA_* mirrors CI so insta finds the checked-in snapshots under
# crates/test-framework/src/snapshot/snapshots.
export INSTA_WORKSPACE_ROOT="${REPO_ROOT}"
export CARGO_MANIFEST_DIR="${REPO_ROOT}"
export INSTA_UPDATE="${INSTA_UPDATE:-no}"

echo "Running ${CONFIG}: test/spicepods/tpcds/sf1/${SPICEPOD}"
echo "  spiced:       ${SPICED_BIN}"
echo "  INSTA_UPDATE: ${INSTA_UPDATE}"

rm -rf .spice/data

cargo run -p testoperator -- run bench \
    -s "${SPICED_BIN}" \
    -p "test/spicepods/tpcds/sf1/${SPICEPOD}" \
    -d "${FLEET_PARQUET_DIR}" \
    --query-set tpcds \
    ${OVERRIDES:+--query-overrides ${OVERRIDES}} \
    --ready-wait "${READY_WAIT}" \
    --validate=false \
    --scale-factor 1 \
    "$@"
