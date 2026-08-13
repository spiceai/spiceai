#!/usr/bin/env bash
# Shared configuration for the local benchmark harness.
# Source this from the other scripts; do not run it directly.

# Repo root (scripts live in test/local-bench)
LOCAL_BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${LOCAL_BENCH_DIR}/../.." && pwd)"
TPC_BENCH_DIR="${REPO_ROOT}/test/tpc-bench"

# All generated data lives here (gitignored via test/tpc-bench/tmp is NOT used;
# this dir is self-contained and safe to delete to reclaim space).
WORK_DIR="${LOCAL_BENCH_DIR}/.work"
FLEET_PARQUET_DIR="${WORK_DIR}/tpcds-parquet"   # parquet for file:// benches + MinIO upload

# Containers (unique names so we never touch unrelated containers)
PG_CONTAINER=spice-bench-postgres
MINIO_CONTAINER=spice-bench-minio

# PostgreSQL (mirrors the CI service container: postgres:16, shm 1g)
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export PGPASSWORD="${POSTGRES_PASSWORD}"        # for psql/createdb in the make targets
PG_TPCDS_DB=tpcds_sf1                            # database name the spicepods expect

# MinIO (mirrors CI's spice-minio: bucket "benchmarks", prefix tpcds_sf1/)
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001
export S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}"
export S3_KEY=minioadmin
export S3_SECRET=minioadmin
MINIO_BUCKET=benchmarks
MINIO_PREFIX=tpcds_sf1

# Pinned DuckDB CLI — the fleet dataset generator. Reuses the tpc-bench
# Makefile's download target so the pin lives in exactly one place.
DUCKDB_BIN="${TPC_BENCH_DIR}/tmp/bin/duckdb"

# TPC-DS tables (matches TPCDS_TABLES in test/tpc-bench/Makefile)
TPCDS_TABLES="call_center catalog_page catalog_returns catalog_sales customer \
customer_address customer_demographics date_dim household_demographics \
income_band inventory item promotion reason ship_mode store store_returns \
store_sales time_dim warehouse web_page web_returns web_sales web_site"

die() { echo "ERROR: $*" >&2; exit 1; }

require() {
    command -v "$1" >/dev/null 2>&1 || die "'$1' not found. $2"
}
