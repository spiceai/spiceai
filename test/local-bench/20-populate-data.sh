#!/usr/bin/env bash
# Populate the storage started by 10-setup-storage.sh with the FLEET TPC-DS
# SF1 dataset (DuckDB's tpcds dsdgen - the generator every recorded expected
# answer was produced with; see tpcds-fleet-gen in test/tpc-bench/Makefile):
#
#   1. parquet files            -> ${FLEET_PARQUET_DIR}   (file:// benches, -d dir)
#   2. the same parquet files   -> MinIO s3://benchmarks/tpcds_sf1/   (s3 benches)
#   3. pipe-delimited .dat data -> PostgreSQL database tpcds_sf1     (postgres benches)
#
# Idempotent: skips each phase whose output already exists. Pass --force to
# regenerate/reload everything.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00-env.sh"

require docker "Install Docker Desktop or colima."
require psql "brew install libpq && brew link --force libpq (or: brew install postgresql@16)"
require createdb "comes with libpq/postgresql (see psql hint above)"
require make ""

FORCE=false
[[ "${1:-}" == "--force" ]] && FORCE=true

mkdir -p "${WORK_DIR}" "${FLEET_PARQUET_DIR}"

# --- 0. Pinned DuckDB CLI (downloaded by the tpc-bench Makefile so the version
#        pin lives in one place) ---
if [[ ! -x "${DUCKDB_BIN}" ]]; then
    make -C "${TPC_BENCH_DIR}" ./tmp/bin/duckdb
fi

# --- 1. Parquet for the file:// and s3:// benches ---
first_table=$(echo ${TPCDS_TABLES} | awk '{print $1}')
if ${FORCE} || [[ ! -f "${FLEET_PARQUET_DIR}/${first_table}.parquet" ]]; then
    echo "Generating TPC-DS SF1 parquet (DuckDB dsdgen) into ${FLEET_PARQUET_DIR}..."
    GEN_DB="${WORK_DIR}/tpcds-parquet-gen.duckdb"
    rm -f "${GEN_DB}" "${GEN_DB}.wal" "${FLEET_PARQUET_DIR}"/*.parquet
    "${DUCKDB_BIN}" "${GEN_DB}" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=1);" > /dev/null
    for table in ${TPCDS_TABLES}; do
        echo "  ${table}.parquet"
        "${DUCKDB_BIN}" "${GEN_DB}" -c "COPY ${table} TO '${FLEET_PARQUET_DIR}/${table}.parquet' (FORMAT parquet); DROP TABLE ${table}; CHECKPOINT;" > /dev/null
    done
    rm -f "${GEN_DB}" "${GEN_DB}.wal"
else
    echo "Parquet already present in ${FLEET_PARQUET_DIR} - skipping (pass --force to regenerate)."
fi

# --- 2. Upload parquet to MinIO (mc runs inside the MinIO container; the
#        parquet dir is mounted there at /fleet by 10-setup-storage.sh) ---
echo "Uploading parquet to MinIO s3://${MINIO_BUCKET}/${MINIO_PREFIX}/ ..."
docker exec "${MINIO_CONTAINER}" mc alias set local "http://127.0.0.1:9000" "${S3_KEY}" "${S3_SECRET}" > /dev/null
docker exec "${MINIO_CONTAINER}" mc mb -p "local/${MINIO_BUCKET}" > /dev/null
docker exec "${MINIO_CONTAINER}" sh -c "mc cp /fleet/*.parquet local/${MINIO_BUCKET}/${MINIO_PREFIX}/" > /dev/null
echo "  $(docker exec "${MINIO_CONTAINER}" sh -c "mc ls local/${MINIO_BUCKET}/${MINIO_PREFIX}/ | wc -l") objects in the bucket."

# --- 3. Seed PostgreSQL with the same generator's data, via the exact make
#        targets CI uses (tpcds-clone for the DDL, tpcds-fleet-gen for the
#        .dat files, then init/load/index) ---
# Guard: only ever seed OUR container. If port 5432 is served by anything else
# (e.g. a personal brew postgres), refuse rather than write into it.
if ! docker ps --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
    echo "SKIPPING PostgreSQL seed: container ${PG_CONTAINER} is not running."
    echo "  (whatever is on ${POSTGRES_HOST}:${POSTGRES_PORT} is not the benchmark postgres - not touching it)"
    echo "  Run './10-setup-storage.sh postgres' first; s3/file benches work without this step."
    exit 0
fi
existing_rows=$(psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -tAc \
    "SELECT COALESCE((SELECT reltuples::bigint FROM pg_class WHERE relname='customer'), 0)" \
    -d "${PG_TPCDS_DB}" 2>/dev/null || echo 0)
if ! ${FORCE} && [[ "${existing_rows:-0}" -gt 0 ]]; then
    echo "PostgreSQL ${PG_TPCDS_DB} already seeded (~${existing_rows} customer rows) - skipping (pass --force to reload)."
else
    echo "Seeding PostgreSQL ${PG_TPCDS_DB} from the fleet generator..."
    make -C "${TPC_BENCH_DIR}" tpcds-clone
    DBGEN_SCALE=1 make -C "${TPC_BENCH_DIR}" tpcds-fleet-gen
    DB_HOST="${POSTGRES_HOST}" DB_PORT="${POSTGRES_PORT}" DB_USER="${POSTGRES_USER}" DB_NAME="${PG_TPCDS_DB}" \
        make -C "${TPC_BENCH_DIR}" pg-tpcds-init
    DB_HOST="${POSTGRES_HOST}" DB_PORT="${POSTGRES_PORT}" DB_USER="${POSTGRES_USER}" DB_NAME="${PG_TPCDS_DB}" \
        TPCDS_DATA_DIR=./tmp/tpcds-fleet \
        make -C "${TPC_BENCH_DIR}" pg-tpcds-load
    DB_HOST="${POSTGRES_HOST}" DB_PORT="${POSTGRES_PORT}" DB_USER="${POSTGRES_USER}" DB_NAME="${PG_TPCDS_DB}" \
        make -C "${TPC_BENCH_DIR}" pg-tpcds-create-index
fi

echo
echo "Data ready. Sanity check (q17/q25/q85 knife-edge join rows must be > 0):"
psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${PG_TPCDS_DB}" -tAc "
SELECT 'q17-shape rows: ' || count(*)
FROM store_sales ss
JOIN store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
  AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number
JOIN catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
  AND sr.sr_item_sk = cs.cs_item_sk
JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk AND d1.d_quarter_name = '1999Q1'
JOIN date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk AND d2.d_quarter_name IN ('1999Q1','1999Q2','1999Q3')
JOIN date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk AND d3.d_quarter_name IN ('1999Q1','1999Q2','1999Q3');"
