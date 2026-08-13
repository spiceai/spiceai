#!/usr/bin/env bash
# Fast local feedback loop for the SF100 nightly q97 OOM
# (tpcds accelerated/s3[parquet]-cayenne[file], issue context: PR #11371).
#
# Runs ONLY q97, against ONLY its three tables, at SF10 with a 5GiB query
# memory pool (see spicepod-q97-sf10.yaml for why that reproduces the SF100
# failure). One iteration = rebuild spiced + this script: minutes, not the
# ~45-minute CI loop.
#
# Usage:
#   ./50-run-q97.sh              # generate/upload data if missing, run q97
#   ./50-run-q97.sh --force-data # regenerate + re-upload the SF10 tables
#
# Env:
#   SPICED_BIN  path to spiced (default: <repo>/target/release/spiced)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00-env.sh"

require docker "Install Docker Desktop or colima."
docker ps --format '{{.Names}}' | grep -q "^${MINIO_CONTAINER}$" || \
    die "MinIO container not running - run ./10-setup-storage.sh minio first."

Q97_TABLES="store_sales catalog_sales date_dim"
SF10_DIR="${WORK_DIR}/tpcds-sf10-parquet"
FORCE_DATA=false
[[ "${1:-}" == "--force-data" ]] && FORCE_DATA=true

# --- 1. Generate the three SF10 tables (pinned DuckDB dsdgen - the fleet
#        generator; ~5-10 minutes and ~2 GB, once) ---
if [[ ! -x "${DUCKDB_BIN}" ]]; then
    make -C "${TPC_BENCH_DIR}" ./tmp/bin/duckdb
fi
if ${FORCE_DATA} || [[ ! -f "${SF10_DIR}/store_sales.parquet" ]]; then
    echo "Generating TPC-DS SF10 (${Q97_TABLES}) into ${SF10_DIR}..."
    mkdir -p "${SF10_DIR}"
    GEN_DB="${WORK_DIR}/tpcds-sf10-gen.duckdb"
    rm -f "${GEN_DB}" "${GEN_DB}.wal" "${SF10_DIR}"/*.parquet
    "${DUCKDB_BIN}" "${GEN_DB}" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=10);" > /dev/null
    for table in ${Q97_TABLES}; do
        echo "  ${table}.parquet"
        "${DUCKDB_BIN}" "${GEN_DB}" -c "COPY ${table} TO '${SF10_DIR}/${table}.parquet' (FORMAT parquet);" > /dev/null
    done
    rm -f "${GEN_DB}" "${GEN_DB}.wal"
else
    echo "SF10 parquet already present - skipping generation (--force-data to regenerate)."
fi

# --- 2. Upload to MinIO as tpcds_sf10/<table>/<table>.parquet (the sf10
#        spicepods address tables as directories) ---
echo "Uploading to MinIO s3://${MINIO_BUCKET}/tpcds_sf10/ ..."
docker exec "${MINIO_CONTAINER}" mc alias set local "http://127.0.0.1:9000" "${S3_KEY}" "${S3_SECRET}" > /dev/null
docker exec "${MINIO_CONTAINER}" mc mb -p "local/${MINIO_BUCKET}" > /dev/null
# The SF10 dir is not the mounted /fleet dir; stream the files in via stdin.
for table in ${Q97_TABLES}; do
    docker exec -i "${MINIO_CONTAINER}" sh -c \
        "mc pipe local/${MINIO_BUCKET}/tpcds_sf10/${table}/${table}.parquet" \
        < "${SF10_DIR}/${table}.parquet"
done
echo "  uploaded: $(docker exec "${MINIO_CONTAINER}" sh -c "mc ls -r local/${MINIO_BUCKET}/tpcds_sf10/ | wc -l") objects"

# --- 3. Run q97 alone via the scenario query set ---
SPICED_BIN="${SPICED_BIN:-${REPO_ROOT}/target/release/spiced}"
[[ -x "${SPICED_BIN}" ]] || die "spiced binary not found at ${SPICED_BIN}. Build it first:
    cargo build --release -p spiced"

cd "${REPO_ROOT}"
export INSTA_WORKSPACE_ROOT="${REPO_ROOT}"
export CARGO_MANIFEST_DIR="${REPO_ROOT}"
# Scenario snapshots are local-only scaffolding; always accept them so the run's
# verdict is execution (OOM vs completed), not snapshot bookkeeping.
export INSTA_UPDATE=always

LOG="${LOCAL_BENCH_DIR}/logs/q97-sf10.log"
mkdir -p "${LOCAL_BENCH_DIR}/logs"
rm -rf .spice/data

set +e
cargo run -p testoperator -- run bench \
    -s "${SPICED_BIN}" \
    -p "${LOCAL_BENCH_DIR}/spicepod-q97-sf10.yaml" \
    --query-set scenario \
    --scenario-query-file "${LOCAL_BENCH_DIR}/q97-scenario.yaml" \
    --ready-wait 900 \
    --validate=false \
    > "${LOG}" 2>&1
BENCH_EXIT=$?
set -e

echo
echo "===== q97 verdict (full log: ${LOG}) ====="
echo "-- oversized-join gate decisions (Full join must show fire=true):"
grep -oE "Evaluated Cayenne oversized-join memory gate.*" "${LOG}" | sort -u | sed 's/^/   /' || echo "   (no gate evaluations logged)"
grep -oE "oversized-join (gate|rewrite) declined.*" "${LOG}" | sort -u | sed 's/^/   /' || true
echo "-- OOM check:"
# Hash-join exhaustion reports "Resources exhausted"; sorter exhaustion reports
# "Some resource has been exhausted" - both carry "Additional allocation failed".
if grep -qE "Resources exhausted|resource has been exhausted|Additional allocation failed" "${LOG}"; then
    echo "   STILL OOMs:"
    grep -m1 -oE "Additional allocation failed for [A-Za-z]+\[[0-9]+\][^\"]{0,140}" "${LOG}" | sed 's/^/   /'
else
    echo "   no memory exhaustion - q97 completed."
fi
echo "-- q97 result rows:"
grep -E "tpcds_q97" "${LOG}" | grep -oE "\| tpcds_q97 +\| (passed|failed).*" | head -2 | sed 's/^/   /' || grep -m2 "tpcds_q97" "${LOG}" | sed 's/^/   /'
exit ${BENCH_EXIT}
