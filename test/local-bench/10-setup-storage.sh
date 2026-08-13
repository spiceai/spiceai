#!/usr/bin/env bash
# Start the storage the target benchmarks need:
#   - PostgreSQL 16 (source database for the postgres benches)
#   - MinIO         (S3 source for the s3[parquet] benches)
#
# Usage: ./10-setup-storage.sh [all|postgres|minio] [--reset]
#
# Idempotent: reuses running containers. --reset removes and recreates them
# (drops all previously loaded data).
#
# NOTE: the spicepods hard-code pg_port 5432 (matching the CI service
# container), so the benchmark postgres container must own host port 5432.
# If a local PostgreSQL is already running there, stop it first, e.g.:
#     brew services stop postgresql@16
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/00-env.sh"

require docker "Install Docker Desktop or colima."
docker info >/dev/null 2>&1 || die "the Docker daemon is not running - start Docker Desktop (open -a Docker) or colima first."

COMPONENT="all"; RESET=false
for arg in "$@"; do
    case "${arg}" in
        all|postgres|minio) COMPONENT="${arg}" ;;
        --reset) RESET=true ;;
        *) die "unknown argument '${arg}' (usage: $0 [all|postgres|minio] [--reset])" ;;
    esac
done

port_in_use_by_other() {
    # True if the TCP port is bound by something other than the named container.
    local port=$1 container=$2
    if docker ps --format '{{.Names}} {{.Ports}}' | grep -v "^${container} " | grep -q ":${port}->"; then
        return 0
    fi
    if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        (echo >"/dev/tcp/127.0.0.1/${port}") 2>/dev/null && return 0
    fi
    return 1
}

start_container() {
    local name=$1; shift
    if ${RESET}; then
        docker rm -f "${name}" >/dev/null 2>&1 || true
    fi
    if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
        echo "${name} already running - reusing (pass --reset to recreate)."
        return 0
    fi
    docker rm -f "${name}" >/dev/null 2>&1 || true   # remove stopped leftover of OUR container only
    docker run -d --name "${name}" "$@"
}

mkdir -p "${FLEET_PARQUET_DIR}"

# --- PostgreSQL (mirrors the CI service container incl. --shm-size=1g: the
# default 64MB /dev/shm makes parallel-worker TPC-DS queries fail with
# "could not resize shared memory segment") ---
if [[ "${COMPONENT}" == "all" || "${COMPONENT}" == "postgres" ]]; then
    port_in_use_by_other "${POSTGRES_PORT}" "${PG_CONTAINER}" && \
        die "port ${POSTGRES_PORT} is in use by something other than ${PG_CONTAINER} (a local PostgreSQL?).
The spicepods hard-code pg_port 5432, so stop the other process first (e.g. brew services stop postgresql@16),
or run './10-setup-storage.sh minio' to set up only the s3/file benches."

    start_container "${PG_CONTAINER}" \
        -p "${POSTGRES_PORT}:5432" \
        -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
        --shm-size=1g \
        postgres:16

    echo "Waiting for PostgreSQL..."
    for i in $(seq 1 60); do
        docker exec "${PG_CONTAINER}" pg_isready -U postgres >/dev/null 2>&1 && break
        [[ $i == 60 ]] && die "PostgreSQL not ready after 60s"
        sleep 1
    done
    echo "  PostgreSQL: ${POSTGRES_HOST}:${POSTGRES_PORT} (user ${POSTGRES_USER})"
fi

# --- MinIO (FLEET_PARQUET_DIR mounted at /fleet so 20-populate-data.sh can
# `mc cp` the generated parquet into the bucket from inside the container) ---
if [[ "${COMPONENT}" == "all" || "${COMPONENT}" == "minio" ]]; then
    port_in_use_by_other "${MINIO_PORT}" "${MINIO_CONTAINER}" && \
        die "port ${MINIO_PORT} is in use by something other than ${MINIO_CONTAINER}. Stop it or change MINIO_PORT in 00-env.sh."

    start_container "${MINIO_CONTAINER}" \
        -p "${MINIO_PORT}:9000" -p "${MINIO_CONSOLE_PORT}:9001" \
        -e MINIO_ROOT_USER="${S3_KEY}" -e MINIO_ROOT_PASSWORD="${S3_SECRET}" \
        -v "${FLEET_PARQUET_DIR}:/fleet:ro" \
        minio/minio server /data --console-address ":9001"

    echo "Waiting for MinIO..."
    for i in $(seq 1 60); do
        curl -sf "${S3_ENDPOINT}/minio/health/ready" >/dev/null 2>&1 && break
        [[ $i == 60 ]] && die "MinIO not ready after 60s"
        sleep 1
    done
    echo "  MinIO:      ${S3_ENDPOINT} (console http://127.0.0.1:${MINIO_CONSOLE_PORT})"
fi

echo "Storage ready."
