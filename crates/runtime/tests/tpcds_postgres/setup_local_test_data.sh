#!/bin/bash
# Copyright 2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# TPC-DS Test Data Setup Script
# =============================================================================
#
# This script sets up the local test environment for TPC-DS PostgreSQL 
# acceleration integration tests. It:
#
# 1. Starts a rustfs container (S3-compatible storage)
# 2. Uses DuckDB to generate TPC-DS SF1 data and export to Parquet
# 3. Uploads the Parquet files to rustfs
#
# Usage:
#   ./setup_local_test_data.sh
#
# After running, set these environment variables to run the tests:
#   export MINIO_ENDPOINT="http://localhost:9000"
#   export MINIO_ACCESS_KEY_ID="rustfsadmin"
#   export MINIO_SECRET_ACCESS_KEY="rustfsadmin"
#
# Then run the tests:
#   cargo test -p runtime --test integration --features postgres tpcds_postgres
#
# To clean up:
#   ./setup_local_test_data.sh cleanup
# =============================================================================

set -euo pipefail

# Configuration
RUSTFS_CONTAINER_NAME="spice_tpcds_test_rustfs"
RUSTFS_PORT="${RUSTFS_PORT:-9000}"
RUSTFS_IMAGE="rustfs/rustfs:latest"
RUSTFS_ACCESS_KEY="rustfsadmin"
RUSTFS_SECRET_KEY="rustfsadmin"
S3_BUCKET="benchmarks"
TPCDS_SCALE_FACTOR="${TPCDS_SCALE_FACTOR:-1}"
DATA_DIR="${DATA_DIR:-/tmp/tpcds_test_data}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    
    # Stop and remove rustfs container
    if docker ps -a --format '{{.Names}}' | grep -q "^${RUSTFS_CONTAINER_NAME}$"; then
        log_info "Stopping and removing rustfs container..."
        docker stop "${RUSTFS_CONTAINER_NAME}" 2>/dev/null || true
        docker rm "${RUSTFS_CONTAINER_NAME}" 2>/dev/null || true
    fi
    
    # Clean up data directory
    if [ -d "${DATA_DIR}" ]; then
        log_info "Removing data directory: ${DATA_DIR}"
        rm -rf "${DATA_DIR}"
    fi
    
    log_info "Cleanup complete."
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v duckdb &> /dev/null; then
        log_error "DuckDB CLI is not installed."
        log_info "Install via: brew install duckdb (macOS) or see https://duckdb.org/docs/installation/"
        exit 1
    fi
    
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed."
        log_info "Install via: brew install awscli (macOS) or see https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi
    
    log_info "Dependencies check passed."
}

start_rustfs() {
    log_info "Starting rustfs container..."
    
    # Check if container already exists
    if docker ps --format '{{.Names}}' | grep -q "^${RUSTFS_CONTAINER_NAME}$"; then
        log_info "rustfs container is already running."
        return 0
    fi
    
    # Remove stopped container if exists
    if docker ps -a --format '{{.Names}}' | grep -q "^${RUSTFS_CONTAINER_NAME}$"; then
        log_info "Removing stopped rustfs container..."
        docker rm "${RUSTFS_CONTAINER_NAME}"
    fi
    
    # Create data directory for rustfs
    mkdir -p "${DATA_DIR}/rustfs"
    
    # Start rustfs container
    docker run -d \
        --name "${RUSTFS_CONTAINER_NAME}" \
        -p "${RUSTFS_PORT}:9000" \
        -v "${DATA_DIR}/rustfs:/data" \
        "${RUSTFS_IMAGE}" \
        /data
    
    # Wait for rustfs to be ready
    log_info "Waiting for rustfs to be ready..."
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl -s "http://localhost:${RUSTFS_PORT}" > /dev/null 2>&1; then
            log_info "rustfs is ready."
            return 0
        fi
        sleep 1
        retries=$((retries - 1))
    done
    
    log_error "rustfs failed to start within timeout."
    exit 1
}

generate_tpcds_data() {
    log_info "Generating TPC-DS SF${TPCDS_SCALE_FACTOR} data using DuckDB..."
    
    mkdir -p "${DATA_DIR}/parquet"
    
    # TPC-DS tables to generate
    local tables=(
        "call_center"
        "catalog_page"
        "catalog_returns"
        "catalog_sales"
        "customer"
        "customer_address"
        "customer_demographics"
        "date_dim"
        "household_demographics"
        "income_band"
        "inventory"
        "item"
        "promotion"
        "reason"
        "ship_mode"
        "store"
        "store_returns"
        "store_sales"
        "time_dim"
        "warehouse"
        "web_page"
        "web_returns"
        "web_sales"
        "web_site"
    )
    
    # Generate TPC-DS data and export each table to Parquet
    log_info "Installing TPC-DS extension and generating data..."
    
    duckdb -c "
        INSTALL tpcds;
        LOAD tpcds;
        CALL dsdgen(sf=${TPCDS_SCALE_FACTOR});
    " "${DATA_DIR}/tpcds.duckdb"
    
    log_info "Exporting tables to Parquet format..."
    
    for table in "${tables[@]}"; do
        log_info "  Exporting ${table}..."
        duckdb "${DATA_DIR}/tpcds.duckdb" -c "
            COPY ${table} TO '${DATA_DIR}/parquet/${table}.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);
        "
    done
    
    log_info "TPC-DS data generation complete."
}

create_bucket() {
    log_info "Creating S3 bucket: ${S3_BUCKET} via S3 API..."
    
    # Use AWS CLI to create the bucket via the S3 API
    # rustfs requires buckets to be created via S3 API, not filesystem
    export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY}"
    export AWS_DEFAULT_REGION="us-east-1"
    
    local endpoint="http://localhost:${RUSTFS_PORT}"
    
    # Create the bucket (ignore error if it already exists)
    aws --endpoint-url "${endpoint}" s3 mb "s3://${S3_BUCKET}" 2>/dev/null || true
    
    log_info "Bucket created."
}

upload_data() {
    log_info "Uploading Parquet files to rustfs via S3 API..."
    
    export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY}"
    export AWS_DEFAULT_REGION="us-east-1"
    
    local endpoint="http://localhost:${RUSTFS_PORT}"
    
    # Upload all parquet files to the S3 bucket
    for parquet_file in "${DATA_DIR}/parquet"/*.parquet; do
        local filename=$(basename "${parquet_file}")
        log_info "  Uploading ${filename}..."
        aws --endpoint-url "${endpoint}" s3 cp "${parquet_file}" "s3://${S3_BUCKET}/tpcds_sf1/${filename}"
    done
    
    log_info "Upload complete."
    
    # Verify the upload (use subshell to avoid broken pipe error from head)
    log_info "Verifying uploaded files..."
    local file_count
    file_count=$(aws --endpoint-url "${endpoint}" s3 ls "s3://${S3_BUCKET}/tpcds_sf1/" | wc -l)
    log_info "Uploaded ${file_count} files to s3://${S3_BUCKET}/tpcds_sf1/"
}

print_env_vars() {
    echo ""
    echo "=============================================="
    echo "Setup complete! Set these environment variables:"
    echo "=============================================="
    echo ""
    echo "export MINIO_ENDPOINT=\"http://localhost:${RUSTFS_PORT}\""
    echo "export MINIO_ACCESS_KEY_ID=\"${RUSTFS_ACCESS_KEY}\""
    echo "export MINIO_SECRET_ACCESS_KEY=\"${RUSTFS_SECRET_KEY}\""
    echo ""
    echo "Then run the tests:"
    echo ""
    echo "cargo test -p runtime --test integration --features postgres tpcds_postgres"
    echo ""
    echo "To clean up when done:"
    echo ""
    echo "$0 cleanup"
    echo ""
}

main() {
    if [ "${1:-}" = "cleanup" ]; then
        cleanup
        exit 0
    fi
    
    log_info "Starting TPC-DS test data setup..."
    
    check_dependencies
    start_rustfs
    generate_tpcds_data
    create_bucket
    upload_data
    print_env_vars
    
    log_info "Setup complete!"
}

main "$@"
