#!/bin/bash

set -euo pipefail

OUTPUT_FILE=".spice/service_data.parquet"
TEMP_OUTPUT_FILE="${OUTPUT_FILE}.tmp"
DB_FILE=".spice/service_data.db"
UPDATE_INTERVAL_SEC=3

rm -f "$DB_FILE" "$TEMP_OUTPUT_FILE"
trap 'rm -f "$TEMP_OUTPUT_FILE"' EXIT

# Create the directory if it does not exist
mkdir -p .spice

# Initialize the DuckDB database and create test table
duckdb "$DB_FILE" <<SQL
CREATE TABLE IF NOT EXISTS service_data (
  Id INTEGER,
  Data VARCHAR(4000) NOT NULL,
  DateCreated TIMESTAMP,
  DateUpdated TIMESTAMP
);
SQL

generate_row() {
  ROW_NUM=$1
  DATA="Sample data $ROW_NUM updated at $(date '+%Y-%m-%d %H:%M:%S')"
  DATE_CREATED=$(date '+%Y-%m-%d %H:%M:%S')
  DATE_UPDATED=$(date '+%Y-%m-%d %H:%M:%S')

  echo "$ROW_NUM, '$DATA', '$DATE_CREATED', '$DATE_UPDATED'"
}

# Simulate updates every N seconds
while true; do
  # Truncate the table before inserting new data
  duckdb "$DB_FILE" <<SQL
TRUNCATE TABLE service_data;
SQL

  # Generate 100 rows of data
  for i in $(seq 1 100); do
    DATA=$(generate_row "$i")
    duckdb "$DB_FILE" <<SQL
INSERT INTO service_data (Id, Data, DateCreated, DateUpdated) VALUES ($DATA);
SQL
  done

  # Publish each Parquet snapshot atomically so readers never see a partial file.
  rm -f "$TEMP_OUTPUT_FILE"
  duckdb "$DB_FILE" <<SQL
COPY (SELECT * FROM service_data) TO '$TEMP_OUTPUT_FILE' (FORMAT PARQUET);
SQL
  mv -f "$TEMP_OUTPUT_FILE" "$OUTPUT_FILE"

  echo "File '$OUTPUT_FILE' updated at $(date)"

  sleep $UPDATE_INTERVAL_SEC
done