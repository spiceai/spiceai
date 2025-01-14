#!/bin/bash
set -e
set -o pipefail

# Initialize variables
pg_host=localhost
pg_port=5432
pg_user=postgres
pg_pass=postgres
pg_sslmode=disable
pg_db=postgres
engine=arrow

# Function to display usage
usage() {
    echo "Usage: $0 [-engine acceleration_engine] [-pg_host pg_host] [-pg_port pg_port] [-pg_user pg_user] [-pg_pass pg_pass] [-pg_sslmode pg_sslmode]"
    echo "  --engine Acceleration Engine (default: arrow)"
    echo "  --pg_host Acceleration parameter: pg_host (default: localhost)"
    echo "  --pg_port Acceleration parameter: pg_port (default: 5432)"
    echo "  --pg_user Acceleration parameter: pg_user (default: postgres)"
    echo "  --pg_pass Acceleration parameter: pg_pass (default: postgres)"
    echo "  --pg_sslmode Acceleration parameter: pg_sslmode (default: disabled)"
    echo "  --pg_db Acceleration parameter: pg_db (default: postgres)"
}

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case "${1#--}" in
        engine)
            engine="$2"
            shift 2
            ;;
        pg_host)
            pg_host="$2"
            shift 2
            ;;
        pg_port)
            pg_port="$2"
            shift 2
            ;;
        pg_user)
            pg_user="$2"
            shift 2
            ;;
        pg_pass)
            pg_pass="$2"
            shift 2
            ;;
        pg_sslmode)
            pg_sslmode="$2"
            shift 2
            ;;
        pg_db)
            pg_db="$2"
            shift 2
            ;;
        *)
            echo "error: Invalid option: $1" 1>&2
            usage
            exit 1
            ;;
    esac
done

# test if duckdb command exists
if ! type "duckdb" 1> /dev/null 2>&1; then
  echo "'duckdb' is required"
fi

# Download clickbench data if not already downloaded and uncompressed
if [ ! -f hits.csv ]; then
  wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
  gzip -d hits.csv.gz
fi

# Command to load clickbench data into DuckDB
dbname="clickbench.db"
dbname_accelerated="clickbench-accelerated"
read_command() {
    echo ".read 'create.sql'"
}
copy_command() {
    echo "COPY hits FROM 'hits.csv';"
}

# Load clickbench data into DuckDB
if [ -f "$dbname" ]; then
  echo "Database '$dbname' already exists. Skipping creation, using existing database."
else
  duckdb -c "$(read_command)" "$dbname"
  duckdb -c "$(copy_command)" "$dbname"
  echo "Created database '$dbname'"
fi

# test if spicepod.yaml exists
if [ -f "spicepod.yaml" ]; then
  echo "spicepod.yaml found. Aborting."
  exit 1
fi

echo "version: v1" >> spicepod.yaml
echo "kind: Spicepod" >> spicepod.yaml
echo "name: $dbname" >> spicepod.yaml

echo "datasets:" >> spicepod.yaml

# Load clickbench data into Arrow Accelerator
if [ "$engine" = "arrow" ]; then
echo "  - from: duckdb:hits" >> spicepod.yaml
echo "    name: hits" >> spicepod.yaml
echo "    params:" >> spicepod.yaml
echo "      duckdb_open: $dbname" >> spicepod.yaml
echo "    acceleration:" >> spicepod.yaml
echo "      enabled: true" >> spicepod.yaml
fi

# Load clickbench data into Postgres Accelerator
if [ "$engine" = "postgres" ]; then
echo "  - from: duckdb:hits" >> spicepod.yaml
echo "    name: hits" >> spicepod.yaml
echo "    params:" >> spicepod.yaml
echo "      duckdb_open: $dbname" >> spicepod.yaml
echo "    acceleration:" >> spicepod.yaml
echo "      enabled: true" >> spicepod.yaml
echo "      engine: postgres" >> spicepod.yaml
echo "      params:" >> spicepod.yaml
echo "        pg_host: $pg_host" >> spicepod.yaml
echo "        pg_port: $pg_port" >> spicepod.yaml
echo "        pg_user: $pg_user" >> spicepod.yaml
echo "        pg_pass: $pg_pass" >> spicepod.yaml
echo "        pg_sslmode: $pg_sslmode" >> spicepod.yaml
echo "        pg_db: $pg_db" >> spicepod.yaml
fi

# Load clickbench data into SQLite Accelerator
if [ "$engine" = "sqlite" ]; then
echo "  - from: duckdb:hits" >> spicepod.yaml
echo "    name: hits" >> spicepod.yaml
echo "    params:" >> spicepod.yaml
echo "      duckdb_open: $dbname" >> spicepod.yaml
echo "    acceleration:" >> spicepod.yaml
echo "      enabled: true" >> spicepod.yaml
echo "      engine: sqlite" >> spicepod.yaml
echo "      mode: file" >> spicepod.yaml
echo "      params:" >> spicepod.yaml
echo "        sqlite_file: $dbname_accelerated.db" >> spicepod.yaml
fi

spiced
