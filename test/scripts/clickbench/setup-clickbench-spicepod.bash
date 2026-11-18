#!/bin/bash
set -e
set -o pipefail

# Initialize variables
engine=arrow

# Function to display usage
usage() {
    echo "Usage: $0 [-engine acceleration_engine]"
    echo "  --engine Acceleration Engine: arrow or cayenne (default: arrow)"
}

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case "${1#--}" in
        engine)
            engine="$2"
            shift 2
            ;;
        *)
            echo "error: Invalid option: $1" 1>&2
            usage
            exit 1
            ;;
    esac
done

# Each engine below writes its own dataset; any other value would write a Spicepod with none.
case "$engine" in
    arrow | cayenne ) ;;
    * )
        echo "error: Unsupported engine: $engine (expected arrow or cayenne)" 1>&2
        usage
        exit 1
        ;;
esac

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
echo "      engine: arrow" >> spicepod.yaml
fi

# Load clickbench data into Cayenne Accelerator
if [ "$engine" = "cayenne" ]; then
echo "  - from: duckdb:hits" >> spicepod.yaml
echo "    name: hits" >> spicepod.yaml
echo "    params:" >> spicepod.yaml
echo "      duckdb_open: $dbname" >> spicepod.yaml
echo "    acceleration:" >> spicepod.yaml
echo "      enabled: true" >> spicepod.yaml
echo "      engine: cayenne" >> spicepod.yaml
echo "      mode: file" >> spicepod.yaml
fi

spiced
