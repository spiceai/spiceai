#!/bin/bash

set -e
set -o pipefail

# Initialize variables
engine=arrow

# Function to display flag usage
usage() {
    echo "Usage: ./setup-tpc-spicepod.bash <tpch or tpcds> <scale factor> [-engine acceleration_engine]"
    echo "  --engine Acceleration Engine: arrow, cayenne or sqlite (default: arrow)"
}

# two arguments, one is tpch or tpcds, the other one is the scale factor

if [ "$#" -lt 2 ]; then
  usage
  echo "Example: ./setup-tpc-spicepod.bash tpcds 1"
  exit 1
fi

# verify the scale factor is an integer
re='^[0-9]+$'
if ! [[ $2 =~ $re ]] ; then
  usage
  echo "error: scale factor must be an integer"
  exit 1
fi

# verify tpch or tpcds
if [ "$1" != "tpch" ] && [ "$1" != "tpcds" ]; then
  usage
  echo "Example: ./setup-tpc-spicepod.bash tpcds 1"
  exit 1
fi

bench=$1
sf=$2
shift 2

# Parse command-line options
while [[ "$#" -gt 0 ]]; do
    case "${1#--}" in
        engine )
            engine="$2"
            shift 2
            ;;
        * )
            echo "error: Invalid option: $1" 1>&2
            usage
            exit 1
            ;;
    esac
done

# Each engine below writes its own datasets; any other value would write a Spicepod with none.
case "$engine" in
    arrow | cayenne | sqlite ) ;;
    * )
        echo "error: Unsupported engine: $engine (expected arrow, cayenne or sqlite)" 1>&2
        usage
        exit 1
        ;;
esac

# test if duckdb command exists
if ! type "duckdb" 1> /dev/null 2>&1; then
  echo "'duckdb' is required"
fi

dbname="$bench-sf$sf.db"
dbname_accelerated_prefix="$bench-sf$sf-accelerated-"
generate_command() {
  if [ "$bench" = "tpch" ]; then
    echo "INSTALL tpch; LOAD tpch; CALL dbgen(sf = $sf)"
  elif [ "$bench" = "tpcds" ]; then
    echo "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = $sf)"
  fi
}
if [ -f "$dbname" ]; then
  echo "Database '$dbname' already exists. Skipping creation, using existing database."
else
  duckdbcommand=`generate_command $bench $sf`
  duckdb -c "$duckdbcommand" "$dbname"
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

if [ "$bench" = "tpch" ]; then
  if [ "$engine" = "arrow" ]; then
    for i in customer lineitem nation orders part partsupp region supplier; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: arrow" >> spicepod.yaml
    done
  fi
  if [ "$engine" = "cayenne" ]; then
    for i in customer lineitem nation orders part partsupp region supplier; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: cayenne" >> spicepod.yaml
      echo "      mode: file" >> spicepod.yaml
    done
  fi
  if [ "$engine" = "sqlite" ]; then
    for i in customer lineitem nation orders part partsupp region supplier; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: sqlite" >> spicepod.yaml
      echo "      mode: file" >> spicepod.yaml
      echo "      params:" >> spicepod.yaml
      echo "        sqlite_file: $dbname_accelerated_prefix$i.db" >> spicepod.yaml
    done
  fi
fi

if [ "$bench" = "tpcds" ]; then
  if [ "$engine" = "arrow" ]; then
    for i in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: arrow" >> spicepod.yaml
    done
  fi
  if [ "$engine" = "cayenne" ]; then
    for i in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: cayenne" >> spicepod.yaml
      echo "      mode: file" >> spicepod.yaml
    done
  fi
  if [ "$engine" = "sqlite" ]; then
    for i in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do
      echo "  - from: duckdb:$i" >> spicepod.yaml
      echo "    name: $i" >> spicepod.yaml
      echo "    params:" >> spicepod.yaml
      echo "      duckdb_open: $dbname" >> spicepod.yaml
      echo "    acceleration:" >> spicepod.yaml
      echo "      enabled: true" >> spicepod.yaml
      echo "      engine: sqlite" >> spicepod.yaml
      echo "      mode: file" >> spicepod.yaml
      echo "      params:" >> spicepod.yaml
      echo "        sqlite_file: $dbname_accelerated_prefix$i.db" >> spicepod.yaml
    done
  fi
fi
