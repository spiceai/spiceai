#!/bin/bash

set -e
set -o pipefail


# two arguments, one is tpch or tpcds, the other one is the scale factor

if [ "$#" -ne 2 ]; then
  echo "Usage: ./setup-tpc-spicepod.bash <tpch or tpcds> <scale factor>"
  echo "Example: ./setup-tpc-spicepod.bash tpcds 1"
  exit 1
fi

# verify the scale factor is an integer
re='^[0-9]+$'
if ! [[ $2 =~ $re ]] ; then
  echo "Usage: ./setup-tpc-spicepod.bash <tpch or tpcds> <scale factor>"
  echo "error: scale factor must be an integer"
  exit 1
fi

# verify tpch or tpcds
if [ "$1" != "tpch" ] && [ "$1" != "tpcds" ]; then
  echo "Usage: ./setup-tpc-spicepod.bash <tpch or tpcds> <scale factor>"
  echo "Example: ./setup-tpc-spicepod.bash tpcds 1"
  exit 1
fi

# test if duckdb command exists
if ! type "duckdb" 1> /dev/null 2>&1; then
  echo "'duckdb' is required"
fi

dbname="$1-sf$2.db"
generate_command() {
  if [ "$1" = "tpch" ]; then
    echo "INSTALL tpch; LOAD tpch; CALL dbgen(sf = $2)"
  elif [ "$1" = "tpcds" ]; then
    echo "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = $2)"
  fi
}
if [ -f "$dbname" ]; then
  echo "Database '$dbname' already exists. Skipping creation, using existing database."
else
  duckdbcommand=`generate_command $1 $2`
  duckdb -c "$duckdbcommand" "$dbname"
  echo "Created database '$dbname'"
fi

# test if spicepod.yaml exists
if [ -f "spicepod.yaml" ]; then
  echo "spicepod.yaml found. Aborting."
  exit 1
fi

echo "version: v1beta1" >> spicepod.yaml
echo "kind: Spicepod" >> spicepod.yaml
echo "name: $dbname" >> spicepod.yaml

echo "datasets:" >> spicepod.yaml

if [ "$1" = "tpch" ]; then
  for i in customer lineitem nation orders part partsupp region supplier; do
    echo "  - from: duckdb:$i" >> spicepod.yaml
    echo "    name: $i" >> spicepod.yaml
    echo "    params:" >> spicepod.yaml
    echo "      duckdb_open: $dbname" >> spicepod.yaml
    echo "    acceleration:" >> spicepod.yaml
    echo "      enabled: true" >> spicepod.yaml
  done
fi

if [ "$1" = "tpcds" ]; then
  for i in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do
    echo "  - from: duckdb:$i" >> spicepod.yaml
    echo "    name: $i" >> spicepod.yaml
    echo "    params:" >> spicepod.yaml
    echo "      duckdb_open: $dbname" >> spicepod.yaml
    echo "    acceleration:" >> spicepod.yaml
    echo "      enabled: true" >> spicepod.yaml
  done
fi

spiced
