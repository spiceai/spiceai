#!/bin/bash
# Load TPC-H SF0.01 (scale factor 0.01 for quick testing) into ScyllaDB
# Usage: ./load_tpch_scylladb.sh [scale_factor]

set -e

SCALE_FACTOR="${1:-0.01}"
KEYSPACE="tpch_sf1"
SCYLLA_HOST="${SCYLLA_HOST:-localhost}"
SCYLLA_PORT="${SCYLLA_PORT:-9042}"

echo "Loading TPC-H data with scale factor ${SCALE_FACTOR} into ScyllaDB..."
echo "Keyspace: ${KEYSPACE}"
echo "Host: ${SCYLLA_HOST}:${SCYLLA_PORT}"

# Create keyspace
echo "Creating keyspace..."
docker exec scylladb cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Create tables
echo "Creating tables..."

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.region;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.region (r_regionkey int PRIMARY KEY, r_name text, r_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.nation;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.nation (n_nationkey int PRIMARY KEY, n_name text, n_regionkey int, n_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.supplier;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.supplier (s_suppkey int PRIMARY KEY, s_name text, s_address text, s_nationkey int, s_phone text, s_acctbal decimal, s_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.customer;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.customer (c_custkey int PRIMARY KEY, c_name text, c_address text, c_nationkey int, c_phone text, c_acctbal decimal, c_mktsegment text, c_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.part;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.part (p_partkey int PRIMARY KEY, p_name text, p_mfgr text, p_brand text, p_type text, p_size int, p_container text, p_retailprice decimal, p_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.partsupp;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.partsupp (ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost decimal, ps_comment text, PRIMARY KEY (ps_partkey, ps_suppkey));"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.orders;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.orders (o_orderkey bigint PRIMARY KEY, o_custkey int, o_orderstatus text, o_totalprice decimal, o_orderdate date, o_orderpriority text, o_clerk text, o_shippriority int, o_comment text);"

docker exec scylladb cqlsh -e "DROP TABLE IF EXISTS ${KEYSPACE}.lineitem;"
docker exec scylladb cqlsh -e "CREATE TABLE ${KEYSPACE}.lineitem (l_orderkey bigint, l_linenumber int, l_partkey int, l_suppkey int, l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal, l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text, PRIMARY KEY (l_orderkey, l_linenumber));"

echo "Tables created successfully!"

# Generate TPC-H data using DuckDB and export to CSV
echo "Generating TPC-H data using DuckDB..."
TEMP_DIR=$(mktemp -d)
echo "Temp directory: ${TEMP_DIR}"

duckdb -c "
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${SCALE_FACTOR});

COPY region TO '${TEMP_DIR}/region.csv' (HEADER false);
COPY nation TO '${TEMP_DIR}/nation.csv' (HEADER false);
COPY supplier TO '${TEMP_DIR}/supplier.csv' (HEADER false);
COPY customer TO '${TEMP_DIR}/customer.csv' (HEADER false);
COPY part TO '${TEMP_DIR}/part.csv' (HEADER false);
COPY partsupp TO '${TEMP_DIR}/partsupp.csv' (HEADER false);
COPY orders TO '${TEMP_DIR}/orders.csv' (HEADER false);
COPY lineitem TO '${TEMP_DIR}/lineitem.csv' (HEADER false);
"

echo "CSV files generated!"
ls -la ${TEMP_DIR}/

# Copy CSV files into the Docker container
echo "Copying CSV files to Docker container..."
docker cp ${TEMP_DIR}/region.csv scylladb:/tmp/region.csv
docker cp ${TEMP_DIR}/nation.csv scylladb:/tmp/nation.csv
docker cp ${TEMP_DIR}/supplier.csv scylladb:/tmp/supplier.csv
docker cp ${TEMP_DIR}/customer.csv scylladb:/tmp/customer.csv
docker cp ${TEMP_DIR}/part.csv scylladb:/tmp/part.csv
docker cp ${TEMP_DIR}/partsupp.csv scylladb:/tmp/partsupp.csv
docker cp ${TEMP_DIR}/orders.csv scylladb:/tmp/orders.csv
docker cp ${TEMP_DIR}/lineitem.csv scylladb:/tmp/lineitem.csv

# Load data using COPY
echo "Loading data into ScyllaDB using COPY..."

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.region (r_regionkey, r_name, r_comment) FROM '/tmp/region.csv' WITH DELIMITER=',' AND NULL='';"
echo "region loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.nation (n_nationkey, n_name, n_regionkey, n_comment) FROM '/tmp/nation.csv' WITH DELIMITER=',' AND NULL='';"
echo "nation loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) FROM '/tmp/supplier.csv' WITH DELIMITER=',' AND NULL='';"
echo "supplier loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) FROM '/tmp/customer.csv' WITH DELIMITER=',' AND NULL='';"
echo "customer loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) FROM '/tmp/part.csv' WITH DELIMITER=',' AND NULL='';"
echo "part loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) FROM '/tmp/partsupp.csv' WITH DELIMITER=',' AND NULL='';"
echo "partsupp loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) FROM '/tmp/orders.csv' WITH DELIMITER=',' AND NULL='';"
echo "orders loaded"

docker exec scylladb cqlsh -e "COPY ${KEYSPACE}.lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) FROM '/tmp/lineitem.csv' WITH DELIMITER=',' AND NULL='';"
echo "lineitem loaded"

# Verify data
echo ""
echo "Verifying row counts..."
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.region;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.nation;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.supplier;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.customer;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.part;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.partsupp;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.orders;"
docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.lineitem;"

# Cleanup
rm -rf ${TEMP_DIR}

echo ""
echo "TPC-H data loaded successfully into ScyllaDB!"
echo "Keyspace: ${KEYSPACE}"
