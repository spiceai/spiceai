#!/bin/bash
# Copyright 2025 Spice AI
# SPDX-License-Identifier: Apache-2.0

# Load TPC-H data into ScyllaDB
set -e

DATA_DIR="${1:-../tpch-kit/dbgen}"
KEYSPACE="${SCYLLADB_KEYSPACE:-tpch_sf1}"
CONTAINER="${SCYLLADB_CONTAINER:-scylladb}"

echo "Creating keyspace..."

docker exec "$CONTAINER" cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

echo "Creating tables..."

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS region (r_regionkey INT PRIMARY KEY, r_name TEXT, r_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS nation (n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT, n_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS customer (c_custkey INT PRIMARY KEY, c_name TEXT, c_address TEXT, c_nationkey INT, c_phone TEXT, c_acctbal DECIMAL, c_mktsegment TEXT, c_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS supplier (s_suppkey INT PRIMARY KEY, s_name TEXT, s_address TEXT, s_nationkey INT, s_phone TEXT, s_acctbal DECIMAL, s_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS part (p_partkey INT PRIMARY KEY, p_name TEXT, p_mfgr TEXT, p_brand TEXT, p_type TEXT, p_size INT, p_container TEXT, p_retailprice DECIMAL, p_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost DECIMAL, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey));"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice DECIMAL, o_orderdate DATE, o_orderpriority TEXT, o_clerk TEXT, o_shippriority INT, o_comment TEXT);"

docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "CREATE TABLE IF NOT EXISTS lineitem (l_orderkey INT, l_linenumber INT, l_partkey INT, l_suppkey INT, l_quantity DECIMAL, l_extendedprice DECIMAL, l_discount DECIMAL, l_tax DECIMAL, l_returnflag TEXT, l_linestatus TEXT, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber));"

echo "Tables created."

# Remove trailing pipes from data files before copying
echo "Pre-processing data files..."
for table in region nation customer supplier part partsupp orders lineitem; do
    sed 's/|$//' "$DATA_DIR/$table.tbl" > "/tmp/${table}_clean.tbl"
done

# Copy cleaned data files to container
echo "Copying data files to container..."
docker cp /tmp/region_clean.tbl "$CONTAINER:/tmp/region.tbl"
docker cp /tmp/nation_clean.tbl "$CONTAINER:/tmp/nation.tbl"
docker cp /tmp/customer_clean.tbl "$CONTAINER:/tmp/customer.tbl"
docker cp /tmp/supplier_clean.tbl "$CONTAINER:/tmp/supplier.tbl"
docker cp /tmp/part_clean.tbl "$CONTAINER:/tmp/part.tbl"
docker cp /tmp/partsupp_clean.tbl "$CONTAINER:/tmp/partsupp.tbl"
docker cp /tmp/orders_clean.tbl "$CONTAINER:/tmp/orders.tbl"
docker cp /tmp/lineitem_clean.tbl "$CONTAINER:/tmp/lineitem.tbl"

echo "Loading data using COPY..."

# Load each table
echo "Loading region..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY region (r_regionkey, r_name, r_comment) FROM '/tmp/region.tbl' WITH DELIMITER='|';"

echo "Loading nation..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY nation (n_nationkey, n_name, n_regionkey, n_comment) FROM '/tmp/nation.tbl' WITH DELIMITER='|';"

echo "Loading customer..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) FROM '/tmp/customer.tbl' WITH DELIMITER='|';"

echo "Loading supplier..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) FROM '/tmp/supplier.tbl' WITH DELIMITER='|';"

echo "Loading part..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) FROM '/tmp/part.tbl' WITH DELIMITER='|';"

echo "Loading partsupp..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) FROM '/tmp/partsupp.tbl' WITH DELIMITER='|';"

echo "Loading orders..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) FROM '/tmp/orders.tbl' WITH DELIMITER='|';"

echo "Loading lineitem..."
docker exec "$CONTAINER" cqlsh -k "$KEYSPACE" -e "COPY lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) FROM '/tmp/lineitem.tbl' WITH DELIMITER='|';"

echo "Cleaning up..."
docker exec "$CONTAINER" rm -f /tmp/*.tbl

echo "Done! All TPC-H data loaded into $KEYSPACE keyspace."
