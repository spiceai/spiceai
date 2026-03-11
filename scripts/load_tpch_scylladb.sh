#!/bin/bash
# Copyright 2025 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Load TPC-H data into ScyllaDB via CQL or Alternator (DynamoDB) API
# Usage: ./load_tpch_scylladb.sh [scale_factor] [mode]
#   scale_factor: TPC-H scale factor (default: 0.01)
#   mode: 'cql' (default), 'alternator', or 'both'
#
# Examples:
#   ./load_tpch_scylladb.sh 1           # Load SF1 via CQL
#   ./load_tpch_scylladb.sh 1 alternator # Load SF1 via Alternator API
#   ./load_tpch_scylladb.sh 1 both      # Load SF1 via both APIs
#
# Environment variables:
#   SCYLLA_HOST: CQL host (default: localhost)
#   SCYLLA_PORT: CQL port (default: 9042)
#   ALTERNATOR_HOST: Alternator host (default: localhost)
#   ALTERNATOR_PORT: Alternator port (default: 8000)

set -e

SCALE_FACTOR="${1:-0.01}"
MODE="${2:-cql}"
KEYSPACE="tpch_sf1"
SCYLLA_HOST="${SCYLLA_HOST:-localhost}"
SCYLLA_PORT="${SCYLLA_PORT:-9042}"
ALTERNATOR_HOST="${ALTERNATOR_HOST:-localhost}"
ALTERNATOR_PORT="${ALTERNATOR_PORT:-8000}"
ALTERNATOR_ENDPOINT="http://${ALTERNATOR_HOST}:${ALTERNATOR_PORT}"

# Table prefix for Alternator tables
ALTERNATOR_TABLE_PREFIX="tpch_"

echo "=============================================="
echo "TPC-H ScyllaDB Data Loader"
echo "=============================================="
echo "Scale Factor: ${SCALE_FACTOR}"
echo "Mode: ${MODE}"
echo "CQL Host: ${SCYLLA_HOST}:${SCYLLA_PORT}"
echo "Alternator Endpoint: ${ALTERNATOR_ENDPOINT}"
echo "=============================================="

# Generate TPC-H data using DuckDB and export to CSV
generate_tpch_data() {
    echo ""
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
}

# Load data via CQL
load_via_cql() {
    echo ""
    echo "=============================================="
    echo "Loading data via CQL..."
    echo "=============================================="
    
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
    echo "Verifying CQL row counts..."
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.region;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.nation;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.supplier;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.customer;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.part;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.partsupp;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.orders;"
    docker exec scylladb cqlsh -e "SELECT COUNT(*) FROM ${KEYSPACE}.lineitem;"

    echo ""
    echo "CQL data loaded successfully!"
    echo "Keyspace: ${KEYSPACE}"
}

# Load data via Alternator (DynamoDB API)
load_via_alternator() {
    echo ""
    echo "=============================================="
    echo "Loading data via Alternator (DynamoDB API)..."
    echo "=============================================="
    
    # Check if Python and boto3 are available
    if ! command -v python3 &> /dev/null; then
        echo "Error: python3 is required for Alternator loading"
        exit 1
    fi

    # Create a Python script to load data via boto3
    LOADER_SCRIPT="${TEMP_DIR}/alternator_loader.py"
    cat > "${LOADER_SCRIPT}" << 'PYTHON_SCRIPT'
#!/usr/bin/env python3
"""Load TPC-H data into ScyllaDB Alternator (DynamoDB-compatible API)."""

import boto3
import csv
import os
import sys
import time
from decimal import Decimal
from botocore.config import Config

# Configuration from environment
ENDPOINT_URL = os.environ.get('ALTERNATOR_ENDPOINT', 'http://localhost:8000')
TABLE_PREFIX = os.environ.get('TABLE_PREFIX', 'tpch_')
TEMP_DIR = os.environ.get('TEMP_DIR', '/tmp')
AWS_REGION = 'us-east-1'

# Batch write settings
BATCH_SIZE = 25  # DynamoDB max batch size

# Create DynamoDB client for Alternator
config = Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=50
)

dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url=ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id='none',
    aws_secret_access_key='none',
    config=config
)

client = boto3.client(
    'dynamodb',
    endpoint_url=ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id='none',
    aws_secret_access_key='none',
    config=config
)

# Table definitions with DynamoDB schema
TABLES = {
    'region': {
        'key_schema': [{'AttributeName': 'r_regionkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 'r_regionkey', 'AttributeType': 'N'}],
        'columns': ['r_regionkey', 'r_name', 'r_comment'],
        'types': ['N', 'S', 'S']
    },
    'nation': {
        'key_schema': [{'AttributeName': 'n_nationkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 'n_nationkey', 'AttributeType': 'N'}],
        'columns': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'],
        'types': ['N', 'S', 'N', 'S']
    },
    'supplier': {
        'key_schema': [{'AttributeName': 's_suppkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 's_suppkey', 'AttributeType': 'N'}],
        'columns': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'],
        'types': ['N', 'S', 'S', 'N', 'S', 'N', 'S']
    },
    'customer': {
        'key_schema': [{'AttributeName': 'c_custkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 'c_custkey', 'AttributeType': 'N'}],
        'columns': ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
        'types': ['N', 'S', 'S', 'N', 'S', 'N', 'S', 'S']
    },
    'part': {
        'key_schema': [{'AttributeName': 'p_partkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 'p_partkey', 'AttributeType': 'N'}],
        'columns': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'],
        'types': ['N', 'S', 'S', 'S', 'S', 'N', 'S', 'N', 'S']
    },
    'partsupp': {
        'key_schema': [
            {'AttributeName': 'ps_partkey', 'KeyType': 'HASH'},
            {'AttributeName': 'ps_suppkey', 'KeyType': 'RANGE'}
        ],
        'attributes': [
            {'AttributeName': 'ps_partkey', 'AttributeType': 'N'},
            {'AttributeName': 'ps_suppkey', 'AttributeType': 'N'}
        ],
        'columns': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
        'types': ['N', 'N', 'N', 'N', 'S']
    },
    'orders': {
        'key_schema': [{'AttributeName': 'o_orderkey', 'KeyType': 'HASH'}],
        'attributes': [{'AttributeName': 'o_orderkey', 'AttributeType': 'N'}],
        'columns': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        'types': ['N', 'N', 'S', 'N', 'S', 'S', 'S', 'N', 'S']
    },
    'lineitem': {
        'key_schema': [
            {'AttributeName': 'l_orderkey', 'KeyType': 'HASH'},
            {'AttributeName': 'l_linenumber', 'KeyType': 'RANGE'}
        ],
        'attributes': [
            {'AttributeName': 'l_orderkey', 'AttributeType': 'N'},
            {'AttributeName': 'l_linenumber', 'AttributeType': 'N'}
        ],
        'columns': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
        'types': ['N', 'N', 'N', 'N', 'N', 'N', 'N', 'N', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S']
    }
}


def create_table(table_name, schema):
    """Create a DynamoDB table if it doesn't exist."""
    full_name = f"{TABLE_PREFIX}{table_name}"
    
    try:
        # Try to delete existing table
        try:
            client.delete_table(TableName=full_name)
            print(f"  Deleted existing table {full_name}")
            time.sleep(2)
        except client.exceptions.ResourceNotFoundException:
            pass
        
        # Create table
        client.create_table(
            TableName=full_name,
            KeySchema=schema['key_schema'],
            AttributeDefinitions=schema['attributes'],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"  Created table {full_name}")
        
        # Wait for table to be active
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=full_name, WaiterConfig={'Delay': 1, 'MaxAttempts': 30})
        
    except Exception as e:
        print(f"  Error creating table {full_name}: {e}")
        raise


def convert_value_for_resource(value, dtype):
    """Convert a CSV value for DynamoDB resource API (Table.batch_writer)."""
    if not value or value == '':
        return None
    
    if dtype == 'N':
        return Decimal(str(value))
    else:
        return str(value)


def load_table_data(table_name, schema):
    """Load data from CSV into DynamoDB table using Table.batch_writer()."""
    full_name = f"{TABLE_PREFIX}{table_name}"
    csv_file = os.path.join(TEMP_DIR, f"{table_name}.csv")
    
    if not os.path.exists(csv_file):
        print(f"  CSV file not found: {csv_file}")
        return 0
    
    table = dynamodb.Table(full_name)
    columns = schema['columns']
    types = schema['types']
    
    total_count = 0
    
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        
        # Use table.batch_writer() which handles batching and retries
        with table.batch_writer() as batch:
            for row in reader:
                item = {}
                for i, (col, dtype) in enumerate(zip(columns, types)):
                    if i < len(row):
                        val = convert_value_for_resource(row[i], dtype)
                        if val is not None:
                            item[col] = val
                
                if item:
                    try:
                        batch.put_item(Item=item)
                        total_count += 1
                    except Exception as e:
                        print(f"  Error putting item: {e}")
                
                if total_count % 50000 == 0 and total_count > 0:
                    print(f"    {table_name}: {total_count} rows loaded...")
    
    return total_count


def get_table_count(table_name):
    """Get approximate item count for a table."""
    full_name = f"{TABLE_PREFIX}{table_name}"
    try:
        response = client.describe_table(TableName=full_name)
        return response['Table'].get('ItemCount', 0)
    except Exception:
        return 0


def main():
    print(f"Alternator Endpoint: {ENDPOINT_URL}")
    print(f"Table Prefix: {TABLE_PREFIX}")
    print(f"Data Directory: {TEMP_DIR}")
    print()
    
    # Test connection
    try:
        response = client.list_tables()
        print(f"Connected! Existing tables: {response.get('TableNames', [])}")
    except Exception as e:
        print(f"Failed to connect to Alternator: {e}")
        sys.exit(1)
    
    print()
    print("Creating tables...")
    for table_name, schema in TABLES.items():
        create_table(table_name, schema)
    
    print()
    print("Loading data...")
    for table_name, schema in TABLES.items():
        print(f"  Loading {table_name}...")
        count = load_table_data(table_name, schema)
        print(f"    {table_name}: {count} rows loaded")
    
    print()
    print("Verifying table counts...")
    for table_name in TABLES.keys():
        count = get_table_count(table_name)
        print(f"  {TABLE_PREFIX}{table_name}: ~{count} items")
    
    print()
    print("Alternator data load complete!")


if __name__ == '__main__':
    main()
PYTHON_SCRIPT

    # Create a virtual environment if boto3 is not available
    VENV_DIR="${TEMP_DIR}/venv"
    PYTHON_CMD="python3"
    
    if ! python3 -c "import boto3" 2>/dev/null; then
        echo "boto3 not found. Creating virtual environment..."
        python3 -m venv "${VENV_DIR}"
        source "${VENV_DIR}/bin/activate"
        pip install --quiet boto3
        PYTHON_CMD="${VENV_DIR}/bin/python3"
    fi

    # Run the loader script
    echo "Running Alternator data loader..."
    ALTERNATOR_ENDPOINT="${ALTERNATOR_ENDPOINT}" \
    TABLE_PREFIX="${ALTERNATOR_TABLE_PREFIX}" \
    TEMP_DIR="${TEMP_DIR}" \
    ${PYTHON_CMD} "${LOADER_SCRIPT}"


    echo ""
    echo "Alternator data loaded successfully!"
    echo "Table prefix: ${ALTERNATOR_TABLE_PREFIX}"
}

# Main execution
case "${MODE}" in
    cql)
        generate_tpch_data
        load_via_cql
        ;;
    alternator)
        generate_tpch_data
        load_via_alternator
        ;;
    both)
        generate_tpch_data
        load_via_cql
        load_via_alternator
        ;;
    *)
        echo "Unknown mode: ${MODE}"
        echo "Usage: $0 [scale_factor] [mode]"
        echo "  mode: cql, alternator, or both"
        exit 1
        ;;
esac

# Cleanup
rm -rf ${TEMP_DIR}

echo ""
echo "=============================================="
echo "TPC-H data load complete!"
echo "=============================================="
