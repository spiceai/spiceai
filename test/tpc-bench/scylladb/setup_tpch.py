#!/usr/bin/env python3
# Copyright 2025 Spice AI
# SPDX-License-Identifier: Apache-2.0

"""Load TPC-H data into ScyllaDB."""

import csv
import os
from cassandra.cluster import Cluster
from datetime import datetime

# ScyllaDB connection
HOST = os.environ.get("SCYLLADB_HOST", "127.0.0.1")
PORT = int(os.environ.get("SCYLLADB_PORT", "9042"))
KEYSPACE = os.environ.get("SCYLLADB_KEYSPACE", "tpch_sf1")

# TPC-H data directory
DATA_DIR = os.path.join(os.path.dirname(__file__), "../tpch-kit/dbgen")


def create_keyspace_and_tables(session):
    """Create keyspace and TPC-H tables."""
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

    # Region table
    session.execute("""
        CREATE TABLE IF NOT EXISTS region (
            r_regionkey INT PRIMARY KEY,
            r_name TEXT,
            r_comment TEXT
        )
    """)

    # Nation table
    session.execute("""
        CREATE TABLE IF NOT EXISTS nation (
            n_nationkey INT PRIMARY KEY,
            n_name TEXT,
            n_regionkey INT,
            n_comment TEXT
        )
    """)

    # Customer table
    session.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            c_custkey INT PRIMARY KEY,
            c_name TEXT,
            c_address TEXT,
            c_nationkey INT,
            c_phone TEXT,
            c_acctbal DECIMAL,
            c_mktsegment TEXT,
            c_comment TEXT
        )
    """)

    # Supplier table
    session.execute("""
        CREATE TABLE IF NOT EXISTS supplier (
            s_suppkey INT PRIMARY KEY,
            s_name TEXT,
            s_address TEXT,
            s_nationkey INT,
            s_phone TEXT,
            s_acctbal DECIMAL,
            s_comment TEXT
        )
    """)

    # Part table
    session.execute("""
        CREATE TABLE IF NOT EXISTS part (
            p_partkey INT PRIMARY KEY,
            p_name TEXT,
            p_mfgr TEXT,
            p_brand TEXT,
            p_type TEXT,
            p_size INT,
            p_container TEXT,
            p_retailprice DECIMAL,
            p_comment TEXT
        )
    """)

    # Partsupp table
    session.execute("""
        CREATE TABLE IF NOT EXISTS partsupp (
            ps_partkey INT,
            ps_suppkey INT,
            ps_availqty INT,
            ps_supplycost DECIMAL,
            ps_comment TEXT,
            PRIMARY KEY (ps_partkey, ps_suppkey)
        )
    """)

    # Orders table
    session.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            o_orderkey INT PRIMARY KEY,
            o_custkey INT,
            o_orderstatus TEXT,
            o_totalprice DECIMAL,
            o_orderdate DATE,
            o_orderpriority TEXT,
            o_clerk TEXT,
            o_shippriority INT,
            o_comment TEXT
        )
    """)

    # Lineitem table
    session.execute("""
        CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey INT,
            l_linenumber INT,
            l_partkey INT,
            l_suppkey INT,
            l_quantity DECIMAL,
            l_extendedprice DECIMAL,
            l_discount DECIMAL,
            l_tax DECIMAL,
            l_returnflag TEXT,
            l_linestatus TEXT,
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct TEXT,
            l_shipmode TEXT,
            l_comment TEXT,
            PRIMARY KEY (l_orderkey, l_linenumber)
        )
    """)

    print("Tables created successfully.")


def load_region(session):
    """Load region data."""
    filepath = os.path.join(DATA_DIR, "region.tbl")
    stmt = session.prepare("""
        INSERT INTO region (r_regionkey, r_name, r_comment)
        VALUES (?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 3:
                continue
            session.execute(stmt, [int(row[0]), row[1], row[2]])
    print("Loaded region table.")


def load_nation(session):
    """Load nation data."""
    filepath = os.path.join(DATA_DIR, "nation.tbl")
    stmt = session.prepare("""
        INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment)
        VALUES (?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 4:
                continue
            session.execute(stmt, [int(row[0]), row[1], int(row[2]), row[3]])
    print("Loaded nation table.")


def load_customer(session):
    """Load customer data."""
    filepath = os.path.join(DATA_DIR, "customer.tbl")
    stmt = session.prepare("""
        INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 8:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [
                    int(row[0]),
                    row[1],
                    row[2],
                    int(row[3]),
                    row[4],
                    Decimal(row[5]),
                    row[6],
                    row[7],
                ],
            )
    print("Loaded customer table.")


def load_supplier(session):
    """Load supplier data."""
    filepath = os.path.join(DATA_DIR, "supplier.tbl")
    stmt = session.prepare("""
        INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 7:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [
                    int(row[0]),
                    row[1],
                    row[2],
                    int(row[3]),
                    row[4],
                    Decimal(row[5]),
                    row[6],
                ],
            )
    print("Loaded supplier table.")


def load_part(session):
    """Load part data."""
    filepath = os.path.join(DATA_DIR, "part.tbl")
    stmt = session.prepare("""
        INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 9:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [
                    int(row[0]),
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    int(row[5]),
                    row[6],
                    Decimal(row[7]),
                    row[8],
                ],
            )
    print("Loaded part table.")


def load_partsupp(session):
    """Load partsupp data."""
    filepath = os.path.join(DATA_DIR, "partsupp.tbl")
    stmt = session.prepare("""
        INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
        VALUES (?, ?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 5:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [int(row[0]), int(row[1]), int(row[2]), Decimal(row[3]), row[4]],
            )
    print("Loaded partsupp table.")


def load_orders(session):
    """Load orders data."""
    filepath = os.path.join(DATA_DIR, "orders.tbl")
    stmt = session.prepare("""
        INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 9:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [
                    int(row[0]),
                    int(row[1]),
                    row[2],
                    Decimal(row[3]),
                    datetime.strptime(row[4], "%Y-%m-%d").date(),
                    row[5],
                    row[6],
                    int(row[7]),
                    row[8],
                ],
            )
    print("Loaded orders table.")


def load_lineitem(session):
    """Load lineitem data."""
    filepath = os.path.join(DATA_DIR, "lineitem.tbl")
    stmt = session.prepare("""
        INSERT INTO lineitem (l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    count = 0
    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 16:
                continue
            from decimal import Decimal

            session.execute(
                stmt,
                [
                    int(row[0]),
                    int(row[3]),  # l_linenumber is 4th column
                    int(row[1]),
                    int(row[2]),
                    Decimal(row[4]),
                    Decimal(row[5]),
                    Decimal(row[6]),
                    Decimal(row[7]),
                    row[8],
                    row[9],
                    datetime.strptime(row[10], "%Y-%m-%d").date(),
                    datetime.strptime(row[11], "%Y-%m-%d").date(),
                    datetime.strptime(row[12], "%Y-%m-%d").date(),
                    row[13],
                    row[14],
                    row[15],
                ],
            )
            count += 1
            if count % 10000 == 0:
                print(f"Loaded {count} lineitem rows...")

    print(f"Loaded lineitem table ({count} rows).")


def main():
    """Main entry point."""
    print(f"Connecting to ScyllaDB at {HOST}:{PORT}...")
    cluster = Cluster([HOST], port=PORT)
    session = cluster.connect()

    try:
        create_keyspace_and_tables(session)

        print("Loading TPC-H data...")
        load_region(session)
        load_nation(session)
        load_customer(session)
        load_supplier(session)
        load_part(session)
        load_partsupp(session)
        load_orders(session)
        load_lineitem(session)

        print("All data loaded successfully!")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
