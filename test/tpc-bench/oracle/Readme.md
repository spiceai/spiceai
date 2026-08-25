# Oracle TPC-H Benchmark Setup

Steps to load the TPC-H SF1 dataset into the Oracle database.

## Prerequisites

- An Oracle Database instance with administrative access
- Oracle SQLPlus and SQL*Loader utilities installed
- DuckDB installed and populated with TPC-H data (`tpch.db`)

## Setup Instructions

### 1. Create Database Schema

Connect to the Oracle instance using SQLPlus and execute the setup script:

```sql
SQL> @setup_tpch.sql
```

### 2. Export Data from DuckDB

Generate CSV files from the TPC-H DuckDB database:

```bash
for table in customer lineitem nation orders part partsupp region supplier; do
  duckdb tpch.db "COPY $table TO '${table}.csv' (FORMAT CSV, DELIMITER ',', HEADER false, FORCE_QUOTE *);"
done
```

`FORCE_QUOTE *` encloses every value, and the control files specify `PRESERVE BLANKS`. TPC-H
string columns (`C_ADDRESS`, `S_ADDRESS`, and every `*_COMMENT`) legitimately contain leading
spaces, and SQL*Loader trims leading whitespace from a delimited field that is not enclosed, so
without both of these the loaded rows silently differ from the TPC-H answer set (spiceai#6450).
`HEADER false` keeps the column-name row out of the input, which SQL*Loader would otherwise
reject into a `.bad` file.

### 3. Load Data into Oracle

- Replace `password` and `connection_string` with your actual Oracle credentials

#### Option A: Individual Commands

```bash
sqlldr admin/password@connection_string control=customer.ctl direct=true
sqlldr admin/password@connection_string control=orders.ctl direct=true
sqlldr admin/password@connection_string control=lineitem.ctl direct=true
sqlldr admin/password@connection_string control=part.ctl direct=true
sqlldr admin/password@connection_string control=partsupp.ctl direct=true
sqlldr admin/password@connection_string control=nation.ctl direct=true
sqlldr admin/password@connection_string control=region.ctl direct=true
sqlldr admin/password@connection_string control=supplier.ctl direct=true
```

#### Option B: Batch Script

```bash
USER="admin"
PASS="your_password"
CONNECT_STRING="your_connection_string"

for table in customer orders lineitem part partsupp nation region supplier; do
  echo "Loading $table..."
  sqlldr "${USER}/${PASS}@${CONNECT_STRING}" control="${table}.ctl" direct=true
done
```

### 4. Verify the Load

A load that reports no errors can still have altered the data, so check the row counts and the
whitespace-sensitive columns against the source DuckDB database before benchmarking.

```sql
SELECT COUNT(*) FROM TPCH_SF1.LINEITEM;   -- SF1: 6001215
SELECT COUNT(*) FROM TPCH_SF1.CUSTOMER;   -- SF1: 150000

-- Leading spaces must survive the load. SF1 expects 2353 / 796269.
SELECT SUM(CASE WHEN C_ADDRESS LIKE ' %' THEN 1 ELSE 0 END) FROM TPCH_SF1.CUSTOMER;
SELECT SUM(CASE WHEN L_COMMENT LIKE ' %' THEN 1 ELSE 0 END) FROM TPCH_SF1.LINEITEM;

-- Spot-check a known value: 10 characters, with a leading space.
SELECT LENGTH(C_ADDRESS) FROM TPCH_SF1.CUSTOMER WHERE C_CUSTKEY = 146149;  -- 10
```

The same counts from the source database:

```bash
duckdb tpch.db "SELECT (SELECT count(*) FILTER (c_address LIKE ' %') FROM customer) AS customer,
                       (SELECT count(*) FILTER (l_comment LIKE ' %') FROM lineitem) AS lineitem;"
```
