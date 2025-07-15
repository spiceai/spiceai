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
  duckdb tpch.db "COPY $table TO '${table}.csv' (DELIMITER ',');"
done
```

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