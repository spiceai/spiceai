# DataFusion Table Providers

Note: This is not an official Apache Software Foundation project.

The goal of this repo is to extend the capabilities of DataFusion to support additional data sources via implementations of the `TableProvider` trait.

Many of the table providers in this repo are for querying data from other database systems. Those providers also integrate with the [`datafusion-federation`](https://github.com/datafusion-contrib/datafusion-federation/) crate to allow for more efficient query execution, such as pushing down joins between multiple tables from the same database system, or efficiently implementing TopK style queries (`SELECT * FROM table ORDER BY foo LIMIT 10`).

To use these table providers with efficient federation push-down, add the `datafusion-federation` crate and create a DataFusion `SessionContext` using the Federation optimizer rule and query planner with:

```rust
use datafusion::prelude::SessionContext;

let state = datafusion_federation::default_session_state();
let ctx = SessionContext::with_state(state);

// Register the specific table providers into ctx
// queries will now automatically be federated
```

## Table Providers

- PostgreSQL
- MySQL
- SQLite
- ClickHouse
- DuckDB
- Flight SQL
- MongoDB
- ODBC

## Examples (in Rust)

Run the included examples to see how to use the table providers:

### DuckDB

```bash
# Read from a table in a DuckDB file
cargo run --example duckdb --features duckdb
# Create an external table backed by DuckDB directly in DataFusion
cargo run --example duckdb_external_table --features duckdb
# Use the result of a DuckDB function call as the source of a table
cargo run --example duckdb_function --features duckdb
```

### SQLite

```bash
# Run from repo folder
cargo run --example sqlite --features sqlite
```

### Postgres

In order to run the Postgres example, you need to have a Postgres server running. You can use the following command to start a Postgres server in a Docker container the example can use:

```bash
docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
# Wait for the Postgres server to start
sleep 30

# Create a table in the Postgres server and insert some data
docker exec -i postgres psql -U postgres -d postgres_db <<EOF
CREATE TABLE companies (
   id INT PRIMARY KEY,
  name VARCHAR(100)
);

INSERT INTO companies (id, name) VALUES
    (1, 'Acme Corporation'),
    (2, 'Widget Inc.'),
    (3, 'Gizmo Corp.'),
    (4, 'Tech Solutions'),
    (5, 'Data Innovations');

CREATE VIEW companies_view AS
  SELECT id, name FROM companies;

CREATE MATERIALIZED VIEW companies_materialized_view AS
  SELECT id, name FROM companies;
EOF
```

```bash
# Run from repo folder
cargo run -p datafusion-table-providers --example postgres --features postgres

```

### ClickHouse

In order to run the Clickhouse example, you need to have a Clickhouse server running. You can use the following command to start a Clickhouse server in a Docker container the example can use:

```bash
docker run --name clickhouse \
    -e CLICKHOUSE_DB=default \
    -e CLICKHOUSE_USER=admin \
    -e CLICKHOUSE_PASSWORD=secret \
    -p 8123:8123 \
    -p 9000:9000 \
    -d clickhouse/clickhouse-server:24.8-alpine

# 2. Wait for readiness
echo "Waiting for ClickHouse to start..."
until curl -s http://localhost:8123/ping | grep -q 'Ok'; do
    sleep 2
done
echo

# 3. Create tables and a parameterized view
docker exec -i clickhouse clickhouse-client \
    --user=admin --password=secret --multiquery <<EOF
CREATE TABLE IF NOT EXISTS Reports (
  id UInt32,
  name String
) ENGINE = Memory;

INSERT INTO Reports VALUES (1, 'Monthly Report'), (2, 'Quarterly Report');

CREATE TABLE IF NOT EXISTS Users (
  workspace_uid String,
  id UInt32,
  username String
) ENGINE = Memory;

INSERT INTO Users VALUES
  ('abc', 1, 'alice'),
  ('abc', 2, 'bob'),
  ('xyz', 3, 'charlie');

CREATE OR REPLACE VIEW Users AS
SELECT workspace_uid, id, username
FROM Users
WHERE workspace_uid = {workspace_uid:String};
EOF
```

```bash
# Run from repo folder
cargo run -p datafusion-table-providers --example clickhouse --features clickhouse
```

### MySQL

In order to run the MySQL example, you need to have a MySQL server running. You can use the following command to start a MySQL server in a Docker container the example can use:

```bash
docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=mysql_db -p 3306:3306 -d mysql:9.0
# Wait for the MySQL server to start
sleep 30

# Create a table in the MySQL server and insert some data
docker exec -i mysql mysql -uroot -ppassword mysql_db <<EOF
CREATE TABLE companies (
   id INT PRIMARY KEY,
  name VARCHAR(100)
);

INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
EOF
```

```bash
# Run from repo folder
cargo run -p datafusion-table-providers --example mysql --features mysql
```

### Flight SQL

```bash
brew install roapi
# or
# cargo install --locked --git https://github.com/roapi/roapi --branch main --bins roapi
roapi -t taxi=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet &

# Run from repo folder
cargo run -p datafusion-table-providers --example flight-sql --features flight
```

### MongoDB

In order to run the MongoDB example, you need to have a MongoDB server running. You can use the following command to start a MongoDB server in a Docker container the example can use:

```bash
docker run --name mongodb \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  -e MONGO_INITDB_DATABASE=mongo_db \
  -p 27017:27017 \
  -d mongo:7.0
# Wait for the MongoDB server to start
sleep 30

# Create a table in the MongoDB server and insert some data
docker exec -i mongodb mongosh -u root -p password --authenticationDatabase admin <<EOF
use mongo_db;
db.companies.insertOne({
  id: 1,
  name: "Acme Corporation"
});
EOF

# Run from repo folder
cargo run -p datafusion-table-providers --example mongodb --features mongodb
```

### ODBC

```bash
apt-get install unixodbc-dev libsqliteodbc
# or
# brew install unixodbc & brew install sqliteodbc

cargo run --example odbc_sqlite --features odbc
```

#### ARM Mac

Please see https://github.com/pacman82/odbc-api#os-x-arm--mac-m1 for reference.

Steps:

1. Install unixodbc and sqliteodbc by `brew install unixodbc sqliteodbc`.
2. Find local sqliteodbc driver path by running `brew info sqliteodbc`. The path might look like `/opt/homebrew/Cellar/sqliteodbc/0.99991`.
3. Set up odbc config file at `~/.odbcinst.ini` with your local sqliteodbc path.
   Example config file:

```
[SQLite3]
Description = SQLite3 ODBC Driver
Driver      = /opt/homebrew/Cellar/sqliteodbc/0.99991/lib/libsqlite3odbc.dylib
```

4. Test configuration by running `odbcinst -q -d -n SQLite3`. If the path is printed out correctly, then you are all set.

## Examples (in Python)

1. Start a Python venv
2. Enter into venv
3. Inside python/ folder, run `maturin develop`.
4. Inside python/examples/ folder, run the corresponding test using `python3 [file_name]`.
