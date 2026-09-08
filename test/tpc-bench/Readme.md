# TPC-H Setup

Instructions to create test queries and dataset for [TPC-H benchmark](https://www.tpc.org/tpch/).

## Prerequisites

- Linux or MacOS
- Git
- `make` utility
- PostgreSQL or MySQL instance to load TPC-H dataset

## Generate TPC-H test dataset and test queries

Run commands below to generate test queries (`tpch_queries.sql`) and test data (`tmp/*.tbl`):

```bash
make tpch-init
make tpch-gen
```

## Load test dataset to PostgreSQL

Run commands below to create `tpch` dataset and load test data:

```bash
make pg-init
make pg-load
```

Pass PostgreSQL connection parameters as needed:

```bash
PGPORT=5432 PGUSER=postgres make pg-init
PGPORT=5432 PGUSER=postgres make pg-load
```

## Verify TCP-H queries using PostgreSQL

Verify generated queries and test data by running queries against configured PostgreSQL instance using `make tpch-run-pq`, for example

```bash
PGPORT=5432 PGUSER=postgres make tpch-run-pq
```

## Load test dataset to MySQL

Ensure `local_infile` is enabled on the MySQL database by first connecting using your `mysql` client and running `SET GLOBAL local_infile=ON;`. This security setting permits the loading on data from files on the client-side, and is disabled by default. The option will disable again at server reboot.

Run commands below to create `tpch` dataset and load test data:

```bash
make mysql-init
make mysql-load
```

Pass MySQL connection parameters as needed:

```bash
DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root make mysql-init
DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root make mysql-load
```

## Run TCP-H queries

1. Use code editor to update `tpch-spicepod` PostgreSQL configuration to match your environment

```yaml
params:
  pg_host: localhost
  pg_port: "5432"
  pg_db: tpch
  pg_user: postgres
  pg_pass: postgres
```

1. Start the Spice runtime: `cd tpch-spicepod && spice run`
1. Start the Spice SQL: `spice sql`
1. Enter the test query from `test/tpch/tpch-queries.sql`
