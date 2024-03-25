# TPC-H Setup

Instructions to create test queries and dataset for [TPC-H benchmark](https://www.tpc.org/tpch/). 

## Prerequisites
- Linux or MacOS
- Git
- `make` utility
- PostgreSQL installed and running to load test dataset

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
Pass PostgresSQL connection parameters as needed:

```bash
PGPORT=5432 PGUSER=postgres make pg-init
PGPORT=5432 PGUSER=postgres make pg-load
```

## Run TPC-H queries
Verify generated queries and test data by running queries against configured PostgreSQL instance using `make tpch-run-pq`, for example

```bash
PGPORT=5432 PGUSER=postgres make tpch-run-pq
```
