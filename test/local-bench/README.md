# Local benchmark harness

Reproduces the `testoperator_run_bench.yml` benchmark runs locally for the
configurations affected by the current fixes, using the same fleet TPC-DS SF1
dataset (DuckDB's `tpcds` dsdgen — the generator all recorded expected answers
come from), the same seeding path (`test/tpc-bench/Makefile` targets), and the
same runner (`testoperator run bench`).

## Prerequisites

- Docker (for `postgres:16` and MinIO containers)
- `psql`/`createdb` (`brew install libpq && brew link --force libpq`)
- A spiced binary: `cargo build --release -p spiced --features postgres,duckdb`

## Usage

```bash
cd test/local-bench

./10-setup-storage.sh          # start postgres:16 + MinIO   (idempotent; --reset to recreate)
./20-populate-data.sh          # generate + load fleet SF1    (idempotent; --force to reload)
```

The spicepods hard-code `pg_port: 5432` (CI parity), so the benchmark postgres
container must own host port 5432. If a personal PostgreSQL is running there,
stop it first (`brew services stop postgresql@16`) — or set up only the
non-postgres benches with `./10-setup-storage.sh minio`. The seed script only
ever writes to the `spice-bench-postgres` container, never to another server
that happens to be on 5432.

```bash

./30-run-bench.sh s3-cayenne           # eager-agg ROLLUP fix: q22/q36/q70/q86
./30-run-bench.sh file-cayenne         # eager-agg ROLLUP fix
./30-run-bench.sh postgres-federated   # fleet-seed fix: q17/q25/q85 zero rows
./30-run-bench.sh postgres-arrow       # fleet-seed fix
./30-run-bench.sh postgres-duckdb      # fleet-seed fix
```

## What "fixed" looks like

Runs may still report failures from pre-existing explain-plan snapshot drift
(`Explain plan snapshot assertion failed`) — that is cleared separately by the
trunk-side snapshot regeneration pass. The fixes are verified when the failure
list contains **none** of:

- `Query execution failed ... col.name() == matching_name` (tpcds_q36, cayenne)
- `Query driver task ended unexpectedly` (tpcds_q22/q70/q86, cayenne)
- `returned 0 rows` (tpcds_q17/q25/q85, postgres configs)

To view current plans instead of asserting old snapshots:
`INSTA_UPDATE=always ./30-run-bench.sh <config>` rewrites the local `.snap`
files (review with `git diff`, never commit unreviewed).

## Cleanup

```bash
docker rm -f spice-bench-postgres spice-bench-minio
rm -rf .work ../tpc-bench/tmp
```
