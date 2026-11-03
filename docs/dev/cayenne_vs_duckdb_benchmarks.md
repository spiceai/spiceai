# Cayenne vs DuckDB benchmarks

This page documents the head-to-head **performance** comparison between the
Cayenne and DuckDB accelerators. The goal is to make it easy to confirm
Cayenne wins on every dimension that matters — ingestion, query, mutation,
retention, throughput — and to catch regressions early.

**Result correctness is a separate gate.** Full-content SQL result equality
compares **standalone engines outside Spice** (`duckdb` / `rusqlite` / `chdb-rust`
crates) with **Spice accelerators** (Cayenne, DuckDB accel, SQLite accel) on
TPC-H/TPC-DS/ClickBench/CH-benCHmark/SSB/SpiceBench/SQLLancer/micro. Entry points:

- `crates/cayenne/tests/correctness/` — inventory, standalone DuckDB↔SQLite
  baseline, Cayenne↔standalone oracles
- `crates/runtime/tests/result_correctness.rs` — Spice DuckDB/SQLite accelerators
  ↔ standalone oracles

Run with `cargo test -p cayenne --features result-correctness-duckdb|result-correctness-chdb`
and `cargo test -p runtime --features duckdb,sqlite --test integration result_correctness::`
— not with `cargo bench` or Criterion features. See
`crates/cayenne/tests/correctness/README.md`. Do not treat those tests as perf
regressions or fold them into this matrix.

The comparison runs in process: the `vs_duckdb_*` benches in
`crates/cayenne/benches/` drive `CayenneTableProvider` and an installed DuckDB,
loaded over ADBC through `duckdb-oracle`, with identical Arrow input. Run them
with `cargo bench -p cayenne --features duckdb-bench --bench <name>`.

There is no end-to-end comparison inside `spiced`: the DuckDB acceleration
engine was removed in v2.5 (#15684), so a Spicepod cannot accelerate into
DuckDB. For end-to-end Cayenne numbers, run the Cayenne Spicepods under
`test/spicepods/` with `testoperator run bench`, `throughput`, `load` or
`append`, and compare against an earlier `spiced` build.

## In-process micro-benchmarks

The `vs_duckdb_*` benches in `crates/cayenne/benches/` exercise the
accelerator-internal write/read paths directly, with no spiced and no
Flight. They run identical work against `CayenneTableProvider` and a
file-backed DuckDB database opened through `duckdb-oracle` (an installed
DuckDB over ADBC).

| Bench                  | What it measures                                                        |
| ---------------------- | ----------------------------------------------------------------------- |
| `vs_duckdb_ingest`     | Bulk load from parquet and incremental append of N batches              |
| `vs_duckdb_burst`      | Burst append patterns across Cayenne metastore lanes and DuckDB         |
| `vs_duckdb_concurrent` | Concurrent append and query workers against the same table              |
| `vs_duckdb_scan`       | `COUNT(*)`, full-column `SUM`, range-filtered `SUM`                     |
| `vs_duckdb_groupby`    | Grouped aggregate scans over identical data                             |
| `vs_duckdb_join`       | Same-source join query shapes and optimizer behavior                    |
| `vs_duckdb_pk_lookup`  | `WHERE id = ?`, `WHERE id IN (...)`, `WHERE id BETWEEN ? AND ?`         |
| `vs_duckdb_delete`     | DELETE of ~10% of rows, then scan exercising the deletion-vector filter |
| `vs_duckdb_upsert`     | Primary-key upsert conflict-resolution throughput                       |

Each bench groups Cayenne and DuckDB measurements together so criterion's
HTML report shows them on the same chart. To run the full suite:

```sh
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_ingest
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_burst
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_concurrent
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_scan
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_groupby
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_join
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_pk_lookup
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_delete
cargo bench -p cayenne --features duckdb-bench --bench vs_duckdb_upsert
```

Shared fixtures (schema, batch generation, parquet materialization,
Cayenne/DuckDB setup helpers) live in `vs_duckdb_helpers/common.rs` and
are included via `#[path = "vs_duckdb_helpers/common.rs"] mod common;`
from each bench file. The subdirectory keeps Cargo's bench
auto-discovery from picking up the helper as a standalone target.

### What's measured, what's not

The micro-benches isolate the engine's hot path. They explicitly do not
measure:

- Flight serialization or DataFusion plan construction overhead.
- Real read/write interference between queries and loads.
- Resource consumption (peak RSS, disk usage).

Measure those end to end on a Cayenne Spicepod with `testoperator`, against
an earlier `spiced` build. For "why is Cayenne winning or losing on path X?",
run the micro-bench that targets that path.

## Adding a new dimension

1. Add `crates/cayenne/benches/vs_duckdb_<dimension>.rs`, register it in
   `crates/cayenne/Cargo.toml`'s `[[bench]]` section, and reuse the helpers in
   `crates/cayenne/benches/vs_duckdb_helpers/common.rs` where possible.
2. Update this page with the new entry.
