# Cayenne vs DuckDB benchmarks

This page documents the head-to-head performance comparison between the
Cayenne and DuckDB accelerators. The goal is to make it easy to confirm
Cayenne wins on every dimension that matters — ingestion, query, mutation,
retention, throughput — and to catch regressions early.

The comparison has two layers:

- End-to-end spicepod benchmarks: real `spiced` ingesting from real sources,
  with queries via Flight. These live in
  `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/pairs.yaml`, which
  references existing yamls under `test/spicepods/`. Run them with
  `testoperator run bench`, `throughput`, `load`, or `append` on each side of a pair.
- Mixed append+query benchmarks: analytical query workers running while
  append/upsert/retention loads mutate data. These are `pairs.yaml` entries
  with `workload: mixed` and append dispatch yamls under
  `tools/testoperator/dispatch/tpch/`. Run them with
  `testoperator run append --concurrency <N> --load-interval <S> --load-steps <N>`.
- In-process micro-benchmarks: direct `CayenneTableProvider` vs
  `duckdb::Connection` on identical Arrow input. These live in
  `crates/cayenne/benches/vs_duckdb_*.rs` and run with
  `cargo bench -p cayenne --features duckdb-bench --bench <name>`.

## Layer 1 — Spicepod matrix

`tools/testoperator/dispatch/perf-cayenne-vs-duckdb/pairs.yaml` lists every
paired (cayenne, duckdb) spicepod plus the workload they should be
compared on. The manifest references existing yamls under
`test/spicepods/` rather than duplicating them, so the comparison always
runs against the same pods that drive the dedicated benchmark workflows.

To compare a single pair locally:

```sh
# Cayenne side
cargo run -p testoperator -- run bench \
  -p test/spicepods/tpch/sf1/accelerated/file\[parquet\]-cayenne\[file\].yaml \
  -s spiced -d ./.data --query-set tpch --validate

# DuckDB side
cargo run -p testoperator -- run bench \
  -p test/spicepods/tpch/sf1/accelerated/file\[parquet\]-duckdb\[file\].yaml \
  -s spiced -d ./.data --query-set tpch --validate
```

Then diff the query durations. A first-class `testoperator compare`
subcommand that ingests `pairs.yaml` and produces a side-by-side report is
planned — see the manifest's README for the input format.

### Mixed append+query runs

The matrix includes `workload: mixed` entries for real-world interference
tests: one or more analytical query workers loop over the query set while
the append worker periodically generates new load files. This is the
Cayenne-vs-DuckDB analogue of the CH-benCH idea: reads and writes compete
for the same accelerator instead of being measured in isolation.

Run both sides of the SF1 mixed pair locally with the same duration,
append cadence, load count, and query-worker count:

```sh
# Cayenne side
cargo run -p testoperator -- run append \
  -p test/spicepods/tpch/sf1/accelerated/append/file\[parquet\]-cayenne\[file\]-append.yaml \
  -s spiced -d ./.data --query-set tpch --validate \
  --duration 720 --concurrency 4 --load-interval 30 --load-steps 20

# DuckDB side
cargo run -p testoperator -- run append \
  -p test/spicepods/tpch/sf1/accelerated/append/file\[parquet\]-duckdb\[file\]-append.yaml \
  -s spiced -d ./.data --query-set tpch --validate \
  --duration 720 --concurrency 4 --load-interval 30 --load-steps 20
```

The pass/fail bar is correctness first: all analytical queries must
succeed, appended row counts must match expectations, and memory/health
metrics must remain stable. The performance comparison then looks at
query latency under write pressure, append completion, and resource usage.
For a source-level OLTP plus analytical-query benchmark, use
`testoperator run htap --query-set chbench`; that command follows the
CH-benCH shape directly and complements these accelerator-pair runs.

### Fair-comparison rules

Two pods are a valid pair only if they differ in the accelerator engine
(and accelerator-specific tuning) and nothing else. Specifically:

- `mode: file` on both sides. Cayenne does not support memory mode, so a
  `cayenne[file]` vs `duckdb[memory]` pair is rejected.
- Identical source connector, schema, primary key, partition column,
  retention policy, refresh policy, and `on_conflict` semantics.
- The same query overrides on both runs (or none).

When something must differ — e.g. one engine supports a feature the other
doesn't — the pair carries a `notes:` explanation and `must_beat: false`.
See `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/README.md` for
the full rule set.

## Layer 2 — In-process micro-benchmarks

The `vs_duckdb_*` benches in `crates/cayenne/benches/` exercise the
accelerator-internal write/read paths directly, with no spiced and no
Flight. They run identical work against `CayenneTableProvider` and a
file-backed `duckdb::Connection`.

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

- Flight serialization or DataFusion plan construction overhead — those
  are covered by the layer-1 spicepod benchmarks.
- Real read/write interference — covered by the mixed append+query runs,
  where analytical query workers execute while append loads are generated.
- Resource consumption (peak RSS, disk usage) — covered by spiced's OTLP
  metrics during layer-1 runs.

For end-to-end "is Cayenne winning?" answers, run the layer-1 pairs.
For "why is Cayenne winning/losing on path X?" investigations, run the
layer-2 micro-bench that targets that path.

## Adding a new dimension

1. If the new dimension is a workload, decide whether it's better served
   by a spicepod pair (more realistic) or a micro-bench (more isolated).
2. **Spicepod pair**: add the yamls under `test/spicepods/`, append an
  entry to `pairs.yaml`, and document any unavoidable asymmetry. For a
  real-world mixed workload, use `workload: mixed` and include the append
  cadence plus query concurrency in the entry.
3. **Micro-bench**: add `crates/cayenne/benches/vs_duckdb_<dimension>.rs`,
   register it in `crates/cayenne/Cargo.toml`'s `[[bench]]` section, and
   reuse the helpers in `crates/cayenne/benches/vs_duckdb_helpers/common.rs`
   where possible.
4. Update this page with the new entry.
