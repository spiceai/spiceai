# Cayenne vs DuckDB benchmarks

This page documents the head-to-head performance comparison between the
Cayenne and DuckDB accelerators. The goal is to make it easy to confirm
Cayenne wins on every dimension that matters — ingestion, query, mutation,
retention, throughput — and to catch regressions early.

The comparison has two layers:

| Layer | What it measures | Where it lives | How to run |
|---|---|---|---|
| End-to-end spicepod benchmarks | Real spiced ingesting from real sources, queries via Flight | `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/pairs.yaml` references existing yamls under `test/spicepods/` | `testoperator run bench` / `throughput` / `load` / `append` on each side of a pair |
| In-process micro-benchmarks | Direct `CayenneTableProvider` vs `duckdb::Connection` on identical Arrow input | `crates/cayenne/benches/vs_duckdb_*.rs` | `cargo bench -p cayenne --bench vs_duckdb_ingest` (or the other three) |

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

| Bench | What it measures |
|---|---|
| `vs_duckdb_ingest` | Bulk load from parquet and incremental append of N batches |
| `vs_duckdb_scan` | `COUNT(*)`, full-column `SUM`, range-filtered `SUM` |
| `vs_duckdb_pk_lookup` | `WHERE id = ?`, `WHERE id IN (...)`, `WHERE id BETWEEN ? AND ?` |
| `vs_duckdb_delete` | DELETE of ~10% of rows, then scan exercising the deletion-vector filter |

Each bench groups Cayenne and DuckDB measurements together so criterion's
HTML report shows them on the same chart. To run all four:

```sh
cargo bench -p cayenne --bench vs_duckdb_ingest
cargo bench -p cayenne --bench vs_duckdb_scan
cargo bench -p cayenne --bench vs_duckdb_pk_lookup
cargo bench -p cayenne --bench vs_duckdb_delete
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
- Concurrency or throughput under load — covered by `testoperator run
  throughput` against the same pods.
- Resource consumption (peak RSS, disk usage) — covered by spiced's OTLP
  metrics during layer-1 runs.

For end-to-end "is Cayenne winning?" answers, run the layer-1 pairs.
For "why is Cayenne winning/losing on path X?" investigations, run the
layer-2 micro-bench that targets that path.

## Adding a new dimension

1. If the new dimension is a workload, decide whether it's better served
   by a spicepod pair (more realistic) or a micro-bench (more isolated).
2. **Spicepod pair**: add the yamls under `test/spicepods/`, append an
   entry to `pairs.yaml`, and document any unavoidable asymmetry.
3. **Micro-bench**: add `crates/cayenne/benches/vs_duckdb_<dimension>.rs`,
   register it in `crates/cayenne/Cargo.toml`'s `[[bench]]` section, and
   reuse `vs_duckdb_common.rs` helpers where possible.
4. Update this page with the new entry.
