# Result-correctness suite (Spice accelerators × standalone engines)

**This is not a performance or Criterion benchmark suite.**

These tests assert **exact SQL result equality** (schema + row multiset, or
ordered rows when `ORDER BY`+`LIMIT` apply) between:

1. **Standalone engines outside Spice** — raw embedded crates used as oracles  
   (`duckdb`, `rusqlite`, `chdb-rust`)
2. **Spice accelerators** — Cayenne, DuckDB accelerator, SQLite accelerator

They measure correctness only. Numeric compare uses existing float tolerance.

## Engine roles

| Label | What it is | How it is linked |
|-------|------------|------------------|
| `standalone-duckdb` | DuckDB **outside** Spice | `duckdb` crate (`query_arrow`) |
| `standalone-sqlite` | SQLite **outside** Spice | `rusqlite` (not Spice SQLite accel) |
| `standalone-chdb` | chDB **outside** Spice | `chdb-rust` |
| `spice-cayenne` | Cayenne accelerator | `CayenneTableProvider` |
| `spice-duckdb-accel` | Spice DuckDB accelerator | `accelerator_duckdb` |
| `spice-sqlite-accel` | Spice SQLite accelerator | `accelerator_sqlite` |

**DuckDB and chDB cannot co-link** in one process; multi-engine coverage is
pairwise across separate test binaries.

## Correctness matrix

```
                    standalone-duckdb   standalone-sqlite   standalone-chdb
standalone-duckdb          —            ✅ cayenne suite          —
standalone-sqlite          ✅                 —                   —
spice-cayenne              ✅                 ✅                   ✅
spice-duckdb-accel         ✅ (runtime)       —                   —
spice-sqlite-accel         —                  ✅ (runtime)        —
```

1. **Oracle baseline** — standalone DuckDB ↔ standalone SQLite agree on portable
   SQL (micro / SSB / SQLLancer) with **no Spice code in the path**.
2. **Accelerator gates** — each Spice accelerator matches a standalone oracle on
   the same data + SQL.

## Out of scope here

| Concern | Where it lives instead |
|---------|------------------------|
| Latency / throughput vs DuckDB or chDB | `crates/cayenne/benches/vs_duckdb_*`, `vs_chdb_*` |
| Perf matrix / `must_beat` spicepods | `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/` |
| How to run Criterion benches | `docs/dev/cayenne_vs_duckdb_benchmarks.md` |

## What runs

### Cayenne crate (`crates/cayenne/tests/`)

| Binary | Feature | Engines | Suites |
|--------|---------|---------|--------|
| `result_correctness_inventory_test` | (none) | — | Inventory completeness + pure `compare_query_result_batches` |
| `result_correctness_standalone_engines_test` | `result-correctness-duckdb` | **standalone DuckDB ↔ standalone SQLite** (no Spice) | micro, SSB, SQLLancer |
| `result_correctness_vs_duckdb_test` | `result-correctness-duckdb` | Cayenne ↔ standalone DuckDB | TPC-H/DS SF1, ClickBench, CH-benCH × modes, SSB, SpiceBench, SQLLancer, micro |
| `result_correctness_vs_chdb_test` | `result-correctness-chdb` | Cayenne ↔ standalone chDB | SQLLancer + micro |
| `result_correctness_vs_sqlite_test` | (none) | Cayenne ↔ standalone SQLite | SSB, SQLLancer, micro |

### Runtime crate (`crates/runtime/tests/result_correctness.rs`)

Dedicated binary (not the full `integration` suite):

| Test | Features | Engines | Suites |
|------|----------|---------|--------|
| `spice_duckdb_accel_vs_standalone_duckdb_micro` | `duckdb,sqlite` | Spice DuckDB accel ↔ standalone DuckDB | micro shapes |
| `spice_sqlite_accel_vs_standalone_sqlite_micro` | `duckdb,sqlite` | Spice SQLite accel ↔ standalone SQLite | micro shapes |

### CH-benCHmark load-mode matrix (Cayenne only)

| Mode | Cayenne API |
|------|-------------|
| `full` | `InsertOp::Overwrite` |
| `append` | multiple `InsertOp::Append` chunks |
| `changes` | `write_cdc_append_stream` + `finish()` |

### SSB

Classic Q1.1–Q4.3; pure-Rust deterministic star schema. Scale:
`CAYENNE_PARITY_SSB_SCALE` (default 1).

## How to run

```bash
# Inventory
cargo test -p cayenne --test result_correctness_inventory_test

# Standalone oracles only (DuckDB ↔ SQLite, no Spice)
cargo test -p cayenne --features result-correctness-duckdb \
  --test result_correctness_standalone_engines_test

# Cayenne ↔ standalone DuckDB
CAYENNE_PARITY_TPCH_SF=1 CAYENNE_PARITY_TPCDS_SF=1 CAYENNE_PARITY_CHBENCH_SF=1 \
  cargo test -p cayenne --features result-correctness-duckdb \
  --test result_correctness_vs_duckdb_test

# Cayenne ↔ standalone chDB
cargo test -p cayenne --features result-correctness-chdb \
  --test result_correctness_vs_chdb_test

# Cayenne ↔ standalone SQLite
cargo test -p cayenne --test result_correctness_vs_sqlite_test

# Spice DuckDB / SQLite accelerators ↔ standalone oracles
cargo test -p runtime --features duckdb,sqlite --test result_correctness -- --nocapture
```

Optional env: `CAYENNE_PARITY_SCRATCH`, `CAYENNE_PARITY_*_SF`,
`CAYENNE_PARITY_SSB_SCALE`, `CLICKBENCH_HITS_PARQUET`, `SQLLANCER_EXTRA_SQL`.

## Who compares results?

**The harness / shipped compare path — not a human reading logs.**

1. Execute SQL on each side (standalone crate and/or Spice accelerator).
2. Pass **actual** `RecordBatch` results into
   `compare_query_result_batches` (or cayenne `compare_actual_results`).
3. **`assert!`** / `assert_all_pass_or_excluded` on outcomes.

Logs under `CAYENNE_PARITY_SCRATCH` are diagnostics only.
