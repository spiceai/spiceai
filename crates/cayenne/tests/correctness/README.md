# Cayenne result-correctness suite

**This is not a performance or Criterion benchmark suite.**

These integration tests assert that Cayenne returns the **same query results**
as reference engines (DuckDB, chDB) for the same SQL on identical input data.
They measure correctness only: schema + row multiset / ordered rows when
`ORDER BY`+`LIMIT` apply, with existing numeric tolerance for floats.

## Out of scope here

| Concern | Where it lives instead |
|---------|------------------------|
| Latency / throughput vs DuckDB or chDB | `crates/cayenne/benches/vs_duckdb_*`, `vs_chdb_*` |
| Perf matrix / `must_beat` spicepods | `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/` |
| How to run Criterion benches | `docs/dev/cayenne_vs_duckdb_benchmarks.md` |

Do not gate or interpret these tests as performance regressions.

## What runs

| Binary | Feature | Engines | Suites |
|--------|---------|---------|--------|
| `result_correctness_inventory_test` | (none) | — | Completeness of the query inventory + pure `compare_query_result_batches` |
| `result_correctness_vs_duckdb_test` | `result-correctness-duckdb` | Cayenne ↔ DuckDB | TPC-H SF1, TPC-DS SF1, ClickBench, **CH-benCHmark SF1 × {full, append, changes}**, SpiceBench (TPC-H scenario) SF1, SQLLancer corpus, micro SQL shapes |
| `result_correctness_vs_chdb_test` | `result-correctness-chdb` | Cayenne ↔ chDB | SQLLancer + micro (analytical suite SQL inventory-excluded with dialect reasons) |

### CH-benCHmark load-mode matrix

For CH-benCHmark only, Cayenne is loaded three ways (spicepod `refresh_mode` analogs)
against the same DuckDB final state:

| Mode | Cayenne API |
|------|-------------|
| `full` | `InsertOp::Overwrite` (bulk replace) |
| `append` | multiple `InsertOp::Append` chunks |
| `changes` | `write_cdc_append_stream` + `finish()` |

Query result content must match DuckDB after every mode.

DuckDB and chDB cannot be linked in one process; three-engine coverage is
**pairwise**.

## How to run

```bash
# Inventory + pure comparison unit path (no reference engine)
cargo test -p cayenne --test result_correctness_inventory_test

# Full correctness vs DuckDB (SF1 defaults)
CAYENNE_PARITY_TPCH_SF=1 CAYENNE_PARITY_TPCDS_SF=1 CAYENNE_PARITY_CHBENCH_SF=1 \
  cargo test -p cayenne --features result-correctness-duckdb \
  --test result_correctness_vs_duckdb_test

# Correctness vs chDB
cargo test -p cayenne --features result-correctness-chdb \
  --test result_correctness_vs_chdb_test
```

Optional env:

| Variable | Purpose |
|----------|---------|
| `CAYENNE_PARITY_SCRATCH` | Directory for logs / coverage markdown |
| `CAYENNE_PARITY_TPCH_SF` / `TPCDS_SF` / `CHBENCH_SF` | Scale (default 1) |
| `CLICKBENCH_HITS_PARQUET` | Real ClickBench hits file; else deterministic fixture |
| `SQLLANCER_EXTRA_SQL` | Extra newline-separated SQL on the SQLLancer schema |

## Layout

```
tests/correctness/README.md          ← this file
tests/correctness/support/
  harness.rs                         ← execute SQL + compare actual batches (only oracle)
  inventory.rs / sqllancer.rs / …    ← suites, fixtures, reports
tests/result_correctness_*.rs        ← integration test binaries that call the harness
```

## Who compares results?

**The harness does — not a human and not ad-hoc scripts.**

1. Integration tests **execute** SQL on Cayenne / DuckDB / chDB.
2. They pass the **actual** `RecordBatch` (or chDB CSV→Arrow) results into
   `support::compare_actual_results` → shipped
   `test_framework::queries::validation::compare_query_result_batches`.
3. They **`assert!`** / `assert_all_pass_or_excluded` on harness outcomes.

Logs under `CAYENNE_PARITY_SCRATCH` are diagnostics only; CI green/red is the
harness assertion. Do not “grade” logs by hand as the pass criterion.

CH-benCH load modes additionally run **cross-mode** actual-result compares
(`full` vs `append` vs `changes`) in the harness, not by transitive reasoning.
