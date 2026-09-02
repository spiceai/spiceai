# Result-correctness suite (Spice accelerators × standalone engines)

**This is not a performance or Criterion benchmark suite.**

These tests assert **exact SQL result equality** (schema + row multiset, or
ordered rows when `ORDER BY`+`LIMIT` apply) and that each side **honors its own
query's `ORDER BY`** (see [Row order](#row-order)) between:

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

# Cayenne ↔ standalone DuckDB. `--test-threads=1` is required, not stylistic:
# parallel runs have hit allocator aborts in the bundled DuckDB crate, which fail
# in a way that reads like a correctness mismatch.
CAYENNE_PARITY_TPCH_SF=1 CAYENNE_PARITY_TPCDS_SF=1 CAYENNE_PARITY_CHBENCH_SF=1 \
  cargo test -p cayenne --features result-correctness-duckdb \
  --test result_correctness_vs_duckdb_test -- --test-threads=1

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

## Row order

Content equality is checked as a multiset unless a `LIMIT` makes the row set
itself order-dependent. Multiset comparison canonically sorts both sides first,
so on its own it says **nothing about the order an engine returned rows in** —
and most of the corpus sorts without a `LIMIT` (every CH-benCHmark query, every
SSB query with an `ORDER BY`, half of TPC-H). A wrong sort over the right rows
compared equal.

`compare_query_result_batches_with_sort_check` closes that: alongside the content
comparison it verifies **each side separately** against the query's own top-level
`ORDER BY`, resolved from the SQL by the parser
(`validation::sort_order::resolve_sort_key`). Because it is a self-check on one
engine's output it needs no oracle, so it runs on every lane — including the
single-oracle ones.

It stays deliberately narrow where engines legitimately differ:

- **Tied rows are never a violation.** An `ORDER BY` on a non-unique key leaves
  the order of equal rows engine-dependent; only a row that sorts strictly
  *before* its predecessor fails.
- **`NULL` placement is not policed unless the query states it.** DataFusion and
  PostgreSQL sort `NULL`s last for `ASC`, SQLite sorts them first, so a pair with
  a `NULL` on exactly one side is left unjudged. An explicit `NULLS FIRST` /
  `NULLS LAST` makes the placement part of the requested order, and is enforced.

  Two rows that are **both** `NULL` in a key column are tied under every
  convention, so the check continues to the next key column for them, as SQL
  requires. And because leaving a pair unjudged would hide an inversion that
  straddles a `NULL` — `[2, NULL, 1]` is illegal either way — the leading key
  column's non-`NULL` values are also checked as a subsequence. Only the leading
  one: a later column orders rows within a tie of those before it, so
  `ORDER BY cnt, state` may legally step `state` backwards when `cnt` changes.
- **A term that maps to no output column does not sink the whole key.** The
  mappable leading terms are still verified and the rest is named, so an
  `ORDER BY a, CASE …, b` still enforces `a`.

An `ORDER BY` inside a subquery, a CTE, or a window frame does not constrain the
result and is not read as a sort key — the check parses the statement rather than
searching for the text. The same parser decides whether a `LIMIT` is top-level,
which is what selects positional vs multiset content comparison.

### An unverified order is reported, never passed

`compare_query_result_batches_with_sort_check` returns
`SortCheckedComparison { result, unchecked }`. Anything the check could not
cover — an unparseable statement, a term that maps to no output column, a key
type with no comparator — lands in `unchecked` rather than folding into `Pass`.
The Cayenne harness turns that into `ParityOutcome::OrderUnchecked`, which
`report.rs` and `summary_line` count in their own bucket.

That distinction is the whole point: a coverage hole that reads as a pass is the
failure this check exists to remove, so it must not be reintroduced by the check
itself. A caller that ignores `unchecked` is back to reporting unverified order
as verified.

## Who compares results?

**The harness / shipped compare path — not a human reading logs.**

1. Execute SQL on each side (standalone crate and/or Spice accelerator).
2. Pass **actual** `RecordBatch` results into
   `compare_query_result_batches_with_sort_check` (or cayenne
   `compare_actual_results`, which wraps it).
3. **`assert!`** / `assert_all_pass_or_excluded` on outcomes.

Logs under `CAYENNE_PARITY_SCRATCH` are diagnostics only.
