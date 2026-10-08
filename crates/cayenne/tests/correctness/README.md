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

Cayenne against each oracle, by suite:

| Suite | DuckDB | SQLite | chDB |
|-------|--------|--------|------|
| TPC-H | ✅ SF1 | ✅ SF 0.1 (`CAYENNE_PARITY_SQLITE_TPCH_SF`) | ✅ SF1 |
| TPC-DS | ✅ SF1 (`dsdgen`) | ✅ SF 0.1 (`tpcdsgen`, `CAYENNE_PARITY_SQLITE_TPCDS_SF`) | ✅ SF1 (`tpcdsgen`) |
| ClickBench | ✅ reduced `hits` | ✅ reduced `hits` | ✅ reduced `hits` |
| CH-benCHmark | ✅ full/append/changes | ✅ full | ✅ full/append/changes |
| SSB | ✅ | ✅ | — |
| SQLLancer, micro | ✅ | ✅ | ✅ |

Two engines disagreeing say *someone* is wrong; three say who. SQLite and
ClickHouse share no code or lineage with DataFusion, so they are where a bug
Cayenne inherits from DataFusion shows up — a DuckDB mismatch that Cayenne's own
DataFusion baseline agrees with is reported as a failure, never excused.

## Asking an oracle the same question

The suites are written for DataFusion. `support/dialect.rs` rewrites each query
for SQLite or ClickHouse on the parsed statement — it never plans the query in
DataFusion, since an oracle fed DataFusion's reading would agree with Cayenne on
exactly the bugs it is there to catch. Every rule keeps the query's meaning or
refuses: a construct an oracle cannot express (SQLite has no `ROLLUP`,
`stddev_samp` or regular expressions) is named by `untranslatable`, and the
inventory records that name as the cell's exclusion.

| Oracle | What differs from DataFusion | How the lane asks it |
|--------|------------------------------|----------------------|
| both | `NULL` placement when an `ORDER BY` states none | every `ORDER BY` states DataFusion's: last ascending, first descending |
| both | `0.06 + 0.01` is a double, below the `0.07` a row holds (TPC-H Q6) | literal decimal arithmetic folded exactly |
| both | a join predicate inside every `OR` branch becomes a cross product (TPC-H Q19) | common conjuncts factored out of the `OR` |
| SQLite | no `DATE`, `DECIMAL` or `INTERVAL` | dates stored as ISO text, compared with ISO literals and `date()` arithmetic; decimals stored and cast as `REAL` |
| SQLite | `LIKE` folds ASCII case | `PRAGMA case_sensitive_like` |
| ClickHouse | an outer join's unmatched side is `0`, not `NULL` (TPC-H Q13); `SUM` of nothing is `0`; bare `UNION` is an error | session settings in `support/chdb_engine.rs` |
| ClickHouse | `/` is floating-point on integers and keeps only the dividend's scale on decimals | `intDiv` for integers, doubles otherwise |
| ClickHouse | `DATE` wraps before 1970 | dates cast as `Date32` |
| DuckDB | `/` on integers returns a double | `SET integer_division = true` |

A lane prints the rewritten SQL beside any mismatch it reports.

## Fixtures

Every lane loads Cayenne and its oracle from the same parquet files.

| Suite | Generator |
|-------|-----------|
| TPC-H | `tpchgen`, in-process (`support/tpch_data.rs`) |
| TPC-DS | DuckDB's `dsdgen` for the DuckDB lane; `tpcdsgen`'s C-compatible mode for the SQLite and chDB lanes, whose processes cannot drive DuckDB (`support/tpcds_data.rs`). Row counts, keys and dimensions such as `item` agree; fact-table measures do not (SF1 `store_sales`: 2,880,404 rows in both, `sum(ss_quantity)` 138,963,631 against 138,943,711), so a query can select rows from one and none from the other. |
| ClickBench | the reduced, ranking-deterministic `hits` table (`support/clickbench_data.rs`), or `CLICKBENCH_HITS_PARQUET` |
| CH-benCHmark | a synthetic TPC-C warehouse with TPC-H's nations, built in-process so every query's filters select rows (`support/chbench_data.rs`) |

## Empty answers are not passes

Two engines that both return nothing agree, and have compared nothing. A cell
whose two answers hold no value — no rows, or only `NULL` — is `Vacuous`, not
`Pass`, and a lane fails on it unless the inventory's `empty_result_review`
names why that query's answer is empty on the fixture the lane loaded. A review
names its fixtures (`inventory::fixture`): TPC-DS Q13 selects nothing from
`tpcdsgen`'s SF 0.1 rows and answers at SF1, so an empty Q13 fails the SF1 lanes.
The census lists those reviewed holes per fixture; each is a query to reach with
better data, not coverage.

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
| `result_correctness_vs_chdb_test` | `result-correctness-chdb` | Cayenne ↔ standalone chDB | TPC-H/DS SF1, ClickBench, CH-benCH × modes, SQLLancer, micro |
| `result_correctness_vs_sqlite_test` | (none) | Cayenne ↔ standalone SQLite | TPC-H/DS SF 0.1, ClickBench, CH-benCH full load, SSB, SQLLancer, micro |

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

# Cayenne ↔ standalone SQLite. TPC-H and TPC-DS run at SF 0.1 unless
# CAYENNE_PARITY_SQLITE_TPCH_SF / CAYENNE_PARITY_SQLITE_TPCDS_SF say otherwise,
# which keeps this binary, gated by `make nextest`, inside the gate's per-test
# ceiling; the DuckDB and chDB lanes compare SF1.
cargo test -p cayenne --test result_correctness_vs_sqlite_test -- --test-threads=1

# Spice DuckDB / SQLite accelerators ↔ standalone oracles
cargo test -p runtime --features duckdb,sqlite --test result_correctness -- --nocapture
```

## What the gate runs

`make nextest` builds with `--features cayenne/result-correctness-duckdb`, which
is what makes the DuckDB and oracle-baseline binaries exist at all: cargo skips a
test target whose `required-features` are unmet without reporting it, so before
that flag the filterset selected them and they silently never ran.

| Binary | In `make nextest` |
|--------|-------------------|
| `result_correctness_inventory_test` | yes |
| `result_correctness_census_test` | yes |
| `result_correctness_vs_sqlite_test` | yes |
| `result_correctness_standalone_engines_test` | yes |
| `result_correctness_vs_duckdb_test` | yes |
| `result_correctness_vs_chdb_test` | no — runs in `.github/workflows/correctness_chdb.yml` |
| runtime `result_correctness` | no — see below |

The chDB lane has a job of its own for two reasons. `chdb-rust` fetches libchdb
at build time, so folding it into the gate would make every sign-off depend on
that fetch and on a machine that can link it; and the two embedded engines must
not both be *called* in one process — linking them together is fine, but driving
DuckDB and chDB from the same binary aborts it at startup on a static-init
conflict. `make nextest` says out loud that this lane is not in its run, because
a target whose `required-features` are unmet is dropped by cargo silently, which
is how the lanes above once went unbuilt.

The runtime accelerator lane stays out of the fast gate on purpose.
`runtime/duckdb,runtime/sqlite` flow through the whole `--all` build, so
every runtime test binary the gate builds relinks with them at hundreds of megabytes
each — a permanent cost on every sign-off for two micro-shape comparisons. It
belongs in the integration workflow, which already builds with `duckdb,sqlite`.

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
  requires — and two `NULL`s likewise hold a tie group together. Leaving pairs
  unjudged would hide two shapes that no convention produces, so both are caught
  within the run of rows tied on the columns before the key: an inversion
  straddling a `NULL` (`[2, NULL, 1]`), by checking the key column's non-`NULL`
  values as a subsequence; and an interleaved `NULL` block (`[1, NULL, 2]`, which
  is neither `NULLS FIRST`'s `[NULL, 1, 2]` nor `NULLS LAST`'s `[1, 2, NULL]`), by
  rejecting a run that crosses between `NULL` and non-`NULL` more than once. So
  `ORDER BY cnt, state` may still step `state` backwards when `cnt` changes,
  while either shape inside one `cnt` group is caught.

  The value scan and the `NULL` block restart per group; the boundary the
  `NULL`s sit against does not. Placement belongs to the term, which the engine
  sorts the whole result by, so `state` trailing its `NULL`s in one `cnt` group
  and leading with them in the next is an order no placement produces — caught
  even though each group on its own looks fine.
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
