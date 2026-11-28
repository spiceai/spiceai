---
name: verify-federation-pushdown-semantics
description: Check a running spiced for DuckDB federation pushdown that changes query results. Use after changing FunctionSupport, the DuckDB dialect, or can_evaluate_expression, to confirm SIMILAR TO, LIKE without ESCAPE, substr, / and % by zero, to_char, to_unixtime and cardinality answer the same federated as in DataFusion.
---

# Verify federation pushdown semantics

Project-local check that a running `spiced` gives the same answer for a query
whether DuckDB evaluates it (federated) or DataFusion does.

## When to use

- After changing `runtime-udfs-api`'s `FunctionSupportBuilder`, the policies in
  `crates/runtime-datafusion/src/function_support.rs`, or the DuckDB checks in
  `crates/runtime-datafusion/src/dialect/mod.rs`.
- To collect before and after evidence for a pushdown change.

## What it checks

One 4-row CSV (`k,s,x,n`) is registered twice: `fed` (`duckdb:read_csv`, so
the query is pushed to DuckDB) and `ref` (`file:`, evaluated by DataFusion).
Each probe runs against both:

1. `SIMILAR TO` and `NOT SIMILAR TO` (once pushed as `LIKE` with the regex
   pattern unchanged).
2. `LIKE`/`ILIKE` with a backslash and no `ESCAPE` (DuckDB has no default
   escape) and a plain `LIKE`.
3. `substr` with a negative or zero start (DuckDB counts from the end) and a
   positive start.
4. `x / 0`, `n % 0`, `n / (k - 3)` (DuckDB answers `NULL` where DataFusion
   fails or gives an infinity), and `n / 2`, `n % 2`.
5. `to_char`, `to_unixtime`, `cardinality` (not in DuckDB, or different).

Both routes must answer with a JSON array, and the two arrays must match byte
for byte. The two probes DataFusion fails by design (`n % 0`, `n / (k - 3)`)
must fail on both routes with `Divide by zero`; any other failure is a
mismatch, so a shared planning error never passes. Two `EXPLAIN` checks follow:
`SIMILAR TO` must not reach DuckDB as `LIKE 'cust_[0-9]+'`, and `n / 2` must
still push (`//` in `base_sql`).

## Run

Needs a `spiced` with the `duckdb` feature, `curl`, and `python3`. The script
starts `spiced` unless `REUSE=1` and exits non-zero on any mismatch.

```bash
SPICED=/path/to/spiced .agents/skills/verify-federation-pushdown-semantics/run.sh
```

Optional environment: `HTTP`, `FLIGHT`, `METRICS` (bind addresses, defaults
`127.0.0.1:1859{0,1,2}`), `WORKDIR` (kept), `KEEP=1` (leave `spiced` running),
`REUSE=1` (use a `spiced` already serving this spicepod at `HTTP`).

For before and after evidence, run once with the released or trunk binary and
once with the branch binary, and save both transcripts.

## In-process companion

The unit tests in `crates/runtime-datafusion/src/function_support.rs`
(`similar_to_stays_local_on_every_backend`,
`duckdb_keeps_a_possibly_zero_divisor_local`,
`duckdb_keeps_a_backslash_like_pattern_local`,
`duckdb_keeps_a_substr_outside_the_shared_positions_local`,
`duckdb_keeps_functions_it_lacks_local`) cover the same decisions at the
planning level, without a server.
