---
name: verify-duckdb-decimal-division
description: Reproduce DuckDB (and SQLite) acceleration decimal-division correctness against a running spiced. Use when verifying that amt/3 on a DECIMAL(38,10) past 2^53 matches the unaccelerated table, that price/7 truncates, and that integer / is unchanged.
---

# Verify DuckDB decimal division

Project-local check that a running `spiced` evaluates decimal `/` on a
`DuckDB` acceleration the way `DataFusion` does, not as a `DOUBLE` via `//`.

## When to use

- After changing DuckDB/SQLite federation pushdown, the unparser dialect, or
  `can_evaluate_expression`.
- To collect trunk-vs-branch before/after evidence for this defect.

## What it checks

Against one parquet file (`amt DECIMAL(38,10)`, `price DECIMAL(10,2)`, `n INT`):

1. `SELECT id, amt/3, price/7 FROM t_duckdb WHERE id IN (1,5,8)` equals `t_fed`.
2. id 5's `amt/3` contains `33333333333333333333` and not the `DOUBLE`
   garbage `33333333333333333440`.
3. `price/7` truncates (`0.014285`, not `0.014286`).
4. `SELECT n/2 FROM t_duckdb WHERE id = 1` is `3`.
5. `EXPLAIN` of the decimal query has no `//` in `base_sql`; the integer
   query still has `//`.

`t_sqlite` is queried too. Wide-decimal storage is #14662, so only `price/7`,
integer `/`, and the `EXPLAIN` guard are required there.

## Run

Needs a `spiced` with the `duckdb` (and optionally `sqlite`) feature, `curl`,
and `python3` with `pyarrow` (`pip install 'numpy>=2' pyarrow`). The script
starts `spiced` unless `REUSE=1`, writes each `/v1/sql` body under `WORKDIR`,
and exits non-zero when `t_duckdb` disagrees with `t_fed`, `t_sqlite`
`price/7` or integer `/` is wrong, or `EXPLAIN` still pushes decimal `/`
as `//`. A launched `spiced` is killed on `EXIT` unless `KEEP=1`.

```bash
# from the repo root; writes a workdir under /tmp
SPICED=/path/to/spiced .cursor/skills/verify-duckdb-decimal-division/run.sh
```

Optional environment:

- `SPICED` — `spiced` binary (default: `spiced` on `PATH`, then
  `target/debug/spiced`)
- `HTTP` — bind address (default `127.0.0.1:18490`)
- `FLIGHT` — bind address (default `127.0.0.1:18491`)
- `WORKDIR` — existing directory; default is a fresh temp dir
- `KEEP=1` — leave the workdir and `spiced` running

Trunk vs branch: run once with the trunk binary and once with the branch
binary, same `run.sh`. Save both transcripts. The decimal query on trunk
`t_duckdb` shows the `33440` garbage digits; the branch matches `t_fed`.

## In-process companion

`crates/runtime/tests/acceleration/decimal_division.rs` is the same fixture
and assertions through the in-process `Runtime` (memory and file `DuckDB`,
memory `SQLite`).
