# Mode A TPC-H baseline

Captured on this box against the workspace DataFusion fork after the
VarChar literal fix ([spiceai/datafusion#215](https://github.com/spiceai/datafusion/pull/215)).
The workspace pin is now the `spiceai-54` merge commit
`6006901cb602d845ee1441269d6eaa142c2580a6`. The measured table below was
already taken on the pre-merge tip
`2e6ebfd97adcf6d6d192d1d4f23d2e67fff4395c` (same VarChar fix); Mode A
was not re-run after the merge-commit pin bump.
Re-run the command below to regenerate `results/mode-a-tpch.json`
(gitignored; CI uploads it as an artifact).

## Pins

| Item | Value |
|------|--------|
| IBM tag | `v0.1.1` |
| `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion rev | `6006901cb602d845ee1441269d6eaa142c2580a6` (`spiceai-54`, merged spiceai/datafusion#215) |
| Suite | TPC-H SF 0.01 (22 queries) |
| Oracle | DuckDB 1.2.0 (IBM goldens) |
| Run | 2026-09-04T16:07:58Z → 16:08:02Z |

## Counts

| PASS | FAIL | SKIP | ERROR | Total |
|------|------|------|-------|-------|
| 5 | 14 | 0 | 3 | 22 |

Command and headline output:

```text
cargo run -p spice-substrait-compliance -- \
  --mode mode-a \
  --suite tools/substrait-compliance/.ibm/test-suites/tpch \
  --out-json tools/substrait-compliance/results/mode-a-tpch.json \
  --out-csv tools/substrait-compliance/results/mode-a-tpch.csv

DataFusion fork rev: 2e6ebfd97adcf6d6d192d1d4f23d2e67fff4395c
5/14/3  pass/fail/skip+error  total=22
  passed=5 failed=14 skipped=0 errored=3
```

Pre-#215 baseline on the same harness (same IBM tag, older DF pin
`f9a635e6b580d5fe6ed0a70975e36014ea86c476`): **PASS 1 | FAIL 7 | SKIP 0 | ERROR 14 | Total 22**.

## Per-query

| Query | Status | Notes |
|-------|--------|-------|
| q01 | FAIL | column 9 type `bigint` != `integer` (`COUNT` is `Int64` in DataFusion) |
| q02 | FAIL | leading space on a `CHAR`/`varchar` cell (` foxes boost…` vs `foxes boost…`) |
| q03 | **PASS** | |
| q04 | FAIL | column 1 type `bigint` != `integer` |
| q05 | **PASS** | |
| q06 | FAIL | cell `1193053.2253` vs `1193053.225299999` (abs delta ≈ 1.16e-9, over IBM ε = 1e-9) |
| q07 | ERROR | `from_substrait_plan`: `Function argument non-Value type not supported` |
| q08 | ERROR | `from_substrait_plan`: `Function argument non-Value type not supported` |
| q09 | ERROR | `from_substrait_plan`: `Function argument non-Value type not supported` |
| q10 | FAIL | leading space on a `CHAR`/`varchar` cell |
| q11 | FAIL | column name `TOTAL_VALUE` != `value` |
| q12 | FAIL | column 1 type `bigint` != `integer` |
| q13 | FAIL | column 0 type `bigint` != `integer` |
| q14 | **PASS** | |
| q15 | FAIL | trailing space on a `CHAR`/`varchar` cell |
| q16 | FAIL | column 3 type `bigint` != `integer` |
| q17 | FAIL | cell `''` != `'""'` (empty vs quoted-empty golden) |
| q18 | FAIL | column name `TOTAL_QTY` != `sum(l_quantity)` |
| q19 | **PASS** | |
| q20 | **PASS** | |
| q21 | FAIL | row count `0` != `1` |
| q22 | FAIL | column 0 type `string` != `integer` |

## Former VarChar ERROR queries (13)

Isthmus `VarChar` literals no longer ERROR after DF #215. None of these
13 remain ERROR for `unsupported VarChar literal`.

| Query | Pre-#215 | After #215 (measured) | Notes |
|-------|----------|-----------------------|-------|
| q02 | ERROR (VarChar `EUROPE`) | FAIL | `CHAR` padding |
| q03 | ERROR (VarChar `BUILDING`) | **PASS** | |
| q05 | ERROR (VarChar `ASIA`) | **PASS** | |
| q07 | ERROR (VarChar `FRANCE`) | ERROR | non-Value function argument (not VarChar) |
| q08 | ERROR (VarChar `AMERICA`) | ERROR | non-Value function argument (not VarChar) |
| q11 | ERROR (VarChar `GERMANY`) | FAIL | output alias |
| q12 | ERROR (VarChar `MAIL`) | FAIL | `COUNT` width |
| q16 | ERROR (VarChar `Brand#45`) | FAIL | `COUNT` width |
| q17 | ERROR (VarChar `Brand#23`) | FAIL | empty vs `"\"\""` golden |
| q19 | ERROR (VarChar `Brand#12`) | **PASS** | |
| q20 | ERROR (VarChar `CANADA`) | **PASS** | |
| q21 | ERROR (VarChar `SAUDI ARABIA`) | FAIL | 0 rows vs 1 expected |
| q22 | ERROR (VarChar `13`) | FAIL | `string` vs `integer` |

Summary of those 13: **PASS 4** (q03, q05, q19, q20) · **FAIL 7** (q02, q11, q12, q16, q17, q21, q22) · **ERROR 2** (q07, q08 — non-Value function arg, same class as q09).

q09 remains ERROR (non-Value function argument), as expected. It was never a VarChar ERROR.

## Failure groups

| Group | Count | Queries | Meaning |
|-------|-------|---------|---------|
| Non-value function argument | 3 | q07, q08, q09 | Consumer gap (`from_substrait_plan`); plan never executes |
| `COUNT` width (`bigint` vs `integer`) | 5 | q01, q04, q12, q13, q16 | IBM treats `integer` and `bigint` as distinct; DuckDB goldens label `COUNT` as `integer`, DataFusion emits `Int64` |
| Numeric epsilon | 1 | q06 | Decimal rounding vs DuckDB; Python `math.isclose(abs_tol=1e-9)` also fails (delta 1.16e-9) |
| String padding | 3 | q02, q10, q15 | Isthmus `CHAR` padding vs trimmed DuckDB goldens |
| Output alias | 2 | q11, q18 | Plan alias (`TOTAL_VALUE`, `TOTAL_QTY`) vs DuckDB names |
| Empty vs quoted-empty | 1 | q17 | Actual empty cell vs golden `"\"\""` |
| Empty result | 1 | q21 | Plan executes; 0 rows vs 1 golden row |
| Type mismatch (`string` vs `integer`) | 1 | q22 | First column typed `string` vs golden `integer` |
| Pass | 5 | q03, q05, q14, q19, q20 | Values and types match IBM goldens |
| DataFusion Substrait `VarChar` literals | 0 | — | Gone after DF #215; no remaining VarChar ERROR |

Do not treat these counts as a merge gate. Nightly CI is report-only until a
threshold is set from this baseline (and preferably from Mode B).
