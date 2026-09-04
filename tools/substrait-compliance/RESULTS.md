# Mode A TPC-H baseline

Captured locally against this workspace's DataFusion fork. Re-run the
command below to regenerate `results/mode-a-tpch.json` (gitignored; CI
uploads it as an artifact).

## Pins

| Item | Value |
|------|--------|
| IBM tag | `v0.1.1` |
| `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion rev | `2e6ebfd97adcf6d6d192d1d4f23d2e67fff4395c` (`lukekim/substrait-varchar-literal` / spiceai/datafusion#215) |
| Suite | TPC-H SF 0.01 (22 queries) |
| Oracle | DuckDB 1.2.0 (IBM goldens) |

## Pass rate

| Suite | Pass | Fail | Skip | Error | Total | Pass rate |
|-------|------|------|------|-------|-------|-----------|
| TPC-H Mode A | 1 | 7 | 0 | 14 | 22 | **4.5%** |

Pass rate is `passed / total` (IBM semantics; skips stay in the
denominator). This run had no skips.

Command and headline output:

```text
cargo run -p spice-substrait-compliance -- \
  --mode mode-a \
  --suite tools/substrait-compliance/.ibm/test-suites/tpch \
  --out-json tools/substrait-compliance/results/mode-a-tpch.json \
  --out-csv tools/substrait-compliance/results/mode-a-tpch.csv

1/7/14  pass/fail/skip+error  total=22  pass_rate=4.5%
  passed=1 failed=7 skipped=0 errored=14
```

## Per-query

| Query | Status | Notes |
|-------|--------|-------|
| q01 | FAIL | column 9 type `bigint` != `integer` (`COUNT` is `Int64` in DataFusion) |
| q02 | ERROR | `from_substrait_plan`: unsupported `VarChar` literal (`EUROPE`) |
| q03 | ERROR | unsupported `VarChar` literal (`BUILDING`) |
| q04 | FAIL | column 1 type `bigint` != `integer` |
| q05 | ERROR | unsupported `VarChar` literal (`ASIA`) |
| q06 | FAIL | cell `1193053.2253` vs `1193053.225299999` (abs delta ≈ 1.16e-9, over IBM ε = 1e-9) |
| q07 | ERROR | unsupported `VarChar` literal (`FRANCE`) |
| q08 | ERROR | unsupported `VarChar` literal (`AMERICA`) |
| q09 | ERROR | `Function argument non-Value type not supported` |
| q10 | FAIL | leading space on a `CHAR`/`varchar` cell |
| q11 | ERROR | unsupported `VarChar` literal (`GERMANY`) |
| q12 | ERROR | unsupported `VarChar` literal (`MAIL`) |
| q13 | FAIL | column 0 type `bigint` != `integer` |
| q14 | **PASS** | |
| q15 | FAIL | trailing space on a `CHAR`/`varchar` cell |
| q16 | ERROR | unsupported `VarChar` literal (`Brand#45`) |
| q17 | ERROR | unsupported `VarChar` literal (`Brand#23`) |
| q18 | FAIL | column name `TOTAL_QTY` != `sum(l_quantity)` |
| q19 | ERROR | unsupported `VarChar` literal (`Brand#12`) |
| q20 | ERROR | unsupported `VarChar` literal (`CANADA`) |
| q21 | ERROR | unsupported `VarChar` literal (`SAUDI ARABIA`) |
| q22 | ERROR | unsupported `VarChar` literal (`13`) |

## Failure groups

| Group | Count | Queries | Meaning |
|-------|-------|---------|---------|
| DataFusion Substrait `VarChar` literals | 13 | q02, q03, q05, q07, q08, q11, q12, q16, q17, q19–q22 | Consumer cannot lower Isthmus `VarChar` literals; plan never executes |
| Non-value function argument | 1 | q09 | Consumer gap (`from_substrait_plan`) |
| `COUNT` width (`bigint` vs `integer`) | 3 | q01, q04, q13 | IBM treats `integer` and `bigint` as distinct; DuckDB goldens label `COUNT` as `integer`, DataFusion emits `Int64` |
| Numeric epsilon | 1 | q06 | Decimal rounding vs DuckDB; Python `math.isclose(abs_tol=1e-9)` also fails (delta 1.16e-9) |
| String padding | 2 | q10, q15 | Isthmus `CHAR` padding vs trimmed DuckDB goldens |
| Output alias | 1 | q18 | Plan alias `TOTAL_QTY` vs DuckDB `sum(l_quantity)` |
| Pass | 1 | q14 | Values and types match IBM goldens |

## Expected after DF #215 (VarChar) — not yet re-run on this box

DF consumer now lowers `LiteralType::VarChar` → `Utf8`. The **13** Isthmus VarChar
rows below should leave **ERROR** (become **PASS** or **FAIL** once the plan executes).
q09 stays **ERROR** (non-value function argument). Baseline table above is pre-fix.

| Query | Was | Expected after #215 |
|-------|-----|---------------------|
| q02, q03, q05, q07, q08, q11, q12, q16, q17, q19, q20, q21, q22 | ERROR (VarChar) | PASS or FAIL (not ERROR) |
| q09 | ERROR (non-value fn arg) | ERROR (unchanged) |
| q01, q04, q06, q10, q13, q15, q18 | FAIL | FAIL (unchanged taxonomy) |
| q14 | PASS | PASS |

Headline expectation if only VarChar moves: **ERROR 14 → 1**; PASS+FAIL redistribute among the 13 former VarChar ERRORs. Re-run Mode A after `cargo update` resolves the new DF rev and IBM suite is present; then replace this section with measured PASS|FAIL|SKIP|ERROR.

**Mode A re-run blocker on executor box:** full workspace compile + IBM `test-suites/tpch` + DuckDB goldens not exercised here. Pin bumped to DF #215 head for CI/local re-run on #13879.

Do not treat 4.5% as a merge gate. Nightly CI is report-only until a
threshold is set from this baseline (and preferably from Mode B).
