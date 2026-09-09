# Mode A TPC-H results

Captured on this box against the pinned suite and the workspace `DataFusion`
fork (`Cargo.toml` `[patch.crates-io]`). Re-run the command below to regenerate
`results/mode-a-tpch.json` (gitignored; CI uploads it as an artifact).

## Pins

| Item | Value |
|------|--------|
| Suite | [spiceai/substrait-compliance](https://github.com/spiceai/substrait-compliance) branch `spiceai` @ `5ccb99672853bd768019101ebb6a7d1aa4c8f547` = IBM `main` `b9b5f6a` (suite files identical to `v0.1.1`) plus the corrections in its `SPICEAI.md` |
| `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion rev | `45b2f1091dffa98f92d87bdeaf50cf3905f73b0f` (spiceai/datafusion#226 head on `spiceai-54` `ce010574…`, which carries #215, #220 and #221) |
| Suite | TPC-H SF 0.01 (22 queries) |
| Oracle | DuckDB 1.2.0 (IBM goldens) |
| Run | 2026-09-09T00:55:38Z → 2026-09-09T00:55:43Z |

## Counts

| PASS | FAIL | SKIP | ERROR | Total |
|------|------|------|-------|-------|
| 22 | 0 | 0 | 0 | 22 |

Command and headline output:

```text
cargo run -p spice-substrait-compliance -- \
  --mode mode-a \
  --suite tools/substrait-compliance/.ibm/test-suites/tpch

Suite: spiceai/substrait-compliance@5ccb99672853bd768019101ebb6a7d1aa4c8f547
DataFusion fork rev: 45b2f1091dffa98f92d87bdeaf50cf3905f73b0f
  PASS  q01
  PASS  q02
  PASS  q03
  PASS  q04
  PASS  q05
  PASS  q06
  PASS  q07
  PASS  q08
  PASS  q09
  PASS  q10
  PASS  q11
  PASS  q12
  PASS  q13
  PASS  q14
  PASS  q15
  PASS  q16
  PASS  q17
  PASS  q18
  PASS  q19
  PASS  q20
  PASS  q21
  PASS  q22

22/0/0  pass/fail/skip+error  total=22  pass_rate=100.0%
  passed=22 failed=0 skipped=0 errored=0
```

## How the count moved

Every row is the same command on this box; the causes were located, not
inferred.

| Step | PASS / FAIL / SKIP / ERROR | What changed |
|------|----------------------------|--------------|
| IBM-strict compare, first `spiceai-54` pin (`f9a635e6…`) | 1 / 7 / 0 / 14 | Isthmus `VarChar` literals were not consumed |
| spiceai/datafusion#215 (`VarChar` literals) | 5 / 14 / 0 / 3 | 13 queries execute |
| Value-preserving compare lifts | 16 / 3 / 0 / 3 | `COUNT` width, column names, trailing `CHAR` pad, numeric ε, quoted-empty `""` |
| Trailing-only trim on the engine side, string↔numeric labels | 15 / 4 / 0 / 3 | q22 PASS; q02 and q10 FAIL because the golden decoder still trimmed both ends |
| spiceai/datafusion#220 (`extract` enum arguments) | 18 / 4 / 0 / 0 | q07, q08, q09 ERROR → PASS |
| Suite correction (q01 cutoff) + golden trailing-only trim + engine-declared decimal scale | 21 / 1 / 0 / 0 | q01, q02, q10 PASS |
| spiceai/datafusion#226 (subquery scan qualifier) | 22 / 0 / 0 / 0 | q21 PASS |

## What each remaining failure turned out to be

| Query | Symptom | Cause (measured) | Fix |
|-------|---------|------------------|-----|
| q01 | `count_order` 29162 vs 29181 in the `N\|O` group, every sum and average of that group off; first cell reported `AVG_QTY` `25.575154` vs `25.575154611454693` | `plans/q01.bin` filtered `l_shipdate <= 1998-09-01` (Substrait date 10470) while the golden's SQL cutoff is 1998-09-02; `data/lineitem.csv` has exactly 19 rows on 1998-09-02, all `N\|O`. The averages then differed only by scale: the plan declares them `decimal(15,2)`, `DataFusion` returns `decimal(19,6)`, the golden prints the unrounded `double` | Suite: the `spiceai` branch carries date 10471 in both plan files. Harness: a `decimal(p,s)` engine column keeps its declared scale in the typed header and a `double` golden must be the actual rounded or truncated at that scale |
| q02, q10 | `' foxes boost…'` vs `'foxes boost…'`; `' are carefully…'` vs `'are carefully…'` | `decode_csv_cell` trimmed both ends of a golden cell while the engine side trims trailing pad only; `data/supplier.csv:86`, `data/customer.csv:422` and the goldens all carry the leading space | Harness: goldens decode trailing-only |
| q07, q08, q09 | `Function argument non-Value type not supported` | `extract:req_date` with a `YEAR` enum argument; the consumer accepted `ArgType::Value` only (upstream `main` too) | spiceai/datafusion#220: enum arguments lower to literals; `extract` is translated component by component to `date_part` cast to the declared type, with `NotImplemented` for components `date_part` defines differently |
| q21 | `row count 0 != 1` | Both the outer scan and the EXISTS / NOT EXISTS subquery scans were qualified `LINEITEM`; decorrelation resolved `LINEITEM.L_ORDERKEY = outer_ref(LINEITEM.L_ORDERKEY)` to the inner scan alone, the semi/anti joins lost their condition and `L_SUPPKEY != L_SUPPKEY` stayed behind. The same query as SQL (aliases `L1`/`L2`/`L3`) returned the golden `Supplier#000000074\|9` | spiceai/datafusion#226: a subquery's scan of a table an enclosing scope reads gets its own qualifier (`LINEITEM_1`) |

## Compare rules

Harness compare in `src/compare.rs`:

- `integer` / `bigint` are type-compatible (`COUNT` width)
- column names are not compared (plan alias vs DuckDB; IBM Rust SDK skips names)
- string cells trim trailing `CHAR` pad only, on both sides; a leading space is significant
- numerics: `integer`/`bigint` exactly; floats/`double` absolute ε `1e-8` or relative `1e-14`; printed fractional length is not a tolerance; one ULP when both headers share a declared decimal scale ≥ 2; when the engine's schema declares the actual column `decimal(p,s)` and the golden is `double`, the actual must be the golden rounded or truncated at that scale, and one unit off still fails
- quoted-empty `""` is NULL/empty; after decode only the empty string is NULL, whitespace is a value
- a zero-byte, headerless, or malformed first line is an oracle-load error, not an empty PASS; a legitimate empty result is a typed header with zero data rows; incomplete rows mismatch

IBM README is absolute ε `1e-9` and distinct `integer`/`bigint`. None of these
rules ignores a row-count miss or a value that is not the engine's rendering
of the golden.

## Per-query

| Query | Status | Notes |
|-------|--------|-------|
| q01 | PASS | suite cutoff corrected; averages at the engine's declared scale |
| q02 | PASS | trailing-only golden trim (leading space kept) |
| q03 | PASS | unchanged |
| q04 | PASS | unchanged |
| q05 | PASS | unchanged |
| q06 | PASS | unchanged |
| q07 | PASS | `extract` enum argument (fork #220) |
| q08 | PASS | `extract` enum argument (fork #220) |
| q09 | PASS | `extract` enum argument (fork #220) |
| q10 | PASS | trailing-only golden trim (leading space kept) |
| q11 | PASS | unchanged |
| q12 | PASS | unchanged |
| q13 | PASS | unchanged |
| q14 | PASS | unchanged |
| q15 | PASS | unchanged |
| q16 | PASS | unchanged |
| q17 | PASS | quoted-empty `""` decode |
| q18 | PASS | unchanged |
| q19 | PASS | unchanged |
| q20 | PASS | unchanged |
| q21 | PASS | subquery scan qualifier (fork #226) |
| q22 | PASS | string↔numeric type labels reach value compare |

Do not treat these counts as a merge gate. Nightly CI is report-only until a
threshold is set from this baseline (and preferably from Mode B).
