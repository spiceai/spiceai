# Substrait compliance harness

Measures [IBM/substrait-compliance](https://github.com/IBM/substrait-compliance)
TPC-H pass rate against the Spice `DataFusion` fork (Mode A) and sketches the
product path through FlightSQL `CommandStatementSubstraitPlan` (Mode B).

This is a **DataFusion consumer baseline**. CI (`pull_request`,
`merge_group`, and nightly) is report-only on pass rate: it does not
fail the repository on a low pass rate. A harness or build crash still
fails the job.

## Pins

| Item | Value |
|------|--------|
| Suite | [spiceai/substrait-compliance](https://github.com/spiceai/substrait-compliance) branch `spiceai` @ `43d31411c69ef7594887c7d759037bcf8244eeed` = IBM `main` `b9b5f6a` (suite files identical to `v0.1.1`) plus the TPC-H q01 shipdate-cutoff correction, the only difference from upstream |
| Workspace `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion fork | `spiceai-54` @ `11624fb82dc5460d201d0379d269a4613e82f9c7` (workspace `[patch.crates-io]`; merged spiceai/datafusion#220, #221 and #226, includes #215) |

The IBM `examples/datafusion-rust` tree on **`main`** pins
`datafusion` / `datafusion-substrait` **54.1** and is the layout Mode A
follows. Test suites and expected-output CSVs come from the pinned
fork commit; `scripts/fetch-ibm.sh`, the CI workflow and `SUITE_REF`
in `src/main.rs` carry the same pin and move together.

Nothing from the IBM repository is vendored. The suite is cloned at run
time. See [`NOTICE`](NOTICE) for Apache-2.0 attribution.

## Mode A baseline (measured 2026-09-09 on the pins above)

| Suite | PASS | FAIL | SKIP | ERROR | Total |
|-------|------|------|------|-------|-------|
| TPC-H SF 0.01 | 22 | 0 | 0 | 0 | 22 |

Each step measured with the same command: IBM-strict compare on the first
`spiceai-54` pin **5 / 14 / 0 / 3**; value-preserving compare lifts
**16 / 3 / 0 / 3**; `extract` enum arguments in the fork consumer
(spiceai/datafusion#220) **18 / 4 / 0 / 0**; the q01 suite correction with
the golden-trim and declared-scale fixes in this harness **21 / 1 / 0 / 0**;
a subquery scan's own qualifier in the fork consumer
(spiceai/datafusion#226) **22 / 0 / 0 / 0**. Per-query notes:
[`RESULTS.md`](RESULTS.md).

## Local run (Mode A)

```bash
# From the spiceai/spiceai repository root
./tools/substrait-compliance/scripts/fetch-ibm.sh

cargo run -p spice-substrait-compliance -- \
  --mode mode-a \
  --suite tools/substrait-compliance/.ibm/test-suites/tpch \
  --out-json tools/substrait-compliance/results/mode-a-tpch.json \
  --out-csv tools/substrait-compliance/results/mode-a-tpch.csv
```

Single query: add `--query q01`.

Mode B (encodes the FlightSQL command; does not contact `spiced`). Each
mode defaults to its own report paths, `results/<mode>-tpch.{json,csv}`:

```bash
cargo run -p spice-substrait-compliance -- --mode mode-b
```

## Mode A

Registers the IBM TPC-H CSVs (pipe-delimited, no header, SF 0.01) with
schemas matching the Isthmus plans (`LINEITEM`, `i32` keys,
`decimal(15,2)`, `date`) via `TableReference::bare` so the catalog keeps
the uppercase Isthmus names (`register_csv(&str)` would lowercase them).
Each `.bin` plan is lowered with
`datafusion_substrait::logical_plan::consumer::from_substrait_plan` —
the same consumer `spiced` uses.

Comparison follows the IBM TPC-H README (row/column counts, normalised
types, per-cell values) with these harness lifts for known-fail
cosmetics — values must still match:

- `integer` / `bigint` are type-compatible (`COUNT` width)
- column names are not compared (plan alias vs `DuckDB` name; IBM Rust SDK
  also skips names)
- string / `CHAR` cells trim trailing pad only (leading spaces stay significant)
- numeric ε is absolute `1e-8` or relative `1e-14` for floats/`double`
  (`decimal` vs `DuckDB` float; IBM documents absolute `1e-9`). Printed
  fractional length is not a tolerance. One ULP at a declared decimal
  scale applies when both headers are `decimal(p,s)` with the same
  scale ≥ 2; when the engine's schema declares the actual column
  `decimal(p,s)` and the golden is `double`, the actual must be the
  golden rounded or truncated at that scale (q01 `AVG_QTY`
  `decimal(19,6)` `25.575154` vs `25.575154611454693`), and a value one
  unit off still fails. `integer`/`bigint` cells compare exactly.
  Quoted-empty `""` in a golden CSV decodes to empty/NULL; after decode
  only the empty string is NULL, whitespace is a value. Golden cells are
  trimmed trailing-only, as engine cells are: a leading space is part of
  the value (q02 `s_comment`, q10 `c_comment`). Incomplete CSV rows
  (field count ≠ header width) mismatch.

Not lifted: row-count misses. `string` ↔ numeric type labels
(q22 country codes) go through to value compare.

A test with no expected CSV is `SKIPPED`, never `PASSED`.

## Mode B (stub)

Product path for any CI we keep long-term.

1. Start `spiced` with a Spicepod that mounts the IBM CSVs as datasets
   whose names match the plans (`LINEITEM`, …).
2. Wrap plan bytes in `arrow_flight::sql::CommandStatementSubstraitPlan`
   (`mode_b::command_statement_substrait_plan`).
3. `GetFlightInfo(FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec()))`.
4. `DoGet` the ticket; compare batches with `compare.rs`.

Server: `crates/runtime/src/flight/flightsql/statement_substrait_plan.rs`.

Open work: catalog mapping from `spice.public.lineitem` onto unqualified
Isthmus names; `spiced` bring-up in this harness; auth.

## CI

`.github/workflows/substrait_compliance.yml` runs on `pull_request` and
`merge_group` (gated to harness source, scripts, DataFusion pin,
toolchain, and workflow paths so repo docs and harness Markdown skip
the job), plus nightly `schedule` and `workflow_dispatch`. Per-query
FAIL/ERROR already exit 0; a harness/build crash fails the job (no
`continue-on-error`). Uploads the JSON and CSV reports as an artifact.
Do not gate merge on pass rate until a threshold is set from this
baseline. The job is not in `REQUIRED_CHECKS`.

## License

Apache-2.0 (this repository). IBM suite: Apache-2.0, cloned not vendored.
