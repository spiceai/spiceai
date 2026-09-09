# Substrait compliance harness

Measures [IBM/substrait-compliance](https://github.com/IBM/substrait-compliance)
TPC-H pass rate against the Spice `DataFusion` fork (Mode A) and sketches the
product path through FlightSQL `CommandStatementSubstraitPlan` (Mode B).

This is a **DataFusion consumer baseline**, not product CI. Nightly is
report-only; it does not fail the repository on a low pass rate.

## Pins

| Item | Value |
|------|--------|
| Suite | [spiceai/substrait-compliance](https://github.com/spiceai/substrait-compliance) branch `spiceai` @ `5ccb99672853bd768019101ebb6a7d1aa4c8f547` = IBM `main` `b9b5f6a` (suite files identical to `v0.1.1`) plus the corrections in its `SPICEAI.md` (TPC-H q01 shipdate cutoff) |
| Workspace `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion fork | `spiceai-54` @ `ce0105748e153bcfe4ae182061ad875694ab4a1c` (workspace `[patch.crates-io]`; merged spiceai/datafusion#220 and #221, includes #215) |

The IBM `examples/datafusion-rust` tree on **`main`** pins
`datafusion` / `datafusion-substrait` **54.1** and is the layout Mode A
follows. Test suites and expected-output CSVs come from the pinned
fork commit; `scripts/fetch-ibm.sh`, the nightly workflow and `SUITE_REF`
in `src/main.rs` carry the same pin and move together.

Nothing from the IBM repository is vendored. The suite is cloned at run
time. See [`NOTICE`](NOTICE) for Apache-2.0 attribution.

## Mode A baseline (measured 2026-09-05 on `6006901cb602d845ee1441269d6eaa142c2580a6`)

| Suite | PASS | FAIL | SKIP | ERROR | Total |
|-------|------|------|------|-------|-------|
| TPC-H SF 0.01 | 16 | 3 | 0 | 3 | 22 |

Before compare lifts (same pin): **PASS 5 | FAIL 14 | SKIP 0 | ERROR 3**.
After value-preserving compare lifts: **PASS 15 | FAIL 4**. Quoted-empty
`""` decode then flips q17 (measured) → **PASS 16 | FAIL 3**. Isthmus `VarChar`
literals no longer ERROR after DF #215. Remaining ERRORs are non-Value
function arguments (q07, q08, q09). Per-query notes and known-fail
flips: [`RESULTS.md`](RESULTS.md).

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

Mode B (encodes the FlightSQL command; does not contact `spiced`):

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
  scale applies only when both headers are `decimal(p,s)` with the same
  scale ≥ 2. `integer`/`bigint` cells compare exactly. Quoted-empty `""`
  in a golden CSV decodes to empty/NULL; after decode only the empty
  string is NULL. Incomplete CSV rows (field count ≠ header width)
  mismatch.

Not lifted: row-count misses (q21). `string` ↔ numeric type labels
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

## Nightly CI

`.github/workflows/substrait_compliance.yml` — `schedule` +
`workflow_dispatch` only. Per-query FAIL/ERROR already exit 0; a
harness/build crash fails the job (no `continue-on-error`). Uploads
the JSON report as an artifact. Do not gate merge on pass rate until
a threshold is set from this baseline.

## License

Apache-2.0 (this repository). IBM suite: Apache-2.0, cloned not vendored.
