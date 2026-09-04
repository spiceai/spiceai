# Substrait compliance harness

Measures [IBM/substrait-compliance](https://github.com/IBM/substrait-compliance)
TPC-H pass rate against the Spice `DataFusion` fork (Mode A) and sketches the
product path through FlightSQL `CommandStatementSubstraitPlan` (Mode B).

This is a **DataFusion consumer baseline**, not product CI. Nightly is
report-only; it does not fail the repository on a low pass rate.

## Pins

| Item | Value |
|------|--------|
| IBM tag | `v0.1.1` ([release](https://github.com/IBM/substrait-compliance/releases/tag/v0.1.1)) |
| Workspace `datafusion` / `datafusion-substrait` | `54.1.0` |
| spiceai/datafusion fork | `lukekim/substrait-varchar-literal` @ `2e6ebfd97adcf6d6d192d1d4f23d2e67fff4395c` (workspace `[patch.crates-io]`; spiceai/datafusion#215) |

The IBM `examples/datafusion-rust` tree on **`main`** pins
`datafusion` / `datafusion-substrait` **54.1**. The same example on tag
`v0.1.1` still pins DataFusion **35** and is not used. Test suites and
expected-output CSVs come from **`v0.1.1`**.

Nothing from the IBM repository is vendored. The suite is cloned at run
time. See [`NOTICE`](NOTICE) for Apache-2.0 attribution.

## Mode A baseline (this pin)

| Suite | PASS | FAIL | SKIP | ERROR | Total |
|-------|------|------|------|-------|-------|
| TPC-H SF 0.01 | 5 | 14 | 0 | 3 | 22 |

Isthmus `VarChar` literals no longer ERROR after DF #215. Remaining
ERRORs are non-Value function arguments (q07, q08, q09). Per-query
notes: [`RESULTS.md`](RESULTS.md).

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

Comparison follows the IBM TPC-H README: row/column counts, normalised
types, per-cell values with absolute epsilon `1e-9`. Column names are
compared case-insensitively (`L_RETURNFLAG` vs `l_returnflag`).

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
`workflow_dispatch` only. `continue-on-error: true`. Uploads the JSON
report as an artifact. Do not gate merge on pass rate until a threshold
is set from this baseline.

## License

Apache-2.0 (this repository). IBM suite: Apache-2.0, cloned not vendored.
