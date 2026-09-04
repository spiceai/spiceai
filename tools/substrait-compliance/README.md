# Substrait compliance harness

Nightly, report-only runner for [IBM/substrait-compliance](https://github.com/IBM/substrait-compliance) against Spice.

This crate is **not** a workspace member. It pins DataFusion / `datafusion-substrait` to the spiceai fork revision in the repo's root `Cargo.toml` (`[patch.crates-io]`) so Mode A is a fair fork signal without re-fingerprinting the main workspace.

## Pins

See [`pin.toml`](pin.toml):

| Item | Pin |
|------|-----|
| IBM/substrait-compliance | **v0.1.1** (`7fff86d04a7124123a3f2692fa2a69de0b0a1704`) |
| spiceai/datafusion | `f9a635e6b580d5fe6ed0a70975e36014ea86c476` (workspace 54.1) |

v0.1.1 is the latest tagged IBM release and includes honest SKIPPED semantics plus all 22 TPC-H expected-output CSVs.

**Verified against the IBM tree (do not assume):** `examples/datafusion-rust` on **v0.1.1** is structural (`lib.rs` empty) and pins DataFusion **35.0**. The **54.1** `datafusion` / `datafusion-substrait` pin is on IBM `main`, not the tag. Mode A follows the `main` example's consumer call (`from_substrait_plan`) and points dependencies at the spiceai fork.

## Mode A — DataFusion-consumer baseline

Requires `protoc` and the well-known protobuf includes (`protobuf-compiler` and `libprotobuf-dev` on Debian/Ubuntu).

```bash
./tools/substrait-compliance/scripts/fetch-suite.sh
cargo run --manifest-path tools/substrait-compliance/Cargo.toml --release -- \
  --mode a \
  --suite-dir .data/substrait-compliance/test-suites/tpch \
  --output substrait-compliance-report.json
```

This is **not** product CI. A low pass rate must not fail the repo. `--fail-below` exists for a future threshold once Luke/CTO set one; nightly leaves it unset.

### Mode A TPC-H baseline (this pin)

Captured in [`baseline-mode-a-tpch.json`](baseline-mode-a-tpch.json) against IBM v0.1.1 + spiceai DataFusion `f9a635e6…`:

| status | count |
|--------|------:|
| pass | 7 |
| fail | 1 |
| skip | 0 |
| error | 14 |
| total | 22 |
| **rate** | **31.8%** |

14 errors are `from_substrait_plan` `NotImplemented` (`VarChar` literals on most queries; q09 is `Function argument non-Value type not supported`). The one fail is q01 `AVG` decimal precision vs DuckDB's expected double. 7 of the 8 plans that executed matched expected output.

## Mode B — Spice FlightSQL product path (stub)

```bash
cargo run --manifest-path tools/substrait-compliance/Cargo.toml -- --print-approach
cargo run --manifest-path tools/substrait-compliance/Cargo.toml -- \
  --mode b \
  --suite-dir .data/substrait-compliance/test-suites/tpch \
  --output substrait-compliance-mode-b-stub.json
```

Preferred long-term CI. The FlightSQL handler already exists:

- `crates/runtime/src/flight/flightsql/statement_substrait_plan.rs`
- `crates/runtime/tests/flight/statement_substrait_plan.rs`

`--mode b` writes a SKIPPED stub report. A later change should start `spiced` with the IBM CSVs registered as `LINEITEM` / `ORDERS` / … and feed `CommandStatementSubstraitPlan`.

## CI

`.github/workflows/substrait_compliance.yml` is `schedule` + `workflow_dispatch` only. It is report-only (`continue-on-error` on the run step) and uploads the JSON artifact. It does not run on pull requests or the merge queue.
