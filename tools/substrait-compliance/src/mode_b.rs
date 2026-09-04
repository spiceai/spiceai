/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Mode B stub: product path through Spice FlightSQL
//! `CommandStatementSubstraitPlan`.
//!
//! The FlightSQL handler already lowers plan bytes with
//! `from_substrait_plan` and runs them via `QueryBuilder` — see
//! `crates/runtime/src/flight/flightsql/statement_substrait_plan.rs` and
//! `crates/runtime/tests/flight/statement_substrait_plan.rs`. Wiring a
//! full TPC-H harness through `spiced` is the follow-up; this module
//! records the approach and emits a stub report so nightly CI can grow
//! a `--mode b` job without a runtime change.

use chrono::Utc;

use crate::error::Result;
use crate::report::{CaseResult, ComplianceReport, TestStatus};
use crate::suite::Suite;

pub fn run_stub(suite: &Suite) -> Result<ComplianceReport> {
    let started_at = Utc::now();
    let cases = suite
        .cases
        .iter()
        .map(|test| CaseResult {
            id: test.id.clone(),
            description: test.description.clone(),
            status: TestStatus::Skipped,
            execution_time_ms: 0,
            error: Some(
                "Mode B stub: FlightSQL CommandStatementSubstraitPlan harness is not wired yet"
                    .to_string(),
            ),
        })
        .collect();

    Ok(ComplianceReport::from_cases(
        "b",
        "Spice FlightSQL CommandStatementSubstraitPlan (stub)",
        "unwired",
        &suite.name,
        &suite.version,
        started_at,
        cases,
    ))
}

pub fn approach() -> &'static str {
    "\
Mode B — Spice product path (stub)
==================================

Preferred long-term CI mode. Mode A measures the DataFusion consumer;
Mode B measures what a FlightSQL client actually hits on `spiced`.

Existing surface (already landed, do not duplicate)
  - Handler: crates/runtime/src/flight/flightsql/statement_substrait_plan.rs
      decode_plan_proto → from_substrait_plan → QueryBuilder::from_plan
      → get_flight_info / do_get
  - Integration tests: crates/runtime/tests/flight/statement_substrait_plan.rs
      produce plan bytes with datafusion-substrait's `to_substrait_plan`
      and round-trip `CommandStatementSubstraitPlan` through a test
      `spiced`. That path is enough to prove the wire format; it does
      not load IBM TPC-H tables or IBM's pre-built plan binaries.

Draft ComplianceEngine (not implemented in this spike)
  struct FlightSqlComplianceEngine {
      // arrow_flight::sql::client::FlightSqlServiceClient
      // endpoint: grpc://127.0.0.1:50051
  }

  impl FlightSqlComplianceEngine {
      async fn execute_plan(&self, plan_bytes: &[u8]) {
          // 1. Wrap bytes in arrow_flight::sql::SubstraitPlan
          //    { plan: plan_bytes, version: \"0.62.0\" }
          // 2. Build CommandStatementSubstraitPlan { plan: Some(..), transaction_id: None }
          // 3. FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec())
          // 4. client.get_flight_info(descriptor)
          // 5. client.do_get(info.endpoint[0].ticket)
          // 6. Collect RecordBatches, convert to TableData, compare
      }
  }

Spicepod the engine would need
  Register the IBM SF 0.01 CSVs as datasets whose table names match the
  plans (`LINEITEM`, `ORDERS`, `CUSTOMER`, `PART`, `SUPPLIER`,
  `PARTSUPP`, `NATION`, `REGION`) — or a view layer that aliases them.
  `file` connector + CSV is enough; no acceleration required for the
  compliance run.

Why Mode B is what we keep
  Mode A can pass while Spice still rejects a plan (auth, read-only
  validator, cache-key, schema-view expansion). Mode B exercises
  `decode_plan` + `QueryBuilder` + Flight stream framing. Existing
  runtime tests cover the happy-path wire format; the missing piece is
  feeding IBM's 22 TPC-H binaries at a registered catalog.

This spike
  `--mode b` writes a report with every case SKIPPED and the stub
  message above. No `runtime` / `flight_client` dependency is added so
  the harness stays a standalone crate and does not re-fingerprint the
  workspace.
"
}
