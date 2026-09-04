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

//! Mode B: product path through Spice `FlightSQL` `CommandStatementSubstraitPlan`.
//!
//! Long-term CI should use this path, not Mode A. This module wires the
//! `FlightSQL` command the runtime already accepts and leaves the live
//! `spiced` bring-up as follow-up work.
//!
//! Server handlers:
//! `crates/runtime/src/flight/flightsql/statement_substrait_plan.rs`
//! (`get_flight_info` / `do_get` → `from_substrait_plan` → `QueryBuilder`).
//!
//! Remaining work before this is a real engine:
//! - Start `spiced` with a Spicepod that mounts the IBM TPC-H CSVs as
//!   datasets named so Isthmus plans resolve (`LINEITEM`, `ORDERS`, …).
//! - Connect a `FlightSQL` client (see `crates/flight_client`) to the runtime
//!   Flight endpoint (default `50051`).
//! - `GetFlightInfo(FlightDescriptor::new_cmd(command_bytes))` then `DoGet`
//!   the ticket; reuse [`crate::compare`] on the collected batches.
//! - Map Spice catalog names (`spice.public.lineitem`) onto the unqualified
//!   names the IBM plans use.

use arrow_flight::sql::{CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan};
use bytes::Bytes;
use prost::Message;

use crate::error::{self, Result};
use crate::report::{CaseResult, TestStatus};
use crate::suite::LoadedCase;

pub const ENGINE_NAME: &str = "Spice FlightSQL";
pub const ENGINE_VERSION: &str = "stub";

/// Substrait version string the existing `FlightSQL` integration tests send.
pub const SUBSTRAIT_VERSION: &str = "0.62.0";

/// Build the exact `FlightSQL` command `spiced` decodes in
/// `statement_substrait_plan`.
#[must_use]
pub fn command_statement_substrait_plan(plan_bytes: &[u8]) -> CommandStatementSubstraitPlan {
    CommandStatementSubstraitPlan {
        plan: Some(SubstraitPlan {
            plan: Bytes::copy_from_slice(plan_bytes),
            version: SUBSTRAIT_VERSION.to_string(),
        }),
        transaction_id: None,
    }
}

/// Protobuf payload for `FlightDescriptor::new_cmd(...)`.
#[must_use]
pub fn command_bytes(plan_bytes: &[u8]) -> Vec<u8> {
    command_statement_substrait_plan(plan_bytes)
        .as_any()
        .encode_to_vec()
}

/// Product-path engine. Execute is intentionally unimplemented until a
/// `spiced` fixture owns the TPC-H catalog.
pub struct FlightSqlComplianceEngine {
    pub endpoint: String,
}

impl FlightSqlComplianceEngine {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    pub fn run_case(&self, case: &LoadedCase) -> Result<CaseResult> {
        // Prove the command encodes; do not pretend a result was verified.
        let _cmd = command_bytes(&case.plan_bytes);
        error::ModeBNotImplementedSnafu {
            detail: format!(
                "would send CommandStatementSubstraitPlan for '{}' to {}",
                case.id, self.endpoint
            ),
        }
        .fail()
    }

    pub fn stub_results(cases: &[LoadedCase]) -> Vec<CaseResult> {
        cases
            .iter()
            .map(|case| CaseResult {
                test_id: case.id.clone(),
                description: case.description.clone(),
                status: TestStatus::Skipped,
                execution_time_ms: 0,
                error_message: Some(format!(
                    "Mode B stub: CommandStatementSubstraitPlan encodes ({} plan bytes) but is not sent to spiced yet",
                    case.plan_bytes.len()
                )),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_bytes_are_a_non_empty_any_payload() {
        let bytes = command_bytes(&[0x0a, 0x00]);
        assert!(!bytes.is_empty(), "FlightSQL Any payload must not be empty");
        let cmd = command_statement_substrait_plan(&[0x0a, 0x00]);
        let plan = cmd.plan.expect("plan must be present");
        assert_eq!(plan.plan.as_ref(), &[0x0a, 0x00]);
        assert_eq!(plan.version, SUBSTRAIT_VERSION);
    }

    #[test]
    fn run_case_is_explicitly_unimplemented() {
        let engine = FlightSqlComplianceEngine::new("http://127.0.0.1:50051");
        let case = LoadedCase {
            id: "q01".to_string(),
            description: "fixture".to_string(),
            plan_path: std::path::PathBuf::from("plans/q01.bin"),
            plan_bytes: vec![0x0a, 0x00],
            input_tables: Vec::new(),
            expected: None,
        };
        let err = engine.run_case(&case).expect_err("stub must not execute");
        assert!(
            err.to_string().contains("CommandStatementSubstraitPlan"),
            "{err}"
        );
    }
}
