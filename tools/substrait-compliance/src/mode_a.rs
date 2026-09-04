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

//! Mode A: DataFusion-consumer baseline.
//!
//! Feeds IBM TPC-H Substrait plan bytes through `datafusion-substrait`'s
//! `from_substrait_plan` on the spiceai DataFusion fork. This is the same
//! consumer Spice's FlightSQL `CommandStatementSubstraitPlan` path uses, but
//! it does not go through `spiced` / Flight.

use std::time::Instant;

use chrono::Utc;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;

use crate::compare::{batches_to_table_data, compare_results};
use crate::error::Result;
use crate::report::{CaseResult, ComplianceReport, TestStatus};
use crate::suite::Suite;
use crate::tpch::register_tpch_tables;

pub async fn run(suite: &Suite, query_filter: Option<&str>) -> Result<ComplianceReport> {
    let started_at = Utc::now();
    let ctx = SessionContext::new();
    register_tpch_tables(&ctx, &suite.data_dir).await?;

    let mut cases = Vec::with_capacity(suite.cases.len());
    for test in &suite.cases {
        if let Some(filter) = query_filter
            && test.id != filter
        {
            continue;
        }
        cases.push(run_one(&ctx, test).await);
    }

    Ok(ComplianceReport::from_cases(
        "a",
        "DataFusion (spiceai fork consumer)",
        "54.1",
        &suite.name,
        &suite.version,
        started_at,
        cases,
    ))
}

async fn run_one(ctx: &SessionContext, test: &crate::suite::TestCase) -> CaseResult {
    let start = Instant::now();
    let Some(expected) = test.expected_output.as_ref() else {
        return CaseResult {
            id: test.id.clone(),
            description: test.description.clone(),
            status: TestStatus::Skipped,
            execution_time_ms: start.elapsed().as_millis() as u64,
            error: Some("No expected output — cannot verify correctness".to_string()),
        };
    };

    match execute_plan(ctx, &test.plan_bytes).await {
        Ok(actual) => match compare_results(&actual, expected) {
            Ok(()) => CaseResult {
                id: test.id.clone(),
                description: test.description.clone(),
                status: TestStatus::Passed,
                execution_time_ms: start.elapsed().as_millis() as u64,
                error: None,
            },
            Err(msg) => CaseResult {
                id: test.id.clone(),
                description: test.description.clone(),
                status: TestStatus::Failed,
                execution_time_ms: start.elapsed().as_millis() as u64,
                error: Some(msg),
            },
        },
        Err(msg) => CaseResult {
            id: test.id.clone(),
            description: test.description.clone(),
            status: TestStatus::Error,
            execution_time_ms: start.elapsed().as_millis() as u64,
            error: Some(msg),
        },
    }
}

async fn execute_plan(
    ctx: &SessionContext,
    plan_bytes: &[u8],
) -> std::result::Result<crate::compare::TableData, String> {
    let proto = Plan::decode(plan_bytes).map_err(|e| format!("decode Substrait plan: {e}"))?;
    let state = ctx.state();
    let logical = from_substrait_plan(&state, &proto)
        .await
        .map_err(|e| format!("from_substrait_plan: {e}"))?;
    let df = ctx
        .execute_logical_plan(logical)
        .await
        .map_err(|e| format!("execute_logical_plan: {e}"))?;
    let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
    batches_to_table_data(&batches)
}

/// Mode A engine metadata for `--print-approach`.
pub fn approach() -> &'static str {
    "\
Mode A — DataFusion-consumer baseline
=====================================

What it measures
  Pass rate of IBM/substrait-compliance TPC-H plans when consumed by
  `datafusion-substrait::from_substrait_plan` on the spiceai DataFusion
  fork (workspace `[patch.crates-io]` rev). This is a fork-signal
  baseline, not product CI.

How it runs
  1. Load IBM TPC-H SF 0.01 CSVs as named tables (`LINEITEM`, …).
  2. Decode each `plans/qNN.bin` as `substrait::proto::Plan`.
  3. Lower via `from_substrait_plan` (same call Spice FlightSQL uses).
  4. Execute the logical plan in-process and compare to `expected/qNN.csv`.

Verified hunches
  - IBM tag v0.1.1 exists and is the pin used here.
  - `examples/datafusion-rust` on v0.1.1 is structural and pins
    DataFusion 35.0 (empty lib). The 54.1 pin lives on IBM `main`, not
    the tagged release. This harness follows the `main` example's
    consumer wiring and points deps at the spiceai fork.
  - Workspace major is DataFusion 54.1 — matches IBM `main`'s example.

Not product CI
  Failures here do not fail the spiceai repo. A low pass rate is a
  DataFusion-substrait consumer gap, not a Spice regression.
"
}
