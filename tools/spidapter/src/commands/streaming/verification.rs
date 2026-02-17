/*
Copyright 2026 The Spice.ai OSS Authors

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

//! TPCH query verification for streaming benchmarks.
//!
//! Uses `SpiceTest` from test-framework to run TPCH queries with validation,
//! collecting per-query metrics (timing, status, row counts).

use std::collections::BTreeMap;
use std::sync::Arc;

use test_framework::TestType;
use test_framework::anyhow::Result;
use test_framework::metrics::{
    DatasetMetrics, MetricCollector, NoExtendedMetrics, QueryMetrics, QueryStatus,
};
use test_framework::queries::{Query, QueryOverrides, QuerySet, get_tpch_test_queries};
use test_framework::spiced::SpicedInstance;
use test_framework::spicetest::SpiceTest;
use test_framework::spicetest::datasets::{EndCondition, NotStarted};

/// Result of running TPCH verification using `SpiceTest`.
pub struct VerificationResult {
    /// The `SpicedInstance` (returned for continued use)
    pub spiced_instance: SpicedInstance,
    /// Per-query metrics collected during verification
    pub metrics: QueryMetrics<DatasetMetrics, NoExtendedMetrics>,
    /// Row counts per query
    pub row_counts: BTreeMap<Arc<str>, usize>,
    /// Whether all queries passed
    pub all_passed: bool,
}

/// Run TPCH query verification against the spiced instance using `SpiceTest`.
///
/// This uses the same `SpiceTest` infrastructure as the bench command,
/// running queries with validation and collecting per-query metrics.
///
/// # Arguments
/// * `spiced_instance` - Takes ownership of the `SpicedInstance`
/// * `config_name` - Configuration name (e.g. "tpch-duckdb") used to differentiate snapshots
/// * `iterations` - Number of times to run each query (default: 1 for correctness, higher for timing stats)
/// * `scale_factor` - TPCH scale factor for validation
///
/// # Returns
/// * `VerificationResult` containing the `SpicedInstance` (for continued use) and metrics
pub async fn run_verification(
    spiced_instance: SpicedInstance,
    config_name: &str,
    iterations: usize,
    scale_factor: f64,
) -> Result<VerificationResult> {
    println!("\n{}", "=".repeat(60));
    println!("Starting TPCH Query Verification (SpiceTest)");
    println!("{}", "=".repeat(60));

    // Get TPCH queries with DynamoDB overrides
    let queries = get_tpch_test_queries(Some(QueryOverrides::DynamoDB));

    // Filter to only the main TPCH queries (q1-q22), excluding simple queries
    let queries: Vec<Query> = queries
        .into_iter()
        .filter(|q| q.name.starts_with("tpch_q") && !q.name.contains("simple"))
        .collect();

    println!(
        "Running {} TPCH queries x {} iterations...\n",
        queries.len(),
        iterations
    );

    // Create SpiceTest state
    let state = NotStarted::new()
        .with_parallel_count(1)
        .with_query_set(queries)
        .with_end_condition(EndCondition::QuerySetCompleted(iterations))
        .with_validate(true)
        .with_scale_factor(scale_factor)
        .with_query_set_type(QuerySet::Tpch)
        .with_query_overrides(Some(QueryOverrides::DynamoDB));

    // Create and run SpiceTest (name differentiates snapshots per config)
    let test = SpiceTest::new(format!("streaming_{config_name}"), state)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(false)
        .with_explain_plan_snapshot()
        .start()
        .await?
        .wait()
        .await?;

    // Collect metrics
    let row_counts = test.validate_returned_row_counts()?;
    let all_passed = test.succeeded();
    let metrics: QueryMetrics<DatasetMetrics, NoExtendedMetrics> =
        test.collect(TestType::Streaming)?;

    // Print summary
    println!("\n{}", "-".repeat(60));
    let passed_count = metrics
        .metrics
        .iter()
        .filter(|m| matches!(m.query_status, QueryStatus::Passed))
        .count();
    let failed_count = metrics.metrics.len() - passed_count;

    println!("Verification complete: {passed_count} passed, {failed_count} failed");

    if !all_passed {
        println!("WARNING: Some queries failed verification!");
        for metric in &metrics.metrics {
            if let QueryStatus::Failed(reason) = &metric.query_status {
                let reason_str = reason.as_ref().map_or(
                    "unknown error".to_string(),
                    std::string::ToString::to_string,
                );
                println!("  {}: {}", metric.query_name, reason_str);
            }
        }
    }
    println!("{}", "=".repeat(60));

    // Get back the SpicedInstance
    let spiced_instance = test.end()?;

    Ok(VerificationResult {
        spiced_instance,
        metrics,
        row_counts,
        all_passed,
    })
}
