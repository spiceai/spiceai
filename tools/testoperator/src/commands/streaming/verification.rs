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
//! Uses the existing test-framework validation logic to verify query results
//! against expected TPCH answers at SF=1.

use std::time::Duration;

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use test_framework::anyhow::{self, Context, Result};
use test_framework::queries::validation::{
    QueryValidationFailReason, QueryValidationResult, validate_tpch_query,
};
use test_framework::queries::{Query, QueryOverrides, get_tpch_test_queries};
use test_framework::spiced::SpicedInstance;

/// Result of query verification.
#[derive(Debug)]
pub struct VerificationResult {
    pub query_name: String,
    pub status: VerificationStatus,
    pub row_count: usize,
    pub duration_ms: u64,
}

/// Status of a single query verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationStatus {
    Pass,
    Failed(String),
    Skipped(String),
}

/// Summary of verification results.
#[derive(Debug)]
pub struct VerificationReport {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub results: Vec<VerificationResult>,
}

impl VerificationReport {
    /// Check if all non-skipped queries passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.failed == 0
    }

    /// Print a summary of the verification report.
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("TPCH Query Verification Results");
        println!("{}", "=".repeat(60));
        println!(
            "Total: {} | Passed: {} | Failed: {} | Skipped: {}",
            self.total, self.passed, self.failed, self.skipped
        );
        println!("{}", "-".repeat(60));

        for result in &self.results {
            let status_str = match &result.status {
                VerificationStatus::Pass => "PASS",
                VerificationStatus::Failed(_) => "FAIL",
                VerificationStatus::Skipped(_) => "SKIP",
            };
            println!(
                "{:<20} {:>6} {:>8} rows {:>8} ms",
                result.query_name, status_str, result.row_count, result.duration_ms
            );

            if let VerificationStatus::Failed(reason) = &result.status {
                println!("  Error: {reason}");
            }
            if let VerificationStatus::Skipped(reason) = &result.status {
                println!("  Reason: {reason}");
            }
        }

        println!("{}", "=".repeat(60));

        if self.all_passed() {
            println!("All queries passed verification!");
        } else {
            println!("WARNING: {} queries failed verification!", self.failed);
        }
    }
}

/// Run TPCH query verification against the spiced instance.
///
/// Uses the test-framework's `validate_tpch_query` function to compare
/// actual results against expected TPCH answers at SF=1.
pub async fn verify_tpch_queries(spiced: &SpicedInstance) -> Result<VerificationReport> {
    // Get TPCH queries with DynamoDB overrides (removes unsupported queries like q6)
    let queries = get_tpch_test_queries(Some(QueryOverrides::DynamoDB));

    // Filter to only the main 22 TPCH queries (q1-q22), excluding simple queries
    let queries: Vec<Query> = queries
        .into_iter()
        .filter(|q| q.name.starts_with("tpch_q") && !q.name.contains("simple"))
        .collect();

    let mut results = Vec::with_capacity(queries.len());
    let spice_client = spiced.spice_client(None, false).await?;

    for query in &queries {
        let result = verify_single_query(&spice_client, query).await;
        results.push(result);
    }

    let passed = results
        .iter()
        .filter(|r| matches!(r.status, VerificationStatus::Pass))
        .count();
    let failed = results
        .iter()
        .filter(|r| matches!(r.status, VerificationStatus::Failed(_)))
        .count();
    let skipped = results
        .iter()
        .filter(|r| matches!(r.status, VerificationStatus::Skipped(_)))
        .count();

    Ok(VerificationReport {
        total: results.len(),
        passed,
        failed,
        skipped,
        results,
    })
}

/// Verify a single query using test-framework's validation logic.
#[expect(clippy::cast_possible_truncation)]
async fn verify_single_query(client: &spiceai::Client, query: &Query) -> VerificationResult {
    let start = std::time::Instant::now();
    let sql = query.to_sql_with_inlined_params();

    match execute_query_with_timeout(client, &sql, Duration::from_secs(60)).await {
        Ok(batches) => {
            let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
            let duration_ms = start.elapsed().as_millis() as u64;

            // Use test-framework's validation logic
            match validate_tpch_query(query, &batches) {
                Ok(QueryValidationResult::Pass) => VerificationResult {
                    query_name: query.name.to_string(),
                    status: VerificationStatus::Pass,
                    row_count,
                    duration_ms,
                },
                Ok(QueryValidationResult::Fail(reason)) => {
                    let error_msg = format_validation_failure(&reason);
                    VerificationResult {
                        query_name: query.name.to_string(),
                        status: VerificationStatus::Failed(error_msg),
                        row_count,
                        duration_ms,
                    }
                }
                Err(e) => VerificationResult {
                    query_name: query.name.to_string(),
                    status: VerificationStatus::Failed(format!("Validation error: {e}")),
                    row_count,
                    duration_ms,
                },
            }
        }
        Err(e) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            let error_msg = e.to_string();

            // Check if this is a known limitation
            if error_msg.contains("not supported") || error_msg.contains("Unsupported") {
                VerificationResult {
                    query_name: query.name.to_string(),
                    status: VerificationStatus::Skipped(error_msg),
                    row_count: 0,
                    duration_ms,
                }
            } else {
                VerificationResult {
                    query_name: query.name.to_string(),
                    status: VerificationStatus::Failed(error_msg),
                    row_count: 0,
                    duration_ms,
                }
            }
        }
    }
}

/// Format a validation failure reason into a human-readable string.
fn format_validation_failure(reason: &QueryValidationFailReason) -> String {
    match reason {
        QueryValidationFailReason::NoExpectedAnswer => "No expected answer available".to_string(),
        QueryValidationFailReason::NoAnswer => "Query returned no results".to_string(),
        QueryValidationFailReason::SchemaMismatch => "Schema mismatch".to_string(),
        QueryValidationFailReason::RowCountMismatch { expected, actual } => {
            format!("Row count mismatch: expected {expected}, got {actual}")
        }
        QueryValidationFailReason::DataMismatch {
            column,
            row_number,
            expected,
            actual,
        } => {
            format!(
                "Data mismatch in column '{column}' row {row_number}: expected {expected}, got {actual}"
            )
        }
        QueryValidationFailReason::ColumnLengthMismatch {
            column_name,
            left_len,
            right_len,
        } => {
            format!("Column length mismatch in '{column_name}': {left_len} vs {right_len}")
        }
    }
}

/// Execute a query with a timeout.
async fn execute_query_with_timeout(
    client: &spiceai::Client,
    sql: &str,
    timeout: Duration,
) -> Result<Vec<RecordBatch>> {
    tokio::time::timeout(timeout, async {
        let stream = client.query(sql).await.context("Failed to execute query")?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .context("Failed to collect query results")?;
        Ok(batches)
    })
    .await
    .map_err(|_| anyhow::anyhow!("Query execution timed out after {timeout:?}"))?
}
