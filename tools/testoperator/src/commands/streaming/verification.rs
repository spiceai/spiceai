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
//!
//! Expected results are generated at runtime using `DuckDB`'s `dbgen` extension
//! at the target scale factor, so verification works for any SF (not just SF=1).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use duckdb::Connection;
use test_framework::TestType;
use test_framework::anyhow::{Context, Result};
use test_framework::execution::FlightExecutor;
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
    with_explain_plan_snapshot: bool,
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

    // Generate expected results at runtime using DuckDB for the target scale factor
    let validation_data = generate_expected_results(scale_factor, &queries)?;
    println!(
        "Generated expected results for {} queries at SF={scale_factor}",
        validation_data.len()
    );

    // Create query executor for running queries against the spiced instance
    let spice_client = spiced_instance
        .spice_client(None, false)
        .await
        .context("Failed to create Flight client for query verification")?;
    let executor: Box<dyn test_framework::execution::QueryExecutor> =
        Box::new(FlightExecutor::new(Arc::new(spice_client)));

    // Create SpiceTest state with runtime-generated validation data
    let state = NotStarted::new()
        .with_parallel_count(1)
        .with_query_set(queries)
        .with_end_condition(EndCondition::QuerySetCompleted(iterations))
        .with_validate(true)
        .with_scale_factor(scale_factor)
        .with_query_set_type(QuerySet::Tpch)
        .with_query_overrides(Some(QueryOverrides::DynamoDB))
        .with_validation_data(validation_data)
        .with_query_executor(executor);

    // Create and run SpiceTest (name differentiates snapshots per config)
    let mut test = SpiceTest::new(format!("streaming_{config_name}"), state)
        .with_spiced_instance(spiced_instance)
        .with_progress_bars(false);

    if with_explain_plan_snapshot {
        test = test.with_explain_plan_snapshot();
    }

    let test = test.start()?.wait().await?;

    // Collect metrics
    let _row_counts = test.validate_returned_row_counts()?;
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
        all_passed,
    })
}

/// Generate expected TPC-H query results at the given scale factor using `DuckDB`.
///
/// Creates an in-memory `DuckDB` database, generates TPC-H data with `dbgen`,
/// then runs each query to produce the expected `RecordBatch` results.
fn generate_expected_results(
    scale_factor: f64,
    queries: &[Query],
) -> Result<HashMap<Arc<str>, Vec<RecordBatch>>> {
    println!("Generating expected TPC-H results at SF={scale_factor} using DuckDB...");

    let conn =
        Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

    conn.execute_batch("INSTALL tpch; LOAD tpch;")
        .context("Failed to load TPC-H extension")?;

    conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
        .context("Failed to generate TPC-H data")?;

    // Recreate tables with Decimal→DOUBLE casts to match streaming dataset types.
    // Dates are kept as native DATE (not VARCHAR) so TPC-H date comparisons
    // and arithmetic (e.g., `l_shipdate >= date '1994-01-01'`) work correctly.
    // The Date32↔Utf8 type difference is handled by `datatype_equivalent`.
    conn.execute_batch(
        "CREATE OR REPLACE TABLE lineitem AS SELECT
            l_orderkey, l_partkey, l_suppkey, l_linenumber,
            CAST(l_quantity AS BIGINT) AS l_quantity,
            CAST(l_extendedprice AS DOUBLE) AS l_extendedprice,
            CAST(l_discount AS DOUBLE) AS l_discount,
            CAST(l_tax AS DOUBLE) AS l_tax,
            l_returnflag, l_linestatus,
            l_shipdate, l_commitdate, l_receiptdate,
            l_shipinstruct, l_shipmode, l_comment
        FROM lineitem;

        CREATE OR REPLACE TABLE orders AS SELECT
            o_orderkey, o_custkey, o_orderstatus,
            CAST(o_totalprice AS DOUBLE) AS o_totalprice,
            o_orderdate,
            o_orderpriority, o_clerk, o_shippriority, o_comment
        FROM orders;

        CREATE OR REPLACE TABLE customer AS SELECT
            c_custkey, c_name, c_address, c_nationkey, c_phone,
            CAST(c_acctbal AS DOUBLE) AS c_acctbal,
            c_mktsegment, c_comment
        FROM customer;

        CREATE OR REPLACE TABLE supplier AS SELECT
            s_suppkey, s_name, s_address, s_nationkey, s_phone,
            CAST(s_acctbal AS DOUBLE) AS s_acctbal,
            s_comment
        FROM supplier;

        CREATE OR REPLACE TABLE part AS SELECT
            p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container,
            CAST(p_retailprice AS DOUBLE) AS p_retailprice,
            p_comment
        FROM part;

        CREATE OR REPLACE TABLE partsupp AS SELECT
            ps_partkey, ps_suppkey, ps_availqty,
            CAST(ps_supplycost AS DOUBLE) AS ps_supplycost,
            ps_comment
        FROM partsupp;",
    )
    .context("Failed to recreate TPC-H tables with streaming-compatible types")?;

    let mut validation_data = HashMap::new();

    for query in queries {
        let sql = query.to_sql_with_inlined_params();

        match conn.prepare(&sql) {
            Ok(mut stmt) => match stmt.query_arrow([]) {
                Ok(arrow_result) => {
                    let batches: Vec<RecordBatch> = arrow_result.collect();
                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    println!("  {}: {row_count} rows", query.name);
                    validation_data.insert(Arc::clone(&query.name), batches);
                }
                Err(e) => {
                    eprintln!("  Warning: Failed to execute {}: {e}", query.name);
                }
            },
            Err(e) => {
                eprintln!("  Warning: Failed to prepare {}: {e}", query.name);
            }
        }
    }

    Ok(validation_data)
}
