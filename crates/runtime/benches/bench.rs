//! This is a benchmark test suite for the Spice runtime.
//!
//! It performs the following actions:
//! 1. Starts the runtime with all of the datasets to test loaded.
//! 2. Runs a series of queries against the runtime.
//! 3. Reports the results to the spice.ai dataset <https://spice.ai/spicehq/spice-tests/datasets/oss_benchmarks>

// spice.ai/spicehq/spice-tests/datasets/spicehq."spice-tests".oss_benchmarks
// schema
// run_id, started_at, finished_at, query_name, status, min_duration, max_duration, iterations, commit_sha

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use results::BenchmarkResultsBuilder;
use runtime::{dataupdate::DataUpdate, Runtime};

use crate::results::Status;

mod results;
mod setup;

mod bench_postgres;
mod bench_spicecloud;

#[tokio::main]
async fn main() -> Result<(), String> {
    let _ = run_benchmark_test(setup::DataConnector::SpiceAI).await;
    let _ = run_benchmark_test(setup::DataConnector::Postgres).await;
    Ok(())
}

async fn run_benchmark_test(dataconnector: setup::DataConnector) -> Result<(), String> {
    let mut upload_results_dataset: Option<String> = None;
    if let Ok(env_var) = std::env::var("UPLOAD_RESULTS_DATASET") {
        println!("UPLOAD_RESULTS_DATASET: {env_var}");
        upload_results_dataset = Some(env_var);
    }

    let (mut benchmark_results, mut rt) =
        setup::setup_benchmark(&upload_results_dataset, dataconnector.clone()).await;

    match dataconnector {
        setup::DataConnector::SpiceAI => {
            bench_spicecloud::run(&mut rt, &mut benchmark_results).await?
        }
        setup::DataConnector::Postgres => {
            bench_postgres::run(&mut rt, &mut benchmark_results).await?
        }
    }

    let data_update: DataUpdate = benchmark_results.into();

    let display_records = data_update.data.clone();

    if let Some(upload_results_dataset) = upload_results_dataset {
        tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
        setup::write_benchmark_results(data_update, &rt).await?;
    }

    display_benchmark_records(display_records).await?;

    Ok(())
}

fn get_current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0)
}

async fn run_query_and_record_result(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    tracing::info!("Running query `{query_name}`...");
    let start_time = get_current_unix_ms();

    let mut min_iter_duration_ms = i64::MAX;
    let mut max_iter_duration_ms = i64::MIN;

    for _ in 0..benchmark_results.iterations() {
        let start_iter_time = get_current_unix_ms();
        let _ = rt
            .datafusion()
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query_name}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query `{query_name}` to results: {e}"))?;
        let end_iter_time = get_current_unix_ms();

        let iter_duration_ms = end_iter_time - start_iter_time;
        if iter_duration_ms < min_iter_duration_ms {
            min_iter_duration_ms = iter_duration_ms;
        }
        if iter_duration_ms > max_iter_duration_ms {
            max_iter_duration_ms = iter_duration_ms;
        }
    }

    let end_time = get_current_unix_ms();

    benchmark_results.record_result(
        start_time,
        end_time,
        query_name,
        Status::Passed,
        min_iter_duration_ms,
        max_iter_duration_ms,
    );

    Ok(())
}

/// Display the benchmark results record batches to the console.
async fn display_benchmark_records(records: Vec<RecordBatch>) -> Result<(), String> {
    let schema = records[0].schema();

    let ctx = SessionContext::new();
    let provider = MemTable::try_new(schema, vec![records]).map_err(|e| e.to_string())?;
    let df = DataFrame::new(
        ctx.state(),
        LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(provider)), None)
            .map_err(|e| e.to_string())?
            .build()
            .map_err(|e| e.to_string())?,
    );

    if let Err(e) = df.show().await {
        println!("Error displaying results: {e}");
    };
    Ok(())
}
