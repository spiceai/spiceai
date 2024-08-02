/*
Copyright 2024 The Spice.ai OSS Authors

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

//! This is a benchmark test suite for the Spice runtime.
//!
//! It performs the following actions:
//! 1. Starts the runtime with all of the datasets to test loaded.
//! 2. Runs a series of queries against the runtime.
//! 3. Reports the results to the spice.ai dataset <https://spice.ai/spicehq/spice-tests/datasets/oss_benchmarks>

// spice.ai/spicehq/spice-tests/datasets/spicehq."spice-tests".oss_benchmarks
// schema
// run_id, started_at, finished_at, connector_name, query_name, status, min_duration, max_duration, iterations, commit_sha

use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use results::BenchmarkResultsBuilder;
use runtime::{dataupdate::DataUpdate, Runtime};
use spicepod::component::dataset::acceleration::{self, Acceleration};

use crate::results::Status;

mod results;
mod setup;

mod bench_s3;
mod bench_spicecloud;

#[cfg(feature = "delta_lake")]
mod bench_delta;
#[cfg(feature = "mysql")]
mod bench_mysql;
#[cfg(feature = "odbc")]
mod bench_odbc_athena;
#[cfg(feature = "odbc")]
mod bench_odbc_databricks;
#[cfg(feature = "postgres")]
mod bench_postgres;
#[cfg(feature = "spark")]
mod bench_spark;

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut upload_results_dataset: Option<String> = None;
    if let Ok(env_var) = std::env::var("UPLOAD_RESULTS_DATASET") {
        println!("UPLOAD_RESULTS_DATASET: {env_var}");
        upload_results_dataset = Some(env_var);
    }

    let connectors = vec![
        "spice.ai",
        "s3",
        #[cfg(feature = "spark")]
        "spark",
        #[cfg(feature = "postgres")]
        "postgres",
        #[cfg(feature = "mysql")]
        "mysql",
        #[cfg(feature = "odbc")]
        "odbc-databricks",
        #[cfg(feature = "odbc")]
        "odbc-athena",
        #[cfg(feature = "delta_lake")]
        "delta_lake",
    ];

    let mut display_records = vec![];

    for connector in connectors {
        let (mut benchmark_results, mut rt) =
            setup::setup_benchmark(&upload_results_dataset, connector, None).await;

        match connector {
            "spice.ai" => {
                bench_spicecloud::run(&mut rt, &mut benchmark_results).await?;
            }
            "s3" => {
                bench_s3::run(&mut rt, &mut benchmark_results, None).await?;
            }
            #[cfg(feature = "spark")]
            "spark" => {
                bench_spark::run(&mut rt, &mut benchmark_results).await?;
            }
            #[cfg(feature = "postgres")]
            "postgres" => {
                bench_postgres::run(&mut rt, &mut benchmark_results).await?;
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                bench_mysql::run(&mut rt, &mut benchmark_results).await?;
            }
            #[cfg(feature = "odbc")]
            "odbc-databricks" => {
                bench_odbc_databricks::run(&mut rt, &mut benchmark_results).await?;
            }
            #[cfg(feature = "odbc")]
            "odbc-athena" => {
                bench_odbc_athena::run(&mut rt, &mut benchmark_results).await?;
            }
            #[cfg(feature = "delta_lake")]
            "delta_lake" => {
                bench_delta::run(&mut rt, &mut benchmark_results).await?;
            }
            _ => {}
        }
        let data_update: DataUpdate = benchmark_results.into();

        let mut records = data_update.data.clone();
        display_records.append(&mut records);

        if let Some(upload_results_dataset) = upload_results_dataset.clone() {
            tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
            setup::write_benchmark_results(data_update, &rt).await?;
        }
    }

    let accelerators: Vec<Acceleration> = vec![
        create_acceleration("arrow", acceleration::Mode::Memory),
        #[cfg(feature = "duckdb")]
        create_acceleration("duckdb", acceleration::Mode::Memory),
        #[cfg(feature = "duckdb")]
        create_acceleration("duckdb", acceleration::Mode::File),
        #[cfg(feature = "sqlite")]
        create_acceleration("sqlite", acceleration::Mode::Memory),
        #[cfg(feature = "sqlite")]
        create_acceleration("sqlite", acceleration::Mode::File),
    ];

    for accelerator in accelerators {
        let (mut benchmark_results, mut rt) =
            setup::setup_benchmark(&upload_results_dataset, "s3", Some(&accelerator)).await;

        bench_s3::run(&mut rt, &mut benchmark_results, Some(accelerator)).await?;

        let data_update: DataUpdate = benchmark_results.into();

        let mut records = data_update.data.clone();
        display_records.append(&mut records);

        if let Some(upload_results_dataset) = upload_results_dataset.clone() {
            tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
            setup::write_benchmark_results(data_update, &rt).await?;
        }
    }

    display_benchmark_records(display_records).await?;
    Ok(())
}

fn create_acceleration(engine: &str, mode: acceleration::Mode) -> Acceleration {
    Acceleration {
        engine: Some(engine.to_string()),
        mode,
        ..Default::default()
    }
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
    connector: &str,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    tracing::info!("Running query `{connector}` `{query_name}`...");
    let start_time = get_current_unix_ms();

    let mut min_iter_duration_ms = i64::MAX;
    let mut max_iter_duration_ms = i64::MIN;

    let mut query_err: Option<String> = None;

    let mut completed_iterations = 0;

    for _ in 0..benchmark_results.iterations() {
        completed_iterations += 1;

        let start_iter_time = get_current_unix_ms();

        let res = run_query(rt, connector, query_name, query).await;
        let end_iter_time = get_current_unix_ms();

        let iter_duration_ms = end_iter_time - start_iter_time;
        if iter_duration_ms < min_iter_duration_ms {
            min_iter_duration_ms = iter_duration_ms;
        }
        if iter_duration_ms > max_iter_duration_ms {
            max_iter_duration_ms = iter_duration_ms;
        }

        if let Err(e) = res {
            query_err = Some(e);
            break;
        }
    }

    let end_time = get_current_unix_ms();

    benchmark_results.record_result(
        start_time,
        end_time,
        connector,
        query_name,
        if query_err.is_some() {
            Status::Failed
        } else {
            Status::Passed
        },
        min_iter_duration_ms,
        max_iter_duration_ms,
        completed_iterations,
    );

    if let Some(e) = query_err {
        return Err(e);
    }

    Ok(())
}

async fn run_query(
    rt: &mut Runtime,
    connector: &str,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    let _ = rt
        .datafusion()
        .ctx
        .sql(query)
        .await
        .map_err(|e| format!("query `{connector}` `{query_name}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{connector}` `{query_name}` to results: {e}"))?;

    Ok(())
}

/// Display the benchmark results record batches to the console.
async fn display_benchmark_records(records: Vec<RecordBatch>) -> Result<(), String> {
    if records.is_empty() {
        return Ok(());
    }

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
