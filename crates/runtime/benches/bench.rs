/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::fmt::{Display, Formatter};
use std::panic;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "postgres")]
use crate::bench_postgres::get_postgres_params;
use crate::results::Status;
use arrow::array::RecordBatch;
use clap::Parser;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use futures::TryStreamExt;
use results::{BenchmarkResult, BenchmarkResultsBuilder};
use runtime::request::{Protocol, RequestContext, UserAgent};
use runtime::{dataupdate::DataUpdate, Runtime};
use spicepod::component::dataset::acceleration::{self, Acceleration, Mode, RefreshMode};

mod results;
mod setup;
mod utils;

mod bench_object_store;
mod bench_spicecloud;

#[cfg(feature = "delta_lake")]
mod bench_delta;
#[cfg(feature = "dremio")]
mod bench_dremio;
#[cfg(feature = "duckdb")]
mod bench_duckdb;
#[cfg(feature = "mssql")]
mod bench_mssql;
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

#[derive(Debug, Default, Clone, Copy)]
enum AcceleratorRefreshMode {
    Append,
    #[default]
    Full,
}

impl FromStr for AcceleratorRefreshMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "append" => Ok(AcceleratorRefreshMode::Append),
            "full" => Ok(AcceleratorRefreshMode::Full),
            _ => Err(format!("Unsupported accelerator refresh mode: {s}")),
        }
    }
}

impl Display for AcceleratorRefreshMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AcceleratorRefreshMode::Append => write!(f, "append"),
            AcceleratorRefreshMode::Full => write!(f, "full"),
        }
    }
}

impl From<AcceleratorRefreshMode> for acceleration::RefreshMode {
    fn from(mode: AcceleratorRefreshMode) -> Self {
        match mode {
            AcceleratorRefreshMode::Append => acceleration::RefreshMode::Append,
            AcceleratorRefreshMode::Full => acceleration::RefreshMode::Full,
        }
    }
}

// Define command line arguments for running benchmark test
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct BenchArgs {
    /// Run the benchmark
    #[arg(long)]
    bench: bool,

    /// Set the connector to run benchmark test on
    #[arg(short, long)]
    connector: Option<String>,

    /// Set the accelerator to run benchmark test on
    #[arg(short, long)]
    accelerator: Option<String>,

    /// Set the acceleration mode for accelerator
    #[arg(short, long)]
    mode: Option<String>,

    #[arg(long, default_value_t=AcceleratorRefreshMode::Full)]
    refresh_mode: AcceleratorRefreshMode,

    /// Set the benchmark to run: TPCH / TPCDS
    #[arg(short, long, default_value = "tpch")]
    bench_name: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    let request_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spicebench/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .build(),
    );

    Box::pin(request_context.scope(bench_main())).await
}

#[allow(clippy::too_many_lines)]
async fn bench_main() -> Result<(), String> {
    let mut upload_results_dataset: Option<String> = None;
    if let Ok(env_var) = std::env::var("UPLOAD_RESULTS_DATASET") {
        println!("UPLOAD_RESULTS_DATASET: {env_var}");
        upload_results_dataset = Some(env_var);
    }

    let args = BenchArgs::parse();

    match (args.connector.as_deref(), args.accelerator.as_deref(), args.mode.as_deref()) {
        (None, None, None) => {
            // Run all connector / accelerator benchmark tests
            let connectors = vec![
                "spice.ai",
                "s3",
                "abfs",
                "file",
                #[cfg(feature = "spark")]
                "spark",
                #[cfg(feature = "postgres")]
                "postgres",
                #[cfg(feature = "mysql")]
                "mysql",
                #[cfg(feature = "duckdb")]
                "duckdb",
                #[cfg(feature = "odbc")]
                "odbc-databricks",
                #[cfg(feature = "odbc")]
                "odbc-athena",
                #[cfg(all(feature = "delta_lake", feature = "databricks"))]
                "delta_lake",
                #[cfg(feature = "mssql")]
                "mssql",
            ];
            for connector in connectors {
                run_connector_bench(connector, upload_results_dataset.as_ref(), args.bench_name.as_ref()).await?;
            }
            let accelerators: Vec<Acceleration> = vec![
                create_acceleration("arrow", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Full),
                create_acceleration("arrow", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Append),
                #[cfg(feature = "duckdb")]
                create_acceleration("duckdb", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Full),
                #[cfg(feature = "duckdb")]
                create_acceleration("duckdb", acceleration::Mode::File, args.bench_name.as_ref(), RefreshMode::Full),
                #[cfg(feature = "duckdb")]
                create_acceleration("duckdb", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Append),
                #[cfg(feature = "duckdb")]
                create_acceleration("duckdb", acceleration::Mode::File, args.bench_name.as_ref(), RefreshMode::Append),
                #[cfg(feature = "sqlite")]
                create_acceleration("sqlite", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Full),
                #[cfg(feature = "sqlite")]
                create_acceleration("sqlite", acceleration::Mode::File, args.bench_name.as_ref(), RefreshMode::Full),
                #[cfg(feature = "postgres")]
                create_acceleration("postgres", acceleration::Mode::Memory, args.bench_name.as_ref(), RefreshMode::Full),
            ];
            for accelerator in accelerators {
                if accelerator.refresh_mode == Some(RefreshMode::Append) {
                    run_accelerator_bench("file", accelerator.clone(), upload_results_dataset.as_ref(), "tpch").await?;
                    run_accelerator_bench("file", accelerator.clone(), upload_results_dataset.as_ref(), "tpcds").await?;
                    run_accelerator_bench("file", accelerator.clone(), upload_results_dataset.as_ref(), "clickbench").await?;
                } else {
                    run_accelerator_bench("s3", accelerator.clone(), upload_results_dataset.as_ref(), "tpch").await?;
                    run_accelerator_bench("s3", accelerator.clone(), upload_results_dataset.as_ref(), "tpds").await?;
                }
            }
        },
        (Some(connector), None, None) => {
            // Run connector benchmark test
            run_connector_bench(connector, upload_results_dataset.as_ref(), args.bench_name.as_ref()).await?;
        },
        (None, Some(accelerator), mode) => {
            // Run accelerator benchmark test
            let mode = match mode {
                Some("file") => Mode::File,
                Some("memory") | None => Mode::Memory,
                _ => return Err(format!("Invalid mode parameter for {accelerator} accelerator")),
            };

            let refresh_mode = RefreshMode::from(args.refresh_mode);
            let acceleration = create_acceleration(accelerator, mode, args.bench_name.as_ref(), refresh_mode.clone());

            match (refresh_mode, args.bench_name.as_ref()) {
                (RefreshMode::Append, "tpch") => {
                    run_accelerator_bench("file", acceleration, upload_results_dataset.as_ref(), "tpch").await?;
                }
                (RefreshMode::Append, "tpcds") => {
                    run_accelerator_bench("file", acceleration, upload_results_dataset.as_ref(), "tpcds").await?;
                }
                (RefreshMode::Append, "clickbench") => {
                    run_accelerator_bench("file", acceleration, upload_results_dataset.as_ref(), "clickbench").await?;
                }
                (RefreshMode::Full, "tpch") => {
                    run_accelerator_bench("s3", acceleration, upload_results_dataset.as_ref(), "tpch").await?;
                }
                (RefreshMode::Full, "tpcds") => {
                    run_accelerator_bench("s3", acceleration, upload_results_dataset.as_ref(), "tpcds").await?;
                }
                (RefreshMode::Full, "clickbench") => {
                    run_accelerator_bench("s3", acceleration, upload_results_dataset.as_ref(), "clickbench").await?;
                }
                (RefreshMode::Append, benchmark) => return Err(format!("Append mode benchmark is not implemented for {benchmark}")),
                (RefreshMode::Changes, benchmark) => return Err(format!("CDC mode benchmark is not implemented for {benchmark}")),
                (RefreshMode::Full, benchmark) => return Err(format!("Invalid benchmark parameter for accelerator benchmark: {benchmark}")),
            }
        },
        _ => return Err("Invalid command line input: accelerator or mode parameter supplied for connector benchmark".to_string()),
    }

    Ok(())
}

async fn run_connector_bench(
    connector: &str,
    upload_results_dataset: Option<&String>,
    bench_name: &str,
) -> Result<(), String> {
    let mut display_records = vec![];

    let (mut benchmark_results, mut rt) =
        setup::setup_benchmark(upload_results_dataset, connector, None, bench_name).await?;

    match connector {
        "spice.ai" => {
            bench_spicecloud::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        "s3" | "abfs" | "file" => {
            bench_object_store::run(connector, &mut rt, &mut benchmark_results, None, bench_name)
                .await?;
        }
        #[cfg(feature = "spark")]
        "spark" => {
            bench_spark::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            bench_postgres::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            bench_mysql::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            bench_duckdb::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "odbc")]
        "odbc-databricks" => {
            bench_odbc_databricks::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "odbc")]
        "odbc-athena" => {
            bench_odbc_athena::run(&mut rt, &mut benchmark_results).await?;
        }
        #[cfg(feature = "delta_lake")]
        "delta_lake" => {
            bench_delta::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "mssql")]
        "mssql" => {
            bench_mssql::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        #[cfg(feature = "dremio")]
        "dremio" => {
            bench_dremio::run(&mut rt, &mut benchmark_results, bench_name).await?;
        }
        _ => {}
    }
    let data_update: DataUpdate = benchmark_results.into();

    let mut records = data_update.data.clone();
    display_records.append(&mut records);

    if let Some(upload_results_dataset) = upload_results_dataset {
        tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
        setup::write_benchmark_results(data_update, &rt).await?;
    }

    display_benchmark_records(display_records).await?;
    Ok(())
}

async fn run_accelerator_bench(
    connector: &str,
    accelerator: Acceleration,
    upload_results_dataset: Option<&String>,
    bench_name: &str,
) -> Result<(), String> {
    let mut display_records = vec![];

    let (benchmark_results, rt) = match (accelerator.refresh_mode.clone(), connector) {
        #[cfg(feature = "duckdb")]
        (Some(RefreshMode::Append), "file") => {
            let scale_factor = 10.0; // TODO: parameterize this
            let handle = bench_duckdb::delayed_source_load_to_parquet(
                bench_name,
                10,                       // TODO: parameterize this
                Duration::from_secs(120), // 2 minutes * 10 = loading over 20 minutes + overhead for data generation
                scale_factor,
            )?;

            // tracing doesn't initialize until setup_benchmark, but I don't want to call it until data is ready to avoid missing table errors in spiced log
            println!("Waiting for delayed source load to start...");

            let mut append_startup_timer: usize = 0;
            let append_startup_modifier = match bench_name {
                "tpcds" => 2,
                "clickbench" => 3,
                _ => 1,
            };

            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                append_startup_timer += 5;
                if handle.is_finished() {
                    if let Ok(Err(e)) = handle.await {
                        return Err(format!("Error in delayed source load: {e}"));
                    }

                    return Err("Delayed source load failed - exited with no error".to_string());
                }

                println!(
                    "Waiting another {} seconds for delayed source load to start.",
                    120 * append_startup_modifier - append_startup_timer
                );
                if append_startup_timer >= 120 * append_startup_modifier {
                    break;
                }
            }

            let (mut benchmark_results, mut rt) = setup::setup_benchmark(
                upload_results_dataset,
                connector,
                Some(accelerator.clone()),
                bench_name,
            )
            .await?;

            bench_object_store::file::run_file_append(
                &mut rt,
                &mut benchmark_results,
                bench_name,
                Some(accelerator),
            )
            .await?;

            if let Ok(Err(e)) = handle.await {
                return Err(format!("Error in delayed source load: {e}"));
            }

            (benchmark_results, rt)
        }
        (Some(RefreshMode::Append), _) => {
            return Err(format!(
                "Append mode benchmark is not implemented for connector source {connector}"
            ));
        }
        (None, "s3" | "abfs") => {
            let (mut benchmark_results, mut rt) = setup::setup_benchmark(
                upload_results_dataset,
                connector,
                Some(accelerator.clone()),
                bench_name,
            )
            .await?;

            bench_object_store::run(
                connector,
                &mut rt,
                &mut benchmark_results,
                Some(accelerator),
                bench_name,
            )
            .await?;

            (benchmark_results, rt)
        }
        _ => {
            return Err(format!(
                "Connector {connector} is not supported for accelerator benchmark"
            ));
        }
    };

    let data_update: DataUpdate = benchmark_results.into();

    let mut records = data_update.data.clone();
    display_records.append(&mut records);

    if let Some(upload_results_dataset) = upload_results_dataset {
        tracing::info!("Writing benchmark results to dataset {upload_results_dataset}...");
        setup::write_benchmark_results(data_update, &rt).await?;
    }

    display_benchmark_records(display_records).await?;
    Ok(())
}

fn create_acceleration(
    engine: &str,
    mode: acceleration::Mode,
    bench_name: &str,
    append: acceleration::RefreshMode,
) -> Acceleration {
    let mut acceleration = match engine {
        #[cfg(feature = "postgres")]
        "postgres" => Acceleration {
            engine: Some(engine.to_string()),
            mode,
            params: Some(get_postgres_params(true, bench_name)),
            ..Default::default()
        },
        _ => Acceleration {
            engine: Some(engine.to_string()),
            mode,
            params: None,
            ..Default::default()
        },
    };

    if append == RefreshMode::Append {
        acceleration.refresh_mode = Some(append);
        acceleration.refresh_check_interval = Some("3m".to_string());
    }

    acceleration
}

fn get_current_unix_ms() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0)
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn run_query_and_return_result(
    rt: &mut Runtime,
    iterations: i32,
    connector: &str,
    query_name: &str,
    query: &str,
    verify_query_result: bool,
) -> Result<BenchmarkResult, String> {
    // Additional round of query run before recording results.
    // To discard the abnormal results caused by: establishing initial connection / spark cluster startup time
    let _ = run_query(rt, connector, query_name, query).await;
    let snapshot_err = record_explain_plan(rt, connector, query_name, query)
        .await
        .err();

    tracing::info!("Running query `{connector}` `{query_name}`...");
    let start_time = get_current_unix_ms();

    let mut min_iter_duration_ms = i64::MAX;
    let mut max_iter_duration_ms = i64::MIN;

    let mut query_err: Option<String> = None;

    let mut completed_iterations = 0;

    for idx in 0..iterations {
        completed_iterations += 1;

        let start_iter_time = get_current_unix_ms();

        tracing::debug!(
            "Running iteration {} of {iterations} for query `{connector}` `{query_name}`...",
            idx + 1,
        );
        let res = run_query(rt, connector, query_name, query).await;
        let end_iter_time = get_current_unix_ms();

        let iter_duration_ms = end_iter_time - start_iter_time;
        if iter_duration_ms < min_iter_duration_ms {
            min_iter_duration_ms = iter_duration_ms;
        }
        if iter_duration_ms > max_iter_duration_ms {
            max_iter_duration_ms = iter_duration_ms;
        }

        match res {
            Ok(records) => {
                // Show the first 10 records of the result from the first iteration
                if idx == 0 {
                    let num_rows = records
                        .iter()
                        .map(arrow::array::RecordBatch::num_rows)
                        .sum::<usize>();
                    let limited_records: Vec<_> = records
                        .iter()
                        .flat_map(|batch: &RecordBatch| {
                            // We only take up to 10 records anyway, so avoid iterating over large row results
                            let end = if batch.num_rows() > 10 {
                                10
                            } else {
                                batch.num_rows()
                            };

                            (0..end).map(move |i| batch.slice(i, 1))
                        })
                        .take(10)
                        .collect();
                    let records_pretty =
                        arrow::util::pretty::pretty_format_batches(&limited_records)
                            .map_err(|e| e.to_string())?;
                    tracing::info!(
                    "Query `{connector}` `{query_name}` returned {num_rows} rows:\n{records_pretty}",
                );
                    if verify_query_result {
                        let result = panic::catch_unwind(|| {
                            insta::assert_snapshot!(
                                format!("{connector}_{query_name}"),
                                records_pretty
                            );
                        });
                        if result.is_err() {
                            let error_str = format!(
                                "Query `{connector}` `{query_name}` snapshot assertion failed",
                            );
                            tracing::error!(error_str);
                            query_err = Some(error_str);
                            break;
                        }
                    }
                }

                tracing::debug!(
                    "Query `{connector}` `{query_name}` iteration {idx} completed in {iter_duration_ms}ms",
                );
            }
            Err(e) => {
                tracing::error!(
                    "Query `{connector}` `{query_name}` iteration {idx} failed with error: \n{e}",
                );
                query_err = Some(e);
                break;
            }
        }
    }

    let end_time = get_current_unix_ms();
    // Both query failure and snapshot test failure will cause the result to be written as Status::Failed
    let result = BenchmarkResult::new(
        start_time,
        end_time,
        connector,
        query_name,
        if query_err.is_some() || snapshot_err.is_some() {
            Status::Failed
        } else {
            Status::Passed
        },
        min_iter_duration_ms,
        max_iter_duration_ms,
        completed_iterations,
    );

    match (query_err, snapshot_err) {
        (Some(query), Some(snapshot)) => {
            return Err(format!(
                "Query Error: {query}; Snapshot Test Error: {snapshot}",
            ));
        }
        (Some(query), None) => {
            return Err(format!("Query Error: {query}"));
        }
        (None, Some(snapshot)) => {
            return Err(format!("Snapshot Test Error: {snapshot}"));
        }
        (None, None) => {}
    }

    Ok(result)
}

pub(crate) async fn run_query_and_record_result(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    connector: &str,
    query_name: &str,
    query: &str,
    verify_query_result: bool,
) -> Result<(), String> {
    let result = run_query_and_return_result(
        rt,
        benchmark_results.iterations(),
        connector,
        query_name,
        query,
        verify_query_result,
    )
    .await?;

    benchmark_results.record_result(result);

    Ok(())
}

async fn run_query(
    rt: &mut Runtime,
    connector: &str,
    query_name: &str,
    query: &str,
) -> Result<Vec<RecordBatch>, String> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{connector}` `{query_name}` to plan: {e}"))?;

    let res = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("query `{connector}` `{query_name}` to results: {e}"))?;

    Ok(res)
}

const ENABLED_SNAPSHOT_CONNECTORS: &[&str] = &["spice.ai", "s3", "s3_arrow_memory"];

async fn record_explain_plan(
    rt: &mut Runtime,
    connector: &str,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    if query_name.starts_with("clickbench") {
        return Ok(());
    }

    if !ENABLED_SNAPSHOT_CONNECTORS.contains(&connector) {
        return Ok(());
    }

    // Check the plan
    let plan_results = rt
        .datafusion()
        .ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    let Ok(explain_plan) = arrow::util::pretty::pretty_format_batches(&plan_results) else {
        return Err("Failed to format plan".to_string());
    };

    let mut assertion_err: Option<String> = None;

    insta::with_settings!({
        description => format!("Query: {query}"),
        omit_expression => true,
        filters => vec![
            (r"required_guarantees=\[[^\]]*\]", "required_guarantees=[N]"),
        ],
    }, {
        let result = panic::catch_unwind(|| {
            insta::assert_snapshot!(format!("{connector}_{query_name}_explain"), explain_plan);
        });
        if result.is_err() {
            assertion_err = Some(format!("Snapshort assertion failed for {connector}, {query_name}"));
        }
    });

    if let Some(assertion_err) = assertion_err {
        return Err(assertion_err);
    }

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
