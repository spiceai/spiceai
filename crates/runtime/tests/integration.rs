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

// The runtime's async call graph nests deeply enough that computing the layout of a test's
// top-level future exceeds rustc's default 128-deep query limit. Matches the `recursion_limit`
// the `runtime` crate itself and the sibling integration test crates set.
#![recursion_limit = "256"]

use arrow::{array::RecordBatch, util::display::FormatOptions};
#[cfg(feature = "mysql")]
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use futures::TryStreamExt;
#[cfg(feature = "postgres-accel")]
use std::sync::Arc;

#[cfg(feature = "postgres-accel")]
use crate::utils::TEST_REQUEST_CONTEXT;

use runtime::Runtime;
use runtime::datafusion::builder::DEFAULT_DATAFUSION_CONFIG;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

// The force-links these tests depend on live in `utils`, which every binary sharing
// these helpers includes; the guard below is what proves they are working.
/// An engine reaches the registry only if its crate is linked into this binary, which a
/// Cargo dependency does not guarantee — the linker drops the unreferenced slice static.
/// Asserted here rather than left to the first accelerated test of each engine, which
/// needs a live database and so cannot tell a missing registration apart from a missing
/// server. Extend this with each engine that moves into its own crate.
#[test]
fn accelerator_crates_register_their_engines() {
    let engines = data_accelerator_api::registered_engine_names();
    #[cfg(feature = "postgres-accel")]
    assert!(
        engines.iter().any(|engine| engine == "postgres"),
        "the postgres accelerator is not registered in this test binary; linked engines: {engines:?}"
    );
    #[cfg(feature = "sqlite")]
    assert!(
        engines.iter().any(|engine| engine == "sqlite"),
        "the sqlite accelerator is not registered in this test binary; linked engines: {engines:?}"
    );
    #[cfg(feature = "turso")]
    assert!(
        engines.iter().any(|engine| engine == "turso"),
        "the turso accelerator is not registered in this test binary; linked engines: {engines:?}"
    );
    #[cfg(feature = "duckdb")]
    assert!(
        engines.iter().any(|engine| engine == "duckdb"),
        "the duckdb accelerator is not registered in this test binary; linked engines: {engines:?}"
    );
    #[cfg(not(windows))]
    assert!(
        engines.iter().any(|engine| engine == "cayenne"),
        "the cayenne accelerator is not registered in this test binary; linked engines: {engines:?}"
    );
    let _ = &engines;
}

mod abfs;
mod acceleration;
#[cfg(feature = "adbc")]
mod adbc;
mod cache;
mod catalog;
#[cfg(not(windows))]
mod cayenne;
#[cfg(not(windows))]
mod cayenne_catalog_ddl;
#[cfg(feature = "duckdb")]
mod clickbench;
mod cluster;
mod cors;
#[cfg(feature = "cosmosdb")]
mod cosmosdb;
#[cfg(all(feature = "delta_lake", feature = "databricks"))]
mod databricks_delta;
#[cfg(all(feature = "delta_lake", feature = "databricks"))]
mod databricks_delta_catalog;
#[cfg(all(feature = "delta_lake", feature = "databricks"))]
mod databricks_delta_catalog_m2m;
#[cfg(all(feature = "delta_lake", feature = "databricks"))]
mod databricks_delta_m2m;
#[cfg(all(feature = "spark", feature = "databricks"))]
mod databricks_spark;
#[cfg(all(feature = "spark", feature = "databricks"))]
mod databricks_spark_catalog;
#[cfg(all(feature = "spark", feature = "databricks"))]
mod databricks_spark_catalog_m2m;
#[cfg(all(feature = "spark", feature = "databricks"))]
mod databricks_spark_m2m;
#[cfg(feature = "databricks")]
mod databricks_sql_warehouse;
#[cfg(feature = "databricks")]
mod databricks_sql_warehouse_permissions;
mod dataset_availability;
mod datasets_api;
#[cfg(feature = "delta_lake")]
mod delta_lake;
mod docker;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
mod ducklake;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
mod endpoint_auth;
mod file;
mod flight;
mod gcs;
mod git;
mod github;
mod glue;
mod graphql;
#[cfg(all(feature = "postgres", feature = "hashicorp_vault"))]
mod hashicorp_vault;
mod http;
mod iceberg;
mod iceberg_api;
mod json;

#[cfg(feature = "debezium")]
mod cdc_ingest;
mod cluster_tls_reload;
#[cfg(feature = "kafka")]
mod kafka;
mod metadata;
#[cfg(feature = "mongodb")]
mod mongo;
#[cfg(feature = "mssql")]
mod mssql;
mod mtls_connector;
mod mtls_public;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "odbc")]
mod odbc;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(not(windows))]
mod otel_ingest_races;
#[cfg(not(windows))]
mod otel_restart;
mod plan_capture;
#[cfg(feature = "postgres")]
mod postgres;
mod prepared_statements;
#[cfg(feature = "rate-control")]
mod rate_control;
mod ready_state;
mod refresh_retry;
mod refresh_sql;
mod refresh_worker_panic;
mod results_cache;
#[cfg(all(unix, feature = "duckdb", feature = "postgres"))]
mod retention;
mod s3;
mod s3_location_pruning;
#[cfg(any(
    feature = "postgres",
    feature = "duckdb",
    feature = "sqlite",
    feature = "turso"
))]
mod schema_evolution;
#[cfg(feature = "sharepoint")]
mod sharepoint;
#[cfg(feature = "snapshots")]
mod snapshot_integration;
#[cfg(feature = "snowflake")]
mod snowflake;
#[cfg(feature = "snowflake")]
mod snowflake_catalog;
#[cfg(feature = "spark")]
mod spark;
mod spiceai;
#[cfg(feature = "sqlite")]
mod sqlite;
mod tls;
mod tls_reload;
#[cfg(feature = "postgres-accel")]
mod tpcds_postgres;
mod utils;
mod view;

mod management;
// MySQL is required for the rehydration tests (source container); the
// local-db verification covers whichever persistent engines are enabled.
mod podswatcher;
#[cfg(all(feature = "mysql", any(feature = "duckdb", feature = "sqlite")))]
mod rehydration;
mod shutdown;

/// The CPU entitlement every test in this binary is pinned to.
///
/// Sizing derived from the CPU budget — `target_partitions` above all, but also
/// worker-thread counts and encode permits — would otherwise follow the host and
/// make explain-plan snapshots machine-dependent.
const TEST_CPU_CORES: usize = 3;

/// Modifies the `DataFusion` configuration to make test results reproducible across all machines.
///
/// 1) Pins the CPU budget, and with it `target_partitions`, to [`TEST_CPU_CORES`].
/// 2) Disables coalesce batches and repartition joins for terser plans.
fn configure_test_datafusion() {
    pin_test_cpu_budget();

    match DEFAULT_DATAFUSION_CONFIG.write() {
        Ok(mut config) => {
            config.options_mut().execution.target_partitions = TEST_CPU_CORES;

            config.options_mut().execution.coalesce_batches = false;

            config.options_mut().optimizer.repartition_joins = false;
        }
        _ => panic!("Must obtain write lock to defaults"),
    }
}

/// Pin the process-wide CPU budget to [`TEST_CPU_CORES`].
///
/// Setting `target_partitions` on the default session config is not enough on its
/// own: with `runtime.query.target_partitions` unset the session builder sizes
/// partitions from the CPU budget, overwriting whatever the config carried. Both
/// are pinned to the same constant so they cannot disagree.
///
/// Installing is idempotent by intent — the budget is a process-wide `OnceLock`
/// and all 300-odd callers ask for the same value, so every call after the first
/// is an expected no-op rather than an error worth surfacing.
fn pin_test_cpu_budget() {
    let config = cpu_budget::CpuConfig::from_sources(None, None, Some(&TEST_CPU_CORES.to_string()));
    match cpu_budget::CpuBudget::resolve(&config, &cpu_budget::HostReadings::detect()) {
        Ok(budget) => drop(budget.install()),
        Err(e) => panic!("{TEST_CPU_CORES} must be a valid CPU quantity: {e}"),
    }
}
#[cfg(feature = "postgres-accel")]
fn configure_test_datafusion_request_context() {
    match DEFAULT_DATAFUSION_CONFIG.write() {
        Ok(mut config) => config.set_extension(Arc::clone(&TEST_REQUEST_CONTEXT)),
        _ => panic!("Must obtain write lock to defaults"),
    }
}

fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new("runtime=TRACE,datafusion-federation=TRACE"),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

#[cfg(feature = "mysql")]
async fn get_tpch_lineitem() -> Result<Vec<RecordBatch>, anyhow::Error> {
    let lineitem_parquet_bytes =
        reqwest::get("https://public-data.spiceai.org/tpch_lineitem.parquet")
            .await?
            .bytes()
            .await?;

    let parquet_reader =
        ParquetRecordBatchReaderBuilder::try_new(lineitem_parquet_bytes)?.build()?;

    Ok(parquet_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?)
}

type ValidateFn = dyn FnOnce(Vec<RecordBatch>);

async fn run_query_and_check_results<F>(
    rt: &mut Runtime,
    snapshot_name: &str,
    query: &str,
    snapshot_plan: bool,
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
    // Check the plan
    let query_results = rt
        .datafusion()
        .query_builder(&format!("EXPLAIN {query}"))
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?;

    let plan_results: Vec<RecordBatch> = query_results
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    println!("Query: {query}");

    let Ok(explain_plan) = arrow::util::pretty::pretty_format_batches(&plan_results) else {
        panic!("Failed to format plan");
    };

    if snapshot_plan {
        insta::with_settings!({
            description => format!("Query: {query}"),
            omit_expression => true,
            filters => vec![
                // Normalize HTTP server ports: http://127.0.0.1:12345 → http://127.0.0.1:<PORT>
                (r"http://127\.0\.0\.1:\d+", "http://127.0.0.1:<PORT>"),
            ],
        }, {
            insta::assert_snapshot!(snapshot_name, explain_plan);
        });
    }

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = rt
            .datafusion()
            .query_builder(query)
            .build()
            .run()
            .await
            .map_err(|e| format!("query `{query}` failed to run: {e}"))?
            .data
            .try_collect()
            .await
            .map_err(|e| format!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}

type PlanCheckFn = Box<dyn Fn(&str) -> bool>;

async fn run_query_and_check_results_with_plan_checks<F>(
    rt: &mut Runtime,
    query: &str,
    expected_plan_checks: Vec<(&str, PlanCheckFn)>,
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
    // Check the plan
    let plan_results: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(&format!("EXPLAIN {query}"))
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    let Ok(formatted) = arrow::util::pretty::pretty_format_batches_with_options(
        &plan_results,
        &FormatOptions::default(),
    ) else {
        panic!("Failed to format plan");
    };
    let formatted = formatted.to_string();

    let actual_lines: Vec<&str> = formatted.trim().lines().collect();

    let mut matched_checks = vec![false; expected_plan_checks.len()];

    for line in actual_lines {
        for (i, (key, check_fn)) in expected_plan_checks.iter().enumerate() {
            if line.contains(key) {
                if matched_checks[i] {
                    return Err(format!(
                        "Check '{key}' matched multiple lines in plan:\n{formatted}",
                    ));
                }
                matched_checks[i] = true;
                if !check_fn(line) {
                    return Err(format!("Check failed for line: {line}"));
                }
            }
        }
    }

    if let Some(i) = matched_checks.iter().position(|&x| !x) {
        return Err(format!(
            "Expected check '{}' did not appear in plan:\n{formatted}",
            expected_plan_checks[i].0,
        ));
    }

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = rt
            .datafusion()
            .query_builder(query)
            .build()
            .run()
            .await
            .map_err(|e| format!("query `{query}` failed to run: {e}"))?
            .data
            .try_collect()
            .await
            .map_err(|e| format!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}
