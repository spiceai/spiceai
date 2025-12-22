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
use arrow::{array::RecordBatch, util::display::FormatOptions};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use futures::TryStreamExt;
use std::sync::Arc;

use crate::utils::TEST_REQUEST_CONTEXT;
use runtime::Runtime;
use runtime::datafusion::builder::DEFAULT_DATAFUSION_CONFIG;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

mod abfs;
mod acceleration;
mod cache;
mod catalog;
mod cayenne;
#[cfg(feature = "duckdb")]
mod clickbench;
mod cors;
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
mod dataset_availability;
mod datasets_api;
#[cfg(feature = "delta_lake")]
mod delta_lake;
mod docker;
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
mod endpoint_auth;
mod file;
mod flight;
mod github;
mod glue;
mod graphql;
mod iceberg;
mod iceberg_api;

#[cfg(feature = "kafka")]
mod kafka;
mod metadata;
#[cfg(feature = "mongodb")]
mod mongo;
#[cfg(feature = "mssql")]
mod mssql;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "odbc")]
mod odbc;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(feature = "postgres")]
mod postgres;
mod ready_state;
mod refresh_retry;
mod refresh_sql;
mod refresh_worker_panic;
mod results_cache;
#[cfg(all(unix, feature = "duckdb", feature = "postgres"))]
mod retention;
mod s3;
mod s3_location_pruning;
#[cfg(feature = "postgres")]
mod schema_evolution;
#[cfg(feature = "snapshots")]
mod snapshot_integration;
#[cfg(feature = "snowflake")]
mod snowflake;
#[cfg(feature = "spark")]
mod spark;
mod spiceai;
#[cfg(feature = "sqlite")]
mod sqlite;
mod tls;
mod utils;
mod view;

mod management;
// MySQL is required for the rehydration tests
mod podswatcher;
#[cfg(all(feature = "mysql", feature = "duckdb"))]
mod rehydration;
mod shutdown;

/// Modifies the `DataFusion` configuration to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 3, by default its the number of CPU cores available.
/// 2) Disables coalesce batches and repartition joins for terser plans.
fn configure_test_datafusion() {
    match DEFAULT_DATAFUSION_CONFIG.write() {
        Ok(mut config) => {
            config.options_mut().execution.target_partitions = 3;

            config.options_mut().execution.coalesce_batches = false;

            config.options_mut().optimizer.repartition_joins = false;
        }
        _ => panic!("Must obtain write lock to defaults"),
    }
}

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
            omit_expression => true
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

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}
