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

use std::{sync::Arc, time::Duration};

use arrow::{array::RecordBatch, util::display::FormatOptions};
use datafusion::{
    execution::context::SessionContext,
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use futures::Future;
use runtime::{datafusion::DataFusion, status, Runtime};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

mod acceleration;
mod catalog;
#[cfg(feature = "delta_lake")]
mod delta_lake;
mod docker;
mod federation;
mod graphql;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "odbc")]
mod odbc;
#[cfg(feature = "postgres")]
mod postgres;
mod refresh_retry;
mod refresh_sql;
mod results_cache;
mod s3;
#[cfg(feature = "sqlite")]
mod sqlite;
mod tls;

// MySQL is required for the rehydration tests
#[cfg(feature = "mysql")]
mod rehydration;

/// Gets a test `DataFusion` to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 3, by default its the number of CPU cores available.
fn get_test_datafusion(status: Arc<status::RuntimeStatus>) -> Arc<DataFusion> {
    let mut df = DataFusion::new(status);

    // Set the target partitions to 3 to make RepartitionExec show consistent partitioning across machines with different CPU counts.
    let mut new_state = df.ctx.state();
    new_state
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 3;
    let new_ctx = SessionContext::new_with_state(new_state);

    // Replace the old context with the modified one
    df.ctx = new_ctx.into();
    Arc::new(df)
}

fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new(
            "runtime=TRACE,datafusion-federation=TRACE,datafusion-federation-sql=TRACE",
        ),
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
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
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

    println!("Query: {query}");

    let Ok(explain_plan) = arrow::util::pretty::pretty_format_batches(&plan_results) else {
        panic!("Failed to format plan");
    };
    insta::with_settings!({
        description => format!("Query: {query}"),
        omit_expression => true
    }, {
        insta::assert_snapshot!(snapshot_name, explain_plan);
    });

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = rt
            .datafusion()
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query}` to plan: {e}"))?
            .collect()
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
    let plan_results = rt
        .datafusion()
        .ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
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
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}

async fn runtime_ready_check(rt: &Runtime) {
    assert!(wait_until_true(Duration::from_secs(30), || async { rt.status().is_ready() }).await);
}

async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}
