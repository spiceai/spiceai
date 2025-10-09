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

use arrow::array::RecordBatch;
use futures::TryStreamExt;

use runtime::Runtime;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

#[cfg(all(feature = "databricks", feature = "delta_lake"))]
mod aws_sdk;
mod utils;

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

async fn run_query_and_check_results<F>(
    rt: &mut Runtime,
    query: &str,
    validate_result: Option<F>,
) -> Result<(), anyhow::Error>
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
        .map_err(|e| anyhow::anyhow!("query `{query}` to plan: {e}"))?;

    let plan_results: Vec<RecordBatch> = query_results
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::anyhow!("query `{query}` to results: {e}"))?;

    println!("Query: {query}");

    assert!(
        arrow::util::pretty::pretty_format_batches(&plan_results).is_ok(),
        "Failed to format plan"
    );

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = rt
            .datafusion()
            .query_builder(query)
            .build()
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("query `{query}` failed to run: {e}"))?
            .data
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}
