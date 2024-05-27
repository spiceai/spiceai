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

use arrow::array::RecordBatch;
use datafusion::{
    assert_batches_eq, parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use runtime::Runtime;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

mod docker;
// Run all tests in the `federation` module
mod federation;
mod refresh_sql;

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
    query: &str,
    expected_plan: &[&str],
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
    let df = &mut *rt.df.write().await;

    // Check the plan
    let plan_results = df
        .ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    assert_batches_eq!(expected_plan, &plan_results);

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = df
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
