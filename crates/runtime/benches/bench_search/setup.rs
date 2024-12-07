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
use futures::TryStreamExt;
use runtime::Runtime;

use crate::utils::{get_branch_name, get_commit_sha, init_tracing};

use app::AppBuilder;
use spicepod::component::{
    dataset::acceleration::Acceleration, embeddings::Embeddings, runtime::ResultsCache,
};

use super::SearchBenchmarkResultBuilder;

pub(crate) async fn setup_benchmark(
    configuration_name: &str,
    test_dataset: &str,
    embeddings_model: &str,
    acceleration: &Option<Acceleration>,
) -> Result<(Runtime, SearchBenchmarkResultBuilder), String> {
    init_tracing(Some(
        "runtime=Debug,task_history=WARN,runtime::embeddings=WARN,INFO",
    ));

    let mut benchmark_result =
        SearchBenchmarkResultBuilder::new(get_commit_sha(), get_branch_name(), configuration_name);

    let app = build_bench_app(test_dataset, embeddings_model, acceleration)
        .await?
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // include embeddings initial loading time to indexing time
    benchmark_result.start_index();

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(5 * 60)) => {
            panic!("Timed out waiting for datasets to load in setup_benchmark()");
        }
        () = rt.load_components() => {}
    }

    Ok((rt, benchmark_result))
}

pub(crate) async fn load_search_queries(rt: &Runtime) -> Result<Vec<String>, String> {
    let test_queries = rt
        .datafusion()
        .query_builder("SELECT q._id, q.text, ARRAY_AGG(t.\"corpus-id\") AS corpus_ids FROM tests t JOIN test_query q ON t.\"query-id\" = q._id GROUP BY q._id, q.text ORDER BY q._id DESC")
        .build()
        .run()
        .await
        .map_err(|e| format!("Failed to retrieve test queries: {e}"))?;

    let records = test_queries
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to retrieve test queries: {e}"))?;

    let queries = extract_queries_from_batches(&records, 1)?;

    let limited_records: Vec<_> = records
        .iter()
        .flat_map(|batch: &RecordBatch| (0..batch.num_rows()).map(move |i| batch.slice(i, 1)))
        .take(10)
        .collect();

    let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)
        .map_err(|e| format!("Failed to format test queries: {e}"))?;

    tracing::info!(
        "Loaded {num_rows} benchmark queries:\n{records_pretty}",
        num_rows = queries.len()
    );

    Ok(queries)
}

async fn build_bench_app(
    test_dataset: &str,
    embeddings_model: &str,
    acceleration: &Option<Acceleration>,
) -> Result<AppBuilder, String> {
    let app_builder = AppBuilder::new("vector_search_benchmark_test")
        .with_results_cache(ResultsCache {
            enabled: false,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
        })
        .with_embedding(create_embeddings_model(embeddings_model));

    add_benchmark_dataset(app_builder, test_dataset, acceleration.clone()).await
}

async fn add_benchmark_dataset(
    app_builder: AppBuilder,
    dataset: &str,
    acceleration: Option<Acceleration>,
) -> Result<AppBuilder, String> {
    match dataset.to_lowercase().as_str() {
        "quoraretrieval" => {
            super::datasets::add_mtep_quora_retrieval_dataset(app_builder, acceleration).await
        }
        _ => Err(format!("Unknown benchmark dataset: {dataset}")),
    }
}

fn create_embeddings_model(embeddings_model: &str) -> Embeddings {
    let mut model = Embeddings::new(embeddings_model, "test_model");
    // Add OpenAI API key as a secret; HF models will ignore it
    model.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    model
}

fn extract_queries_from_batches(
    records: &[RecordBatch],
    column_index: usize,
) -> Result<Vec<String>, String> {
    let queries = records
        .iter()
        .map(|batch| {
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .ok_or_else(|| {
                    "Failed to downcast query text column to StringViewArray".to_string()
                })?;

            let queries = (0..batch.num_rows())
                .map(|i| Ok(column.value(i).to_string()))
                .collect::<Result<Vec<String>, String>>()?;

            Ok(queries)
        })
        .collect::<Result<Vec<Vec<String>>, String>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<String>>();

    Ok(queries)
}
