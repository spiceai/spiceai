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

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use runtime::{dataupdate::DataUpdate, Runtime};

use crate::{
    utils::{get_branch_name, get_commit_sha, init_tracing},
    SearchBenchmarkConfiguration,
};

use app::AppBuilder;
use spicepod::component::{
    dataset::{acceleration::Acceleration, replication::Replication, Dataset, Mode},
    embeddings::{EmbeddingChunkConfig, Embeddings},
    runtime::ResultsCache,
};

use super::SearchBenchmarkResultBuilder;

#[derive(Clone)]
pub(crate) struct Query {
    pub id: String,
    pub text: String,
}

pub(crate) type QueryRelevance = HashMap<String, HashMap<String, i32>>;

pub(crate) async fn setup_benchmark(
    config: &SearchBenchmarkConfiguration,
    upload_results_dataset: Option<&String>,
) -> Result<(Runtime, SearchBenchmarkResultBuilder), String> {
    init_tracing(Some(
        "runtime=DEBUG,task_history=WARN,runtime::embeddings=WARN,INFO",
    ));

    let mut benchmark_result =
        SearchBenchmarkResultBuilder::new(get_commit_sha(), get_branch_name(), config.name);

    let app_builder = build_bench_app(
        config.test_dataset,
        config.embeddings_model,
        config.acceleration.as_ref(),
        config.chunking.as_ref(),
    )
    .await?;

    let app = match upload_results_dataset {
        Some(dataset_path) => app_builder
            .with_dataset(make_spiceai_rw_dataset(
                dataset_path,
                "oss_search_benchmarks",
            ))
            .build(),
        None => app_builder.build(),
    };

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

pub(crate) async fn load_search_queries(rt: &Runtime) -> Result<Vec<Query>, String> {
    let test_queries = rt
        .datafusion()
        .query_builder("SELECT _id as id, text FROM test_query")
        .build()
        .run()
        .await
        .map_err(|e| format!("Failed to retrieve test queries: {e}"))?;

    let records = test_queries
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to retrieve test queries: {e}"))?;

    let queries = extract_queries_from_batches(&records)?;

    let limited_records: Vec<_> = records
        .iter()
        .flat_map(|batch: &RecordBatch| (0..batch.num_rows()).map(move |i| batch.slice(i, 1)))
        .take(5)
        .collect();

    let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)
        .map_err(|e| format!("Failed to format test queries: {e}"))?;

    tracing::info!(
        "Loaded {num_rows} benchmark queries:\n{records_pretty}",
        num_rows = queries.len()
    );

    Ok(queries)
}

pub(crate) async fn load_query_relevance_data(rt: &Runtime) -> Result<QueryRelevance, String> {
    let test_queries = rt
        .datafusion()
        .query_builder(r#"SELECT "query-id", "corpus-id", score FROM test_score"#)
        .build()
        .run()
        .await
        .map_err(|e| format!("Failed to retrieve test scores: {e}"))?;

    let records = test_queries
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| format!("Failed to retrieve test scores: {e}"))?;

    let qrels = extract_query_relevance_from_batches(&records)?;

    tracing::info!(
        "Loaded benchmark query relevance data for {num_rows} queries",
        num_rows = qrels.len()
    );

    Ok(qrels)
}

fn extract_queries_from_batches(records: &[RecordBatch]) -> Result<Vec<Query>, String> {
    let queries = records
        .iter()
        .map(|batch| {
            let id_column = batch
                .column_by_name("id")
                .ok_or_else(|| "Missing 'id' column".to_string())?
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| "Failed to downcast 'id' column to LargeStringArray".to_string())?;

            let text_column = batch
                .column_by_name("text")
                .ok_or_else(|| "Missing 'text' column".to_string())?
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    "Failed to downcast 'text' column to LargeStringArray".to_string()
                })?;

            let queries = (0..batch.num_rows())
                .map(|i| {
                    let id = id_column.value(i).to_string();
                    let text = text_column.value(i).to_string();
                    Ok(Query { id, text })
                })
                .collect::<Result<Vec<Query>, String>>()?;

            Ok(queries)
        })
        .collect::<Result<Vec<Vec<Query>>, String>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<Query>>();

    Ok(queries)
}

fn extract_query_relevance_from_batches(records: &[RecordBatch]) -> Result<QueryRelevance, String> {
    let mut query_relevance = HashMap::new();

    for batch in records {
        let query_id_column = batch
            .column_by_name("query-id")
            .ok_or_else(|| "Missing 'query-id' column".to_string())?
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .ok_or_else(|| {
                "Failed to downcast 'query-id' column to LargeStringArray".to_string()
            })?;

        let corpus_id_column = batch
            .column_by_name("corpus-id")
            .ok_or_else(|| "Missing 'corpus-id' column".to_string())?
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .ok_or_else(|| {
                "Failed to downcast 'corpus-id' column to LargeStringArray".to_string()
            })?;

        let score_column = batch
            .column_by_name("score")
            .ok_or_else(|| "Missing 'score' column".to_string())?
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| "Failed to downcast 'score' column to Int64Array".to_string())?;

        for i in 0..batch.num_rows() {
            let query_id = query_id_column.value(i).to_string();
            let corpus_id = corpus_id_column.value(i).to_string();
            let score = i32::try_from(score_column.value(i))
                .map_err(|e| format!("Failed to convert score to i32: {e}"))?;

            query_relevance
                .entry(query_id)
                .or_insert_with(HashMap::new)
                .insert(corpus_id, score);
        }
    }

    Ok(query_relevance)
}

async fn build_bench_app(
    test_dataset: &str,
    embeddings_model: &str,
    acceleration: Option<&Acceleration>,
    chunking: Option<&EmbeddingChunkConfig>,
) -> Result<AppBuilder, String> {
    let app_builder = AppBuilder::new("vector_search_benchmark_test")
        .with_results_cache(ResultsCache {
            enabled: false,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
        })
        .with_embedding(create_embeddings_model(embeddings_model));

    add_benchmark_dataset(
        app_builder,
        test_dataset,
        acceleration.cloned(),
        chunking.cloned(),
    )
    .await
}

async fn add_benchmark_dataset(
    app_builder: AppBuilder,
    dataset: &str,
    acceleration: Option<Acceleration>,
    chunking: Option<EmbeddingChunkConfig>,
) -> Result<AppBuilder, String> {
    match dataset.to_lowercase().as_str() {
        "quoraretrieval" => {
            super::datasets::add_mtep_quora_retrieval_dataset(app_builder, acceleration, chunking)
                .await
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

fn make_spiceai_rw_dataset(path: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai:{path}"), name.to_string());
    ds.mode = Mode::ReadWrite;
    ds.replication = Some(Replication { enabled: true });
    ds
}

pub(crate) async fn write_benchmark_results(
    benchmark_results: DataUpdate,
    rt: &Runtime,
) -> Result<(), String> {
    rt.datafusion()
        .write_data(
            &TableReference::parse_str("oss_search_benchmarks"),
            benchmark_results,
        )
        .await
        .map_err(|e| e.to_string())
}
