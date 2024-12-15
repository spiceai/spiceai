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

use std::{collections::HashMap, sync::Arc, time::Duration};

use bench_search::{
    setup::{load_query_relevance_data, load_search_queries, setup_benchmark, Query},
    SearchBenchmarkResultBuilder,
};
use clap::Parser;
use futures::{stream, StreamExt, TryStreamExt};
use runtime::{
    embeddings::vector_search::{
        self, parse_explicit_primary_keys, SearchRequest, VectorSearch, VectorSearchResult,
    },
    request::{Protocol, RequestContext, UserAgent},
};
use spicepod::component::dataset::acceleration::Acceleration;
use tokio::time::Instant;
use utils::runtime_ready_check;

mod bench_search;
mod utils;

// Define command line arguments for running benchmark test
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct BenchArgs {
    /// Run the benchmark
    #[arg(long)]
    bench: bool,

    /// Sets the configuration to run benchmark test on
    #[arg(short, long)]
    configuration: Option<String>,
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

    Box::pin(request_context.scope(vector_search_benchmarks())).await
}

pub struct SearchBenchmarkConfiguration {
    pub name: &'static str,
    pub test_dataset: &'static str,
    pub embeddings_model: &'static str,
    pub acceleration: Option<Acceleration>,
}

fn benchmark_configurations() -> Vec<SearchBenchmarkConfiguration> {
    // TODO: expand configurations with DuckDB acceleration after issue below is resolved
    // https://github.com/spiceai/spiceai/issues/3796

    let args = BenchArgs::parse();

    vec![
        SearchBenchmarkConfiguration {
            name: "quora_minilm-l6-v2_arrow",
            test_dataset: "QuoraRetrieval",
            embeddings_model: "huggingface:huggingface.co/sentence-transformers/all-MiniLM-L6-v2",
            acceleration: Some(Acceleration {
                enabled: true,
                // TODO: temporary limit amout of data to speed up developement/testing. This will be removed in the future.
                refresh_sql: Some("select * from data limit 20000".into()),
                ..Default::default()
            }),
        },
        SearchBenchmarkConfiguration {
            name: "quora_openai-text-embedding-3-small_arrow",
            test_dataset: "QuoraRetrieval",
            embeddings_model: "openai:text-embedding-3-small",
            acceleration: Some(Acceleration {
                enabled: true,
                ..Default::default()
            }),
        },
    ]
    .into_iter()
    .filter(|x| match &args.configuration {
        Some(config) => x.name.to_lowercase() == config.to_lowercase(),
        None => true,
    })
    .collect()
}

async fn vector_search_benchmarks() -> Result<(), String> {
    let benchmark_configurations = benchmark_configurations();

    if benchmark_configurations.is_empty() {
        return Err("No benchmarks to run: the configuration list is empty.".to_string());
    }

    for config in benchmark_configurations {
        let _ = run_benchmark(&config).await;
    }

    Ok(())
}

macro_rules! handle_error {
    ($result:expr, $benchmark_result:expr) => {
        $result.inspect_err(|e| {
            $benchmark_result.finish(false);
            tracing::error!(
                "Benchmark for configuration '{}' failed: {e}\n{}",
                $benchmark_result.configuration_name(),
                $benchmark_result
            );
        })
    };
}

async fn run_benchmark(config: &SearchBenchmarkConfiguration) -> Result<(), String> {
    let (rt, mut benchmark_result) = setup_benchmark(
        config.name,
        config.test_dataset,
        config.embeddings_model,
        config.acceleration.as_ref(),
    )
    .await?;

    tracing::info!("Loading test corpus... Warning: This might take a while!");

    // wait untill embeddings are created during initial data load
    runtime_ready_check(&rt, Duration::from_secs(60 * 60)).await;

    benchmark_result.finish_index();

    let vsearch = Arc::new(vector_search::VectorSearch::new(
        rt.datafusion(),
        rt.embeds(),
        parse_explicit_primary_keys(rt.app()).await,
    ));

    let test_queries = load_search_queries(&rt).await?;

    tracing::info!("Running search queries");

    let search_result = handle_error!(
        run_search_queries(vsearch, test_queries, &mut benchmark_result).await,
        benchmark_result
    )?;

    tracing::info!("Search completed, evaluating results");

    let qrels = handle_error!(load_query_relevance_data(&rt).await, benchmark_result)?;

    let search_score = bench_search::evaluator::evaluate(&qrels, &search_result);
    benchmark_result.record_score(search_score);

    benchmark_result.finish(true);

    tracing::info!(
        "Benchmark for configuration '{}' completed:\n{benchmark_result}",
        benchmark_result.configuration_name()
    );

    Ok(())
}

async fn run_search_queries(
    vsearch: Arc<VectorSearch>,
    queries: Vec<Query>,
    benchmark_result: &mut SearchBenchmarkResultBuilder,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    let queries_per_chunk = 100;
    let num_chunks_in_parallel = 5;
    let search_limit = 10;

    let query_chunks: Vec<Vec<Query>> = queries
        .chunks(queries_per_chunk)
        .map(<[bench_search::setup::Query]>::to_vec)
        .collect();

    let mut result: HashMap<String, HashMap<String, f64>> = HashMap::new();

    benchmark_result.start_search();

    stream::iter(query_chunks.into_iter().enumerate())
        .map(|(chunk_id, chunk)| {
            let vsearch = Arc::clone(&vsearch);
            async move {
                let mut chunk_latency = Duration::ZERO;
                let mut chunk_completed = 0;

                let mut response_time = Vec::new();
                let mut scores: Vec<(String, HashMap<String, f64>)> = Vec::new();

                tracing::info!(
                    "Search chunk {chunk_id}: running {} search queries..",
                    chunk.len()
                );
                for query in chunk {
                    let req = SearchRequest::new(
                        query.text,
                        Some(vec!["data".to_string()]),
                        search_limit,
                        None,
                        vec!["_id".to_string()],
                    );

                    let start = Instant::now();
                    match vsearch.search(&req).await {
                        Ok(search_res) => {
                            scores.push((query.id.clone(), to_search_result(&search_res)?));
                        }
                        Err(e) => return Err(e.to_string()),
                    }

                    let duration = start.elapsed();
                    response_time.push(duration.as_secs_f64() * 1000.0);
                    chunk_latency += duration;
                    chunk_completed += 1;
                }

                tracing::info!(
                    "Search chunk {chunk_id}: completed {chunk_completed} queries in {} ms",
                    chunk_latency.as_millis()
                );

                Ok((response_time, scores))
            }
        })
        .buffer_unordered(num_chunks_in_parallel)
        .try_collect::<Vec<(Vec<f64>, Vec<(String, HashMap<String, f64>)>)>>()
        .await
        .inspect_err(|_| {
            benchmark_result.finish_search();
        })?
        .into_iter()
        .for_each(|(response_time, scores)| {
            for time in response_time {
                benchmark_result.record_response_time(time);
            }

            result.extend(scores);
        });

    benchmark_result.finish_search();

    Ok(result)
}

fn to_search_result(result: &VectorSearchResult) -> Result<HashMap<String, f64>, String> {
    let mut output = HashMap::new();

    for (table_ref, value) in result {
        match value.to_matches(table_ref) {
            Ok(matches) => {
                for m in matches {
                    let id = m.metadata().get("_id").ok_or_else(|| {
                        "Missing '_id' key value in search result metadata".to_string()
                    })?;

                    let id = id
                        .as_str()
                        .ok_or_else(|| "Failed to convert '_id' key value to string".to_string())?;

                    output.insert(id.to_string(), m.score());
                }
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }

    Ok(output)
}
