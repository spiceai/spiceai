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

use bench_search::{
    setup::{load_search_queries, setup_benchmark},
    SearchBenchmarkResultBuilder,
};
use futures::{stream, StreamExt, TryStreamExt};
use runtime::{
    embeddings::vector_search::{self, parse_explicit_primary_keys, SearchRequest, VectorSearch},
    request::{Protocol, RequestContext, UserAgent},
};
use spicepod::component::dataset::acceleration::Acceleration;
use tokio::time::Instant;
use utils::runtime_ready_check;

mod bench_search;
mod utils;

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

    Box::pin(request_context.scope(vector_search_benchmark())).await
}

async fn vector_search_benchmark() -> Result<(), String> {
    let acceleration = Some(Acceleration {
        enabled: true,
        // TODO: temporary limit amout of data to speed up developement/testing. This will be removed in the future.
        refresh_sql: Some("select * from data limit 1000".into()),
        ..Default::default()
    });

    let (rt, mut benchmark_result) = setup_benchmark(
        "Quora_MiniLM-L6-v2_Arrow",
        "QuoraRetrieval",
        "huggingface:huggingface.co/sentence-transformers/all-MiniLM-L6-v2",
        acceleration,
    )
    .await?;

    // wait untill embeddings are created during initial data load
    runtime_ready_check(&rt, Duration::from_secs(5 * 60)).await;

    benchmark_result.finish_index();

    let vsearch = Arc::new(vector_search::VectorSearch::new(
        rt.datafusion(),
        rt.embeds(),
        parse_explicit_primary_keys(rt.app()).await,
    ));

    let test_queries = load_search_queries(&rt).await?;

    let run_search_queries_result =
        run_search_queries(vsearch, test_queries, &mut benchmark_result)
            .await
            .inspect_err(|e| {
                tracing::error!("Search failed: {e}");
            });

    benchmark_result.finish(run_search_queries_result.is_ok());

    tracing::info!("Search benchmark results:\n{}", benchmark_result);

    run_search_queries_result
}

async fn run_search_queries(
    vsearch: Arc<VectorSearch>,
    queries: Vec<String>,
    benchmark_result: &mut SearchBenchmarkResultBuilder,
) -> Result<(), String> {
    let queries_per_chunk = 100;
    let num_chunks_in_parallel = 5;
    let search_limit = 3;

    let query_chunks: Vec<Vec<String>> = queries
        .chunks(queries_per_chunk)
        .map(<[std::string::String]>::to_vec)
        .collect();

    benchmark_result.start_search();

    stream::iter(query_chunks.into_iter().enumerate())
        .map(|(chunk_id, chunk)| {
            let vsearch = Arc::clone(&vsearch);
            async move {
                let mut chunk_latency = Duration::ZERO;
                let mut chunk_completed = 0;

                let mut response_time = Vec::new();

                tracing::info!(
                    "Search chunk {chunk_id}: running {} search queries..",
                    chunk.len()
                );
                for query in chunk {
                    let req = SearchRequest::new(
                        query,
                        Some(vec!["data".to_string()]),
                        search_limit,
                        None,
                        vec!["_id".to_string()],
                    );

                    let start = Instant::now();
                    if let Err(e) = vsearch.search(&req).await {
                        return Err(e.to_string());
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

                Ok(response_time)
            }
        })
        .buffer_unordered(num_chunks_in_parallel)
        .try_collect::<Vec<Vec<f64>>>()
        .await
        .inspect_err(|_| {
            benchmark_result.finish_search();
        })?
        .into_iter()
        .for_each(|response_time| {
            for time in response_time {
                benchmark_result.record_response_time(time);
            }
        });

    benchmark_result.finish_search();

    Ok(())
}
