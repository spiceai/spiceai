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

use runtime::Runtime;

use crate::utils::init_tracing;

use app::AppBuilder;
use spicepod::component::{
    dataset::acceleration::Acceleration, embeddings::Embeddings, runtime::ResultsCache,
};

pub(crate) async fn setup_benchmark(
    test_dataset: &str,
    embeddings_model: &str,
    acceleration: Option<Acceleration>,
) -> Result<Runtime, String> {
    init_tracing();

    let app = build_bench_app(test_dataset, embeddings_model, acceleration)
        .await?
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(5 * 60)) => {
            panic!("Timed out waiting for datasets to load in setup_benchmark()");
        }
        () = rt.load_components() => {}
    }

    Ok(rt)
}

async fn build_bench_app(
    test_dataset: &str,
    embeddings_model: &str,
    acceleration: Option<Acceleration>,
) -> Result<AppBuilder, String> {
    let app_builder = AppBuilder::new("vector_search_benchmark_test")
        .with_results_cache(ResultsCache {
            enabled: false,
            cache_max_size: None,
            item_ttl: None,
            eviction_policy: None,
        })
        .with_embedding(Embeddings::new(embeddings_model, "test_model"));

    add_benchmark_dataset(app_builder, test_dataset, acceleration).await
}

async fn add_benchmark_dataset(
    app_builder: AppBuilder,
    dataset: &str,
    acceleration: Option<Acceleration>,
) -> Result<AppBuilder, String> {
    match dataset {
        "QuoraRetrieval" => {
            super::datasets::add_mtep_quora_retrieval_dataset(app_builder, acceleration).await
        }
        _ => Err(format!("Unknown benchmark dataset: {dataset}")),
    }
}
