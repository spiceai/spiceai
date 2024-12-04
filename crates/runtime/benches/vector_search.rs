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

use bench_search::setup::setup_benchmark;
use runtime::request::{Protocol, RequestContext, UserAgent};
use spicepod::component::dataset::acceleration::Acceleration;
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
        refresh_sql: Some("select * from data limit 5000".into()),
        ..Default::default()
    });

    let rt = setup_benchmark(
        "QuoraRetrieval",
        "huggingface:huggingface.co/sentence-transformers/all-MiniLM-L6-v2",
        acceleration,
    )
    .await?;

    // wait untill embeddings are created during initial data load
    runtime_ready_check(&rt, Duration::from_secs(5 * 60)).await;

    Ok(())
}
