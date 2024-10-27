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

use crate::embeddings::vector_search;
use crate::model::EmbeddingModelStore;
use crate::model::LLMModelStore;
use crate::{config, datafusion::DataFusion};
use app::App;
use axum::routing::patch;
use model_components::model::Model;
use opentelemetry::KeyValue;
use std::net::SocketAddr;
use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    extract::MatchedPath,
    http::Request,
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post, Router},
    Extension,
};
use tokio::{sync::RwLock, time::Instant};

use super::{metrics, v1};

#[allow(clippy::too_many_arguments)]
pub(crate) fn routes(
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    llms: Arc<RwLock<LLMModelStore>>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
    config: Arc<config::Config>,
    with_metrics: Option<SocketAddr>,
    vector_search: Arc<vector_search::VectorSearch>,
) -> Router {
    let mut router = Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/sql", post(v1::query::post))
        .route("/v1/status", get(v1::status::get))
        .route("/v1/catalogs", get(v1::catalogs::get))
        .route("/v1/datasets", get(v1::datasets::get))
        .route("/v1/datasets/sample", post(v1::datasets::sample))
        .route(
            "/v1/datasets/:name/acceleration/refresh",
            post(v1::datasets::refresh),
        )
        .route(
            "/v1/datasets/:name/acceleration",
            patch(v1::datasets::acceleration),
        )
        .route("/v1/spicepods", get(v1::spicepods::get))
        .route("/v1/ready", get(v1::ready::get))
        .route_layer(middleware::from_fn(track_metrics));

    if cfg!(feature = "models") {
        router = router
            .route("/v1/models", get(v1::models::get))
            .route("/v1/models/:name/predict", get(v1::inference::get))
            .route("/v1/predict", post(v1::inference::post))
            .route("/v1/nsql", post(v1::nsql::post))
            .route("/v1/chat/completions", post(v1::chat::post))
            .route("/v1/embeddings", post(v1::embeddings::post))
            .route("/v1/search", post(v1::search::post))
            .layer(Extension(llms))
            .layer(Extension(models))
            .layer(Extension(vector_search))
            .layer(Extension(embeddings));
    }

    router = router
        .layer(Extension(app))
        .layer(Extension(df))
        .layer(Extension(with_metrics))
        .layer(Extension(config));
    router
}

async fn track_metrics(req: Request<Body>, next: Next) -> impl IntoResponse {
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = next.run(req).await;

    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = response.status().as_u16().to_string();

    let labels = [
        KeyValue::new("method", method.to_string()),
        KeyValue::new("path", path),
        KeyValue::new("status", status),
    ];

    metrics::REQUESTS_TOTAL.add(1, &labels);
    metrics::REQUESTS_DURATION_MS.record(latency_ms, &labels);

    response
}
