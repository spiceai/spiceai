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

use crate::{config, datafusion::DataFusion, model::Model};
use app::App;
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

use super::v1;

pub(crate) fn routes(
    app: Arc<RwLock<Option<App>>>,
    df: Arc<RwLock<DataFusion>>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    config: Arc<config::Config>,
    with_metrics: Option<SocketAddr>,
) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/sql", post(v1::query::post))
        .route("/v1/status", get(v1::status::get))
        .route("/v1/datasets", get(v1::datasets::get))
        .route("/v1/datasets/:name/refresh", post(v1::datasets::refresh))
        .route("/v1/spicepods", get(v1::spicepods::get))
        .route("/v1/models", get(v1::models::get))
        .route("/v1/models/:name/predict", get(v1::inference::get))
        .route("/v1/predict", post(v1::inference::post))
        .route_layer(middleware::from_fn(track_metrics))
        .layer(Extension(app))
        .layer(Extension(df))
        .layer(Extension(with_metrics))
        .layer(Extension(models))
        .layer(Extension(config))
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

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status),
    ];

    metrics::counter!("http_requests_total", &labels).increment(1);
    metrics::histogram!("http_requests_duration_seconds", &labels).record(latency);

    response
}
