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

use crate::datafusion::query::Protocol;
use crate::embeddings::vector_search;
use crate::metrics::telemetry::TelemetryContext;
use crate::Runtime;
use crate::{config, metrics::telemetry::UserAgentCollectionState};

use app::App;
use axum::routing::patch;
use opentelemetry::KeyValue;
use spicepod::component::runtime::{CorsConfig, UserAgentCollection};
use std::sync::Arc;
use tokio::sync::RwLock;

use axum::{
    body::Body,
    extract::MatchedPath,
    http::{HeaderValue, Method, Request},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post, Router},
    Extension,
};
use runtime_auth::layer::http::AuthLayer;
use tokio::time::Instant;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use super::user_agent::UserAgent;
use super::{metrics, v1};

pub(crate) fn routes(
    rt: &Arc<Runtime>,
    config: Arc<config::Config>,
    vector_search: Arc<vector_search::VectorSearch>,
    auth_layer: Option<AuthLayer>,
    cors_config: &CorsConfig,
) -> Router {
    let mut authenticated_router = Router::new()
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
        .route("/v1/spicepods", get(v1::spicepods::get));

    if cfg!(feature = "models") {
        authenticated_router = authenticated_router
            .route("/v1/models", get(v1::models::get))
            .route("/v1/models/:name/predict", get(v1::inference::get))
            .route("/v1/predict", post(v1::inference::post))
            .route("/v1/nsql", post(v1::nsql::post))
            .route("/v1/chat/completions", post(v1::chat::post))
            .route("/v1/embeddings", post(v1::embeddings::post))
            .route("/v1/search", post(v1::search::post))
            .route("/v1/tools", get(v1::tools::list))
            .route("/v1/tool/:name", post(v1::tools::post))
            .layer(Extension(Arc::clone(&rt.llms)))
            .layer(Extension(Arc::clone(&rt.models)))
            .layer(Extension(vector_search))
            .layer(Extension(Arc::clone(&rt.embeds)));
    }

    authenticated_router = authenticated_router
        .layer(Extension(Arc::clone(&rt.app)))
        .layer(Extension(Arc::clone(&rt.df)))
        .layer(Extension(Arc::clone(rt)))
        .layer(Extension(rt.metrics_endpoint))
        .layer(Extension(config));

    // If we have an auth layer, add it to the authenticated router
    if let Some(auth_layer) = auth_layer {
        tracing::info!("Enabled authentication on HTTP routes");
        authenticated_router = authenticated_router.route_layer(auth_layer);
    }

    let unauthenticated_router = Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/ready", get(v1::ready::get))
        .layer(Extension(Arc::clone(&rt.status)));

    unauthenticated_router
        .merge(authenticated_router)
        .route_layer(middleware::from_fn(track_metrics))
        .layer(cors_layer(cors_config))
}

async fn track_metrics(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    headers: http::HeaderMap,
    mut req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let app_lock = app.read().await;
    let user_agent_collection = app_lock
        .as_ref()
        .map_or(UserAgentCollection::default(), |app| {
            app.user_agent_collection()
        });
    let user_agent = match user_agent_collection {
        UserAgentCollection::Full => Arc::new(UserAgent::from_headers(&headers)),
        UserAgentCollection::Disabled => Arc::new(UserAgent::Absent),
    };

    req.extensions_mut().insert(Arc::clone(&user_agent));

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

    let mut labels = vec![
        KeyValue::new("method", method.to_string()),
        KeyValue::new("path", path),
        KeyValue::new("status", status),
    ];

    let user_agent: UserAgent =
        Arc::into_inner(user_agent).unwrap_or_else(|| match user_agent_collection {
            UserAgentCollection::Full => UserAgent::from_headers(&headers),
            UserAgentCollection::Disabled => UserAgent::Absent,
        });
    let telemetry_context = TelemetryContext {
        protocol: Protocol::Http,
        user_agent,
    };
    labels.extend(telemetry_context.to_dimensions());

    metrics::REQUESTS_TOTAL.add(1, &labels);
    metrics::REQUESTS_DURATION_MS.record(latency_ms, &labels);

    response
}

fn cors_layer(cors_config: &CorsConfig) -> CorsLayer {
    // By default, the layer is disabled unless .allow* methods are called.
    let cors = CorsLayer::new();

    if !cors_config.enabled {
        return cors;
    }

    let allowed_origins: AllowOrigin = if cors_config.allowed_origins.contains(&"*".to_string()) {
        Any.into()
    } else {
        cors_config
            .allowed_origins
            .iter()
            .filter_map(|o| HeaderValue::try_from(o).ok())
            .collect::<Vec<HeaderValue>>()
            .into()
    };

    cors.allow_methods([Method::GET, Method::POST, Method::PATCH])
        .allow_origin(allowed_origins)
}
