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

use crate::embeddings::vector_search;

#[cfg(feature = "openapi")]
use crate::http::v1::{
    datasets::{DatasetFilter, DatasetQueryParams},
    Format,
};
use crate::request::Protocol;
use crate::Runtime;
use crate::{config, request::RequestContext};

use app::App;
use axum::routing::patch;
use opentelemetry::KeyValue;
use spicepod::component::runtime::CorsConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "openapi")]
use utoipa::OpenApi;

#[cfg(feature = "dev")]
use utoipa_swagger_ui::SwaggerUi;

use super::{metrics, v1};
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

#[cfg(feature = "openapi")]
#[derive(OpenApi)]
#[openapi(
    servers(
        (url = "http://localhost:8090", description = "Local development server. Configure with `--http`."),
    ),
    security(
        ("api_key" = [])
    ),
    paths(
        // Order here will be preserved in sidebar at https://spiceai.org/docs/api/http/runtime.
        v1::query::post,
        v1::datasets::get,
        v1::datasets::acceleration,
        v1::datasets::refresh,
        v1::catalogs::get,
        v1::iceberg::get_config,
        v1::iceberg::get_namespaces,
        v1::iceberg::head_namespace,
        v1::ready::get,
        v1::status::get,
        v1::spicepods::get,
        v1::embeddings::post,
        v1::search::post,
        v1::chat::post,
        v1::models::get,
        v1::nsql::post,
        v1::eval::list,
        v1::eval::post,
        v1::inference::get,
        v1::inference::post,
        v1::tools::list,
        v1::tools::post,
        v1::packages::generate,
    ),

    components(schemas(DatasetQueryParams, DatasetFilter, Format)) // These schemas, for some reason, weren't getting picked up.
)]
pub struct ApiDoc;

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
        .route(
            "/v1/datasets/:name/acceleration/refresh",
            post(v1::datasets::refresh),
        )
        .route(
            "/v1/datasets/:name/acceleration",
            patch(v1::datasets::acceleration),
        )
        .route("/v1/spicepods", get(v1::spicepods::get))
        .route("/v1/packages/generate", post(v1::packages::generate));

    let iceberg_router = Router::new()
        .route("/v1/config", get(v1::iceberg::get_config))
        .route("/v1/namespaces", get(v1::iceberg::get_namespaces))
        .route(
            "/v1/namespaces/:namespace",
            get(v1::iceberg::get_namespace).head(v1::iceberg::head_namespace),
        )
        .route(
            "/v1/namespaces/:namespace/tables",
            get(v1::iceberg::list_tables),
        )
        .route(
            "/v1/namespaces/:namespace/tables/:table",
            get(v1::iceberg::tables::get).head(v1::iceberg::tables::head),
        );

    authenticated_router = authenticated_router.merge(iceberg_router);

    // Enable Swagger UI & OpenAPI JSON for dev.
    #[cfg(feature = "dev")]
    {
        authenticated_router = authenticated_router
            .merge(SwaggerUi::new("/docs").url("/docs/openapi.json", ApiDoc::openapi()));
    }

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
            .route("/v1/tools/:name", post(v1::tools::post))
            // Deprecated, use /v1/evals/:name instead
            .route("/v1/tool/:name", post(v1::tools::post))
            .route("/v1/evals/:name", post(v1::eval::post))
            .route("/v1/evals/", get(v1::eval::list))
            .layer(Extension(Arc::clone(&rt.llms)))
            .layer(Extension(Arc::clone(&rt.models)))
            .layer(Extension(Arc::clone(&rt.eval_scorers)))
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
        .layer(Extension(Arc::clone(&rt.app)))
        .layer(cors_layer(cors_config))
}

async fn track_metrics(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    headers: http::HeaderMap,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let app_lock = app.read().await;
    let request_context = Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_app_opt(app_lock.as_ref().map(Arc::clone))
            .from_headers(&headers)
            .build(),
    );

    let request_dimensions = request_context.to_dimensions();

    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = Arc::clone(&request_context)
        .scope(async move { next.run(req).await })
        .await;

    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = response.status().as_u16().to_string();

    let mut labels = vec![
        KeyValue::new("method", method.to_string()),
        KeyValue::new("path", path),
        KeyValue::new("status", status),
    ];

    labels.extend(request_dimensions.into_iter());

    metrics::REQUESTS_TOTAL.add(1, &labels);
    metrics::REQUESTS.add(1, &labels);
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

    tracing::info!(
        target: "runtime::http",
        "CORS (Cross-Origin Resource Sharing) enabled on HTTP endpoint for allowed origins: {:?}",
        cors_config.allowed_origins
    );

    cors.allow_methods([Method::GET, Method::POST, Method::PATCH])
        .allow_origin(allowed_origins)
}
