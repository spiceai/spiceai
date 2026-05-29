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

#![allow(clippy::needless_for_each)]

use crate::datafusion::DataFusion;
use crate::datafusion::request_context_extension::DataFusionContextExtension;
use crate::model::ModelContextLayer;
use crate::request::DatabricksAuthExtension;
use crate::{search::search_engine, status::RuntimeStatus};

use crate::Runtime;
use crate::cluster::ExecutorRegistry;
use crate::config;
#[cfg(feature = "openapi")]
use crate::http::v1::{
    Format,
    datasets::{DatasetFilter, DatasetQueryParams},
};
use runtime_request_context::{Protocol, RequestContext};

use app::App;
use axum::{extract::State, routing::patch};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use opentelemetry::KeyValue;
#[cfg(feature = "mcp")]
use rmcp::transport::streamable_http_server::{
    StreamableHttpService, session::local::LocalSessionManager, tower::StreamableHttpServerConfig,
};
#[cfg(feature = "mcp")]
use runtime_tools::mcp::server::RuntimeServer;
use spicepod::component::runtime::CorsConfig;
#[cfg(feature = "mcp")]
use spicepod::component::runtime::McpConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "openapi")]
use utoipa::{
    OpenApi,
    openapi::{HttpMethod, path::Operation},
};

#[cfg(feature = "dev")]
use utoipa_swagger_ui::SwaggerUi;

use super::{metrics, v1};

use axum::{
    Extension,
    body::Body,
    extract::MatchedPath,
    http::{HeaderValue, Method, Request},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{Router, get, post},
};
use runtime_auth::{AuthRequestContext, layer::http::AuthLayer};
use tokio::time::Instant;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

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
        v1::ready::get,
        v1::status::get,
        v1::spicepods::get,
        v1::embeddings::post,
        v1::search::post,
        v1::chat::post,
        v1::responses::post,
        v1::models::get,
        v1::workers::get,
        v1::nsql::post,
        v1::inference::get,
        v1::inference::post,
        v1::tools::list,
        v1::tools::search,
        v1::tools::post,
        v1::iceberg::get_config,
        v1::iceberg::get_namespaces,
        v1::iceberg::head_namespace,
        v1::iceberg::get_namespace,
        v1::iceberg::list_tables,
        v1::iceberg::tables::head,
        v1::iceberg::tables::get,
        v1::packages::generate,
    ),

    components(schemas(DatasetQueryParams, DatasetFilter, Format)) // These schemas, for some reason, weren't getting picked up.
)]
pub(crate) struct ApiDoc;

/// Returns the `OpenAPI` documentation for the HTTP API. Adds MCP endpoints if the feature is enabled.
#[cfg(feature = "openapi")]
#[must_use]
pub fn get_api_doc() -> utoipa::openapi::OpenApi {
    let mut openai = ApiDoc::openapi();

    #[cfg(feature = "mcp")]
    {
        use utoipa::openapi::{
            Required,
            path::{Parameter, ParameterIn},
        };

        let session_header = Parameter::builder()
            .name("Mcp-Session-Id")
            .parameter_in(ParameterIn::Header)
            .description(Some(
                "Session identifier returned by the server on `initialize` and required on subsequent requests to maintain MCP session continuity.",
            ))
            .required(Required::False)
            .build();

        openai.paths.add_path_operation(
            "/v1/mcp",
            vec![HttpMethod::Post],
            Operation::builder()
                .operation_id(Some("mcp_message"))
                .tag("mcp")
                .summary(Some("Send a Model Context Protocol message"))
                .description(Some(
                    "Send a JSON-RPC message to the Spice MCP server using the MCP Streamable HTTP transport. \
The response is either a single JSON-RPC response (`application/json`) or an SSE stream (`text/event-stream`), \
selected via the `Accept` header. Session continuity is carried via the `Mcp-Session-Id` header.",
                ))
                .parameter(session_header.clone())
                .response(
                    "200",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "JSON-RPC response. Returned as `application/json` for a single response or `text/event-stream` when the server streams additional messages.",
                        )
                        .build(),
                )
                .response(
                    "202",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Message accepted (for JSON-RPC notifications / responses that do not require a reply).",
                        )
                        .build(),
                )
                .response(
                    "400",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("Malformed JSON-RPC payload.")
                        .build(),
                )
                .response(
                    "404",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Unknown or expired `Mcp-Session-Id`.",
                        )
                        .build(),
                )
                .response(
                    "403",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list. \
Configure `runtime.mcp.allowed_hosts` or set it to `[\"*\"]` to allow all hosts.",
                        )
                        .build(),
                )
                .response(
                    "401",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Unauthorized. The `/v1/mcp` endpoint requires `runtime.auth` to be configured. \
Configure an API key provider in your Spicepod and retry with credentials.",
                        )
                        .build(),
                )
                .response(
                    "413",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("Payload too large. Maximum allowed size is 32 MiB.")
                        .build(),
                )
                .build(),
        );
        openai.paths.add_path_operation(
            "/v1/mcp",
            vec![HttpMethod::Get],
            Operation::builder()
                .operation_id(Some("mcp_stream"))
                .tag("mcp")
                .summary(Some("Open an MCP server-to-client SSE stream"))
                .description(Some(
                    "Open a long-lived server-to-client SSE stream for the current MCP session as defined by the Streamable HTTP transport. \
The `Mcp-Session-Id` header must identify an existing session created via `POST /v1/mcp`.",
                ))
                .parameter(session_header.clone())
                .response(
                    "200",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("SSE stream (`text/event-stream`) of server-originated MCP messages.")
                        .build(),
                )
                .response(
                    "404",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("Unknown or expired `Mcp-Session-Id`.")
                        .build(),
                )
                .response(
                    "401",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Unauthorized. The `/v1/mcp` endpoint requires `runtime.auth` to be configured. \
Configure an API key provider in your Spicepod and retry with credentials.",
                        )
                        .build(),
                )
                .response(
                    "403",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list.",
                        )
                        .build(),
                )
                .build(),
        );
        openai.paths.add_path_operation(
            "/v1/mcp",
            vec![HttpMethod::Delete],
            Operation::builder()
                .operation_id(Some("mcp_terminate_session"))
                .tag("mcp")
                .summary(Some("Terminate an MCP Streamable HTTP session"))
                .description(Some(
                    "Terminate the MCP session identified by the `Mcp-Session-Id` header. Subsequent requests bearing the same session id will receive `404 Not Found`.",
                ))
                .parameter(session_header)
                .response(
                    "204",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("Session terminated.")
                        .build(),
                )
                .response(
                    "404",
                    utoipa::openapi::ResponseBuilder::new()
                        .description("Unknown or already-terminated `Mcp-Session-Id`.")
                        .build(),
                )
                .response(
                    "401",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Unauthorized. The `/v1/mcp` endpoint requires `runtime.auth` to be configured. \
Configure an API key provider in your Spicepod and retry with credentials.",
                        )
                        .build(),
                )
                .response(
                    "403",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list.",
                        )
                        .build(),
                )
                .build(),
        );
    }
    openai
}

// Request body size limits to prevent DoS attacks (all limits use binary units: MiB = 1024 * 1024 bytes)
// Applied at three levels:
// 1. DEFAULT_REQUEST_BODY_LIMIT (128 MiB) - for all authenticated endpoints (queries, chat, embeddings)
//    Applied as a route layer to the entire authenticated router to allow reasonable payload sizes for SQL INSERT operations and LLM requests
// 2. MCP_REQUEST_BODY_LIMIT (32 MiB) - for Model Context Protocol (MCP) endpoints
//    Applied to /v1/mcp routes to support MCP message payloads while preventing excessive memory usage
// 3. HEALTH_REQUEST_BODY_LIMIT (128 KiB) - strict limit for unauthenticated endpoints (health checks, ready checks)
//    Applied to unauthenticated routes to prevent DoS via health check endpoints
const DEFAULT_REQUEST_BODY_LIMIT: usize = 128 * 1024 * 1024; // 128 MiB
#[cfg(feature = "mcp")]
const MCP_REQUEST_BODY_LIMIT: usize = 32 * 1024 * 1024; // 32 MiB
const HEALTH_REQUEST_BODY_LIMIT: usize = 128 * 1024; // 128 KiB

pub(crate) fn routes(
    rt: &Arc<Runtime>,
    config: Arc<config::Config>,
    search: Arc<search_engine::SearchEngine>,
    auth_layer: Option<AuthLayer>,
    cors_config: &CorsConfig,
    #[cfg(feature = "mcp")] mcp_config: Option<&McpConfig>,
) -> Router {
    let mut authenticated_router = Router::new()
        .route("/v1/sql", post(v1::query::post).layer(ModelContextLayer))
        .route("/v1/sql/active", get(v1::queries::list_active))
        .route(
            "/v1/sql/{query_id}/cancel",
            post(v1::queries::cancel_active),
        )
        .route("/v1/status", get(v1::status::get))
        .route("/v1/catalogs", get(v1::catalogs::get))
        .route("/v1/functions", get(v1::functions::list))
        .route("/v1/datasets", get(v1::datasets::get))
        .route(
            "/v1/datasets/{name}/acceleration/refresh",
            post(v1::datasets::refresh),
        )
        .route(
            "/v1/datasets/{name}/acceleration",
            patch(v1::datasets::acceleration),
        )
        .route(
            "/v1/datasets/{name}/acceleration/snapshots",
            get(v1::snapshots::list_snapshots),
        )
        .route(
            "/v1/datasets/{name}/acceleration/snapshots/{snapshot_id}",
            get(v1::snapshots::get_snapshot),
        )
        .route(
            "/v1/datasets/{name}/acceleration/snapshots/current",
            post(v1::snapshots::set_current_snapshot),
        )
        .route("/v1/spicepods", get(v1::spicepods::get))
        .route("/v1/packages/generate", post(v1::packages::generate));

    let iceberg_router = Router::new()
        .route("/v1/config", get(v1::iceberg::get_config))
        .route("/v1/namespaces", get(v1::iceberg::get_namespaces))
        .route(
            "/v1/namespaces/{namespace}",
            get(v1::iceberg::get_namespace).head(v1::iceberg::head_namespace),
        )
        .route(
            "/v1/namespaces/{namespace}/tables",
            get(v1::iceberg::list_tables),
        )
        .route(
            "/v1/namespaces/{namespace}/tables/{table}",
            get(v1::iceberg::tables::get).head(v1::iceberg::tables::head),
        );

    authenticated_router = authenticated_router.merge(iceberg_router);

    // Enable Swagger UI & OpenAPI JSON for dev.
    #[cfg(feature = "dev")]
    {
        authenticated_router = authenticated_router
            .merge(SwaggerUi::new("/docs").url("/docs/openapi.json", get_api_doc()));
    }

    if cfg!(feature = "models") {
        // Tool invocation routes require authentication to be configured on the runtime.
        // `/v1/tools/{name}` forwards the raw request body to `tool.call`, which for
        // built-in tools like `sql` and `websearch` is equivalent to arbitrary query /
        // egress. When no `runtime.auth` provider is attached the request would be
        // anonymous, so we refuse these routes at the edge with a 401 rather than
        // relying on each tool to enforce its own safety posture. Configure
        // `runtime.auth.api_key` (or any future provider) to re-enable this surface.
        let tools_auth_required = auth_layer.is_some();
        let tools_auth_message = "Tool invocation (/v1/tools/*) requires `runtime.auth` to be configured. Configure an API key provider in your Spicepod (see https://spiceai.org/docs/reference/runtime#auth) and retry with credentials.";
        let tools_router = Router::new()
            .route("/v1/tools", get(v1::tools::list))
            .route("/v1/tools/search", get(v1::tools::search))
            .route("/v1/tools/{*name}", post(v1::tools::post))
            // Deprecated, use /v1/tools/:name instead
            .route("/v1/tool/{name}", post(v1::tools::post))
            .route_layer(middleware::from_fn(move |req, next| {
                require_auth_configured(tools_auth_required, tools_auth_message, req, next)
            }));

        authenticated_router = authenticated_router
            .route("/v1/models", get(v1::models::get))
            .route("/v1/models/{name}/predict", get(v1::inference::get))
            .route("/v1/predict", post(v1::inference::post))
            .route("/v1/nsql", post(v1::nsql::post).layer(ModelContextLayer))
            .route(
                "/v1/chat/completions",
                post(v1::chat::post).layer(ModelContextLayer),
            )
            .route(
                "/v1/responses",
                post(v1::responses::post).layer(ModelContextLayer),
            )
            .route("/v1/embeddings", post(v1::embeddings::post))
            .route("/v1/search", post(v1::search::post))
            .merge(tools_router)
            .route("/v1/workers", get(v1::workers::get))
            .layer(Extension(Arc::clone(&rt.completion_llms)))
            .layer(Extension(Arc::clone(&rt.models)))
            .layer(Extension(search))
            .layer(Extension(Arc::clone(&rt.embeds)))
            .layer(Extension(Arc::clone(&rt.workers)))
            .layer(Extension(Arc::clone(&rt.responses_llms)));
    }

    // Add async queries API routes - registered unconditionally for discoverability and consistency.
    // Handlers check at runtime if cluster mode with scheduler role is enabled.
    // This design ensures:
    // 1. API endpoints are discoverable via OpenAPI/health checks regardless of cluster mode
    // 2. Helpful 503 errors guide users on how to enable the feature
    // 3. job_executor can be initialized asynchronously after routes are registered
    let queries_router = Router::new()
        .route("/v1/queries", post(v1::queries::submit))
        .route("/v1/queries", get(v1::queries::list))
        .route("/v1/queries/{query_id}", get(v1::queries::get_query))
        .route(
            "/v1/queries/{query_id}/status",
            get(v1::queries::get_status),
        )
        .route(
            "/v1/queries/{query_id}/results",
            get(v1::queries::get_results),
        )
        .route(
            "/v1/queries/{query_id}/results/chunks/{chunk_index}",
            get(v1::queries::get_chunk),
        )
        .route("/v1/queries/{query_id}/cancel", post(v1::queries::cancel));

    authenticated_router = authenticated_router.merge(queries_router);

    #[cfg(feature = "mcp")]
    {
        // Streamable HTTP transport endpoint per MCP 2025-11-25 spec.
        // This replaces the legacy SSE transport that was removed in rmcp 1.x.
        let runtime_arc = Arc::clone(rt);
        let mcp_config = mcp_server_config(mcp_config);
        let mcp_service = StreamableHttpService::new(
            move || Ok(RuntimeServer::new(Arc::clone(&runtime_arc.tools))),
            Arc::new(LocalSessionManager::default()),
            mcp_config,
        );

        tracing::debug!(
            "MCP request body size limit set to {} bytes",
            MCP_REQUEST_BODY_LIMIT
        );
        let mcp_auth_required = auth_layer.is_some();
        let mcp_auth_message = "MCP endpoint (/v1/mcp) requires `runtime.auth` to be configured. Configure an API key provider in your Spicepod (see https://spiceai.org/docs/reference/runtime#auth) and retry with credentials.";
        let mcp_router = Router::new()
            .nest_service("/v1/mcp", mcp_service)
            .route_layer(RequestBodyLimitLayer::new(MCP_REQUEST_BODY_LIMIT))
            .route_layer(middleware::from_fn(move |req, next| {
                require_auth_configured(mcp_auth_required, mcp_auth_message, req, next)
            }));
        authenticated_router = mcp_router.merge(authenticated_router);
    }

    authenticated_router = authenticated_router
        .layer(Extension(Arc::clone(rt)))
        .layer(Extension(rt.metrics_endpoint))
        .layer(Extension(config));

    // Apply request body size limit to prevent DoS attacks via unbounded request payloads
    // This must be applied as a route layer before auth
    authenticated_router =
        authenticated_router.route_layer(RequestBodyLimitLayer::new(DEFAULT_REQUEST_BODY_LIMIT));

    // If we have an auth layer, add it to the authenticated router
    if let Some(auth_layer) = auth_layer {
        tracing::info!("Enabled API key authentication on HTTP routes");
        authenticated_router = authenticated_router.route_layer(auth_layer);
    }

    // mTLS route gate. Wired onto the authenticated router *before* the
    // unauthenticated `/health` and `/v1/ready` are merged in, so probe
    // routes bypass the gate by construction. Under `client_auth: required`
    // the HTTP listener admits no-cert handshakes (so probes work over
    // TLS without mounting a probe certificate); this layer 401s any
    // non-probe request whose connection presented no verified peer cert.
    authenticated_router = authenticated_router
        .route_layer(middleware::from_fn(super::mtls::require_channel_identity));

    // The executor registry only exists when the runtime is in scheduler role; it is `None`
    // otherwise. `/v1/ready`'s executor gating reads it through this layer.
    let executor_registry: Option<Arc<ExecutorRegistry>> = rt.df.executor_registry().cloned();

    let unauthenticated_router = Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/ready", get(v1::ready::get))
        .layer(Extension(Arc::clone(&rt.status)))
        .layer(Extension(executor_registry))
        .route_layer(RequestBodyLimitLayer::new(HEALTH_REQUEST_BODY_LIMIT));

    unauthenticated_router
        .merge(authenticated_router)
        .route_layer(middleware::from_fn(super::mtls::mtls_request_layer))
        .route_layer(middleware::from_fn_with_state(rt.status(), check_shutdown))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&rt.df),
            track_metrics,
        ))
        .layer(Extension(Arc::clone(&rt.app)))
        .layer(cors_layer(cors_config))
}

async fn track_metrics(
    State(df): State<Arc<DataFusion>>,
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    headers: http::HeaderMap,
    mut req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let app_lock = app.read().await;
    let app = app_lock.as_ref().map(Arc::clone);
    let mut request_context_builder = RequestContext::builder(Protocol::Http)
        .with_app_opt(app_lock.as_ref().map(Arc::clone))
        .from_headers(&headers);

    if let Some(ext) = DatabricksAuthExtension::from_headers(&app, &Some(Arc::clone(&df)), &headers)
    {
        request_context_builder = ext.add_from_headers(request_context_builder, &headers);
    }
    let request_context = Arc::new(
        request_context_builder
            .with_extension(DataFusionContextExtension::new(Arc::clone(&df)))
            .build(),
    );
    let auth_request_context: Arc<dyn AuthRequestContext + Send + Sync> =
        Arc::clone(&request_context) as Arc<dyn AuthRequestContext + Send + Sync>;
    req.extensions_mut().insert(auth_request_context);

    let request_dimensions = request_context.to_dimensions();

    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = Arc::clone(&request_context)
        .scope(async move {
            request_context.load_extensions().await;
            // Install a drop guard on the request's cancellation token so
            // that if the response body is dropped before the body completes
            // (for example, the client disconnects while a streaming SQL or
            // SSE response is being produced), the cancellation token fires
            // and any cooperating in-flight query terminates promptly.
            //
            // The guard is attached to the response body via
            // `CancelGuardBody`, which disarms the guard once the body
            // signals end-of-stream. This means the guard's lifetime tracks
            // the streaming response, not just the response future.
            let cancel_guard = request_context.cancellation_token().clone().drop_guard();
            let response = next.run(req).await;
            let (parts, body) = response.into_parts();
            let body = axum::body::Body::new(util::cancel_guard_body::CancelGuardBody::new(
                body,
                cancel_guard,
            ));
            axum::response::Response::from_parts(parts, body)
        })
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

/// Build the MCP [`StreamableHttpServerConfig`] from the optional `runtime.mcp` config.
///
/// - If `runtime.mcp` is not set or `runtime.mcp.allowed_hosts` is `None`, rmcp defaults
///   apply (`localhost`, `127.0.0.1`, `::1`).
/// - If `runtime.mcp.allowed_hosts` contains `"*"`, host checking is disabled entirely
///   (matches how `runtime.cors.allowed_origins: ["*"]` works).
/// - Otherwise the provided list replaces the defaults entirely.
#[cfg(feature = "mcp")]
fn mcp_server_config(mcp_config: Option<&McpConfig>) -> StreamableHttpServerConfig {
    let config = StreamableHttpServerConfig::default();
    match mcp_config.and_then(|c| c.allowed_hosts.as_deref()) {
        Some(hosts) if hosts.iter().any(|h| h == "*") => config.disable_allowed_hosts(),
        Some(hosts) => config.with_allowed_hosts(hosts.iter().map(String::as_str)),
        None => config,
    }
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

    cors.allow_methods([Method::GET, Method::POST, Method::PATCH, Method::OPTIONS])
        .allow_headers([ACCEPT, CONTENT_TYPE, AUTHORIZATION])
        .allow_origin(allowed_origins)
}

async fn check_shutdown(
    State(status): State<Arc<RuntimeStatus>>,
    req: axum::http::Request<Body>,
    next: Next,
) -> impl IntoResponse {
    // Allow /health to bypass shutdown check
    if req.uri().path() == "/health" {
        return next.run(req).await;
    }

    if status.is_shutdown() {
        return (
            http::StatusCode::SERVICE_UNAVAILABLE,
            "Runtime is shutting down",
        )
            .into_response();
    }

    next.run(req).await
}

/// Reject a request with 401 unless the runtime has an authentication provider attached.
///
/// Used to gate routes whose behavior is unsafe anonymously (`/v1/tools/*`: the raw
/// request body is handed to `tool.call`, which for built-ins like `sql` and
/// `websearch` is equivalent to arbitrary query / outbound fetch).
async fn require_auth_configured(
    auth_configured: bool,
    message: &'static str,
    req: axum::http::Request<Body>,
    next: Next,
) -> axum::response::Response {
    if auth_configured {
        return next.run(req).await;
    }

    (
        http::StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({
            "message": message
        })),
    )
        .into_response()
}
