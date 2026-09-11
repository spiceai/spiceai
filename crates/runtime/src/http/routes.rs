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
use crate::status::RuntimeStatus;

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
use http::{
    HeaderValue,
    header::{ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_REQUEST_HEADERS, HeaderName},
};
use opentelemetry::KeyValue;
#[cfg(feature = "mcp")]
use rmcp::transport::streamable_http_server::{
    StreamableHttpService, session::local::LocalSessionManager, tower::StreamableHttpServerConfig,
};
#[cfg(feature = "mcp")]
use runtime_tools::mcp::server::{McpSchemaSnapshot, RuntimeServer};
use spicepod::component::runtime::CorsConfig;
#[cfg(feature = "mcp")]
use spicepod::component::runtime::McpConfig;
use std::borrow::Cow;
use std::sync::Arc;
#[cfg(feature = "mcp")]
use std::sync::{
    RwLock as StdRwLock,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::RwLock;

#[cfg(feature = "openapi")]
use utoipa::{
    OpenApi,
    openapi::{HttpMethod, path::Operation},
};

#[cfg(feature = "dev")]
use utoipa_swagger_ui::SwaggerUi;

use super::response_outcome;
use super::v1;
use runtime_metrics::http as metrics;

use axum::{
    Extension,
    body::Body,
    extract::MatchedPath,
    http::{Method, Request},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{Router, get, post},
};
use runtime_auth::{AuthRequestContext, layer::http::AuthLayer};
use tokio::time::Instant;
use tower_http::cors::{AllowHeaders, AllowOrigin, Any, CorsLayer};
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
        v1::cdc::post,
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
        v1::nsql::get_context,
        v1::nsql::post,
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
                "Legacy-era only (`2025-11-25` and earlier). Session identifier returned on `initialize` and required on subsequent requests in that session. Ignored for `2026-07-28` requests, which are sessionless.",
            ))
            .required(Required::False)
            .build();
        let protocol_version_header = Parameter::builder()
            .name("MCP-Protocol-Version")
            .parameter_in(ParameterIn::Header)
            .description(Some(
                "Required for `2026-07-28` requests. Must match `_meta['io.modelcontextprotocol/protocolVersion']` in the JSON-RPC body. Spice serves `2026-07-28` and remains dual-era for legacy `initialize` clients.",
            ))
            .required(Required::False)
            .build();
        let method_header = Parameter::builder()
            .name("Mcp-Method")
            .parameter_in(ParameterIn::Header)
            .description(Some(
                "Required for `2026-07-28` requests. Must match the JSON-RPC `method` (e.g. `server/discover`, `tools/list`, `tools/call`). Header/body mismatches are rejected with HTTP 400 and JSON-RPC `-32020`.",
            ))
            .required(Required::False)
            .build();
        let name_header = Parameter::builder()
            .name("Mcp-Name")
            .parameter_in(ParameterIn::Header)
            .description(Some(
                "Required for `2026-07-28` `tools/call` (and `resources/read` / `prompts/get`) requests. Must match `params.name` or `params.uri`.",
            ))
            .required(Required::False)
            .build();
        let param_header = Parameter::builder()
            .name("Mcp-Param-*")
            .parameter_in(ParameterIn::Header)
            .description(Some(
                "For `2026-07-28` `tools/call` requests, one header per tool argument annotated with `x-mcp-header`. \
The header name is `Mcp-Param-` plus the annotation value (for example `Mcp-Param-Region`). \
The value must match the corresponding argument in the JSON-RPC body; mismatches are rejected with HTTP 400 and JSON-RPC `-32020`.",
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
Spice is dual-era: `2026-07-28` clients call `server/discover` and tools without a session; \
legacy clients may still `initialize` and use `Mcp-Session-Id`. \
The response is either a single JSON-RPC response (`application/json`) or an SSE stream (`text/event-stream`), \
selected via the `Accept` header.",
                ))
                .parameter(protocol_version_header)
                .parameter(method_header)
                .parameter(name_header)
                .parameter(param_header)
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
                        .description(
                            "Malformed JSON-RPC payload, unsupported protocol version (`-32022` lists supported versions), or Streamable HTTP header/body mismatch (`-32020`).",
                        )
                        .build(),
                )
                .response(
                    "404",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Unknown method, or unknown/expired `Mcp-Session-Id` on a legacy-era request.",
                        )
                        .build(),
                )
                .response(
                    "403",
                    utoipa::openapi::ResponseBuilder::new()
                        .description(
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list, \
or the `Origin` header is not in `runtime.cors.allowed_origins`. \
Configure `runtime.mcp.allowed_hosts` or `runtime.cors.allowed_origins`. Host `[\"*\"]` disables the Host check. Origin `[\"*\"]` expands to localhost defaults — it does not accept every Origin. A concrete Origin list 403s a mismatched `Origin`.",
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
                    "Legacy-era only (`2025-11-25` and earlier). Open a long-lived server-to-client SSE stream for an MCP session created via `POST /v1/mcp`. \
`2026-07-28` clients do not use GET; they POST `subscriptions/listen` instead. \
The `Mcp-Session-Id` header must identify an existing legacy session.",
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
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list, \
or the `Origin` header is not in `runtime.cors.allowed_origins`. Host `[\"*\"]` disables the Host check. Origin `[\"*\"]` expands to localhost defaults — it does not accept every Origin.",
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
                    "Legacy-era only (`2025-11-25` and earlier). Terminate the MCP session identified by the `Mcp-Session-Id` header. \
`2026-07-28` requests are sessionless and do not use DELETE. Subsequent legacy requests bearing the same session id will receive `404 Not Found`.",
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
                            "Forbidden. The `Host` header value is not in the `runtime.mcp.allowed_hosts` list, \
or the `Origin` header is not in `runtime.cors.allowed_origins`. Host `[\"*\"]` disables the Host check. Origin `[\"*\"]` expands to localhost defaults — it does not accept every Origin.",
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
    search: Arc<
        runtime_search::search_engine::SearchEngine<
            crate::search::util::RuntimeTableProviderExplorer,
        >,
    >,
    auth_layer: Option<AuthLayer>,
    cors_config: &CorsConfig,
    metrics_tls: bool,
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
        .route("/v1/datasets/{name}/cdc", post(v1::cdc::post))
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
            .route("/v1/nsql/context", get(v1::nsql::get_context))
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
            .layer(Extension(rt.completion_llms()))
            .layer(Extension(search))
            .layer(Extension(Arc::clone(&rt.embeds)))
            .layer(Extension(Arc::clone(&rt.workers)))
            .layer(Extension(rt.responses_llms()));
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
        // Streamable HTTP transport. Dual-era: 2026-07-28 is sessionless;
        // legacy initialize clients still get sessions via `legacy_session_mode`.
        let runtime_arc = Arc::clone(rt);
        let mcp_cors = mcp_origin_cors(config.as_ref(), cors_config);
        let mcp_config = mcp_server_config(mcp_config, mcp_cors);
        // Shared with tool registration. rmcp caches `get_tool`'s Option per
        // name (including None); the snapshot epoch rebuilds this service so
        // a miss during startup cannot disable `Mcp-Param-*` after the tool
        // appears.
        let schema_snapshot = Arc::clone(&runtime_arc.mcp_schemas);
        let tools = Arc::clone(&runtime_arc.tools);
        let sessions = Arc::new(LocalSessionManager::default());
        let rebuild = {
            let schema_snapshot = Arc::clone(&schema_snapshot);
            let tools = Arc::clone(&tools);
            let sessions = Arc::clone(&sessions);
            move || {
                let schema_snapshot = Arc::clone(&schema_snapshot);
                let tools = Arc::clone(&tools);
                StreamableHttpService::new(
                    move || {
                        Ok(RuntimeServer::with_schema_snapshot(
                            Arc::clone(&tools),
                            Arc::clone(&schema_snapshot),
                        ))
                    },
                    Arc::clone(&sessions),
                    mcp_config.clone(),
                )
            }
        };
        let mcp_service = EpochReloadingMcpService::new(&schema_snapshot, rebuild);

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
        .layer(Extension(v1::status::MetricsTlsEnabled(metrics_tls)))
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

    let router = unauthenticated_router
        .merge(authenticated_router)
        .route_layer(middleware::from_fn(super::mtls::mtls_request_layer))
        .route_layer(middleware::from_fn_with_state(rt.status(), check_shutdown))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&rt.df),
            track_metrics,
        ))
        .layer(Extension(Arc::clone(&rt.app)))
        .layer(cors_layer(cors_config));

    // tower-http 0.6 has no prefix `AllowHeaders` predicate. `cors_layer`
    // mirrors `Access-Control-Request-Headers` so a preflight that asks for
    // `Mcp-Param-*` is not answered with a closed list that omits them. This
    // rewrite then replaces `Access-Control-Allow-Headers` with the closed
    // MCP set plus requested `Mcp-Param-*` names — not every requested header.
    if cors_config.enabled {
        router.layer(middleware::from_fn(allow_mcp_param_cors_headers))
    } else {
        router
    }
}

async fn track_metrics(
    State(df): State<Arc<DataFusion>>,
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    headers: http::HeaderMap,
    mut req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let app = app.read().await.as_ref().map(Arc::clone);
    let mut request_context_builder = RequestContext::builder(Protocol::Http)
        .with_app_opt(app.clone())
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
    let path: Arc<str> = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        Arc::from(matched_path.as_str())
    } else {
        Arc::from(req.uri().path())
    };
    let method = http_method_label(req.method());

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
            let (mut parts, body) = response.into_parts();
            runtime_request_context::attach_trace_id(&mut parts.headers, &request_context);
            let body = axum::body::Body::new(util::cancel_guard_body::CancelGuardBody::new(
                body,
                cancel_guard,
            ));
            axum::response::Response::from_parts(parts, body)
        })
        .await;

    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = http_status_label(response.status().as_u16());

    let mut labels = vec![
        KeyValue::new("method", method),
        KeyValue::new("path", path),
        KeyValue::new("status", status),
    ];

    labels.extend(request_dimensions);

    metrics::REQUESTS_TOTAL.add(1, &labels);
    metrics::REQUESTS.add(1, &labels);
    metrics::REQUESTS_DURATION_MS.record(latency_ms, &labels);

    // The metrics above describe the response *head*. For a streaming response
    // the head is `200 OK` before the first batch exists, so a query that fails
    // partway through would otherwise be counted as a success. Observe the end
    // of the body as well, and report the terminal outcome and the true
    // end-to-end duration under their own instruments so the `status` series
    // keeps its existing meaning.
    let (parts, body) = response.into_parts();
    let body = axum::body::Body::new(response_outcome::OutcomeTrackedBody::new(
        body,
        move |outcome| {
            labels.push(KeyValue::new("outcome", outcome.as_label()));
            metrics::RESPONSES.add(1, &labels);
            metrics::RESPONSES_DURATION_MS.record(start.elapsed().as_secs_f64() * 1000.0, &labels);
        },
    ));

    axum::response::Response::from_parts(parts, body)
}

/// CORS config that feeds the MCP Origin allow-list.
///
/// `Config.runtime`, when present, replaces the whole spicepod runtime
/// (same as `mcp` / `builder.rs`). Hosts and origins both read from that
/// effective runtime so a programmatic CORS override cannot be ignored
/// by the MCP transport.
#[cfg(feature = "mcp")]
fn mcp_origin_cors<'a>(config: &'a config::Config, app_cors: &'a CorsConfig) -> &'a CorsConfig {
    config
        .runtime
        .as_ref()
        .map_or(app_cors, |runtime| &runtime.cors)
}

/// Build the MCP [`StreamableHttpServerConfig`] from `runtime.mcp` and
/// `runtime.cors`.
///
/// Host (`Host` header):
/// - If `runtime.mcp` is not set or `runtime.mcp.allowed_hosts` is `None`, rmcp defaults
///   apply (`localhost`, `127.0.0.1`, `::1`).
/// - If `runtime.mcp.allowed_hosts` contains `"*"`, host checking is disabled entirely.
/// - Otherwise the provided list replaces the defaults entirely.
///
/// Origin (`Origin` header), derived from [`CorsConfig::mcp_allowed_origins`]:
/// - `"*"` or an empty CORS list expands to localhost defaults so the
///   rmcp allow-list is never empty (empty accepts every `Origin`).
/// - A concrete list is the 2026-07-28 Streamable HTTP origin policy:
///   a mismatched `Origin` is 403; a missing `Origin` still passes.
///
/// `legacy_session_mode` stays on so `initialize` clients still get
/// `Mcp-Session-Id` sessions. `2026-07-28` requests are always served
/// statelessly regardless of this flag.
#[cfg(feature = "mcp")]
fn mcp_server_config(
    mcp_config: Option<&McpConfig>,
    cors_config: &CorsConfig,
) -> StreamableHttpServerConfig {
    let config = StreamableHttpServerConfig::default().with_legacy_session_mode(true);
    let config = match mcp_config.and_then(|c| c.allowed_hosts.as_deref()) {
        Some(hosts) if hosts.iter().any(|h| h == "*") => config.disable_allowed_hosts(),
        Some(hosts) => config.with_allowed_hosts(hosts.iter().map(String::as_str)),
        None => config,
    };

    config.with_allowed_origins(cors_config.mcp_allowed_origins())
}

/// Rebuilds [`StreamableHttpService`] when [`McpSchemaSnapshot::epoch`]
/// changes so rmcp cannot keep a cached `get_tool == None` after a tool
/// is registered.
///
/// Tower clones share this state (via [`Arc`]) so concurrent `/v1/mcp`
/// requests cannot fork `inner` and `loaded_epoch`. Reloads recheck the
/// epoch, rebuild, publish the service, and store the counter under the
/// same write lock; splitting those updates lets a stale rebuild overwrite
/// a newer service and then lose the epoch store (`inner=S1`, `loaded=2`).
#[cfg(feature = "mcp")]
struct EpochReloading<T> {
    inner: StdRwLock<T>,
    epoch: Arc<dyn Fn() -> u64 + Send + Sync>,
    loaded_epoch: AtomicU64,
    rebuild: Arc<dyn Fn() -> T + Send + Sync>,
}

#[cfg(feature = "mcp")]
impl<T: Clone> EpochReloading<T> {
    fn new(
        initial_epoch: u64,
        epoch: impl Fn() -> u64 + Send + Sync + 'static,
        rebuild: impl Fn() -> T + Send + Sync + 'static,
    ) -> Self {
        let rebuild = Arc::new(rebuild);
        Self {
            inner: StdRwLock::new(rebuild()),
            epoch: Arc::new(epoch),
            loaded_epoch: AtomicU64::new(initial_epoch),
            rebuild,
        }
    }

    fn current(&self) -> T {
        // Recheck source epoch *after* the read lock. Sampling once, then
        // cloning `inner`, returns the old service when a publish lands in
        // between (`captured_epoch=0 source_epoch=1 loaded_epoch=0
        // returned_service=S0`).
        let epoch = (self.epoch)();
        if self.loaded_epoch.load(Ordering::Acquire) == epoch
            && let Ok(inner) = self.inner.read()
            && self.loaded_epoch.load(Ordering::Acquire) == (self.epoch)()
        {
            return inner.clone();
        }

        match self.inner.write() {
            Ok(mut inner) => {
                let epoch = (self.epoch)();
                if self.loaded_epoch.load(Ordering::Acquire) != epoch {
                    *inner = (self.rebuild)();
                    self.loaded_epoch.store(epoch, Ordering::Release);
                }
                inner.clone()
            }
            Err(_) => (self.rebuild)(),
        }
    }

    #[cfg(test)]
    fn with_initial(
        initial: T,
        initial_epoch: u64,
        epoch: impl Fn() -> u64 + Send + Sync + 'static,
        rebuild: impl Fn() -> T + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: StdRwLock::new(initial),
            epoch: Arc::new(epoch),
            loaded_epoch: AtomicU64::new(initial_epoch),
            rebuild: Arc::new(rebuild),
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> (u64, T) {
        match self.inner.read() {
            Ok(inner) => {
                let loaded = self.loaded_epoch.load(Ordering::Acquire);
                (loaded, inner.clone())
            }
            Err(poisoned) => {
                let inner = poisoned.into_inner();
                let loaded = self.loaded_epoch.load(Ordering::Acquire);
                (loaded, inner.clone())
            }
        }
    }
}

#[cfg(feature = "mcp")]
#[derive(Clone)]
struct EpochReloadingMcpService {
    state: Arc<EpochReloading<StreamableHttpService<RuntimeServer, LocalSessionManager>>>,
}

#[cfg(feature = "mcp")]
impl EpochReloadingMcpService {
    fn new(
        schemas: &Arc<McpSchemaSnapshot>,
        rebuild: impl Fn() -> StreamableHttpService<RuntimeServer, LocalSessionManager>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let epoch_schemas = Arc::clone(schemas);
        Self {
            state: Arc::new(EpochReloading::new(
                schemas.epoch(),
                move || epoch_schemas.epoch(),
                rebuild,
            )),
        }
    }

    fn current(&self) -> StreamableHttpService<RuntimeServer, LocalSessionManager> {
        self.state.current()
    }
}

#[cfg(feature = "mcp")]
impl<B> tower::Service<http::Request<B>> for EpochReloadingMcpService
where
    B: http_body::Body + Send + 'static,
    B::Data: Send + 'static,
    B::Error: std::fmt::Display,
{
    type Response = <StreamableHttpService<RuntimeServer, LocalSessionManager> as tower::Service<
        http::Request<B>,
    >>::Response;
    type Error = std::convert::Infallible;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let service = self.current();
        Box::pin(async move { Ok(service.handle(req).await) })
    }
}

fn cors_layer(cors_config: &CorsConfig) -> CorsLayer {
    // By default, the layer is disabled unless .allow* methods are called.
    let cors = CorsLayer::new();

    if !cors_config.enabled {
        return cors;
    }

    let allowed_origins: AllowOrigin = if cors_config.allowed_origins.iter().any(|o| o == "*") {
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

    cors.allow_methods([
        Method::GET,
        Method::POST,
        Method::PATCH,
        Method::DELETE,
        Method::OPTIONS,
    ])
    // Mirror requested names so `Mcp-Param-*` survives preflight. The outer
    // `allow_mcp_param_cors_headers` layer is the allow-list.
    .allow_headers(AllowHeaders::mirror_request())
    .expose_headers([
        HeaderName::from_static("mcp-session-id"),
        HeaderName::from_static("mcp-protocol-version"),
    ])
    .allow_origin(allowed_origins)
}

/// Closed CORS request-header set, plus any `Mcp-Param-*` name.
const CORS_ALLOWED_HEADERS: &[&str] = &[
    "accept",
    "content-type",
    "authorization",
    "mcp-protocol-version",
    "mcp-method",
    "mcp-name",
    "mcp-session-id",
    "x-api-key",
];

/// Closed-set `Access-Control-Allow-Headers` when a requested name is
/// not a valid [`HeaderValue`]. Must stay aligned with
/// [`CORS_ALLOWED_HEADERS`].
const CORS_ALLOWED_HEADERS_VALUE: HeaderValue = HeaderValue::from_static(
    "accept, content-type, authorization, mcp-protocol-version, mcp-method, mcp-name, mcp-session-id, x-api-key",
);

fn is_allowed_cors_request_header(name: &str) -> bool {
    let lower = name.trim().to_ascii_lowercase();
    CORS_ALLOWED_HEADERS.contains(&lower.as_str()) || lower.starts_with("mcp-param-")
}

/// `Access-Control-Allow-Headers` listing the closed set plus requested `Mcp-Param-*`.
fn cors_allow_headers_value(requested: Option<&str>) -> HeaderValue {
    let mut names: Vec<String> = CORS_ALLOWED_HEADERS
        .iter()
        .map(ToString::to_string)
        .collect();
    if let Some(requested) = requested {
        for name in requested.split(',') {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                continue;
            }
            if is_allowed_cors_request_header(trimmed)
                && !names
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(trimmed))
            {
                names.push(trimmed.to_string());
            }
        }
    }
    HeaderValue::from_str(&names.join(", ")).unwrap_or_else(|_| CORS_ALLOWED_HEADERS_VALUE)
}

async fn allow_mcp_param_cors_headers(req: Request<Body>, next: Next) -> axum::response::Response {
    let requested = req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS).cloned();
    let mut response = next.run(req).await;
    if let Some(requested) = requested {
        let value = cors_allow_headers_value(requested.to_str().ok());
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_HEADERS, value);
    }
    response
}

/// Map common HTTP methods to static metric labels (avoids per-request allocation).
fn http_method_label(method: &Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "OTHER",
    }
}

/// Map common HTTP status codes to static metric labels; rare codes allocate.
fn http_status_label(code: u16) -> Cow<'static, str> {
    match code {
        200 => Cow::Borrowed("200"),
        201 => Cow::Borrowed("201"),
        204 => Cow::Borrowed("204"),
        400 => Cow::Borrowed("400"),
        401 => Cow::Borrowed("401"),
        403 => Cow::Borrowed("403"),
        404 => Cow::Borrowed("404"),
        429 => Cow::Borrowed("429"),
        500 => Cow::Borrowed("500"),
        502 => Cow::Borrowed("502"),
        503 => Cow::Borrowed("503"),
        other => Cow::Owned(other.to_string()),
    }
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

#[cfg(test)]
mod tests {
    use super::{
        Body, CORS_ALLOWED_HEADERS, CORS_ALLOWED_HEADERS_VALUE, CorsConfig,
        allow_mcp_param_cors_headers, cors_allow_headers_value, cors_layer,
        is_allowed_cors_request_header,
    };
    use axum::{Router, middleware, routing::post};
    use http::{
        Method, Request, StatusCode,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_ORIGIN,
            ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION,
            CONTENT_TYPE, HeaderName, ORIGIN,
        },
    };
    use tower::ServiceExt;
    use tower_http::cors::{Any, CorsLayer};

    #[test]
    fn cors_allows_closed_set_and_mcp_param_prefix() {
        assert!(is_allowed_cors_request_header("MCP-Protocol-Version"));
        assert!(is_allowed_cors_request_header("mcp-param-region"));
        assert!(is_allowed_cors_request_header("Mcp-Param-Count"));
        assert!(!is_allowed_cors_request_header("x-evil"));
        assert!(!is_allowed_cors_request_header("cookie"));
    }

    #[test]
    fn cors_allow_headers_includes_requested_mcp_param() {
        let value = cors_allow_headers_value(Some(
            "content-type, mcp-param-region, x-evil, MCP-Param-Count",
        ));
        let allowed = value
            .to_str()
            .expect("allow-headers is ascii")
            .to_ascii_lowercase();
        assert!(allowed.contains("content-type"));
        assert!(allowed.contains("mcp-param-region"));
        assert!(allowed.contains("mcp-param-count"));
        assert!(!allowed.contains("x-evil"));
    }

    #[test]
    fn cors_allow_headers_fallback_matches_closed_set() {
        let closed = cors_allow_headers_value(None);
        assert_eq!(
            closed.to_str().expect("closed allow-headers is ascii"),
            CORS_ALLOWED_HEADERS.join(", "),
            "None requested must emit the closed set"
        );
        assert_eq!(
            closed, CORS_ALLOWED_HEADERS_VALUE,
            "static fallback must stay aligned with CORS_ALLOWED_HEADERS"
        );

        let fallback = cors_allow_headers_value(Some("mcp-param-\0region"));
        assert_eq!(
            fallback, CORS_ALLOWED_HEADERS_VALUE,
            "an invalid requested name must not shrink the allow-list to accept, content-type, authorization"
        );
        let allowed = fallback
            .to_str()
            .expect("fallback allow-headers is ascii")
            .to_ascii_lowercase();
        assert!(allowed.contains("mcp-protocol-version"));
        assert!(allowed.contains("x-api-key"));
        assert!(
            !allowed.contains("mcp-param-"),
            "fallback is the closed set, not the invalid requested name"
        );
    }

    fn enabled_cors() -> CorsConfig {
        CorsConfig {
            enabled: true,
            allowed_origins: vec!["https://app.example.com".to_string()],
        }
    }

    async fn preflight(
        app: Router,
        request_headers: &str,
    ) -> (StatusCode, Option<String>, Option<String>) {
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/v1/mcp")
            .header(ORIGIN, "https://app.example.com")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, request_headers)
            .body(Body::empty())
            .expect("valid CORS preflight");
        let response = app
            .oneshot(request)
            .await
            .expect("preflight should complete");
        let allow_headers = response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_HEADERS)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase);
        let allow_origin = response
            .headers()
            .get(ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        (response.status(), allow_headers, allow_origin)
    }

    #[tokio::test]
    async fn closed_cors_allow_list_omits_mcp_param() {
        // Reproduction of the 2026-07-28 browser preflight failure: a closed
        // `allow_headers` list answers `Access-Control-Allow-Headers` without
        // `mcp-param-region`, so the browser reports it as not allowed.
        let cors = CorsLayer::new()
            .allow_methods([Method::POST, Method::OPTIONS])
            .allow_headers([
                CONTENT_TYPE,
                AUTHORIZATION,
                HeaderName::from_static("mcp-protocol-version"),
                HeaderName::from_static("mcp-method"),
                HeaderName::from_static("mcp-name"),
                HeaderName::from_static("mcp-session-id"),
                HeaderName::from_static("x-api-key"),
            ])
            .allow_origin(Any);
        let app = Router::new()
            .route("/v1/mcp", post(|| async { "ok" }))
            .layer(cors);
        let (status, allow_headers, _) = preflight(app, "content-type, mcp-param-region").await;
        assert_eq!(status, StatusCode::OK);
        let allow_headers =
            allow_headers.expect("CorsLayer always emits Access-Control-Allow-Headers");
        assert!(
            !allow_headers.contains("mcp-param-region"),
            "closed list must omit mcp-param-region (the browser failure): {allow_headers}"
        );
    }

    #[tokio::test]
    async fn cors_preflight_allows_mcp_param_and_rejects_unknown() {
        let cors_config = enabled_cors();
        let app = Router::new()
            .route("/v1/mcp", post(|| async { "ok" }))
            .layer(cors_layer(&cors_config))
            .layer(middleware::from_fn(allow_mcp_param_cors_headers));
        let (status, allow_headers, allow_origin) =
            preflight(app, "content-type, mcp-param-region, mcp-method, x-evil").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            allow_origin.as_deref(),
            Some("https://app.example.com"),
            "preflight must keep Access-Control-Allow-Origin"
        );
        let allow_headers = allow_headers.expect("Access-Control-Allow-Headers after rewrite");
        assert!(
            allow_headers.contains("mcp-param-region"),
            "Mcp-Param-* must be allowed: {allow_headers}"
        );
        assert!(
            allow_headers.contains("mcp-method"),
            "closed MCP headers stay: {allow_headers}"
        );
        assert!(
            !allow_headers.contains("x-evil"),
            "unknown headers must not be mirrored: {allow_headers}"
        );
    }

    #[test]
    fn cors_layer_mirrors_request_headers() {
        let debug = format!("{:?}", cors_layer(&enabled_cors()));
        assert!(
            debug.contains("MirrorRequest"),
            "CorsLayer must mirror Access-Control-Request-Headers so Mcp-Param-* is not dropped, got {debug}"
        );
    }
}

#[cfg(all(test, feature = "mcp"))]
mod epoch_reload_tests {
    use super::EpochReloading;
    use std::sync::{
        Arc, Barrier, RwLock,
        atomic::{AtomicU64, Ordering},
    };
    use std::thread;

    /// Reproduction of the previous `current()` that wrote `inner` and
    /// `loaded_epoch` in separate steps. Epoch 2 publishes first, epoch 1
    /// overwrites `inner`, epoch 2 stores last → `inner=S1`, `loaded=2`,
    /// later calls skip rebuild.
    #[test]
    fn split_reload_updates_can_mark_stale_service_current() {
        let inner = Arc::new(RwLock::new("S0".to_string()));
        let loaded = Arc::new(AtomicU64::new(0));
        let schemas = 2u64;

        let after_rebuild = Arc::new(Barrier::new(2));
        let after_e2_write = Arc::new(Barrier::new(2));
        let after_e1_write = Arc::new(Barrier::new(2));
        let after_e1_store = Arc::new(Barrier::new(2));

        let t1_inner = Arc::clone(&inner);
        let t1_loaded = Arc::clone(&loaded);
        let t1_after_rebuild = Arc::clone(&after_rebuild);
        let t1_after_e2_write = Arc::clone(&after_e2_write);
        let t1_after_e1_write = Arc::clone(&after_e1_write);
        let t1_after_e1_store = Arc::clone(&after_e1_store);
        let h1 = thread::spawn(move || {
            let epoch = 1u64;
            let rebuilt = format!("S{epoch}");
            t1_after_rebuild.wait();
            t1_after_e2_write.wait();
            *t1_inner.write().expect("epoch 1 write lock") = rebuilt.clone();
            t1_after_e1_write.wait();
            t1_loaded.store(epoch, Ordering::Release);
            t1_after_e1_store.wait();
            rebuilt
        });

        let t2_inner = Arc::clone(&inner);
        let t2_loaded = Arc::clone(&loaded);
        let t2_after_rebuild = Arc::clone(&after_rebuild);
        let t2_after_e2_write = Arc::clone(&after_e2_write);
        let t2_after_e1_write = Arc::clone(&after_e1_write);
        let t2_after_e1_store = Arc::clone(&after_e1_store);
        let h2 = thread::spawn(move || {
            let epoch = 2u64;
            let rebuilt = format!("S{epoch}");
            t2_after_rebuild.wait();
            *t2_inner.write().expect("epoch 2 write lock") = rebuilt.clone();
            t2_after_e2_write.wait();
            t2_after_e1_write.wait();
            t2_after_e1_store.wait();
            t2_loaded.store(epoch, Ordering::Release);
            rebuilt
        });

        h1.join().expect("epoch 1 thread");
        h2.join().expect("epoch 2 thread");

        let loaded_v = loaded.load(Ordering::Acquire);
        let inner_v = inner.read().expect("read published service").clone();
        let next_call_rebuilds = loaded_v != schemas;
        assert_eq!(
            (schemas, loaded_v, inner_v.as_str(), next_call_rebuilds),
            (2, 2, "S1", false),
            "split updates retain a stale service as current"
        );
    }

    /// Previous `current()` compared `loaded_epoch` to one sample, then
    /// cloned `inner`. A publish between those steps returns S0 for a
    /// request that should rebuild (`captured_epoch=0 source_epoch=1
    /// loaded_epoch=0 returned_service=S0`).
    #[test]
    fn fast_path_without_post_lock_recheck_returns_stale_service() {
        let loaded = AtomicU64::new(0);
        let source = AtomicU64::new(0);
        let inner = "S0";

        let captured_epoch = source.load(Ordering::Acquire);
        let loaded_epoch = loaded.load(Ordering::Acquire);
        source.store(1, Ordering::Release);
        let returned = if loaded_epoch == captured_epoch {
            inner
        } else {
            "S1"
        };
        assert_eq!(
            (
                captured_epoch,
                source.load(Ordering::Acquire),
                loaded.load(Ordering::Acquire),
                returned
            ),
            (0, 1, 0, "S0"),
            "captured_epoch=0 source_epoch=1 loaded_epoch=0 returned_service=S0"
        );
    }

    /// The first `epoch()` sample matches `loaded_epoch`; the post-lock
    /// sample sees the publish and must rebuild rather than return S0.
    #[test]
    fn epoch_reloading_fast_path_rechecks_source_epoch_after_lock() {
        let samples = Arc::new(AtomicU64::new(0));
        let epoch = Arc::new(AtomicU64::new(0));
        let samples_for_src = Arc::clone(&samples);
        let epoch_for_src = Arc::clone(&epoch);
        let epoch_for_rebuild = Arc::clone(&epoch);
        let reloader = EpochReloading::with_initial(
            "S0".to_string(),
            0,
            move || {
                let n = samples_for_src.fetch_add(1, Ordering::AcqRel);
                if n >= 1 {
                    epoch_for_src.store(1, Ordering::Release);
                }
                epoch_for_src.load(Ordering::Acquire)
            },
            move || format!("S{}", epoch_for_rebuild.load(Ordering::Acquire)),
        );

        let got = reloader.current();
        eprintln!(
            "captured_then_bumped_got={got} snapshot={:?}",
            reloader.snapshot()
        );
        assert_eq!(got, "S1", "post-lock recheck must rebuild, not return S0");
        let (loaded, inner) = reloader.snapshot();
        assert_eq!((loaded, inner.as_str()), (1, "S1"));
    }

    #[test]
    fn epoch_reloading_current_skips_rebuild_when_epoch_matches() {
        let rebuilds = Arc::new(AtomicU64::new(0));
        let rebuilds_for_fn = Arc::clone(&rebuilds);
        let reloader = EpochReloading::new(
            0,
            || 0,
            move || {
                rebuilds_for_fn.fetch_add(1, Ordering::Relaxed);
                "S0".to_string()
            },
        );
        assert_eq!(reloader.current(), "S0");
        assert_eq!(reloader.current(), "S0");
        assert_eq!(
            rebuilds.load(Ordering::Relaxed),
            1,
            "construction rebuilds once; matching epoch must not rebuild again"
        );
    }

    #[test]
    fn epoch_reloading_current_publishes_new_epoch() {
        let epoch = Arc::new(AtomicU64::new(0));
        let epoch_for_src = Arc::clone(&epoch);
        let epoch_for_rebuild = Arc::clone(&epoch);
        let reloader = EpochReloading::new(
            0,
            move || epoch_for_src.load(Ordering::Acquire),
            move || format!("S{}", epoch_for_rebuild.load(Ordering::Acquire)),
        );
        epoch.store(2, Ordering::Release);
        assert_eq!(reloader.current(), "S2");
        let (loaded, inner) = reloader.snapshot();
        assert_eq!(loaded, 2);
        assert_eq!(inner, "S2");
    }

    #[test]
    fn epoch_reloading_overlapping_reloads_cannot_publish_stale_inner() {
        for iteration in 0..64 {
            let epoch = Arc::new(AtomicU64::new(1));
            let epoch_for_src = Arc::clone(&epoch);
            let epoch_for_rebuild = Arc::clone(&epoch);
            let reloader = Arc::new(EpochReloading::with_initial(
                "S0".to_string(),
                0,
                move || epoch_for_src.load(Ordering::Acquire),
                move || {
                    thread::yield_now();
                    format!("S{}", epoch_for_rebuild.load(Ordering::Acquire))
                },
            ));

            let first = Arc::clone(&reloader);
            let first_epoch = Arc::clone(&epoch);
            let h1 = thread::spawn(move || {
                let _ = first.current();
                first_epoch.store(2, Ordering::Release);
                let _ = first.current();
            });
            let second = Arc::clone(&reloader);
            let second_epoch = Arc::clone(&epoch);
            let h2 = thread::spawn(move || {
                second_epoch.store(2, Ordering::Release);
                second.current()
            });

            h1.join().expect("first reloader thread should finish");
            h2.join().expect("second reloader thread should finish");

            let pinned = reloader.current();
            let (loaded, inner) = reloader.snapshot();
            let schema = epoch.load(Ordering::Acquire);
            assert_eq!(schema, 2, "iteration {iteration}: schema settled at 2");
            assert_eq!(
                (loaded, inner.as_str(), pinned.as_str()),
                (2, "S2", "S2"),
                "iteration {iteration}: overlapping reloads must not retain inner=S1 with loaded=2"
            );
        }
    }
}

/// MCP Streamable HTTP `Origin` policy is derived from `runtime.cors`.
#[cfg(all(test, feature = "mcp"))]
mod mcp_origin_tests {
    use super::{
        CorsConfig, LocalSessionManager, McpConfig, StreamableHttpServerConfig,
        StreamableHttpService, mcp_origin_cors, mcp_server_config,
    };
    use crate::config::Config;
    use http::StatusCode;
    use rmcp::{
        ServerHandler,
        model::{ProtocolVersion, ServerCapabilities, ServerInfo},
    };
    use spicepod::component::runtime::Runtime as SpicepodRuntime;
    use std::sync::Arc;

    fn allowlist_cors() -> CorsConfig {
        CorsConfig {
            enabled: true,
            allowed_origins: vec!["https://app.example.com".to_string()],
        }
    }

    #[derive(Clone, Copy)]
    struct OriginCheckServer;

    impl ServerHandler for OriginCheckServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::V_2026_07_28)
        }
    }

    async fn post_mcp_with_origin(
        config: StreamableHttpServerConfig,
        origin: Option<&str>,
    ) -> StatusCode {
        let service = StreamableHttpService::new(
            || Ok(OriginCheckServer),
            Arc::new(LocalSessionManager::default()),
            config.disable_allowed_hosts().with_json_response(true),
        );
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "ping",
                "arguments": {},
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "origin-test",
                        "version": "0.0.0"
                    },
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        });
        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/call")
            .header("mcp-name", "ping");
        if let Some(origin) = origin {
            builder = builder.header(http::header::ORIGIN, origin);
        }
        let request = builder
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .expect("valid MCP POST");
        service.handle(request).await.status()
    }

    #[test]
    fn mcp_server_config_mirrors_cors_allowed_origins() {
        let config = mcp_server_config(None, &allowlist_cors());
        assert_eq!(
            config.allowed_origins,
            vec!["https://app.example.com".to_string()],
            "concrete CORS origins must become rmcp allowed_origins"
        );
    }

    #[test]
    fn mcp_server_config_default_wildcard_expands_to_localhost() {
        let rmcp_default = StreamableHttpServerConfig::default();
        let config = mcp_server_config(None, &CorsConfig::default());
        eprintln!(
            "rmcp_default_allowed_origins_empty={}",
            rmcp_default.allowed_origins.is_empty()
        );
        eprintln!(
            "spice_mcp_config_sets_allowed_origins={}",
            !config.allowed_origins.is_empty()
        );
        assert!(
            rmcp_default.allowed_origins.is_empty(),
            "rmcp default allowed_origins must stay empty: {:?}",
            rmcp_default.allowed_origins
        );
        assert!(
            config
                .allowed_origins
                .iter()
                .any(|o| o == "http://localhost"),
            "default CORS * must install localhost MCP origins, got {:?}",
            config.allowed_origins
        );
    }

    #[test]
    fn mcp_server_config_wildcard_cors_expands_to_localhost() {
        let cors = CorsConfig {
            enabled: true,
            allowed_origins: vec!["*".to_string()],
        };
        let config = mcp_server_config(None, &cors);
        assert!(
            config
                .allowed_origins
                .iter()
                .any(|o| o == "http://localhost"),
            "wildcard CORS must install localhost MCP origins, got {:?}",
            config.allowed_origins
        );
    }

    #[test]
    fn mcp_origin_cors_uses_config_runtime_override() {
        let app_cors = CorsConfig {
            enabled: true,
            allowed_origins: vec!["https://app.example".to_string()],
        };
        let override_runtime = SpicepodRuntime {
            cors: CorsConfig {
                enabled: true,
                allowed_origins: vec!["https://override.example".to_string()],
            },
            mcp: Some(McpConfig {
                allowed_hosts: Some(vec!["override-host".to_string()]),
            }),
            ..SpicepodRuntime::default()
        };
        let config = Config::new().with_spicepod_runtime(override_runtime);
        let cors = mcp_origin_cors(&config, &app_cors);
        eprintln!(
            "effective_mcp_hosts={:?} effective_mcp_origins={:?} override_ignored={}",
            config
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.mcp.as_ref())
                .and_then(|mcp| mcp.allowed_hosts.as_ref()),
            cors.allowed_origins,
            cors.allowed_origins == app_cors.allowed_origins
        );
        assert_eq!(
            cors.allowed_origins,
            vec!["https://override.example".to_string()],
            "MCP Origin must use Config.runtime.cors, not the app spicepod"
        );
        let mcp = mcp_server_config(
            config
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.mcp.as_ref()),
            cors,
        );
        assert_eq!(mcp.allowed_hosts, vec!["override-host".to_string()]);
        assert_eq!(
            mcp.allowed_origins,
            vec!["https://override.example".to_string()]
        );
    }

    #[test]
    fn mcp_origin_cors_falls_back_to_app_without_override() {
        let app_cors = allowlist_cors();
        let config = Config::new();
        let cors = mcp_origin_cors(&config, &app_cors);
        assert_eq!(
            cors.allowed_origins, app_cors.allowed_origins,
            "without Config.runtime, MCP Origin must keep the app CORS list"
        );
    }

    #[test]
    fn mcp_server_config_still_sets_allowed_hosts() {
        let mcp = McpConfig {
            allowed_hosts: Some(vec!["spice-test.local".to_string()]),
        };
        let config = mcp_server_config(Some(&mcp), &CorsConfig::default());
        assert_eq!(
            config.allowed_hosts,
            vec!["spice-test.local".to_string()],
            "Host allow-list must stay independent of the CORS origin policy"
        );
    }

    /// rmcp's empty `allowed_origins` accepts every `Origin`. That is the
    /// pre-fix gap this PR closes for Spice's default CORS `"*"`.
    #[tokio::test]
    async fn unconfigured_rmcp_origin_policy_accepts_any_origin() {
        let config = StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true);
        assert!(
            config.allowed_origins.is_empty(),
            "rmcp default allowed_origins must be empty: {:?}",
            config.allowed_origins
        );
        let status = post_mcp_with_origin(config, Some("https://evil.example")).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "empty allowed_origins is the pre-fix reproduction: Origin https://evil.example was accepted, got {status}"
        );
        eprintln!("unconfigured_rmcp_origin_policy_accepts_any_origin status={status}");
    }

    #[tokio::test]
    async fn default_cors_wildcard_rejects_disallowed_origin_post() {
        let config = mcp_server_config(None, &CorsConfig::default());
        let status = post_mcp_with_origin(config, Some("https://evil.example")).await;
        eprintln!(
            "origin_https_evil_example_accepted={}",
            status != StatusCode::FORBIDDEN
        );
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "default CORS * must 403 Origin https://evil.example, got {status}"
        );
    }

    #[tokio::test]
    async fn default_cors_wildcard_accepts_localhost_origin_post() {
        let config = mcp_server_config(None, &CorsConfig::default());
        let status = post_mcp_with_origin(config, Some("http://localhost:8090")).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "localhost Origin must pass the default CORS * MCP allow-list, got {status}"
        );
    }

    #[tokio::test]
    async fn empty_cors_list_rejects_disallowed_origin_post() {
        let cors = CorsConfig {
            enabled: true,
            allowed_origins: vec![],
        };
        let config = mcp_server_config(None, &cors);
        let status = post_mcp_with_origin(config, Some("https://evil.example")).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "empty CORS list (no *) must still 403 Origin https://evil.example, got {status}"
        );
    }

    #[tokio::test]
    async fn empty_cors_list_accepts_localhost_origin_post() {
        let cors = CorsConfig {
            enabled: true,
            allowed_origins: vec![],
        };
        let config = mcp_server_config(None, &cors);
        let status = post_mcp_with_origin(config, Some("http://localhost:8090")).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "localhost Origin must pass the empty-list MCP allow-list, got {status}"
        );
    }

    #[tokio::test]
    async fn cors_allowlist_rejects_disallowed_origin_post() {
        let config = mcp_server_config(None, &allowlist_cors());
        let status = post_mcp_with_origin(config, Some("https://evil.example")).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "Origin https://evil.example must be 403 when CORS allow-list is https://app.example.com, got {status}"
        );
    }

    #[tokio::test]
    async fn cors_allowlist_accepts_matching_origin_post() {
        let config = mcp_server_config(None, &allowlist_cors());
        let status = post_mcp_with_origin(config, Some("https://app.example.com")).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "matching Origin https://app.example.com must not be 403, got {status}"
        );
    }

    #[tokio::test]
    async fn cors_allowlist_accepts_missing_origin_post() {
        let config = mcp_server_config(None, &allowlist_cors());
        let status = post_mcp_with_origin(config, None).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "non-browser clients that omit Origin must still pass, got {status}"
        );
    }
}
