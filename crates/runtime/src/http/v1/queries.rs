/*
Copyright 2026 The Spice.ai OSS Authors

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

//! HTTP API endpoints for async SQL queries.
//!
//! Async query API:
//! - `POST /v1/queries` - Submit a new query for async execution
//! - `GET /v1/queries/{query_id}` - Get query status and first result chunk
//! - `GET /v1/queries/{query_id}/status` - Get query status only
//! - `GET /v1/queries/{query_id}/results` - Get full results (with pagination)
//! - `GET /v1/queries/{query_id}/results/chunks/{chunk_index}` - Get a specific result chunk
//! - `POST /v1/queries/{query_id}/cancel` - Cancel a running async query
//! - `GET /v1/queries` - List all queries
//! - `GET /v1/sql/active` - List active synchronous queries
//! - `POST /v1/sql/{query_id}/cancel` - Cancel an active synchronous query

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::Runtime;
use crate::config::ClusterRole;
use crate::datafusion::query::write_to_json_value;
use crate::jobs::{JobErrorCode, JobExecutor, JobState, JobStatus};

/// Check if cluster mode with scheduler role is enabled.
/// Returns 503 error response if not in scheduler cluster mode.
#[expect(
    clippy::result_large_err,
    reason = "Response type is needed for HTTP error responses"
)]
fn require_cluster_mode(rt: &Arc<Runtime>) -> Result<(), Response> {
    if rt.df.cluster_config.effective_role() != Some(ClusterRole::Scheduler) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Async queries API requires distributed mode with `runtime.scheduler.state_location` configured. Start with: spiced --role scheduler"
            })),
        )
            .into_response());
    }
    Ok(())
}

/// Helper to get job executor from runtime.
/// Requires cluster mode to be enabled. Returns 503 if executor is not available yet.
#[expect(
    clippy::result_large_err,
    reason = "Response type is needed for HTTP error responses"
)]
fn get_executor(rt: &Arc<Runtime>) -> Result<Arc<JobExecutor>, Response> {
    require_cluster_mode(rt)?;

    rt.job_executor().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Queries API is initializing. Please retry shortly."
            })),
        )
            .into_response()
    })
}

/// Request body for submitting a new query.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SubmitQueryRequest {
    /// The SQL statement to execute.
    pub sql: String,
    /// Optional query parameters (bind variables).
    #[serde(default)]
    pub parameters: Option<serde_json::Value>,
    /// Optional timeout for async jobs.
    /// Jobs running for longer than this will automatically timeout and fail.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    /// Optional maximum size of results for async jobs.
    /// Jobs with results larger than this will be failed with an error for exceeding the maximum size.
    #[serde(default)]
    pub maximum_size: Option<u64>,
}

/// Response for query submission.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SubmitQueryResponse {
    /// Unique identifier for the query.
    pub query_id: String,
    /// Current status of the query.
    pub status: JobStatus,
    /// Optional error details if the query failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorResponse>,
    /// URL to check status.
    pub status_url: String,
    /// URL to get results (once completed).
    pub results_url: String,
}

/// Error details in responses.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorResponse {
    /// Error code categorizing the failure.
    pub error_code: JobErrorCode,
    /// Human-readable error message.
    pub message: String,
    /// SQL state code if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
}

/// Schema information for a column.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ColumnSchemaResponse {
    /// Column name.
    pub name: String,
    /// Data type name.
    pub type_name: String,
    /// Whether the column can contain nulls.
    pub nullable: bool,
    /// Column position (0-indexed).
    pub position: usize,
}

/// Schema information for the result set.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaResponse {
    /// Number of columns.
    pub column_count: usize,
    /// Column definitions.
    pub columns: Vec<ColumnSchemaResponse>,
}

/// Result manifest in responses.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ManifestResponse {
    /// Data format (`ARROW_IPC`).
    pub format: String,
    /// Result schema.
    pub schema: SchemaResponse,
    /// Total number of rows.
    pub total_row_count: usize,
    /// Total number of chunks.
    pub total_chunk_count: usize,
}

/// Chunk information in result responses.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChunkResponse {
    /// Index of this chunk.
    pub chunk_index: usize,
    /// Row offset for this chunk.
    pub row_offset: usize,
    /// Number of rows in this chunk.
    pub row_count: usize,
    /// Index of the next chunk, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_index: Option<usize>,
    /// Link to next chunk, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_chunk_url: Option<String>,
    /// Data as JSON array (for application/json response).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_array: Option<Vec<serde_json::Value>>,
}

/// Full query status response.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct QueryResponse {
    /// Unique identifier for the query.
    pub query_id: String,
    /// Current status.
    pub status: JobStatus,
    /// Error details if the query failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorResponse>,
    /// Result manifest if completed successfully.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ManifestResponse>,
    /// First result chunk if completed successfully.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ChunkResponse>,
    /// When the query was created (ISO 8601).
    pub created_at: String,
    /// When execution started (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When execution completed (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    /// When results expire (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

/// Query parameters for listing queries.
#[derive(Debug, Default, Deserialize)]
pub struct ListQueriesQuery {
    /// Filter by status.
    #[serde(default)]
    pub status: Option<String>,
    /// Maximum number of results.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response for listing queries.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListQueriesResponse {
    /// List of queries.
    pub queries: Vec<QuerySummary>,
    /// Total count.
    pub total_count: usize,
}

/// Summary of a query for listing.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct QuerySummary {
    /// Query ID.
    pub query_id: String,
    /// Current status.
    pub status: JobStatus,
    /// SQL preview (up to 100 chars, including an ellipsis when truncated).
    pub sql_preview: String,
    /// When created (ISO 8601).
    pub created_at: String,
}

/// Summary of an active (in-flight) synchronous query.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ActiveQuerySummary {
    /// Query ID (UUID as string).
    pub query_id: String,
    /// Protocol this query was submitted through (http, flight, flightsql, internal).
    pub protocol: String,
    /// SQL preview (up to 100 chars, including an ellipsis when truncated).
    pub sql_preview: String,
    /// When the query began execution, in milliseconds since epoch.
    pub started_at_ms: u64,
}

/// Response for listing active sync queries.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListActiveQueriesResponse {
    pub queries: Vec<ActiveQuerySummary>,
    pub total_count: usize,
}

/// Response object for the query status route
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StatusResponse {
    /// Current status of the query.
    pub status: JobStatus,
    /// Optional error details if the query failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorResponse>,
}

/// Response for cancelling an active synchronous query.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CancelActiveQueryResponse {
    /// Query ID (UUID as string).
    pub query_id: String,
    /// Cancellation status.
    pub status: String,
}

/// Submit a new SQL query for async execution.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/queries",
    operation_id = "submit_query",
    tag = "Queries",
    request_body(
        description = "SQL query to execute asynchronously",
        content(
            (SubmitQueryRequest = "application/json", example = json!({
                "sql": "SELECT * FROM large_table WHERE status = $1",
                "parameters": ["active"],
                "timeout_seconds": 0
            }))
        )
    ),
    responses(
        (status = 202, description = "Query accepted for async execution", body = SubmitQueryResponse),
        (status = 400, description = "Invalid request"),
        (status = 503, description = "Queries API requires cluster mode")
    )
))]
pub(crate) async fn submit(
    Extension(rt): Extension<Arc<Runtime>>,
    Json(request): Json<SubmitQueryRequest>,
) -> Response {
    let read_only = super::current_principal_requires_read_only().await;

    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let result = executor.submit(request, read_only).await;

    match result {
        Ok(state) => {
            let query_id = state.job_id.clone();
            let response = SubmitQueryResponse {
                query_id: query_id.clone(),
                status: state.status,
                error: state.error.as_ref().map(|e| ErrorResponse {
                    error_code: e.error_code,
                    message: e.message.clone(),
                    sql_state: e.sql_state.clone(),
                }),
                status_url: format!("/v1/queries/{query_id}/status"),
                results_url: format!("/v1/queries/{query_id}/results"),
            };
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Err(e) => error_to_response(&e),
    }
}

/// Get query status and first result chunk.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/queries/{query_id}",
    operation_id = "get_query",
    tag = "Queries",
    params(
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query status", body = QueryResponse),
        (status = 404, description = "Query not found"),
        (status = 410, description = "Query results expired")
    )
))]
pub(crate) async fn get_query(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Response {
    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let result = executor.get_status(&query_id).await;

    match result {
        Ok(state) => {
            let mut response = job_state_to_response(&state);

            // If succeeded, include first chunk data
            if state.status == JobStatus::Succeeded
                && let Ok(batches) = executor.get_chunk(&query_id, 0).await
                && let Some(result) = &state.result
            {
                match build_chunk_response(&query_id, 0, &batches, result) {
                    Ok(first_chunk) => response.result = Some(first_chunk),
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({"error": e})),
                        )
                            .into_response();
                    }
                }
            }

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => error_to_response(&e),
    }
}

/// Get query status only.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/queries/{query_id}/status",
    operation_id = "get_query_status",
    tag = "Queries",
    params(
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query status", body = StatusResponse),
        (status = 404, description = "Query not found")
    )
))]
pub(crate) async fn get_status(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Response {
    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let result = executor.get_status(&query_id).await;

    match result {
        Ok(state) => {
            let error = state.error.as_ref().map(|e| ErrorResponse {
                error_code: e.error_code,
                message: e.message.clone(),
                sql_state: e.sql_state.clone(),
            });

            let status_response = StatusResponse {
                status: state.status,
                error,
            };

            (StatusCode::OK, Json(status_response)).into_response()
        }
        Err(e) => error_to_response(&e),
    }
}

/// Get query results (first partition/chunk by default).
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/queries/{query_id}/results",
    operation_id = "get_query_results",
    tag = "Queries",
    params(
        ("query_id" = String, Path, description = "Query ID"),
        ("partition" = Option<usize>, Query, description = "Partition/chunk index (default 0)"),
        ("format" = Option<String>, Query, description = "Result format: json (default)")
    ),
    responses(
        (status = 200, description = "Query results", body = ChunkResponse),
        (status = 400, description = "Invalid request format"),
        (status = 404, description = "Query not found"),
        (status = 409, description = "Query not yet complete"),
        (status = 410, description = "Query results expired"),
        (status = 425, description = "Query still running (Too Early)")
    )
))]
pub(crate) async fn get_results(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(query_id): Path<String>,
    Query(params): Query<ResultsQueryParams>,
) -> Response {
    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let partition = params.partition.unwrap_or(0);
    if let Some(format) = params.format.as_deref()
        && !format.eq_ignore_ascii_case("json")
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("Unsupported result format '{format}'. Supported format: json")
            })),
        )
            .into_response();
    }

    // First check the job state
    let state = match executor.get_status(&query_id).await {
        Ok(s) => s,
        Err(e) => return error_to_response(&e),
    };

    if state.status != JobStatus::Succeeded {
        // Return appropriate error based on terminal state
        let (status_code, error_msg) = match state.status {
            JobStatus::Failed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Query execution failed".to_string(),
            ),
            JobStatus::Cancelled => (StatusCode::GONE, "Query was cancelled".to_string()),
            JobStatus::Closed => (StatusCode::GONE, "Query results have expired".to_string()),
            // Pending or Running - use 425 Too Early
            // Safety: 425 is a valid HTTP status code (RFC 8470), so from_u16 cannot fail
            _ => {
                let Ok(too_early) = StatusCode::from_u16(425) else {
                    unreachable!("425 is a valid HTTP status code")
                };
                (
                    too_early,
                    format!("Query not yet complete (status: {})", state.status),
                )
            }
        };
        return (
            status_code,
            Json(serde_json::json!({
                "error": error_msg,
                "status": state.status.to_string()
            })),
        )
            .into_response();
    }

    let result = executor.get_chunk(&query_id, partition).await;

    match result {
        Ok(batches) => {
            if let Some(job_result) = &state.result {
                match build_chunk_response(&query_id, partition, &batches, job_result) {
                    Ok(chunk_response) => (StatusCode::OK, Json(chunk_response)).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e})),
                    )
                        .into_response(),
                }
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "No result metadata found"})),
                )
                    .into_response()
            }
        }
        Err(e) => error_to_response(&e),
    }
}

/// Query parameters for results endpoint.
#[derive(Debug, Default, Deserialize)]
pub struct ResultsQueryParams {
    /// Partition/chunk index.
    #[serde(default)]
    pub partition: Option<usize>,
    /// Result format (json).
    #[serde(default)]
    pub format: Option<String>,
}

/// Get a specific result chunk.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/queries/{query_id}/results/chunks/{chunk_index}",
    operation_id = "get_query_chunk",
    tag = "Queries",
    params(
        ("query_id" = String, Path, description = "Query ID"),
        ("chunk_index" = usize, Path, description = "Chunk index (0-based)")
    ),
    responses(
        (status = 200, description = "Result chunk", body = ChunkResponse),
        (status = 404, description = "Query or chunk not found"),
        (status = 409, description = "Query not yet complete"),
        (status = 410, description = "Query results expired")
    )
))]
pub(crate) async fn get_chunk(
    Extension(rt): Extension<Arc<Runtime>>,
    Path((query_id, chunk_index)): Path<(String, usize)>,
) -> Response {
    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    // First check the job state to get manifest
    let state = match executor.get_status(&query_id).await {
        Ok(s) => s,
        Err(e) => return error_to_response(&e),
    };

    if state.status != JobStatus::Succeeded {
        let (status_code, error_msg) = match state.status {
            JobStatus::Failed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Query execution failed".to_string(),
            ),
            JobStatus::Cancelled => (StatusCode::GONE, "Query was cancelled".to_string()),
            JobStatus::Closed => (StatusCode::GONE, "Query results have expired".to_string()),
            // Pending or Running
            _ => (
                StatusCode::CONFLICT,
                format!("Query not yet complete (status: {})", state.status),
            ),
        };
        return (status_code, Json(serde_json::json!({"error": error_msg}))).into_response();
    }

    let result = executor.get_chunk(&query_id, chunk_index).await;

    match result {
        Ok(batches) => {
            if let Some(job_result) = &state.result {
                match build_chunk_response(&query_id, chunk_index, &batches, job_result) {
                    Ok(chunk_response) => (StatusCode::OK, Json(chunk_response)).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e})),
                    )
                        .into_response(),
                }
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "No result metadata found"})),
                )
                    .into_response()
            }
        }
        Err(e) => error_to_response(&e),
    }
}

/// Cancel a running async query.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/queries/{query_id}/cancel",
    operation_id = "cancel_query",
    tag = "Queries",
    params(
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query cancelled", body = QueryResponse),
        (status = 404, description = "Query not found"),
        (status = 409, description = "Query already completed")
    )
))]
pub(crate) async fn cancel(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Response {
    if let Some(response) = super::require_write_access().await {
        return response;
    }

    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let result = executor.cancel(&query_id).await;

    match result {
        Ok(state) => {
            let response = job_state_to_response(&state);
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => error_to_response(&e),
    }
}

/// Cancel an active synchronous query known to this runtime.
///
/// This covers queries running behind `/v1/sql`, `FlightSQL`, NSQL, `/v1/search`,
/// and tool-driven SQL.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql/{query_id}/cancel",
    operation_id = "cancel_active_sql_query",
    tag = "SQL",
    params(
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Active SQL query cancelled", body = CancelActiveQueryResponse),
        (status = 400, description = "Invalid query ID"),
        (status = 403, description = "API key does not allow write access"),
        (status = 404, description = "Active SQL query not found")
    )
))]
pub(crate) async fn cancel_active(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(query_id): Path<String>,
) -> Response {
    if let Some(response) = super::require_write_access().await {
        return response;
    }

    let parsed_uuid = match uuid::Uuid::parse_str(&query_id) {
        Ok(uuid) => uuid,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Invalid query_id (expected UUID): {e}"),
                })),
            )
                .into_response();
        }
    };

    if rt.df.query_cancel_registry().cancel(parsed_uuid) {
        return (
            StatusCode::OK,
            Json(CancelActiveQueryResponse {
                query_id,
                status: "cancelled".to_string(),
            }),
        )
            .into_response();
    }

    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": format!("Active SQL query '{query_id}' not found"),
        })),
    )
        .into_response()
}

/// List currently-active synchronous queries known to this runtime.
///
/// This reports queries running behind `/v1/sql`, `FlightSQL`, NSQL, `/v1/search`,
/// and tool-driven SQL. Async jobs queries are reported via `GET /v1/queries`.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/sql/active",
    operation_id = "list_active_queries",
    tag = "SQL",
    responses(
        (status = 200, description = "List of active sync queries", body = ListActiveQueriesResponse),
        (status = 403, description = "API key does not allow write access"),
    )
))]
pub(crate) async fn list_active(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    if let Some(response) = super::require_write_access().await {
        return response;
    }

    let registry = rt.df.query_cancel_registry();
    let queries: Vec<ActiveQuerySummary> = registry
        .list()
        .into_iter()
        .map(|info| ActiveQuerySummary {
            query_id: info.query_id.to_string(),
            protocol: info.protocol.as_str().to_string(),
            sql_preview: info.sql_preview.to_string(),
            started_at_ms: info.started_at_ms,
        })
        .collect();

    let total_count = queries.len();
    (
        StatusCode::OK,
        Json(ListActiveQueriesResponse {
            queries,
            total_count,
        }),
    )
        .into_response()
}

/// List all queries.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/queries",
    operation_id = "list_queries",
    tag = "Queries",
    params(
        ("status" = Option<String>, Query, description = "Filter by status (queued, running, completed, failed, cancelled)"),
        ("limit" = Option<usize>, Query, description = "Maximum number of results")
    ),
    responses(
        (status = 200, description = "List of queries", body = ListQueriesResponse),
        (status = 503, description = "Queries API requires cluster mode")
    )
))]
pub(crate) async fn list(
    Extension(rt): Extension<Arc<Runtime>>,
    Query(query): Query<ListQueriesQuery>,
) -> Response {
    let executor = match get_executor(&rt) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    // Parse status filter
    let status_filter = query.status.and_then(|s| match s.to_lowercase().as_str() {
        "queued" | "pending" => Some(JobStatus::Pending),
        "running" => Some(JobStatus::Running),
        "completed" | "succeeded" => Some(JobStatus::Succeeded),
        "failed" => Some(JobStatus::Failed),
        "cancelled" => Some(JobStatus::Cancelled),
        "closed" => Some(JobStatus::Closed),
        _ => None,
    });

    match executor.list_jobs(status_filter).await {
        Ok(jobs) => {
            let limit = query.limit.unwrap_or(100);
            let queries: Vec<QuerySummary> = jobs
                .into_iter()
                .take(limit)
                .map(|job| {
                    // Truncate SQL preview safely using chars (not bytes)
                    let sql_preview = if job.sql.chars().count() > 100 {
                        let truncated: String = job.sql.chars().take(97).collect();
                        format!("{truncated}...")
                    } else {
                        job.sql.clone()
                    };
                    QuerySummary {
                        query_id: job.job_id,
                        status: job.status,
                        sql_preview,
                        created_at: ms_to_iso8601(job.created_at_ms),
                    }
                })
                .collect();

            let total_count = queries.len();
            let response = ListQueriesResponse {
                queries,
                total_count,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => error_to_response(&e),
    }
}

fn job_state_to_response(state: &JobState) -> QueryResponse {
    let error = state.error.as_ref().map(|e| ErrorResponse {
        error_code: e.error_code,
        message: e.message.clone(),
        sql_state: e.sql_state.clone(),
    });

    let manifest = state.result.as_ref().map(|r| ManifestResponse {
        format: r.manifest.format.clone(),
        schema: SchemaResponse {
            column_count: r.manifest.schema.column_count,
            columns: r
                .manifest
                .schema
                .columns
                .iter()
                .map(|c| ColumnSchemaResponse {
                    name: c.name.clone(),
                    type_name: c.type_name.clone(),
                    nullable: c.nullable,
                    position: c.position,
                })
                .collect(),
        },
        total_row_count: r.manifest.total_row_count,
        total_chunk_count: r.manifest.total_chunk_count,
    });

    QueryResponse {
        query_id: state.job_id.clone(),
        status: state.status,
        error,
        manifest,
        result: None,
        created_at: ms_to_iso8601(state.created_at_ms),
        started_at: state.started_at_ms.map(ms_to_iso8601),
        completed_at: state.completed_at_ms.map(ms_to_iso8601),
        expires_at: state.expires_at_ms.map(ms_to_iso8601),
    }
}

fn build_chunk_response(
    query_id: &str,
    chunk_index: usize,
    batches: &[arrow::array::RecordBatch],
    job_result: &crate::jobs::JobResult,
) -> std::result::Result<ChunkResponse, String> {
    use crate::jobs::DEFAULT_CHUNK_SIZE;

    // Calculate row count and offset
    let row_count: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    // Calculate row offset based on chunk index using the configured chunk size
    let row_offset = chunk_index.saturating_mul(DEFAULT_CHUNK_SIZE);

    // Determine next chunk info
    let (next_chunk_index, next_chunk_url) =
        if chunk_index.saturating_add(1) < job_result.manifest.total_chunk_count {
            let next_idx = chunk_index.saturating_add(1);
            (
                Some(next_idx),
                Some(format!("/v1/queries/{query_id}/results/chunks/{next_idx}")),
            )
        } else {
            (None, None)
        };

    // Convert batches to JSON - propagate error if serialization fails
    let data_array = batches_to_json(batches)?;

    Ok(ChunkResponse {
        chunk_index,
        row_offset,
        row_count,
        next_chunk_index,
        next_chunk_url,
        data_array: Some(data_array),
    })
}

/// Converts record batches to JSON values.
/// Returns an error if any batch fails to serialize rather than returning partial results.
fn batches_to_json(
    batches: &[arrow::array::RecordBatch],
) -> std::result::Result<Vec<serde_json::Value>, String> {
    match write_to_json_value(batches).map_err(|e| e.to_string())? {
        serde_json::Value::Array(rows) => Ok(rows),
        _ => Err("Expected JSON array when serializing query results".to_string()),
    }
}

fn error_to_response(error: &crate::jobs::Error) -> Response {
    use crate::jobs::Error;

    match error {
        Error::JobNotFound { .. } | Error::ChunkNotFound { .. } | Error::NoRowsReturned { .. } => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        Error::JobResultsExpired { .. } => (
            StatusCode::GONE,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        Error::JobNotComplete { .. } => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        Error::ClusterModeRequired => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

/// Converts a Unix timestamp in milliseconds to ISO 8601 format.
/// Returns a sentinel string for invalid timestamps rather than silently returning epoch.
fn ms_to_iso8601(ms: u64) -> String {
    use chrono::DateTime;

    // Convert milliseconds to seconds and nanoseconds for chrono
    // The casts are safe: secs < 2^63 for reasonable timestamps (until year 292M+)
    // nanos < 1_000_000_000 which fits in u32
    #[expect(clippy::cast_possible_wrap)]
    let secs = (ms / 1000) as i64;
    #[expect(clippy::cast_possible_truncation)]
    let nanos = ((ms % 1000) * 1_000_000) as u32;

    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
        dt.to_rfc3339()
    } else {
        // Data correctness: never silently coerce invalid timestamps to Unix epoch.
        // Instead, log and return a clearly invalid sentinel value.
        tracing::error!(
            timestamp_ms = ms,
            "Invalid Unix timestamp; returning sentinel ISO 8601 string"
        );
        format!("INVALID_TIMESTAMP({ms})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use app::spicepod::component::runtime::ApiKey;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_request_context::{Protocol, RequestContextBuilder};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    async fn test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::builder().build().await)
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body should be readable");
        serde_json::from_slice(&body).expect("response body should be valid JSON")
    }

    #[tokio::test]
    async fn list_active_requires_write_access() {
        let rt = test_runtime().await;
        let context = Arc::new(RequestContextBuilder::new(Protocol::Http).build());
        context
            .set_auth_principal(Arc::new(ApiKey::ReadOnly {
                key: "read-only".to_string(),
            }) as AuthPrincipalRef)
            .expect("auth principal should be set");

        let response = context.scope(list_active(Extension(rt))).await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cancel_active_requires_write_access() {
        let rt = test_runtime().await;
        let context = Arc::new(RequestContextBuilder::new(Protocol::Http).build());
        context
            .set_auth_principal(Arc::new(ApiKey::ReadOnly {
                key: "read-only".to_string(),
            }) as AuthPrincipalRef)
            .expect("auth principal should be set");

        let response = context
            .scope(cancel_active(
                Extension(rt),
                Path(Uuid::new_v4().to_string()),
            ))
            .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cancel_active_rejects_invalid_uuid() {
        let rt = test_runtime().await;

        let response = cancel_active(Extension(rt), Path("not-a-uuid".to_string())).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cancel_active_returns_not_found_for_missing_query() {
        let rt = test_runtime().await;

        let response = cancel_active(Extension(rt), Path(Uuid::new_v4().to_string())).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_active_cancels_registered_query() {
        let rt = test_runtime().await;
        let query_id = Uuid::new_v4();
        let token = CancellationToken::new();
        let registry = rt.df.query_cancel_registry();
        let _guard = registry.register(query_id, "SELECT 1", Protocol::Http, token.clone());

        let response = cancel_active(Extension(rt), Path(query_id.to_string())).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(token.is_cancelled());
        let body = response_json(response).await;
        assert_eq!(body["query_id"], query_id.to_string());
        assert_eq!(body["status"], "cancelled");
    }

    #[tokio::test]
    async fn list_active_returns_stable_truncated_summaries() {
        let rt = test_runtime().await;
        let registry = rt.df.query_cancel_registry();
        let long_query_id = Uuid::from_u128(2);
        let short_query_id = Uuid::from_u128(1);
        let long_sql = format!("SELECT '{}'", "x".repeat(160));

        let _long_guard = registry.register(
            long_query_id,
            &long_sql,
            Protocol::Http,
            CancellationToken::new(),
        );
        let _short_guard = registry.register(
            short_query_id,
            "SELECT 1",
            Protocol::Flight,
            CancellationToken::new(),
        );

        let response = list_active(Extension(rt)).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["total_count"], 2);
        let queries = body["queries"]
            .as_array()
            .expect("queries should be an array");
        assert!(queries.windows(2).all(|window| {
            let left = &window[0];
            let right = &window[1];
            let left_key = (
                left["started_at_ms"]
                    .as_u64()
                    .expect("started_at_ms should be a u64"),
                left["query_id"]
                    .as_str()
                    .expect("query_id should be a string"),
            );
            let right_key = (
                right["started_at_ms"]
                    .as_u64()
                    .expect("started_at_ms should be a u64"),
                right["query_id"]
                    .as_str()
                    .expect("query_id should be a string"),
            );
            left_key <= right_key
        }));

        let long_query = queries
            .iter()
            .find(|query| query["query_id"] == long_query_id.to_string())
            .expect("long query should be listed");
        let preview = long_query["sql_preview"]
            .as_str()
            .expect("sql_preview should be a string");
        assert_eq!(preview.chars().count(), 100);
        assert!(preview.ends_with("..."));
        assert!(long_sql.starts_with(preview.trim_end_matches("...")));
    }
}
