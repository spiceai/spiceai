/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! API client for the /v1/queries async queries endpoint.

use crate::context::RuntimeContext;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default poll interval for checking query status.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Client for interacting with the async queries API.
pub struct QueriesClient<'a> {
    ctx: &'a RuntimeContext,
    client: Client,
}

impl<'a> QueriesClient<'a> {
    /// Create a new queries client.
    pub fn new(ctx: &'a RuntimeContext) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(300)) // Long timeout for queries
            .build()
            .unwrap_or_default();
        Self { ctx, client }
    }

    /// Submit a new SQL query for async execution.
    pub async fn submit(&self, req: &SubmitRequest) -> Result<SubmitResponse, ApiError> {
        let url = format!("{}/v1/queries", self.ctx.http_endpoint());
        let mut request = self.client.post(&url).json(req);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            202 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            503 => Err(ApiError::ServiceUnavailable(
                "Async queries require cluster mode with scheduler.state_location configured"
                    .to_string(),
            )),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    /// Get the status of a query.
    pub async fn get_status(&self, query_id: &str) -> Result<QueryStatus, ApiError> {
        let url = format!(
            "{}/v1/queries/{}/status",
            self.ctx.http_endpoint(),
            query_id
        );
        let mut request = self.client.get(&url);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            200 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            404 => Err(ApiError::NotFound(format!("query '{query_id}' not found"))),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    /// Get full query information including first result chunk if completed.
    pub async fn get_query(&self, query_id: &str) -> Result<QueryInfo, ApiError> {
        let url = format!("{}/v1/queries/{}", self.ctx.http_endpoint(), query_id);
        let mut request = self.client.get(&url);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            200 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            404 => Err(ApiError::NotFound(format!("query '{query_id}' not found"))),
            410 => Err(ApiError::Gone(format!(
                "query '{query_id}' results have expired"
            ))),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    /// Get results for a completed query.
    pub async fn get_results(
        &self,
        query_id: &str,
        chunk_index: usize,
    ) -> Result<ResultChunk, ApiError> {
        let path = if chunk_index > 0 {
            format!(
                "{}/v1/queries/{}/results/chunks/{}",
                self.ctx.http_endpoint(),
                query_id,
                chunk_index
            )
        } else {
            format!(
                "{}/v1/queries/{}/results",
                self.ctx.http_endpoint(),
                query_id
            )
        };

        let mut request = self.client.get(&path);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            200 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            404 => Err(ApiError::NotFound(format!("query '{query_id}' not found"))),
            410 => Err(ApiError::Gone(format!(
                "query '{query_id}' results have expired or were cancelled"
            ))),
            425 | 409 => Err(ApiError::NotReady(format!(
                "query '{query_id}' is not yet complete"
            ))),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    /// Cancel a running query.
    pub async fn cancel(&self, query_id: &str) -> Result<QueryInfo, ApiError> {
        let url = format!(
            "{}/v1/queries/{}/cancel",
            self.ctx.http_endpoint(),
            query_id
        );
        let mut request = self.client.post(&url);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            200 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            404 => Err(ApiError::NotFound(format!("query '{query_id}' not found"))),
            409 => Err(ApiError::Conflict(format!(
                "query '{query_id}' has already completed"
            ))),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    /// List queries with optional status filter.
    pub async fn list(
        &self,
        status: Option<&str>,
        limit: Option<usize>,
    ) -> Result<QueryListResponse, ApiError> {
        let mut url = format!("{}/v1/queries", self.ctx.http_endpoint());
        let mut params = Vec::new();

        if let Some(s) = status {
            params.push(format!("status={s}"));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let mut request = self.client.get(&url);

        for (key, value) in self.ctx.get_headers() {
            request = request.header(&key, &value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| self.handle_request_error(&e))?;

        match response.status().as_u16() {
            200 => response
                .json()
                .await
                .map_err(|e| ApiError::Parse(e.to_string())),
            503 => Err(ApiError::ServiceUnavailable(
                "Async queries require cluster mode with scheduler.state_location configured"
                    .to_string(),
            )),
            _ => Err(self.parse_error_response(response).await),
        }
    }

    fn handle_request_error(&self, e: &reqwest::Error) -> ApiError {
        let err_str = e.to_string();
        if err_str.contains("connection refused") {
            ApiError::Unavailable(format!(
                "Spice runtime is unavailable at {}. Is it running?",
                self.ctx.http_endpoint()
            ))
        } else {
            ApiError::Request(err_str)
        }
    }

    async fn parse_error_response(&self, response: reqwest::Response) -> ApiError {
        let status = response.status().as_u16();
        match response.text().await {
            Ok(body) => {
                // Try to parse as JSON error
                if let Ok(err_resp) = serde_json::from_str::<ErrorResponse>(&body)
                    && !err_resp.error.is_empty()
                {
                    return ApiError::Server(err_resp.error);
                }
                ApiError::Server(format!("request failed with status {status}: {body}"))
            }
            Err(_) => ApiError::Server(format!("request failed with status {status}")),
        }
    }
}

/// API error types.
#[derive(Debug)]
pub enum ApiError {
    /// Request failed (network error).
    Request(String),
    /// Runtime unavailable.
    Unavailable(String),
    /// Service unavailable (503).
    ServiceUnavailable(String),
    /// Resource not found (404).
    NotFound(String),
    /// Resource gone/expired (410).
    Gone(String),
    /// Resource not ready (425/409).
    NotReady(String),
    /// Conflict (409).
    Conflict(String),
    /// Server error.
    Server(String),
    /// Parse error.
    Parse(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(msg)
            | Self::Unavailable(msg)
            | Self::ServiceUnavailable(msg)
            | Self::NotFound(msg)
            | Self::Gone(msg)
            | Self::NotReady(msg)
            | Self::Conflict(msg)
            | Self::Server(msg)
            | Self::Parse(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ApiError {}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
}

/// Request body for submitting a new query.
#[derive(Debug, Serialize)]
pub struct SubmitRequest {
    pub sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i64>,
}

/// Response from submitting a query.
#[derive(Debug, Deserialize)]
pub struct SubmitResponse {
    pub query_id: String,
    pub status: QueryStatus,
    pub status_url: String,
    pub results_url: String,
}

/// Current status of a query.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryStatus {
    pub state: String,
    pub error: Option<QueryError>,
}

impl QueryStatus {
    /// Returns true if the query state is terminal (no longer running).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state.to_uppercase().as_str(),
            "SUCCEEDED" | "FAILED" | "CANCELLED" | "CLOSED"
        )
    }

    /// Returns true if the query completed successfully.
    pub fn is_success(&self) -> bool {
        self.state.eq_ignore_ascii_case("SUCCEEDED")
    }

    /// Returns true if the query failed.
    pub fn is_failed(&self) -> bool {
        self.state.eq_ignore_ascii_case("FAILED")
    }

    /// Returns true if the query was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state.eq_ignore_ascii_case("CANCELLED")
    }
}

/// Error details for a failed query.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryError {
    #[expect(dead_code)]
    pub error_code: String,
    pub message: String,
    #[expect(dead_code)]
    pub sql_state: Option<String>,
}

/// Full information about a query.
#[derive(Debug, Deserialize)]
pub struct QueryInfo {
    pub query_id: String,
    pub status: QueryStatus,
    pub manifest: Option<ResultManifest>,
    #[expect(dead_code)]
    pub result: Option<ResultChunk>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub expires_at: Option<String>,
}

/// Result set metadata.
#[derive(Debug, Deserialize)]
pub struct ResultManifest {
    #[expect(dead_code)]
    pub format: String,
    pub schema: ResultSchema,
    pub total_row_count: usize,
    pub total_chunk_count: usize,
    #[expect(dead_code)]
    pub truncated: bool,
}

/// Schema of the result set.
#[derive(Debug, Deserialize)]
pub struct ResultSchema {
    #[expect(dead_code)]
    pub column_count: usize,
    pub columns: Vec<ColumnSchema>,
}

/// Schema for a single column.
#[derive(Debug, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub type_name: String,
    #[expect(dead_code)]
    pub nullable: bool,
    pub position: usize,
}

/// A chunk of result data.
#[derive(Debug, Deserialize)]
pub struct ResultChunk {
    #[expect(dead_code)]
    pub chunk_index: usize,
    #[expect(dead_code)]
    pub row_offset: usize,
    #[expect(dead_code)]
    pub row_count: usize,
    pub next_chunk_index: Option<usize>,
    #[expect(dead_code)]
    pub next_chunk_url: Option<String>,
    pub data_array: Option<Vec<serde_json::Map<String, serde_json::Value>>>,
}

/// Response from listing queries.
#[derive(Debug, Deserialize)]
pub struct QueryListResponse {
    pub queries: Vec<QuerySummary>,
    #[expect(dead_code)]
    pub total_count: usize,
}

/// Summary of a query for listing.
#[derive(Debug, Deserialize)]
pub struct QuerySummary {
    pub query_id: String,
    pub state: String,
    pub sql_preview: String,
    pub created_at: String,
}
