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

//! Async query Flight actions for submitting, polling, and retrieving async query results.
//!
//! This module provides Arrow Flight actions for async query execution:
//! - `SubmitAsyncQuery` - Submit a SQL query for async execution
//! - `GetAsyncQueryStatus` - Check the status of an async query
//! - `GetAsyncQueryResult` - Get the result of a completed async query
//! - `CancelAsyncQuery` - Cancel a running async query

use std::fmt;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::datafusion::job_executor_context_extension::get_job_executor;
use crate::jobs::{JobState, JobStatus};
use runtime_request_context::{AsyncMarker, RequestContext};

/// Action types for async query operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncActionType {
    /// Submit a SQL query for async execution.
    SubmitAsyncQuery,
    /// Get the status of an async query.
    GetAsyncQueryStatus,
    /// Get the result of a completed async query.
    GetAsyncQueryResult,
    /// Cancel a running async query.
    CancelAsyncQuery,
}

impl AsyncActionType {
    /// Returns the action type string for Flight protocol.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubmitAsyncQuery => "SubmitAsyncQuery",
            Self::GetAsyncQueryStatus => "GetAsyncQueryStatus",
            Self::GetAsyncQueryResult => "GetAsyncQueryResult",
            Self::CancelAsyncQuery => "CancelAsyncQuery",
        }
    }

    /// Parses an action type string from the Flight protocol.
    #[must_use]
    #[expect(dead_code, reason = "API for external consumers")]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "SubmitAsyncQuery" => Some(Self::SubmitAsyncQuery),
            "GetAsyncQueryStatus" => Some(Self::GetAsyncQueryStatus),
            "GetAsyncQueryResult" => Some(Self::GetAsyncQueryResult),
            "CancelAsyncQuery" => Some(Self::CancelAsyncQuery),
            _ => None,
        }
    }
}

impl fmt::Display for AsyncActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Module providing action type string constants for backward compatibility.
pub mod action_types {
    use super::AsyncActionType;
    pub const SUBMIT_ASYNC_QUERY: &str = AsyncActionType::SubmitAsyncQuery.as_str();
    pub const GET_ASYNC_QUERY_STATUS: &str = AsyncActionType::GetAsyncQueryStatus.as_str();
    pub const GET_ASYNC_QUERY_RESULT: &str = AsyncActionType::GetAsyncQueryResult.as_str();
    pub const CANCEL_ASYNC_QUERY: &str = AsyncActionType::CancelAsyncQuery.as_str();
}

/// Strongly-typed query status for API responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    /// Query is queued but not yet running.
    Pending,
    /// Query is actively executing.
    Running,
    /// Query completed successfully, results available.
    Succeeded,
    /// Query execution failed.
    Failed,
    /// Query was cancelled by user.
    Cancelled,
    /// Query results have been cleaned up / expired.
    Closed,
}

impl From<JobStatus> for QueryStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Pending => Self::Pending,
            JobStatus::Running => Self::Running,
            JobStatus::Succeeded => Self::Succeeded,
            JobStatus::Failed => Self::Failed,
            JobStatus::Cancelled => Self::Cancelled,
            JobStatus::Closed => Self::Closed,
        }
    }
}

impl fmt::Display for QueryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Closed => write!(f, "CLOSED"),
        }
    }
}

/// Strongly-typed query ID to avoid mixing with regular strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryId(pub String);

impl QueryId {
    /// Creates a new query ID from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the query ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for QueryId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Request to submit an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitAsyncQueryRequest {
    /// The SQL query to execute.
    pub sql: String,
    /// Optional query parameters as JSON.
    #[serde(default)]
    pub parameters: Option<serde_json::Value>,
}

/// Response from submitting an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitAsyncQueryResponse {
    /// The unique query ID.
    pub query_id: QueryId,
    /// Current status of the query.
    pub status: QueryStatus,
}

/// Request to get the status of an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAsyncQueryStatusRequest {
    /// The query ID to check.
    pub query_id: QueryId,
}

/// Response with the status of an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAsyncQueryStatusResponse {
    /// The query ID.
    pub query_id: QueryId,
    /// Current status: pending, running, succeeded, failed, cancelled, closed.
    pub status: QueryStatus,
    /// Error message if the query failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AsyncQueryError>,
    /// Result metadata if completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<AsyncQueryResultMetadata>,
}

/// Error details for a failed query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncQueryError {
    /// Error code.
    pub error_code: String,
    /// Error message.
    pub message: String,
}

/// Metadata about the query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncQueryResultMetadata {
    /// Total number of rows.
    pub total_row_count: usize,
    /// Number of result chunks.
    pub total_chunk_count: usize,
}

/// Request to get the result of an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAsyncQueryResultRequest {
    /// The query ID.
    pub query_id: QueryId,
    /// Which chunk to retrieve (0-indexed).
    #[serde(default)]
    pub chunk_index: usize,
}

/// Request to cancel an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAsyncQueryRequest {
    /// The query ID to cancel.
    pub query_id: QueryId,
}

/// Response from cancelling an async query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAsyncQueryResponse {
    /// The query ID.
    pub query_id: QueryId,
    /// Whether the cancellation was successful.
    pub cancelled: bool,
    /// Current status after cancellation attempt.
    pub status: QueryStatus,
}

/// Handles the `SubmitAsyncQuery` action.
pub async fn handle_submit_async_query(body: &[u8]) -> Result<Vec<u8>, Status> {
    let context = RequestContext::current(AsyncMarker::new().await);
    let executor = get_job_executor(&context)
        .ok_or_else(|| Status::unavailable("Async queries are only available in cluster mode"))?;

    let request: SubmitAsyncQueryRequest = serde_json::from_slice(body).map_err(|e| {
        Status::invalid_argument(format!("Failed to parse SubmitAsyncQueryRequest: {e}"))
    })?;

    let state = executor
        .submit(request.sql, request.parameters)
        .await
        .map_err(|e| Status::internal(format!("Failed to submit query: {e}")))?;

    let response = SubmitAsyncQueryResponse {
        query_id: QueryId::new(state.job_id),
        status: QueryStatus::from(state.status),
    };

    serde_json::to_vec(&response)
        .map_err(|e| Status::internal(format!("Failed to serialize response: {e}")))
}

/// Handles the `GetAsyncQueryStatus` action.
pub async fn handle_get_async_query_status(body: &[u8]) -> Result<Vec<u8>, Status> {
    let context = RequestContext::current(AsyncMarker::new().await);
    let executor = get_job_executor(&context)
        .ok_or_else(|| Status::unavailable("Async queries are only available in cluster mode"))?;

    let request: GetAsyncQueryStatusRequest = serde_json::from_slice(body).map_err(|e| {
        Status::invalid_argument(format!("Failed to parse GetAsyncQueryStatusRequest: {e}"))
    })?;

    let state = executor
        .get_status(request.query_id.as_str())
        .await
        .map_err(|e| Status::not_found(format!("Query not found: {e}")))?;

    let response = job_state_to_status_response(&state);

    serde_json::to_vec(&response)
        .map_err(|e| Status::internal(format!("Failed to serialize response: {e}")))
}

/// Handles the `GetAsyncQueryResult` action.
/// Returns result data as Arrow IPC format.
pub async fn handle_get_async_query_result(body: &[u8]) -> Result<Vec<u8>, Status> {
    let context = RequestContext::current(AsyncMarker::new().await);
    let executor = get_job_executor(&context)
        .ok_or_else(|| Status::unavailable("Async queries are only available in cluster mode"))?;

    let request: GetAsyncQueryResultRequest = serde_json::from_slice(body).map_err(|e| {
        Status::invalid_argument(format!("Failed to parse GetAsyncQueryResultRequest: {e}"))
    })?;

    // Check job status first
    let state = executor
        .get_status(request.query_id.as_str())
        .await
        .map_err(|e| Status::not_found(format!("Query not found: {e}")))?;

    // Return appropriate error for non-succeeded terminal states
    match state.status {
        JobStatus::Succeeded => {}
        JobStatus::Failed => {
            return Err(Status::internal("Query execution failed"));
        }
        JobStatus::Cancelled => {
            return Err(Status::cancelled("Query was cancelled"));
        }
        JobStatus::Closed => {
            return Err(Status::not_found("Query results have expired"));
        }
        _ => {
            return Err(Status::failed_precondition(format!(
                "Query not yet complete (status: {})",
                state.status
            )));
        }
    }

    // Get the result chunk
    let batches = executor
        .get_chunk(request.query_id.as_str(), request.chunk_index)
        .await
        .map_err(|e| Status::internal(format!("Failed to get result chunk: {e}")))?;

    // Serialize to Arrow IPC
    serialize_batches_to_ipc(&batches)
}

/// Handles the `CancelAsyncQuery` action.
pub async fn handle_cancel_async_query(body: &[u8]) -> Result<Vec<u8>, Status> {
    let context = RequestContext::current(AsyncMarker::new().await);
    let executor = get_job_executor(&context)
        .ok_or_else(|| Status::unavailable("Async queries are only available in cluster mode"))?;

    let request: CancelAsyncQueryRequest = serde_json::from_slice(body).map_err(|e| {
        Status::invalid_argument(format!("Failed to parse CancelAsyncQueryRequest: {e}"))
    })?;

    let state = executor
        .cancel(request.query_id.as_str())
        .await
        .map_err(|e| Status::internal(format!("Failed to cancel query: {e}")))?;

    let response = CancelAsyncQueryResponse {
        query_id: QueryId::new(state.job_id),
        cancelled: state.status == JobStatus::Cancelled,
        status: QueryStatus::from(state.status),
    };

    serde_json::to_vec(&response)
        .map_err(|e| Status::internal(format!("Failed to serialize response: {e}")))
}

fn job_state_to_status_response(state: &JobState) -> GetAsyncQueryStatusResponse {
    let error = state.error.as_ref().map(|e| AsyncQueryError {
        error_code: e.error_code.clone(),
        message: e.message.clone(),
    });

    let result = state.result.as_ref().map(|r| AsyncQueryResultMetadata {
        total_row_count: r.manifest.total_row_count,
        total_chunk_count: r.manifest.total_chunk_count,
    });

    GetAsyncQueryStatusResponse {
        query_id: QueryId::new(state.job_id.clone()),
        status: QueryStatus::from(state.status),
        error,
        result,
    }
}

fn serialize_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, Status> {
    if batches.is_empty() {
        // Return empty schema response
        let schema = Schema::new(vec![Field::new("empty", DataType::Null, true)]);
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema)
                .map_err(|e| Status::internal(format!("Failed to create IPC writer: {e}")))?;
            writer
                .finish()
                .map_err(|e| Status::internal(format!("Failed to finish IPC stream: {e}")))?;
        }
        return Ok(buffer);
    }

    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| Status::internal(format!("Failed to create IPC writer: {e}")))?;

        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| Status::internal(format!("Failed to write batch: {e}")))?;
        }

        writer
            .finish()
            .map_err(|e| Status::internal(format!("Failed to finish IPC stream: {e}")))?;
    }

    Ok(buffer)
}
