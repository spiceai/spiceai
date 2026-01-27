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

//! API client for async queries using the spiceai SDK.

use crate::context::RuntimeContext;
use std::sync::Arc;
use std::time::Duration;

// Re-export types from spiceai SDK for use in mod.rs
pub use spiceai::query::{
    QueryError, QueryInfo, QueryJob, QueryListResponse, QueryStatus, QuerySummary,
};
pub use spiceai::{Client, ClientBuilder};

/// Default poll interval for checking query status.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Client for interacting with the async queries API using the spiceai SDK.
pub struct QueriesClient {
    client: Arc<Client>,
}

impl QueriesClient {
    /// Create a new queries client using the spiceai SDK.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be built (e.g., connection failed).
    pub async fn new(ctx: &RuntimeContext) -> Result<Self, ApiError> {
        // Only http_url is needed for async queries API (/v1/queries)
        let mut builder = ClientBuilder::new().http_url(ctx.http_endpoint());

        if let Some(api_key) = ctx.api_key() {
            builder = builder.api_key(api_key);
        }

        builder = builder.user_agent(ctx.user_agent());

        let client = builder.build().await.map_err(|e| {
            let err_str = e.to_string();
            if err_str.contains("connection refused") {
                ApiError::Unavailable(format!(
                    "Spice runtime is unavailable at {}. Is it running?",
                    ctx.http_endpoint()
                ))
            } else {
                ApiError::Connection(err_str)
            }
        })?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Submit a new SQL query for async execution.
    ///
    /// Returns a `QueryJob` that can be used to track the query status,
    /// wait for completion, retrieve results, or cancel the query.
    pub async fn submit(&self, sql: &str) -> Result<QueryJob, ApiError> {
        self.client.query(sql).await.map_err(ApiError::from)
    }

    /// List queries with optional status filter.
    ///
    /// # Arguments
    ///
    /// * `status` - Optional filter by status: "pending", "running", "succeeded", "failed", "cancelled"
    /// * `limit` - Optional maximum number of queries to return
    pub async fn list(
        &self,
        status: Option<&str>,
        limit: Option<usize>,
    ) -> Result<QueryListResponse, ApiError> {
        self.client
            .queries(status, limit)
            .await
            .map_err(ApiError::from)
    }

    /// Get a `QueryJob` handle for an existing query by ID.
    ///
    /// This allows you to resume tracking a query that was submitted earlier,
    /// check its status, retrieve results, or cancel it.
    pub fn get_query(&self, query_id: &str) -> Result<QueryJob, ApiError> {
        self.client.get_query(query_id).map_err(ApiError::from)
    }

    /// Cancel a running query by ID.
    ///
    /// Returns the final query info after cancellation.
    pub async fn cancel(&self, query_id: &str) -> Result<QueryInfo, ApiError> {
        self.client
            .cancel_query(query_id)
            .await
            .map_err(ApiError::from)
    }

    /// Get the status of a query.
    ///
    /// This is a convenience method that gets a `QueryJob` and fetches its status.
    pub async fn get_status(&self, query_id: &str) -> Result<QueryStatus, ApiError> {
        let job = self.get_query(query_id).await?;
        job.status().await.map_err(ApiError::from)
    }

    /// Get the full info of a query.
    ///
    /// This is a convenience method that gets a `QueryJob` and fetches its info.
    pub async fn get_info(&self, query_id: &str) -> Result<QueryInfo, ApiError> {
        let job = self.get_query(query_id).await?;
        job.info().await.map_err(ApiError::from)
    }

    /// Wait for a query to complete with optional timeout.
    ///
    /// Returns the final query result when complete.
    pub async fn wait(
        &self,
        query_id: &str,
        timeout: Option<Duration>,
    ) -> Result<spiceai::query::QueryResult, ApiError> {
        let job = self.get_query(query_id).await?;
        match timeout {
            Some(t) => job.wait_timeout(t).await.map_err(ApiError::from),
            None => job.wait().await.map_err(ApiError::from),
        }
    }

    /// Get results for a completed query as Arrow `RecordBatch`es.
    pub async fn get_results(
        &self,
        query_id: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, ApiError> {
        let job = self.get_query(query_id).await?;
        job.results().await.map_err(ApiError::from)
    }
}

/// API error types.
#[derive(Debug)]
pub enum ApiError {
    /// Connection/build error.
    Connection(String),
    /// Runtime unavailable.
    Unavailable(String),
    /// Query operation error (wraps SDK QueryError).
    Query(QueryError),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection(msg) | Self::Unavailable(msg) => write!(f, "{msg}"),
            Self::Query(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<QueryError> for ApiError {
    fn from(e: QueryError) -> Self {
        // Map specific QueryError variants to more user-friendly ApiError variants
        let msg = e.to_string();
        if msg.contains("unavailable") || msg.contains("connection refused") {
            Self::Unavailable(msg)
        } else {
            Self::Query(e)
        }
    }
}
