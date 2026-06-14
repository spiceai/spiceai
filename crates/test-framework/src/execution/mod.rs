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

//! Query execution abstraction for test framework
//!
//! This module provides a trait-based architecture for executing queries against different backends.
//! It enables the test framework to work with both Spice databases and non-Spice databases through
//! a common interface.
//!
//! # Architecture
//!
//! The core abstraction is the [`QueryExecutor`] trait, which defines a common interface for
//! executing queries. Different executor implementations handle the specifics of communicating
//! with their respective backends.
//!
//! # Built-in Executors
//!
//! ## [`FlightExecutor`]
//! Executes queries via Arrow Flight SQL protocol.
//! - **Use when**: Testing Spice with the default Flight SQL interface
//! - **Supports validation**: Yes (returns full Arrow batches)
//! - **Supports explain plans**: Yes (via `as_spice_client()`)
//!
//! ## [`HttpExecutor`]
//! Executes queries via synchronous HTTP `/v1/sql` endpoint.
//! - **Use when**: Testing Spice's HTTP API or measuring HTTP performance
//! - **Supports validation**: No (only returns row counts)
//! - **Supports explain plans**: No
//!
//! ## [`DistributedExecutor`]
//! Executes queries via asynchronous HTTP `/v1/queries` API with polling.
//! - **Use when**: Testing Spice in cluster mode with distributed query execution
//! - **Supports validation**: No (only returns row counts)
//! - **Supports explain plans**: No
//!
//! # Adding Support for New Databases
//!
//! To add support for a new database (e.g., `PostgreSQL`, `MySQL`), implement the [`QueryExecutor`] trait:
//!
//! ```rust,ignore
//! use async_trait::async_trait;
//! use std::sync::Arc;
//!
//! pub struct PostgresExecutor {
//!     client: Arc<tokio_postgres::Client>,
//! }
//!
//! #[async_trait]
//! impl QueryExecutor for PostgresExecutor {
//!     async fn execute(&self, query: &Query) -> Result<ExecutionResult> {
//!         let start = std::time::Instant::now();
//!         let rows = self.client.query(&query.sql, &[]).await?;
//!
//!         Ok(ExecutionResult {
//!             duration: start.elapsed(),
//!             row_count: rows.len(),
//!             batches: None, // Or convert rows to Arrow batches for validation
//!         })
//!     }
//!
//!     fn name(&self) -> &str { "postgres" }
//!     fn supports_validation(&self) -> bool { false }
//!     fn clone_box(&self) -> Box<dyn QueryExecutor> { Box::new(self.clone()) }
//! }
//! ```
//!
//! Then use it in your test command:
//!
//! ```rust,ignore
//! let executor: Box<dyn QueryExecutor> = Box::new(PostgresExecutor::new(client));
//! let test_builder = NotStarted::new().with_query_executor(executor);
//! ```

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use std::{sync::Arc, time::Duration};

use crate::queries::Query;

/// Result of executing a single query
#[derive(Debug)]
pub struct ExecutionResult {
    /// Time taken to execute the query
    pub duration: Duration,
    /// Number of rows returned
    pub row_count: usize,
    /// Actual record batches (optional, for validation/snapshots)
    /// Some executors (like HTTP) may not provide this
    pub batches: Option<Vec<RecordBatch>>,
}

/// Trait for executing queries against different backends
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute a query and return the result
    async fn execute(&self, query: &Query) -> Result<ExecutionResult>;

    /// Name of this executor for logging/metrics
    fn name(&self) -> &'static str;

    /// Whether this executor supports validation (returns batches)
    fn supports_validation(&self) -> bool {
        true
    }

    /// Get the underlying `SpiceClient` if this is a Flight executor
    /// Used for Flight-specific features like explain plan snapshots
    fn as_spice_client(&self) -> Option<Arc<spiceai::Client>> {
        None
    }

    /// Clone this executor into a Box
    fn clone_box(&self) -> Box<dyn QueryExecutor>;
}

// Implement Clone for Box<dyn QueryExecutor>
impl Clone for Box<dyn QueryExecutor> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ============================================================================
// Flight SQL Executor
// ============================================================================

/// Flight SQL executor - executes queries via Arrow Flight SQL protocol
pub struct FlightExecutor {
    client: Arc<spiceai::Client>,
}

impl FlightExecutor {
    #[must_use]
    pub fn new(client: Arc<spiceai::Client>) -> Self {
        Self { client }
    }
}

impl Clone for FlightExecutor {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
        }
    }
}

#[async_trait]
impl QueryExecutor for FlightExecutor {
    async fn execute(&self, query: &Query) -> Result<ExecutionResult> {
        let start = std::time::Instant::now();

        let mut result_stream = self
            .client
            .sql_with_params(&query.sql, query.get_parameters_batch().transpose()?)
            .await?;

        let mut batches = Vec::new();
        let mut row_count = 0;

        while let Some(batch) = result_stream.try_next().await? {
            let batch_rows = batch.num_rows();
            row_count += batch_rows;
            batches.push(batch);
        }

        Ok(ExecutionResult {
            duration: start.elapsed(),
            row_count,
            batches: Some(batches),
        })
    }

    fn name(&self) -> &'static str {
        "flight"
    }

    fn supports_validation(&self) -> bool {
        true
    }

    fn as_spice_client(&self) -> Option<Arc<spiceai::Client>> {
        Some(Arc::clone(&self.client))
    }

    fn clone_box(&self) -> Box<dyn QueryExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// HTTP SQL Executor
// ============================================================================

/// HTTP SQL executor - executes queries via synchronous /v1/sql endpoint
pub struct HttpExecutor {
    client: reqwest::Client,
    base_url: String,
}

impl HttpExecutor {
    #[must_use]
    pub fn new(client: reqwest::Client, base_url: String) -> Self {
        Self { client, base_url }
    }
}

impl Clone for HttpExecutor {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
        }
    }
}

#[async_trait]
impl QueryExecutor for HttpExecutor {
    async fn execute(&self, query: &Query) -> Result<ExecutionResult> {
        let start = std::time::Instant::now();
        let sql_text = query.to_sql_with_inlined_params();
        let sql_url = format!("{}/v1/sql", self.base_url);

        let response = self
            .client
            .post(&sql_url)
            .header("Accept", "application/vnd.spiceai.sql.v1+json")
            .body(sql_text.to_string())
            .send()
            .await?;

        let status = response.status();
        let response_text = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("HTTP request failed: {status} - {response_text}");
        }

        let duration = start.elapsed();

        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;
        let row_count = response_json
            .get("row_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("No row_count in HTTP response"))?;

        #[expect(clippy::cast_possible_truncation)]
        let row_count = row_count as usize;

        Ok(ExecutionResult {
            duration,
            row_count,
            batches: None, // HTTP endpoint doesn't return full batches
        })
    }

    fn name(&self) -> &'static str {
        "http"
    }

    fn supports_validation(&self) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn QueryExecutor> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Distributed Query Executor
// ============================================================================

/// Maximum interval between status polls for distributed queries (caps exponential backoff)
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Maximum time to wait for a distributed query to complete (1 hour)
const POLL_TIMEOUT: Duration = Duration::from_hours(1);

/// Distributed query executor - executes queries via async /v1/queries endpoint
pub struct DistributedExecutor {
    client: reqwest::Client,
    base_url: String,
}

impl DistributedExecutor {
    #[must_use]
    pub fn new(client: reqwest::Client, base_url: String) -> Self {
        Self { client, base_url }
    }
}

impl Clone for DistributedExecutor {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
        }
    }
}

#[async_trait]
impl QueryExecutor for DistributedExecutor {
    async fn execute(&self, query: &Query) -> Result<ExecutionResult> {
        let start = std::time::Instant::now();
        let sql_text = query.to_sql_with_inlined_params();
        let queries_url = format!("{}/v1/queries", self.base_url);

        // Step 1: Submit the query
        let submit_body = serde_json::json!({
            "sql": sql_text,
        });

        let submit_response = self
            .client
            .post(&queries_url)
            .header("Content-Type", "application/json")
            .json(&submit_body)
            .send()
            .await?;

        let submit_status = submit_response.status();
        if !submit_status.is_success() {
            let error_text = submit_response.text().await.unwrap_or_default();
            anyhow::bail!("Query distributed submit failed: {submit_status} - {error_text}");
        }

        let submit_json: serde_json::Value = submit_response.json().await?;
        let query_id = submit_json
            .get("query_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("No query_id in submit response"))?;

        // Step 2: Poll for completion
        let status_url = format!("{queries_url}/{query_id}/status");
        let mut poll_interval = Duration::from_millis(100);

        let poll_start = std::time::Instant::now();
        loop {
            if poll_start.elapsed() > POLL_TIMEOUT {
                anyhow::bail!("Query timed out waiting for distributed execution");
            }

            let status_response = self.client.get(&status_url).send().await?;

            if !status_response.status().is_success() {
                let error_text = status_response.text().await.unwrap_or_default();
                anyhow::bail!("Query status check failed: {error_text}");
            }

            let status_json: serde_json::Value = status_response.json().await?;
            // The /v1/queries status response uses JSON field `status` (not
            // `state`) with values serialized as SCREAMING_SNAKE_CASE
            // (see `runtime/src/jobs/state.rs::JobStatus`). Matching either
            // the wrong key or the wrong casing makes every poll fall through
            // to "keep waiting" and the query never exits the loop until the
            // hard timeout.
            let status = status_json
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("UNKNOWN");

            match status {
                "SUCCEEDED" => break,
                "FAILED" => {
                    let error_msg = status_json
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("Unknown error");
                    anyhow::bail!("Query distributed execution failed: {error_msg}");
                }
                "CANCELLED" => anyhow::bail!("Query was cancelled"),
                "CLOSED" => anyhow::bail!("Query results expired before retrieval"),
                "PENDING" | "RUNNING" => {
                    // Continue polling with exponential backoff
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = std::cmp::min(poll_interval * 2, MAX_POLL_INTERVAL);
                }
                _ => {
                    // Unknown status, continue polling
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = std::cmp::min(poll_interval * 2, MAX_POLL_INTERVAL);
                }
            }
        }

        // Step 3: Fetch total row count from the full query response.
        // `/results` returns a ChunkResponse with per-chunk counts; the manifest
        // (with `total_row_count`) lives on the query-level response at
        // `GET /v1/queries/{id}`. See `runtime/src/http/v1/queries.rs::QueryResponse`.
        let query_url = format!("{queries_url}/{query_id}");
        let query_response = self.client.get(&query_url).send().await?;

        let query_status = query_response.status();
        if !query_status.is_success() {
            let error_text = query_response.text().await.unwrap_or_default();
            anyhow::bail!("Query info fetch failed: {query_status} - {error_text}");
        }

        let query_json: serde_json::Value = query_response.json().await?;

        let manifest = query_json
            .get("manifest")
            .ok_or_else(|| anyhow::anyhow!("Query response missing 'manifest' field"))?;

        let total_row_count_value = manifest
            .get("total_row_count")
            .ok_or_else(|| anyhow::anyhow!("Query manifest missing 'total_row_count' field"))?;

        let total_row_count_u64 = total_row_count_value.as_u64().ok_or_else(|| {
            anyhow::anyhow!("Query manifest 'total_row_count' field is not a valid u64")
        })?;

        #[expect(clippy::cast_possible_truncation)]
        let row_count = total_row_count_u64 as usize;

        Ok(ExecutionResult {
            duration: start.elapsed(),
            row_count,
            batches: None, // Distributed mode doesn't return batches
        })
    }

    fn name(&self) -> &'static str {
        "distributed"
    }

    fn supports_validation(&self) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn QueryExecutor> {
        Box::new(self.clone())
    }
}
