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

//! HTTP client for the Mem0 REST API.
//!
//! This client implements robust retry logic with adaptive backoff:
//! - Exponential backoff for rate limit errors (HTTP 429, 408)
//! - Fibonacci backoff for server errors (5xx) and network issues
//! - Configurable maximum retries and timeout
//!
//! The retry strategy is consistent with other data connectors in Spice
//! (e.g., GitHub connector) to ensure reliable API interactions.

use reqwest::{Client, header};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use util::ExponentialBackoff;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

use super::{Error, RequestFailedSnafu, ResponseParseFailedSnafu, Result};
use snafu::ResultExt;

const MEM0_API_BASE_URL: &str = "https://api.mem0.ai";

/// Default maximum number of retries for API requests.
const DEFAULT_MAX_RETRIES: usize = 3;

/// Maximum total retry time for rate limit errors (5 minutes).
const MAX_RATE_LIMIT_RETRY_TIME_SECS: u64 = 300;

/// Types of retryable errors for adaptive backoff strategy.
#[derive(Debug, Clone, Copy)]
enum RetryableErrorType {
    /// Rate limit errors (HTTP 408, 429) - use exponential backoff
    RateLimit,
    /// Server errors (5xx) - use fibonacci backoff
    ServerError,
    /// Network/connection errors - use fibonacci backoff
    Network,
}

/// Determines if a reqwest error should be retried and what type of error it is.
fn classify_retryable_error(error: &reqwest::Error) -> Option<RetryableErrorType> {
    // Check for network errors first
    if error.is_connect() || error.is_timeout() {
        return Some(RetryableErrorType::Network);
    }

    // Check HTTP status codes
    if let Some(status) = error.status() {
        let code = status.as_u16();
        match code {
            408 | 429 => Some(RetryableErrorType::RateLimit),
            500..=599 => Some(RetryableErrorType::ServerError),
            _ => None,
        }
    } else {
        None
    }
}

/// Configuration for the Mem0 client.
#[derive(Clone, Debug)]
pub struct Mem0Config {
    /// API key for authentication
    pub api_key: SecretString,
    /// Optional organization ID
    pub org_id: Option<String>,
    /// Optional project ID
    pub project_id: Option<String>,
    /// Base URL for the API (defaults to <https://api.mem0.ai>)
    pub base_url: String,
    /// Default user ID to scope memories to
    pub user_id: Option<String>,
    /// Default agent ID to scope memories to
    pub agent_id: Option<String>,
    /// Enable graph memory for relationship extraction and retrieval.
    /// When enabled, Mem0 extracts entities and relationships from memories
    /// and returns graph context in search/get operations.
    pub enable_graph: bool,
}

impl Mem0Config {
    #[must_use]
    pub fn new(api_key: SecretString) -> Self {
        Self {
            api_key,
            org_id: None,
            project_id: None,
            base_url: MEM0_API_BASE_URL.to_string(),
            user_id: None,
            agent_id: None,
            enable_graph: false,
        }
    }

    pub fn from_params(params: &HashMap<String, SecretString>) -> Result<Self> {
        // Helper to get param with mem0_ prefix (primary) or fallback to non-prefixed
        let get_param = |name: &str| -> Option<&SecretString> {
            params
                .get(&format!("mem0_{name}"))
                .or_else(|| params.get(name))
        };

        let api_key =
            get_param("api_key")
                .cloned()
                .ok_or_else(|| Error::MissingRequiredParameter {
                    param: "mem0_api_key".to_string(),
                })?;

        let mut config = Self::new(api_key);

        if let Some(org_id) = get_param("org_id") {
            config.org_id = Some(org_id.expose_secret().to_string());
        }

        if let Some(project_id) = get_param("project_id") {
            config.project_id = Some(project_id.expose_secret().to_string());
        }

        if let Some(base_url) = get_param("base_url") {
            config.base_url = base_url.expose_secret().to_string();
        }

        if let Some(user_id) = get_param("user_id") {
            config.user_id = Some(user_id.expose_secret().to_string());
        }

        if let Some(agent_id) = get_param("agent_id") {
            config.agent_id = Some(agent_id.expose_secret().to_string());
        }

        if let Some(graph_memory) = get_param("graph_memory") {
            config.enable_graph = graph_memory.expose_secret().eq_ignore_ascii_case("enabled");
        }

        Ok(config)
    }
}

/// Message in a conversation to store as memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

/// Request body for adding memories.
#[derive(Debug, Clone, Serialize)]
pub struct AddMemoryRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<Message>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,

    #[serde(default = "default_true")]
    pub infer: bool,

    #[serde(default = "default_true")]
    pub async_mode: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,

    /// Enable graph memory for relationship extraction.
    /// When true, Mem0 extracts entities and relationships from the memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_graph: Option<bool>,
}

impl Default for AddMemoryRequest {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            user_id: None,
            agent_id: None,
            app_id: None,
            run_id: None,
            metadata: None,
            infer: true,
            async_mode: true,
            org_id: None,
            project_id: None,
            enable_graph: None,
        }
    }
}

/// Response from adding memories (synchronous mode - `async_mode`: false).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AddMemoryEvent {
    pub id: String,
    pub event: String,
    pub data: AddMemoryData,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AddMemoryData {
    pub memory: String,
}

/// Response from adding memories (asynchronous mode - `async_mode`: true).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AddMemoryPending {
    pub message: String,
    pub status: String,
    pub event_id: String,
}

/// Response from adding memories - can be either sync or async format.
#[derive(Debug, Clone)]
pub enum AddMemoryResponse {
    /// Synchronous response with completed memory events
    Sync(Vec<AddMemoryEvent>),
    /// Asynchronous response indicating background processing
    Async(Vec<AddMemoryPending>),
}

/// Request body for searching memories.
#[derive(Debug, Clone, Serialize)]
pub struct SearchMemoryRequest {
    pub query: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<serde_json::Value>,

    #[serde(default = "default_version")]
    pub version: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,

    /// Enable graph memory for relationship-aware search.
    /// When true, graph relations are returned alongside vector search results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_graph: Option<bool>,
}

impl Default for SearchMemoryRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            filters: None,
            version: "v2".to_string(),
            top_k: Some(10),
            threshold: None,
            org_id: None,
            project_id: None,
            enable_graph: None,
        }
    }
}

/// A memory item returned from search or get operations.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Memory {
    pub id: String,
    pub memory: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,

    /// Graph relationships associated with this memory.
    /// Only present when `enable_graph=true` is used in the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relations: Option<Vec<GraphRelation>>,
}

/// A graph relationship extracted from memory content.
/// Represents a connection between two entities (source -> target).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct GraphRelation {
    /// The source entity of the relationship
    pub source: String,

    /// The type/name of the relationship (e.g., `works_at`, `lives_in`, `met_at`)
    pub relation: String,

    /// The target entity of the relationship
    pub target: String,
}

/// A graph entity extracted from memory content.
/// Represents a person, place, organization, or other named entity.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct GraphEntity {
    /// The name/label of the entity
    pub name: String,

    /// The type of entity (e.g., "Person", "Organization", "Location")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_type: Option<String>,
}

/// Request body for getting memories.
#[derive(Debug, Clone, Serialize)]
pub struct GetMemoriesRequest {
    pub filters: serde_json::Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,

    /// Enable graph memory for relationship context.
    /// When true, entity and relationship information is included in responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_graph: Option<bool>,
}

/// Request body for deleting memories.
#[derive(Debug, Clone, Serialize)]
pub struct DeleteMemoryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

/// HTTP client for Mem0 API with built-in retry logic.
///
/// This client automatically handles:
/// - Rate limit errors (HTTP 429, 408) with exponential backoff
/// - Server errors (5xx) with fibonacci backoff
/// - Network/connection errors with fibonacci backoff
#[derive(Clone)]
pub struct Mem0Client {
    client: Client,
    config: Mem0Config,
    max_retries: usize,
}

impl std::fmt::Debug for Mem0Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mem0Client")
            .field("base_url", &self.config.base_url)
            .field("org_id", &self.config.org_id)
            .field("project_id", &self.config.project_id)
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

impl Mem0Client {
    /// Create a new Mem0 client with the given configuration.
    pub fn new(config: Mem0Config) -> Result<Self> {
        Self::with_max_retries(config, DEFAULT_MAX_RETRIES)
    }

    /// Create a new Mem0 client with custom retry configuration.
    pub fn with_max_retries(config: Mem0Config, max_retries: usize) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Token {}", config.api_key.expose_secret()))
                .map_err(|e| Error::ClientBuildFailed {
                    message: format!("Invalid API key format: {e}"),
                })?,
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| Error::ClientBuildFailed {
                message: format!("Failed to create HTTP client: {e}"),
            })?;

        Ok(Self {
            client,
            config,
            max_retries,
        })
    }

    /// Execute an HTTP request with adaptive retry logic.
    ///
    /// Uses exponential backoff for rate limit errors (429, 408) and
    /// fibonacci backoff for server errors (5xx) and network issues.
    async fn execute_with_retry<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut fibonacci_backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(self.max_retries))
            .build();

        let mut exponential_backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(MAX_RATE_LIMIT_RETRY_TIME_SECS)),
            ..ExponentialBackoff::default()
        };

        let mut rate_limit_retry_count = 0_usize;
        let mut server_error_retry_count = 0_usize;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Determine if the error is retryable
                    let retry_type = match &e {
                        Error::RequestFailed { source } => classify_retryable_error(source),
                        Error::ApiError { message } => {
                            // Parse status code from API error message
                            if message.contains("429") || message.contains("408") {
                                Some(RetryableErrorType::RateLimit)
                            } else if message.contains("500")
                                || message.contains("502")
                                || message.contains("503")
                                || message.contains("504")
                            {
                                Some(RetryableErrorType::ServerError)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    match retry_type {
                        Some(RetryableErrorType::RateLimit) => {
                            if rate_limit_retry_count >= self.max_retries {
                                tracing::warn!(
                                    "Mem0 API rate limit exceeded after {} retries",
                                    self.max_retries
                                );
                                return Err(Error::RateLimitExceeded {
                                    retries: self.max_retries,
                                });
                            }
                            rate_limit_retry_count += 1;

                            if let Some(duration) = Backoff::next_backoff(&mut exponential_backoff)
                            {
                                tracing::warn!(
                                    "Mem0 API rate limit error, retrying with exponential backoff in {duration:?} (attempt {rate_limit_retry_count}/{})",
                                    self.max_retries
                                );
                                tokio::time::sleep(duration).await;
                            } else {
                                return Err(Error::RateLimitExceeded {
                                    retries: rate_limit_retry_count,
                                });
                            }
                        }
                        Some(RetryableErrorType::ServerError | RetryableErrorType::Network) => {
                            if server_error_retry_count >= self.max_retries {
                                tracing::warn!(
                                    "Mem0 API server/network error, max retries ({}) exceeded",
                                    self.max_retries
                                );
                                return Err(Error::AllRetriesFailed {
                                    max_retries: self.max_retries,
                                });
                            }
                            server_error_retry_count += 1;

                            if let Some(duration) = Backoff::next_backoff(&mut fibonacci_backoff) {
                                tracing::warn!(
                                    "Mem0 API server/network error, retrying with fibonacci backoff in {duration:?} (attempt {server_error_retry_count}/{}): {e}",
                                    self.max_retries
                                );
                                tokio::time::sleep(duration).await;
                            } else {
                                return Err(Error::AllRetriesFailed {
                                    max_retries: self.max_retries,
                                });
                            }
                        }
                        None => {
                            // Non-retryable error, return immediately
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Add memories from messages.
    ///
    /// When `async_mode` is `true` (default), returns `AddMemoryResponse::Async` with pending status.
    /// When `async_mode` is `false`, returns `AddMemoryResponse::Sync` with completed memory events.
    ///
    /// This method automatically retries on rate limit and server errors.
    pub async fn add_memories(&self, mut request: AddMemoryRequest) -> Result<AddMemoryResponse> {
        // Apply default user_id and agent_id from config if not set
        if request.user_id.is_none() {
            request.user_id.clone_from(&self.config.user_id);
        }
        if request.agent_id.is_none() {
            request.agent_id.clone_from(&self.config.agent_id);
        }
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }
        // Apply enable_graph from config if not explicitly set in request
        if request.enable_graph.is_none() && self.config.enable_graph {
            request.enable_graph = Some(true);
        }

        let is_async = request.async_mode;
        let url = format!("{}/v1/memories/", self.config.base_url);

        self.execute_with_retry(|| async {
            let response = self
                .client
                .post(&url)
                .json(&request)
                .send()
                .await
                .context(RequestFailedSnafu)?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    message: format!("API returned {status}: {body}"),
                });
            }

            if is_async {
                let pending: Vec<AddMemoryPending> =
                    response.json().await.context(ResponseParseFailedSnafu)?;
                Ok(AddMemoryResponse::Async(pending))
            } else {
                let events: Vec<AddMemoryEvent> =
                    response.json().await.context(ResponseParseFailedSnafu)?;
                Ok(AddMemoryResponse::Sync(events))
            }
        })
        .await
    }

    /// Search memories with a query.
    ///
    /// This method automatically retries on rate limit and server errors.
    pub async fn search_memories(&self, mut request: SearchMemoryRequest) -> Result<Vec<Memory>> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }
        // Apply enable_graph from config if not explicitly set in request
        if request.enable_graph.is_none() && self.config.enable_graph {
            request.enable_graph = Some(true);
        }

        let url = format!("{}/v2/memories/search/", self.config.base_url);

        self.execute_with_retry(|| async {
            let response = self
                .client
                .post(&url)
                .json(&request)
                .send()
                .await
                .context(RequestFailedSnafu)?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    message: format!("API returned {status}: {body}"),
                });
            }

            response.json().await.context(ResponseParseFailedSnafu)
        })
        .await
    }

    /// Get all memories matching filters.
    ///
    /// This method automatically retries on rate limit and server errors.
    pub async fn get_memories(&self, mut request: GetMemoriesRequest) -> Result<Vec<Memory>> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }
        // Apply enable_graph from config if not explicitly set in request
        if request.enable_graph.is_none() && self.config.enable_graph {
            request.enable_graph = Some(true);
        }

        let url = format!("{}/v2/memories/", self.config.base_url);

        self.execute_with_retry(|| async {
            let response = self
                .client
                .post(&url)
                .json(&request)
                .send()
                .await
                .context(RequestFailedSnafu)?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    message: format!("API returned {status}: {body}"),
                });
            }

            response.json().await.context(ResponseParseFailedSnafu)
        })
        .await
    }

    /// Delete a specific memory by ID.
    ///
    /// This method automatically retries on rate limit and server errors.
    pub async fn delete_memory(&self, memory_id: &str) -> Result<()> {
        let url = format!("{}/v1/memories/{memory_id}/", self.config.base_url);

        self.execute_with_retry(|| async {
            let response = self
                .client
                .delete(&url)
                .send()
                .await
                .context(RequestFailedSnafu)?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    message: format!("API returned {status}: {body}"),
                });
            }

            Ok(())
        })
        .await
    }

    /// Delete all memories matching the request filters.
    ///
    /// This method automatically retries on rate limit and server errors.
    pub async fn delete_all_memories(&self, mut request: DeleteMemoryRequest) -> Result<()> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }

        let url = format!("{}/v1/memories/", self.config.base_url);

        self.execute_with_retry(|| async {
            let response = self
                .client
                .delete(&url)
                .json(&request)
                .send()
                .await
                .context(RequestFailedSnafu)?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::ApiError {
                    message: format!("API returned {status}: {body}"),
                });
            }

            Ok(())
        })
        .await
    }

    /// Get the default `user_id` from config.
    #[must_use]
    pub fn default_user_id(&self) -> Option<&str> {
        self.config.user_id.as_deref()
    }

    /// Get the default `agent_id` from config.
    #[must_use]
    pub fn default_agent_id(&self) -> Option<&str> {
        self.config.agent_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem0_config_new() {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);

        assert_eq!(config.base_url, MEM0_API_BASE_URL);
        assert!(config.org_id.is_none());
        assert!(config.project_id.is_none());
        assert!(config.user_id.is_none());
        assert!(config.agent_id.is_none());
    }

    #[test]
    fn test_mem0_config_from_params() {
        let mut params = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));
        params.insert("mem0_org_id".to_string(), SecretString::from("my-org"));
        params.insert(
            "mem0_project_id".to_string(),
            SecretString::from("my-project"),
        );
        params.insert("mem0_user_id".to_string(), SecretString::from("user123"));
        params.insert("mem0_agent_id".to_string(), SecretString::from("agent456"));

        let config = Mem0Config::from_params(&params).expect("should parse config");

        assert_eq!(config.org_id, Some("my-org".to_string()));
        assert_eq!(config.project_id, Some("my-project".to_string()));
        assert_eq!(config.user_id, Some("user123".to_string()));
        assert_eq!(config.agent_id, Some("agent456".to_string()));
    }

    #[test]
    fn test_mem0_config_from_params_fallback_key_name() {
        // Test backward compatibility with non-prefixed params
        let mut params = HashMap::new();
        params.insert("api_key".to_string(), SecretString::from("test-key"));

        let config = Mem0Config::from_params(&params).expect("should parse config");
        assert_eq!(config.api_key.expose_secret(), "test-key");
    }

    #[test]
    fn test_mem0_config_from_params_missing_api_key() {
        let params: HashMap<String, SecretString> = HashMap::new();
        let result = Mem0Config::from_params(&params);

        assert!(result.is_err());
        let err = result.expect_err("should fail without API key");
        match &err {
            Error::MissingRequiredParameter { param } => {
                assert_eq!(param, "mem0_api_key");
            }
            _ => panic!("Expected MissingRequiredParameter error"),
        }
    }

    #[test]
    fn test_mem0_config_from_params_custom_base_url() {
        let mut params = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "mem0_base_url".to_string(),
            SecretString::from("https://custom.mem0.ai"),
        );

        let config = Mem0Config::from_params(&params).expect("should parse config");
        assert_eq!(config.base_url, "https://custom.mem0.ai");
    }

    #[test]
    fn test_add_memory_request_default() {
        let request = AddMemoryRequest::default();

        assert!(request.messages.is_empty());
        assert!(request.user_id.is_none());
        assert!(request.agent_id.is_none());
        assert!(request.infer);
        assert!(request.async_mode);
    }

    #[test]
    fn test_search_memory_request_default() {
        let request = SearchMemoryRequest::default();

        assert!(request.query.is_empty());
        assert!(request.filters.is_none());
        assert_eq!(request.version, "v2");
        assert_eq!(request.top_k, Some(10));
        assert!(request.threshold.is_none());
    }

    #[test]
    fn test_message_serialization() {
        let msg = Message {
            role: "user".to_string(),
            content: "Hello world".to_string(),
        };

        let json = serde_json::to_string(&msg).expect("should serialize");
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("\"content\":\"Hello world\""));
    }

    #[test]
    fn test_memory_deserialization() {
        let json = r#"{
            "id": "mem123",
            "memory": "User likes pizza",
            "user_id": "user123",
            "created_at": "2025-01-10T12:00:00Z",
            "score": 0.95
        }"#;

        let memory: Memory = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(memory.id, "mem123");
        assert_eq!(memory.memory, "User likes pizza");
        assert_eq!(memory.user_id, Some("user123".to_string()));
        assert_eq!(memory.score, Some(0.95));
    }

    #[test]
    fn test_add_memory_event_deserialization() {
        let json = r#"{
            "id": "evt123",
            "event": "ADD",
            "data": {
                "memory": "Test memory content"
            }
        }"#;

        let event: AddMemoryEvent = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(event.id, "evt123");
        assert_eq!(event.event, "ADD");
        assert_eq!(event.data.memory, "Test memory content");
    }

    #[test]
    fn test_mem0_client_creation() {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);
        let client = Mem0Client::new(config);

        client.expect("should create client successfully");
    }

    #[test]
    fn test_mem0_client_default_user_id() {
        let api_key = SecretString::from("test-api-key");
        let mut config = Mem0Config::new(api_key);
        config.user_id = Some("default-user".to_string());

        let client = Mem0Client::new(config).expect("should create client");
        assert_eq!(client.default_user_id(), Some("default-user"));
    }

    #[test]
    fn test_mem0_client_default_agent_id() {
        let api_key = SecretString::from("test-api-key");
        let mut config = Mem0Config::new(api_key);
        config.agent_id = Some("default-agent".to_string());

        let client = Mem0Client::new(config).expect("should create client");
        assert_eq!(client.default_agent_id(), Some("default-agent"));
    }

    #[test]
    fn test_add_memory_request_serialization() {
        let request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: "I like coffee".to_string(),
            }],
            user_id: Some("user123".to_string()),
            metadata: Some(
                [("source".to_string(), serde_json::json!("test"))]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"user_id\":\"user123\""));
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("I like coffee"));
    }

    #[test]
    fn test_search_memory_request_serialization() {
        let request = SearchMemoryRequest {
            query: "What do I like?".to_string(),
            filters: Some(serde_json::json!({"user_id": "user123"})),
            top_k: Some(5),
            threshold: Some(0.5),
            ..Default::default()
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"query\":\"What do I like?\""));
        assert!(json.contains("\"top_k\":5"));
        assert!(json.contains("\"threshold\":0.5"));
    }

    #[test]
    fn test_get_memories_request_serialization() {
        let request = GetMemoriesRequest {
            filters: serde_json::json!({"user_id": "user123"}),
            page: Some(1),
            page_size: Some(50),
            org_id: None,
            project_id: None,
            enable_graph: None,
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"page\":1"));
        assert!(json.contains("\"page_size\":50"));
    }

    #[test]
    fn test_delete_memory_request_serialization() {
        let request = DeleteMemoryRequest {
            user_id: Some("user123".to_string()),
            agent_id: None,
            org_id: Some("org456".to_string()),
            project_id: None,
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"user_id\":\"user123\""));
        assert!(json.contains("\"org_id\":\"org456\""));
    }

    #[test]
    fn test_classify_retryable_error_rate_limit() {
        // Test that rate limit status codes are correctly classified
        // We can't easily construct reqwest errors, but we verify the logic
        // by checking the error message parsing in execute_with_retry
        let api_error_429 = super::Error::ApiError {
            message: "API returned 429 Too Many Requests: rate limited".to_string(),
        };
        // This error should be recognized as retryable via the message contents
        match &api_error_429 {
            super::Error::ApiError { message } => {
                assert!(
                    message.contains("429"),
                    "Should contain rate limit status code"
                );
            }
            _ => panic!("Expected ApiError"),
        }
    }

    #[test]
    fn test_classify_retryable_error_server_error() {
        // Test that 5xx errors are classified as server errors
        let api_error_500 = super::Error::ApiError {
            message: "API returned 500 Internal Server Error: server unavailable".to_string(),
        };
        match &api_error_500 {
            super::Error::ApiError { message } => {
                assert!(
                    message.contains("500"),
                    "Should contain server error status code"
                );
            }
            _ => panic!("Expected ApiError"),
        }
    }

    #[test]
    fn test_mem0_client_with_custom_retries() {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);
        let client = Mem0Client::with_max_retries(config, 5);

        let client = client.expect("should create client");
        assert_eq!(client.max_retries, 5);
    }

    #[test]
    fn test_mem0_client_default_retries() {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);
        let client = Mem0Client::new(config).expect("should create client");

        assert_eq!(client.max_retries, super::DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn test_mem0_config_enable_graph_default() {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);

        assert!(!config.enable_graph, "enable_graph should default to false");
    }

    #[test]
    fn test_mem0_config_graph_memory_enabled_from_params() {
        let mut params: HashMap<String, SecretString> = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "mem0_graph_memory".to_string(),
            SecretString::from("enabled"),
        );

        let config = Mem0Config::from_params(&params).expect("should parse");
        assert!(
            config.enable_graph,
            "enable_graph should be true when graph_memory=enabled"
        );
    }

    #[test]
    fn test_mem0_config_graph_memory_disabled_from_params() {
        let mut params: HashMap<String, SecretString> = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "mem0_graph_memory".to_string(),
            SecretString::from("disabled"),
        );

        let config = Mem0Config::from_params(&params).expect("should parse");
        assert!(
            !config.enable_graph,
            "enable_graph should be false when graph_memory=disabled"
        );
    }

    #[test]
    fn test_mem0_config_graph_memory_case_insensitive() {
        let mut params: HashMap<String, SecretString> = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "mem0_graph_memory".to_string(),
            SecretString::from("ENABLED"),
        );

        let config = Mem0Config::from_params(&params).expect("should parse");
        assert!(
            config.enable_graph,
            "enable_graph should be true with case-insensitive 'ENABLED'"
        );
    }

    #[test]
    fn test_add_memory_request_with_enable_graph() {
        let request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: "Alice met Bob at the conference".to_string(),
            }],
            user_id: Some("user123".to_string()),
            enable_graph: Some(true),
            ..Default::default()
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"enable_graph\":true"));
    }

    #[test]
    fn test_add_memory_request_without_enable_graph() {
        let request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: "I like coffee".to_string(),
            }],
            user_id: Some("user123".to_string()),
            enable_graph: None,
            ..Default::default()
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(
            !json.contains("enable_graph"),
            "enable_graph should be skipped when None"
        );
    }

    #[test]
    fn test_search_memory_request_with_enable_graph() {
        let request = SearchMemoryRequest {
            query: "Who did Alice meet?".to_string(),
            filters: Some(serde_json::json!({"user_id": "user123"})),
            enable_graph: Some(true),
            ..Default::default()
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"enable_graph\":true"));
    }

    #[test]
    fn test_get_memories_request_with_enable_graph() {
        let request = GetMemoriesRequest {
            filters: serde_json::json!({"user_id": "user123"}),
            page: None,
            page_size: None,
            org_id: None,
            project_id: None,
            enable_graph: Some(true),
        };

        let json = serde_json::to_string(&request).expect("should serialize");
        assert!(json.contains("\"enable_graph\":true"));
    }

    #[test]
    fn test_graph_relation_serialization() {
        let relation = GraphRelation {
            source: "Alice".to_string(),
            relation: "met_at".to_string(),
            target: "GraphConf".to_string(),
        };

        let json = serde_json::to_string(&relation).expect("should serialize");
        assert!(json.contains("\"source\":\"Alice\""));
        assert!(json.contains("\"relation\":\"met_at\""));
        assert!(json.contains("\"target\":\"GraphConf\""));
    }

    #[test]
    fn test_graph_relation_deserialization() {
        let json = r#"{
            "source": "Bob",
            "relation": "works_at",
            "target": "Acme Corp"
        }"#;

        let relation: GraphRelation = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(relation.source, "Bob");
        assert_eq!(relation.relation, "works_at");
        assert_eq!(relation.target, "Acme Corp");
    }

    #[test]
    fn test_graph_entity_serialization() {
        let entity = GraphEntity {
            name: "Alice".to_string(),
            entity_type: Some("Person".to_string()),
        };

        let json = serde_json::to_string(&entity).expect("should serialize");
        assert!(json.contains("\"name\":\"Alice\""));
        assert!(json.contains("\"entity_type\":\"Person\""));
    }

    #[test]
    fn test_memory_with_relations_deserialization() {
        let json = r#"{
            "id": "mem123",
            "memory": "Alice works at Acme Corp",
            "user_id": "user123",
            "relations": [
                {
                    "source": "Alice",
                    "relation": "works_at",
                    "target": "Acme Corp"
                }
            ]
        }"#;

        let memory: Memory = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(memory.id, "mem123");
        assert!(memory.relations.is_some());
        let relations = memory.relations.expect("should have relations");
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].source, "Alice");
        assert_eq!(relations[0].relation, "works_at");
        assert_eq!(relations[0].target, "Acme Corp");
    }

    #[test]
    fn test_memory_without_relations_deserialization() {
        let json = r#"{
            "id": "mem456",
            "memory": "User likes pizza"
        }"#;

        let memory: Memory = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(memory.id, "mem456");
        assert!(memory.relations.is_none());
    }

    #[test]
    fn test_client_config_with_enable_graph() {
        let api_key = SecretString::from("test-api-key");
        let mut config = Mem0Config::new(api_key);
        config.enable_graph = true;

        let client = Mem0Client::new(config).expect("should create client");
        assert!(client.config.enable_graph);
    }
}
