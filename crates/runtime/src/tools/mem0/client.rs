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

//! HTTP client for the Mem0 REST API.

use reqwest::{Client, header};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Error, RequestFailedSnafu, ResponseParseFailedSnafu, Result};
use snafu::ResultExt;

const MEM0_API_BASE_URL: &str = "https://api.mem0.ai";

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
        }
    }

    pub fn from_params(params: &HashMap<String, SecretString>) -> Result<Self> {
        let api_key = params
            .get("api_key")
            .or_else(|| params.get("mem0_api_key"))
            .cloned()
            .ok_or_else(|| Error::MissingRequiredParameter {
                param: "api_key".to_string(),
            })?;

        let mut config = Self::new(api_key);

        if let Some(org_id) = params.get("org_id") {
            config.org_id = Some(org_id.expose_secret().to_string());
        }

        if let Some(project_id) = params.get("project_id") {
            config.project_id = Some(project_id.expose_secret().to_string());
        }

        if let Some(base_url) = params.get("base_url") {
            config.base_url = base_url.expose_secret().to_string();
        }

        if let Some(user_id) = params.get("user_id") {
            config.user_id = Some(user_id.expose_secret().to_string());
        }

        if let Some(agent_id) = params.get("agent_id") {
            config.agent_id = Some(agent_id.expose_secret().to_string());
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
}

#[expect(dead_code, reason = "Required by serde default attribute")]
fn default_true() -> bool {
    true
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
        }
    }
}

/// Response from adding memories.
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
}

#[expect(dead_code, reason = "Required by serde default attribute")]
fn default_version() -> String {
    "v2".to_string()
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

/// HTTP client for Mem0 API.
#[derive(Clone)]
pub struct Mem0Client {
    client: Client,
    config: Mem0Config,
}

impl std::fmt::Debug for Mem0Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mem0Client")
            .field("base_url", &self.config.base_url)
            .field("org_id", &self.config.org_id)
            .field("project_id", &self.config.project_id)
            .finish_non_exhaustive()
    }
}

impl Mem0Client {
    /// Create a new Mem0 client with the given configuration.
    pub fn new(config: Mem0Config) -> Result<Self> {
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
            .build()
            .map_err(|e| Error::ClientBuildFailed {
                message: format!("Failed to create HTTP client: {e}"),
            })?;

        Ok(Self { client, config })
    }

    /// Add memories from messages.
    pub async fn add_memories(&self, mut request: AddMemoryRequest) -> Result<Vec<AddMemoryEvent>> {
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

        let url = format!("{}/v1/memories/", self.config.base_url);

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
    }

    /// Search memories with a query.
    pub async fn search_memories(&self, mut request: SearchMemoryRequest) -> Result<Vec<Memory>> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }

        let url = format!("{}/v2/memories/search", self.config.base_url);

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
    }

    /// Get all memories matching filters.
    pub async fn get_memories(&self, mut request: GetMemoriesRequest) -> Result<Vec<Memory>> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }

        let url = format!("{}/v2/memories/", self.config.base_url);

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
    }

    /// Delete a specific memory by ID.
    pub async fn delete_memory(&self, memory_id: &str) -> Result<()> {
        let url = format!("{}/v1/memories/{memory_id}/", self.config.base_url);

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
    }

    /// Delete all memories matching the request filters.
    pub async fn delete_all_memories(&self, mut request: DeleteMemoryRequest) -> Result<()> {
        if request.org_id.is_none() {
            request.org_id.clone_from(&self.config.org_id);
        }
        if request.project_id.is_none() {
            request.project_id.clone_from(&self.config.project_id);
        }

        let url = format!("{}/v1/memories/", self.config.base_url);

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
        params.insert("api_key".to_string(), SecretString::from("test-key"));
        params.insert("org_id".to_string(), SecretString::from("my-org"));
        params.insert("project_id".to_string(), SecretString::from("my-project"));
        params.insert("user_id".to_string(), SecretString::from("user123"));
        params.insert("agent_id".to_string(), SecretString::from("agent456"));

        let config = Mem0Config::from_params(&params).expect("should parse config");

        assert_eq!(config.org_id, Some("my-org".to_string()));
        assert_eq!(config.project_id, Some("my-project".to_string()));
        assert_eq!(config.user_id, Some("user123".to_string()));
        assert_eq!(config.agent_id, Some("agent456".to_string()));
    }

    #[test]
    fn test_mem0_config_from_params_alternate_key_name() {
        let mut params = HashMap::new();
        params.insert("mem0_api_key".to_string(), SecretString::from("test-key"));

        let config = Mem0Config::from_params(&params).expect("should parse config");
        assert_eq!(config.api_key.expose_secret(), "test-key");
    }

    #[test]
    fn test_mem0_config_from_params_missing_api_key() {
        let params: HashMap<String, SecretString> = HashMap::new();
        let result = Mem0Config::from_params(&params);

        assert!(result.is_err());
        let err = result.expect_err("should fail without API key");
        assert!(matches!(err, Error::MissingRequiredParameter { .. }));
    }

    #[test]
    fn test_mem0_config_from_params_custom_base_url() {
        let mut params = HashMap::new();
        params.insert("api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "base_url".to_string(),
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
}
