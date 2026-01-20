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

//! Mem0 memory tools for LLM interactions.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::SpiceModelTool;

use super::client::{
    AddMemoryRequest, AddMemoryResponse, DeleteMemoryRequest, GetMemoriesRequest, Mem0Client,
    Message, SearchMemoryRequest,
};

/// Generate JSON schema parameters for a type.
fn parameters<T: JsonSchema>() -> Option<Value> {
    let schema = schemars::schema_for!(T);
    serde_json::to_value(schema).ok()
}

/// Parameters for the add memory tool.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AddMemoryParams {
    /// The content to store as a memory. Can be a single thought or observation.
    pub content: String,

    /// Optional role for the message (defaults to "user").
    #[serde(default = "default_role")]
    pub role: String,

    /// Optional user ID to associate this memory with.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Optional metadata to attach to the memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

fn default_role() -> String {
    "user".to_string()
}

/// Tool for adding memories to Mem0.
pub struct AddMemoryTool {
    name: String,
    description: String,
    client: Arc<Mem0Client>,
}

impl AddMemoryTool {
    #[must_use]
    pub fn new(client: Arc<Mem0Client>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            client,
            name: name.unwrap_or("add_memory").to_string(),
            description: description
                .unwrap_or(
                    "Store a new memory or observation for future retrieval. Use this to remember important information from conversations.",
                )
                .to_string(),
        }
    }
}

impl std::fmt::Debug for AddMemoryTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddMemoryTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpiceModelTool for AddMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<AddMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mem0_add_memory", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: AddMemoryParams = serde_json::from_str(arg).boxed()?;

            let request = AddMemoryRequest {
                messages: vec![Message {
                    role: params.role,
                    content: params.content,
                }],
                user_id: params
                    .user_id
                    .or_else(|| self.client.default_user_id().map(ToString::to_string)),
                metadata: params.metadata.and_then(|v| {
                    v.as_object()
                        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                }),
                async_mode: false, // Use sync mode to get immediate results
                ..Default::default()
            };

            let response = self.client.add_memories(request).await.boxed()?;

            match response {
                AddMemoryResponse::Sync(events) => Ok(json!({
                    "success": true,
                    "memories_added": events.len(),
                    "events": events
                })),
                AddMemoryResponse::Async(pending) => Ok(json!({
                    "success": true,
                    "status": "pending",
                    "pending_events": pending
                })),
            }
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, error = %e);
                Err(e)
            }
        }
    }
}

/// Parameters for the search memory tool.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SearchMemoryParams {
    /// The search query to find relevant memories.
    pub query: String,

    /// Optional user ID to filter memories by.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Maximum number of results to return (default: 10).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<i32>,

    /// Minimum similarity threshold (0.0 to 1.0, default: 0.3).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
}

/// Tool for searching memories in Mem0.
pub struct SearchMemoryTool {
    name: String,
    description: String,
    client: Arc<Mem0Client>,
}

impl SearchMemoryTool {
    #[must_use]
    pub fn new(client: Arc<Mem0Client>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            client,
            name: name.unwrap_or("search_memory").to_string(),
            description: description
                .unwrap_or(
                    "Search for relevant memories based on a query. Use this to recall previously stored information.",
                )
                .to_string(),
        }
    }
}

impl std::fmt::Debug for SearchMemoryTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchMemoryTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpiceModelTool for SearchMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SearchMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mem0_search_memory", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: SearchMemoryParams = serde_json::from_str(arg).boxed()?;

            let user_id = params
                .user_id
                .or_else(|| self.client.default_user_id().map(ToString::to_string));

            let filters = user_id.map(|uid| json!({"user_id": uid}));

            let request = SearchMemoryRequest {
                query: params.query,
                filters,
                top_k: params.top_k,
                threshold: params.threshold,
                ..Default::default()
            };

            let memories = self.client.search_memories(request).await.boxed()?;

            Ok(json!({
                "success": true,
                "count": memories.len(),
                "memories": memories
            }))
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, error = %e);
                Err(e)
            }
        }
    }
}

/// Parameters for the get memories tool.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct GetMemoriesParams {
    /// User ID to get memories for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Page number for pagination (default: 1).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<i32>,

    /// Number of items per page (default: 100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i32>,
}

/// Tool for getting all memories from Mem0.
pub struct GetMemoriesTool {
    name: String,
    description: String,
    client: Arc<Mem0Client>,
}

impl GetMemoriesTool {
    #[must_use]
    pub fn new(client: Arc<Mem0Client>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            client,
            name: name.unwrap_or("get_memories").to_string(),
            description: description
                .unwrap_or(
                    "Get all stored memories for a user. Use this to list all remembered information.",
                )
                .to_string(),
        }
    }
}

impl std::fmt::Debug for GetMemoriesTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetMemoriesTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpiceModelTool for GetMemoriesTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<GetMemoriesParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mem0_get_memories", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: GetMemoriesParams = serde_json::from_str(arg).boxed()?;

            let user_id = params
                .user_id
                .or_else(|| self.client.default_user_id().map(ToString::to_string));

            let filters = user_id.map_or_else(|| json!({}), |uid| json!({"user_id": uid}));

            let request = GetMemoriesRequest {
                filters,
                page: params.page,
                page_size: params.page_size,
                org_id: None,
                project_id: None,
                enable_graph: None, // Use client config default
            };

            let memories = self.client.get_memories(request).await.boxed()?;

            Ok(json!({
                "success": true,
                "count": memories.len(),
                "memories": memories
            }))
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, error = %e);
                Err(e)
            }
        }
    }
}

/// Parameters for the delete memory tool.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DeleteMemoryParams {
    /// The ID of the specific memory to delete. If not provided, all memories for the user will be deleted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_id: Option<String>,

    /// User ID to delete memories for (when deleting all memories).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

/// Tool for deleting memories from Mem0.
pub struct DeleteMemoryTool {
    name: String,
    description: String,
    client: Arc<Mem0Client>,
}

impl DeleteMemoryTool {
    #[must_use]
    pub fn new(client: Arc<Mem0Client>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            client,
            name: name.unwrap_or("delete_memory").to_string(),
            description: description
                .unwrap_or(
                    "Delete a specific memory by ID, or all memories for a user. Use with caution.",
                )
                .to_string(),
        }
    }
}

impl std::fmt::Debug for DeleteMemoryTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteMemoryTool")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpiceModelTool for DeleteMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<DeleteMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mem0_delete_memory", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: DeleteMemoryParams = serde_json::from_str(arg).boxed()?;

            if let Some(memory_id) = params.memory_id {
                // Delete specific memory
                self.client.delete_memory(&memory_id).await.boxed()?;
                Ok(json!({
                    "success": true,
                    "deleted": "single",
                    "memory_id": memory_id
                }))
            } else {
                // Delete all memories for user
                let user_id = params
                    .user_id
                    .or_else(|| self.client.default_user_id().map(ToString::to_string));

                let request = DeleteMemoryRequest {
                    user_id: user_id.clone(),
                    agent_id: None,
                    org_id: None,
                    project_id: None,
                };

                self.client.delete_all_memories(request).await.boxed()?;

                Ok(json!({
                    "success": true,
                    "deleted": "all",
                    "user_id": user_id
                }))
            }
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, error = %e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::client::Mem0Config;
    use secrecy::SecretString;

    fn create_test_client() -> Arc<Mem0Client> {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);
        Arc::new(Mem0Client::new(config).expect("should create client"))
    }

    #[test]
    fn test_add_memory_params_deserialization() {
        let json = r#"{
            "content": "I love pizza",
            "role": "user",
            "user_id": "user123"
        }"#;

        let params: AddMemoryParams = serde_json::from_str(json).expect("should parse");
        assert_eq!(params.content, "I love pizza");
        assert_eq!(params.role, "user");
        assert_eq!(params.user_id, Some("user123".to_string()));
    }

    #[test]
    fn test_add_memory_params_default_role() {
        let json = r#"{"content": "test content"}"#;

        let params: AddMemoryParams = serde_json::from_str(json).expect("should parse");
        assert_eq!(params.role, "user");
    }

    #[test]
    fn test_search_memory_params_deserialization() {
        let json = r#"{
            "query": "What do I like?",
            "user_id": "user123",
            "top_k": 5,
            "threshold": 0.7
        }"#;

        let params: SearchMemoryParams = serde_json::from_str(json).expect("should parse");
        assert_eq!(params.query, "What do I like?");
        assert_eq!(params.user_id, Some("user123".to_string()));
        assert_eq!(params.top_k, Some(5));
        assert_eq!(params.threshold, Some(0.7));
    }

    #[test]
    fn test_get_memories_params_deserialization() {
        let json = r#"{
            "user_id": "user123",
            "page": 2,
            "page_size": 25
        }"#;

        let params: GetMemoriesParams = serde_json::from_str(json).expect("should parse");
        assert_eq!(params.user_id, Some("user123".to_string()));
        assert_eq!(params.page, Some(2));
        assert_eq!(params.page_size, Some(25));
    }

    #[test]
    fn test_delete_memory_params_deserialization() {
        let json = r#"{
            "memory_id": "mem123",
            "user_id": "user456"
        }"#;

        let params: DeleteMemoryParams = serde_json::from_str(json).expect("should parse");
        assert_eq!(params.memory_id, Some("mem123".to_string()));
        assert_eq!(params.user_id, Some("user456".to_string()));
    }

    #[test]
    fn test_add_memory_tool_creation() {
        let client = create_test_client();
        let tool = AddMemoryTool::new(Arc::clone(&client), None, None);

        assert_eq!(tool.name(), "add_memory");
        assert!(tool.description().is_some());
        assert!(tool.parameters().is_some());
    }

    #[test]
    fn test_add_memory_tool_custom_name() {
        let client = create_test_client();
        let tool = AddMemoryTool::new(
            Arc::clone(&client),
            Some("custom_add"),
            Some("Custom description"),
        );

        assert_eq!(tool.name(), "custom_add");
        assert_eq!(
            tool.description(),
            Some(Cow::Borrowed("Custom description"))
        );
    }

    #[test]
    fn test_search_memory_tool_creation() {
        let client = create_test_client();
        let tool = SearchMemoryTool::new(Arc::clone(&client), None, None);

        assert_eq!(tool.name(), "search_memory");
        assert!(tool.description().is_some());
        assert!(tool.parameters().is_some());
    }

    #[test]
    fn test_get_memories_tool_creation() {
        let client = create_test_client();
        let tool = GetMemoriesTool::new(Arc::clone(&client), None, None);

        assert_eq!(tool.name(), "get_memories");
        assert!(tool.description().is_some());
        assert!(tool.parameters().is_some());
    }

    #[test]
    fn test_delete_memory_tool_creation() {
        let client = create_test_client();
        let tool = DeleteMemoryTool::new(Arc::clone(&client), None, None);

        assert_eq!(tool.name(), "delete_memory");
        assert!(tool.description().is_some());
        assert!(tool.parameters().is_some());
    }

    #[test]
    fn test_tool_parameters_are_valid_json_schema() {
        let client = create_test_client();

        let add_tool = AddMemoryTool::new(Arc::clone(&client), None, None);
        let add_params = add_tool.parameters().expect("should have parameters");
        assert!(add_params.is_object());

        let search_tool = SearchMemoryTool::new(Arc::clone(&client), None, None);
        let search_params = search_tool.parameters().expect("should have parameters");
        assert!(search_params.is_object());

        let get_tool = GetMemoriesTool::new(Arc::clone(&client), None, None);
        let get_params = get_tool.parameters().expect("should have parameters");
        assert!(get_params.is_object());

        let delete_tool = DeleteMemoryTool::new(Arc::clone(&client), None, None);
        let delete_params = delete_tool.parameters().expect("should have parameters");
        assert!(delete_params.is_object());
    }

    #[test]
    fn test_add_memory_tool_debug() {
        let client = create_test_client();
        let tool = AddMemoryTool::new(Arc::clone(&client), None, None);
        let debug_str = format!("{tool:?}");
        assert!(debug_str.contains("AddMemoryTool"));
        assert!(debug_str.contains("add_memory"));
    }

    #[test]
    fn test_search_memory_tool_debug() {
        let client = create_test_client();
        let tool = SearchMemoryTool::new(Arc::clone(&client), None, None);
        let debug_str = format!("{tool:?}");
        assert!(debug_str.contains("SearchMemoryTool"));
    }

    #[test]
    fn test_get_memories_tool_debug() {
        let client = create_test_client();
        let tool = GetMemoriesTool::new(Arc::clone(&client), None, None);
        let debug_str = format!("{tool:?}");
        assert!(debug_str.contains("GetMemoriesTool"));
    }

    #[test]
    fn test_delete_memory_tool_debug() {
        let client = create_test_client();
        let tool = DeleteMemoryTool::new(Arc::clone(&client), None, None);
        let debug_str = format!("{tool:?}");
        assert!(debug_str.contains("DeleteMemoryTool"));
    }
}
