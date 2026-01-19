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

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::{
    Runtime,
    tools::{SpiceModelTool, utils::parameters},
};

use super::get_memory_engine;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct StoreMemoryParams {
    /// A list of details to persist
    thoughts: Vec<String>,
}

pub struct StoreMemoryTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
}

impl StoreMemoryTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            rt,
            name: name.unwrap_or("store_memory").to_string(),
            description: description.unwrap_or("Record any details from 'user' messages that are worth remembering for future conversations.").to_string(),
        }
    }
}
impl From<&Arc<Runtime>> for StoreMemoryTool {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self::new(Arc::clone(rt), None, None)
    }
}

#[async_trait]
impl SpiceModelTool for StoreMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<StoreMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::store_memory", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: StoreMemoryParams = serde_json::from_str(arg).boxed()?;
            let engine = get_memory_engine(Arc::clone(&self.rt)).await?;

            // Store each thought as a separate memory
            let mut results = Vec::new();
            for thought in &params.thoughts {
                let result = engine.store(thought, None).await?;
                results.push(result);
            }

            Ok(serde_json::json!({
                "success": true,
                "memories_stored": results.len()
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
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_memory_params_deserialization() {
        let json = r#"{"thoughts": ["remember this", "and this too"]}"#;
        let params: StoreMemoryParams =
            serde_json::from_str(json).expect("should deserialize params");
        assert_eq!(params.thoughts.len(), 2);
        assert_eq!(params.thoughts[0], "remember this");
        assert_eq!(params.thoughts[1], "and this too");
    }

    #[test]
    fn test_store_memory_params_empty_thoughts() {
        let json = r#"{"thoughts": []}"#;
        let params: StoreMemoryParams =
            serde_json::from_str(json).expect("should deserialize empty thoughts");
        assert!(params.thoughts.is_empty());
    }

    #[test]
    fn test_store_memory_params_single_thought() {
        let json = r#"{"thoughts": ["single thought"]}"#;
        let params: StoreMemoryParams =
            serde_json::from_str(json).expect("should deserialize single thought");
        assert_eq!(params.thoughts.len(), 1);
        assert_eq!(params.thoughts[0], "single thought");
    }

    #[test]
    fn test_store_memory_params_missing_thoughts() {
        let json = r"{}";
        let result: Result<StoreMemoryParams, _> = serde_json::from_str(json);
        let _ = result.expect_err("should fail to parse");
    }

    #[test]
    fn test_store_memory_params_with_special_characters() {
        let json = r#"{"thoughts": ["thought with \"quotes\"", "thought\nwith\nnewlines"]}"#;
        let params: StoreMemoryParams =
            serde_json::from_str(json).expect("should deserialize special chars");
        assert_eq!(params.thoughts[0], "thought with \"quotes\"");
        assert_eq!(params.thoughts[1], "thought\nwith\nnewlines");
    }

    #[test]
    fn test_store_memory_params_with_unicode() {
        let json = r#"{"thoughts": ["remember 你好", "émoji 🎉"]}"#;
        let params: StoreMemoryParams =
            serde_json::from_str(json).expect("should deserialize unicode");
        assert_eq!(params.thoughts[0], "remember 你好");
        assert_eq!(params.thoughts[1], "émoji 🎉");
    }

    #[tokio::test]
    async fn test_store_memory_tool_default_name() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::new(rt, None, None);
        assert_eq!(tool.name(), "store_memory");
    }

    #[tokio::test]
    async fn test_store_memory_tool_custom_name() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::new(rt, Some("custom_store"), None);
        assert_eq!(tool.name(), "custom_store");
    }

    #[tokio::test]
    async fn test_store_memory_tool_default_description() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::new(rt, None, None);
        let desc = tool.description().expect("should have description");
        assert!(desc.contains("Record"));
        assert!(desc.contains("user"));
    }

    #[tokio::test]
    async fn test_store_memory_tool_custom_description() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::new(rt, None, Some("Custom description"));
        let desc = tool.description().expect("should have description");
        assert_eq!(desc, "Custom description");
    }

    #[tokio::test]
    async fn test_store_memory_tool_has_parameters() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::new(rt, None, None);
        let params = tool.parameters();
        assert!(params.is_some());

        let params = params.expect("should have parameters");
        assert!(params.is_object());
    }

    #[tokio::test]
    async fn test_store_memory_tool_from_runtime() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = StoreMemoryTool::from(&rt);
        assert_eq!(tool.name(), "store_memory");
    }
}
