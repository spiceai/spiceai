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
pub struct LoadMemoryParams {
    /// Retrieve memories created in the 'last' interval. ISO 8601 Format, e.g: "1h", "2m30s".
    pub last: String,
}

pub struct LoadMemoryTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
}

impl LoadMemoryTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            rt,
            name: name.unwrap_or("load_memory").to_string(),
            description: description
                .unwrap_or("Load memories previously saved by the language model.")
                .to_string(),
        }
    }
}

impl From<&Arc<Runtime>> for LoadMemoryTool {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self::new(Arc::clone(rt), None, None)
    }
}

#[async_trait]
impl SpiceModelTool for LoadMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<LoadMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::load_memory", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: LoadMemoryParams = serde_json::from_str(arg).boxed()?;
            let engine = get_memory_engine(Arc::clone(&self.rt)).await?;

            engine.load(&params.last).await
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
    fn test_load_memory_params_deserialization() {
        let json = r#"{"last": "1h"}"#;
        let params: LoadMemoryParams =
            serde_json::from_str(json).expect("should deserialize params");
        assert_eq!(params.last, "1h");
    }

    #[test]
    fn test_load_memory_params_various_intervals() {
        let test_cases = [
            (r#"{"last": "30m"}"#, "30m"),
            (r#"{"last": "2h30m"}"#, "2h30m"),
            (r#"{"last": "1d"}"#, "1d"),
            (r#"{"last": "5s"}"#, "5s"),
            (r#"{"last": "PT1H"}"#, "PT1H"),
        ];

        for (json, expected) in test_cases {
            let params: LoadMemoryParams =
                serde_json::from_str(json).expect("should deserialize params");
            assert_eq!(params.last, expected, "Failed for JSON: {json}");
        }
    }

    #[test]
    fn test_load_memory_params_missing_last() {
        let json = r"{}";
        let result: Result<LoadMemoryParams, _> = serde_json::from_str(json);
        let _ = result.expect_err("should fail to parse");
    }

    #[test]
    fn test_load_memory_params_serialization() {
        let params = LoadMemoryParams {
            last: "2h".to_string(),
        };
        let json = serde_json::to_string(&params).expect("should serialize params");
        assert!(json.contains("\"last\":\"2h\""));
    }

    #[tokio::test]
    async fn test_load_memory_tool_default_name() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::new(rt, None, None);
        assert_eq!(tool.name(), "load_memory");
    }

    #[tokio::test]
    async fn test_load_memory_tool_custom_name() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::new(rt, Some("custom_load"), None);
        assert_eq!(tool.name(), "custom_load");
    }

    #[tokio::test]
    async fn test_load_memory_tool_default_description() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::new(rt, None, None);
        let desc = tool.description().expect("should have description");
        assert!(desc.contains("Load"));
        assert!(desc.contains("memories"));
    }

    #[tokio::test]
    async fn test_load_memory_tool_custom_description() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::new(rt, None, Some("Custom load description"));
        let desc = tool.description().expect("should have description");
        assert_eq!(desc, "Custom load description");
    }

    #[tokio::test]
    async fn test_load_memory_tool_has_parameters() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::new(rt, None, None);
        let params = tool.parameters();
        assert!(params.is_some());

        let params = params.expect("should have parameters");
        assert!(params.is_object());
    }

    #[tokio::test]
    async fn test_load_memory_tool_from_runtime() {
        let rt = Arc::new(Runtime::builder().build().await);
        let tool = LoadMemoryTool::from(&rt);
        assert_eq!(tool.name(), "load_memory");
    }
}
