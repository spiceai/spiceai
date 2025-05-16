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
use rmcp::{
    ServiceError,
    model::{CallToolRequestParam, CallToolResult, JsonObject, Tool, object},
};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tokio::sync::RwLock;
use tools::McpProxy;
use tracing::Span;
use tracing_futures::Instrument;

use crate::tools::SpiceModelTool;

use super::{Result, catalog::McpClient};

pub struct McpToolWrapper {
    client: Arc<RwLock<McpClient>>,
    spec: Tool,

    /// Spicepod defined name, not from underlying MCP.
    server_name: String,
}

impl McpToolWrapper {
    pub fn new(client: Arc<RwLock<McpClient>>, spec: Tool, server_name: String) -> Self {
        Self {
            client,
            spec,
            server_name,
        }
    }

    #[must_use]
    pub fn internal_name(&self) -> Cow<'static, str> {
        self.spec.name.clone()
    }
}

#[async_trait]
impl SpiceModelTool for McpToolWrapper {
    fn name(&self) -> Cow<'_, str> {
        self.internal_name()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.spec.description.clone()
    }

    fn parameters(&self) -> Option<Value> {
        Some(Value::Object(
            self.spec
                .input_schema
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ))
    }

    async fn as_mcp_proxy(&self) -> Option<&dyn McpProxy> {
        Some(self)
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mcp", tool = self.name().to_string(), input = arg);
        span.in_scope(
            || tracing::info!(target: "task_history", task_override = %format!("tool_use::{}/{}", self.server_name, self.spec.name), "labels"),
        );

        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let client = self.client.read().await;

            let input: Value = if arg.is_empty() {
                Value::Null
            } else {
                serde_json::from_str(arg).map_err(|e| {
                    tracing::error!(target: "task_history", parent: &span, "Failed to parse input: {e}");
                    e
                })?
            };
            let response = client
                .call_tool(CallToolRequestParam{name: self.internal_name(), arguments: Some(object(input))})
                .await
                .boxed()?;

            let v = serde_json::to_value(response.content).boxed()?;
            Ok(v)
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
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

#[async_trait]
impl McpProxy for McpToolWrapper {
    async fn call_tool(
        &self,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, ServiceError> {
        let inner = self.client.read().await;
        inner
            .call_tool(CallToolRequestParam {
                name: self.internal_name(),
                arguments,
            })
            .await
    }
}
