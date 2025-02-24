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
use mcp_client::McpClientTrait;
use mcp_core::Tool as McpTool;
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use crate::{tools::SpiceModelTool, Runtime};

use super::Result;

pub struct McpToolWrapper {
    client: Arc<RwLock<Box<dyn McpClientTrait>>>,
    spec: McpTool,
}

impl McpToolWrapper {
    pub fn new(client: Arc<RwLock<Box<dyn McpClientTrait>>>, spec: McpTool) -> Self {
        Self { client, spec }
    }

    pub fn internal_name(&self) -> &str {
        self.spec.name.as_str()
    }
}

#[async_trait]
impl SpiceModelTool for McpToolWrapper {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.internal_name())
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.spec.description))
    }

    fn parameters(&self) -> Option<Value> {
        Some(self.spec.input_schema.clone())
    }

    async fn call(
        &self,
        arg: &str,
        _rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mcp", tool = self.name().to_string(), input = arg);
        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let client = self.client.read().await;

            let response = client
                .call_tool(self.internal_name(), serde_json::from_str(arg).unwrap())
                .await
                .boxed()?;

            let v = serde_json::to_value(response.content).boxed()?;
            Ok(v)
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
