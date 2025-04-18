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

use crate::Runtime;
use futures::StreamExt;
use mcp_core::{
    Content, ToolError,
    handler::{PromptError, ResourceError},
    protocol::{ServerCapabilities, ToolsCapability},
};
use mcp_server;
use serde_json::json;
use std::{future::Future, ops::Deref, pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct RuntimeServer(Arc<Runtime>);
impl Deref for RuntimeServer {
    type Target = Arc<Runtime>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&Arc<Runtime>> for RuntimeServer {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self(Arc::clone(rt))
    }
}

impl mcp_server::Router for RuntimeServer {
    fn name(&self) -> String {
        "Spiced".to_string()
    }

    fn instructions(&self) -> String {
        "Instructions for Spiced".to_string()
    }

    fn capabilities(&self) -> ServerCapabilities {
        ServerCapabilities {
            prompts: None,
            resources: None,
            tools: Some(ToolsCapability {
                list_changed: Some(false),
            }),
        }
    }

    fn list_tools(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<mcp_core::tool::Tool>, ToolError>> + Send + '_>>
    {
        Box::pin(async move {
            let result = self
                .list_all_tools()
                .map(|t| mcp_core::tool::Tool {
                    name: t.name().to_string(),
                    description: t.description().map(|d| d.to_string()).unwrap_or_default(),
                    // For null inputs, we default to an empty object.
                    input_schema: t.parameters().unwrap_or(json!({
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "empty",
                        "type": "object",
                        "required": [],
                        "properties": {}
                        }
                    )),
                })
                .collect::<Vec<_>>()
                .await;
            Ok(result)
        })
    }

    fn call_tool(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Content>, ToolError>> + Send + '_>> {
        let tool_name = tool_name.to_string();
        Box::pin(async move {
            let Some(tool) = self.get_tool(tool_name.as_str()).await else {
                return Err(ToolError::NotFound(format!("Tool {tool_name} not found")));
            };

            // If possible, we pass the call through to the MCP server.
            if let Some(mcp_proxy) = tool.as_mcp_proxy().await {
                tracing::debug!("{tool_name} uses MCP. Will call directly");
                return mcp_proxy
                    .call_tool(arguments)
                    .await
                    .map(|r| r.content)
                    .map_err(|e| ToolError::ExecutionError(e.to_string()));
            }

            let args = serde_json::to_string(&arguments)
                .map_err(|e| ToolError::InvalidParameters(e.to_string()))?;

            let result = tool
                .call(args.as_str())
                .await
                .map_err(|e| ToolError::ExecutionError(e.to_string()))?;

            let text = serde_json::to_string(&result)
                .map_err(|e| ToolError::ExecutionError(e.to_string()))?;

            Ok(vec![Content::Text(mcp_core::TextContent {
                text,
                annotations: None,
            })])
        })
    }

    fn list_resources(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<mcp_core::Resource>, ToolError>> + Send + '_>> {
        Box::pin(async move { Ok(vec![]) })
    }

    fn read_resource(
        &self,
        uri: &str,
    ) -> Pin<Box<dyn Future<Output = Result<String, ResourceError>> + Send + '_>> {
        let uri = uri.to_string();
        Box::pin(async move { Err(ResourceError::NotFound(uri)) })
    }

    fn list_prompts(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<mcp_core::prompt::Prompt>, ToolError>> + Send + '_>>
    {
        Box::pin(async move { Ok(vec![]) })
    }

    fn get_prompt(
        &self,
        prompt_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<String, PromptError>> + Send + '_>> {
        let prompt_name = prompt_name.to_string();
        Box::pin(async move { Err(PromptError::NotFound(prompt_name)) })
    }
}

pub(crate) mod codec {
    use tokio_util::codec::Decoder;

    #[derive(Default)]
    pub struct JsonRpcFrameCodec;
    impl Decoder for JsonRpcFrameCodec {
        type Item = tokio_util::bytes::Bytes;
        type Error = tokio::io::Error;
        fn decode(
            &mut self,
            src: &mut tokio_util::bytes::BytesMut,
        ) -> Result<Option<Self::Item>, Self::Error> {
            if let Some(end) = src
                .iter()
                .enumerate()
                .find_map(|(idx, &b)| (b == b'\n').then_some(idx))
            {
                let line = src.split_to(end);
                let _char_next_line = src.split_to(1);
                Ok(Some(line.freeze()))
            } else {
                Ok(None)
            }
        }
    }
}
