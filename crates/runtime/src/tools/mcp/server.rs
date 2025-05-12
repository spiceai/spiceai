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

use rmcp::{
    Error as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestMethod, CallToolRequestParam, CallToolResult, Content, ListToolsResult,
        PaginatedRequestParam,
    },
    service::RequestContext,
};
use serde_json::{Map, Value, json};
use std::{borrow::Cow, future::Future, ops::Deref, sync::Arc};

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

impl ServerHandler for RuntimeServer {
    fn call_tool(
        &self,
        request: CallToolRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let CallToolRequestParam {
            name: tool_name,
            arguments,
        } = request;
        Box::pin(async move {
            let Some(tool) = self.get_tool(tool_name.to_string().as_str()).await else {
                return Err(McpError::method_not_found::<CallToolRequestMethod>());
            };

            // If possible, we pass the call through to the MCP server.
            if let Some(mcp_proxy) = tool.as_mcp_proxy().await {
                tracing::debug!("{tool_name} uses MCP. Will call directly");
                return mcp_proxy
                    .call_tool(arguments)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None));
            }

            let args = serde_json::to_string(&arguments)
                .map_err(|e| McpError::invalid_params(e.to_string(), None))?;

            let result = tool
                .call(args.as_str())
                .await
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            let text = serde_json::to_string(&result)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            Ok(CallToolResult {
                content: vec![Content::text(text)],
                is_error: Some(false),
            })
        })
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        Box::pin(async move {
            let tools = self
                .list_all_tools()
                .map(|t| rmcp::model::Tool {
                    name: t.name().into_owned().into(),
                    description: t
                        .description()
                        .as_deref()
                        .map(|s| Cow::Owned(s.to_string())),
                    // For null inputs, we default to an empty object.
                    input_schema: to_map(t.parameters().unwrap_or(json!({
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "empty",
                        "type": "object",
                        "required": [],
                        "properties": {}
                        }
                    )))
                    .into(),
                    annotations: None,
                })
                .collect::<Vec<_>>()
                .await;
            Ok(ListToolsResult {
                tools,
                next_cursor: None,
            })
        })
    }
}

fn to_map(v: Value) -> Map<String, Value> {
    let Value::Object(m) = v else {
        return Map::default();
    };
    m
}
