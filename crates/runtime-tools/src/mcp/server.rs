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

use crate::tooling::Tooling;

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
};
use serde_json::{Map, Value, json};
use std::{borrow::Cow, collections::HashMap, future::Future, sync::Arc};
use tokio::sync::RwLock;
use tools::SpiceModelTool;
use tools::rename::with_name;
use util::security::{MAX_SAFE_JSON_DEPTH, get_json_depth};

#[derive(Clone)]
pub struct RuntimeServer {
    tools: Arc<RwLock<HashMap<String, Tooling>>>,
}

impl RuntimeServer {
    pub fn new(tools: Arc<RwLock<HashMap<String, Tooling>>>) -> Self {
        Self { tools }
    }

    async fn get_tool(&self, tool_name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        let tools = self.tools.read().await;
        if let Some((catalog_name, name)) = tool_name.split_once('/') {
            let Tooling::Catalog { tools: catalog, .. } = tools.get(catalog_name)? else {
                return None;
            };
            return catalog.get(name).await;
        }
        match tools.get(tool_name)? {
            Tooling::Tool(tool) | Tooling::FunctionTool(tool) => Some(Arc::clone(tool)),
            Tooling::Catalog { .. } => None,
        }
    }

    async fn all_tools(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        let tools = self.tools.read().await;
        let mut result = Vec::new();
        for (_, tooling) in tools.iter() {
            match tooling {
                Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                    result.push(Arc::clone(tool));
                }
                Tooling::Catalog { tools: catalog, .. } => {
                    let catalog_name = catalog.name();
                    for tool in catalog.all().await {
                        result.push(with_name(
                            &tool,
                            format!("{catalog_name}/{}", tool.name()).as_str(),
                        ));
                    }
                }
            }
        }
        result
    }
}

impl ServerHandler for RuntimeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "Spice.ai Open Source",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_protocol_version(ProtocolVersion::LATEST)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let tool_name = request.name.clone();
        let arguments = request.arguments.clone();
        Box::pin(async move {
            // Security constants
            const MAX_TOOL_NAME_LENGTH: usize = 256;
            const MAX_ARGS_SIZE: usize = 1024 * 1024; // 1 MB

            // Security: Validate tool name to prevent injection attacks
            if tool_name.len() > MAX_TOOL_NAME_LENGTH {
                return Err(McpError::invalid_params(
                    format!(
                        "Tool name too long ({} chars). Maximum: {MAX_TOOL_NAME_LENGTH}",
                        tool_name.len()
                    ),
                    None,
                ));
            }

            // Security: Validate tool name contains only safe characters
            if !tool_name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/')
            {
                return Err(McpError::invalid_params(
                    "Tool name contains invalid characters. Only alphanumeric, underscore, hyphen, dot, and forward-slash allowed".to_string(),
                    None,
                ));
            }

            let Some(tool) = self.get_tool(tool_name.as_ref()).await else {
                return Err(McpError::method_not_found::<
                    rmcp::model::CallToolRequestMethod,
                >());
            };

            // If possible, we pass the call through to the MCP server.
            if let Some(mcp_proxy) = tool.as_mcp_proxy().await {
                tracing::debug!("{tool_name} uses MCP. Will call directly");

                // Security: Validate arguments JSON depth before proxying
                if let Some(ref args) = arguments {
                    let depth = get_json_depth(&Value::Object(args.clone()));
                    if depth > MAX_SAFE_JSON_DEPTH {
                        return Err(McpError::invalid_params(
                            format!(
                                "Arguments JSON too deeply nested (depth: {depth}). Maximum: {MAX_SAFE_JSON_DEPTH}"
                            ),
                            None,
                        ));
                    }
                }

                return mcp_proxy
                    .call_tool(arguments)
                    .await
                    .map_err(|e| McpError::internal_error(e.to_string(), None));
            }

            let args = serde_json::to_string(&arguments)
                .map_err(|e| McpError::invalid_params(e.to_string(), None))?;

            // Security: Validate serialized argument size to prevent DoS
            if args.len() > MAX_ARGS_SIZE {
                return Err(McpError::invalid_params(
                    format!(
                        "Arguments too large ({} bytes). Maximum: {MAX_ARGS_SIZE} bytes",
                        args.len()
                    ),
                    None,
                ));
            }

            let result = tool
                .call(args.as_str())
                .await
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            let text = serde_json::to_string(&result)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            Ok(CallToolResult::success(vec![Content::text(text)]))
        })
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        Box::pin(async move {
            let all = self.all_tools().await;
            let tools = all
                .into_iter()
                .map(|t| {
                    let name: Cow<'static, str> = t.name().into_owned().into();
                    let description: Option<Cow<'static, str>> =
                        t.description().map(|s| Cow::Owned(s.into_owned()));
                    let schema = to_map(t.parameters().unwrap_or(json!({
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "empty",
                        "type": "object",
                        "required": [],
                        "properties": {}
                    })));
                    Tool::new_with_raw(name, description, schema)
                })
                .collect::<Vec<_>>();
            Ok(ListToolsResult {
                tools,
                next_cursor: None,
                meta: None,
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
