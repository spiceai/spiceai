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

use async_openai::types::{ChatCompletionTool, ChatCompletionToolType, FunctionObject};
use async_trait::async_trait;
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{
        CallToolRequestParam, CallToolResult, ClientCapabilities, ClientInfo, Implementation,
        InitializeRequestParam, ListToolsResult, PaginatedRequestParam, ProtocolVersion,
    },
    serve_client,
    service::RunningService,
    transport::{SseTransport, TokioChildProcess},
};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::{process::Command, sync::RwLock};

use crate::tools::{SpiceModelTool, catalog::SpiceToolCatalog};

use super::{MCPConfig, Result, UnderlyingTransportSnafu, tool::McpToolWrapper};

pub(crate) struct McpToolCatalog {
    client: Arc<RwLock<McpClient>>,

    /// Spicepod defined name & description, not from underlying MCP.
    name: String,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for McpToolCatalog {
    fn drop(&mut self) {
        if let Some(heartbeat_task) = &self.heartbeat_task {
            heartbeat_task.abort();
        }
    }
}

impl McpToolCatalog {
    pub async fn try_new(cfg: MCPConfig, name: &str) -> Result<Self> {
        let client = Self::create_client(&cfg).await?;
        let client = Arc::new(RwLock::new(client));

        // TODO: Implement Ping health checks.

        Ok(Self {
            client,
            name: name.to_string(),
            heartbeat_task: None,
        })
    }

    async fn create_client(cfg: &MCPConfig) -> Result<McpClient> {
        match cfg {
            MCPConfig::Stdio { command, args, env } => Ok(McpClient::Stdio(
                serve_client(
                    (),
                    TokioChildProcess::new(Command::new(command.as_str()).args(args).envs(env))
                        .boxed()
                        .context(UnderlyingTransportSnafu)?,
                )
                .await
                .boxed()
                .context(UnderlyingTransportSnafu)?,
            )),
            MCPConfig::Https { url } => {
                let transport = SseTransport::start(url.clone())
                    .await
                    .boxed()
                    .context(UnderlyingTransportSnafu)?;

                let client_info = ClientInfo {
                    protocol_version: ProtocolVersion::default(),
                    capabilities: ClientCapabilities::default(),
                    client_info: Implementation {
                        name: "spiced".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        name: "Spice.ai".to_string(),
                    },
                };

                Ok(McpClient::Sse(
                    client_info
                        .serve(transport)
                        .await
                        .boxed()
                        .context(UnderlyingTransportSnafu)?,
                ))
            }
        }
    }

    async fn list_tools(&self) -> std::result::Result<Vec<rmcp::model::Tool>, ServiceError> {
        let mut cursor: Option<String> = None;
        let mut tools: Vec<rmcp::model::Tool> = vec![];
        loop {
            let response = self
                .client
                .read()
                .await
                .list_tools(Some(PaginatedRequestParam {
                    cursor: cursor.clone(),
                }))
                .await?;
            tools.extend(response.tools);
            cursor = response.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    async fn get_tool(
        &self,
        name: &str,
    ) -> std::result::Result<Option<rmcp::model::Tool>, ServiceError> {
        let mut cursor: Option<String> = None;
        loop {
            let response = self
                .client
                .read()
                .await
                .list_tools(Some(PaginatedRequestParam {
                    cursor: cursor.clone(),
                }))
                .await?;
            if let Some(t) = response.tools.iter().find(|t| t.name == name) {
                return Ok(Some(t.clone()));
            }
            cursor = response.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(None)
    }
}

pub enum McpClient {
    Stdio(RunningService<RoleClient, ()>),
    Sse(RunningService<RoleClient, InitializeRequestParam>),
}

impl McpClient {
    pub async fn list_tools(
        &self,
        params: Option<PaginatedRequestParam>,
    ) -> Result<ListToolsResult, ServiceError> {
        match self {
            McpClient::Stdio(s) => s.list_tools(params).await,
            McpClient::Sse(s) => s.list_tools(params).await,
        }
    }
    pub async fn call_tool(
        &self,
        params: CallToolRequestParam,
    ) -> Result<CallToolResult, ServiceError> {
        match self {
            McpClient::Stdio(s) => s.call_tool(params).await,
            McpClient::Sse(s) => s.call_tool(params).await,
        }
    }
}

#[async_trait]
impl SpiceToolCatalog for McpToolCatalog {
    fn name(&self) -> &str {
        self.name.as_str()
    }
    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        let tools = self.list_tools().await.unwrap_or_default();
        tools
            .into_iter()
            .map(|t| {
                Arc::new(McpToolWrapper::new(
                    Arc::clone(&self.client),
                    t,
                    self.name.clone(),
                )) as Arc<dyn SpiceModelTool>
            })
            .collect()
    }

    async fn all_definitons(&self) -> Vec<ChatCompletionTool> {
        let tools = self.list_tools().await.unwrap_or_default();
        tools
            .into_iter()
            .map(|t| ChatCompletionTool {
                r#type: ChatCompletionToolType::Function,
                function: FunctionObject {
                    strict: None,
                    name: t.name.to_string(),
                    description: t.description.as_deref().map(ToString::to_string),
                    parameters: Some(serde_json::Value::Object(
                        t.input_schema
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    )),
                },
            })
            .collect()
    }

    /// `name` is the name from the underlying MCP server.
    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        let Ok(Some(tool)) = self.get_tool(name).await else {
            return None;
        };

        Some(Arc::new(McpToolWrapper::new(
            Arc::clone(&self.client),
            tool,
            self.name.clone(),
        )))
    }
}
