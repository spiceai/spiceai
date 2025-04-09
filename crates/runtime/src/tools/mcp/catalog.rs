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
use mcp_client::{
    ClientCapabilities, ClientInfo, Error as McpError, McpClient, McpClientTrait, McpService,
    SseTransport, StdioTransport, Transport, transport::Error as TransportError,
};
use mcp_core::Tool as McpTool;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::RwLock,
    time::{MissedTickBehavior, interval},
};

use crate::tools::{SpiceModelTool, catalog::SpiceToolCatalog};

use super::{
    MCPConfig, Result, UnderlyingInitilizationSnafu, UnderlyingTransportSnafu, tool::McpToolWrapper,
};

const HEARTBEAT_INTERVAL_SECONDS: u64 = 30; // 30 seconds

pub(crate) struct McpToolCatalog {
    client: Arc<RwLock<Box<dyn McpClientTrait>>>,

    /// User defined name & description, not from underlying MCP.
    name: String,
    heartbeat_task: tokio::task::JoinHandle<()>,
}

impl Drop for McpToolCatalog {
    fn drop(&mut self) {
        self.heartbeat_task.abort();
    }
}

impl McpToolCatalog {
    pub async fn try_new(cfg: MCPConfig, name: &str) -> Result<Self> {
        let client = Self::create_client(&cfg)
            .await
            .context(UnderlyingTransportSnafu)?;

        client
            .write()
            .await
            .initialize(
                ClientInfo {
                    name: "spiced".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                ClientCapabilities::default(),
            )
            .await
            .context(UnderlyingInitilizationSnafu)?;

        let client = Arc::new(client);
        let client_clone = Arc::clone(&client);
        let cfg_clone = cfg.clone();
        let name_clone = name.to_string();

        let heartbeat_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let heartbeat_result = client_clone.read().await.ping().await;

                if let Err(ref e) = heartbeat_result {
                    tracing::warn!("MCP client heartbeat failed, attempting reconnection");
                    tracing::debug!("MCP client heartbeat failed with error: {e}");
                    if let Ok(new_client_rwlock) = Self::create_client(&cfg_clone).await {
                        if new_client_rwlock
                            .write()
                            .await
                            .initialize(
                                ClientInfo {
                                    name: "spiced".to_string(),
                                    version: env!("CARGO_PKG_VERSION").to_string(),
                                },
                                ClientCapabilities::default(),
                            )
                            .await
                            .is_ok()
                        {
                            let mut client_lock = client_clone.write().await;
                            *client_lock = new_client_rwlock.into_inner(); // Directly assign the unwrapped Box<dyn McpClientTrait>
                            tracing::info!(
                                "Successfully reconnected MCP client for {}",
                                name_clone
                            );
                        }
                    }
                }
            }
        });

        Ok(Self {
            client,
            name: name.to_string(),
            heartbeat_task,
        })
    }

    async fn create_client(
        cfg: &MCPConfig,
    ) -> std::result::Result<RwLock<Box<dyn McpClientTrait>>, TransportError> {
        match cfg {
            MCPConfig::Stdio { command, args, env } => {
                Self::stdio_client(command.as_str(), args, env).await
            }
            MCPConfig::Https { url } => Self::https_client(url.clone()).await,
        }
    }

    async fn stdio_client(
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> std::result::Result<RwLock<Box<dyn McpClientTrait>>, TransportError> {
        let transport = StdioTransport::new(command, args.to_vec(), env.clone());
        let transport_handle = transport.start().await?;
        let service = McpService::with_timeout(transport_handle, Duration::from_secs(10));
        Ok(RwLock::new(Box::new(McpClient::new(service))))
    }

    async fn https_client(
        url: url::Url,
    ) -> std::result::Result<RwLock<Box<dyn McpClientTrait>>, TransportError> {
        let transport = SseTransport::new(url, HashMap::new());
        let transport_handle = transport.start().await?;
        let service = McpService::with_timeout(transport_handle, Duration::from_secs(10));
        Ok(RwLock::new(Box::new(McpClient::new(service))))
    }

    async fn list_tools(&self) -> std::result::Result<Vec<McpTool>, McpError> {
        let mut cursor: Option<String> = None;
        let mut tools: Vec<McpTool> = vec![];
        loop {
            let response = self.client.read().await.list_tools(cursor.clone()).await?;
            tools.extend(response.tools);
            cursor = response.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    async fn get_tool(&self, name: &str) -> std::result::Result<Option<McpTool>, McpError> {
        let mut cursor: Option<String> = None;
        loop {
            let response = self.client.read().await.list_tools(cursor.clone()).await?;
            if let Some(t) = response.tools.iter().find(|t| t.name.as_str() == name) {
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
                Arc::new(McpToolWrapper::new(Arc::clone(&self.client), t))
                    as Arc<dyn SpiceModelTool>
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
                    name: t.name,
                    description: Some(t.description),
                    parameters: Some(t.input_schema),
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
        )))
    }
}
