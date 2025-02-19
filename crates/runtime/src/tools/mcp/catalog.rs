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
    transport::Error as TransportError, ClientCapabilities, ClientInfo, Error as McpError,
    McpClient, McpClientTrait, McpService, SseTransport, StdioTransport, Transport,
};
use mcp_core::Tool as McpTool;
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::tools::{catalog::SpiceToolCatalog, SpiceModelTool};

use super::{
    tool::McpToolWrapper, MCPConfig, Result, UnderlyingInitilizationSnafu, UnderlyingTransportSnafu,
};

pub(crate) struct McpToolCatalog {
    client: Arc<RwLock<Box<dyn McpClientTrait>>>,

    /// User defined name & description, not from underlying MCP.
    name: String,
}

impl McpToolCatalog {
    pub async fn try_new(cfg: MCPConfig, name: &str) -> Result<Self> {
        let client = match cfg {
            MCPConfig::Stdio { command, args } => Self::stdio_client(command.as_str(), args).await,
            MCPConfig::Https { url } => Self::https_client(url).await,
        }
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

        Ok(Self {
            client: Arc::new(client),
            name: name.to_string(),
        })
    }

    async fn stdio_client(
        command: &str,
        args: Option<Vec<String>>,
    ) -> std::result::Result<RwLock<Box<dyn McpClientTrait>>, TransportError> {
        let transport = StdioTransport::new(command, args.unwrap_or_default(), HashMap::new());
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
