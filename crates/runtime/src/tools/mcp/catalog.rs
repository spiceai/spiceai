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

use async_openai::types::chat::{ChatCompletionTool, FunctionObject};
use async_trait::async_trait;
use globset::{Glob, GlobSet, GlobSetBuilder};
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{
        CallToolRequestParam, CallToolResult, ClientCapabilities, ClientInfo, ClientRequest,
        Extensions, Implementation, InitializeRequestParam, ListToolsResult, PaginatedRequestParam,
        PingRequest, PingRequestMethod, ProtocolVersion,
    },
    serve_client,
    service::RunningService,
    transport::{ConfigureCommandExt, SseClientTransport, TokioChildProcess},
};
use snafu::ResultExt;
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{
    process::Command,
    sync::RwLock,
    time::{MissedTickBehavior, interval},
};

use crate::tools::{SpiceModelTool, catalog::SpiceToolCatalog};

use super::{Error, MCPConfig, Result, UnderlyingTransportSnafu, tool::McpToolWrapper};

const HEARTBEAT_INTERVAL_SECONDS: u64 = 30; // 30 seconds

/// Glob patterns for detecting dangerous path components
const DANGEROUS_PATH_PATTERNS: &[&str] = &[
    "*/..*",  // Unix parent directory traversal (anywhere in path)
    "..*",    // Parent at start (Unix) - matches paths starting with ..
    "*\\..*", // Windows parent directory traversal (backslash-dot-dot)
    "*\\\\*", // Windows UNC path or backslash (absolute paths)
    "/*",     // Unix absolute path (starts with /)
    "?:*",    // Windows drive letter (C:, D:, etc.)
];

/// Pre-compiled glob set for path validation
static DANGEROUS_PATH_GLOB_SET: LazyLock<GlobSet> = LazyLock::new(|| {
    let mut builder = GlobSetBuilder::new();
    for pattern in DANGEROUS_PATH_PATTERNS {
        if let Ok(glob) = Glob::new(pattern) {
            builder.add(glob);
        }
    }
    // This should never fail since DANGEROUS_PATH_PATTERNS are hardcoded and validated
    builder.build().unwrap_or_else(|e| {
        unreachable!("Failed to build dangerous path glob set with hardcoded patterns: {e}")
    })
});

/// Check if a hostname is localhost
fn is_localhost(host: &str) -> bool {
    matches!(
        host,
        "localhost" | "127.0.0.1" | "::1" | "[::1]" | "0.0.0.0"
    )
}

pub(crate) struct McpToolCatalog {
    client: Arc<RwLock<McpClient>>,

    /// Spicepod defined name & description, not from underlying MCP.
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
        let client = Self::create_client(&cfg).await?;
        let client = Arc::new(RwLock::new(client));

        let client_clone = Arc::clone(&client);
        let cfg_clone = cfg.clone();
        let name_clone = name.to_string();

        let heartbeat_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Perform the heartbeat ping. The read lock is held during the ping call.
                // Note: The underlying McpClient wraps a RunningService which is not Clone,
                // so we cannot clone the client to release the lock before the network call.
                // This is acceptable because the ping timeout is bounded.
                let heartbeat_result = {
                    let client_guard = client_clone.read().await;
                    client_guard.ping().await
                };
                if let Err(ref e) = heartbeat_result {
                    tracing::warn!("MCP client heartbeat failed, attempting reconnection");
                    tracing::debug!("MCP client heartbeat failed with error: {e}");
                    if let Ok(new_client_rwlock) = Self::create_client(&cfg_clone).await {
                        let mut client_lock = client_clone.write().await;
                        *client_lock = new_client_rwlock;
                        tracing::info!("Successfully reconnected MCP client for {}", name_clone);
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

    async fn create_client(cfg: &MCPConfig) -> Result<McpClient> {
        match cfg {
            MCPConfig::Stdio { command, args, env } => {
                // Security constants
                const MAX_ARGS: usize = 100;
                const MAX_ARG_LENGTH: usize = 4096;

                // Security: Validate command path to prevent command injection
                if DANGEROUS_PATH_GLOB_SET.is_match(command) {
                    return Err(Error::CouldNotConstructTool {
                        name: "mcp_stdio".to_string(),
                        e: format!(
                            "Invalid command path '{command}'. Path contains dangerous components"
                        ),
                    });
                }

                // Security: Limit number of arguments to prevent resource exhaustion
                if args.len() > MAX_ARGS {
                    return Err(Error::CouldNotConstructTool {
                        name: "mcp_stdio".to_string(),
                        e: format!(
                            "Too many arguments ({}). Maximum allowed: {MAX_ARGS}",
                            args.len()
                        ),
                    });
                }

                // Security: Validate argument lengths to prevent buffer overflow attacks
                for (i, arg) in args.iter().enumerate() {
                    if arg.len() > MAX_ARG_LENGTH {
                        return Err(Error::CouldNotConstructTool {
                            name: "mcp_stdio".to_string(),
                            e: format!(
                                "Argument {i} too long ({} bytes). Maximum allowed: {MAX_ARG_LENGTH} bytes",
                                arg.len()
                            ),
                        });
                    }
                }

                Ok(McpClient::Stdio(
                    serve_client(
                        (),
                        TokioChildProcess::new(Command::new(command.as_str()).configure(|c| {
                            c.envs(env).args(args);
                        }))
                        .boxed()
                        .context(UnderlyingTransportSnafu)?,
                    )
                    .await
                    .boxed()
                    .context(UnderlyingTransportSnafu)?,
                ))
            }
            MCPConfig::Https { url } => {
                // Security: Validate URL scheme (only https allowed, http for localhost testing)
                if url.scheme() != "https" && url.scheme() != "http" {
                    return Err(Error::CouldNotConstructTool {
                        name: "mcp_https".to_string(),
                        e: format!(
                            "Invalid URL scheme '{}'. Only https:// (or http:// for localhost) allowed",
                            url.scheme()
                        ),
                    });
                }

                // Security: Warn if using http (unencrypted) for non-localhost
                let host = url.host_str().unwrap_or("<unknown>");
                if url.scheme() == "http" && !is_localhost(host) {
                    tracing::warn!(
                        "MCP HTTPS client using unencrypted HTTP connection to non-localhost host '{}': {}. This is insecure.",
                        host,
                        url
                    );
                }

                let transport = SseClientTransport::start(url.to_string())
                    .await
                    .boxed()
                    .context(UnderlyingTransportSnafu)?;

                let client_info = ClientInfo {
                    protocol_version: ProtocolVersion::default(),
                    capabilities: ClientCapabilities::default(),
                    client_info: Implementation {
                        name: "Spice.ai Open Source".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
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
        // Security: Limit pagination to prevent infinite loops and memory exhaustion
        const MAX_PAGINATION_ITERATIONS: usize = 100;
        const MAX_TOTAL_TOOLS: usize = 10000;

        let mut cursor: Option<String> = None;
        let mut tools: Vec<rmcp::model::Tool> = vec![];
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > MAX_PAGINATION_ITERATIONS {
                tracing::warn!(
                    "MCP tool listing exceeded maximum pagination iterations ({MAX_PAGINATION_ITERATIONS}), stopping iteration"
                );
                break;
            }

            let response = self
                .client
                .read()
                .await
                .list_tools(Some(PaginatedRequestParam {
                    cursor: cursor.clone(),
                }))
                .await?;

            // Security: Validate total tools count to prevent memory exhaustion
            if tools.len().saturating_add(response.tools.len()) > MAX_TOTAL_TOOLS {
                tracing::warn!(
                    "MCP tool listing exceeded maximum tools count ({MAX_TOTAL_TOOLS}), limiting results"
                );
                let remaining = MAX_TOTAL_TOOLS - tools.len();
                tools.extend(response.tools.into_iter().take(remaining));
                break;
            }

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
        // Security: Limit pagination to prevent infinite loops
        const MAX_PAGINATION_ITERATIONS: usize = 100;

        let mut cursor: Option<String> = None;
        let mut iterations = 0;

        loop {
            iterations += 1;
            if iterations > MAX_PAGINATION_ITERATIONS {
                tracing::warn!(
                    "MCP get_tool pagination exceeded maximum iterations ({MAX_PAGINATION_ITERATIONS}), stopping iteration"
                );
                break;
            }

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

    pub async fn ping(&self) -> Result<(), ServiceError> {
        let req = ClientRequest::PingRequest(PingRequest {
            method: PingRequestMethod,
            extensions: Extensions::new(),
        });
        match self {
            McpClient::Stdio(s) => s.send_request(req).await,
            McpClient::Sse(s) => s.send_request(req).await,
        }
        .map(|_| ())
    }
}

#[async_trait]
impl SpiceToolCatalog for McpToolCatalog {
    fn name(&self) -> &str {
        self.name.as_str()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dangerous_patterns_reject_parent_traversal() {
        // Unix-style parent directory traversal
        assert!(DANGEROUS_PATH_GLOB_SET.is_match(".."));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("../etc/passwd"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("subdir/../../etc/passwd"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("foo/../bar"));
    }

    #[test]
    fn test_dangerous_patterns_reject_windows_parent_traversal() {
        // Windows-style parent directory traversal
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("..\\etc\\passwd"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("subdir\\..\\..\\etc\\passwd"));
    }

    #[test]
    fn test_dangerous_patterns_reject_absolute_paths() {
        // Unix absolute paths
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("/etc/passwd"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("/var/log/secrets"));
    }

    #[test]
    fn test_dangerous_patterns_reject_windows_absolute_paths() {
        // Windows drive letters
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("C:\\Windows\\System32"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("D:\\secrets"));

        // Windows UNC paths
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("\\\\server\\share"));
        assert!(DANGEROUS_PATH_GLOB_SET.is_match("\\\\192.168.1.1\\admin"));
    }

    #[test]
    fn test_dangerous_patterns_allow_legitimate_hidden_files() {
        // Legitimate hidden files and directories should NOT match
        // These start with . but are not path traversal attempts
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match(".config"),
            ".config should be allowed (legitimate hidden directory)"
        );
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match(".cache"),
            ".cache should be allowed (legitimate hidden directory)"
        );
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match(".bashrc"),
            ".bashrc should be allowed (legitimate hidden file)"
        );
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match(".ssh/id_rsa"),
            ".ssh/id_rsa should be allowed (legitimate path in hidden directory)"
        );
    }

    #[test]
    fn test_dangerous_patterns_allow_safe_relative_paths() {
        // Safe relative paths should NOT match
        assert!(!DANGEROUS_PATH_GLOB_SET.is_match("myfile.txt"));
        assert!(!DANGEROUS_PATH_GLOB_SET.is_match("subdir/myfile.txt"));
        assert!(!DANGEROUS_PATH_GLOB_SET.is_match("a/b/c/file.txt"));
    }

    #[test]
    fn test_dangerous_patterns_allow_current_directory_simple() {
        // Simple current directory references are safe and should NOT match
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match("."),
            ". (current dir) should be allowed"
        );
        assert!(
            !DANGEROUS_PATH_GLOB_SET.is_match("./script.sh"),
            "./script.sh should be allowed"
        );
    }

    #[test]
    fn test_is_localhost_ipv4() {
        assert!(is_localhost("127.0.0.1"));
        assert!(is_localhost("localhost"));
        assert!(is_localhost("0.0.0.0"));
        assert!(!is_localhost("192.168.1.1"));
        assert!(!is_localhost("example.com"));
    }

    #[test]
    fn test_is_localhost_ipv6() {
        assert!(is_localhost("::1"));
        assert!(is_localhost("[::1]"));
        assert!(!is_localhost("::2"));
        assert!(!is_localhost("2001:db8::1"));
    }
}
