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

#![allow(clippy::missing_errors_doc)]

use async_openai::types::chat::{ChatCompletionTool, FunctionObject};
use async_trait::async_trait;
use globset::{Glob, GlobSet, GlobSetBuilder};
use rmcp::{
    ClientLifecycleMode, ClientServiceExt, RoleClient,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities, ClientRequest,
        Implementation, InitializeRequestParams, ListToolsResult, PaginatedRequestParams,
        PingRequest, ProtocolVersion, ServerResult,
    },
    service::{RunningService, ServiceError},
    transport::{
        ConfigureCommandExt, StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use secrecy::ExposeSecret;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, RwLock as StdRwLock},
    time::{Duration, Instant},
};
use tokio::{
    process::Command,
    sync::RwLock,
    time::{MissedTickBehavior, interval},
};

use crate::catalog::SpiceToolCatalog;
use tools::SpiceModelTool;

use super::{
    Error, MCPConfig, Result, UnderlyingTransportSnafu,
    server::{McpSchemaSnapshot, apply_listed_catalog_cache},
    tool::McpToolWrapper,
};

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
    /// Schemas from the last successful `tools/list` / `tools/get`, used by
    /// [`SpiceToolCatalog::try_get`] so Streamable HTTP can validate `Mcp-Param-*`
    /// without taking the async client lock. Freshness follows the list
    /// result's `ttlMs` (SEP-2549): omitted or zero means immediately stale.
    tool_cache: Arc<StdRwLock<ToolListCache>>,
    /// Shared gateway snapshot. TTL / reconnect refreshes write through
    /// here and bump the epoch so Streamable HTTP drops rmcp's per-name
    /// `get_tool` cache. Attached after the catalog is registered.
    schemas: Arc<StdRwLock<Option<Arc<McpSchemaSnapshot>>>>,
}

#[derive(Default)]
struct ToolListCache {
    tools: HashMap<String, rmcp::model::Tool>,
    /// `None` means the list is already stale.
    expires_at: Option<Instant>,
}

/// Wire a proxied MCP catalog to the gateway schema snapshot.
///
/// No-op for non-MCP catalogs. After attach, TTL and reconnect refreshes
/// write through to the snapshot and bump its epoch so Streamable HTTP
/// rebuilds without waiting for a gateway `tools/list`.
pub fn attach_mcp_schema_snapshot(
    catalog: &dyn SpiceToolCatalog,
    snapshot: &Arc<McpSchemaSnapshot>,
) {
    if let Some(mcp) = catalog.as_any().downcast_ref::<McpToolCatalog>() {
        mcp.attach_schema_snapshot(snapshot);
    }
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
        let tool_cache = Arc::new(StdRwLock::new(ToolListCache::default()));
        let tool_cache_clone = Arc::clone(&tool_cache);
        let schemas = Arc::new(StdRwLock::new(None::<Arc<McpSchemaSnapshot>>));
        let schemas_clone = Arc::clone(&schemas);

        let heartbeat_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // The read lock is held during the heartbeat call. The underlying
                // McpClient wraps a RunningService which is not Clone, so we cannot
                // clone the client to release the lock before the network call.
                // This is acceptable because the call timeout is bounded.
                let heartbeat_result = {
                    let client_guard = client_clone.read().await;
                    client_guard.heartbeat().await
                };
                if let Err(ref e) = heartbeat_result {
                    tracing::warn!("MCP client heartbeat failed, attempting reconnection");
                    tracing::debug!("MCP client heartbeat failed with error: {e}");
                    if let Ok(new_client_rwlock) = Self::create_client(&cfg_clone).await {
                        let mut client_lock = client_clone.write().await;
                        *client_lock = new_client_rwlock;
                        // Keep the last successful cache on list failure.
                        // Clearing it would make `try_get` miss and let rmcp
                        // cache `get_tool == None` for that name forever.
                        if let Ok((listed, complete, ttl_ms)) =
                            list_tools_from_client(&client_lock).await
                            && let Ok(mut cache) = tool_cache_clone.write()
                        {
                            apply_tool_cache(&mut cache, &listed, complete, ttl_ms);
                            if let Ok(slot) = schemas_clone.read()
                                && let Some(snapshot) = slot.as_ref()
                            {
                                let changed = apply_listed_catalog_cache(
                                    snapshot,
                                    &name_clone,
                                    &cache.tools,
                                    complete,
                                );
                                snapshot.bump_if(changed);
                            }
                        }
                        tracing::info!("Successfully reconnected MCP client for {}", name_clone);
                    }
                }
            }
        });

        let catalog = Self {
            client,
            name: name.to_string(),
            heartbeat_task,
            tool_cache,
            schemas,
        };
        // Fill the sync schema cache before the catalog is registered so
        // Streamable HTTP `get_tool` can validate `Mcp-Param-*` on the first
        // `tools/call` without waiting for a later `tools/list`. A failed
        // list leaves the cache empty; fail construction so the existing
        // component-load retry rebuilds the catalog instead of registering
        // one that would make rmcp cache `get_tool == None`.
        catalog
            .list_tools()
            .await
            .map_err(|e| Error::CouldNotConstructTool {
                name: name.to_string(),
                e: e.to_string(),
            })?;
        Ok(catalog)
    }

    fn remember_tools(&self, tools: &[rmcp::model::Tool], replace: bool, ttl_ms: u64) {
        let Ok(mut cache) = self.tool_cache.write() else {
            return;
        };
        apply_tool_cache(&mut cache, tools, replace, ttl_ms);
        self.publish_cache_to_snapshot(&cache, replace);
    }

    fn attach_schema_snapshot(&self, snapshot: &Arc<McpSchemaSnapshot>) {
        if let Ok(mut slot) = self.schemas.write() {
            *slot = Some(Arc::clone(snapshot));
        }
        if let Ok(cache) = self.tool_cache.read() {
            self.publish_cache_to_snapshot(&cache, true);
        }
    }

    fn publish_cache_to_snapshot(&self, cache: &ToolListCache, replace: bool) {
        let Ok(slot) = self.schemas.read() else {
            return;
        };
        let Some(snapshot) = slot.as_ref() else {
            return;
        };
        let changed = apply_listed_catalog_cache(snapshot, &self.name, &cache.tools, replace);
        snapshot.bump_if(changed);
    }

    fn cache_is_fresh(&self) -> bool {
        self.tool_cache
            .read()
            .ok()
            .is_some_and(|cache| list_cache_is_fresh(cache.expires_at, Instant::now()))
    }

    fn cached_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        self.tool_cache
            .read()
            .ok()
            .and_then(|cache| cache.tools.get(name).cloned())
    }

    fn cached_tools(&self) -> Vec<rmcp::model::Tool> {
        self.tool_cache
            .read()
            .ok()
            .map(|cache| cache.tools.values().cloned().collect())
            .unwrap_or_default()
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

                let transport =
                    TokioChildProcess::new(Command::new(command.as_str()).configure(|c| {
                        c.envs(env).args(args);
                    }))
                    .boxed()
                    .context(UnderlyingTransportSnafu)?;

                Ok(McpClient::Stdio(
                    ().serve_with_lifecycle(transport, client_lifecycle())
                        .await
                        .boxed()
                        .context(UnderlyingTransportSnafu)?,
                ))
            }
            MCPConfig::StreamableHttp {
                url,
                auth_token,
                headers,
            } => {
                // Security: Validate URL scheme (only https allowed, http for localhost testing)
                if url.scheme() != "https" && url.scheme() != "http" {
                    return Err(Error::CouldNotConstructTool {
                        name: "mcp_streamable_http".to_string(),
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

                let mut transport_config =
                    StreamableHttpClientTransportConfig::with_uri(url.to_string())
                        .custom_headers(headers.clone());
                if let Some(auth_token) = auth_token {
                    transport_config = transport_config.auth_header(auth_token.expose_secret());
                }
                let transport = StreamableHttpClientTransport::from_config(transport_config);

                let client_info = InitializeRequestParams::new(
                    ClientCapabilities::default(),
                    Implementation::new("Spice.ai Open Source", env!("CARGO_PKG_VERSION")),
                )
                .with_protocol_version(ProtocolVersion::V_2026_07_28);

                Ok(McpClient::Http(
                    client_info
                        .serve_with_lifecycle(transport, client_lifecycle())
                        .await
                        .boxed()
                        .context(UnderlyingTransportSnafu)?,
                ))
            }
        }
    }

    async fn list_tools(&self) -> std::result::Result<Vec<rmcp::model::Tool>, ServiceError> {
        if self.cache_is_fresh() {
            return Ok(self.cached_tools());
        }
        let client = self.client.read().await;
        let (tools, complete, ttl_ms) = list_tools_from_client(&client).await?;
        drop(client);
        self.remember_tools(&tools, complete, ttl_ms);
        Ok(tools)
    }

    async fn get_tool(
        &self,
        name: &str,
    ) -> std::result::Result<Option<rmcp::model::Tool>, ServiceError> {
        if self.cache_is_fresh() {
            return Ok(self.cached_tool(name));
        }
        match self.list_tools().await {
            Ok(_) => Ok(self.cached_tool(name)),
            Err(e) => self.cached_tool(name).map_or(Err(e), |tool| Ok(Some(tool))),
        }
    }
}

/// Page through `tools/list`. `complete` is true only when the peer
/// finished (no cursor) before the pagination or total-tool caps.
async fn list_tools_from_client(
    client: &McpClient,
) -> std::result::Result<(Vec<rmcp::model::Tool>, bool, u64), ServiceError> {
    // Security: Limit pagination to prevent infinite loops and memory exhaustion
    const MAX_PAGINATION_ITERATIONS: usize = 100;
    const MAX_TOTAL_TOOLS: usize = 10000;

    let mut cursor: Option<String> = None;
    let mut tools: Vec<rmcp::model::Tool> = vec![];
    let mut iterations = 0;
    let mut ttl_ms: u64 = 0;
    let mut seen_page = false;

    loop {
        iterations += 1;
        if iterations > MAX_PAGINATION_ITERATIONS {
            tracing::warn!(
                "MCP tool listing exceeded maximum pagination iterations ({MAX_PAGINATION_ITERATIONS}), stopping iteration"
            );
            return Ok((tools, false, ttl_ms));
        }

        let response = client
            .list_tools(Some(
                PaginatedRequestParams::default().with_cursor(cursor.clone()),
            ))
            .await?;
        ttl_ms = fold_page_ttl(ttl_ms, response.ttl_ms, seen_page);
        seen_page = true;

        // Security: Validate total tools count to prevent memory exhaustion
        if tools.len().saturating_add(response.tools.len()) > MAX_TOTAL_TOOLS {
            tracing::warn!(
                "MCP tool listing exceeded maximum tools count ({MAX_TOTAL_TOOLS}), limiting results"
            );
            let remaining = MAX_TOTAL_TOOLS - tools.len();
            tools.extend(response.tools.into_iter().take(remaining));
            return Ok((tools, false, ttl_ms));
        }

        tools.extend(response.tools);
        cursor = response.next_cursor;
        if cursor.is_none() {
            return Ok((tools, true, ttl_ms));
        }
    }
}

fn fold_page_ttl(acc: u64, page: Option<u64>, seen_page: bool) -> u64 {
    let page = page.unwrap_or(0);
    if !seen_page {
        return page;
    }
    if acc == 0 || page == 0 {
        0
    } else {
        acc.min(page)
    }
}

fn expires_at_from_ttl_ms(ttl_ms: u64, now: Instant) -> Option<Instant> {
    (ttl_ms > 0).then(|| now.checked_add(Duration::from_millis(ttl_ms)))?
}

fn list_cache_is_fresh(expires_at: Option<Instant>, now: Instant) -> bool {
    expires_at.is_some_and(|deadline| now < deadline)
}

fn apply_tool_cache(
    cache: &mut ToolListCache,
    tools: &[rmcp::model::Tool],
    replace: bool,
    ttl_ms: u64,
) {
    if replace {
        cache.tools.clear();
    }
    for tool in tools {
        cache.tools.insert(tool.name.to_string(), tool.clone());
    }
    cache.expires_at = expires_at_from_ttl_ms(ttl_ms, Instant::now());
}

/// Dual-era client startup: prefer `server/discover` + `2026-07-28`, and fall
/// back to the legacy `initialize` handshake when the peer is pre-2026.
fn client_lifecycle() -> ClientLifecycleMode {
    ClientLifecycleMode::Auto {
        preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        legacy_version: Some(ProtocolVersion::V_2025_03_26),
    }
}

pub enum McpClient {
    Stdio(RunningService<RoleClient, ()>),
    Http(RunningService<RoleClient, InitializeRequestParams>),
}

impl McpClient {
    pub async fn list_tools(
        &self,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListToolsResult, ServiceError> {
        match self {
            McpClient::Stdio(s) => s.list_tools(params).await,
            McpClient::Http(s) => s.list_tools(params).await,
        }
    }
    pub async fn call_tool(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResult, ServiceError> {
        match self {
            McpClient::Stdio(s) => s.call_tool(params).await,
            McpClient::Http(s) => s.call_tool(params).await,
        }
    }

    /// One `tools/call` without driving MRTR follow-up rounds.
    ///
    /// The high-level [`Self::call_tool`] fulfils `input_required` locally and
    /// returns only [`CallToolResult`]. The `/v1/mcp` gateway must use this so
    /// a downstream client can continue the round trip.
    pub async fn call_tool_once(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResponse, ServiceError> {
        match self {
            McpClient::Stdio(s) => s.call_tool_once(params).await,
            McpClient::Http(s) => s.call_tool_once(params).await,
        }
    }

    pub async fn ping(&self) -> Result<(), ServiceError> {
        let result = match self {
            McpClient::Stdio(s) => {
                s.peer()
                    .send_request(ClientRequest::PingRequest(PingRequest::default()))
                    .await?
            }
            McpClient::Http(s) => {
                s.peer()
                    .send_request(ClientRequest::PingRequest(PingRequest::default()))
                    .await?
            }
        };
        match result {
            ServerResult::EmptyResult(_) => Ok(()),
            _ => Err(ServiceError::UnexpectedResponse),
        }
    }

    /// Liveness check that works for both protocol eras.
    ///
    /// `ping` is not part of `2026-07-28`. A modern peer still answers
    /// `tools/list`, so a failed ping is retried as a list before the catalog
    /// treats the connection as dead.
    pub async fn heartbeat(&self) -> Result<(), ServiceError> {
        match self.ping().await {
            Ok(()) => Ok(()),
            Err(ping_err) => match self.list_tools(None).await {
                Ok(_) => Ok(()),
                Err(_) => Err(ping_err),
            },
        }
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

    fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        let spec = self.cached_tool(name)?;
        Some(Arc::new(McpToolWrapper::new(
            Arc::clone(&self.client),
            spec,
            self.name.clone(),
        )))
    }

    fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        let Ok(cache) = self.tool_cache.read() else {
            return Vec::new();
        };
        cache
            .tools
            .values()
            .map(|spec| {
                Arc::new(McpToolWrapper::new(
                    Arc::clone(&self.client),
                    spec.clone(),
                    self.name.clone(),
                )) as Arc<dyn SpiceModelTool>
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    fn sample_listed_tool(name: &'static str) -> rmcp::model::Tool {
        rmcp::model::Tool::new_with_raw(name, None, serde_json::Map::new())
    }

    #[test]
    fn complete_list_replaces_removed_tools() {
        let mut cache = ToolListCache::default();
        apply_tool_cache(
            &mut cache,
            &[sample_listed_tool("keep"), sample_listed_tool("drop")],
            true,
            5_000,
        );
        apply_tool_cache(
            &mut cache,
            &[sample_listed_tool("keep"), sample_listed_tool("new")],
            true,
            5_000,
        );
        assert!(cache.tools.contains_key("keep"));
        assert!(cache.tools.contains_key("new"));
        assert!(
            !cache.tools.contains_key("drop"),
            "a complete tools/list must drop tools the peer no longer advertises"
        );
    }

    #[test]
    fn incomplete_list_keeps_existing_tools() {
        let mut cache = ToolListCache::default();
        apply_tool_cache(&mut cache, &[sample_listed_tool("keep")], true, 5_000);
        apply_tool_cache(&mut cache, &[sample_listed_tool("page")], false, 5_000);
        assert!(
            cache.tools.contains_key("keep"),
            "a truncated page must not wipe tools from an earlier complete list"
        );
        assert!(cache.tools.contains_key("page"));
    }

    #[test]
    fn omitted_or_zero_ttl_is_immediately_stale() {
        let now = Instant::now();
        assert!(
            !list_cache_is_fresh(expires_at_from_ttl_ms(0, now), now),
            "omitted or zero ttlMs is immediately stale"
        );
        let later = now + Duration::from_millis(1);
        assert!(list_cache_is_fresh(
            expires_at_from_ttl_ms(5_000, now),
            later
        ));
        assert!(!list_cache_is_fresh(
            expires_at_from_ttl_ms(5_000, now),
            now + Duration::from_secs(6)
        ));
    }

    #[test]
    fn a_zero_ttl_page_makes_the_whole_list_stale() {
        assert_eq!(fold_page_ttl(0, Some(5_000), false), 5_000);
        assert_eq!(fold_page_ttl(5_000, Some(0), true), 0);
        assert_eq!(fold_page_ttl(5_000, None, true), 0);
        assert_eq!(fold_page_ttl(0, None, false), 0);
    }

    fn listed_deploy_with_header(header: &str) -> rmcp::model::Tool {
        let mut schema = serde_json::Map::new();
        schema.insert("type".into(), json!("object"));
        schema.insert(
            "properties".into(),
            json!({
                "region": { "type": "string", "x-mcp-header": header }
            }),
        );
        rmcp::model::Tool::new_with_raw(
            "deploy",
            Some(std::borrow::Cow::Borrowed("deploy a thing")),
            schema,
        )
    }

    fn x_mcp_header(tool: &rmcp::model::Tool) -> Option<&str> {
        tool.input_schema
            .get("properties")
            .and_then(|properties| properties.get("region"))
            .and_then(|region| region.get("x-mcp-header"))
            .and_then(serde_json::Value::as_str)
    }

    /// Catalog TTL / reconnect used to rewrite only `tool_cache`. The
    /// gateway snapshot (and therefore Streamable HTTP) kept `Region`
    /// until a later `tools/list`. Publishing the cache identity change
    /// bumps the snapshot without that intervening list.
    #[test]
    fn catalog_cache_refresh_publishes_schema_identity_change_to_snapshot() {
        use tools::naming::encode_tool_name;

        let snapshot = McpSchemaSnapshot::new();
        let mut cache = ToolListCache::default();
        apply_tool_cache(
            &mut cache,
            &[listed_deploy_with_header("Region")],
            true,
            0,
        );
        let changed = apply_listed_catalog_cache(&snapshot, "srv", &cache.tools, true);
        snapshot.bump_if(changed);
        let epoch = snapshot.epoch();
        let exposed = encode_tool_name("srv", "deploy");
        let first = snapshot
            .get(&exposed)
            .expect("snapshot should hold the catalog tool after the first publish");
        assert_eq!(x_mcp_header(&first), Some("Region"));

        apply_tool_cache(&mut cache, &[listed_deploy_with_header("Zone")], true, 0);
        assert_eq!(
            cache.tools.get("deploy").and_then(x_mcp_header),
            Some("Zone"),
            "catalog cache after upstream refresh: Zone"
        );
        let still_region = snapshot
            .get(&exposed)
            .expect("snapshot still holds the pre-publish schema");
        assert_eq!(
            x_mcp_header(&still_region),
            Some("Region"),
            "direct call schema after cache-only refresh: Region"
        );

        let changed = apply_listed_catalog_cache(&snapshot, "srv", &cache.tools, true);
        assert!(
            changed,
            "Region → Zone on the same catalog tool must count as a schema change"
        );
        snapshot.bump_if(changed);
        assert!(
            snapshot.epoch() > epoch,
            "catalog cache identity change must bump the snapshot epoch without a gateway tools/list"
        );
        let rewritten = snapshot
            .get(&exposed)
            .expect("snapshot should still hold deploy after the rewrite");
        assert_eq!(x_mcp_header(&rewritten), Some("Zone"));
    }

    #[test]
    fn client_lifecycle_prefers_2026_07_28_with_legacy_fallback() {
        match client_lifecycle() {
            ClientLifecycleMode::Auto {
                preferred_versions,
                legacy_version,
            } => {
                assert_eq!(preferred_versions, vec![ProtocolVersion::V_2026_07_28]);
                assert_eq!(legacy_version, Some(ProtocolVersion::V_2025_03_26));
            }
            other => panic!("expected Auto lifecycle, got {other:?}"),
        }
    }
}
