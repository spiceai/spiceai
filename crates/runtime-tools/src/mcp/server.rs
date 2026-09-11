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

use crate::mcp::task_name_for_exposed_tool;
use crate::tooling::Tooling;

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        ListToolsResult, PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo,
        Tool,
    },
    service::RequestContext,
};
use serde_json::{Map, Value, json};
use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::RwLock;
use tools::SpiceModelTool;
use tools::naming::{decode_tool_name, encode_tool_name};
use tools::rename::with_name;
use tracing_futures::Instrument;
use util::security::{MAX_SAFE_JSON_DEPTH, get_json_depth};

/// Shared MCP tool schemas plus a generation counter.
///
/// rmcp 3.3.0 `StreamableHttpService::tool_schema` caches `get_tool`'s
/// `Option` per name, including `None`. When the registry mutates, bump
/// [`Self::epoch`] so the HTTP layer can rebuild that service and drop
/// stale misses.
#[derive(Default)]
pub struct McpSchemaSnapshot {
    tools: StdRwLock<HashMap<String, Tool>>,
    epoch: AtomicU64,
}

impl McpSchemaSnapshot {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn get(&self, name: &str) -> Option<Tool> {
        self.tools
            .read()
            .ok()
            .and_then(|schemas| schemas.get(name).cloned())
    }

    /// Replace the snapshot from the live tool map and bump [`Self::epoch`].
    pub fn replace_from_map(&self, tools: &HashMap<String, Tooling>) {
        let next = mcp_schemas_from_map(tools);
        if let Ok(mut schemas) = self.tools.write() {
            *schemas = next;
        }
        self.epoch.fetch_add(1, Ordering::Release);
    }

    pub fn clear(&self) {
        if let Ok(mut schemas) = self.tools.write() {
            schemas.clear();
        }
        self.epoch.fetch_add(1, Ordering::Release);
    }

    fn insert(&self, name: String, tool: Tool) -> bool {
        let Ok(mut schemas) = self.tools.write() else {
            return false;
        };
        let changed = schemas.get(&name).is_none_or(|existing| existing != &tool);
        schemas.insert(name, tool);
        changed
    }

    fn merge_from_map(&self, tools: &HashMap<String, Tooling>) -> bool {
        let next = mcp_schemas_from_map(tools);
        let Ok(mut schemas) = self.tools.write() else {
            return false;
        };
        let mut changed = false;
        for (name, tool) in next {
            if schemas.get(&name).is_none_or(|existing| existing != &tool) {
                schemas.insert(name, tool);
                changed = true;
            }
        }
        changed
    }

    fn replace_listed(&self, tools: &[Tool]) -> bool {
        let next: HashMap<String, Tool> = tools
            .iter()
            .map(|tool| (tool.name.to_string(), tool.clone()))
            .collect();
        let Ok(mut schemas) = self.tools.write() else {
            return false;
        };
        let changed = schema_maps_changed(&schemas, &next);
        *schemas = next;
        changed
    }

    fn bump_if(&self, changed: bool) {
        if changed {
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }
}

#[derive(Clone)]
pub struct RuntimeServer {
    tools: Arc<RwLock<HashMap<String, Tooling>>>,
    /// Sync copy of MCP tool definitions. rmcp caches `get_tool`'s `Option`
    /// per name, including `None`, so a transient miss would disable
    /// `Mcp-Param-*` checks for every later call. This map is the source
    /// `get_tool` consults first and is shared across HTTP service clones.
    schemas: Arc<McpSchemaSnapshot>,
}

/// A tool resolved from a request name, with the identity to record it under.
struct ResolvedTool {
    tool: Arc<dyn SpiceModelTool>,
    /// The canonical name the tool is exposed as — see [`RuntimeServer::get_tool`].
    exposed_name: String,
    /// The catalog the tool came from, or `None` for a top-level tool.
    catalog: Option<String>,
}

impl ResolvedTool {
    /// The `task_history` labels for a call on this tool: the `task` override,
    /// and the MCP server to attribute the call to.
    ///
    /// `mcp_server` names the server a call was proxied to, so it is reported
    /// only when the resolve found a catalog. A top-level tool came from none,
    /// and labelling one with its own name would report a server that does not
    /// exist — the `__` in a name like `top__level` makes it look qualified by a
    /// catalog even though nothing served it.
    fn task_history_labels(&self) -> (String, Option<&str>) {
        (
            task_name_for_exposed_tool(&self.exposed_name),
            self.catalog.as_deref(),
        )
    }
}

impl RuntimeServer {
    pub fn new(tools: Arc<RwLock<HashMap<String, Tooling>>>) -> Self {
        Self::with_schema_snapshot(tools, McpSchemaSnapshot::new())
    }

    /// Build a server that shares `schemas` with other factory clones.
    ///
    /// Streamable HTTP constructs a new [`RuntimeServer`] per request; the
    /// snapshot must outlive that so a successful `tools/list` (or lookup)
    /// is visible to the next `tools/call`.
    #[must_use]
    pub fn with_schema_snapshot(
        tools: Arc<RwLock<HashMap<String, Tooling>>>,
        schemas: Arc<McpSchemaSnapshot>,
    ) -> Self {
        let server = Self { tools, schemas };
        if let Ok(guard) = server.tools.try_read() {
            server.remember_from_map(&guard);
        }
        server
    }

    /// Resolve `tool_name` to a tool and the canonical name that tool is exposed
    /// under.
    ///
    /// The requested name is not a stable identity for the tool: `decode_tool_name`
    /// accepts a component's `__` both escaped (`tool_-_name`, what the encoder
    /// emits) and raw (`tool__name`), so several spellings resolve to the same
    /// `(catalog, tool)` pair and all of them execute. A caller that records the
    /// call must label it with the returned canonical name — labelling with the
    /// requested one splits a single tool's `task_history` rows by whichever
    /// spelling each caller happened to send.
    async fn get_tool(&self, tool_name: &str) -> Option<ResolvedTool> {
        let tools = self.tools.read().await;
        if let Some((catalog_name, name)) = decode_tool_name(tool_name)
            && let Some(Tooling::Catalog { tools: catalog, .. }) = tools.get(&catalog_name)
            && let Some(tool) = catalog.get(&name).await
        {
            return Some(ResolvedTool {
                tool,
                exposed_name: encode_tool_name(&catalog_name, &name),
                catalog: Some(catalog_name),
            });
        }
        // Fall back to a direct (non-catalog) lookup. This covers top-level
        // tools whose names legitimately contain the `__` catalog separator.
        // Such a tool is exposed under its own name, so that name is already
        // canonical and must not be re-encoded — and it belongs to no catalog,
        // however much its name may look like one qualified by the separator.
        match tools.get(tool_name)? {
            Tooling::Tool(tool) | Tooling::FunctionTool(tool) => Some(ResolvedTool {
                tool: Arc::clone(tool),
                exposed_name: tool_name.to_string(),
                catalog: None,
            }),
            Tooling::Catalog { .. } => None,
        }
    }

    async fn all_tools(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        let tools = self.tools.read().await;
        let mut result = Vec::new();
        for tooling in tools.values() {
            match tooling {
                Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                    result.push(Arc::clone(tool));
                }
                Tooling::Catalog { tools: catalog, .. } => {
                    let catalog_name = catalog.name();
                    for tool in catalog.all().await {
                        result.push(with_name(
                            &tool,
                            encode_tool_name(catalog_name, &tool.name()).as_str(),
                        ));
                    }
                }
            }
        }
        result
    }

    fn snapshot_tool(&self, name: &str) -> Option<Tool> {
        self.schemas.get(name)
    }

    fn remember_tool(&self, name: String, tool: Tool) {
        let added = self.schemas.insert(name, tool);
        self.schemas.bump_if(added);
    }

    fn remember_from_map(&self, tools: &HashMap<String, Tooling>) {
        let added = self.schemas.merge_from_map(tools);
        self.schemas.bump_if(added);
    }

    fn definition_from_map(tools: &HashMap<String, Tooling>, tool_name: &str) -> Option<Tool> {
        if let Some((catalog_name, name)) = decode_tool_name(tool_name)
            && let Some(Tooling::Catalog { tools: catalog, .. }) = tools.get(&catalog_name)
            && let Some(tool) = catalog.try_get(&name)
        {
            return Some(mcp_tool_from_spice(
                encode_tool_name(&catalog_name, &name),
                tool.as_ref(),
            ));
        }
        match tools.get(tool_name)? {
            Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                Some(mcp_tool_from_spice(tool_name.to_string(), tool.as_ref()))
            }
            Tooling::Catalog { .. } => None,
        }
    }

    /// Sync tool definition for Streamable HTTP `Mcp-Param-*` validation.
    ///
    /// Consults the shared schema snapshot first. rmcp caches `get_tool`'s
    /// `Option` per name, including `None`, so this must not return a
    /// transient miss for a tool that still exists.
    ///
    /// Never waits on the Tokio tools map. rmcp 3.3.0 calls `get_tool`
    /// synchronously from async `handle_post`, and
    /// [`tokio::sync::RwLock::blocking_read`] panics in that context.
    /// An uncontended `try_read` refreshes the snapshot; a contended
    /// map falls back to whatever the snapshot already holds.
    fn mcp_tool_definition(&self, tool_name: &str) -> Option<Tool> {
        if let Some(tool) = self.snapshot_tool(tool_name) {
            return Some(tool);
        }
        let Ok(tools) = self.tools.try_read() else {
            return self.snapshot_tool(tool_name);
        };
        self.remember_from_map(&tools);
        let Some(tool) = Self::definition_from_map(&tools, tool_name) else {
            return self.snapshot_tool(tool_name);
        };
        self.remember_tool(tool_name.to_string(), tool.clone());
        Some(tool)
    }
}

impl ServerHandler for RuntimeServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "Spice.ai Open Source",
                env!("CARGO_PKG_VERSION"),
            ))
            // Prefer the 2026-07-28 revision. Dual-era clients that still send
            // `initialize` negotiate any version advertised by the default
            // `supported_protocol_versions()` (every revision this SDK knows).
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.mcp_tool_definition(name)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, McpError>> + Send + '_ {
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

            let Some(resolved) = self.get_tool(tool_name.as_ref()).await else {
                return Err(McpError::method_not_found::<
                    rmcp::model::CallToolRequestMethod,
                >());
            };

            // If possible, we pass the call through to the MCP server.
            if let Some(mcp_proxy) = resolved.tool.as_mcp_proxy().await {
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

                // Record the proxied call in task history so tool calls made
                // through the `/v1/mcp` gateway are audited identically to
                // model-driven tool calls (see `McpToolWrapper::call`). Without
                // this, gateway tool calls bypass the task_history span entirely.
                let input = serde_json::to_string(&arguments).unwrap_or_default();

                // Security: Validate serialized argument size to prevent DoS,
                // matching the non-proxy path below. `/v1/mcp` is externally
                // accessible, so reject oversized payloads before logging them
                // to task history or forwarding them upstream.
                if input.len() > MAX_ARGS_SIZE {
                    return Err(McpError::invalid_params(
                        format!(
                            "Arguments too large ({} bytes). Maximum: {MAX_ARGS_SIZE} bytes",
                            input.len()
                        ),
                        None,
                    ));
                }

                // Labelled from the canonical identity `get_tool` resolved, never
                // the requested spelling — see `get_tool`.
                let exposed_name = &resolved.exposed_name;
                let (task_name, mcp_server) = resolved.task_history_labels();
                let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::mcp", tool = %exposed_name, input = %input);
                tracing::info!(target: "task_history", parent: &span, task_override = %task_name, "labels");
                if let Some(mcp_server) = mcp_server {
                    tracing::info!(target: "task_history", parent: &span, mcp_server = %mcp_server, "labels");
                }

                return match mcp_proxy
                    .call_tool(arguments)
                    .instrument(span.clone())
                    .await
                {
                    Ok(result) => {
                        if let Ok(captured_output) = serde_json::to_string(&result.content) {
                            tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                        }
                        Ok(result.into())
                    }
                    Err(e) => {
                        tracing::error!(target: "task_history", parent: &span, "{e}");
                        Err(McpError::internal_error(e.to_string(), None))
                    }
                };
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

            let result = resolved
                .tool
                .call(args.as_str())
                .await
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            let text = serde_json::to_string(&result)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;

            Ok(CallToolResult::success(vec![ContentBlock::text(text)]).into())
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
                    let name = t.name().into_owned();
                    mcp_tool_from_spice(name, t.as_ref())
                })
                .collect::<Vec<_>>();
            let changed = self.schemas.replace_listed(&tools);
            self.schemas.bump_if(changed);
            Ok(ListToolsResult {
                tools,
                ..ListToolsResult::default()
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

fn empty_input_schema() -> Value {
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "empty",
        "type": "object",
        "required": [],
        "properties": {}
    })
}

/// True when the snapshot's tool set or any stored schema identity changed.
///
/// Name-only comparison would miss an in-place `x-mcp-header` or input-schema
/// edit, and rmcp would keep validating against the stale cached contract.
fn schema_maps_changed(current: &HashMap<String, Tool>, next: &HashMap<String, Tool>) -> bool {
    current != next
}

/// Collect MCP tool definitions that catalogs can expose without I/O.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn mcp_schemas_from_map(tools: &HashMap<String, Tooling>) -> HashMap<String, Tool> {
    let mut schemas = HashMap::new();
    for (name, tooling) in tools {
        match tooling {
            Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                schemas.insert(
                    name.clone(),
                    mcp_tool_from_spice(name.clone(), tool.as_ref()),
                );
            }
            Tooling::Catalog { tools: catalog, .. } => {
                let catalog_name = catalog.name();
                for tool in catalog.try_all() {
                    let exposed = encode_tool_name(catalog_name, &tool.name());
                    schemas.insert(exposed.clone(), mcp_tool_from_spice(exposed, tool.as_ref()));
                }
            }
        }
    }
    schemas
}

fn mcp_tool_from_spice(name: impl Into<Cow<'static, str>>, tool: &dyn SpiceModelTool) -> Tool {
    let description: Option<Cow<'static, str>> =
        tool.description().map(|s| Cow::Owned(s.into_owned()));
    let schema = to_map(tool.parameters().unwrap_or_else(empty_input_schema));
    Tool::new_with_raw(name.into(), description, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SpiceToolCatalog;

    struct StubTool(&'static str);

    #[async_trait::async_trait]
    impl SpiceModelTool for StubTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }
        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn parameters(&self) -> Option<Value> {
            None
        }
        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::Null)
        }
    }

    /// A catalog holding one tool, looked up by its exact upstream name — the
    /// same contract `McpToolCatalog::get` has.
    struct StubCatalog {
        name: &'static str,
        tool: &'static str,
    }

    #[async_trait::async_trait]
    impl SpiceToolCatalog for StubCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            self.name
        }
        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![Arc::new(StubTool(self.tool)) as Arc<dyn SpiceModelTool>]
        }
        async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == self.tool).then(|| Arc::new(StubTool(self.tool)) as Arc<dyn SpiceModelTool>)
        }

        fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == self.tool).then(|| Arc::new(StubTool(self.tool)) as Arc<dyn SpiceModelTool>)
        }

        fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![Arc::new(StubTool(self.tool)) as Arc<dyn SpiceModelTool>]
        }
    }

    struct ZoneAnnotatedTool;

    #[async_trait::async_trait]
    impl SpiceModelTool for ZoneAnnotatedTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("deploy")
        }
        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("deploy a thing"))
        }
        fn parameters(&self) -> Option<Value> {
            Some(json!({
                "type": "object",
                "properties": {
                    "region": { "type": "string", "x-mcp-header": "Zone" }
                }
            }))
        }
        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(json!({ "ok": true }))
        }
    }

    struct HeaderAnnotatedTool;

    #[async_trait::async_trait]
    impl SpiceModelTool for HeaderAnnotatedTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("deploy")
        }
        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("deploy a thing"))
        }
        fn parameters(&self) -> Option<Value> {
            Some(json!({
                "type": "object",
                "properties": {
                    "region": { "type": "string", "x-mcp-header": "Region" }
                }
            }))
        }
        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(json!({ "ok": true }))
        }
    }

    fn server_with(catalog_name: &'static str, tool_name: &'static str) -> RuntimeServer {
        let mut tools = HashMap::new();
        tools.insert(
            catalog_name.to_string(),
            Tooling::Catalog {
                tools: Arc::new(StubCatalog {
                    name: catalog_name,
                    tool: tool_name,
                }) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        RuntimeServer::new(Arc::new(RwLock::new(tools)))
    }

    #[tokio::test]
    async fn aliased_request_spellings_resolve_to_one_exposed_name() {
        // Part of https://github.com/spiceai/spiceai/issues/13338: the gateway
        // used to label the task with the requested name, but `decode_tool_name`
        // accepts a component's `__` both escaped (`tool_-_name`, what the
        // encoder emits) and raw (`tool__name`). Both spellings reach the same
        // tool and execute, so labelling from the request would still split one
        // tool across two `task_history` rows.
        let server = server_with("srv", "tool__name");
        let canonical = encode_tool_name("srv", "tool__name");
        assert_eq!(canonical, "srv__tool_-_name");

        for requested in [canonical.as_str(), "srv__tool__name"] {
            let resolved = server
                .get_tool(requested)
                .await
                .unwrap_or_else(|| panic!("{requested} should resolve"));
            assert_eq!(
                resolved.exposed_name, canonical,
                "request {requested} was not canonicalized"
            );
            let (task_name, mcp_server) = resolved.task_history_labels();
            assert_eq!(task_name, "tool_use::srv__tool_-_name");
            // The server is reported from the resolve, not re-derived by
            // decoding the canonical name a second time.
            assert_eq!(mcp_server, Some("srv"));
        }
    }

    #[tokio::test]
    async fn a_top_level_tool_keeps_its_own_name() {
        // A non-catalog tool is exposed under its own name, even when that name
        // contains the `__` separator, so re-encoding it would rename the tool.
        let mut tools = HashMap::new();
        tools.insert(
            "top__level".to_string(),
            Tooling::Tool(Arc::new(StubTool("top__level")) as Arc<dyn SpiceModelTool>),
        );
        let server = RuntimeServer::new(Arc::new(RwLock::new(tools)));

        let resolved = server
            .get_tool("top__level")
            .await
            .expect("a top-level tool resolves by its own name");
        assert_eq!(resolved.exposed_name, "top__level");

        let (task_name, mcp_server) = resolved.task_history_labels();
        assert_eq!(task_name, "tool_use::top__level");
        // It came from no catalog, however much the `__` in its name looks like
        // one — so the call carries no `mcp_server` label at all, rather than a
        // phantom `top` or a server named after the tool itself.
        assert_eq!(mcp_server, None);
    }

    #[test]
    fn advertises_2026_07_28_and_keeps_legacy_initialize_versions() {
        let server = RuntimeServer::new(Arc::new(RwLock::new(HashMap::new())));
        let info = server.get_info();
        assert_eq!(info.protocol_version, ProtocolVersion::V_2026_07_28);
        let supported = server.supported_protocol_versions();
        assert!(
            supported.contains(&ProtocolVersion::V_2026_07_28),
            "modern clients must be able to negotiate 2026-07-28: {supported:?}"
        );
        assert!(
            supported.contains(&ProtocolVersion::V_2025_03_26),
            "legacy initialize clients on 2025-03-26 must still negotiate: {supported:?}"
        );
    }

    fn x_mcp_header_region(tool: &Tool) -> Option<&str> {
        tool.input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("region"))
            .and_then(Value::as_object)
            .and_then(|region| region.get("x-mcp-header"))
            .and_then(Value::as_str)
    }

    /// Name-only comparison would miss `x-mcp-header: Region` → `Zone` on
    /// the same `deploy` key. rmcp caches `get_tool` per name, so the
    /// snapshot epoch must bump or Streamable HTTP keeps the stale header.
    #[test]
    fn schema_maps_changed_when_same_name_x_mcp_header_rewrites() {
        let region = mcp_tool_from_spice("deploy", &HeaderAnnotatedTool);
        let zone = mcp_tool_from_spice("deploy", &ZoneAnnotatedTool);
        let current = HashMap::from([("deploy".to_string(), region.clone())]);
        let same = HashMap::from([("deploy".to_string(), region)]);
        let rewritten = HashMap::from([("deploy".to_string(), zone)]);
        assert!(!schema_maps_changed(&current, &same));
        assert!(
            schema_maps_changed(&current, &rewritten),
            "Region → Zone on the same deploy key must count as a schema change"
        );
    }

    #[test]
    fn replace_listed_treats_same_name_schema_identity_change_as_a_change() {
        let snapshot = McpSchemaSnapshot::default();
        let region = mcp_tool_from_spice("deploy", &HeaderAnnotatedTool);
        let zone = mcp_tool_from_spice("deploy", &ZoneAnnotatedTool);
        assert!(snapshot.insert("deploy".to_string(), region.clone()));
        snapshot.bump_if(true);
        let epoch = snapshot.epoch();

        let unchanged = snapshot.replace_listed(std::slice::from_ref(&region));
        assert!(
            !unchanged,
            "an identical listed schema must not count as a change"
        );
        snapshot.bump_if(unchanged);
        assert_eq!(snapshot.epoch(), epoch);

        let changed = snapshot.replace_listed(std::slice::from_ref(&zone));
        assert!(
            changed,
            "same-name x-mcp-header rewrite must count as a change"
        );
        snapshot.bump_if(changed);
        assert!(
            snapshot.epoch() > epoch,
            "same-name schema identity change must bump the snapshot epoch so Streamable HTTP reloads"
        );
        let tool = snapshot
            .get("deploy")
            .expect("deploy should still be present after the schema rewrite");
        assert_eq!(x_mcp_header_region(&tool), Some("Zone"));
    }

    #[test]
    fn insert_and_merge_treat_same_name_schema_identity_change_as_a_change() {
        let snapshot = McpSchemaSnapshot::default();
        assert!(snapshot.insert(
            "deploy".to_string(),
            mcp_tool_from_spice("deploy", &HeaderAnnotatedTool)
        ));
        snapshot.bump_if(true);
        let after_insert = snapshot.epoch();

        assert!(
            snapshot.insert(
                "deploy".to_string(),
                mcp_tool_from_spice("deploy", &ZoneAnnotatedTool)
            ),
            "insert of a same-name rewritten schema must report a change"
        );
        snapshot.bump_if(true);
        assert!(
            snapshot.epoch() > after_insert,
            "insert of a rewritten schema must bump the snapshot epoch"
        );

        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        assert!(
            snapshot.merge_from_map(&tools),
            "merge of a same-name rewritten schema must report a change"
        );
    }

    #[test]
    fn get_tool_returns_x_mcp_header_schema_for_top_level_tool() {
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        let server = RuntimeServer::new(Arc::new(RwLock::new(tools)));

        let top = ServerHandler::get_tool(&server, "deploy")
            .expect("top-level annotated tool must be visible to Streamable HTTP");
        assert_eq!(x_mcp_header_region(&top), Some("Region"));
    }

    #[test]
    fn get_tool_returns_x_mcp_header_schema_for_catalog_tool() {
        struct AnnotatedCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for AnnotatedCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
        }

        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(AnnotatedCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        let server = RuntimeServer::new(Arc::new(RwLock::new(tools)));
        let exposed = encode_tool_name("srv", "deploy");
        let from_catalog = ServerHandler::get_tool(&server, &exposed)
            .expect("catalog try_get must supply the schema for Mcp-Param validation");
        assert_eq!(x_mcp_header_region(&from_catalog), Some("Region"));
    }

    #[test]
    fn get_tool_survives_tools_map_write_lock_after_snapshot() {
        // rmcp 3.3.0 `tool_schema` caches `get_tool`'s Option, including None.
        // A contended try_read that returned None would disable Mcp-Param-*
        // checks for every later call of that name.
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        let tools = Arc::new(RwLock::new(tools));
        let server = RuntimeServer::new(Arc::clone(&tools));
        assert!(
            ServerHandler::get_tool(&server, "deploy").is_some(),
            "snapshot must be populated before contention"
        );

        let _write = tools.blocking_write();
        let under_contention = ServerHandler::get_tool(&server, "deploy");
        assert!(
            under_contention.is_some(),
            "write-lock contention must not return None after the snapshot is warm"
        );
        assert_eq!(
            x_mcp_header_region(&under_contention.expect("schema under contention")),
            Some("Region")
        );
    }

    /// `RwLock::blocking_read` panics when called from an async context.
    /// A cold snapshot plus a held write lock used to take that path.
    #[tokio::test]
    async fn get_tool_does_not_block_on_tokio_rwlock_from_async() {
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        let tools = Arc::new(RwLock::new(tools));
        let _write = tools.write().await;
        let server =
            RuntimeServer::with_schema_snapshot(Arc::clone(&tools), McpSchemaSnapshot::new());

        let looked_up = ServerHandler::get_tool(&server, "deploy");
        assert!(
            looked_up.is_none(),
            "cold snapshot + contended map must miss without waiting on the Tokio lock"
        );
    }

    #[tokio::test]
    async fn get_tool_uses_snapshot_under_async_write_lock() {
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        let tools = Arc::new(RwLock::new(tools));
        let server = RuntimeServer::new(Arc::clone(&tools));
        assert!(
            ServerHandler::get_tool(&server, "deploy").is_some(),
            "constructor must warm the snapshot"
        );

        let _write = tools.write().await;
        let under_contention = ServerHandler::get_tool(&server, "deploy");
        assert!(
            under_contention.is_some(),
            "a warm snapshot must answer get_tool from async handle_post without the Tokio lock"
        );
        assert_eq!(
            x_mcp_header_region(&under_contention.expect("schema under async contention")),
            Some("Region")
        );
    }

    fn header_annotated_server() -> RuntimeServer {
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        RuntimeServer::new(Arc::new(RwLock::new(tools)))
    }

    struct HeaderAnnotatedCatalog;

    #[async_trait::async_trait]
    impl SpiceToolCatalog for HeaderAnnotatedCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &'static str {
            "srv"
        }
        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
        }
        async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
        }
        fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
        }
        fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
        }
    }

    fn header_annotated_catalog_server() -> RuntimeServer {
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(HeaderAnnotatedCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        RuntimeServer::new(Arc::new(RwLock::new(tools)))
    }

    async fn post_tools_call<S>(
        service: &rmcp::transport::streamable_http_server::StreamableHttpService<
            S,
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
        >,
        tool_name: &str,
        region_header: Option<&str>,
        region_body: &str,
    ) -> (http::StatusCode, Value)
    where
        S: ServerHandler,
    {
        post_tools_call_param(service, tool_name, "region", region_header, region_body).await
    }

    async fn post_tools_call_param<S>(
        service: &rmcp::transport::streamable_http_server::StreamableHttpService<
            S,
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
        >,
        tool_name: &str,
        param: &str,
        param_header: Option<&str>,
        param_body: &str,
    ) -> (http::StatusCode, Value)
    where
        S: ServerHandler,
    {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": { "region": param_body },
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "runtime-tools-test",
                        "version": "0.0.0"
                    },
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        });
        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/call")
            .header("mcp-name", tool_name);
        if let Some(value) = param_header {
            builder = builder.header(format!("mcp-param-{param}"), value);
        }
        let request = builder
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .expect("valid tools/call request");
        let response = service.handle(request).await;
        let status = response.status();
        let collected = http_body_util::BodyExt::collect(response.into_body())
            .await
            .expect("response body");
        let bytes = collected.to_bytes();
        let json_str = std::str::from_utf8(&bytes).unwrap_or("<non-utf8>");
        let json_payload = json_str
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap_or(json_str);
        let json: Value = serde_json::from_str(json_payload)
            .unwrap_or_else(|e| panic!("JSON-RPC body ({status}): {e}: {json_str:?}"));
        (status, json)
    }

    /// rmcp 3.3.0's default `get_tool` returns `None` and skips `Mcp-Param-*`
    /// checks. This is the Streamable HTTP failure #13792 requires us to close.
    #[derive(Clone, Copy)]
    struct SchemaLessServer;

    impl ServerHandler for SchemaLessServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_protocol_version(ProtocolVersion::V_2026_07_28)
        }

        fn call_tool(
            &self,
            _request: CallToolRequestParams,
            _context: RequestContext<RoleServer>,
        ) -> impl Future<Output = Result<CallToolResponse, McpError>> + Send + '_ {
            std::future::ready(Ok(
                CallToolResult::success(vec![ContentBlock::text("ok")]).into()
            ))
        }
    }

    #[tokio::test]
    async fn default_get_tool_skips_mcp_param_mismatch() {
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            || Ok(SchemaLessServer),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .disable_allowed_hosts()
                .with_json_response(true),
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "eu-west1").await;
        assert_ne!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "default get_tool must skip HeaderMismatch so RuntimeServer's override is load-bearing: {status} {json}"
        );
    }

    #[tokio::test]
    async fn mismatched_mcp_param_header_is_rejected() {
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            || Ok(header_annotated_server()),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .disable_allowed_hosts()
                .with_json_response(true),
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "eu-west1").await;
        assert_eq!(
            status,
            http::StatusCode::BAD_REQUEST,
            "mismatched Mcp-Param-Region must be HTTP 400, got {status}: {json}"
        );
        assert_eq!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "expected HeaderMismatch (-32020), got {json}"
        );
    }

    #[tokio::test]
    async fn matching_mcp_param_header_is_accepted() {
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            || Ok(header_annotated_server()),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .disable_allowed_hosts()
                .with_json_response(true),
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "matching Mcp-Param-Region must not be rejected: {status} {json}"
        );
        assert!(
            json.get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_i64)
                != Some(-32020),
            "matching headers must not raise HeaderMismatch: {json}"
        );
    }

    /// Direct `tools/call` before `tools/list` must still see a catalog
    /// `try_all` schema. rmcp caches the first `get_tool` result.
    #[tokio::test]
    async fn mismatched_mcp_param_header_is_rejected_for_catalog_tool() {
        let exposed = encode_tool_name("srv", "deploy");
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            || Ok(header_annotated_catalog_server()),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .disable_allowed_hosts()
                .with_json_response(true),
        );

        let (status, json) =
            post_tools_call(&service, &exposed, Some("us-west1"), "eu-west1").await;
        assert_eq!(
            status,
            http::StatusCode::BAD_REQUEST,
            "catalog tool Mcp-Param mismatch must be HTTP 400 before tools/list, got {status}: {json}"
        );
        assert_eq!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "expected HeaderMismatch (-32020) for catalog tool, got {json}"
        );
    }

    /// rmcp caches `get_tool == None`. A later registry update is invisible
    /// until Streamable HTTP is rebuilt (the HTTP layer watches snapshot epoch).
    #[tokio::test]
    async fn rebuilt_service_validates_after_tool_registers_following_a_miss() {
        let tools = Arc::new(RwLock::new(HashMap::new()));
        let schemas = McpSchemaSnapshot::new();
        let factory_tools = Arc::clone(&tools);
        let factory_schemas = Arc::clone(&schemas);
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true);
        let sessions = Arc::new(
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
        );
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            {
                let factory_tools = Arc::clone(&factory_tools);
                let factory_schemas = Arc::clone(&factory_schemas);
                move || {
                    Ok(RuntimeServer::with_schema_snapshot(
                        Arc::clone(&factory_tools),
                        Arc::clone(&factory_schemas),
                    ))
                }
            },
            Arc::clone(&sessions),
            config.clone(),
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "eu-west1").await;
        assert_ne!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "a miss must skip HeaderMismatch so the cached None is load-bearing: {status} {json}"
        );
        let epoch_after_miss = schemas.epoch();

        {
            let mut map = tools.write().await;
            map.insert(
                "deploy".to_string(),
                Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
            );
            schemas.replace_from_map(&map);
        }
        assert_ne!(
            schemas.epoch(),
            epoch_after_miss,
            "registering a tool must bump the snapshot epoch"
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "eu-west1").await;
        assert_ne!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "the same StreamableHttpService must keep the cached miss: {status} {json}"
        );

        let rebuilt = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            move || {
                Ok(RuntimeServer::with_schema_snapshot(
                    Arc::clone(&factory_tools),
                    Arc::clone(&factory_schemas),
                ))
            },
            sessions,
            config,
        );
        let (status, json) =
            post_tools_call(&rebuilt, "deploy", Some("us-west1"), "eu-west1").await;
        assert_eq!(
            status,
            http::StatusCode::BAD_REQUEST,
            "rebuilt service must validate Mcp-Param after the tool appears: {status} {json}"
        );
        assert_eq!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "expected HeaderMismatch (-32020) after rebuild, got {json}"
        );
    }

    /// rmcp caches `get_tool` per name. After `deploy` rewrites
    /// `x-mcp-header` from `Region` to `Zone`, the existing
    /// `StreamableHttpService` still validates `mcp-param-region`; a
    /// rebuilt service validates `mcp-param-zone`.
    #[tokio::test]
    async fn rebuilt_service_validates_after_same_name_schema_identity_change() {
        let mut tools = HashMap::new();
        tools.insert(
            "deploy".to_string(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        let tools = Arc::new(RwLock::new(tools));
        let schemas = McpSchemaSnapshot::new();
        schemas.replace_from_map(&*tools.read().await);
        let first_epoch = schemas.epoch();

        let factory_tools = Arc::clone(&tools);
        let factory_schemas = Arc::clone(&schemas);
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true);
        let sessions = Arc::new(
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
        );
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            {
                let factory_tools = Arc::clone(&factory_tools);
                let factory_schemas = Arc::clone(&factory_schemas);
                move || {
                    Ok(RuntimeServer::with_schema_snapshot(
                        Arc::clone(&factory_tools),
                        Arc::clone(&factory_schemas),
                    ))
                }
            },
            Arc::clone(&sessions),
            config.clone(),
        );

        let (status, json) =
            post_tools_call(&service, "deploy", Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "matching Mcp-Param-Region must be accepted before the rewrite: {status} {json}"
        );

        {
            let mut map = tools.write().await;
            map.insert(
                "deploy".to_string(),
                Tooling::Tool(Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>),
            );
            let listed = vec![mcp_tool_from_spice("deploy", &ZoneAnnotatedTool)];
            let changed = schemas.replace_listed(&listed);
            schemas.bump_if(changed);
            assert!(
                changed,
                "Region → Zone on deploy must count as a listed-schema change"
            );
        }
        assert!(
            schemas.epoch() > first_epoch,
            "same-name schema identity change must bump the snapshot epoch"
        );

        let (status, json) =
            post_tools_call_param(&service, "deploy", "zone", Some("us-west1"), "us-west1").await;
        assert_eq!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32020),
            "existing StreamableHttpService must still validate mcp-param-region: {status} {json}"
        );

        let rebuilt = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            move || {
                Ok(RuntimeServer::with_schema_snapshot(
                    Arc::clone(&factory_tools),
                    Arc::clone(&factory_schemas),
                ))
            },
            sessions,
            config,
        );
        let (status, json) =
            post_tools_call_param(&rebuilt, "deploy", "zone", Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "rebuilt service must validate mcp-param-zone after the schema rewrite: {status} {json}"
        );
        assert!(
            json.get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_i64)
                != Some(-32020),
            "matching Zone header must not raise HeaderMismatch: {json}"
        );
    }
}
