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
        CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
        Implementation, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
        ServerCapabilities, ServerInfo, Tool,
    },
    service::RequestContext,
};
use serde_json::{Map, Value, json};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        Arc, Mutex as StdMutex, RwLock as StdRwLock,
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
    /// Top-level / function-tool schemas keyed by exposed name.
    ///
    /// A catalog publication overwrites [`Self::tools`] on a colliding
    /// [`encode_tool_name`] key. A later complete catalog list that
    /// drops that tool must restore this schema: deleting the key
    /// leaves `get_tool` empty under tools-map contention and rmcp
    /// caches that `None`, disabling `Mcp-Param-*` for the live tool
    /// (`dispatched=top:Region validated_schema=None`).
    direct: StdRwLock<HashMap<String, Tool>>,
    epoch: AtomicU64,
    /// Serializes [`Self::replace_from_map`] and [`Self::merge_from_map`]
    /// with catalog publications so a full-map update cannot overwrite
    /// a newer TTL/reconnect schema (and still bump the epoch).
    publish: StdMutex<()>,
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
    ///
    /// Computes `next` under [`Self::publish`] so a concurrent catalog
    /// TTL/reconnect publish waits, then applies after this write.
    /// A precomputed map taken outside that lock can overwrite a newer
    /// catalog schema while the epoch still advances.
    pub fn replace_from_map(&self, tools: &HashMap<String, Tooling>) {
        let _publish = self.lock_publish();
        self.replace_direct_from_map(tools);
        let next = mcp_schemas_from_map(tools);
        if let Ok(mut schemas) = self.tools.write() {
            *schemas = next;
        }
        self.epoch.fetch_add(1, Ordering::Release);
    }

    fn lock_publish(&self) -> std::sync::MutexGuard<'_, ()> {
        self.publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn clear(&self) {
        if let Ok(mut schemas) = self.tools.write() {
            schemas.clear();
        }
        if let Ok(mut direct) = self.direct.write() {
            direct.clear();
        }
        self.epoch.fetch_add(1, Ordering::Release);
    }

    fn replace_direct_from_map(&self, tools: &HashMap<String, Tooling>) {
        let next = mcp_direct_schemas_from_map(tools);
        if let Ok(mut direct) = self.direct.write() {
            *direct = next;
        }
    }

    fn direct_schema(&self, name: &str) -> Option<Tool> {
        self.direct
            .read()
            .ok()
            .and_then(|direct| direct.get(name).cloned())
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
        // Collect under `publish` so a concurrent catalog TTL/reconnect
        // write waits, then applies after this merge — the same protocol
        // as [`Self::replace_from_map`]. Computing `next` first lets a
        // stale Region page overwrite a Zone that already published.
        let _publish = self.lock_publish();
        self.replace_direct_from_map(tools);
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

    #[cfg(test)]
    fn replace_listed(&self, tools: &[Tool]) -> bool {
        let _publish = self.lock_publish();
        self.install_listed(tools)
    }

    /// Replace the snapshot from the live tool map under [`Self::publish`].
    ///
    /// A precomputed `tools/list` page collected before this lock can
    /// be older than a concurrent catalog TTL/reconnect publish. Reading
    /// `try_all` here means that refresh either waits and applies after
    /// this write, or this write already sees the refreshed cache.
    fn replace_listed_from_map(&self, tools: &HashMap<String, Tooling>) -> (Vec<Tool>, bool) {
        let _publish = self.lock_publish();
        self.replace_direct_from_map(tools);
        let next = mcp_schemas_from_map(tools);
        let listed = tools_listed_by_name(&next);
        (listed, self.install_listed_map(next))
    }

    #[cfg(test)]
    fn install_listed(&self, tools: &[Tool]) -> bool {
        let next: HashMap<String, Tool> = tools
            .iter()
            .map(|tool| (tool.name.to_string(), tool.clone()))
            .collect();
        self.install_listed_map(next)
    }

    fn install_listed_map(&self, next: HashMap<String, Tool>) -> bool {
        let Ok(mut schemas) = self.tools.write() else {
            return false;
        };
        let changed = schema_maps_changed(&schemas, &next);
        *schemas = next;
        changed
    }

    /// Merge or replace gateway-exposed schemas for one proxied catalog.
    ///
    /// Catalog TTL / reconnect refreshes mutate the catalog cache, not this
    /// snapshot. rmcp caches `get_tool` per name, so a same-name rewrite
    /// (`Region` → `Zone`) must land here and bump [`Self::epoch`] or
    /// Streamable HTTP keeps validating the stale header until a later
    /// gateway `tools/list`.
    ///
    /// `tools` are already named with [`encode_tool_name`]. A complete list
    /// (`replace`) drops snapshot entries that decode to `catalog` and are
    /// no longer advertised, except a colliding top-level / function tool
    /// recorded in [`Self::direct`] — dispatch falls back to that tool,
    /// so its schema must stay visible to `get_tool`.
    fn apply_catalog_tools(&self, catalog: &str, tools: &[Tool], replace: bool) -> bool {
        let advertised: HashSet<String> = tools.iter().map(|tool| tool.name.to_string()).collect();
        let _publish = self.lock_publish();
        let Ok(mut schemas) = self.tools.write() else {
            return false;
        };
        let mut changed = false;
        if replace {
            let stale: Vec<String> = schemas
                .keys()
                .filter(|name| {
                    decode_tool_name(name)
                        .is_some_and(|(owner, _)| owner == catalog && !advertised.contains(*name))
                })
                .cloned()
                .collect();
            for name in stale {
                if let Some(direct) = self.direct_schema(&name) {
                    if schemas
                        .get(&name)
                        .is_none_or(|existing| existing != &direct)
                    {
                        schemas.insert(name, direct);
                        changed = true;
                    }
                } else {
                    schemas.remove(&name);
                    changed = true;
                }
            }
        }
        for tool in tools {
            let name = tool.name.to_string();
            if schemas.get(&name).is_none_or(|existing| existing != tool) {
                schemas.insert(name, tool.clone());
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn bump_if(&self, changed: bool) {
        if changed {
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }
}

/// Push a proxied catalog's `tools/list` cache into the shared snapshot.
///
/// Returns whether any stored schema identity changed so the caller can
/// bump [`McpSchemaSnapshot::epoch`].
pub(crate) fn apply_listed_catalog_cache(
    snapshot: &McpSchemaSnapshot,
    catalog: &str,
    listed: &HashMap<String, Tool>,
    replace: bool,
) -> bool {
    let tools: Vec<Tool> = listed
        .values()
        .map(|spec| mcp_tool_from_upstream(catalog, spec))
        .collect();
    snapshot.apply_catalog_tools(catalog, &tools, replace)
}

fn mcp_tool_from_upstream(catalog: &str, spec: &Tool) -> Tool {
    let exposed = encode_tool_name(catalog, spec.name.as_ref());
    Tool::new_with_raw(
        Cow::Owned(exposed),
        spec.description.clone(),
        Arc::clone(&spec.input_schema),
    )
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

/// Result of pinning `tools/call` dispatch to the schema generation
/// Streamable HTTP used for `Mcp-Param-*` validation.
enum ResolveOutcome {
    Ready(ResolvedTool),
    /// The tool was not in the snapshot the transport validated.
    /// The schema has been published; the caller must retry.
    Retry,
    Missing,
}

impl ResolveOutcome {
    fn into_ready(self) -> Option<ResolvedTool> {
        match self {
            Self::Ready(resolved) => Some(resolved),
            Self::Retry | Self::Missing => None,
        }
    }
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
    async fn get_tool(&self, tool_name: &str) -> ResolveOutcome {
        let tools = self.tools.read().await;
        if let Some((catalog_name, name)) = decode_tool_name(tool_name)
            && let Some(Tooling::Catalog { tools: catalog, .. }) = tools.get(&catalog_name)
        {
            let exposed_name = encode_tool_name(&catalog_name, &name);
            // Capture *before* any `get` / `remember_tool`. The transport
            // validated `Mcp-Param-*` against this snapshot generation
            // (or skipped the check when it was empty). Executing a tool
            // discovered after that sample is `validated_schema=None
            // executed=True`.
            let validated = self.snapshot_tool(&exposed_name).is_some()
                || self.snapshot_tool(tool_name).is_some();

            // Prefer `try_get` — the listed spec `ServerHandler::get_tool`
            // used for `Mcp-Param-*`. `get` may refresh an expired TTL
            // (`validated=Region executed=Zone`).
            if let Some(tool) = catalog.try_get(&name) {
                return self.resolved_if_validated(
                    validated,
                    tool,
                    exposed_name,
                    Some(catalog_name),
                );
            }

            // Expired `try_get`: `get` returns the listed spec the
            // snapshot already described. A name that was never in the
            // snapshot is a first-seen discovery — publish and retry.
            if let Some(tool) = catalog.get(&name).await {
                return self.resolved_if_validated(
                    validated,
                    tool,
                    exposed_name,
                    Some(catalog_name),
                );
            }
            return ResolveOutcome::Missing;
        }
        // Fall back to a direct (non-catalog) lookup. This covers top-level
        // tools whose names legitimately contain the `__` catalog separator.
        // Such a tool is exposed under its own name, so that name is already
        // canonical and must not be re-encoded — and it belongs to no catalog,
        // however much its name may look like one qualified by the separator.
        match tools.get(tool_name) {
            Some(Tooling::Tool(tool) | Tooling::FunctionTool(tool)) => {
                ResolveOutcome::Ready(ResolvedTool {
                    tool: Arc::clone(tool),
                    exposed_name: tool_name.to_string(),
                    catalog: None,
                })
            }
            Some(Tooling::Catalog { .. }) | None => ResolveOutcome::Missing,
        }
    }

    fn resolved_if_validated(
        &self,
        validated: bool,
        tool: Arc<dyn SpiceModelTool>,
        exposed_name: String,
        catalog: Option<String>,
    ) -> ResolveOutcome {
        if validated {
            return ResolveOutcome::Ready(ResolvedTool {
                tool,
                exposed_name,
                catalog,
            });
        }
        self.remember_tool(mcp_tool_from_spice(exposed_name, tool.as_ref()));
        ResolveOutcome::Retry
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
        // `decode_tool_name` accepts `srv__tool__name` as an alias of
        // canonical `srv__tool_-_name`. Prefer the canonical snapshot
        // key so an incomplete catalog refresh cannot leave the alias
        // on `Region` while dispatch runs `Zone`.
        if let Some((catalog, tool)) = decode_tool_name(name) {
            let canonical = encode_tool_name(&catalog, &tool);
            if let Some(schema) = self.schemas.get(&canonical) {
                return Some(schema);
            }
        }
        self.schemas.get(name)
    }

    fn remember_tool(&self, tool: Tool) {
        let added = self.schemas.insert(tool.name.to_string(), tool);
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
        self.remember_tool(tool.clone());
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

            let resolved = match self.get_tool(tool_name.as_ref()).await {
                ResolveOutcome::Ready(resolved) => resolved,
                ResolveOutcome::Retry => {
                    return Err(McpError::invalid_params(
                        format!(
                            "Tool '{tool_name}' was not in the schema used to validate this request, so it was not executed. Retry the call so `Mcp-Param-*` headers can be checked."
                        ),
                        None,
                    ));
                }
                ResolveOutcome::Missing => {
                    return Err(McpError::method_not_found::<
                        rmcp::model::CallToolRequestMethod,
                    >());
                }
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
                    .call_tool_once(request)
                    .instrument(span.clone())
                    .await
                {
                    Ok(response) => {
                        if let CallToolResponse::Complete(result) = &response
                            && let Ok(captured_output) = serde_json::to_string(&result.content)
                        {
                            tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                        }
                        Ok(response)
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
            // Warm catalog caches so `try_all` is populated, then
            // snapshot under the publish lock from the live map. A
            // page collected here and installed later can be older
            // than a concurrent TTL/reconnect publish.
            let _warm = self.all_tools().await;
            let map = self.tools.read().await;
            let (tools, changed) = self.schemas.replace_listed_from_map(&map);
            self.schemas.bump_if(changed);
            Ok(listed_tools_result(tools))
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
///
/// Two passes so a catalog tool deterministically overwrites a
/// top-level name that collides with `encode_tool_name` — the same
/// preference as gateway `tools/call` dispatch. A single `HashMap`
/// walk would let iteration order pick the schema `tools/list` and
/// `Mcp-Param-*` validate against.
#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn mcp_direct_schemas_from_map(tools: &HashMap<String, Tooling>) -> HashMap<String, Tool> {
    let mut schemas = HashMap::new();
    for (name, tooling) in tools {
        match tooling {
            Tooling::Tool(tool) | Tooling::FunctionTool(tool) => {
                schemas.insert(
                    name.clone(),
                    mcp_tool_from_spice(name.clone(), tool.as_ref()),
                );
            }
            Tooling::Catalog { .. } => {}
        }
    }
    schemas
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn mcp_schemas_from_map(tools: &HashMap<String, Tooling>) -> HashMap<String, Tool> {
    let mut schemas = mcp_direct_schemas_from_map(tools);
    for tooling in tools.values() {
        if let Tooling::Catalog { tools: catalog, .. } = tooling {
            let catalog_name = catalog.name();
            for tool in catalog.try_all() {
                let exposed = encode_tool_name(catalog_name, &tool.name());
                schemas.insert(exposed.clone(), mcp_tool_from_spice(exposed, tool.as_ref()));
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

/// `2026-07-28` requires `ttlMs` and `cacheScope` on `tools/list`.
///
/// [`ListToolsResult::default`] leaves both `None`, which rmcp 3.3.0
/// omits on the wire. Omitted or zero `ttlMs` is immediately stale
/// (SEP-2549); emitting `0` + `private` is an explicit non-cacheable
/// private result instead of a legacy-compatible omission.
///
/// The spec also requires a deterministic tool order when the set is
/// unchanged so clients can keep a stable prompt-cache prefix. Collecting
/// a `HashMap` via `values()` is not that order.
fn listed_tools_result(tools: Vec<Tool>) -> ListToolsResult {
    ListToolsResult {
        tools: tools_listed_by_name_vec(tools),
        ..ListToolsResult::default()
    }
    .with_ttl_ms(0)
    .with_cache_scope(CacheScope::Private)
}

/// Sort listed tools by name so repeated `tools/list` pages match.
fn tools_listed_by_name(schemas: &HashMap<String, Tool>) -> Vec<Tool> {
    tools_listed_by_name_vec(schemas.values().cloned().collect())
}

fn tools_listed_by_name_vec(mut tools: Vec<Tool>) -> Vec<Tool> {
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    tools
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SpiceToolCatalog;
    use rmcp::model::InputRequiredResult;
    use rmcp::service::ServiceError;
    use spicepod::component::runtime::CorsConfig;
    use tools::McpProxy;

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
            Ok(json!({ "executed": "Zone" }))
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
            Ok(json!({ "executed": "Region" }))
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
                .into_ready()
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
            .into_ready()
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

    /// `replace_from_map` used to compute `next` (Region) and then take
    /// the snapshot lock, so a concurrent catalog publish of Zone was
    /// overwritten and the epoch still advanced (`final_schema=Region
    /// epoch=2`). Computing under the publish lock lets the catalog
    /// write land last.
    #[test]
    fn replace_from_map_does_not_overwrite_a_newer_catalog_publish() {
        let snapshot = Arc::new(McpSchemaSnapshot::default());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let catalog = Arc::new(GatedTryAllCatalog {
            started: std::sync::Mutex::new(Some(started_tx)),
            release: std::sync::Mutex::new(Some(release_rx)),
        });
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::clone(&catalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );

        let replace_snapshot = Arc::clone(&snapshot);
        let replacer = std::thread::spawn(move || {
            replace_snapshot.replace_from_map(&tools);
        });

        started_rx
            .recv()
            .expect("replace_from_map must enter try_all before the catalog publishes");

        let publish_snapshot = Arc::clone(&snapshot);
        let publisher = std::thread::spawn(move || {
            let mut listed = HashMap::new();
            listed.insert(
                "deploy".to_string(),
                mcp_tool_from_spice("deploy", &ZoneAnnotatedTool),
            );
            let changed = apply_listed_catalog_cache(&publish_snapshot, "srv", &listed, true);
            publish_snapshot.bump_if(changed);
        });

        // Give the publisher time to block on the publish lock (or, on
        // the unsynchronized path, to write Zone before we release).
        std::thread::sleep(std::time::Duration::from_millis(50));
        let exposed = encode_tool_name("srv", "deploy");
        assert_ne!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "catalog publish must wait for replace_from_map; Zone already present means a stale overwrite can follow"
        );

        release_tx
            .send(())
            .expect("replace_from_map is waiting in try_all");
        replacer
            .join()
            .expect("replace_from_map thread should finish");
        publisher
            .join()
            .expect("catalog publish thread should finish");

        assert_eq!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "final_schema must be Zone after the catalog publish; Region after Zone is the stale overwrite"
        );
        assert!(
            snapshot.epoch() >= 2,
            "both the full replace and the catalog publish bump the epoch"
        );
    }

    /// `replace_listed` used to install a precomputed Region page
    /// without [`McpSchemaSnapshot::publish`], so a concurrent catalog
    /// publish of Zone was overwritten (`final_schema=Region epoch=2`).
    /// Production `tools/list` recomputes from the live map under that
    /// lock.
    #[test]
    fn replace_listed_from_map_does_not_overwrite_a_newer_catalog_publish() {
        let snapshot = Arc::new(McpSchemaSnapshot::default());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let catalog = Arc::new(GatedTryAllCatalog {
            started: std::sync::Mutex::new(Some(started_tx)),
            release: std::sync::Mutex::new(Some(release_rx)),
        });
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::clone(&catalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );

        let replace_snapshot = Arc::clone(&snapshot);
        let replacer = std::thread::spawn(move || {
            let (_listed, changed) = replace_snapshot.replace_listed_from_map(&tools);
            replace_snapshot.bump_if(changed);
        });

        started_rx
            .recv()
            .expect("replace_listed_from_map must enter try_all before the catalog publishes");

        let publish_snapshot = Arc::clone(&snapshot);
        let publisher = std::thread::spawn(move || {
            let mut listed = HashMap::new();
            listed.insert(
                "deploy".to_string(),
                mcp_tool_from_spice("deploy", &ZoneAnnotatedTool),
            );
            let changed = apply_listed_catalog_cache(&publish_snapshot, "srv", &listed, true);
            publish_snapshot.bump_if(changed);
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        let exposed = encode_tool_name("srv", "deploy");
        assert_ne!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "catalog publish must wait for replace_listed_from_map; Zone already present means a stale overwrite can follow"
        );

        release_tx
            .send(())
            .expect("replace_listed_from_map is waiting in try_all");
        replacer
            .join()
            .expect("replace_listed_from_map thread should finish");
        publisher
            .join()
            .expect("catalog publish thread should finish");

        assert_eq!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "final_schema must be Zone after the catalog publish; Region after Zone is the stale overwrite"
        );
        assert!(
            snapshot.epoch() >= 2,
            "both the listed replace and the catalog publish bump the epoch"
        );
    }

    /// `merge_from_map` used to compute `next` (Region) outside
    /// [`McpSchemaSnapshot::publish`], so a concurrent catalog publish
    /// of Zone was overwritten (`newer_catalog_publish=Zone
    /// final_schema=Region stale_overwrite=True`). Collecting under
    /// that lock lets the catalog write land last.
    #[test]
    fn merge_from_map_does_not_overwrite_a_newer_catalog_publish() {
        let snapshot = Arc::new(McpSchemaSnapshot::default());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let catalog = Arc::new(GatedTryAllCatalog {
            started: std::sync::Mutex::new(Some(started_tx)),
            release: std::sync::Mutex::new(Some(release_rx)),
        });
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::clone(&catalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );

        let merge_snapshot = Arc::clone(&snapshot);
        let merger = std::thread::spawn(move || {
            let changed = merge_snapshot.merge_from_map(&tools);
            merge_snapshot.bump_if(changed);
        });

        started_rx
            .recv()
            .expect("merge_from_map must enter try_all before the catalog publishes");

        let publish_snapshot = Arc::clone(&snapshot);
        let publisher = std::thread::spawn(move || {
            let mut listed = HashMap::new();
            listed.insert(
                "deploy".to_string(),
                mcp_tool_from_spice("deploy", &ZoneAnnotatedTool),
            );
            let changed = apply_listed_catalog_cache(&publish_snapshot, "srv", &listed, true);
            publish_snapshot.bump_if(changed);
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        let exposed = encode_tool_name("srv", "deploy");
        assert_ne!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "catalog publish must wait for merge_from_map; Zone already present means a stale overwrite can follow"
        );

        release_tx
            .send(())
            .expect("merge_from_map is waiting in try_all");
        merger.join().expect("merge_from_map thread should finish");
        publisher
            .join()
            .expect("catalog publish thread should finish");

        assert_eq!(
            snapshot
                .get(&exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "final_schema must be Zone after the catalog publish; Region after Zone is the stale overwrite"
        );
    }

    #[test]
    fn listed_tools_result_emits_explicit_non_cacheable_private_hints() {
        let default_json = serde_json::to_value(ListToolsResult::default())
            .expect("ListToolsResult::default should serialize");
        assert!(
            default_json.get("ttlMs").is_none() || default_json.get("ttlMs") == Some(&Value::Null),
            "rmcp 3.3.0 default omits ttlMs; that omission is the 2026 wire bug: {default_json}"
        );
        assert!(
            default_json.get("cacheScope").is_none()
                || default_json.get("cacheScope") == Some(&Value::Null),
            "rmcp 3.3.0 default omits cacheScope; that omission is the 2026 wire bug: {default_json}"
        );

        let result = listed_tools_result(Vec::new());
        assert_eq!(result.ttl_ms, Some(0), "zero ttlMs is immediately stale");
        assert_eq!(
            result.cache_scope,
            Some(CacheScope::Private),
            "gateway tools/list is explicitly private, not an omitted public default"
        );
        let json = serde_json::to_value(&result).expect("listed tools result should serialize");
        assert_eq!(
            json.get("ttlMs"),
            Some(&json!(0)),
            "2026-07-28 tools/list must emit ttlMs, got {json}"
        );
        assert_eq!(
            json.get("cacheScope"),
            Some(&json!("private")),
            "2026-07-28 tools/list must emit cacheScope, got {json}"
        );
    }

    #[test]
    fn listed_tools_result_orders_tools_by_name() {
        let tools = ["search", "sql", "memory", "web", "get_readiness"]
            .into_iter()
            .map(|name| mcp_tool_from_spice(name, &StubTool(name)))
            .collect();
        let result = listed_tools_result(tools);
        let names: Vec<&str> = result.tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(
            names,
            ["get_readiness", "memory", "search", "sql", "web"],
            "2026-07-28 tools/list must be name-sorted for prompt-cache stability"
        );
    }

    /// A fresh `HashMap` collected via `values()` is the production page
    /// before `tools_listed_by_name`. A rustc harness of that pattern
    /// printed `distinct_orders=78` over 128 runs.
    #[test]
    fn hashmap_values_list_order_varies_until_sorted() {
        let labels = ["search", "sql", "memory", "web", "get_readiness"];
        let mut unsorted = HashSet::new();
        let mut sorted = HashSet::new();
        for _ in 0..128 {
            let mut next = HashMap::new();
            for name in labels {
                next.insert(name.to_string(), mcp_tool_from_spice(name, &StubTool(name)));
            }
            let listed: Vec<Tool> = next.values().cloned().collect();
            unsorted.insert(
                listed
                    .iter()
                    .map(|tool| tool.name.to_string())
                    .collect::<Vec<_>>(),
            );
            let names: Vec<String> = listed_tools_result(listed)
                .tools
                .iter()
                .map(|tool| tool.name.to_string())
                .collect();
            sorted.insert(names);
        }
        assert!(
            unsorted.len() > 1,
            "distinct_orders={} — HashMap values() must vary to witness the 2026 list shuffle",
            unsorted.len()
        );
        assert_eq!(
            sorted.len(),
            1,
            "listed_tools_result must collapse HashMap iteration to one order, got {sorted:?}"
        );
        let only = sorted
            .iter()
            .next()
            .expect("listed_tools_result must produce one sorted order");
        assert_eq!(
            only.as_slice(),
            ["get_readiness", "memory", "search", "sql", "web"],
            "expected_order=get_readiness,memory,search,sql,web"
        );
    }

    #[test]
    fn replace_listed_from_map_returns_name_sorted_tools_list() {
        let mut tools = HashMap::new();
        for name in ["search", "sql", "memory", "web", "get_readiness"] {
            tools.insert(
                name.to_string(),
                Tooling::Tool(Arc::new(StubTool(name)) as Arc<dyn SpiceModelTool>),
            );
        }
        let snapshot = McpSchemaSnapshot::default();
        let (listed, _) = snapshot.replace_listed_from_map(&tools);
        let names: Vec<&str> = listed.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(
            names,
            ["get_readiness", "memory", "search", "sql", "web"],
            "replace_listed_from_map must not return HashMap values() order"
        );
        let result = listed_tools_result(listed);
        let result_names: Vec<&str> = result.tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(
            result_names,
            ["get_readiness", "memory", "search", "sql", "web"]
        );
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

    /// A top-level tool named `srv__deploy` collides with catalog `srv`
    /// tool `deploy`. Dispatch prefers the catalog; the snapshot must
    /// too. A single-pass `HashMap` walk let iteration order pick
    /// `top_schema_wins` (~half the time in Copilot's harness).
    #[test]
    fn mcp_schemas_from_map_lets_catalog_overwrite_colliding_top_level() {
        struct ZoneCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for ZoneCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                self.try_all()
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                self.try_get(name)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
        }

        let exposed = encode_tool_name("srv", "deploy");
        let mut tools = HashMap::new();
        tools.insert(
            exposed.clone(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(ZoneCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );

        let schemas = mcp_schemas_from_map(&tools);
        assert_eq!(
            schemas.get(&exposed).and_then(x_mcp_header_region),
            Some("Zone"),
            "top_schema_wins leaves Region; catalogs must overwrite to match get_tool"
        );
        assert_eq!(
            RuntimeServer::definition_from_map(&tools, &exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "tools/call dispatch prefers the catalog; the snapshot must describe the same tool"
        );
    }

    /// A complete catalog list that drops `deploy` used to delete
    /// `srv__deploy` even when a live top-level tool still owns that
    /// name. Dispatch falls back to `top:Region`; `get_tool` under
    /// tools-map contention then returned `None` and rmcp cached the
    /// miss (`validated_schema=None mismatch=True`).
    #[test]
    fn complete_catalog_refresh_restores_colliding_top_level_schema() {
        struct ZoneCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for ZoneCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                self.try_all()
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                self.try_get(name)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
        }

        struct EmptyCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for EmptyCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                Vec::new()
            }
            async fn get(&self, _name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                None
            }
            fn try_get(&self, _name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                None
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                Vec::new()
            }
        }

        let exposed = encode_tool_name("srv", "deploy");
        let mut tools = HashMap::new();
        tools.insert(
            exposed.clone(),
            Tooling::Tool(Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>),
        );
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(ZoneCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );

        let tools = Arc::new(RwLock::new(tools));
        let server = RuntimeServer::new(Arc::clone(&tools));
        assert_eq!(
            ServerHandler::get_tool(&server, &exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "catalog must overwrite the colliding top-level schema while it advertises deploy"
        );

        let changed = apply_listed_catalog_cache(&server.schemas, "srv", &HashMap::new(), true);
        server.schemas.bump_if(changed);
        {
            let mut live = tools.blocking_write();
            live.insert(
                "srv".to_string(),
                Tooling::Catalog {
                    tools: Arc::new(EmptyCatalog) as Arc<dyn SpiceToolCatalog>,
                    default_catalog_names: vec![],
                },
            );
        }

        assert_eq!(
            RuntimeServer::definition_from_map(&tools.blocking_read(), &exposed)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Region"),
            "dispatched=top:Region after the catalog drops deploy"
        );

        let _write = tools.blocking_write();
        let validated = ServerHandler::get_tool(&server, &exposed);
        assert_eq!(
            validated.as_ref().and_then(x_mcp_header_region),
            Some("Region"),
            "dispatched=top:Region validated_schema=None mismatch=True is the reported collision delete"
        );
    }

    #[test]
    fn complete_catalog_refresh_drops_schema_when_no_colliding_top_level() {
        struct ZoneCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for ZoneCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                self.try_all()
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                self.try_get(name)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
        }

        let exposed = encode_tool_name("srv", "deploy");
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(ZoneCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        let snapshot = McpSchemaSnapshot::default();
        assert!(snapshot.merge_from_map(&tools));
        snapshot.bump_if(true);
        assert!(
            snapshot.get(&exposed).is_some(),
            "catalog deploy must land in the snapshot"
        );

        let changed = apply_listed_catalog_cache(&snapshot, "srv", &HashMap::new(), true);
        snapshot.bump_if(changed);
        assert!(
            snapshot.get(&exposed).is_none(),
            "a complete catalog drop with no colliding top-level tool must remove the schema"
        );
    }

    /// `decode_tool_name("srv__tool__name")` is `("srv", "tool__name")`,
    /// whose canonical encoding is `srv__tool_-_name`. Caching the
    /// alias leaves a second snapshot entry; an incomplete catalog
    /// refresh updates only the canonical key
    /// (`canonical_schema=Zone alias_schema=Region stale_alias=True`).
    #[test]
    fn get_tool_uses_canonical_schema_not_stale_alias() {
        struct UnderscoreNamedTool(&'static str);

        #[async_trait::async_trait]
        impl SpiceModelTool for UnderscoreNamedTool {
            fn name(&self) -> Cow<'_, str> {
                Cow::Borrowed("tool__name")
            }
            fn description(&self) -> Option<Cow<'_, str>> {
                None
            }
            fn parameters(&self) -> Option<Value> {
                Some(json!({
                    "type": "object",
                    "properties": {
                        "region": { "type": "string", "x-mcp-header": self.0 }
                    }
                }))
            }
            async fn call(
                &self,
                _arg: &str,
            ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
                Ok(Value::Null)
            }
        }

        struct UnderscoreCatalog(&'static str);

        #[async_trait::async_trait]
        impl SpiceToolCatalog for UnderscoreCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                self.try_all()
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                self.try_get(name)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "tool__name")
                    .then(|| Arc::new(UnderscoreNamedTool(self.0)) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(UnderscoreNamedTool(self.0)) as Arc<dyn SpiceModelTool>]
            }
        }

        let canonical = encode_tool_name("srv", "tool__name");
        let alias = "srv__tool__name";
        assert_ne!(
            canonical.as_str(),
            alias,
            "the lax alias must differ from the encoded name"
        );
        assert_eq!(
            decode_tool_name(alias),
            Some(("srv".to_string(), "tool__name".to_string()))
        );

        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(UnderscoreCatalog("Zone")) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        let tools = Arc::new(RwLock::new(tools));
        let server = RuntimeServer::new(Arc::clone(&tools));

        let from_alias = ServerHandler::get_tool(&server, alias)
            .expect("alias must resolve through the catalog");
        assert_eq!(from_alias.name.as_ref(), canonical.as_str());
        assert!(
            server.schemas.get(alias).is_none(),
            "remember_tool must not store the caller alias"
        );
        assert_eq!(
            server
                .schemas
                .get(&canonical)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone")
        );

        server.schemas.insert(
            alias.to_string(),
            mcp_tool_from_spice(alias, &HeaderAnnotatedTool),
        );
        let mut listed = HashMap::new();
        listed.insert(
            "tool__name".to_string(),
            mcp_tool_from_spice("tool__name", &UnderscoreNamedTool("Zone")),
        );
        let changed = apply_listed_catalog_cache(&server.schemas, "srv", &listed, false);
        server.schemas.bump_if(changed);

        assert_eq!(
            server
                .schemas
                .get(&canonical)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "canonical_schema=Zone after the incomplete refresh"
        );
        assert_eq!(
            server
                .schemas
                .get(alias)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Region"),
            "alias_schema=Region is the leftover key an incomplete refresh does not touch"
        );
        assert_eq!(
            ServerHandler::get_tool(&server, alias)
                .as_ref()
                .and_then(x_mcp_header_region),
            Some("Zone"),
            "canonical_schema=Zone alias_schema=Region stale_alias=True is the reported miss"
        );
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

    /// `try_all` blocks until the test releases it so
    /// [`McpSchemaSnapshot::replace_from_map`] can be held on the
    /// publish lock while a catalog publish is spawned.
    struct GatedTryAllCatalog {
        started: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl SpiceToolCatalog for GatedTryAllCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &'static str {
            "srv"
        }
        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            self.try_all()
        }
        async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            self.try_get(name)
        }
        fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
        }
        fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            if let Some(started) = self
                .started
                .lock()
                .expect("gated catalog start lock")
                .take()
            {
                started
                    .send(())
                    .expect("replace_from_map test is waiting for try_all");
            }
            if let Some(release) = self
                .release
                .lock()
                .expect("gated catalog release lock")
                .take()
            {
                release
                    .recv()
                    .expect("test must release try_all after spawning the catalog publish");
            }
            vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
        }
    }

    struct SwappableHeaderCatalog {
        use_zone: std::sync::atomic::AtomicBool,
    }

    impl SwappableHeaderCatalog {
        fn new() -> Self {
            Self {
                use_zone: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn use_zone(&self) {
            self.use_zone
                .store(true, std::sync::atomic::Ordering::Release);
        }

        fn current_tool(&self) -> Arc<dyn SpiceModelTool> {
            if self.use_zone.load(std::sync::atomic::Ordering::Acquire) {
                Arc::new(ZoneAnnotatedTool)
            } else {
                Arc::new(HeaderAnnotatedTool)
            }
        }
    }

    #[async_trait::async_trait]
    impl SpiceToolCatalog for SwappableHeaderCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &'static str {
            "srv"
        }
        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![self.current_tool()]
        }
        async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == "deploy").then(|| self.current_tool())
        }
        fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            (name == "deploy").then(|| self.current_tool())
        }
        fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![self.current_tool()]
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

    struct InputRequiredProxyTool;

    #[async_trait::async_trait]
    impl SpiceModelTool for InputRequiredProxyTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("ask")
        }
        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("asks for more input"))
        }
        fn parameters(&self) -> Option<Value> {
            Some(json!({ "type": "object", "properties": {} }))
        }
        async fn as_mcp_proxy(&self) -> Option<&dyn McpProxy> {
            Some(self)
        }
        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(json!({ "ok": true }))
        }
    }

    #[async_trait::async_trait]
    impl McpProxy for InputRequiredProxyTool {
        async fn call_tool(
            &self,
            _arguments: Option<rmcp::model::JsonObject>,
        ) -> Result<CallToolResult, ServiceError> {
            Ok(CallToolResult::success(vec![ContentBlock::text("ok")]))
        }

        async fn call_tool_once(
            &self,
            _request: CallToolRequestParams,
        ) -> Result<CallToolResponse, ServiceError> {
            Ok(CallToolResponse::InputRequired(
                InputRequiredResult::from_request_state("opaque-server-state"),
            ))
        }
    }

    /// `CallToolResult.into()` is always `Complete`. A proxied `input_required`
    /// must reach the Streamable HTTP client so MRTR can continue.
    #[tokio::test]
    async fn proxied_input_required_is_relayed_not_forced_complete() {
        let forced_complete: CallToolResponse =
            CallToolResult::success(vec![ContentBlock::text("ok")]).into();
        assert!(
            matches!(forced_complete, CallToolResponse::Complete(_)),
            "CallToolResult conversion is what used to drop input_required"
        );

        let mut tools = HashMap::new();
        tools.insert(
            "ask".to_string(),
            Tooling::Tool(Arc::new(InputRequiredProxyTool) as Arc<dyn SpiceModelTool>),
        );
        let server = RuntimeServer::new(Arc::new(RwLock::new(tools)));
        let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
            move || Ok(server.clone()),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .disable_allowed_hosts()
                .with_json_response(true),
        );

        let (status, json) = post_tools_call(&service, "ask", None, "unused").await;
        assert!(
            status.is_success(),
            "input_required is a successful tools/call result, got {status}: {json}"
        );
        assert_eq!(
            json.pointer("/result/resultType").and_then(Value::as_str),
            Some("input_required"),
            "proxied input_required must be relayed, not forced to Complete: {status} {json}"
        );
        assert_eq!(
            json.pointer("/result/requestState").and_then(Value::as_str),
            Some("opaque-server-state"),
            "MRTR requestState must survive the gateway: {json}"
        );
        assert!(
            json.pointer("/result/content").is_none(),
            "Complete would carry content; input_required must not: {json}"
        );
    }

    /// A proxied catalog TTL refresh used to update only the catalog
    /// cache. Streamable HTTP kept validating `mcp-param-region` until a
    /// gateway `tools/list`. Publishing the cache into the snapshot and
    /// rebuilding the service validates `mcp-param-zone` without that list.
    #[tokio::test]
    async fn rebuilt_service_validates_after_catalog_cache_schema_identity_change() {
        let exposed = encode_tool_name("srv", "deploy");
        let catalog = Arc::new(SwappableHeaderCatalog::new());
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::clone(&catalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
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
            post_tools_call(&service, &exposed, Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "matching Mcp-Param-Region must be accepted before the catalog rewrite: {status} {json}"
        );

        catalog.use_zone();
        let mut listed = HashMap::new();
        listed.insert(
            "deploy".to_string(),
            mcp_tool_from_spice("deploy", &ZoneAnnotatedTool),
        );
        let changed = apply_listed_catalog_cache(&schemas, "srv", &listed, true);
        schemas.bump_if(changed);
        assert!(
            changed,
            "Region → Zone on the catalog tool must count as a schema change"
        );
        assert!(
            schemas.epoch() > first_epoch,
            "catalog cache identity change must bump the snapshot epoch without a gateway tools/list"
        );

        let (status, json) =
            post_tools_call_param(&service, &exposed, "zone", Some("us-west1"), "us-west1").await;
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
            post_tools_call_param(&rebuilt, &exposed, "zone", Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "rebuilt service must validate mcp-param-zone after the catalog cache rewrite: {status} {json}"
        );
        assert!(
            json.get("error")
                .and_then(|error| error.get("code"))
                .and_then(Value::as_i64)
                != Some(-32020),
            "matching Zone header must not raise HeaderMismatch: {json}"
        );
    }

    /// `try_get` keeps the expired Region schema; `get` refreshes to
    /// Zone. rmcp validates via `try_get`/snapshot, then dispatch used
    /// to call `get` (`validated=Region executed=Zone accepted=True`).
    /// A catalog `get()` discovery after `try_get` / snapshot miss used
    /// to execute in the same request (`validated_schema=None
    /// executed=Region`). Publish the schema and require retry instead.
    /// regression test for #14043
    #[tokio::test]
    async fn cache_miss_must_not_dispatch_unvalidated_tool() {
        struct CacheMissUntilGetCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for CacheMissUntilGetCatalog {
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
            fn try_get(&self, _name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                None
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                Vec::new()
            }
        }

        let exposed = encode_tool_name("srv", "deploy");
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(CacheMissUntilGetCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        let tools = Arc::new(RwLock::new(tools));
        let schemas = McpSchemaSnapshot::new();
        let first_epoch = schemas.epoch();
        let server = RuntimeServer::with_schema_snapshot(Arc::clone(&tools), Arc::clone(&schemas));
        assert!(
            ServerHandler::get_tool(&server, &exposed).is_none(),
            "first-call snapshot must have no schema so transport validation is skipped"
        );

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
            post_tools_call(&service, &exposed, Some("us-east-1"), "us-east-1").await;
        assert_eq!(
            json.pointer("/error/code").and_then(Value::as_i64),
            Some(-32602),
            "unvalidated discovery must reject instead of execute: {status} {json}"
        );
        let message = json
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            message.contains("retry") && message.contains(&format!("'{exposed}'")),
            "retry error must name the tool and tell the client to retry: {message}"
        );
        assert!(
            json.pointer("/result").is_none(),
            "first-call discovery must not return a tools/call result: {json}"
        );
        assert!(
            schemas.get(&exposed).is_some(),
            "discovery must publish the refreshed schema so the next request can validate"
        );
        assert!(
            schemas.epoch() > first_epoch,
            "publish must bump the epoch so Streamable HTTP drops the cached None"
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
        let rebuilt_server =
            RuntimeServer::with_schema_snapshot(Arc::clone(&tools), Arc::clone(&schemas));
        assert!(
            ServerHandler::get_tool(&rebuilt_server, &exposed).is_some(),
            "rebuilt transport must see the published schema"
        );

        let (status, json) =
            post_tools_call(&rebuilt, &exposed, Some("us-east-1"), "us-east-1").await;
        assert!(
            status.is_success(),
            "retry after publish must validate and execute: {status} {json}"
        );
        let executed = json
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .and_then(|body| {
                body.get("executed")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            });
        assert_eq!(
            executed.as_deref(),
            Some("Region"),
            "retry after publish must execute the discovered tool: {json}"
        );
    }

    #[tokio::test]
    async fn expired_try_get_schema_must_not_dispatch_refreshed_tool() {
        struct ExpiredThenRefreshCatalog;

        #[async_trait::async_trait]
        impl SpiceToolCatalog for ExpiredThenRefreshCatalog {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &'static str {
                "srv"
            }
            async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
            async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(ZoneAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
                (name == "deploy").then(|| Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>)
            }
            fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
                vec![Arc::new(HeaderAnnotatedTool) as Arc<dyn SpiceModelTool>]
            }
        }

        let exposed = encode_tool_name("srv", "deploy");
        let mut tools = HashMap::new();
        tools.insert(
            "srv".to_string(),
            Tooling::Catalog {
                tools: Arc::new(ExpiredThenRefreshCatalog) as Arc<dyn SpiceToolCatalog>,
                default_catalog_names: vec![],
            },
        );
        let tools = Arc::new(RwLock::new(tools));
        let schemas = McpSchemaSnapshot::new();
        schemas.replace_from_map(&*tools.read().await);

        let validated = ServerHandler::get_tool(
            &RuntimeServer::with_schema_snapshot(Arc::clone(&tools), Arc::clone(&schemas)),
            &exposed,
        );
        assert_eq!(
            validated.as_ref().and_then(x_mcp_header_region),
            Some("Region"),
            "validated=Region from the expired try_get/snapshot"
        );

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
            post_tools_call(&service, &exposed, Some("us-west1"), "us-west1").await;
        assert!(
            status.is_success(),
            "Region header matches the validated schema: {status} {json}"
        );
        let executed = json
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .and_then(|body| {
                body.get("executed")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            });
        assert_eq!(
            executed.as_deref(),
            Some("Region"),
            "validated=Region executed=Zone accepted=True is the reported miss: {json}"
        );
    }

    async fn post_tools_call_with_origin<S>(
        service: &rmcp::transport::streamable_http_server::StreamableHttpService<
            S,
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
        >,
        origin: Option<&str>,
    ) -> http::StatusCode
    where
        S: ServerHandler,
    {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "deploy",
                "arguments": { "region": "us-west1" },
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
            .header("mcp-name", "deploy");
        if let Some(origin) = origin {
            builder = builder.header(http::header::ORIGIN, origin);
        }
        let request = builder
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .expect("valid tools/call request");
        service.handle(request).await.status()
    }

    fn origin_service(
        config: rmcp::transport::streamable_http_server::StreamableHttpServerConfig,
    ) -> rmcp::transport::streamable_http_server::StreamableHttpService<
        SchemaLessServer,
        rmcp::transport::streamable_http_server::session::local::LocalSessionManager,
    > {
        rmcp::transport::streamable_http_server::StreamableHttpService::new(
            || Ok(SchemaLessServer),
            Arc::new(
                rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
            ),
            config,
        )
    }

    /// rmcp's empty `allowed_origins` accepts every `Origin`. That is the
    /// pre-fix gap this PR closes for Spice's default CORS `"*"`.
    #[tokio::test]
    async fn unconfigured_origin_policy_accepts_disallowed_origin() {
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true);
        assert!(
            config.allowed_origins.is_empty(),
            "rmcp default allowed_origins must be empty: {:?}",
            config.allowed_origins
        );
        let status =
            post_tools_call_with_origin(&origin_service(config), Some("https://evil.example"))
                .await;
        assert_ne!(
            status,
            http::StatusCode::FORBIDDEN,
            "empty allowed_origins is the pre-fix reproduction: Origin https://evil.example was accepted, got {status}"
        );
        eprintln!("unconfigured_origin_policy_accepts_disallowed_origin status={status}");
    }

    #[tokio::test]
    async fn allowed_origins_rejects_disallowed_origin_post() {
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true)
            .with_allowed_origins(["https://app.example.com"]);
        let status =
            post_tools_call_with_origin(&origin_service(config), Some("https://evil.example"))
                .await;
        assert_eq!(
            status,
            http::StatusCode::FORBIDDEN,
            "Origin https://evil.example must be 403 when allowed_origins is https://app.example.com, got {status}"
        );
    }

    #[tokio::test]
    async fn allowed_origins_accepts_matching_origin_post() {
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true)
            .with_allowed_origins(["https://app.example.com"]);
        let status =
            post_tools_call_with_origin(&origin_service(config), Some("https://app.example.com"))
                .await;
        assert_ne!(
            status,
            http::StatusCode::FORBIDDEN,
            "matching Origin https://app.example.com must not be 403, got {status}"
        );
    }

    #[tokio::test]
    async fn allowed_origins_accepts_missing_origin_post() {
        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true)
            .with_allowed_origins(["https://app.example.com"]);
        let status = post_tools_call_with_origin(&origin_service(config), None).await;
        assert_ne!(
            status,
            http::StatusCode::FORBIDDEN,
            "non-browser clients that omit Origin must still pass, got {status}"
        );
    }

    /// Default `runtime.cors.allowed_origins: ["*"]` expands to localhost
    /// so `https://evil.example` is 403.
    #[tokio::test]
    async fn spice_default_cors_rejects_disallowed_origin_post() {
        let rmcp_default =
            rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default();
        let origins = CorsConfig::default().mcp_allowed_origins();
        eprintln!(
            "rmcp_default_allowed_origins_empty={}",
            rmcp_default.allowed_origins.is_empty()
        );
        eprintln!(
            "spice_mcp_config_sets_allowed_origins={}",
            !origins.is_empty()
        );
        assert!(
            rmcp_default.allowed_origins.is_empty(),
            "rmcp default allowed_origins must be empty: {:?}",
            rmcp_default.allowed_origins
        );
        assert!(
            !origins.is_empty(),
            "default CORS * must expand to a non-empty MCP Origin list, got {origins:?}"
        );

        let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
            .with_legacy_session_mode(true)
            .disable_allowed_hosts()
            .with_json_response(true)
            .with_allowed_origins(origins);
        let status =
            post_tools_call_with_origin(&origin_service(config), Some("https://evil.example"))
                .await;
        eprintln!(
            "origin_https_evil_example_accepted={}",
            status != http::StatusCode::FORBIDDEN
        );
        assert_eq!(
            status,
            http::StatusCode::FORBIDDEN,
            "default CORS * must 403 Origin https://evil.example, got {status}"
        );
    }
}
