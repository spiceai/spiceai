/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
use secrecy::SecretString;
use spicepod::component::{access::AccessMode, tool::Tool};
use std::{collections::HashMap, sync::Arc};

use crate::Runtime;

use runtime_query_engine::query_engine::QueryEngine;
#[cfg(feature = "mcp")]
use runtime_tools::mcp::factory::McpCatalogFactory;

use runtime_tools::{
    catalog::SpiceToolCatalog,
    factory::{IndividualToolFactory, ToolCatalogFactory},
    memory::catalog::MemoryToolCatalog,
};

use super::{Tooling, builtin::catalog::BuiltinToolCatalog};

#[derive(Clone)]
pub enum ToolFactory {
    Catalog(Arc<dyn ToolCatalogFactory>),
    Tool(Arc<dyn IndividualToolFactory>),
}

impl ToolFactory {
    async fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
        env: HashMap<String, SecretString>,
    ) -> Result<Tooling, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ToolFactory::Catalog(c) => {
                let catalog = c.construct(component, params_with_secrets, env).await?;
                Ok(Tooling::Catalog {
                    tools: catalog,
                    default_catalog_names: default_catalog_names()
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                })
            }
            ToolFactory::Tool(t) => t.construct(component, params_with_secrets).map(Into::into),
        }
    }
}

#[must_use]
pub fn default_catalog_names<'a>() -> Vec<&'a str> {
    vec![MemoryToolCatalog::name(), BuiltinToolCatalog::name()]
}

impl From<Arc<dyn ToolCatalogFactory>> for ToolFactory {
    fn from(catalog: Arc<dyn ToolCatalogFactory>) -> Self {
        ToolFactory::Catalog(catalog)
    }
}

impl From<Arc<dyn IndividualToolFactory>> for ToolFactory {
    fn from(tool: Arc<dyn IndividualToolFactory>) -> Self {
        ToolFactory::Tool(tool)
    }
}

pub async fn register_all_factories(rt: Arc<Runtime>) {
    let tool_factories = rt.tool_factories();
    let mut registry = tool_factories.lock().await;
    registry.insert(
        "builtin".to_string(),
        ToolFactory::Tool(Arc::new(BuiltinToolCatalog::new(
            rt.datafusion() as Arc<dyn QueryEngine>,
            Arc::clone(&rt.app),
            rt.status(),
            rt.datafusion().search_cache_provider(),
        ))),
    );
    registry.insert(
        "memory".to_string(),
        ToolFactory::Tool(Arc::new(MemoryToolCatalog::new(
            rt.datafusion() as Arc<dyn QueryEngine>,
            Arc::clone(&rt.app),
        ))),
    );
    #[cfg(feature = "mcp")]
    registry.insert(
        "mcp".to_string(),
        ToolFactory::Catalog(Arc::new(McpCatalogFactory {})),
    );
}

pub async fn unregister_all_factories(rt: &Runtime) {
    let tool_factories = rt.tool_factories();
    let mut registry = tool_factories.lock().await;
    registry.clear();

    let mut tools = rt.tools.write().await;
    tools.clear();
    #[cfg(feature = "mcp")]
    rt.mcp_schemas.clear();
}

/// Get all catalogs available by default in the spice runtime.
#[must_use]
pub fn default_available_catalogs(rt: &Arc<Runtime>) -> Vec<Arc<dyn SpiceToolCatalog>> {
    vec![
        Arc::new(BuiltinToolCatalog::new(
            rt.datafusion() as Arc<dyn QueryEngine>,
            Arc::clone(&rt.app),
            rt.status(),
            rt.datafusion().search_cache_provider(),
        )),
        Arc::new(MemoryToolCatalog::new(
            rt.datafusion() as Arc<dyn QueryEngine>,
            Arc::clone(&rt.app),
        )),
    ]
}

/// Forge creates `Tooling` from a `Tool` component. It uses the `from` field to determine if it should create a [`SpiceToolCatalog`] or a [`SpiceModelTool`].
#[expect(clippy::implicit_hasher)]
pub async fn forge(
    component: &Tool,
    secrets: HashMap<String, SecretString>,
    rt: Arc<Runtime>,
    env: HashMap<String, SecretString>,
) -> Result<Tooling, Box<dyn std::error::Error + Send + Sync>> {
    let from_source = component
        .from
        .split_once(':')
        .map_or("builtin", |(a, _b)| a);

    // Not held across `construct`: one tool that is slow to construct must not block the others.
    if let Some(message) = access_config_error(component, from_source) {
        return Err(Box::new(ToolAccessConfigError { message }));
    }

    let factory = rt.tool_factories().lock().await.get(from_source).cloned();

    match factory {
        Some(factory) => factory.construct(component, secrets, env).await,
        None => Err(Box::new(ToolFactoryNotFound {
            from_source: from_source.to_string(),
        })),
    }
}

/// Where `access` and `read_only_tools` are documented.
const TOOLS_DOCS: &str = "https://spiceai.org/docs/components/tools";

/// What is wrong with the `access` and `read_only_tools` of `tool`, whose `from` names the
/// source `kind`, if anything: both apply only to an MCP server's tools (a built-in tool's access
/// is fixed), `access` there is `read` or `read_write`, and `read_only_tools` lists exceptions
/// to `access: read_write`.
#[must_use]
pub fn access_config_error(tool: &Tool, kind: &str) -> Option<String> {
    let name = &tool.name;
    if kind != "mcp" {
        if tool.access.is_some() {
            return Some(format!(
                "Failed to load tool '{name}' ({kind}): `access` cannot be set on a built-in tool, whose access is fixed. Remove `access` from tool '{name}'. See: {TOOLS_DOCS}"
            ));
        }
        if !tool.read_only_tools.is_empty() {
            return Some(format!(
                "Failed to load tool '{name}' ({kind}): `read_only_tools` cannot be set on a built-in tool, whose access is fixed. Remove `read_only_tools` from tool '{name}'. See: {TOOLS_DOCS}"
            ));
        }
        return None;
    }
    match tool.access {
        Some(AccessMode::ReadWriteCreate) => Some(format!(
            "Failed to load tool '{name}' ({kind}): `access` accepts `read` or `read_write` on an MCP tool, got '{}'. See: {TOOLS_DOCS}",
            AccessMode::ReadWriteCreate.as_str()
        )),
        Some(AccessMode::Read) if !tool.read_only_tools.is_empty() => Some(format!(
            "Failed to load tool '{name}' ({kind}): `read_only_tools` lists the read-only exceptions to `access: read_write`, and tool '{name}' sets `access: read`, so every tool already only reads. Remove `read_only_tools` or set `access: read_write`. See: {TOOLS_DOCS}"
        )),
        Some(AccessMode::Read | AccessMode::ReadWrite) | None => None,
    }
}

/// A tool's `access` or `read_only_tools` is invalid for its kind (see
/// [`access_config_error`]). No retry can clear it.
#[derive(Debug)]
pub struct ToolAccessConfigError {
    message: String,
}

impl std::fmt::Display for ToolAccessConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ToolAccessConfigError {}

/// A tool's `from` names a source no tool factory handles. No retry can clear it.
#[derive(Debug)]
pub struct ToolFactoryNotFound {
    from_source: String,
}

impl std::fmt::Display for ToolFactoryNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Tool factory not found for source: {}", self.from_source)
    }
}

impl std::error::Error for ToolFactoryNotFound {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;

    struct Stalled;

    #[async_trait]
    impl ToolCatalogFactory for Stalled {
        async fn construct(
            &self,
            _component: &Tool,
            _params_with_secrets: HashMap<String, SecretString>,
            _env: HashMap<String, SecretString>,
        ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>> {
            std::future::pending().await
        }
    }

    fn tool(from: &str) -> Tool {
        serde_json::from_value(serde_json::json!({"from": from, "name": "t"}))
            .expect("a valid tool")
    }

    /// `access` and `read_only_tools` are accepted only on an MCP server's tools, with the
    /// values that mean something there; every mistake names the tool and the fix.
    #[test]
    fn access_config_errors() {
        let with = |name: &str, from: &str, extra: serde_json::Value| -> Tool {
            let mut tool = serde_json::json!({"from": from, "name": name});
            if let (Some(tool), Some(extra)) = (tool.as_object_mut(), extra.as_object()) {
                tool.extend(extra.clone());
            }
            serde_json::from_value(tool).expect("a valid tool")
        };
        for (tool, kind, expected) in [
            (
                with(
                    "store_memory",
                    "memory:store",
                    serde_json::json!({"access": "read"}),
                ),
                "memory",
                Some(
                    "Failed to load tool 'store_memory' (memory): `access` cannot be set on a built-in tool, whose access is fixed. Remove `access` from tool 'store_memory'. See: https://spiceai.org/docs/components/tools",
                ),
            ),
            (
                with("sql", "sql", serde_json::json!({"read_only_tools": ["x"]})),
                "builtin",
                Some(
                    "Failed to load tool 'sql' (builtin): `read_only_tools` cannot be set on a built-in tool, whose access is fixed. Remove `read_only_tools` from tool 'sql'. See: https://spiceai.org/docs/components/tools",
                ),
            ),
            (
                with(
                    "tickets",
                    "mcp:https://tickets.example.com/mcp",
                    serde_json::json!({"access": "read_write_create"}),
                ),
                "mcp",
                Some(
                    "Failed to load tool 'tickets' (mcp): `access` accepts `read` or `read_write` on an MCP tool, got 'read_write_create'. See: https://spiceai.org/docs/components/tools",
                ),
            ),
            (
                with(
                    "docs",
                    "mcp:https://docs.example.com/mcp",
                    serde_json::json!({"access": "read", "read_only_tools": ["search_*"]}),
                ),
                "mcp",
                Some(
                    "Failed to load tool 'docs' (mcp): `read_only_tools` lists the read-only exceptions to `access: read_write`, and tool 'docs' sets `access: read`, so every tool already only reads. Remove `read_only_tools` or set `access: read_write`. See: https://spiceai.org/docs/components/tools",
                ),
            ),
            (
                with(
                    "tickets",
                    "mcp:https://tickets.example.com/mcp",
                    serde_json::json!({"access": "read_write", "read_only_tools": ["search_*", "get_ticket"]}),
                ),
                "mcp",
                None,
            ),
            (
                with(
                    "docs",
                    "mcp:https://docs.example.com/mcp",
                    serde_json::json!({"access": "read"}),
                ),
                "mcp",
                None,
            ),
            (with("sql", "sql", serde_json::json!({})), "builtin", None),
        ] {
            assert_eq!(
                access_config_error(&tool, kind).as_deref(),
                expected,
                "{} ({kind})",
                tool.name
            );
        }
    }

    #[tokio::test]
    async fn a_tool_that_never_constructs_does_not_block_other_tools() {
        let rt = Arc::new(Runtime::builder().build().await);
        rt.tool_factories().lock().await.insert(
            "stalled".to_string(),
            ToolFactory::Catalog(Arc::new(Stalled)),
        );
        let stalled_rt = Arc::clone(&rt);
        let stalled = tokio::spawn(async move {
            forge(
                &tool("stalled:x"),
                HashMap::new(),
                stalled_rt,
                HashMap::new(),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let other = tokio::time::timeout(
            Duration::from_secs(10),
            forge(
                &tool("missing:x"),
                HashMap::new(),
                Arc::clone(&rt),
                HashMap::new(),
            ),
        )
        .await
        .expect("another tool's load is not blocked by the stalled one");
        assert!(other.is_err_and(|e| e.to_string().contains("Tool factory not found")));
        stalled.abort();
    }
}
