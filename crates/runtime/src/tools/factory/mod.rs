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
use secrecy::SecretString;
use spicepod::component::tool::Tool;
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

    let tool_factories = rt.tool_factories();
    let registry = tool_factories.lock().await;

    match registry.get(from_source) {
        Some(factory) => factory.construct(component, secrets, env).await,
        None => Err(format!("Tool factory not found for source: {from_source}").into()),
    }
}
