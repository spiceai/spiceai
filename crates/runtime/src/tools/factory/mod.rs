use secrecy::SecretString;
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
use spicepod::component::tool::Tool;

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};
use tokio::sync::Mutex;

use super::{
    builtin::catalog::BuiltinToolCatalog, catalog::SpiceToolCatalog,
    memory::catalog::MemoryToolCatalog, SpiceModelTool,
};

pub trait ToolFactory: Send + Sync {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>>;
}

static TOOL_SHED_FACTORY: LazyLock<Mutex<HashMap<String, Arc<dyn ToolFactory>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register a tool factory. The `from_source` is defined in the [`Tool`]'s `from`.
pub async fn register_tool_factory(from_source: &str, tool: Arc<dyn ToolFactory>) {
    let mut registry = TOOL_SHED_FACTORY.lock().await;

    registry.insert(from_source.to_string(), tool);
}

pub async fn register_all_factories() {
    register_tool_factory("builtin", Arc::new(BuiltinToolCatalog {})).await;
    register_tool_factory("memory", Arc::new(MemoryToolCatalog {})).await;
}

/// Get all catalogs available by default in the spice runtime.
#[must_use]
pub fn default_available_catalogs() -> Vec<Arc<dyn SpiceToolCatalog>> {
    vec![
        Arc::new(BuiltinToolCatalog {}),
        Arc::new(MemoryToolCatalog {}),
    ]
}

#[allow(clippy::implicit_hasher)]
pub async fn forge(
    component: &Tool,
    secrets: HashMap<String, SecretString>,
) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
    let from_source = component
        .from
        .split_once(':')
        .map_or("builtin", |(a, _b)| a);

    let registry = TOOL_SHED_FACTORY.lock().await;

    match registry.get(from_source) {
        Some(factory) => factory.construct(component, secrets),
        None => Err(format!("Tool factory not found for source: {from_source}").into()),
    }
}
