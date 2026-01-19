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

use async_trait::async_trait;
use secrecy::SecretString;
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::{
    Runtime,
    tools::{
        SpiceModelTool, catalog::SpiceToolCatalog, factory::IndividualToolFactory,
        memory::store::StoreMemoryTool,
    },
};

use super::load::LoadMemoryTool;

pub struct MemoryToolCatalog {
    rt: Arc<Runtime>,
}

impl MemoryToolCatalog {
    #[must_use]
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self { rt }
    }

    pub(crate) fn name() -> &'static str {
        "memory"
    }

    fn get_tool(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            "load" => Some(Arc::new(LoadMemoryTool::new(
                Arc::clone(&self.rt),
                Some(name),
                description,
            ))),
            "store" => Some(Arc::new(StoreMemoryTool::new(
                Arc::clone(&self.rt),
                Some(name),
                description,
            ))),
            _ => None,
        }
    }
}

impl IndividualToolFactory for MemoryToolCatalog {
    fn construct(
        &self,
        component: &Tool,
        _params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("memory", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `memory:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        self.get_tool(
            id,
            Some(component.name.as_str()),
            component.description.as_deref(),
        )
        .ok_or_else(|| format!("Tool with id `{id}` not found in memory tool catalog").into())
    }
}

#[async_trait]
impl SpiceToolCatalog for MemoryToolCatalog {
    fn name(&self) -> &str {
        Self::name()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        vec![
            Arc::new(LoadMemoryTool::new(Arc::clone(&self.rt), None, None)),
            Arc::new(StoreMemoryTool::new(Arc::clone(&self.rt), None, None)),
        ]
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.get_tool(name, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::component::tool::Tool;

    fn create_tool(from: &str, name: &str) -> Tool {
        Tool {
            name: name.to_string(),
            from: from.to_string(),
            description: None,
            params: std::collections::HashMap::new(),
            env: std::collections::HashMap::new(),
            depends_on: vec![],
            metrics: None,
        }
    }

    #[test]
    fn test_catalog_name() {
        assert_eq!(MemoryToolCatalog::name(), "memory");
    }

    #[tokio::test]
    async fn test_construct_invalid_from_no_colon() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let component = create_tool("memory", "my_tool");
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_err());
        let err = result.err().expect("should have error");
        assert!(
            err.to_string().contains("Invalid component"),
            "Error should mention invalid component: {err}",
        );
    }

    #[tokio::test]
    async fn test_construct_invalid_from_wrong_prefix() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let component = create_tool("other:load", "my_tool");
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_construct_unknown_tool_id() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let component = create_tool("memory:unknown", "my_tool");
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_err());
        let err = result.err().expect("should have error");
        assert!(
            err.to_string().contains("not found"),
            "Error should mention tool not found: {err}",
        );
    }

    #[tokio::test]
    async fn test_construct_load_tool() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let component = create_tool("memory:load", "custom_load");
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_ok());
        let tool = result.expect("should create tool");
        assert_eq!(tool.name(), "custom_load");
    }

    #[tokio::test]
    async fn test_construct_store_tool() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let component = create_tool("memory:store", "custom_store");
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_ok());
        let tool = result.expect("should create tool");
        assert_eq!(tool.name(), "custom_store");
    }

    #[tokio::test]
    async fn test_construct_with_custom_description() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);
        let mut component = create_tool("memory:load", "my_load");
        component.description = Some("Custom description for loading memories".to_string());
        let params = std::collections::HashMap::new();

        let result = catalog.construct(&component, params);
        assert!(result.is_ok());
        let tool = result.expect("should create tool");
        assert_eq!(tool.name(), "my_load");
        assert_eq!(
            tool.description().map(|d| d.to_string()),
            Some("Custom description for loading memories".to_string())
        );
    }

    #[tokio::test]
    async fn test_catalog_all_returns_both_tools() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);

        let tools = catalog.all().await;
        assert_eq!(tools.len(), 2);

        let names: Vec<_> = tools.iter().map(|t| t.name().to_string()).collect();
        assert!(names.contains(&"load_memory".to_string()));
        assert!(names.contains(&"store_memory".to_string()));
    }

    #[tokio::test]
    async fn test_catalog_get_load() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);

        let tool = catalog.get("load").await;
        assert!(tool.is_some());
        assert_eq!(tool.expect("should have tool").name(), "load");
    }

    #[tokio::test]
    async fn test_catalog_get_store() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);

        let tool = catalog.get("store").await;
        assert!(tool.is_some());
        assert_eq!(tool.expect("should have tool").name(), "store");
    }

    #[tokio::test]
    async fn test_catalog_get_unknown() {
        let rt = Arc::new(Runtime::builder().build().await);
        let catalog = MemoryToolCatalog::new(rt);

        let tool = catalog.get("unknown").await;
        assert!(tool.is_none());
    }
}
