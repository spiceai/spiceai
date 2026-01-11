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

//! Mem0 tool catalog for providing memory tools to LLMs.

use async_trait::async_trait;
use std::sync::Arc;

use crate::tools::{SpiceModelTool, catalog::SpiceToolCatalog};

use super::{
    client::Mem0Client,
    tools::{AddMemoryTool, DeleteMemoryTool, GetMemoriesTool, SearchMemoryTool},
};

/// Available tool IDs in the Mem0 catalog.
pub const TOOL_ADD: &str = "add";
pub const TOOL_SEARCH: &str = "search";
pub const TOOL_GET: &str = "get";
pub const TOOL_DELETE: &str = "delete";

/// Catalog of Mem0 memory tools.
pub struct Mem0ToolCatalog {
    client: Arc<Mem0Client>,
    name: String,
}

impl Mem0ToolCatalog {
    /// Create a new Mem0 tool catalog.
    #[must_use]
    pub fn new(client: Mem0Client, name: &str) -> Self {
        Self {
            client: Arc::new(client),
            name: name.to_string(),
        }
    }

    /// Get a tool by its ID with optional custom name and description.
    fn get_tool(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        match id {
            TOOL_ADD => Some(Arc::new(AddMemoryTool::new(
                Arc::clone(&self.client),
                name,
                description,
            ))),
            TOOL_SEARCH => Some(Arc::new(SearchMemoryTool::new(
                Arc::clone(&self.client),
                name,
                description,
            ))),
            TOOL_GET => Some(Arc::new(GetMemoriesTool::new(
                Arc::clone(&self.client),
                name,
                description,
            ))),
            TOOL_DELETE => Some(Arc::new(DeleteMemoryTool::new(
                Arc::clone(&self.client),
                name,
                description,
            ))),
            _ => None,
        }
    }

    /// List all available tool IDs.
    #[must_use]
    pub fn available_tools() -> &'static [&'static str] {
        &[TOOL_ADD, TOOL_SEARCH, TOOL_GET, TOOL_DELETE]
    }
}

#[async_trait]
impl SpiceToolCatalog for Mem0ToolCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        Self::available_tools()
            .iter()
            .filter_map(|id| self.get_tool(id, None, None))
            .collect()
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.get_tool(name, None, None)
    }
}

impl std::fmt::Debug for Mem0ToolCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mem0ToolCatalog")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::mem0::client::Mem0Config;
    use secrecy::SecretString;

    fn create_test_catalog() -> Mem0ToolCatalog {
        let api_key = SecretString::from("test-api-key");
        let config = Mem0Config::new(api_key);
        let client = Mem0Client::new(config).expect("should create client");
        Mem0ToolCatalog::new(client, "test-mem0")
    }

    #[test]
    fn test_catalog_name() {
        let catalog = create_test_catalog();
        assert_eq!(catalog.name(), "test-mem0");
    }

    #[test]
    fn test_available_tools() {
        let tools = Mem0ToolCatalog::available_tools();
        assert_eq!(tools.len(), 4);
        assert!(tools.contains(&TOOL_ADD));
        assert!(tools.contains(&TOOL_SEARCH));
        assert!(tools.contains(&TOOL_GET));
        assert!(tools.contains(&TOOL_DELETE));
    }

    #[test]
    fn test_get_add_tool() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool(TOOL_ADD, None, None);
        assert!(tool.is_some());
        assert_eq!(tool.expect("tool exists").name(), "add_memory");
    }

    #[test]
    fn test_get_search_tool() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool(TOOL_SEARCH, None, None);
        assert!(tool.is_some());
        assert_eq!(tool.expect("tool exists").name(), "search_memory");
    }

    #[test]
    fn test_get_get_tool() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool(TOOL_GET, None, None);
        assert!(tool.is_some());
        assert_eq!(tool.expect("tool exists").name(), "get_memories");
    }

    #[test]
    fn test_get_delete_tool() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool(TOOL_DELETE, None, None);
        assert!(tool.is_some());
        assert_eq!(tool.expect("tool exists").name(), "delete_memory");
    }

    #[test]
    fn test_get_unknown_tool() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool("unknown", None, None);
        assert!(tool.is_none());
    }

    #[test]
    fn test_get_tool_with_custom_name() {
        let catalog = create_test_catalog();
        let tool = catalog.get_tool(TOOL_ADD, Some("custom_add"), None);
        assert!(tool.is_some());
        assert_eq!(tool.expect("tool exists").name(), "custom_add");
    }

    #[test]
    fn test_catalog_debug() {
        let catalog = create_test_catalog();
        let debug_str = format!("{catalog:?}");
        assert!(debug_str.contains("Mem0ToolCatalog"));
        assert!(debug_str.contains("test-mem0"));
    }

    #[test]
    fn test_catalog_as_any() {
        let catalog = create_test_catalog();
        let any = catalog.as_any();
        assert!(any.downcast_ref::<Mem0ToolCatalog>().is_some());
    }

    #[tokio::test]
    async fn test_catalog_all() {
        let catalog = create_test_catalog();
        let tools = catalog.all().await;
        assert_eq!(tools.len(), 4);

        let names: Vec<_> = tools.iter().map(|t| t.name().to_string()).collect();
        assert!(names.contains(&"add_memory".to_string()));
        assert!(names.contains(&"search_memory".to_string()));
        assert!(names.contains(&"get_memories".to_string()));
        assert!(names.contains(&"delete_memory".to_string()));
    }

    #[tokio::test]
    async fn test_catalog_get() {
        let catalog = create_test_catalog();

        let add = catalog.get("add").await;
        assert!(add.is_some());
        assert_eq!(add.expect("tool exists").name(), "add_memory");

        let unknown = catalog.get("nonexistent").await;
        assert!(unknown.is_none());
    }
}
