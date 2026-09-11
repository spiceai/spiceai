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
use tools::SpiceModelTool;

use std::sync::Arc;

#[async_trait]
pub trait SpiceToolCatalog: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn name(&self) -> &str;

    /// Retrieve all available tools from a tool catalog.
    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>>;

    /// Return all available tool definitions for a given catalog.
    ///
    /// Overriding this method if it can be implemented more efficiently than by using [`Self::all`].
    async fn all_definitons(&self) -> Vec<ChatCompletionTool> {
        self.all()
            .await
            .into_iter()
            .map(|t| ChatCompletionTool {
                function: FunctionObject {
                    strict: t.strict(),
                    name: t.name().to_string(),
                    description: t.description().map(|d| d.to_string()),
                    parameters: t.parameters(),
                },
            })
            .collect()
    }

    /// Retrieve a tool by name from a tool catalog.
    ///
    /// Tool will either be built with default parameters, or additional
    /// parameters from the catalog.
    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>>;

    /// Synchronous lookup used by the MCP gateway to validate `Mcp-Param-*`
    /// headers before the async `tools/call` path runs.
    ///
    /// Return `None` only when the catalog cannot resolve the tool without I/O.
    /// rmcp caches that miss per tool name, so catalogs that can answer
    /// synchronously must do so here (see [`Self::try_all`]).
    ///
    /// Default is `None` so downstream implementers keep compiling. Catalogs
    /// that participate in MCP `Mcp-Param-*` validation must override this.
    /// Wrappers must forward; inheriting the default is a silent miss.
    fn try_get(&self, _name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        None
    }

    /// Synchronous listing used to keep the MCP schema snapshot populated.
    ///
    /// Return every tool the catalog can expose without I/O. Empty means the
    /// snapshot cannot yet name this catalog's tools.
    ///
    /// Default is empty so downstream implementers keep compiling. Catalogs
    /// that participate in MCP schema snapshots must override this. Wrappers
    /// must forward; inheriting the default leaves the snapshot unnamed.
    fn try_all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Downstream-shaped catalog that implements only the methods that existed
    /// before `try_get` / `try_all`. Used to prove those lookups need defaults.
    struct DownstreamCatalog;

    #[async_trait]
    impl SpiceToolCatalog for DownstreamCatalog {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &'static str {
            "downstream"
        }

        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            Vec::new()
        }

        async fn get(&self, _name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            None
        }
    }

    #[test]
    fn default_try_get_and_try_all_are_empty() {
        let catalog = DownstreamCatalog;
        assert!(
            catalog.try_get("any").is_none(),
            "default try_get must be a miss so downstream impls compile"
        );
        assert!(
            catalog.try_all().is_empty(),
            "default try_all must be empty so downstream impls compile"
        );
    }
}
