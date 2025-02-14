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
use async_openai::types::{ChatCompletionTool, ChatCompletionToolType, FunctionObject};
use async_trait::async_trait;

use super::SpiceModelTool;
use std::sync::Arc;

#[async_trait]
pub trait SpiceToolCatalog: Send + Sync {
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
                r#type: ChatCompletionToolType::Function,
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
}
