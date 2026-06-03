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

pub use runtime_tools::tooling::Tooling;
pub use tools::SpiceModelTool;

pub mod builtin;
pub mod factory;
pub(crate) mod registry;
pub mod utils;

#[cfg(test)]
mod tests {

    use super::*;
    use async_trait::async_trait;
    use runtime_tools::catalog::SpiceToolCatalog;
    use serde_json::Value;
    use std::borrow::Cow;
    use std::sync::Arc;

    struct MockTool {
        name: String,
    }

    #[async_trait]
    impl SpiceModelTool for MockTool {
        fn name(&self) -> Cow<'_, str> {
            self.name.clone().into()
        }
        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }
        fn parameters(&self) -> Option<Value> {
            None
        }
        async fn call(&self, _: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::Null)
        }
    }

    struct MockCatalog {
        name: String,
    }

    #[async_trait]
    impl SpiceToolCatalog for MockCatalog {
        fn name(&self) -> &str {
            self.name.as_str()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
            vec![
                Arc::new(MockTool {
                    name: "foo".to_string(),
                }),
                Arc::new(MockTool {
                    name: "bar".to_string(),
                }),
                Arc::new(MockTool {
                    name: "baz".to_string(),
                }),
            ]
        }

        async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
            Some(Arc::new(MockTool {
                name: name.to_string(),
            }))
        }
    }

    #[tokio::test]
    async fn test_non_default_catalog() {
        let t = Tooling::Catalog {
            tools: Arc::new(MockCatalog {
                name: "not_in_default_catalogs".to_string(),
            }),
            default_catalog_names: vec![],
        };
        assert_eq!(
            t.tools()
                .await
                .iter()
                .map(|tt| tt.name().to_string())
                .collect::<Vec<String>>(),
            vec![
                "not_in_default_catalogs/foo",
                "not_in_default_catalogs/bar",
                "not_in_default_catalogs/baz",
            ]
        );
    }

    #[tokio::test]
    async fn test_default_catalog() {
        let t = Tooling::Catalog {
            tools: Arc::new(MockCatalog {
                name: "memory".to_string(),
            }),
            default_catalog_names: vec!["memory".to_string(), "auto".to_string()],
        };
        assert_eq!(
            t.tools()
                .await
                .iter()
                .map(|tt| tt.name().to_string())
                .collect::<Vec<String>>(),
            vec!["foo", "bar", "baz",]
        );
    }
}
