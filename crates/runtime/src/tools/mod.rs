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

use catalog::SpiceToolCatalog;
use factory::default_catalog_names;
use std::{borrow::Cow, sync::Arc};
use tools::{SpiceModelTool, rename::with_name};

pub mod builtin;
pub mod catalog;
pub mod factory;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod memory;
pub mod options;
pub mod utils;

/// [`Tooling`] define several ways to access and load tools into the runtime.
/// Tools can be defined singularly, or as a set of tools a user may want to
/// include all together (i.e. a catalog).
pub enum Tooling {
    Tool(Arc<dyn SpiceModelTool>),
    Catalog(Arc<dyn SpiceToolCatalog>),
}

impl Tooling {
    #[must_use]
    pub async fn tools(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        match self {
            Tooling::Tool(t) => vec![Arc::clone(t)],
            Tooling::Catalog(c) => {
                let catalog_name = c.name();
                if default_catalog_names().contains(&catalog_name) {
                    return c.all().await;
                };

                // If non-default catalog, tool name must be prefixed by catalog.
                c.all()
                    .await
                    .iter()
                    .map(|t| with_name(t, format!("{catalog_name}/{}", t.name()).as_str()))
                    .collect()
            }
        }
    }

    #[must_use]
    pub fn name(&self) -> Cow<'_, str> {
        match self {
            Tooling::Tool(t) => t.name(),
            Tooling::Catalog(c) => Cow::Borrowed(c.name()),
        }
    }
}

impl From<Arc<dyn SpiceModelTool>> for Tooling {
    fn from(tool: Arc<dyn SpiceModelTool>) -> Self {
        Tooling::Tool(tool)
    }
}

impl From<Arc<dyn SpiceToolCatalog>> for Tooling {
    fn from(catalog: Arc<dyn SpiceToolCatalog>) -> Self {
        Tooling::Catalog(catalog)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use async_trait::async_trait;
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
        let t = Tooling::Catalog(Arc::new(MockCatalog {
            name: "not_in_default_catalogs".to_string(),
        }));
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
        let t = Tooling::Catalog(Arc::new(MockCatalog {
            name: default_catalog_names()[0].to_string(),
        }));
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
