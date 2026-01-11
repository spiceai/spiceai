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

//! Factory for creating Mem0 tool catalogs.

use async_trait::async_trait;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::tools::{catalog::SpiceToolCatalog, factory::ToolCatalogFactory};

use super::{
    Error,
    catalog::Mem0ToolCatalog,
    client::{Mem0Client, Mem0Config},
};

/// Factory for creating Mem0 tool catalogs.
pub struct Mem0CatalogFactory {}

#[async_trait]
impl ToolCatalogFactory for Mem0CatalogFactory {
    async fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
        _env: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("mem0", _id)) = component.from.split_once(':') else {
            return Err(Box::new(Error::InvalidMem0Directive {
                id: component.from.clone(),
            }));
        };

        let config = Mem0Config::from_params(&params_with_secrets).boxed()?;
        let client = Mem0Client::new(config).boxed()?;
        let catalog = Mem0ToolCatalog::new(client, &component.name);

        Ok(Arc::new(catalog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::component::tool::Tool;

    fn create_tool_with_from(from: &str) -> Tool {
        Tool {
            name: "test-mem0".to_string(),
            from: from.to_string(),
            description: None,
            params: HashMap::new(),
            env: HashMap::new(),
            depends_on: vec![],
            metrics: None,
        }
    }

    #[tokio::test]
    async fn test_factory_invalid_directive_no_colon() {
        let factory = Mem0CatalogFactory {};
        let component = create_tool_with_from("mem0");
        let params = HashMap::new();
        let env = HashMap::new();

        let result = factory.construct(&component, params, env).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_factory_invalid_directive_wrong_prefix() {
        let factory = Mem0CatalogFactory {};
        let component = create_tool_with_from("other:id");
        let params = HashMap::new();
        let env = HashMap::new();

        let result = factory.construct(&component, params, env).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_factory_missing_api_key() {
        let factory = Mem0CatalogFactory {};
        let component = create_tool_with_from("mem0:memory");
        let params = HashMap::new();
        let env = HashMap::new();

        let result = factory.construct(&component, params, env).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_factory_valid_config() {
        let factory = Mem0CatalogFactory {};
        let component = create_tool_with_from("mem0:memory");
        let mut params = HashMap::new();
        params.insert("api_key".to_string(), SecretString::from("test-key"));
        let env = HashMap::new();

        let result = factory.construct(&component, params, env).await;

        let catalog = result.expect("should create catalog");
        assert_eq!(catalog.name(), "test-mem0");
    }

    #[tokio::test]
    async fn test_factory_with_custom_base_url() {
        let factory = Mem0CatalogFactory {};
        let component = create_tool_with_from("mem0:memory");
        let mut params = HashMap::new();
        params.insert("api_key".to_string(), SecretString::from("test-key"));
        params.insert(
            "base_url".to_string(),
            SecretString::from("https://custom.api.mem0.ai"),
        );
        let env = HashMap::new();

        let result = factory.construct(&component, params, env).await;
        result.expect("should create catalog with custom base URL");
    }
}
