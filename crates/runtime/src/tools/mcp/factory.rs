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
use std::{collections::HashMap, str::FromStr, sync::Arc};

use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;

use crate::tools::{catalog::SpiceToolCatalog, factory::ToolCatalogFactory};

use super::{catalog::McpToolCatalog, MCPConfig, MCPType};

pub struct McpCatalogFactory {}

#[async_trait]
impl ToolCatalogFactory for McpCatalogFactory {
    async fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("mcp", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `mcp:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        let mcp_type = MCPType::from_str(id)?;
        let cfg = MCPConfig::from_type(&mcp_type, &params_with_secrets);

        let ctlg = McpToolCatalog::try_new(cfg, component.name.as_str())
            .await
            .boxed()?;

        Ok(Arc::new(ctlg))
    }
}
