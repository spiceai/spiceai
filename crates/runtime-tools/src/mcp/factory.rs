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

use crate::catalog::SpiceToolCatalog;
use crate::factory::ToolCatalogFactory;

use super::{MCPConfig, MCPType, access::McpToolAccess, catalog::McpToolCatalog};

pub struct McpCatalogFactory {}

#[async_trait]
impl ToolCatalogFactory for McpCatalogFactory {
    async fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
        env: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("mcp", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `mcp:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        let mcp_type = MCPType::from_str(id)?;
        let cfg = MCPConfig::from_type(&mcp_type, &params_with_secrets, &env)?;

        let access =
            McpToolAccess::from_spicepod(component.access.as_ref(), &component.read_only_tools);
        let ctlg =
            McpToolCatalog::try_new(cfg, component.name.as_str(), component.description.clone())
                .await
                .boxed()?
                .with_access(access.clone());

        let listed = ctlg.listed_tool_names();
        for entry in access.unmatched(&listed) {
            tracing::warn!(
                "Tool '{}' (mcp): `read_only_tools` entry '{entry}' matches no tool the server reports, so it has no effect. Check the server's tool names. See: https://spiceai.org/docs/components/tools",
                component.name
            );
        }

        Ok(Arc::new(ctlg))
    }
}
