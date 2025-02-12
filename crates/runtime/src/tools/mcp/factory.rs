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

use std::{collections::HashMap, sync::Arc};

use futures::FutureExt;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;

use crate::tools::{catalog::SpiceToolCatalog, factory::ToolCatalogFactory, SpiceModelTool};

use super::{catalog::McpToolCatalog, Error, MCPConfig, MCPType};

pub struct McpCatalogFactory {}

impl ToolCatalogFactory for McpCatalogFactory {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> std::result::Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>>
    {
        let Some(("mcp", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `mcp:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };
        let mcp_type: MCPType = serde_json::from_str(id)
            .map_err(|_| Error::InvalidMCPDirective { id: id.to_string() })
            .boxed()?;

        let cfg = MCPConfig::try_from_type(&mcp_type, &params_with_secrets).boxed()?;

        McpToolCatalog::try_new(
            cfg,
            component.name.as_str(),
            component.description.as_deref(),
        )
        .boxed()
    }
}
