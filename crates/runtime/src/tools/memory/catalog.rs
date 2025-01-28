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

use crate::tools::{
    catalog::SpiceToolCatalog, factory::ToolFactory, memory::store::StoreMemoryTool, SpiceModelTool,
};

use super::load::LoadMemoryTool;

pub struct MemoryToolCatalog {}

impl MemoryToolCatalog {
    fn get_tool(
        id: &str,
        name: Option<&str>,
        description: Option<String>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            "load" => Some(Arc::new(LoadMemoryTool::new(name, description))),
            "store" => Some(Arc::new(StoreMemoryTool::new(name, description))),
            _ => None,
        }
    }
}

impl ToolFactory for MemoryToolCatalog {
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

        Self::get_tool(
            id,
            Some(component.name.as_str()),
            component.description.clone(),
        )
        .ok_or_else(|| format!("Tool with id `{id}` not found in memory tool catalog").into())
    }
}

#[async_trait]
impl SpiceToolCatalog for MemoryToolCatalog {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        vec![
            Arc::new(LoadMemoryTool::default()),
            Arc::new(StoreMemoryTool::default()),
        ]
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        Self::get_tool(name, None, None)
    }
}
