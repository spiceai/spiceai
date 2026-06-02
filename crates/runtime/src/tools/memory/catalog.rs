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

use crate::{
    Runtime,
    tools::{
        SpiceModelTool, catalog::SpiceToolCatalog, factory::IndividualToolFactory,
        memory::store::StoreMemoryTool,
    },
};

use super::load::LoadMemoryTool;

pub struct MemoryToolCatalog {
    rt: Arc<Runtime>,
}

impl MemoryToolCatalog {
    #[must_use]
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self { rt }
    }

    pub(crate) fn name() -> &'static str {
        "memory"
    }

    fn get_tool(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            "load" => Some(Arc::new(LoadMemoryTool::new(
                Arc::clone(&self.rt),
                Some(name),
                description,
            ))),
            "store" => Some(Arc::new(StoreMemoryTool::new(
                Arc::clone(&self.rt),
                Some(name),
                description,
            ))),
            _ => None,
        }
    }
}

impl IndividualToolFactory for MemoryToolCatalog {
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

        self.get_tool(
            id,
            Some(component.name.as_str()),
            component.description.as_deref(),
        )
        .ok_or_else(|| format!("Tool with id `{id}` not found in memory tool catalog").into())
    }
}

#[async_trait]
impl SpiceToolCatalog for MemoryToolCatalog {
    fn name(&self) -> &str {
        Self::name()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        vec![
            Arc::new(LoadMemoryTool::new(Arc::clone(&self.rt), None, None)),
            Arc::new(StoreMemoryTool::new(Arc::clone(&self.rt), None, None)),
        ]
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.get_tool(name, None, None)
    }
}
