/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::sync::Arc;

use crate::tools::SpiceModelTool;
use crate::Runtime;
use async_trait::async_trait;
use serde_json::Value;

pub struct GetReadinessTool {
    name: String,
    description: Option<String>,
}

impl GetReadinessTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }
}
impl Default for GetReadinessTool {
    fn default() -> Self {
        Self::new(
            "get_readiness",
            Some("Retrieves the readiness status of all runtime components including registered datasets, models, and embeddings.".to_string()),
        )
    }
}

#[async_trait]
impl SpiceModelTool for GetReadinessTool {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        _arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::get_readiness", tool = self.name());

        let statuses = rt.status().get_all_statuses();
        let statuses_map: serde_json::Map<String, Value> = statuses
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.to_string())))
            .collect();
        Ok(Value::Object(statuses_map))
    }
}

impl From<GetReadinessTool> for spicepod::component::tool::Tool {
    fn from(val: GetReadinessTool) -> Self {
        spicepod::component::tool::Tool {
            from: format!("builtin:{}", val.name()),
            name: val.name().to_string(),
            description: val.description().map(ToString::to_string),
            params: HashMap::default(),
            depends_on: Vec::default(),
        }
    }
}
