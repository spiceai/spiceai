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

use async_trait::async_trait;
use serde_json::Value;
use spicepod::component::tool::{Tool, ToolExecutionError, ToolExecutionResult, ToolSpec};
use crate::status::RuntimeStatus;
use crate::tools::SpiceModelTool;

pub struct GetReadinessTool {
    runtime_status: Arc<RuntimeStatus>,
}

impl GetReadinessTool {
    pub fn new(runtime_status: Arc<RuntimeStatus>) -> Self {
        Self { runtime_status }
    }
}

#[async_trait]
impl SpiceModelTool for GetReadinessTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "get_readiness".to_string(),
            description: "Retrieves the readiness status of all registered datasets, models, embeddings, views, and other primitives.".to_string(),
            ..Default::default()
        }
    }

    async fn execute(&self, _params: HashMap<String, Value>) -> Result<ToolExecutionResult, ToolExecutionError> {
        let statuses = self.runtime_status.get_all_statuses();
        Ok(ToolExecutionResult::new(statuses))
    }
}

impl From<GetReadinessTool> for Tool {
    fn from(tool: GetReadinessTool) -> Self {
        Tool {
            name: tool.spec().name,
            description: tool.spec().description,
            ..Default::default()
        }
    }
}
