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
use async_trait::async_trait;
use datafusion::sql::TableReference;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::{
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    tools::SpiceModelTool,
    Runtime,
};

pub struct ListDatasetsTool {}

#[async_trait]
impl SpiceModelTool for ListDatasetsTool {
    fn name(&self) -> &'static str {
        "list_datasets"
    }

    fn description(&self) -> Option<&'static str> {
        Some("List all SQL tables available.")
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::list_datasets", tool = self.name(), input = arg);

        let Some(app) = &*rt.app.read().await else {
            return Err("Couldn't get runtime `App`".into());
        };

        let tables = app.datasets.iter()
            .map(|d| {
                json!({
                    "table": TableReference::parse_str(&d.name).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA).to_string(),
                    "can_search_documents": !d.embeddings.is_empty(),
                    "description": d.description.clone(),
                    "metadata": d.metadata.clone(),
                })
            })
            .collect::<Vec<Value>>();

        Ok(Value::Array(tables))
    }
}
