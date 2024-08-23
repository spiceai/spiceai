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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::{
    tools::{parameters, SpiceModelTool},
    Runtime,
};
use snafu::ResultExt;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TableSchemaToolParams {
    /// Which subset of tables to return results for. Default to all tables.
    tables: Vec<String>,
}
pub struct TableSchemaTool {
    name: String,
    description: Option<String>,
}

impl TableSchemaTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }
}
impl Default for TableSchemaTool {
    fn default() -> Self {
        Self::new(
            "table_schema",
            Some("Retrieve the schema of all available SQL tables".to_string()),
        )
    }
}
#[async_trait]
impl SpiceModelTool for TableSchemaTool {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<TableSchemaToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::table_schema", tool = self.name(), input = arg);
        let req: TableSchemaToolParams = serde_json::from_str(arg)?;

        let mut table_schemas: Vec<Value> = Vec::with_capacity(req.tables.len());
        for t in &req.tables {
            let schema = rt
                .datafusion()
                .get_arrow_schema(t)
                .instrument(span.clone())
                .await
                .boxed()?;

            let v = serde_json::value::to_value(schema).boxed()?;

            table_schemas.push(v);
        }
        Ok(Value::Array(table_schemas))
    }
}
