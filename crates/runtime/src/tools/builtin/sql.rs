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
use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use crate::{
    datafusion::query::Protocol,
    tools::{parameters, SpiceModelTool},
    Runtime,
};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SqlToolParams {
    /// The SQL query to run. Double quote all select columns and never select columns ending in '_embedding'. The `table_catalog` is 'spice'. Always use it in the query
    query: String,
}
pub struct SqlTool {
    name: String,
    description: Option<String>,
}

impl SqlTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }
}

impl Default for SqlTool {
    fn default() -> Self {
        Self::new(
            "sql",
            Some("Run an SQL query on the data source".to_string()),
        )
    }
}

impl From<SqlTool> for spicepod::component::tool::Tool {
    fn from(val: SqlTool) -> Self {
        spicepod::component::tool::Tool {
            from: format!("builtin:{}", val.name()),
            name: val.name().to_string(),
            description: val.description().map(ToString::to_string),
            params: HashMap::default(),
            depends_on: Vec::default(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for SqlTool {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SqlToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sql", tool = self.name(), input = arg);
        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let req: SqlToolParams = serde_json::from_str(arg)?;

            let query_result = rt
                .datafusion()
                .query_builder(&req.query, Protocol::Flight)
                .build()
                .run()
                .await
                .boxed()?;

            let batches = query_result
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .boxed()?;

            let buf = Vec::new();
            let mut writer = arrow_json::ArrayWriter::new(buf);
            writer.write_batches(batches.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
            Ok(Value::String(String::from_utf8(writer.into_inner())?))
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
