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

use arrow::array::{AsArray, RecordBatch};
use async_trait::async_trait;
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::{
    tools::{utils::parameters, SpiceModelTool},
    Runtime,
};

use super::memory_table_name;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct LoadMemoryParams {
    /// Retrieve memories created in the 'last' interval. ISO 8601 Format, e.g: "1h", "2m30s".
    pub last: String,
}

pub struct LoadMemoryTool {
    name: String,
    description: Option<String>,
}

impl LoadMemoryTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }
}

impl Default for LoadMemoryTool {
    fn default() -> Self {
        Self::new(
            "load_memory",
            Some("Load memories previously saved by the language model.".to_string()),
        )
    }
}

#[async_trait]
impl SpiceModelTool for LoadMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_deref().map(Cow::Borrowed)
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<LoadMemoryParams>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::load_memory", tool = self.name().to_string(), input = arg);

        let table_name = memory_table_name(&rt).await?;
        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: LoadMemoryParams = serde_json::from_str(arg).boxed()?;
            let last_interval = fundu::parse_duration(params.last.as_str()).boxed()?;

            let batches = rt
                .datafusion()
                .query_builder(
                    &format!(
                        "SELECT value FROM {table_name} WHERE created_at > (NOW() - INTERVAL '{}' SECOND);",
                        last_interval.as_secs()
                    ),
                )
                .build()
                .run()
                .await
                .boxed()?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .boxed()?;

            let history = batches
                .iter()
                .filter_map(|b| {
                    if let Some(s) = b.column(0).as_string_opt::<i32>() {
                        Some(s.iter().map(Option::unwrap_or_default).collect::<Vec<_>>())
                    } else {
                        tracing::trace!(
                            "Using tool={}, failed to convert record batch to string",
                            self.name()
                        );
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>();

            Ok(json!(history))
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
