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

use arrow::array::RecordBatch;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::{
    Runtime,
    dataupdate::{DataUpdate, UpdateType},
    tools::{SpiceModelTool, utils::parameters},
};

use super::{MemoryTableElement, memory_table_name, try_from};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct StoreMemoryParams {
    /// A list of details to persist
    thoughts: Vec<String>,
}

impl From<StoreMemoryParams> for Vec<MemoryTableElement> {
    fn from(val: StoreMemoryParams) -> Self {
        val.thoughts
            .iter()
            .map(|thought| MemoryTableElement {
                id: uuid::Uuid::now_v7(),
                value: thought.to_string(),
                created_by: None,
                created_at: chrono::Utc::now().timestamp(),
            })
            .collect()
    }
}

pub struct StoreMemoryTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
}

impl StoreMemoryTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            rt,
            name: name.unwrap_or("store_memory").to_string(),
            description: description.unwrap_or("Record any details from 'user' messages that are worth remembering for future conversations.").to_string(),
        }
    }
}
impl From<&Arc<Runtime>> for StoreMemoryTool {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self::new(Arc::clone(rt), None, None)
    }
}

#[async_trait]
impl SpiceModelTool for StoreMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<StoreMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::store_memory", tool = self.name().to_string(), input = arg);
        let table_name = memory_table_name(&self.rt).await?;
        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: StoreMemoryParams = serde_json::from_str(arg).boxed()?;

            let elements: Vec<MemoryTableElement> = params.into();
            let batch: RecordBatch = try_from(&elements).boxed()?;

            self.rt
                .datafusion()
                .write_data(
                    &table_name,
                    DataUpdate {
                        schema: batch.schema(),
                        data: vec![batch],
                        update_type: UpdateType::Append,
                    },
                )
                .await
                .boxed()?;
            Ok(Value::Null)
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
