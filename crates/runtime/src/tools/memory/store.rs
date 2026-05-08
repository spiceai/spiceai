/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

const MAX_MEMORY_THOUGHTS_PER_REQUEST: usize = 128;
const MAX_MEMORY_THOUGHT_BYTES: usize = 4 * 1024;
const MAX_MEMORY_TOTAL_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct StoreMemoryParams {
    /// A list of details to persist
    thoughts: Vec<String>,
}

fn validate_store_memory_params(
    params: &StoreMemoryParams,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if params.thoughts.is_empty() {
        return Err("At least one thought must be provided".into());
    }

    if params.thoughts.len() > MAX_MEMORY_THOUGHTS_PER_REQUEST {
        return Err(format!(
            "Too many thoughts provided. Maximum allowed per request is {MAX_MEMORY_THOUGHTS_PER_REQUEST}"
        )
        .into());
    }

    let mut total_bytes = 0usize;
    for thought in &params.thoughts {
        let thought_bytes = thought.len();
        if thought_bytes > MAX_MEMORY_THOUGHT_BYTES {
            return Err(format!(
                "Thought exceeds maximum size of {MAX_MEMORY_THOUGHT_BYTES} bytes"
            )
            .into());
        }

        total_bytes += thought_bytes;
        if total_bytes > MAX_MEMORY_TOTAL_BYTES {
            return Err(format!(
                "Combined thoughts exceed maximum size of {MAX_MEMORY_TOTAL_BYTES} bytes"
            )
            .into());
        }
    }

    Ok(())
}

impl From<StoreMemoryParams> for Vec<MemoryTableElement> {
    fn from(val: StoreMemoryParams) -> Self {
        val.thoughts
            .iter()
            .map(|thought| MemoryTableElement {
                id: uuid::Uuid::now_v7(),
                value: thought.clone(),
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
            description: description.unwrap_or("Persist short notes ('thoughts') from the current conversation so they can be retrieved in future sessions via `load_memory`. Call this when the user states a durable preference, fact, identity, or instruction worth remembering across conversations; do not record transient query inputs or tool results. Pass `thoughts` as a list of concise strings (each up to 4 KiB, at most 128 entries per call, 64 KiB total).").to_string(),
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
            validate_store_memory_params(&params)?;

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

#[cfg(test)]
mod tests {
    use super::{
        MAX_MEMORY_THOUGHT_BYTES, MAX_MEMORY_THOUGHTS_PER_REQUEST, MAX_MEMORY_TOTAL_BYTES,
        StoreMemoryParams, validate_store_memory_params,
    };

    #[test]
    fn test_store_memory_rejects_empty_thoughts() {
        let params = StoreMemoryParams { thoughts: vec![] };

        let err = validate_store_memory_params(&params).expect_err("must reject empty thoughts");
        assert_eq!(err.to_string(), "At least one thought must be provided");
    }

    #[test]
    fn test_store_memory_rejects_too_many_thoughts() {
        let params = StoreMemoryParams {
            thoughts: vec!["ok".to_string(); MAX_MEMORY_THOUGHTS_PER_REQUEST + 1],
        };

        let err = validate_store_memory_params(&params).expect_err("must reject too many thoughts");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_THOUGHTS_PER_REQUEST.to_string())
        );
    }

    #[test]
    fn test_store_memory_rejects_oversized_thought() {
        let params = StoreMemoryParams {
            thoughts: vec!["a".repeat(MAX_MEMORY_THOUGHT_BYTES + 1)],
        };

        let err = validate_store_memory_params(&params)
            .expect_err("must reject oversized individual thought");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_THOUGHT_BYTES.to_string())
        );
    }

    #[test]
    fn test_store_memory_rejects_oversized_total_payload() {
        let chunk = "a".repeat(MAX_MEMORY_THOUGHT_BYTES);
        let chunk_count = (MAX_MEMORY_TOTAL_BYTES / MAX_MEMORY_THOUGHT_BYTES) + 1;
        let params = StoreMemoryParams {
            thoughts: vec![chunk; chunk_count],
        };

        let err = validate_store_memory_params(&params)
            .expect_err("must reject oversized combined payload");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_TOTAL_BYTES.to_string())
        );
    }
}
