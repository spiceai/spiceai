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

use arrow::array::AsArray;
use async_trait::async_trait;
use datafusion::{
    prelude::{col, lit},
    scalar::ScalarValue,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::ResultExt;
use std::{borrow::Cow, io, sync::Arc};
use tracing_futures::Instrument;

use crate::{
    Runtime,
    tools::{SpiceModelTool, utils::parameters},
};

use super::memory_table_name;

const DEFAULT_MEMORY_LOAD_LIMIT: usize = 100;
const MAX_MEMORY_LOAD_LIMIT: usize = 500;
const MAX_MEMORY_LOAD_OFFSET: usize = 10_000;

const fn default_memory_load_limit() -> usize {
    DEFAULT_MEMORY_LOAD_LIMIT
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct LoadMemoryParams {
    /// Retrieve memories created in the 'last' interval as a human-readable duration string, e.g. "1h", "2m30s".
    pub last: String,

    /// Maximum number of memories to return.
    #[serde(default = "default_memory_load_limit")]
    pub limit: usize,

    /// Number of memories to skip for pagination.
    #[serde(default)]
    pub offset: usize,
}

fn validate_load_memory_params(
    params: &LoadMemoryParams,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if params.limit == 0 || params.limit > MAX_MEMORY_LOAD_LIMIT {
        return Err(format!(
            "Invalid limit. Provide a value between 1 and {MAX_MEMORY_LOAD_LIMIT}"
        )
        .into());
    }

    if params.offset > MAX_MEMORY_LOAD_OFFSET {
        return Err(
            format!("Invalid offset. Maximum allowed offset is {MAX_MEMORY_LOAD_OFFSET}").into(),
        );
    }

    Ok(())
}

pub struct LoadMemoryTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
}

impl LoadMemoryTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            rt,
            name: name.unwrap_or("load_memory").to_string(),
            description: description
                .unwrap_or("Retrieve thoughts previously persisted via `store_memory`. Call this at the start of a conversation, or whenever the user references prior context (e.g. 'as we discussed', 'like last time'), to recover relevant background. `last` is a duration string (e.g. '1h', '24h', '7d') bounding how far back to look. Use `limit` (1-500, default 100) and `offset` for pagination. Returns the matching memory entries with their timestamps.")
                .to_string(),
        }
    }
}

impl From<&Arc<Runtime>> for LoadMemoryTool {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self::new(Arc::clone(rt), None, None)
    }
}

#[async_trait]
impl SpiceModelTool for LoadMemoryTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<LoadMemoryParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::load_memory", tool = self.name().to_string(), input = arg);

        let table_name = memory_table_name(&self.rt).await?;
        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: LoadMemoryParams = serde_json::from_str(arg).boxed()?;
            validate_load_memory_params(&params)?;
            let last_interval = fundu::parse_duration(params.last.as_str()).boxed()?;
            let last_interval_secs = i64::try_from(last_interval.as_secs()).boxed()?;
            let created_after = chrono::Utc::now()
                .timestamp()
                .checked_sub(last_interval_secs)
                .ok_or_else(|| io::Error::other("Failed to compute memory cutoff timestamp"))?;

            let Some(provider) = self.rt.datafusion().get_table(&table_name).await else {
                return Err(
                    io::Error::other(format!("Memory table not found: {table_name}")).into(),
                );
            };

            let batches = self
                .rt
                .datafusion()
                .ctx
                .read_table(provider)
                .boxed()?
                .filter(
                    col("created_at")
                        .gt(lit(ScalarValue::TimestampSecond(Some(created_after), None))),
                )
                .boxed()?
                .sort(vec![
                    col("created_at").sort(false, false),
                    col("id").sort(false, false),
                ])
                .boxed()?
                .limit(params.offset, Some(params.limit))
                .boxed()?
                .select(vec![col("value")])
                .boxed()?
                .collect()
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

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MEMORY_LOAD_LIMIT, LoadMemoryParams, MAX_MEMORY_LOAD_LIMIT, MAX_MEMORY_LOAD_OFFSET,
        default_memory_load_limit, validate_load_memory_params,
    };

    #[test]
    fn test_default_memory_load_limit_matches_constant() {
        assert_eq!(default_memory_load_limit(), DEFAULT_MEMORY_LOAD_LIMIT);
    }

    #[test]
    fn test_load_memory_rejects_zero_limit() {
        let params = LoadMemoryParams {
            last: "1h".to_string(),
            limit: 0,
            offset: 0,
        };

        let err = validate_load_memory_params(&params).expect_err("must reject zero limit");
        assert!(err.to_string().contains(&MAX_MEMORY_LOAD_LIMIT.to_string()));
    }

    #[test]
    fn test_load_memory_rejects_excessive_limit() {
        let params = LoadMemoryParams {
            last: "1h".to_string(),
            limit: MAX_MEMORY_LOAD_LIMIT + 1,
            offset: 0,
        };

        let err = validate_load_memory_params(&params).expect_err("must reject large limit");
        assert!(err.to_string().contains(&MAX_MEMORY_LOAD_LIMIT.to_string()));
    }

    #[test]
    fn test_load_memory_rejects_excessive_offset() {
        let params = LoadMemoryParams {
            last: "1h".to_string(),
            limit: DEFAULT_MEMORY_LOAD_LIMIT,
            offset: MAX_MEMORY_LOAD_OFFSET + 1,
        };

        let err = validate_load_memory_params(&params).expect_err("must reject large offset");
        assert!(
            err.to_string()
                .contains(&MAX_MEMORY_LOAD_OFFSET.to_string())
        );
    }
}
