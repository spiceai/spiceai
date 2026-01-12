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

//! Builtin memory engine using SQL-based storage.

use arrow::array::AsArray;
use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    Runtime,
    dataupdate::{DataUpdate, UpdateType},
};

use super::{MemoryTableElement, engine::MemoryEngine, memory_table_name, try_from};

/// Builtin memory engine that uses a SQL table for storage.
///
/// This engine stores memories in a `DataFusion` table configured with a memory connector.
/// It requires a dataset with `from: memory:` to be configured in the spicepod.
pub struct BuiltinMemoryEngine {
    rt: Arc<Runtime>,
}

impl BuiltinMemoryEngine {
    #[must_use]
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self { rt }
    }
}

#[async_trait]
impl MemoryEngine for BuiltinMemoryEngine {
    async fn store(
        &self,
        value: &str,
        created_by: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let table_name = memory_table_name(&self.rt).await?;

        let batch = try_from(&[MemoryTableElement {
            id: Uuid::new_v4(),
            value: value.to_string(),
            created_by: created_by.map(ToString::to_string),
            created_at: Utc::now().timestamp(),
        }])?;

        let data_update = DataUpdate {
            schema: batch.schema(),
            data: vec![batch],
            update_type: UpdateType::Append,
        };

        self.rt
            .datafusion()
            .write_data(&table_name, data_update)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        Ok(json!({
            "success": true,
            "message": "Memory stored successfully"
        }))
    }

    async fn load(
        &self,
        last_interval: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let table_name = memory_table_name(&self.rt).await?;
        let last_duration = fundu::parse_duration(last_interval).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Failed to parse interval '{last_interval}': {e}").into()
            },
        )?;

        let batches = self
            .rt
            .datafusion()
            .query_builder(&format!(
                "SELECT value FROM {table_name} WHERE created_at > (NOW() - INTERVAL '{}' SECOND);",
                last_duration.as_secs()
            ))
            .build()
            .run()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
            .data
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        let history = batches
            .iter()
            .filter_map(|b| {
                if let Some(s) = b.column(0).as_string_opt::<i32>() {
                    Some(s.iter().map(Option::unwrap_or_default).collect::<Vec<_>>())
                } else {
                    tracing::trace!("Failed to convert record batch to string for load_memory");
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        Ok(json!(history))
    }
}
