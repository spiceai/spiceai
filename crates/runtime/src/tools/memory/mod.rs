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

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampSecondArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::sql::TableReference;
use once_cell::sync::Lazy;
use uuid::Uuid;

use crate::{component::validate_identifier, Runtime};

pub mod catalog;
pub mod load;
pub mod store;

pub static MEMORY_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("created_by", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
    ])
    .into()
});

pub struct MemoryTableElement {
    pub id: Uuid,
    pub value: String,
    pub created_by: Option<String>,
    pub created_at: i64, // Unix timestamp in Seconds
}

pub fn try_from(data: &[MemoryTableElement]) -> Result<RecordBatch, ArrowError> {
    let ids = StringArray::from_iter_values(data.iter().map(|d| d.id.to_string()));
    let values = StringArray::from_iter_values(data.iter().map(|d| d.value.as_str()));
    let created_by = data
        .iter()
        .map(|d| d.created_by.as_deref())
        .collect::<StringArray>();
    let created_at: TimestampSecondArray =
        TimestampSecondArray::from(data.iter().map(|e| e.created_at).collect::<Vec<_>>());

    RecordBatch::try_new(
        Arc::clone(&MEMORY_TABLE_SCHEMA),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(values) as ArrayRef,
            Arc::new(created_by) as ArrayRef,
            Arc::new(created_at) as ArrayRef,
        ],
    )
}

/// Determine the name of the table to use to store/load memories.
async fn memory_table_name(
    rt: &Arc<Runtime>,
) -> Result<TableReference, Box<dyn std::error::Error + Send + Sync>> {
    let app_lock = rt.app.read().await;
    let Some(app) = app_lock.as_deref() else {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "App not initialized",
        ));
    };

    match app.datasets_of_connector_type("memory").split_first() {
        Some((table, t)) => {
            if !t.is_empty() {
                tracing::warn!(
                    "Multiple memory tables found, using the first one: {}",
                    table
                );
            }
            if validate_identifier(table.as_str()).is_err() {
                return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Invalid memory table name: '{table}'"
                )));
            }
            Ok(TableReference::parse_str(table.as_str()))
        }
        None => Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "No memory table found",
        )),
    }
}
