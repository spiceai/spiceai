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
use uuid::Uuid;

use crate::{Runtime, component::validate_identifier};

pub mod builtin;
pub mod catalog;
pub mod engine;
pub mod load;
pub mod store;

pub use engine::{MemoryEngine, get_memory_engine};

pub static MEMORY_TABLE_SCHEMA: std::sync::LazyLock<SchemaRef> = std::sync::LazyLock::new(|| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow_schema::DataType;

    #[test]
    fn test_memory_table_schema_fields() {
        let schema = &*MEMORY_TABLE_SCHEMA;
        assert_eq!(schema.fields().len(), 4);

        let id_field = schema.field(0);
        assert_eq!(id_field.name(), "id");
        assert_eq!(id_field.data_type(), &DataType::Utf8);
        assert!(!id_field.is_nullable());

        let value_field = schema.field(1);
        assert_eq!(value_field.name(), "value");
        assert_eq!(value_field.data_type(), &DataType::Utf8);
        assert!(!value_field.is_nullable());

        let created_by_field = schema.field(2);
        assert_eq!(created_by_field.name(), "created_by");
        assert_eq!(created_by_field.data_type(), &DataType::Utf8);
        assert!(created_by_field.is_nullable());

        let created_at_field = schema.field(3);
        assert_eq!(created_at_field.name(), "created_at");
        assert_eq!(
            created_at_field.data_type(),
            &DataType::Timestamp(TimeUnit::Second, None)
        );
        assert!(!created_at_field.is_nullable());
    }

    #[test]
    fn test_try_from_single_element() {
        let elements = vec![MemoryTableElement {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                .expect("valid uuid"),
            value: "test memory".to_string(),
            created_by: Some("user1".to_string()),
            created_at: 1704067200, // 2024-01-01 00:00:00 UTC
        }];

        let batch = try_from(&elements).expect("should create record batch");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema(), *MEMORY_TABLE_SCHEMA);

        // Verify id column
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id should be string array");
        assert_eq!(
            id_array.value(0),
            "550e8400-e29b-41d4-a716-446655440000"
        );

        // Verify value column
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value should be string array");
        assert_eq!(value_array.value(0), "test memory");

        // Verify created_by column
        let created_by_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("created_by should be string array");
        assert_eq!(created_by_array.value(0), "user1");
        assert!(!created_by_array.is_null(0));

        // Verify created_at column
        let created_at_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("created_at should be timestamp array");
        assert_eq!(created_at_array.value(0), 1_704_067_200);
    }

    #[test]
    fn test_try_from_multiple_elements() {
        let elements = vec![
            MemoryTableElement {
                id: Uuid::new_v4(),
                value: "first memory".to_string(),
                created_by: Some("user1".to_string()),
                created_at: 1_704_067_200,
            },
            MemoryTableElement {
                id: Uuid::new_v4(),
                value: "second memory".to_string(),
                created_by: None,
                created_at: 1_704_153_600,
            },
            MemoryTableElement {
                id: Uuid::new_v4(),
                value: "third memory".to_string(),
                created_by: Some("user2".to_string()),
                created_at: 1_704_240_000,
            },
        ];

        let batch = try_from(&elements).expect("should create record batch");

        assert_eq!(batch.num_rows(), 3);

        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value should be string array");
        assert_eq!(value_array.value(0), "first memory");
        assert_eq!(value_array.value(1), "second memory");
        assert_eq!(value_array.value(2), "third memory");

        // Check nullable created_by field
        let created_by_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("created_by should be string array");
        assert_eq!(created_by_array.value(0), "user1");
        assert!(created_by_array.is_null(1));
        assert_eq!(created_by_array.value(2), "user2");
    }

    #[test]
    fn test_try_from_empty_slice() {
        let elements: Vec<MemoryTableElement> = vec![];

        let batch = try_from(&elements).expect("should create empty record batch");

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema(), *MEMORY_TABLE_SCHEMA);
    }

    #[test]
    fn test_try_from_with_special_characters() {
        let elements = vec![MemoryTableElement {
            id: Uuid::new_v4(),
            value: "Memory with 'quotes' and \"double quotes\" and\nnewlines".to_string(),
            created_by: Some("user@example.com".to_string()),
            created_at: 1_704_067_200,
        }];

        let batch = try_from(&elements).expect("should create record batch with special chars");

        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value should be string array");
        assert_eq!(
            value_array.value(0),
            "Memory with 'quotes' and \"double quotes\" and\nnewlines"
        );
    }

    #[test]
    fn test_try_from_with_unicode() {
        let elements = vec![MemoryTableElement {
            id: Uuid::new_v4(),
            value: "Memory with unicode: 你好 🎉 émoji".to_string(),
            created_by: Some("用户".to_string()),
            created_at: 1_704_067_200,
        }];

        let batch = try_from(&elements).expect("should create record batch with unicode");

        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value should be string array");
        assert_eq!(value_array.value(0), "Memory with unicode: 你好 🎉 émoji");

        let created_by_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("created_by should be string array");
        assert_eq!(created_by_array.value(0), "用户");
    }
}
