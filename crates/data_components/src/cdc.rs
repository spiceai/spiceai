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

use std::{fmt::Display, sync::Arc};

use arrow::error::ArrowError;
use arrow::{
    array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use arrow_buffer::OffsetBuffer;
use futures::stream::BoxStream;
use snafu::prelude::*;

pub type ChangesStream = BoxStream<'static, Result<ChangeEnvelope, StreamError>>;

#[derive(Debug, Snafu)]
pub enum CommitError {
    #[snafu(display("Failed to commit CDC change to dataset: {source}"))]
    UnableToCommitChange {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug, Snafu)]
pub enum ChangeBatchError {
    #[snafu(display("Schema didn't match expected change batch format {detail} schema={schema}"))]
    SchemaMismatch { detail: String, schema: SchemaRef },
    #[snafu(display("Failed to process change data capture update: {source}"))]
    Arrow { source: ArrowError },
}

#[derive(Debug)]
pub enum StreamError {
    #[cfg(any(feature = "debezium", feature = "kafka"))]
    /// Error from the Kafka client, such as failure to consume messages.
    Kafka(crate::kafka::Error),
    /// Error from Serde JSON, such as failure to serialize or deserialize data.
    SerdeJsonError(String),
    /// Error from Arrow Flight, such as failure during streaming or subscription.
    Flight(String),
    /// Error from the Arrow library, such as failure during batch processing or manipulation.
    Arrow(String),
    /// External error not originating from `ChangesStream` core logic, such as index processing failure.
    External(String),
    #[cfg(feature = "dynamodb")]
    /// Error from `DynamoDB`, such as failure during streaming or subscription.
    DynamoDB(crate::dynamodb::stream::StreamError),
}

impl std::error::Error for StreamError {}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(feature = "debezium", feature = "kafka"))]
            StreamError::Kafka(e) => write!(f, "Kafka error: {e}"),
            StreamError::SerdeJsonError(e) => write!(f, "Serde JSON error: {e}"),
            StreamError::Flight(e) => write!(f, "Arrow Flight error: {e}"),
            StreamError::Arrow(e) => write!(f, "Arrow error: {e}"),
            StreamError::External(e) => write!(f, "External error: {e}"),
            #[cfg(feature = "dynamodb")]
            StreamError::DynamoDB(e) => write!(f, "DynamoDB error: {e}"),
        }
    }
}

/// Allows to commit a change that has been processed.
pub trait CommitChange {
    fn commit(&self) -> Result<(), CommitError>;
}

pub struct ChangeEnvelope {
    change_committer: Box<dyn CommitChange + Send>,
    pub change_batch: ChangeBatch,
    is_dataset_ready: bool,
}

impl ChangeEnvelope {
    #[must_use]
    pub fn new(
        change_committer: Box<dyn CommitChange + Send>,
        change_batch: ChangeBatch,
        is_dataset_ready: bool,
    ) -> Self {
        Self {
            change_committer,
            change_batch,
            is_dataset_ready,
        }
    }

    pub fn commit(self) -> Result<(), CommitError> {
        self.change_committer.commit()
    }

    #[must_use]
    pub fn into_parts(self) -> (Box<dyn CommitChange + Send>, ChangeBatch, bool) {
        (
            self.change_committer,
            self.change_batch,
            self.is_dataset_ready,
        )
    }

    #[must_use]
    pub fn from_parts(
        change_committer: Box<dyn CommitChange + Send>,
        change_batch: ChangeBatch,
        is_dataset_ready: bool,
    ) -> Self {
        Self {
            change_committer,
            change_batch,
            is_dataset_ready,
        }
    }

    #[must_use]
    pub fn is_dataset_ready(&self) -> bool {
        self.is_dataset_ready
    }
}

/// The Arrow schema that represents a `ChangeEvent`
#[must_use]
pub fn changes_schema(table_schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("op", DataType::Utf8, false),
        Field::new(
            "primary_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new(
            "data",
            DataType::Struct(table_schema.fields().clone()),
            true,
        ),
    ])
}

#[derive(Clone, Debug)]
pub struct ChangeBatch {
    pub record: RecordBatch,
    op_idx: usize,
    primary_keys_idx: usize,
    data_idx: usize,
}

pub enum ChangeOperation {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
    Unknown(String),
}

impl From<&str> for ChangeOperation {
    fn from(op: &str) -> Self {
        match op {
            "c" => Self::Create,
            "u" => Self::Update,
            "d" => Self::Delete,
            "r" => Self::Read,
            "t" => Self::Truncate,
            _ => Self::Unknown(op.to_string()),
        }
    }
}

impl Display for ChangeOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create => write!(f, "c"),
            Self::Update => write!(f, "u"),
            Self::Delete => write!(f, "d"),
            Self::Read => write!(f, "r"),
            Self::Truncate => write!(f, "t"),
            Self::Unknown(op) => write!(f, "Unknown({op})"),
        }
    }
}

impl ChangeBatch {
    pub fn try_new(record: RecordBatch) -> Result<Self, ChangeBatchError> {
        let schema = record.schema();
        Self::validate_schema(Arc::clone(&schema))?;

        let Some((op_idx, _)) = schema.column_with_name("op") else {
            unreachable!("The schema is validated to have an 'op' field")
        };
        let Some((primary_keys_idx, _)) = schema.column_with_name("primary_keys") else {
            unreachable!("The schema is validated to have a 'primary_keys' field")
        };
        let Some((data_idx, _)) = schema.column_with_name("data") else {
            unreachable!("The schema is validated to have a 'data' field")
        };

        Ok(Self {
            record,
            op_idx,
            primary_keys_idx,
            data_idx,
        })
    }

    #[must_use]
    pub fn op(&self, row: usize) -> ChangeOperation {
        let Some(op_col) = self
            .record
            .column(self.op_idx)
            .as_any()
            .downcast_ref::<StringArray>()
        else {
            unreachable!("The schema is validated to have an 'op' field which is a StringArray");
        };
        op_col.value(row).into()
    }

    #[must_use]
    pub fn primary_keys(&self, row: usize) -> Vec<String> {
        let Some(primary_keys_col) = self
            .record
            .column(self.primary_keys_idx)
            .as_any()
            .downcast_ref::<ListArray>()
        else {
            unreachable!(
                "The schema is validated to have a 'primary_keys' field which is a ListArray"
            );
        };
        let primary_keys_values = primary_keys_col.value(row);
        let Some(primary_keys_values) = primary_keys_values.as_any().downcast_ref::<StringArray>()
        else {
            unreachable!(
                "The schema is validated to have a 'primary_keys' field which is a ListArray of StringArray"
            );
        };
        let num_keys = primary_keys_values.len();
        let mut primary_keys: Vec<String> = Vec::with_capacity(num_keys);
        for i in 0..num_keys {
            primary_keys.push(primary_keys_values.value(i).to_string());
        }

        primary_keys
    }

    #[must_use]
    pub fn data(&self, row: usize) -> RecordBatch {
        let Some(data_col) = self
            .record
            .column(self.data_idx)
            .as_any()
            .downcast_ref::<StructArray>()
        else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        data_col.slice(row, 1).into()
    }

    #[must_use]
    pub fn data_batch(&self) -> RecordBatch {
        let data_col = self.record.column(self.data_idx);
        let Some(data_array) = data_col.as_any().downcast_ref::<StructArray>() else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        let DataType::Struct(fields) = data_array.data_type() else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        let Ok(record_batch) = RecordBatch::try_new(
            Arc::new(Schema::new(fields.clone())),
            data_array.columns().to_vec(),
        ) else {
            unreachable!("The schema is validated to have a 'data' field which is a StructArray");
        };
        record_batch
    }

    fn validate_schema(schema: SchemaRef) -> Result<(), ChangeBatchError> {
        let Some(data_col) = schema.fields().iter().find(|field| field.name() == "data") else {
            return SchemaMismatchSnafu {
                detail: "Missing 'data' field",
                schema,
            }
            .fail();
        };

        let data_schema = match data_col.data_type() {
            DataType::Struct(fields) => Schema::new(fields.clone()),
            _ => {
                return SchemaMismatchSnafu {
                    detail: "Unexpected data type for 'data' field, expected Struct",
                    schema,
                }
                .fail();
            }
        };

        let expected_schema = changes_schema(&data_schema);
        if *schema != expected_schema {
            return SchemaMismatchSnafu {
                detail: "Schema didn't match expected change batch format",
                schema,
            }
            .fail();
        }

        Ok(())
    }
}

/// Wraps an arbitrary data `RecordBatch` as a `ChangeBatch` with "create" operations.
pub fn wrap_data_as_change_batch(
    table_schema: &SchemaRef,
    data: &RecordBatch,
) -> Result<ChangeBatch, ChangeBatchError> {
    let num_rows = data.num_rows();
    let schema = changes_schema(table_schema);

    // 1) op column ("create" operations)
    let op_array = Arc::new(arrow::array::StringArray::from(vec![
        "c".to_string();
        num_rows
    ]));

    // 2) Dummy primary_keys: List<Utf8> with EMPTY LIST per row
    // Offsets must be length = num_rows + 1. All zeros => [] for every row.
    let offsets = vec![0i32; num_rows + 1];
    let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let primary_keys_array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        values,
        None, // no validity bitmap (all non-null lists)
    ));

    // 3) data: Struct matching the input batch's schema/columns
    let data_array = Arc::new(StructArray::new(
        data.schema().fields().clone(),
        data.columns().to_vec(),
        None,
    ));

    let columns = vec![op_array, primary_keys_array, data_array];
    let record_batch = RecordBatch::try_new(schema.into(), columns).context(ArrowSnafu)?;

    ChangeBatch::try_new(record_batch)
}

pub fn replace_change_batch_data(
    new_data: &RecordBatch,
    change: &ChangeBatch,
) -> Result<ChangeBatch, ChangeBatchError> {
    let schema = changes_schema(&new_data.schema());

    let cols = change
        .record
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if f.name() == "data" {
                Arc::new(StructArray::new(
                    new_data.schema().fields().clone(),
                    new_data.columns().to_vec(),
                    None,
                )) as Arc<dyn Array>
            } else {
                match change.record.column_by_name(f.name()) {
                    Some(column) => Arc::clone(column),
                    None => unreachable!("Column {} must exist", f.name()),
                }
            }
        })
        .collect();

    RecordBatch::try_new(schema.into(), cols)
        .map_err(|source| ChangeBatchError::Arrow { source })
        .and_then(ChangeBatch::try_new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_wrap_batch_as_change_batch() {
        // Create a test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create test data
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
        let data_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, name_array])
            .expect("to create data batch");

        let change_batch =
            wrap_data_as_change_batch(&schema, &data_batch).expect("to create change batch");

        let record = &change_batch.record;

        // Verify the schema has the expected fields
        assert_eq!(record.schema().fields().len(), 3);
        // Verify the number of rows
        assert_eq!(record.num_rows(), 3);

        // Verify the op column
        let op_column = record
            .column_by_name("op")
            .expect("op column exists")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op column is StringArray");
        for i in 0..3 {
            assert_eq!(op_column.value(i), "c");
        }

        // Verify the primary_keys column (should be empty lists)
        let pk_column = record
            .column_by_name("primary_keys")
            .expect("primary_keys column exists")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("primary_keys column is ListArray");
        assert_eq!(pk_column.len(), 3);
        for i in 0..3 {
            assert_eq!(pk_column.value_length(i), 0);
        }

        // Verify the data column
        let data_column = record
            .column_by_name("data")
            .expect("data column exists")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("data column is StructArray");
        assert_eq!(data_column.len(), 3);
        assert_eq!(data_column.num_columns(), 2);
    }
}
