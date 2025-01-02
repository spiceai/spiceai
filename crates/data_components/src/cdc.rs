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

use arrow::{
    array::{Array, ListArray, RecordBatch, StringArray, StructArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use futures::stream::BoxStream;
use snafu::prelude::*;

pub type ChangesStream = BoxStream<'static, Result<ChangeEnvelope, StreamError>>;

#[derive(Debug, Snafu)]
pub enum CommitError {
    #[snafu(display("Unable to commit change: {source}"))]
    UnableToCommitChange {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug, Snafu)]
pub enum ChangeBatchError {
    #[snafu(display("Schema didn't match expected change batch format {detail} schema={schema}"))]
    SchemaMismatch { detail: String, schema: SchemaRef },
}

#[derive(Debug)]
pub enum StreamError {
    Kafka(String),
    SerdeJsonError(String),
    Flight(String),
}

impl std::error::Error for StreamError {}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Kafka(e) => write!(f, "Kafka error: {e}"),
            StreamError::SerdeJsonError(e) => write!(f, "Serde JSON error: {e}"),
            StreamError::Flight(e) => write!(f, "Arrow Flight error: {e}"),
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
}

impl ChangeEnvelope {
    #[must_use]
    pub fn new(change_committer: Box<dyn CommitChange + Send>, change_batch: ChangeBatch) -> Self {
        Self {
            change_committer,
            change_batch,
        }
    }

    pub fn commit(self) -> Result<(), CommitError> {
        self.change_committer.commit()
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

#[derive(Clone)]
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
    #[must_use]
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
            unreachable!("The schema is validated to have a 'primary_keys' field which is a ListArray of StringArray");
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
