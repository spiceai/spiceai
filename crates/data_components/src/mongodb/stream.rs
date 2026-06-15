/*
Copyright 2026 The Spice.ai OSS Authors

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

use crate::cdc::{ChangeBatch, ChangeBatchError, changes_schema};
use arrow::{
    array::{ArrayRef, ListArray, RecordBatch, StringArray, StructArray, new_null_array},
    datatypes::{Field, Schema, SchemaRef},
};
use arrow_buffer::OffsetBuffer;
use datafusion_table_providers::mongodb::{
    Error as MongoDBError,
    utils::{
        arrow::mongo_docs_to_arrow,
        unnest::{UnnestBehavior, UnnestParameters, unnest_bson_documents},
    },
};
use mongodb::{
    bson::Document,
    change_stream::event::{ChangeStreamEvent, OperationType},
};
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum StreamError {
    #[snafu(display("MongoDB change stream event was missing fullDocument for {operation}"))]
    MissingFullDocument { operation: &'static str },

    #[snafu(display("MongoDB change stream delete event was missing documentKey"))]
    MissingDocumentKey,

    #[snafu(display("Unsupported MongoDB change stream operation: {operation}"))]
    UnsupportedOperation { operation: String },

    #[snafu(display("Failed to convert MongoDB change stream documents to Arrow: {source}"))]
    Conversion { source: MongoDBError },

    #[snafu(display("Failed to create MongoDB change batch: {source}"))]
    ChangeBatch { source: ChangeBatchError },

    #[snafu(display("Failed to build MongoDB change batch record: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("MongoDB change stream primary key list is too large"))]
    PrimaryKeyListTooLarge,
}

pub type Result<T, E = StreamError> = std::result::Result<T, E>;

#[must_use]
pub fn default_unnest_parameters(unnest_depth: usize) -> UnnestParameters {
    UnnestParameters {
        behavior: UnnestBehavior::Depth(unnest_depth),
        duplicate_behavior:
            datafusion_table_providers::mongodb::utils::unnest::DuplicateBehavior::Error,
    }
}

pub fn change_events_to_change_batch(
    events: Vec<ChangeStreamEvent<Document>>,
    table_schema: &SchemaRef,
    primary_keys: &[String],
    unnest_parameters: &UnnestParameters,
) -> Result<Option<ChangeBatch>> {
    let mut rows = Vec::with_capacity(events.len());
    let primary_keys = Arc::<[String]>::from(primary_keys.to_vec());
    let empty_primary_keys = Arc::<[String]>::from(Vec::<String>::new());

    for event in events {
        match event.operation_type {
            OperationType::Insert => {
                let document = event.full_document.context(MissingFullDocumentSnafu {
                    operation: "insert",
                })?;
                rows.push(ChangeRow::new("c", Arc::clone(&primary_keys), document));
            }
            OperationType::Update => {
                let document = event.full_document.context(MissingFullDocumentSnafu {
                    operation: "update",
                })?;
                rows.push(ChangeRow::new("u", Arc::clone(&primary_keys), document));
            }
            OperationType::Replace => {
                let document = event.full_document.context(MissingFullDocumentSnafu {
                    operation: "replace",
                })?;
                rows.push(ChangeRow::new("u", Arc::clone(&primary_keys), document));
            }
            OperationType::Delete => {
                let document = event.document_key.context(MissingDocumentKeySnafu)?;
                rows.push(ChangeRow::new("d", Arc::clone(&primary_keys), document));
            }
            OperationType::Drop
            | OperationType::Rename
            | OperationType::DropDatabase
            | OperationType::Invalidate => {
                rows.push(ChangeRow::new(
                    "t",
                    Arc::clone(&empty_primary_keys),
                    Document::new(),
                ));
            }
            operation_type => {
                return UnsupportedOperationSnafu {
                    operation: format!("{operation_type:?}"),
                }
                .fail();
            }
        }
    }

    if rows.is_empty() {
        return Ok(None);
    }

    if rows.iter().any(|row| row.op == "t") {
        return truncate_change_batch(table_schema).map(Some);
    }

    build_change_batch(rows, table_schema, unnest_parameters).map(Some)
}

pub fn truncate_change_batch(table_schema: &SchemaRef) -> Result<ChangeBatch> {
    let data_schema = nullable_clone(table_schema);
    let data_columns = table_schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect::<Vec<_>>();
    let data_struct = StructArray::new(data_schema.fields().clone(), data_columns, None);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec!["t"]));
    let primary_keys = build_primary_keys_array(std::iter::once([].as_slice()))?;
    let wrapper_schema = Arc::new(changes_schema(&data_schema));
    let record = RecordBatch::try_new(
        wrapper_schema,
        vec![op_array, Arc::new(primary_keys), Arc::new(data_struct)],
    )
    .context(ArrowSnafu)?;

    ChangeBatch::try_new(record).context(ChangeBatchSnafu)
}

struct ChangeRow {
    op: &'static str,
    primary_keys: Arc<[String]>,
    document: Document,
}

impl ChangeRow {
    fn new(op: &'static str, primary_keys: Arc<[String]>, document: Document) -> Self {
        Self {
            op,
            primary_keys,
            document,
        }
    }
}

fn build_change_batch(
    rows: Vec<ChangeRow>,
    table_schema: &SchemaRef,
    unnest_parameters: &UnnestParameters,
) -> Result<ChangeBatch> {
    let change_data_schema = nullable_clone(table_schema);
    let row_count = rows.len();
    let ops = rows.iter().map(|row| row.op).collect::<Vec<_>>();
    let primary_keys = build_primary_keys_array(rows.iter().map(|row| row.primary_keys.as_ref()))?;
    let documents = rows.into_iter().map(|row| row.document).collect::<Vec<_>>();
    let documents = match unnest_parameters.behavior {
        UnnestBehavior::Depth(0) => documents,
        _ => unnest_bson_documents(documents, unnest_parameters).context(ConversionSnafu)?,
    };

    let data_batch = mongo_docs_to_arrow(&documents, Arc::clone(&change_data_schema))
        .context(ConversionSnafu)?;
    // `mongo_docs_to_arrow` infers field nullability from the documents (e.g. when
    // every document has `_id`, it produces a non-null `_id` field). The truncate
    // path and the runtime CDC wrapper use the all-nullable `change_data_schema`,
    // so coalescing a TRUNCATE batch with an insert/update batch would fail to
    // concat (Struct with nullable `_id` vs Struct with non-null `_id`). Rebuild
    // the struct with the all-nullable schema fields so every change batch — across
    // all op types — carries an identical struct type.
    let data_array = StructArray::try_new(
        change_data_schema.fields().clone(),
        data_batch.columns().to_vec(),
        None,
    )
    .context(ArrowSnafu)?;
    let op_array: ArrayRef = Arc::new(StringArray::from(ops));
    let wrapper_schema = Arc::new(changes_schema(change_data_schema.as_ref()));

    let record = RecordBatch::try_new(
        wrapper_schema,
        vec![op_array, Arc::new(primary_keys), Arc::new(data_array)],
    )
    .context(ArrowSnafu)?;

    debug_assert_eq!(record.num_rows(), row_count);
    ChangeBatch::try_new(record).context(ChangeBatchSnafu)
}

#[must_use] 
pub fn nullable_clone(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn build_primary_keys_array<'a>(rows: impl Iterator<Item = &'a [String]>) -> Result<ListArray> {
    let mut offsets = vec![0i32];
    let mut values = Vec::new();

    for primary_keys in rows {
        values.extend(primary_keys.iter().map(String::as_str));
        offsets.push(i32::try_from(values.len()).map_err(|_| StreamError::PrimaryKeyListTooLarge)?);
    }

    Ok(ListArray::new(
        Arc::new(Field::new("item", arrow::datatypes::DataType::Utf8, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StringArray::from(values)) as ArrayRef,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::ChangeOperation;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use mongodb::bson::{doc, from_document};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn event(document: Document) -> ChangeStreamEvent<Document> {
        from_document(document).expect("valid change stream event")
    }

    #[test]
    fn converts_insert_update_and_delete_events() {
        let events = vec![
            event(doc! {
                "_id": { "_data": "insert-token" },
                "operationType": "insert",
                "ns": { "db": "db", "coll": "users" },
                "documentKey": { "_id": "1" },
                "fullDocument": { "_id": "1", "name": "Ada" }
            }),
            event(doc! {
                "_id": { "_data": "update-token" },
                "operationType": "update",
                "ns": { "db": "db", "coll": "users" },
                "documentKey": { "_id": "1" },
                "fullDocument": { "_id": "1", "name": "Grace" }
            }),
            event(doc! {
                "_id": { "_data": "delete-token" },
                "operationType": "delete",
                "ns": { "db": "db", "coll": "users" },
                "documentKey": { "_id": "1" }
            }),
        ];

        let batch = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect("change batch should build")
        .expect("batch should not be empty");

        assert_eq!(batch.record.num_rows(), 3);
        assert!(matches!(batch.op(0), ChangeOperation::Create));
        assert!(matches!(batch.op(1), ChangeOperation::Update));
        assert!(matches!(batch.op(2), ChangeOperation::Delete));
        assert_eq!(batch.primary_keys(2), vec!["_id".to_string()]);

        let data = batch.data_batch();
        let names = data
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be utf8");
        assert_eq!(names.value(0), "Ada");
        assert_eq!(names.value(1), "Grace");
        assert!(names.is_null(2));
    }

    #[test]
    fn converts_replace_to_update() {
        let events = vec![event(doc! {
            "_id": { "_data": "replace-token" },
            "operationType": "replace",
            "ns": { "db": "db", "coll": "users" },
            "documentKey": { "_id": "1" },
            "fullDocument": { "_id": "1", "name": "Katherine" }
        })];

        let batch = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect("change batch should build")
        .expect("batch should not be empty");

        assert_eq!(batch.record.num_rows(), 1);
        assert!(matches!(batch.op(0), ChangeOperation::Update));
    }

    #[test]
    fn empty_events_return_no_batch() {
        let batch = change_events_to_change_batch(
            vec![],
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect("empty change batch should not fail");

        assert!(batch.is_none());
    }

    #[test]
    fn converts_drop_to_truncate() {
        let events = vec![event(doc! {
            "_id": { "_data": "drop-token" },
            "operationType": "drop",
            "ns": { "db": "db", "coll": "users" }
        })];

        let batch = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect("change batch should build")
        .expect("batch should not be empty");

        assert!(matches!(batch.op(0), ChangeOperation::Truncate));
        assert!(batch.primary_keys(0).is_empty());
    }

    #[test]
    fn truncate_operation_takes_precedence_over_row_changes() {
        let events = vec![
            event(doc! {
                "_id": { "_data": "insert-token" },
                "operationType": "insert",
                "ns": { "db": "db", "coll": "users" },
                "documentKey": { "_id": "1" },
                "fullDocument": { "_id": "1", "name": "Ada" }
            }),
            event(doc! {
                "_id": { "_data": "invalidate-token" },
                "operationType": "invalidate"
            }),
        ];

        let batch = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect("change batch should build")
        .expect("batch should not be empty");

        assert_eq!(batch.record.num_rows(), 1);
        assert!(matches!(batch.op(0), ChangeOperation::Truncate));
        assert!(batch.primary_keys(0).is_empty());
    }

    #[test]
    fn update_requires_full_document() {
        let events = vec![event(doc! {
            "_id": { "_data": "update-token" },
            "operationType": "update",
            "ns": { "db": "db", "coll": "users" },
            "documentKey": { "_id": "1" }
        })];

        let error = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect_err("missing fullDocument should fail");

        assert!(matches!(error, StreamError::MissingFullDocument { .. }));
    }

    #[test]
    fn delete_requires_document_key() {
        let events = vec![event(doc! {
            "_id": { "_data": "delete-token" },
            "operationType": "delete",
            "ns": { "db": "db", "coll": "users" }
        })];

        let error = change_events_to_change_batch(
            events,
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect_err("missing documentKey should fail");

        assert!(matches!(error, StreamError::MissingDocumentKey));
    }

    #[test]
    fn unsupported_operations_fail() {
        let mut unsupported_event = event(doc! {
            "_id": { "_data": "future-token" },
            "operationType": "insert",
            "ns": { "db": "db", "coll": "users" }
        });
        unsupported_event.operation_type = OperationType::Other("future".to_string());

        let error = change_events_to_change_batch(
            vec![unsupported_event],
            &schema(),
            &["_id".to_string()],
            &default_unnest_parameters(0),
        )
        .expect_err("unsupported operation should fail");

        assert!(matches!(error, StreamError::UnsupportedOperation { .. }));
    }
}
