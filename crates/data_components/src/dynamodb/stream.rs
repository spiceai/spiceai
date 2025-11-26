/*
Copyright 2025 The Spice.ai OSS Authors

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
use super::{Error, Result};
use crate::arrow::struct_builder::StructBuilder;
use crate::cdc::{
    ChangeBatch, ChangeBatchError, ChangeEnvelope, CommitChange, CommitError, changes_schema,
};
use crate::dynamodb::arrow::append_item_to_struct_builder;
use crate::dynamodb::unnest::unnest_dynamodb_item;
use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use arrow_array::builder::{ArrayBuilder, ListBuilder, StringBuilder};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use aws_sdk_dynamodbstreams::types::AttributeValue as StreamsAttributeValue;
use aws_sdk_dynamodbstreams::types::OperationType;
use dynamodb_streams::StreamResult;
use snafu::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum StreamError {
    #[snafu(display("Failed to receive DynamoDB Stream record: {source}"))]
    FailedToReceiveMessage { source: dynamodb_streams::Error },

    #[snafu(display("Unable to downcast ArrayBuilder"))]
    DowncastBuilder,

    #[snafu(display("Failed to unnest DynamoDB Stream record: {source}"))]
    FailedToUnnest { source: Error },

    #[snafu(display("Failed to deserialize DynamoDB Stream record: {source}"))]
    FailedToCreateChangeBatch { source: ChangeBatchError },

    #[snafu(display("Failed to add item to struct: {source}"))]
    FailedToAddItemToStruct { source: Error },
}

pub fn process_batch(
    batch: StreamResult,
    table_schema: &Arc<Schema>,
    primary_keys: &[String],
    unnest_depth: Option<usize>,
    time_format: &str,
) -> Result<ChangeEnvelope, StreamError> {
    let batch = batch.context(FailedToReceiveMessageSnafu)?.records;

    let changes_schema = changes_schema(table_schema).clone();

    let mut changes_struct_builder =
        StructBuilder::from_fields(changes_schema.fields().clone(), batch.len());

    for record in batch {
        let (op_str, item_data) = match (&record.event_name, &record.dynamodb) {
            (Some(event_name), Some(dynamodb)) => match event_name {
                OperationType::Insert | OperationType::Modify => {
                    let Some(item) = &dynamodb.new_image else {
                        continue;
                    };
                    let streams_item = streams_to_dynamodb_item(item.clone());

                    let (unnested_streams_item, _) = match unnest_depth {
                        None => (streams_item, HashSet::new()),
                        Some(depth) => unnest_dynamodb_item(&streams_item, depth)
                            .context(FailedToUnnestSnafu)?,
                    };

                    let op = if matches!(event_name, OperationType::Insert) {
                        "c"
                    } else {
                        "u"
                    };

                    (op, unnested_streams_item)
                }
                OperationType::Remove => {
                    let Some(keys_item) = &dynamodb.keys else {
                        continue;
                    };
                    let streams_keys_item = streams_to_dynamodb_item(keys_item.clone());
                    ("d", streams_keys_item)
                }
                operation => {
                    tracing::debug!("Unexpected OperationType from DynamoDB Streams: {operation}",);
                    continue;
                }
            },
            _ => continue,
        };

        // Append row to changes struct
        changes_struct_builder.append(true);

        // Populate each field in the changes schema
        for (idx, field) in changes_schema.fields().iter().enumerate() {
            let field_builder = changes_struct_builder.field_builder_array(idx);

            match field.name().as_str() {
                "op" => {
                    let str_builder = downcast_builder::<StringBuilder>(field_builder)
                        .context(DowncastBuilderSnafu)?;
                    str_builder.append_value(op_str);
                }
                "primary_keys" => {
                    let list_builder =
                        downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(field_builder)
                            .context(DowncastBuilderSnafu)?;
                    if primary_keys.is_empty() {
                        list_builder.append(false);
                    } else {
                        let str_builder = downcast_builder::<StringBuilder>(list_builder.values())
                            .context(DowncastBuilderSnafu)?;
                        for key in primary_keys {
                            str_builder.append_value(key);
                        }
                        list_builder.append(true);
                    }
                }
                "data" => {
                    let data_struct_builder = downcast_builder::<StructBuilder>(field_builder)
                        .context(DowncastBuilderSnafu)?;
                    append_item_to_struct_builder(&item_data, data_struct_builder, time_format)
                        .context(FailedToAddItemToStructSnafu)?;
                }
                _ => unreachable!("Unexpected field in changes schema {}", field.name()),
            }
        }
    }

    let struct_array = changes_struct_builder.finish();
    let record_batch: RecordBatch = struct_array.into();

    let change_batch =
        ChangeBatch::try_new(record_batch).context(FailedToCreateChangeBatchSnafu)?;

    Ok(ChangeEnvelope::new(
        Box::new(DynamoDBStreamCommitter::new()),
        change_batch,
    ))
}

fn streams_to_dynamodb_item(
    item: HashMap<String, StreamsAttributeValue>,
) -> HashMap<String, DynamoDbAttributeValue> {
    item.into_iter()
        .map(|(k, v)| (k, streams_to_dynamodb_attribute(&v)))
        .collect()
}

fn streams_to_dynamodb_attribute(value: &StreamsAttributeValue) -> DynamoDbAttributeValue {
    match value {
        StreamsAttributeValue::B(blob) => DynamoDbAttributeValue::B(blob.clone()),
        StreamsAttributeValue::Bool(b) => DynamoDbAttributeValue::Bool(*b),
        StreamsAttributeValue::Bs(blobs) => DynamoDbAttributeValue::Bs(blobs.clone()),
        StreamsAttributeValue::L(list) => {
            DynamoDbAttributeValue::L(list.iter().map(streams_to_dynamodb_attribute).collect())
        }
        StreamsAttributeValue::M(map) => DynamoDbAttributeValue::M(
            map.iter()
                .map(|(k, v)| (k.clone(), streams_to_dynamodb_attribute(v)))
                .collect(),
        ),
        StreamsAttributeValue::N(n) => DynamoDbAttributeValue::N(n.clone()),
        StreamsAttributeValue::Ns(ns) => DynamoDbAttributeValue::Ns(ns.clone()),
        StreamsAttributeValue::Null(n) => DynamoDbAttributeValue::Null(*n),
        StreamsAttributeValue::S(s) => DynamoDbAttributeValue::S(s.clone()),
        StreamsAttributeValue::Ss(ss) => DynamoDbAttributeValue::Ss(ss.clone()),
        _ => DynamoDbAttributeValue::Null(true),
    }
}

fn downcast_builder<T: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Option<&mut T> {
    builder.as_any_mut().downcast_mut::<T>()
}

struct DynamoDBStreamCommitter;

impl DynamoDBStreamCommitter {
    pub fn new() -> Self {
        Self {}
    }
}

impl CommitChange for DynamoDBStreamCommitter {
    fn commit(&self) -> std::result::Result<(), CommitError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::ChangeOperation;
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use aws_sdk_dynamodbstreams::types::{
        AttributeValue as StreamsAttributeValue, OperationType, Record, StreamRecord,
    };
    use dynamodb_streams::DynamoDBStreamBatch;
    use dynamodb_streams::checkpoint::GlobalCheckpoint;
    use std::collections::HashMap;

    const TIME_FORMAT: &str = "2006-01-02T15:04:05.000Z07:00";

    // Helper function to create the table schema
    fn create_test_table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    // Helper to create a test record
    fn create_test_record(
        operation: OperationType,
        new_image: Option<HashMap<String, StreamsAttributeValue>>,
        keys: Option<HashMap<String, StreamsAttributeValue>>,
    ) -> Record {
        Record::builder()
            .event_name(operation)
            .dynamodb(
                StreamRecord::builder()
                    .set_new_image(new_image)
                    .set_keys(keys)
                    .build(),
            )
            .build()
    }

    #[allow(clippy::unnecessary_wraps)]
    fn create_stream_result(records: Vec<Record>) -> StreamResult {
        Ok(DynamoDBStreamBatch {
            records,
            checkpoint: GlobalCheckpoint {
                shards: HashMap::default(),
            },
        })
    }

    #[test]
    fn test_process_batch_insert_operation() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test Item".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify the batch has 1 row
        assert_eq!(envelope.change_batch.record.num_rows(), 1);

        // Verify the op field is "c" for create
        let op = envelope.change_batch.op(0);
        assert!(matches!(op, ChangeOperation::Create));
    }

    #[test]
    fn test_process_batch_modify_operation() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("456".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Updated Item".to_string()),
        );

        let record = create_test_record(OperationType::Modify, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify the batch has 1 row
        assert_eq!(envelope.change_batch.record.num_rows(), 1);

        // Verify the op field is "u" for update
        let op = envelope.change_batch.op(0);
        assert!(matches!(op, ChangeOperation::Update));
    }

    #[test]
    fn test_process_batch_remove_operation() {
        let mut keys = HashMap::new();
        keys.insert(
            "id".to_string(),
            StreamsAttributeValue::S("789".to_string()),
        );

        let record = create_test_record(OperationType::Remove, None, Some(keys));
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify the batch has 1 row
        assert_eq!(envelope.change_batch.record.num_rows(), 1);

        // Verify the op field is "d" for delete
        let op = envelope.change_batch.op(0);
        assert!(matches!(op, ChangeOperation::Delete));
    }

    #[test]
    fn test_process_batch_empty_batch() {
        let batch: Vec<Record> = vec![];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Empty batch should produce 0 rows
        assert_eq!(envelope.change_batch.record.num_rows(), 0);
    }

    #[test]
    fn test_process_batch_multiple_records() {
        let mut new_image1 = HashMap::new();
        new_image1.insert("id".to_string(), StreamsAttributeValue::S("1".to_string()));
        new_image1.insert(
            "name".to_string(),
            StreamsAttributeValue::S("First".to_string()),
        );

        let mut new_image2 = HashMap::new();
        new_image2.insert("id".to_string(), StreamsAttributeValue::S("2".to_string()));
        new_image2.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Second".to_string()),
        );

        let mut keys = HashMap::new();
        keys.insert("id".to_string(), StreamsAttributeValue::S("3".to_string()));

        let batch = vec![
            create_test_record(OperationType::Insert, Some(new_image1), None),
            create_test_record(OperationType::Modify, Some(new_image2), None),
            create_test_record(OperationType::Remove, None, Some(keys)),
        ];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Should have 3 rows
        assert_eq!(envelope.change_batch.record.num_rows(), 3);

        // Verify operations
        assert!(matches!(
            envelope.change_batch.op(0),
            ChangeOperation::Create
        ));
        assert!(matches!(
            envelope.change_batch.op(1),
            ChangeOperation::Update
        ));
        assert!(matches!(
            envelope.change_batch.op(2),
            ChangeOperation::Delete
        ));
    }

    #[test]
    fn test_process_batch_with_unnest_depth() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            Some(2),
            TIME_FORMAT,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_process_batch_with_empty_primary_keys() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec![]; // Empty primary keys

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify we can extract primary keys (should be empty)
        let pks = envelope.change_batch.primary_keys(0);
        assert_eq!(pks.len(), 0);
    }

    #[test]
    fn test_process_batch_skips_record_without_event_name() {
        let record = Record::builder().build();
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Should skip the record and produce 0 rows
        assert_eq!(envelope.change_batch.record.num_rows(), 0);
    }

    #[test]
    fn test_process_batch_skips_insert_without_new_image() {
        let record = Record::builder()
            .event_name(OperationType::Insert)
            .dynamodb(StreamRecord::builder().build())
            .build();

        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Should skip the record and produce 0 rows
        assert_eq!(envelope.change_batch.record.num_rows(), 0);
    }

    #[test]
    fn test_process_batch_skips_remove_without_keys() {
        let record = Record::builder()
            .event_name(OperationType::Remove)
            .dynamodb(StreamRecord::builder().build())
            .build();

        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Should skip the record and produce 0 rows
        assert_eq!(envelope.change_batch.record.num_rows(), 0);
    }

    #[test]
    fn test_process_batch_with_multiple_primary_keys() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string(), "sort_key".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify primary keys
        let pks = envelope.change_batch.primary_keys(0);
        assert_eq!(pks.len(), 2);
        assert_eq!(pks[0], "id");
        assert_eq!(pks[1], "sort_key");
    }

    #[test]
    fn test_process_batch_mixed_valid_and_invalid_records() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Valid".to_string()),
        );

        let valid_record = create_test_record(OperationType::Insert, Some(new_image), None);
        let invalid_record = Record::builder().build(); // No event name

        let batch = vec![valid_record, invalid_record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Should only process the valid record
        assert_eq!(envelope.change_batch.record.num_rows(), 1);
    }

    #[test]
    fn test_process_batch_primary_keys_extraction() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("pk-123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys.clone(),
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify primary keys can be extracted
        let extracted_pks = envelope.change_batch.primary_keys(0);
        assert_eq!(extracted_pks, primary_keys);
    }

    #[test]
    fn test_process_batch_data_extraction() {
        let mut new_image = HashMap::new();
        new_image.insert(
            "id".to_string(),
            StreamsAttributeValue::S("123".to_string()),
        );
        new_image.insert(
            "name".to_string(),
            StreamsAttributeValue::S("Test Name".to_string()),
        );

        let record = create_test_record(OperationType::Insert, Some(new_image), None);
        let batch = vec![record];

        let table_schema = create_test_table_schema();
        let primary_keys = vec!["id".to_string()];

        let result = process_batch(
            create_stream_result(batch),
            &table_schema,
            &primary_keys,
            None,
            TIME_FORMAT,
        );

        assert!(result.is_ok());
        let envelope = result.expect("change envelope");

        // Verify data can be extracted
        let data_batch = envelope.change_batch.data(0);
        assert_eq!(data_batch.num_rows(), 1);
        assert_eq!(data_batch.num_columns(), 2); // id and name
    }
}
