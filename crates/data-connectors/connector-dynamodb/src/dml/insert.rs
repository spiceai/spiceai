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

use super::streaming_batch_write;
use crate::utils::scalar_to_attribute_value;
use arrow::array::{Array, AsArray, BooleanArray, GenericByteArray, PrimitiveArray};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client as DbClient,
    types::{AttributeValue, PutRequest, WriteRequest},
};
use datafusion::{
    common::ScalarValue,
    datasource::sink::DataSink,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, metrics::MetricsSet},
};
use futures::StreamExt;
use std::{collections::HashMap, fmt, sync::Arc};

pub struct DynamoDBInsertSink {
    pub db_client: Arc<DbClient>,
    pub table_name: String,
    pub schema: SchemaRef,
    pub time_format: Arc<String>,
    pub parallelism: usize,
}

impl std::fmt::Debug for DynamoDBInsertSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoDBInsertSink")
            .field("table_name", &self.table_name)
            .field("parallelism", &self.parallelism)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DynamoDBInsertSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DynamoDBInsertSink(table={})", self.table_name)
    }
}

/// Fast path: convert common Arrow array cell types directly to `AttributeValue`
/// without building an intermediate `ScalarValue`.
///
/// Returns `Ok(None)` when the type is not handled here (caller should fall back).
fn try_array_value_to_attribute_value(
    array: &dyn Array,
    row_idx: usize,
) -> DataFusionResult<Option<AttributeValue>> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr: &GenericByteArray<arrow::datatypes::GenericStringType<i32>> =
                array.as_string::<i32>();
            Ok(Some(AttributeValue::S(arr.value(row_idx).to_owned())))
        }
        DataType::LargeUtf8 => {
            let arr: &GenericByteArray<arrow::datatypes::GenericStringType<i64>> =
                array.as_string::<i64>();
            Ok(Some(AttributeValue::S(arr.value(row_idx).to_owned())))
        }
        DataType::Boolean => {
            let arr: &BooleanArray = array.as_boolean();
            Ok(Some(AttributeValue::Bool(arr.value(row_idx))))
        }
        DataType::Int8 => {
            let arr: &PrimitiveArray<Int8Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::Int16 => {
            let arr: &PrimitiveArray<Int16Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::Int32 => {
            let arr: &PrimitiveArray<Int32Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::Int64 => {
            let arr: &PrimitiveArray<Int64Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::UInt8 => {
            let arr: &PrimitiveArray<UInt8Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::UInt16 => {
            let arr: &PrimitiveArray<UInt16Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::UInt32 => {
            let arr: &PrimitiveArray<UInt32Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::UInt64 => {
            let arr: &PrimitiveArray<UInt64Type> = array.as_primitive();
            Ok(Some(AttributeValue::N(arr.value(row_idx).to_string())))
        }
        DataType::Float32 => {
            let arr: &PrimitiveArray<Float32Type> = array.as_primitive();
            let f = arr.value(row_idx);
            if f.is_finite() {
                Ok(Some(AttributeValue::N(f.to_string())))
            } else {
                Err(DataFusionError::Execution(format!(
                    "Cannot write non-finite Float32 value ({f}) to DynamoDB"
                )))
            }
        }
        DataType::Float64 => {
            let arr: &PrimitiveArray<Float64Type> = array.as_primitive();
            let f = arr.value(row_idx);
            if f.is_finite() {
                Ok(Some(AttributeValue::N(f.to_string())))
            } else {
                Err(DataFusionError::Execution(format!(
                    "Cannot write non-finite Float64 value ({f}) to DynamoDB"
                )))
            }
        }
        _ => Ok(None),
    }
}

/// Convert a single row from a `RecordBatch` to a `DynamoDB` item.
///
/// `field_names` is precomputed once per batch so `field.name()` is not re-resolved
/// for every row.
fn record_batch_row_to_dynamodb_item(
    batch: &arrow::array::RecordBatch,
    row_idx: usize,
    field_names: &[String],
    time_format: &str,
) -> DataFusionResult<HashMap<String, AttributeValue>> {
    let mut item = HashMap::with_capacity(field_names.len());
    for (col_idx, field_name) in field_names.iter().enumerate() {
        let col = batch.column(col_idx);
        if col.is_null(row_idx) {
            continue;
        }
        if let Some(attr_value) = try_array_value_to_attribute_value(col.as_ref(), row_idx)? {
            item.insert(field_name.clone(), attr_value);
            continue;
        }
        let scalar = ScalarValue::try_from_array(col, row_idx)?;
        if scalar.is_null() {
            continue;
        }
        let attr_value = scalar_to_attribute_value(&scalar, time_format)?;
        item.insert(field_name.to_owned(), attr_value);
    }
    Ok(item)
}

#[async_trait]
impl DataSink for DynamoDBInsertSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let schema = Arc::clone(&self.schema);
        let time_format = Arc::clone(&self.time_format);

        // Build a stream of WriteRequests from the RecordBatch stream
        let request_stream = data.flat_map(move |batch_result| {
            let schema = Arc::clone(&schema);
            let time_format = Arc::clone(&time_format);
            match batch_result {
                Err(e) => futures::stream::iter(vec![Err(e)]).boxed(),
                Ok(batch) => {
                    let rows = batch.num_rows();
                    // Cache field names once per batch (not once per row).
                    let field_names: Vec<String> =
                        schema.fields().iter().map(|f| f.name().clone()).collect();
                    futures::stream::iter((0..rows).map(move |row_idx| {
                        let item = record_batch_row_to_dynamodb_item(
                            &batch,
                            row_idx,
                            &field_names,
                            &time_format,
                        )?;
                        let put_request = PutRequest::builder()
                            .set_item(Some(item))
                            .build()
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to build PutRequest: {e}"
                                ))
                            })?;
                        Ok(WriteRequest::builder().put_request(put_request).build())
                    }))
                    .boxed()
                }
            }
        });

        let request_stream = Box::pin(request_stream);

        streaming_batch_write(
            &self.db_client,
            &self.table_name,
            request_stream,
            self.parallelism,
        )
        .await
    }
}
