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
use crate::dynamodb::utils::scalar_to_attribute_value;
use arrow::array::Array;
use arrow::datatypes::SchemaRef;
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
use std::{any::Any, collections::HashMap, fmt, sync::Arc};

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

/// Convert a single row from a `RecordBatch` to a `DynamoDB` item.
fn record_batch_row_to_dynamodb_item(
    batch: &arrow::array::RecordBatch,
    row_idx: usize,
    schema: &SchemaRef,
    time_format: &str,
) -> DataFusionResult<HashMap<String, AttributeValue>> {
    let mut item = HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);
        if col.is_null(row_idx) {
            continue;
        }
        let scalar = ScalarValue::try_from_array(col, row_idx)?;
        if scalar.is_null() {
            continue;
        }
        let attr_value = scalar_to_attribute_value(&scalar, time_format)?;
        item.insert(field.name().clone(), attr_value);
    }
    Ok(item)
}

#[async_trait]
impl DataSink for DynamoDBInsertSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
                    futures::stream::iter((0..rows).map(move |row_idx| {
                        let item = record_batch_row_to_dynamodb_item(
                            &batch,
                            row_idx,
                            &schema,
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
