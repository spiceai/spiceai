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

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3vectors::{
    operation::put_vectors::PutVectorsInput,
    types::{Index, PutInputVector, VectorData},
};
use datafusion::{
    arrow::{
        array::{Float32Array, ListArray, StringArray},
        datatypes::SchemaRef,
    },
    datasource::sink::DataSink,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt as _;

use crate::{PUT_VECTORS_MAX_ITEMS, S3Vectors};

static NAME: &str = "PutVectorsSink";

pub struct PutVectorsSink {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
}

impl PutVectorsSink {
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>, index: Index, schema: SchemaRef) -> Self {
        Self {
            client,
            index,
            schema,
        }
    }
}

impl std::fmt::Debug for PutVectorsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl DisplayAs for PutVectorsSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[async_trait]
impl DataSink for PutVectorsSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut count = 0;

        while let Some(batch) = data.next().await {
            let batch = batch?;

            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected key to be a StringArray".to_string())
                })?;

            let data_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected data to be a ListArray".to_string())
                })?;

            let mut vectors = Vec::with_capacity(batch.num_rows());
            for i in 0..batch.num_rows() {
                let key = key_array.value(i).to_string();
                let data_values = data_array.value(i);
                let float_array = data_values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected data list to contain Float32".to_string(),
                        )
                    })?;

                let vector_data = VectorData::Float32(float_array.values().to_vec());

                let input_vector = PutInputVector::builder()
                    .key(key)
                    .data(vector_data)
                    .build()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                vectors.push(input_vector);
            }

            for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
                let input = PutVectorsInput::builder()
                    .index_name(self.index.index_name())
                    .vector_bucket_name(self.index.vector_bucket_name())
                    .set_vectors(Some(chunk.to_vec()))
                    .build()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                self.client
                    .put_vectors(input)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                count += chunk.len();
            }
        }

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_client::MockClient;
    use aws_sdk_s3vectors::types::{DistanceMetric, Index};
    use aws_smithy_types::DateTime;
    use datafusion::{
        arrow::{
            array::{Float32Builder, GenericByteArray, ListBuilder, RecordBatch, StringBuilder},
            datatypes::{DataType, Field, Schema, Utf8Type},
        },
        common::Result as DataFusionResult,
        execution::TaskContext,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::stream;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_put_vectors_sink() -> DataFusionResult<()> {
        let mock_client = Arc::new(MockClient::new());
        let index_name = "test_index";
        let index_arn = "test_arn";
        let bucket_name = "test_bucket";

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));

        let index = Index::builder()
            .index_name(index_name)
            .vector_bucket_name(bucket_name)
            .index_arn(index_arn)
            .creation_time(DateTime::from_secs(7))
            .data_type(aws_sdk_s3vectors::types::DataType::Float32)
            .dimension(3)
            .distance_metric(DistanceMetric::Cosine)
            .build()
            .expect("valid index");

        let sink = PutVectorsSink::new(
            Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>,
            index,
            Arc::clone(&schema),
        );

        let mut key_builder = StringBuilder::new();
        let arr: GenericByteArray<Utf8Type> = vec!["v1", "v2"].into();
        key_builder.append_array(&arr);
        let key_array = Arc::new(key_builder.finish());

        let mut list_builder = ListBuilder::new(Float32Builder::new());
        list_builder.values().append_slice(&[1.0, 2.0, 3.0]);
        list_builder.append(true);
        list_builder.values().append_slice(&[4.0, 5.0, 6.0]);
        list_builder.append(true);
        let data_array = Arc::new(list_builder.finish());

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![key_array, data_array])?;

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok(batch)]),
        ));

        let count = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await?;

        assert_eq!(count, 2);

        let data = mock_client.data.lock().expect("lock");
        let put_vectors = &data.put_vectors;
        assert_eq!(put_vectors.len(), 2);

        assert_eq!(put_vectors[0].key(), "v1");
        assert_eq!(
            put_vectors[0].data(),
            Some(&VectorData::Float32(vec![1.0, 2.0, 3.0]))
        );

        assert_eq!(put_vectors[1].key(), "v2");
        assert_eq!(
            put_vectors[1].data(),
            Some(&VectorData::Float32(vec![4.0, 5.0, 6.0]))
        );

        Ok(())
    }
}
