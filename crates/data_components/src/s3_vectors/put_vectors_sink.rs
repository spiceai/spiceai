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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    datasource::sink::DataSink,
    error::Result as DataFusionResult,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt as _;
use s3_vectors::{PutInputVector, PutVectorsInput, S3Vectors, VectorData};

use super::S3VectorIdentifier;

const PUT_VECTORS_MAX_ITEMS: usize = 500;

pub struct PutVectorsSink {
    idx: S3VectorIdentifier,
    client: Arc<dyn S3Vectors + Send + Sync>,
    schema: SchemaRef,
}

impl PutVectorsSink {
    pub fn new(
        idx: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            idx,
            client,
            schema,
        }
    }
}

impl std::fmt::Debug for PutVectorsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutVectorsExec").finish_non_exhaustive()
    }
}

impl DisplayAs for PutVectorsSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PutVectorsExec")
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

        while let Some(record_batch) = data.next().await {
            let record_batch = record_batch?;

            let vectors = create_put_input_vectors(record_batch).unwrap();

            let (index_arn, vector_bucket_name, index_name) = self.idx.index_identifier_variables();

            for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
                self.client
                    .put_vectors(
                        PutVectorsInput::builder()
                            .set_index_arn(index_arn.clone())
                            .set_index_name(index_name.clone())
                            .set_vector_bucket_name(vector_bucket_name.clone())
                            .set_vectors(Some(chunk.to_vec()))
                            .build()
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                count += chunk.len();
            }
        }

        Ok(count as _)
    }
}

fn create_put_input_vectors(record_batch: RecordBatch) -> Result<Vec<PutInputVector>, String> {
    let keys = record_batch
        .column_by_name("key")
        .ok_or("Missing key column")?
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or("Key column must be String")?;

    let vectors = record_batch
        .column_by_name("vector")
        .ok_or("Missing vector column")?
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or("Vector column must be List")?;

    let _metadata = record_batch
        .column_by_name("metadata")
        .ok_or("Missing metadata column")?
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or("Metadata column must be String")?;

    let mut put_input_vectors = Vec::new();
    for i in 0..record_batch.num_rows() {
        let key = keys.value(i).to_string();

        let vector = vectors
            .value(i)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .ok_or("Invalid vector data")?
            .values()
            .to_vec();

        if vector.iter().any(|&x| x.is_nan() || x.is_infinite()) {
            continue;
        }

        if vector.iter().all(|&x| x == 0.0) {
            continue;
        }

        // TODO: add metadata
        let put_input_vector = PutInputVector::builder()
            .key(key)
            .data(VectorData::Float32(vector))
            .build()
            .unwrap();

        put_input_vectors.push(put_input_vector);
    }

    Ok(put_input_vectors)
}
