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
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt as _;
use s3_vectors::{
    BuildError, PutInputVector, PutVectorsError, PutVectorsInput, S3Vectors, VectorData,
};
use snafu::prelude::*;

use super::S3VectorIdentifier;

const PUT_VECTORS_MAX_ITEMS: usize = 500;

#[derive(Debug, Snafu)]
pub enum Error {
    // This means we didn't provide required fields when constructing.
    #[snafu(display("Unable to build input message for S3 Vectors: {source}"))]
    BuildInput { source: BuildError },
    #[snafu(display("Failed to write vectors into S3 Vectors: {source}"))]
    PutVectors { source: PutVectorsError },
    #[snafu(display("Column '{name}' is expected but missing"))]
    MissingColumn { name: String },
    #[snafu(display("Column '{name}' type is not '{expected}' but expected to be"))]
    ColumnTypeMismatch { name: String, expected: String },
}

type Result<T> = std::result::Result<T, Error>;

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
                            .context(BuildInputSnafu)?,
                    )
                    .await
                    .map_err(|e| e.into_service_error())
                    .context(PutVectorsSnafu)?;

                count += chunk.len();
            }
        }

        Ok(count as _)
    }
}

fn create_put_input_vectors(record_batch: RecordBatch) -> Result<Vec<PutInputVector>> {
    let name = "key".to_string();
    let keys = record_batch
        .column_by_name(&name)
        .ok_or_else(|| Error::MissingColumn { name: name.clone() })?
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| Error::ColumnTypeMismatch {
            name,
            expected: "StringArray".to_string(),
        })?;

    let name = "metadata".to_string();
    let _metadata = record_batch
        .column_by_name(&name)
        .ok_or_else(|| Error::MissingColumn { name: name.clone() })?
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| Error::ColumnTypeMismatch {
            name,
            expected: "StringArray".to_string(),
        })?;

    let name = "vector".to_string();
    let vectors = record_batch
        .column_by_name(&name)
        .ok_or_else(|| Error::MissingColumn { name: name.clone() })?
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| Error::ColumnTypeMismatch {
            name,
            expected: "ListArray".to_string(),
        })?;

    let mut put_input_vectors = Vec::new();
    for i in 0..record_batch.num_rows() {
        let key = keys.value(i).to_string();

        let vector = vectors
            .value(i)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .ok_or_else(|| Error::ColumnTypeMismatch {
                name: format!("vector[{i}]"),
                expected: "Float32Array".to_string(),
            })?
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
            .context(BuildInputSnafu)?;

        put_input_vectors.push(put_input_vector);
    }

    Ok(put_input_vectors)
}

impl From<Error> for DataFusionError {
    fn from(value: Error) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}
