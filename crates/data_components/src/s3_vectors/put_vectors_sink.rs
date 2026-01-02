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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef};
use arrow_array::{Array, RecordBatch};
use async_trait::async_trait;
use datafusion::{
    datasource::sink::DataSink,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt as _;
use s3_vectors::{
    BuildError, Document, Number, PutInputVector, PutVectorsInput, SdkError, VectorData,
};
use snafu::{ResultExt, prelude::*};

use super::{S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorsTable};

const PUT_VECTORS_MAX_ITEMS: usize = 500;
// S3 Vectors API has a 1MB (1,048,576 bytes) payload limit
const PUT_VECTORS_MAX_PAYLOAD_BYTES: usize = 1_048_576;
// Estimate overhead per vector: vector_id, metadata, JSON structure (~200 bytes)
const ESTIMATED_OVERHEAD_PER_VECTOR: usize = 200;

#[derive(Debug, Snafu)]
pub enum Error {
    // This means we didn't provide required fields when constructing.
    #[snafu(display("Unable to build input message for S3 Vectors: {source}"))]
    BuildInput { source: BuildError },
    #[snafu(display("Failed to write vectors into S3 Vectors: {source}"))]
    PutVectors {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Column '{name}' is expected but missing"))]
    MissingColumn { name: String },
    #[snafu(display("Column '{name}' type is not '{expected}' but expected to be"))]
    ColumnTypeMismatch { name: String, expected: String },
    #[snafu(display("Expected {expected} datatype but got a different datatype"))]
    DatatypeMismatch { expected: String },
    #[snafu(display("Invalid primary key at row {row}: {reason}"))]
    InvalidPrimaryKey { row: usize, reason: String },
    #[snafu(display("Invalid metadata key '{key}' at row {row}: {reason}"))]
    InvalidMetadataKey {
        key: String,
        row: usize,
        reason: String,
    },
}

type Result<T> = std::result::Result<T, Error>;

pub struct PutVectorsSink {
    table: S3VectorsTable,
}

impl PutVectorsSink {
    #[must_use]
    pub fn new(table: S3VectorsTable) -> Self {
        Self { table }
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
        &self.table.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut count = 0;
        // Calculate batch size once based on vector dimensions from schema
        let vector_dimensions = usize::try_from(self.table.dimension).unwrap_or(0);
        let batch_size = calculate_batch_size(vector_dimensions);

        while let Some(record_batch) = data.next().await {
            let record_batch = record_batch?;

            let vectors = create_put_input_vectors(&record_batch)?;

            let (index_arn, vector_bucket_name, index_name) =
                self.table.idx.index_identifier_variables();

            for chunk in vectors.chunks(batch_size) {
                let chunk_len = chunk.len();
                let _ = self
                    .table
                    .client
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
                    .map_err(SdkError::into_service_error)
                    .boxed()
                    .context(PutVectorsSnafu)?;

                count += chunk_len;
            }
        }

        Ok(count as _)
    }
}

/// Calculate optimal batch size based on vector dimensions to stay under 1MB payload limit
///
/// Each vector consumes: (dimensions * 4 bytes for f32) + overhead (~200 bytes)
/// We conservatively cap at `PUT_VECTORS_MAX_ITEMS` (500) to avoid API limits
fn calculate_batch_size(vector_dimensions: usize) -> usize {
    if vector_dimensions == 0 {
        return PUT_VECTORS_MAX_ITEMS;
    }

    // Each f32 is 4 bytes
    let bytes_per_vector = (vector_dimensions * 4) + ESTIMATED_OVERHEAD_PER_VECTOR;

    // Calculate max vectors that fit in 1MB, leaving 10% safety margin
    let max_by_size = (PUT_VECTORS_MAX_PAYLOAD_BYTES * 9) / (bytes_per_vector * 10);

    // Take the minimum of size-based limit and API item limit
    max_by_size.clamp(1, PUT_VECTORS_MAX_ITEMS)
}

#[expect(clippy::too_many_lines)]
fn create_put_input_vectors(record_batch: &RecordBatch) -> Result<Vec<PutInputVector>> {
    let name = S3_VECTOR_PRIMARY_KEY_NAME.to_string();
    let keys = record_batch
        .column_by_name(&name)
        .ok_or_else(|| Error::MissingColumn { name: name.clone() })?
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| Error::ColumnTypeMismatch {
            name,
            expected: "StringArray".to_string(),
        })?;

    let name = S3_VECTOR_EMBEDDING_NAME.to_string();
    let vectors = record_batch
        .column_by_name(&name)
        .ok_or_else(|| Error::MissingColumn { name: name.clone() })?
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| Error::ColumnTypeMismatch {
            name,
            expected: "ListArray".to_string(),
        })?;

    let schema = record_batch.schema();
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            f.name() != S3_VECTOR_EMBEDDING_NAME && f.name() != S3_VECTOR_PRIMARY_KEY_NAME
        })
        .map(|(i, f)| (i, f.name(), f.data_type()))
        .collect::<Vec<_>>();

    let mut put_input_vectors = Vec::new();
    for row in 0..record_batch.num_rows() {
        let key = keys.value(row).to_string();

        // Validate primary key
        if key.is_empty() {
            return Err(Error::InvalidPrimaryKey {
                row,
                reason: "Primary key cannot be empty".to_string(),
            });
        }
        if key.len() > 1024 {
            return Err(Error::InvalidPrimaryKey {
                row,
                reason: format!(
                    "Primary key exceeds maximum length of 1024 characters (got {})",
                    key.len()
                ),
            });
        }
        // S3 Vectors keys should not contain control characters
        if key.chars().any(char::is_control) {
            return Err(Error::InvalidPrimaryKey {
                row,
                reason: "Primary key contains invalid control characters".to_string(),
            });
        }

        let vector = vectors
            .value(row)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .ok_or_else(|| Error::ColumnTypeMismatch {
                name: format!("data[{row}]"),
                expected: "Float32Array".to_string(),
            })?
            .values()
            .to_vec();

        if vector.iter().any(|&x| x.is_nan() || x.is_infinite()) {
            tracing::debug!("Disregarding a vector that contains NaN or Inf");
            continue;
        }

        if vector.iter().all(|&x| x == 0.0) {
            tracing::debug!("Disregarding a zero vector");
            continue;
        }

        let mut metadata = HashMap::new();

        for (index, name, data_type) in &fields {
            // Validate metadata key
            if name.is_empty() {
                return Err(Error::InvalidMetadataKey {
                    key: (*name).to_string(),
                    row,
                    reason: "Metadata key cannot be empty".to_string(),
                });
            }
            if name.len() > 256 {
                return Err(Error::InvalidMetadataKey {
                    key: (*name).to_string(),
                    row,
                    reason: format!(
                        "Metadata key exceeds maximum length of 256 characters (got {})",
                        name.len()
                    ),
                });
            }
            // Metadata keys should not contain control characters or special chars
            if name.chars().any(|c| c.is_control() || c == '\0') {
                return Err(Error::InvalidMetadataKey {
                    key: (*name).to_string(),
                    row,
                    reason: "Metadata key contains invalid characters".to_string(),
                });
            }

            let col = record_batch.column(*index);
            let value = metadata_from_row(row, data_type, col)?;
            metadata.insert((*name).to_string(), value);
        }

        let metadata = if metadata.is_empty() {
            None
        } else {
            Some(Document::Object(metadata))
        };

        let put_input_vector = PutInputVector::builder()
            .key(key)
            .data(VectorData::Float32(vector))
            .set_metadata(metadata)
            .build()
            .context(BuildInputSnafu)?;

        put_input_vectors.push(put_input_vector);
    }

    Ok(put_input_vectors)
}

fn metadata_from_row(
    row: usize,
    data_type: &DataType,
    col: &Arc<dyn Array + 'static>,
) -> Result<Document> {
    Ok(match data_type {
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .context(DatatypeMismatchSnafu {
                    expected: "Utf8".to_string(),
                })?;
            Document::String(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .context(DatatypeMismatchSnafu {
                    expected: "Int64".to_string(),
                })?;
            Document::Number(Number::NegInt(arr.value(row)))
        }
        DataType::UInt64 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .context(DatatypeMismatchSnafu {
                    expected: "UInt64".to_string(),
                })?;
            Document::Number(Number::PosInt(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .context(DatatypeMismatchSnafu {
                    expected: "Float64".to_string(),
                })?;
            Document::Number(Number::Float(arr.value(row)))
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .context(DatatypeMismatchSnafu {
                    expected: "Boolean".to_string(),
                })?;
            Document::Bool(arr.value(row))
        }
        _ => unimplemented!(),
    })
}

impl From<Error> for DataFusionError {
    fn from(value: Error) -> Self {
        DataFusionError::Execution(value.to_string())
    }
}
