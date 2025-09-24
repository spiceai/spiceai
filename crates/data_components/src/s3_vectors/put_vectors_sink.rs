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

        while let Some(record_batch) = data.next().await {
            let record_batch = record_batch?;

            let vectors = create_put_input_vectors(&record_batch)?;

            let (index_arn, vector_bucket_name, index_name) =
                self.table.idx.index_identifier_variables();

            for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
                self.table
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

                count += chunk.len();
            }
        }

        Ok(count as _)
    }
}

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

#[cfg(test)]
mod tests {
    use arrow::{
        array::ListBuilder,
        datatypes::{DataType, Field, Schema},
    };
    use arrow_array::{Float32Array, GenericListArray, Int32Array, StringArray};

    use super::*;

    fn schema_ref() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new("metadata", DataType::Utf8, false),
            Field::new_list(
                S3_VECTOR_EMBEDDING_NAME,
                Field::new("item", DataType::Float32, true),
                true,
            ),
        ]))
    }

    fn build_vectors(input: &[&[f32]]) -> GenericListArray<i32> {
        let capacity = input.iter().map(|i| i.len()).sum();
        let mut list_builder = ListBuilder::new(Float32Array::builder(capacity));

        for i in input {
            for j in *i {
                list_builder.values().append_value(*j);
            }

            list_builder.append(true);
        }

        list_builder.finish()
    }

    #[test]
    fn test_create_put_input_vectors_success() {
        let keys = StringArray::from(vec!["key1", "key2"]);
        let metadata = StringArray::from(vec!["meta1", "meta2"]);

        let vectors = build_vectors(&[&[1f32, 2f32, 3f32], &[4f32, 5f32, 6f32]]);

        let schema = schema_ref();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys), Arc::new(metadata), Arc::new(vectors)],
        )
        .expect("try_new");

        let result = create_put_input_vectors(&batch).expect("create_put_input_vectors");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key(), "key1");
        assert_eq!(
            result[0].data().expect("data"),
            &VectorData::Float32(vec![1f32, 2f32, 3f32])
        );
    }

    #[test]
    fn test_create_put_input_vectors_missing_key_column() {
        let metadata = StringArray::from(vec!["meta1", "meta2"]);

        let vectors = build_vectors(&[&[1f32, 2f32, 3f32], &[4f32, 5f32, 6f32]]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("metadata", DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(metadata), Arc::new(vectors)])
            .expect("try_new");

        let result = create_put_input_vectors(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_put_input_vectors_wrong_vector_type() {
        let keys = StringArray::from(vec!["key1", "key2"]);
        let metadata = StringArray::from(vec!["meta1", "meta2"]);

        let mut list_builder = ListBuilder::new(Int32Array::builder(6));
        list_builder.values().append_value(1);
        list_builder.values().append_value(2);
        list_builder.values().append_value(3);
        list_builder.append(true);
        list_builder.values().append_value(4);
        list_builder.values().append_value(5);
        list_builder.values().append_value(6);
        list_builder.append(true);

        let vectors = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new("metadata", DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys), Arc::new(metadata), Arc::new(vectors)],
        )
        .expect("try_new");

        let result = create_put_input_vectors(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_put_input_vectors_nan_infinite_vectors_skipped() {
        let keys = StringArray::from(vec!["key1", "key2", "key3"]);
        let metadata = StringArray::from(vec!["meta1", "meta2", "meta3"]);

        let vectors = build_vectors(&[
            &[1.0, 2.0, 3.0],
            &[f32::NAN, 2.0, 3.0],
            &[1.0, f32::INFINITY, 3.0],
        ]);

        let schema = schema_ref();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys), Arc::new(metadata), Arc::new(vectors)],
        )
        .expect("try_new");

        let result = create_put_input_vectors(&batch).expect("create_put_input_vectors");

        // Only the first vector should be included (2 valid vectors)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key(), "key1");
    }

    #[test]
    fn test_create_put_input_vectors_empty_vectors_skipped() {
        let keys = StringArray::from(vec!["key1", "key2"]);
        let metadata = StringArray::from(vec!["meta1", "meta2"]);

        let vectors = build_vectors(&[&[], &[1.0, 2.0, 3.0]]);

        let schema = schema_ref();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(keys), Arc::new(metadata), Arc::new(vectors)],
        )
        .expect("try_new");

        let result = create_put_input_vectors(&batch).expect("create_put_input_vectors");

        // Only the second vector should be included (1 valid vector)
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key(), "key2");
    }
}
