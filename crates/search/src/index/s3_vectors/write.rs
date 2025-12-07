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

use std::{collections::HashMap, num::TryFromIntError, sync::Arc};

use arrow::array::{
    Array, FixedSizeListBuilder, Float32Builder, LargeStringArray, RecordBatch, StringArray,
    StringViewArray,
};
use arrow::compute::concat_batches;
use arrow_json::{EncoderOptions, writer::make_encoder};
use arrow_schema::{DataType, Field, Schema};
use data_components::s3_vectors::S3VectorsTable;
use itertools::Itertools;
use llms::embeddings::{Embed, EmbeddingInput};
use runtime_datafusion_index::Index;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use util::{convert_string_arrow_to_iterator, distribute_nulls};

use crate::index::{SearchIndex, embedding_col, s3_vectors::S3Vector};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Embedding model '{model_name}' was not found"))]
    EmbeddingModelNotFound { model_name: String },

    #[snafu(display("{source}"))]
    FailedToEmbed { source: llms::embeddings::Error },

    #[snafu(display("Cannot write to '{index}' index, data does not have column '{column}'."))]
    ColumnNotFound { index: String, column: String },

    #[snafu(display("Cannot write to '{index}' index, index has no primary key field(s)."))]
    NoPrimaryKeyField { index: String },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing arrow records: {source}."
    ))]
    IssueWithArrowProcessing {
        index: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing JSON values: {source}."
    ))]
    IssueWithJsonProcessing {
        index: String,
        source: serde_json::Error,
    },

    #[snafu(display(
        "Cannot write to '{index}' index, primary key could not be serialized: {source}"
    ))]
    FailedToSerializePrimaryKey {
        index: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot write to '{index}' index, as provided data has mismatch lengths. {mismatch_source} has {mismatch_length} rows, whilst primary key column '{}' has {len} rows. {mismatch_source} != {len}.", primary_key_columns.iter().map(|f| f.name().clone()).join(", ")
    ))]
    LengthMismatch {
        mismatch_source: String,
        index: String,
        mismatch_length: usize,
        primary_key_columns: Vec<Field>,
        len: usize,
    },

    #[snafu(display("Cannot write to '{index}' index: {source}"))]
    CannotWriteIndex {
        index: String,
        source: data_components::s3_vectors::Error,
    },

    #[snafu(display("Cannot update embedding column in record batch: {source}"))]
    CannotUpdateEmbeddingColumn { source: arrow::error::ArrowError },

    #[snafu(display(
        "Cannot create embedding array: no valid embeddings found to determine dimension"
    ))]
    CannotDetermineEmbeddingDimension,

    #[snafu(display("Embedding dimension is too large to fit into an i32"))]
    EmbeddingDimensionTooLarge { source: TryFromIntError },
}

/// Extra index data from the raw table batches, embedded required column and write to [`S3VectorsTable`].
pub async fn write(
    index: &S3Vector,
    table: &S3VectorsTable,
    record: RecordBatch,
    batch_write_rows: usize,
) -> Result<RecordBatch, Error> {
    if record.num_rows() <= batch_write_rows {
        return process_single_batch(index, table, record).await;
    }

    let mut result_batches = Vec::with_capacity(record.num_rows().div_ceil(batch_write_rows));
    let schema = record.schema();

    for chunk_start in (0..record.num_rows()).step_by(batch_write_rows) {
        let chunk_end = (chunk_start + batch_write_rows).min(record.num_rows());
        let chunk_length = chunk_end - chunk_start;

        let chunk_batch = record.slice(chunk_start, chunk_length);

        let processed_chunk = process_single_batch(index, table, chunk_batch).await?;
        result_batches.push(processed_chunk);
    }

    let concatenated =
        concat_batches(&schema, &result_batches).context(IssueWithArrowProcessingSnafu {
            index: index.name(),
        })?;

    Ok(concatenated)
}

async fn process_single_batch(
    index: &S3Vector,
    table: &S3VectorsTable,
    record: RecordBatch,
) -> Result<RecordBatch, Error> {
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        tracing::warn!(
            "Cannot write to '{}' index, data does not have column '{}'.",
            index.name(),
            index.embedded_column
        );
        return Ok(record);
    };

    let embedding_vectors = embed_column(
        &record,
        embedded_column_idx,
        Arc::clone(&index.compute_query),
    )
    .await?;
    let metadata = extract_and_format_metadata(
        index.name(),
        &index
            .metadata_columns()
            .all_names()
            .into_iter()
            .filter(|c| *c != embedding_col(&index.search_column()))
            .collect::<Vec<_>>(),
        &record,
    )
    .map_err(|e| *e)?;
    let primary_key = extract_and_format_primary_key(index.name(), &index.primary_key, &record)
        .map_err(|e| *e)?;

    if primary_key.len() != embedding_vectors.len() {
        return LengthMismatchSnafu {
            index: index.name().to_string(),
            primary_key_columns: index.primary_fields(),
            len: primary_key.len(),
            mismatch_length: embedding_vectors.len(),
            mismatch_source: index.embedded_column.clone(),
        }
        .fail();
    }
    for (name, v) in &metadata {
        if v.len() != primary_key.len() {
            return LengthMismatchSnafu {
                index: index.name().to_string(),
                primary_key_columns: index.primary_fields(),
                len: primary_key.len(),
                mismatch_length: v.len(),
                mismatch_source: name.clone(),
            }
            .fail();
        }
    }

    // Update the embedding column in the batch with computed embeddings
    let updated_record = update_embedding_column_in_batch(
        &record,
        &index.embedded_column,
        &embedding_vectors,
        i32::try_from(table.dimension).unwrap_or_default(),
    )
    .map_err(|e| *e)?;

    // Filter out zero vectors to prevent cosine similarity calculation errors
    let (filtered_embeddings, filtered_primary_key, filtered_metadata) =
        filter_zero_vectors(embedding_vectors, primary_key, metadata, index.name());

    table
        .write_data(filtered_embeddings, filtered_primary_key, filtered_metadata)
        .await
        .context(CannotWriteIndexSnafu {
            index: index.name().to_string(),
        })?;

    // Because of limitations of `DFSchema::logically_equivalent_names_and_types` and its use in
    // `MemTable`, this must be in the same order as outputted by `VectorScanTableProvider`.
    let (schema, arr, _) = updated_record.into_parts();
    let (arrs, fields): (Vec<_>, Vec<_>) = arr
        .into_iter()
        .zip(schema.fields().into_iter())
        .sorted_by_key(|(_, f)| f.name())
        .unzip();

    RecordBatch::try_new(
        Arc::new(Schema::new(fields.into_iter().cloned().collect::<Vec<_>>())),
        arrs,
    )
    .context(IssueWithArrowProcessingSnafu {
        index: index.name(),
    })
}

/// Given a [`RecordBatch`] of data from a [`SearchIndex`]'s associated [`TableProvider`], extract and format the primary key, so as to be ready for indexing into `S3Vectors`.
///
/// Formatting is:
///  - When there is a single [`Field`] in `primary_key`, the relevant [`ArrayRef`] is cast to a [`StringArray`] via [`arrow::compute::cast`].
///  - Otherwise, consider the [`Field`] as a sub-[`RecordBatch`] and convert to a string via [`arrow_json`].
pub fn extract_and_format_primary_key(
    index_name: &str,
    primary_key: &[Field],
    record: &RecordBatch,
) -> Result<Vec<Option<String>>, Box<Error>> {
    let schema = record.schema();
    match primary_key {
        [f] => {
            let Some((i, _)) = schema.column_with_name(f.name().as_str()) else {
                return ColumnNotFoundSnafu {
                    index: index_name.to_string(),
                    column: f.name().clone(),
                }
                .fail()
                .map_err(Box::from);
            };
            let c = record.column(i);

            // If already string like, continue
            if let Some(data) = convert_string_arrow_to_iterator!(c) {
                return Ok(to_string_vec(data));
            }

            // Otherwise cast to UTF8.
            let string_arr = arrow::compute::cast(&c, &arrow_schema::DataType::Utf8).context(
                IssueWithArrowProcessingSnafu {
                    index: index_name.to_string(),
                },
            )?;
            let Some(data) = convert_string_arrow_to_iterator!(string_arr) else {
                return Err(Box::from(Error::FailedToSerializePrimaryKey {
                    index: index_name.to_string(),
                    source: Box::from(format!(
                        "could not cast a '{}' column (column '{}') into string type",
                        f.data_type(),
                        f.name()
                    )),
                }));
            };
            Ok(to_string_vec(data))
        }
        [] => Err(Box::from(Error::NoPrimaryKeyField {
            index: index_name.to_string(),
        })),
        _ => {
            let mut primary_key_projection = vec![];
            for field in primary_key {
                let Some((idx, _)) = schema.column_with_name(field.name().as_str()) else {
                    return ColumnNotFoundSnafu {
                        index: index_name.to_string(),
                        column: field.name().clone(),
                    }
                    .fail()
                    .map_err(Box::from);
                };
                primary_key_projection.push(idx);
            }
            let pk =
                record
                    .project(&primary_key_projection)
                    .context(IssueWithArrowProcessingSnafu {
                        index: index_name.to_string(),
                    })?;

            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer
                .write_batches(&[&pk])
                .context(IssueWithArrowProcessingSnafu {
                    index: index_name.to_string(),
                })?;
            writer.finish().context(IssueWithArrowProcessingSnafu {
                index: index_name.to_string(),
            })?;

            let values = serde_json::from_reader::<_, Vec<Value>>(writer.into_inner().as_slice())
                .context(IssueWithJsonProcessingSnafu {
                index: index_name.to_string(),
            })?;

            values
                .into_iter()
                .map(|v| serde_json::to_string(&v).map(Some))
                .collect::<Result<Vec<_>, _>>()
                .context(IssueWithJsonProcessingSnafu {
                    index: index_name.to_string(),
                })
                .map_err(Box::from)
        }
    }
}

pub fn extract_and_format_metadata(
    index_name: &str,
    metadata_columns: &[String],
    record: &RecordBatch,
) -> Result<HashMap<String, Vec<Option<Value>>>, Box<Error>> {
    let schema = record.schema();
    let mut metadata_projection = vec![];
    for name in metadata_columns {
        let Some((idx, _)) = schema.column_with_name(name) else {
            return ColumnNotFoundSnafu {
                index: index_name.to_string(),
                column: name,
            }
            .fail()
            .map_err(Box::from);
        };
        metadata_projection.push(idx);
    }

    let encoder_options = EncoderOptions::default();
    let metadata: HashMap<String, Vec<Option<Value>>> = metadata_projection
        .iter()
        .filter_map(|i| {
            let c = record.column(*i);
            let field = Arc::new(schema.field(*i).clone());
            let name = field.name();

            let mut encoder = make_encoder(&field, c, &encoder_options).ok()?;

            let mut values = vec![];
            let mut value = Vec::new();
            for row in 0..c.len() {
                if encoder.is_null(row) {
                    values.push(None);
                } else {
                    encoder.encode(row, &mut value);
                    values.push(serde_json::from_slice(&value).ok());
                    value.clear();
                }
            }

            Some((name.clone(), values))
        })
        .collect();
    Ok(metadata)
}

fn to_string_vec<'a, I>(iter: I) -> Vec<Option<String>>
where
    I: Iterator<Item = Option<&'a str>>,
{
    iter.map(|opt| opt.map(ToString::to_string)).collect()
}

/// Embed the given `column_idx` from the [`RecordBatch`]s, assuming it is a String-like value.
///
/// Return results a nullable array of vectors. Null is original string is null or empty.
async fn embed_column(
    rb: &RecordBatch,
    column_idx: usize,
    model: Arc<dyn Embed>,
) -> Result<Vec<Option<Vec<f32>>>, Error> {
    let Some(data) = convert_string_arrow_to_iterator!(rb.column(column_idx)) else {
        return Ok(vec![]);
    };

    let mut nulls = vec![];
    let mut column = vec![];

    for (i, o) in data.enumerate() {
        if o.is_none() || o.is_some_and(str::is_empty) {
            nulls.push(i);
        } else if let Some(s) = o {
            column.push(s.to_string());
        }
    }

    let embedded_data = model
        .embed(EmbeddingInput::StringArray(column))
        .await
        .context(FailedToEmbedSnafu)?;

    Ok(distribute_nulls(embedded_data, nulls))
}

/// Update the embedding column in the `RecordBatch` with the computed embeddings.
fn update_embedding_column_in_batch(
    record: &RecordBatch,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch, Box<Error>> {
    let embedding_column_name = embedding_col(embedded_column_name);

    let schema = record.schema();
    let mut columns = record.columns().to_vec();

    // Create new embedding array that will replace the existing column or be added as a new column
    let embedding_array = create_embedding_array(embedding_vectors, dimension)?;

    // Check if the embedding column already exists
    let target_schema = if let Some((idx, _)) = schema.column_with_name(&embedding_column_name) {
        // Replace existing embedding column
        columns[idx] = embedding_array;
        schema
    } else {
        // Create new schema with the embedding column appended
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            &embedding_column_name,
            embedding_array.data_type().clone(),
            true,
        )));
        // Append embedding column
        columns.push(embedding_array);
        Arc::new(arrow_schema::Schema::new(fields))
    };

    RecordBatch::try_new(target_schema, columns)
        .context(CannotUpdateEmbeddingColumnSnafu)
        .map_err(Box::from)
}

/// Create an Arrow array from embedding vectors.
#[expect(clippy::cast_sign_loss)]
fn create_embedding_array(
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<Arc<dyn Array>, Box<Error>> {
    let mut dimension = dimension;
    if dimension <= 0 {
        // Fallback: determine embedding dimension from first non-null embedding
        dimension = i32::try_from(
            embedding_vectors
                .iter()
                .find_map(|opt| opt.as_ref().map(Vec::len))
                .unwrap_or(0),
        )
        .context(EmbeddingDimensionTooLargeSnafu)
        .map_err(Box::from)?;
        if dimension <= 0 {
            CannotDetermineEmbeddingDimensionSnafu {}
                .fail()
                .map_err(Box::from)?;
        }
    }

    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dimension);
    let field = Field::new_list_field(DataType::Float32, false);
    builder = builder.with_field(field);

    for embedding_opt in embedding_vectors {
        if let Some(embedding) = embedding_opt {
            // Optimized: append_slice automatically marks all values as valid
            // without needing to allocate a separate validity vector
            builder.values().append_slice(embedding);
            builder.append(true);
        } else {
            builder.values().append_nulls(dimension as usize);
            builder.append(false);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Filter out zero vectors (all values in the vector are 0.0)
#[expect(clippy::type_complexity)]
fn filter_zero_vectors(
    mut embeddings: Vec<Option<Vec<f32>>>,
    mut primary_keys: Vec<Option<String>>,
    mut metadata: HashMap<String, Vec<Option<Value>>>,
    index_name: &str,
) -> (
    Vec<Option<Vec<f32>>>,
    Vec<Option<String>>,
    HashMap<String, Vec<Option<Value>>>,
) {
    // Filter in reverse order to avoid index shifting when removing elements
    for i in (0..embeddings.len()).rev() {
        if let Some(embedding) = &embeddings[i]
            && embedding.iter().all(|&x| x == 0.0)
        {
            let key_str = primary_keys
                .get(i)
                .and_then(|k| k.as_ref().map(String::as_str))
                .unwrap_or("unknown");
            tracing::warn!(
                "Skipping record '{key_str}' for S3 Vector index '{index_name}': Embedding vector is all zeroes"
            );

            embeddings.remove(i);
            primary_keys.remove(i);
            for values in metadata.values_mut() {
                values.remove(i);
            }
        }
    }

    (embeddings, primary_keys, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeListArray, Float32Array, Float32Builder, StringArray};
    use arrow::datatypes::{DataType, Schema};

    // Helper function to create a test RecordBatch with text and embedding columns
    #[expect(clippy::cast_sign_loss)]
    fn create_test_record_batch_with_embeddings(
        texts: Vec<Option<&str>>,
        embeddings: Vec<Option<Vec<f32>>>,
        dim: i32,
    ) -> RecordBatch {
        let text_array = StringArray::from(texts);

        // Create embedding array
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
        let field = Field::new_list_field(DataType::Float32, false);
        builder = builder.with_field(field);
        for embedding_opt in embeddings {
            if let Some(embedding) = embedding_opt {
                // Optimized: append_slice is more efficient than append_values with manual validity
                builder.values().append_slice(&embedding);
                builder.append(true);
            } else {
                builder.values().append_nulls(dim as usize);
                builder.append(false);
            }
        }
        let embedding_array = builder.finish();

        let schema = Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new(
                "text_embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    dim,
                ),
                true,
            ),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(text_array), Arc::new(embedding_array)],
        )
        .expect("Failed to create test RecordBatch")
    }

    // Helper function to create a test RecordBatch with only text column
    fn create_test_record_batch_text_only(texts: Vec<Option<&str>>) -> RecordBatch {
        let text_array = StringArray::from(texts);
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(text_array)])
            .expect("Failed to create test RecordBatch with text only")
    }

    #[test]
    #[expect(clippy::float_cmp)]
    fn test_create_embedding_array_valid_embeddings() {
        let embeddings = vec![Some(vec![0.1, 0.2, 0.3]), None, Some(vec![0.7, 0.8, 0.9])];

        let result =
            create_embedding_array(&embeddings, 3).expect("Failed to create embedding array");

        let list_array = result
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("Result should be FixedSizeListArray");

        assert_eq!(list_array.len(), 3);
        assert!(!list_array.is_null(0));
        assert!(list_array.is_null(1));
        assert!(!list_array.is_null(2));

        // Check first embedding values
        let first_values = list_array.value(0);
        let first_floats = first_values
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Values should be Float32Array");
        assert_eq!(first_floats.value(0), 0.1);
        assert_eq!(first_floats.value(1), 0.2);
        assert_eq!(first_floats.value(2), 0.3);
    }

    #[test]
    fn test_create_embedding_array_empty_embeddings() {
        let embeddings: Vec<Option<Vec<f32>>> = vec![None, None];

        let result = create_embedding_array(&embeddings, 0);

        // Should fail because no valid embeddings to determine dimension
        assert!(result.is_err());
        assert!(matches!(
            *result.expect_err("Expected error for empty embeddings"),
            Error::CannotDetermineEmbeddingDimension
        ));
    }

    #[test]
    #[expect(clippy::float_cmp)]
    fn test_update_embedding_column_in_batch_with_existing_column() {
        let record = create_test_record_batch_with_embeddings(
            vec![Some("hello"), Some("world")],
            vec![None, None], // Existing embeddings are null
            3,
        );

        let new_embeddings = vec![Some(vec![0.1, 0.2, 0.3]), Some(vec![0.4, 0.5, 0.6])];

        let result = update_embedding_column_in_batch(&record, "text", &new_embeddings, 3)
            .expect("Failed to update embedding column");

        // Verify the updated batch has the new embeddings
        let embedding_column = result.column(1);
        let list_array = embedding_column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("Embedding column should be FixedSizeListArray");

        assert!(!list_array.is_null(0));
        assert!(!list_array.is_null(1));

        let first_values = list_array.value(0);
        let first_floats = first_values
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Values should be Float32Array");
        assert_eq!(first_floats.value(0), 0.1);
        assert_eq!(first_floats.value(1), 0.2);
        assert_eq!(first_floats.value(2), 0.3);
    }

    #[test]
    #[expect(clippy::float_cmp)]
    fn test_update_embedding_column_in_batch_append_embedding_column() {
        let record = create_test_record_batch_text_only(vec![Some("hello"), Some("world")]);

        let new_embeddings = vec![Some(vec![0.1, 0.2, 0.3]), Some(vec![0.4, 0.5, 0.6])];

        let result = update_embedding_column_in_batch(&record, "text", &new_embeddings, 3)
            .expect("Failed to handle missing embedding column");

        // Should append the embedding column with the correct name
        let expected_embedding_col = embedding_col("text");
        assert_eq!(result.num_columns(), record.num_columns() + 1);
        assert_eq!(result.num_rows(), record.num_rows());

        // Check that the last column is the embedding column
        let schema = result.schema();
        let embedding_field = schema.field(result.num_columns() - 1);
        assert_eq!(embedding_field.name(), &expected_embedding_col);

        // Check that the embedding column contains the correct values
        let embedding_column = result.column(result.num_columns() - 1);
        let list_array = embedding_column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("Embedding column should be FixedSizeListArray");

        assert!(!list_array.is_null(0));
        assert!(!list_array.is_null(1));

        let first_values = list_array.value(0);
        let first_floats = first_values
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Values should be Float32Array");
        assert_eq!(first_floats.value(0), 0.1);
        assert_eq!(first_floats.value(1), 0.2);
        assert_eq!(first_floats.value(2), 0.3);
    }

    #[test]
    fn test_filter_zero_vectors() {
        use serde_json::Value;
        use std::collections::HashMap;

        let embeddings = vec![
            Some(vec![1.0, 2.0]), // Keep
            Some(vec![0.0, 0.0]), // Filter out (zero vector)
            None,                 // Keep
            Some(vec![3.0, 4.0]), // Keep
        ];
        let keys = vec![
            Some("key1".to_string()),
            Some("key2".to_string()),
            Some("key3".to_string()),
            Some("key4".to_string()),
        ];
        let mut metadata = HashMap::new();
        metadata.insert(
            "test".to_string(),
            vec![
                Some(Value::String("a".to_string())),
                Some(Value::String("b".to_string())),
                Some(Value::String("c".to_string())),
                Some(Value::String("d".to_string())),
            ],
        );

        let (filtered_embeddings, filtered_keys, filtered_metadata) =
            filter_zero_vectors(embeddings, keys, metadata, "test_index");

        assert_eq!(filtered_embeddings.len(), 3);
        assert_eq!(filtered_keys.len(), 3);
        assert_eq!(filtered_metadata["test"].len(), 3);

        // Check that zero vector was filtered out
        assert_eq!(filtered_embeddings[0], Some(vec![1.0, 2.0]));
        assert_eq!(filtered_embeddings[1], None);
        assert_eq!(filtered_embeddings[2], Some(vec![3.0, 4.0]));
    }
}
