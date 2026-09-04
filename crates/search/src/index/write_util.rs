/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Store-agnostic helpers shared by external-store [`crate::index::VectorIndex`]
//! implementations (S3 Vectors, in-memory): primary-key formatting, search-column
//! embedding, and embedding-column construction on write.

use std::{num::TryFromIntError, sync::Arc};

use arrow::array::{
    Array, FixedSizeListBuilder, Float32Builder, LargeStringArray, RecordBatch, StringArray,
    StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use itertools::Itertools;
use llms::embeddings::{Embed, EmbeddingInput};
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use util::{convert_string_arrow_to_iterator, distribute_nulls};

use crate::index::embedding_col;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
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

    #[snafu(display("Cannot update embedding column in record batch: {source}"))]
    CannotUpdateEmbeddingColumn { source: arrow::error::ArrowError },

    #[snafu(display("Cannot sort record batch columns alphabetically: {source}"))]
    CannotSortColumnsAlphabetically { source: arrow::error::ArrowError },

    #[snafu(display(
        "Cannot create embedding array: no valid embeddings found to determine dimension"
    ))]
    CannotDetermineEmbeddingDimension,

    #[snafu(display("Embedding dimension is too large to fit into an i32"))]
    EmbeddingDimensionTooLarge { source: TryFromIntError },

    #[snafu(display(
        "Embedding dimension mismatch: expected {expected} but got {actual} at row {row_index}"
    ))]
    EmbeddingDimensionMismatch {
        expected: usize,
        actual: usize,
        row_index: usize,
    },
}

/// Given a [`RecordBatch`] of data from a [`crate::index::SearchIndex`]'s associated
/// `TableProvider`, extract and format the primary key into one string per row.
///
/// Formatting is:
///  - When there is a single [`Field`] in `primary_key`, the relevant array is cast to a
///    [`StringArray`] via [`arrow::compute::cast`].
///  - Otherwise, consider the [`Field`]s as a sub-[`RecordBatch`] and convert to a string via
///    [`arrow_json`].
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
                .map(|v| {
                    if composite_primary_key_is_all_null(&v) {
                        Ok(None)
                    } else {
                        serde_json::to_string(&v).map(Some)
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .context(IssueWithJsonProcessingSnafu {
                    index: index_name.to_string(),
                })
                .map_err(Box::from)
        }
    }
}

fn to_string_vec<'a, I>(iter: I) -> Vec<Option<String>>
where
    I: Iterator<Item = Option<&'a str>>,
{
    iter.map(|opt| opt.map(ToString::to_string)).collect()
}

fn composite_primary_key_is_all_null(value: &Value) -> bool {
    match value {
        // `arrow_json` omits null fields by default, so an all-null composite key serializes
        // to an empty object rather than one with explicit nulls; `all()` on the empty
        // iterator is vacuously true, which is the behavior we want here.
        Value::Object(fields) => fields.values().all(Value::is_null),
        _ => false,
    }
}

/// Embed the given `column_idx` from the [`RecordBatch`], assuming it is a String-like value.
///
/// Returns a nullable array of vectors. Null if the original string is null or empty.
pub async fn embed_column(
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
        if o.is_none_or(str::is_empty) {
            nulls.push(i);
        } else if let Some(s) = o {
            column.push(s.to_string());
        }
    }

    // Every row was null or empty; skip the embed call (some providers reject an
    // empty input array) and return a None per row.
    if column.is_empty() {
        return Ok(vec![None; rb.num_rows()]);
    }

    let embedded_data = model
        .embed(EmbeddingInput::StringArray(column))
        .await
        .context(FailedToEmbedSnafu)?;

    Ok(distribute_nulls(embedded_data, nulls))
}

/// Update the embedding column in the `RecordBatch` with the computed embeddings.
pub fn update_embedding_column_in_batch(
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
pub fn create_embedding_array(
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

    let expected_dim = dimension as usize;
    for (row_index, embedding_opt) in embedding_vectors.iter().enumerate() {
        if let Some(embedding) = embedding_opt {
            // Validate embedding dimension matches expected dimension
            if embedding.len() != expected_dim {
                return Err(Box::new(Error::EmbeddingDimensionMismatch {
                    expected: expected_dim,
                    actual: embedding.len(),
                    row_index,
                }));
            }
            // Optimized: append_slice automatically marks all values as valid
            // without needing to allocate a separate validity vector
            builder.values().append_slice(embedding);
            builder.append(true);
        } else {
            // Store `f32` child values, not `Option<f32>`. Use zero-valued
            // child slots and represent a null embedding at the list level.
            builder.values().append_value_n(0.0, expected_dim);
            builder.append(false);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// The primary keys a write must remove from its index because it could not index
/// the rows they name.
///
/// A vector write drops a row it cannot embed — a NULL or empty search text, or a
/// vector with no defined direction under any metric the index offers. Dropping it
/// from the write is not the same as removing it from the index: whatever the index
/// already holds under that key stays there, so a row rewritten from an indexable
/// value to a rejected one goes on being returned at its previous vector while the
/// write's own log line says the row was not indexed.
///
/// A stale vector is a wrong search result — the index asserts the row matches text
/// it no longer contains. An absent one is a correct-by-omission result that agrees
/// with what the write reported, and it costs nothing that is not recoverable: a
/// vector index is derived from its source table, so the row itself is untouched and
/// the next indexable write restores it. Removing the entry is therefore the one
/// behaviour every backend can implement with the delete primitive it already has,
/// and it is what these paths do.
///
/// `indexed` is what this same write is about to store. A key in both — the batch
/// carries the key twice, once indexable and once not — is not evicted: the write
/// that follows re-establishes it, so issuing a delete for it would only cost a round
/// trip. Callers must still delete before they write, so the two orders agree.
pub fn keys_to_evict<'a>(
    rejected: impl IntoIterator<Item = String>,
    indexed: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let indexed: std::collections::HashSet<&str> = indexed.into_iter().collect();
    rejected
        .into_iter()
        .filter(|key| !indexed.contains(key.as_str()))
        .unique()
        .collect()
}

/// Reorder a [`RecordBatch`]'s columns alphabetically by field name.
///
/// Because of limitations of `DFSchema::logically_equivalent_names_and_types` and its use in
/// `MemTable`, index write output must be in the same column order as outputted by
/// [`crate::index::VectorScanTableProvider`], which sorts alphabetically.
pub fn sort_columns_alphabetically(record: RecordBatch) -> Result<RecordBatch, Box<Error>> {
    let (schema, arr, _) = record.into_parts();
    let (arrs, fields): (Vec<_>, Vec<_>) = arr
        .into_iter()
        .zip(schema.fields())
        .sorted_by_key(|(_, f)| f.name())
        .unzip();

    RecordBatch::try_new(
        Arc::new(Schema::new(fields.into_iter().cloned().collect::<Vec<_>>())),
        arrs,
    )
    .context(CannotSortColumnsAlphabeticallySnafu)
    .map_err(Box::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeListArray, Float32Array, Float32Builder, Int32Array, StringArray};
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
                builder.values().append_value_n(0.0, dim as usize);
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

    fn create_composite_primary_key_batch(
        id: Vec<Option<i32>>,
        tenant: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("tenant", DataType::Utf8, true),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(id)),
                Arc::new(StringArray::from(tenant)),
            ],
        )
        .expect("valid composite primary key batch")
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
        assert_eq!(
            list_array.values().null_count(),
            0,
            "null vectors must not put nulls in a non-nullable child array"
        );

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

    /// Test that create_embedding_array correctly detects dimension mismatch.
    #[test]
    fn test_embedding_dimension_mismatch() {
        // Embeddings with mismatched dimensions: expected 2, but row 1 has 3
        let embeddings = vec![
            Some(vec![0.1, 0.2]),      // dimension 2 - correct
            Some(vec![0.3, 0.4, 0.5]), // dimension 3 - MISMATCH!
        ];

        let result = create_embedding_array(&embeddings, 2);

        assert!(
            result.is_err(),
            "Should fail when embedding dimensions don't match"
        );
        let error = *result.expect_err("Expected error for dimension mismatch");
        match error {
            Error::EmbeddingDimensionMismatch {
                expected,
                actual,
                row_index,
            } => {
                assert_eq!(expected, 2, "Expected dimension should be 2");
                assert_eq!(actual, 3, "Actual dimension should be 3");
                assert_eq!(row_index, 1, "Mismatch should be at row 1");
            }
            _ => panic!("Expected EmbeddingDimensionMismatch error, got: {error:?}"),
        }
    }

    /// Test that create_embedding_array accepts embeddings with correct dimensions.
    #[test]
    fn test_embedding_dimension_correct() {
        let embeddings = vec![
            Some(vec![0.1, 0.2, 0.3]),
            Some(vec![0.4, 0.5, 0.6]),
            None, // Null embeddings should be handled correctly
            Some(vec![0.7, 0.8, 0.9]),
        ];

        let result = create_embedding_array(&embeddings, 3);
        assert!(
            result.is_ok(),
            "Should succeed when all embedding dimensions match"
        );
    }

    #[test]
    fn test_sort_columns_alphabetically() {
        let schema = Schema::new(vec![
            Field::new("zeta", DataType::Utf8, true),
            Field::new("alpha", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .expect("valid batch");

        let sorted = sort_columns_alphabetically(batch).expect("sorts");
        let names: Vec<_> = sorted
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["alpha".to_string(), "zeta".to_string()]);
    }

    fn owned(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| (*k).to_string()).collect()
    }

    #[test]
    fn keys_to_evict_names_every_rejected_key_the_write_does_not_also_store() {
        let evicted = keys_to_evict(owned(&["a", "b"]), ["c"]);
        assert_eq!(
            evicted,
            owned(&["a", "b"]),
            "a rejected key is what leaves a stale vector behind, so it must be evicted"
        );
    }

    /// The batch carries one key twice — once indexable, once not. The write that follows
    /// re-establishes it, so a delete for it would be undone by this same write.
    #[test]
    fn keys_to_evict_skips_a_key_the_same_write_also_indexes() {
        let evicted = keys_to_evict(owned(&["a", "b"]), ["b"]);
        assert_eq!(evicted, owned(&["a"]));
    }

    #[test]
    fn keys_to_evict_deduplicates_a_key_rejected_more_than_once() {
        let evicted = keys_to_evict(owned(&["a", "a", "b", "a"]), std::iter::empty());
        assert_eq!(
            evicted,
            owned(&["a", "b"]),
            "one delete per key is enough; repeating it only costs request size"
        );
    }

    #[test]
    fn keys_to_evict_is_empty_when_the_write_rejected_nothing() {
        assert!(keys_to_evict(Vec::new(), ["a", "b"]).is_empty());
    }

    #[test]
    fn test_extract_and_format_primary_key_composite_all_null_returns_none() {
        let batch = create_composite_primary_key_batch(
            vec![Some(1), None, None],
            vec![Some("a"), Some("b"), None],
        );
        let primary_key = vec![
            Field::new("id", DataType::Int32, true),
            Field::new("tenant", DataType::Utf8, true),
        ];

        let keys =
            extract_and_format_primary_key("test_index", &primary_key, &batch).expect("keys");

        assert_eq!(keys[0].as_deref(), Some("{\"id\":1,\"tenant\":\"a\"}"));
        assert_eq!(keys[1].as_deref(), Some("{\"tenant\":\"b\"}"));
        assert_eq!(keys[2], None);
    }
}
