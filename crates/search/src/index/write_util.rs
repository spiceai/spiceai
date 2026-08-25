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
    index_name: &str,
    record: &RecordBatch,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch, Box<Error>> {
    let embedding_column_name = embedding_col(embedded_column_name);

    let schema = record.schema();
    let mut columns = record.columns().to_vec();

    // Create new embedding array that will replace the existing column or be added as a new column
    let embedding_array = create_embedding_array(index_name, embedding_vectors, dimension)?;

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

/// The position of the first element of `embedding` that is not a finite number.
///
/// This is the one definition of "this vector cannot be scored" that every vector write
/// path shares. A `NaN` or infinite component has no defined distance under any metric an
/// index offers — cosine, L2 and inner product alike — so it poisons every score computed
/// against it. An all-zero vector is a different case and deliberately not covered here:
/// it is well defined for L2 and inner product, and `cosine_distance` answers `0.5` for it.
#[must_use]
pub fn first_non_finite(embedding: &[f32]) -> Option<usize> {
    embedding.iter().position(|value| !value.is_finite())
}

/// Whether every component of `embedding` is exactly zero.
///
/// This is the predicate behind the per-record "all zeroes" report. It deliberately does
/// not treat `NaN` as a zero: a vector such as `[0.0, NaN]` is already named by
/// [`warn_non_finite_embeddings`] in one batched line, so counting it as all-zero here
/// would report the same row twice — once per batch and once per record — and a wholly
/// poisoned batch would emit a line per row. The two predicates together still cover the
/// same rows, because any `NaN` a looser test would have absorbed is non-finite.
#[must_use]
pub fn is_finite_all_zero(embedding: &[f32]) -> bool {
    embedding.iter().all(|&value| value == 0.0)
}

/// The remedy clause every non-finite-embedding warning ends with, kept in one place so the
/// advice and the docs link cannot drift between the paths that skip a record and the ones
/// that null its embedding.
pub const EMBEDDING_REMEDY: &str = "Re-embed the affected rows, or check the embedding model configured for this dataset. \
     See: https://spiceai.org/docs/components/embeddings";

/// The maximum number of row indexes named in a [`non_finite_embedding_warning`].
const NON_FINITE_SAMPLE_LIMIT: usize = 5;

/// Report the rows an embedding builder could not index, if there were any.
///
/// Every write path that meets a non-finite embedding emits through here — the ones that
/// null the embedding and the ones that drop the record entirely — so the emptiness check,
/// the log level and the wording cannot drift apart between them. The message states the
/// consequence both share ("not indexed") rather than the storage outcome, which differs.
pub fn warn_non_finite_embeddings(index_name: &str, affected_rows: &[usize], total_rows: usize) {
    if affected_rows.is_empty() {
        return;
    }
    tracing::warn!(
        "{}",
        non_finite_embedding_warning(index_name, affected_rows, total_rows)
    );
}

/// One line explaining why some rows are not searchable, built in a pure function so a
/// reword cannot quietly drop the index name, the consequence, or the docs link.
///
/// Callers disagree on **two** things, so the line may assert neither. They disagree on the
/// storage outcome — `create_embedding_array` and the DuckDB builder null the embedding and
/// keep the row, Elasticsearch's document builder drops it — and they disagree on what
/// happens to a vector already indexed for that row: the two null-the-embedding callers
/// overwrite it, so it stops matching, while the drop-the-record caller leaves it in place
/// and searchable at its older value. "This write did not index those rows" is the only
/// consequence all three deliver. Both an absolute ("will never return them") and a promise
/// that the old vector survives would each be false for some caller.
#[must_use]
pub fn non_finite_embedding_warning(
    index_name: &str,
    affected_rows: &[usize],
    total_rows: usize,
) -> String {
    let sample = affected_rows
        .iter()
        .take(NON_FINITE_SAMPLE_LIMIT)
        .map(usize::to_string)
        .join(", ");
    let ellipsis = if affected_rows.len() > NON_FINITE_SAMPLE_LIMIT {
        ", ..."
    } else {
        ""
    };
    format!(
        "Index '{index_name}': {} of {total_rows} embeddings contain a NaN or infinite value, so this write did not index those rows and vector search will not match them on this write's data (rows {sample}{ellipsis}). {EMBEDDING_REMEDY}",
        affected_rows.len()
    )
}

/// One line explaining why a single all-zero-embedding record is not searchable, built in a
/// pure function so a reword cannot quietly drop the key, the index, the consequence or the
/// docs link.
///
/// The cause is deliberately narrower than [`non_finite_embedding_warning`]'s: this line is
/// only ever reached for a vector whose every component is a finite zero, so it must not
/// mention NaN. A vector mixing zeroes with a NaN belongs to the batched non-finite report,
/// and claiming "all zeroes" for it would name a cause the row does not have.
///
/// The *consequence*, by contrast, must stay narrower than it is tempting to write. What
/// happens to a vector already indexed for this record is not a property of this call site:
/// an appending write leaves it in place and searchable at its older value (#13504), while a
/// `WriteWindow::ReplaceAll` refresh stages only the surviving rows and swaps the whole set
/// in at `on_write_complete`, discarding it. So this line may not promise either outcome —
/// only that this write did not index the record.
#[must_use]
pub fn all_zero_embedding_warning(index_kind: &str, index_name: &str, key: &str) -> String {
    format!(
        "Skipping record '{key}' for {index_kind} index '{index_name}': its embedding is all zeroes, which has no direction to compare against, so this write did not index the record and vector search will not match it on this write's data. {EMBEDDING_REMEDY}"
    )
}

/// Create an Arrow array from embedding vectors.
///
/// An embedding carrying a non-finite value is stored as a NULL list slot rather than
/// indexed — see [`first_non_finite`]. The row itself is kept, because this builds one
/// column of a batch whose other columns already have their rows.
#[expect(clippy::cast_sign_loss)]
pub fn create_embedding_array(
    index_name: &str,
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
    let mut non_finite_rows: Vec<usize> = Vec::new();
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
            if first_non_finite(embedding).is_some() {
                non_finite_rows.push(row_index);
                builder.values().append_value_n(0.0, expected_dim);
                builder.append(false);
                continue;
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

    warn_non_finite_embeddings(index_name, &non_finite_rows, embedding_vectors.len());

    Ok(Arc::new(builder.finish()))
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

        let result = create_embedding_array("test_index", &embeddings, 3)
            .expect("Failed to create embedding array");

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

        let result = create_embedding_array("test_index", &embeddings, 0);

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

        let result =
            update_embedding_column_in_batch("test_index", &record, "text", &new_embeddings, 3)
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

        let result =
            update_embedding_column_in_batch("test_index", &record, "text", &new_embeddings, 3)
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

        let result = create_embedding_array("test_index", &embeddings, 2);

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

        let result = create_embedding_array("test_index", &embeddings, 3);
        assert!(
            result.is_ok(),
            "Should succeed when all embedding dimensions match"
        );
    }

    /// A vector with one bad component among real values must not be treated as usable —
    /// the pre-fix predicate tested `all` and let `[NaN, 2.0, 3.0]` through (regression
    /// test for #13089).
    #[test]
    fn first_non_finite_rejects_a_single_bad_component() {
        assert_eq!(first_non_finite(&[f32::NAN, 2.0, 3.0]), Some(0));
        assert_eq!(first_non_finite(&[1.0, f32::INFINITY, 3.0]), Some(1));
        assert_eq!(first_non_finite(&[1.0, 2.0, f32::NEG_INFINITY]), Some(2));
        assert_eq!(first_non_finite(&[f32::NAN, f32::NAN]), Some(0));
    }

    /// An all-zero vector is well defined for L2 and inner product, and `cosine_distance`
    /// answers 0.5 for it, so it is deliberately not a non-finite defect.
    #[test]
    fn first_non_finite_accepts_finite_vectors_including_all_zero() {
        assert_eq!(first_non_finite(&[0.0, 0.0, 0.0]), None);
        assert_eq!(first_non_finite(&[-1.5, 0.0, 2.5]), None);
        assert_eq!(first_non_finite(&[]), None);
    }

    /// The per-record report is for finite all-zero vectors only. A predicate of
    /// `v == 0.0 || v.is_nan()` also matches `[0.0, NaN]` and `[NaN, NaN]`, which the
    /// batched non-finite warning has already named — reporting them here too turns one
    /// degraded embedding response into a line per row.
    #[test]
    fn is_finite_all_zero_excludes_a_nan_padded_vector() {
        assert!(is_finite_all_zero(&[0.0, 0.0, 0.0]));
        assert!(is_finite_all_zero(&[0.0, -0.0]));
        assert!(!is_finite_all_zero(&[0.0, f32::NAN]));
        assert!(!is_finite_all_zero(&[f32::NAN, f32::NAN]));
        assert!(!is_finite_all_zero(&[0.0, 1.0]));
    }

    /// Splitting the report between [`is_finite_all_zero`] and [`first_non_finite`] must
    /// change only which warning a row gets, never whether it is written. Every vector the
    /// looser `v == 0.0 || v.is_nan()` test absorbed carries a `NaN`, so the non-finite
    /// half still claims it.
    #[test]
    fn the_split_predicates_filter_the_same_vectors_as_the_combined_one() {
        let vectors: [&[f32]; 9] = [
            &[],
            &[0.0, 0.0],
            &[0.0, -0.0],
            &[0.0, f32::NAN],
            &[f32::NAN, f32::NAN],
            &[1.0, f32::NAN],
            &[1.0, f32::INFINITY],
            &[1.0, 2.0],
            &[0.0, 1.0],
        ];
        for vector in vectors {
            let combined = vector.iter().all(|&v| v == 0.0 || v.is_nan())
                || first_non_finite(vector).is_some();
            let split = is_finite_all_zero(vector) || first_non_finite(vector).is_some();
            assert_eq!(
                split, combined,
                "{vector:?} changes whether the row is written, but the split only moves \
                 which warning names it"
            );
        }
    }

    /// The warning is what a user gets to explain a row that vanished from search, so it
    /// must keep naming the index, the count, the consequence, and the docs link.
    ///
    /// The consequence has to be the one every caller produces. Some paths null the
    /// embedding and keep the row; `memory`, `s3_vectors` and Elasticsearch's document
    /// builder drop the record instead. "Not indexed" is true of all of them, so a reword
    /// back to a storage outcome would make the single shared line wrong for three of the
    /// four backends emitting it.
    #[test]
    fn non_finite_warning_names_index_consequence_and_docs() {
        let message = non_finite_embedding_warning("my_index", &[1, 4], 10);
        assert!(message.contains("'my_index'"), "{message}");
        assert!(message.contains("2 of 10"), "{message}");
        assert!(message.contains("did not index"), "{message}");
        assert!(
            !message.contains("stored without an embedding"),
            "the shared line must not promise a storage outcome the record-dropping callers do not produce: {message}"
        );
        assert!(
            message.contains("this write did not index those rows"),
            "{message}"
        );
        assert!(
            !message.contains("never return"),
            "an absolute one caller does not deliver — Elasticsearch's document builder leaves the previous document searchable at its older vector: {message}"
        );
        assert!(
            !message.contains("stays searchable"),
            "the mirror error: the two null-the-embedding callers overwrite the old vector, so the shared line cannot promise it survives either: {message}"
        );
        assert!(message.contains("rows 1, 4"), "{message}");
        assert!(
            message.contains("https://spiceai.org/docs/components/embeddings"),
            "{message}"
        );
    }

    /// The per-record all-zero line has to carry what the batched non-finite one carries: the
    /// record, the index, what the user will observe, and where to go. Only the cause differs.
    ///
    /// It must *not* mention NaN. The predicate feeding it is `is_finite_all_zero`, so a
    /// vector mixing zeroes with a NaN reaches the batched non-finite report instead; naming
    /// NaN here would describe a cause the reported row does not have.
    #[test]
    fn all_zero_warning_names_record_consequence_and_docs() {
        let message = all_zero_embedding_warning("memory vector", "my_index", "row-7");
        assert!(message.contains("'row-7'"), "{message}");
        assert!(
            message.contains("memory vector index 'my_index'"),
            "{message}"
        );
        assert!(message.contains("all zeroes"), "{message}");
        assert!(
            message.contains("this write did not index the record"),
            "the line must state the consequence, not only the cause: {message}"
        );
        assert!(
            message.contains("this write did not index the record"),
            "{message}"
        );
        assert!(
            !message.contains("never return"),
            "an absolute an appending write does not deliver — the previously indexed vector stays searchable (#13504): {message}"
        );
        assert!(
            !message.contains("stays searchable"),
            "and not the mirror either: a ReplaceAll refresh stages only survivors and swaps the whole set in, discarding that older vector: {message}"
        );
        assert!(
            message.contains("Re-embed the affected rows"),
            "the line must give an actionable remedy: {message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/components/embeddings"),
            "{message}"
        );
        assert!(
            !message.contains("NaN"),
            "a NaN-bearing vector is reported by the batched non-finite line, so this one must not claim NaN as the cause: {message}"
        );
    }

    /// Both causes point at the same fix, so the remedy is shared rather than restated — a
    /// divergence would give two backends two different answers to the same question.
    #[test]
    fn both_embedding_warnings_share_one_remedy() {
        let all_zero = all_zero_embedding_warning("S3 Vector", "idx", "k");
        let non_finite = non_finite_embedding_warning("idx", &[0], 1);
        assert!(all_zero.contains(EMBEDDING_REMEDY), "{all_zero}");
        assert!(non_finite.contains(EMBEDDING_REMEDY), "{non_finite}");
    }

    /// Only the first few row indexes are named, so a whole-batch failure cannot produce a
    /// log line proportional to the batch.
    #[test]
    fn non_finite_warning_truncates_a_long_row_list() {
        let rows: Vec<usize> = (0..50).collect();
        let message = non_finite_embedding_warning("my_index", &rows, 50);
        assert!(message.contains("50 of 50"), "{message}");
        assert!(message.contains("rows 0, 1, 2, 3, 4, ..."), "{message}");
        assert!(!message.contains(", 5,"), "{message}");
    }

    /// A partially non-finite embedding is stored as a NULL list slot, not indexed, and the
    /// rest of the batch is untouched (regression test for #13089).
    #[test]
    fn create_embedding_array_nulls_a_non_finite_embedding() {
        let embeddings = vec![
            Some(vec![0.1, 0.2, 0.3]),
            Some(vec![f32::NAN, 2.0, 3.0]),
            Some(vec![1.0, f32::INFINITY, 3.0]),
            None,
            Some(vec![0.0, 0.0, 0.0]),
        ];

        let array = create_embedding_array("test_index", &embeddings, 3)
            .expect("builds the embedding array");
        let list = array
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeListArray>()
            .expect("FixedSizeListArray");

        assert_eq!(list.len(), 5);
        assert!(list.is_valid(0), "a finite embedding stays indexed");
        assert!(list.is_null(1), "a NaN component nulls the embedding");
        assert!(list.is_null(2), "an infinite component nulls the embedding");
        assert!(list.is_null(3), "a missing embedding stays null");
        assert!(
            list.is_valid(4),
            "an all-zero embedding is finite and stays indexed"
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
