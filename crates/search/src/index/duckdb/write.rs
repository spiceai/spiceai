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

use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListBuilder, Float32Builder, LargeStringArray, RecordBatch, StringArray,
    StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};

use llms::embeddings::{Embed, EmbeddingInput};

use snafu::{ResultExt, Snafu};
use util::{convert_string_arrow_to_iterator, distribute_nulls};

use crate::index::{
    EmbeddingDefect, SkippedEmbeddings, duckdb::DuckDBVectorIndex, embedding_col, embedding_defect,
};

#[derive(Debug, Snafu)]
pub(super) enum WriteError {
    #[snafu(display(
        "Failed to compute DuckDB vector embeddings: embedded column '{column}' not found in record batch."
    ))]
    ColumnNotFound { column: String },

    #[snafu(display(
        "Failed to compute DuckDB vector embeddings: embedded column '{column}' has non-string type {data_type}; expected a Utf8/LargeUtf8/Utf8View column."
    ))]
    EmbeddedColumnNotString { column: String, data_type: String },

    #[snafu(display("Failed to compute DuckDB vector embeddings: {source}"))]
    FailedToEmbed { source: llms::embeddings::Error },

    #[snafu(display("Failed to build DuckDB vector embedding column: {source}"))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display(
        "Failed to build DuckDB vector embedding column: embedding dimension mismatch at row {row_index}: expected {expected}, got {actual}."
    ))]
    EmbeddingDimensionMismatch {
        expected: usize,
        actual: usize,
        row_index: usize,
    },
}

pub(super) async fn write_embeddings(
    index: &DuckDBVectorIndex,
    record: RecordBatch,
) -> Result<RecordBatch, WriteError> {
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        return ColumnNotFoundSnafu {
            column: index.embedded_column.clone(),
        }
        .fail();
    };

    let embedding_vectors = embed_column(
        &record,
        embedded_column_idx,
        index.embedded_column.as_str(),
        Arc::clone(&index.compute_query),
    )
    .await?;

    update_embedding_column_in_batch(
        &record,
        &index.embedded_column,
        &embedding_vectors,
        index.dims,
    )
}

async fn embed_column(
    rb: &RecordBatch,
    column_idx: usize,
    column_name: &str,
    model: Arc<dyn Embed>,
) -> Result<Vec<Option<Vec<f32>>>, WriteError> {
    let column_arr = rb.column(column_idx);
    let iter_opt: Option<Box<dyn Iterator<Item = Option<&str>> + Send>> =
        convert_string_arrow_to_iterator!(column_arr);
    let Some(data) = iter_opt else {
        return EmbeddedColumnNotStringSnafu {
            column: column_name.to_string(),
            data_type: column_arr.data_type().to_string(),
        }
        .fail();
    };

    let mut nulls = Vec::new();
    let mut column = Vec::new();
    for (i, value) in data.enumerate() {
        if value.is_none_or(str::is_empty) {
            nulls.push(i);
        } else if let Some(s) = value {
            column.push(s.to_string());
        }
    }

    if column.is_empty() {
        return Ok(vec![None; rb.num_rows()]);
    }

    let embedded = model
        .embed(EmbeddingInput::StringArray(column))
        .await
        .context(FailedToEmbedSnafu)?;

    Ok(distribute_nulls(embedded, nulls))
}

fn update_embedding_column_in_batch(
    record: &RecordBatch,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch, WriteError> {
    let embedding_column_name = embedding_col(embedded_column_name);
    let schema = record.schema();
    let mut columns = record.columns().to_vec();
    let embedding_array = create_embedding_array(embedding_vectors, dimension)?;

    let target_schema = if let Some((idx, _)) = schema.column_with_name(&embedding_column_name) {
        columns[idx] = embedding_array;
        schema
    } else {
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            &embedding_column_name,
            embedding_array.data_type().clone(),
            true,
        )));
        columns.push(embedding_array);
        Arc::new(Schema::new(fields))
    };

    RecordBatch::try_new(target_schema, columns).context(ArrowSnafu)
}

#[expect(clippy::cast_sign_loss)]
fn create_embedding_array(
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<Arc<dyn Array>, WriteError> {
    let dim = if dimension > 0 {
        dimension
    } else {
        i32::try_from(
            embedding_vectors
                .iter()
                .find_map(|value| value.as_ref().map(Vec::len))
                .unwrap_or(1),
        )
        .unwrap_or(1)
        .max(1)
    };
    let expected = dim as usize;

    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
    builder = builder.with_field(Field::new_list_field(DataType::Float32, false));

    let mut non_finite = SkippedEmbeddings::default();
    let mut all_zero = SkippedEmbeddings::default();

    for (row, embedding) in embedding_vectors.iter().enumerate() {
        // An unusable vector must not reach the column: HNSW builds its neighbour lists
        // from these values, so one undefined distance degrades the results returned for
        // other rows. Storing a null instead keeps the row and its data — it is simply not
        // searchable, the same state a row with no text to embed is already in.
        let usable = match embedding {
            Some(vector) if vector.len() == expected => match embedding_defect(vector) {
                Some(EmbeddingDefect::NonFinite) => {
                    non_finite.record(row);
                    None
                }
                Some(EmbeddingDefect::AllZero) => {
                    all_zero.record(row);
                    None
                }
                None => Some(vector),
            },
            Some(vector) => {
                return Err(WriteError::EmbeddingDimensionMismatch {
                    expected,
                    actual: vector.len(),
                    row_index: row,
                });
            }
            None => None,
        };

        if let Some(vector) = usable {
            builder.values().append_slice(vector);
            builder.append(true);
        } else {
            // Store `f32` child values, not `Option<f32>`; the list slot represents a null embedding.
            builder.values().append_value_n(0.0, expected);
            builder.append(false);
        }
    }

    non_finite.warn("the DuckDB vector index", EmbeddingDefect::NonFinite);
    all_zero.warn("the DuckDB vector index", EmbeddingDefect::AllZero);

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing was checked on this path at all before the fix — `validate_vector` was only
    /// ever called on the query vector — so an unusable vector was written into the
    /// HNSW-indexed column, where an undefined distance degrades the neighbour lists other
    /// rows are found through (regression test for #13089).
    ///
    /// The row keeps its slot either way: dropping it would drop the source data the
    /// accelerated table exists to serve, and would desynchronize this column from the
    /// other columns of the same `RecordBatch`. Which vectors count as unusable is
    /// [`crate::index::embedding_defect`]'s contract and is tested there.
    #[test]
    fn an_unusable_embedding_becomes_a_null_slot_without_dropping_its_row() {
        let embeddings = vec![
            Some(vec![1.0, 2.0, 3.0]),               // usable
            Some(vec![1.0, f32::NAN, 3.0]),          // one NaN among real values
            Some(vec![f32::INFINITY, 2.0, 3.0]),     // +inf
            Some(vec![1.0, 2.0, f32::NEG_INFINITY]), // -inf
            Some(vec![0.0, 0.0, 0.0]),               // no direction
            None,                                    // nothing to embed
            Some(vec![4.0, 5.0, 6.0]),               // usable
        ];

        let array = create_embedding_array(&embeddings, 3)
            .expect("the batch is written, minus the unusable vectors");

        assert_eq!(array.len(), 7, "every row keeps its slot");
        assert_eq!(
            (0..7).map(|i| array.is_null(i)).collect::<Vec<_>>(),
            vec![false, true, true, true, true, true, false]
        );
    }

    /// A dimension mismatch stays an error: it means the column and the model disagree
    /// about the index's shape, which no row-level disposition can paper over.
    #[test]
    fn a_dimension_mismatch_still_fails_the_write() {
        let embeddings = vec![Some(vec![1.0, 2.0])];

        let err = create_embedding_array(&embeddings, 3)
            .expect_err("a wrong-width vector is a configuration error, not a bad row");

        assert!(matches!(
            err,
            WriteError::EmbeddingDimensionMismatch { row_index: 0, .. }
        ));
    }
}
