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
    Index,
    duckdb::DuckDBVectorIndex,
    embedding_col,
    write_util::{first_non_finite, non_finite_embedding_warning},
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
        index.name(),
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
    index_name: &str,
    record: &RecordBatch,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch, WriteError> {
    let embedding_column_name = embedding_col(embedded_column_name);
    let schema = record.schema();
    let mut columns = record.columns().to_vec();
    let embedding_array = create_embedding_array(index_name, embedding_vectors, dimension)?;

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
    index_name: &str,
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

    let mut non_finite_rows: Vec<usize> = Vec::new();
    for (row, embedding) in embedding_vectors.iter().enumerate() {
        match embedding {
            // A non-finite component has no defined distance under any metric the HNSW
            // index offers, so the row is stored with a NULL embedding rather than indexed.
            Some(vector) if vector.len() == expected && first_non_finite(vector).is_none() => {
                builder.values().append_slice(vector);
                builder.append(true);
            }
            Some(vector) if vector.len() == expected => {
                non_finite_rows.push(row);
                builder.values().append_value_n(0.0, expected);
                builder.append(false);
            }
            Some(vector) => {
                return Err(WriteError::EmbeddingDimensionMismatch {
                    expected,
                    actual: vector.len(),
                    row_index: row,
                });
            }
            None => {
                // Store `f32` child values, not `Option<f32>`; the list slot represents a null embedding.
                builder.values().append_value_n(0.0, expected);
                builder.append(false);
            }
        }
    }

    if !non_finite_rows.is_empty() {
        tracing::warn!(
            "{}",
            non_finite_embedding_warning(index_name, &non_finite_rows, embedding_vectors.len())
        );
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeListArray};

    use super::create_embedding_array;

    /// The HNSW write path never screened its vectors — `validate_vector` existed but was
    /// only ever called on the query side, so a `[NaN, 2.0]` embedding was indexed and every
    /// distance computed against it came back NaN (regression test for #13089).
    #[test]
    fn a_non_finite_embedding_is_nulled_rather_than_indexed() {
        let embeddings = vec![
            Some(vec![0.1, 0.2]),
            Some(vec![f32::NAN, 0.2]),
            Some(vec![0.1, f32::INFINITY]),
            Some(vec![f32::NEG_INFINITY, 0.2]),
            None,
            Some(vec![0.0, 0.0]),
        ];

        let array = create_embedding_array("duckdb_vector_index", &embeddings, 2)
            .expect("builds the array");
        let list = array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("FixedSizeListArray");

        assert_eq!(list.len(), 6);
        assert!(list.is_valid(0), "a finite embedding stays indexed");
        assert!(list.is_null(1), "a NaN component nulls the embedding");
        assert!(list.is_null(2), "an infinite component nulls the embedding");
        assert!(
            list.is_null(3),
            "a negative-infinite component nulls the embedding"
        );
        assert!(list.is_null(4), "a missing embedding stays null");
        assert!(
            list.is_valid(5),
            "an all-zero embedding is finite and stays indexed"
        );
    }

    /// A dimension mismatch must still be an error, not silently nulled by the new screen.
    #[test]
    fn a_dimension_mismatch_is_still_an_error() {
        let embeddings = vec![Some(vec![0.1, 0.2, 0.3])];
        create_embedding_array("duckdb_vector_index", &embeddings, 2)
            .expect_err("a wrong-width embedding is rejected");
    }
}
