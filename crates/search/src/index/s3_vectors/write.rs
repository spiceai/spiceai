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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_json::{EncoderOptions, writer::make_encoder};
use arrow_schema::Field;
use data_components::s3_vectors::S3VectorsTable;
use itertools::Itertools;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use spice_table::Index;

use crate::index::write_util::{
    self, embed_column, extract_and_format_primary_key, first_non_finite,
    sort_columns_alphabetically, update_embedding_column_in_batch,
};
use crate::index::{SearchIndex, embedding_col, s3_vectors::S3Vector};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(transparent)]
    WriteUtil { source: write_util::Error },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing arrow records: {source}."
    ))]
    IssueWithArrowProcessing {
        index: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Cannot write to '{index}' index: failed to encode metadata column '{column}': {source}."
    ))]
    MetadataColumnEncoding {
        index: String,
        column: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Cannot write to '{index}' index: failed to convert metadata column '{column}' at row {row} to JSON: {source}."
    ))]
    MetadataValueJson {
        index: String,
        column: String,
        row: usize,
        source: serde_json::Error,
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
        #[snafu(source(from(data_components::s3_vectors::Error, Box::new)))]
        source: Box<data_components::s3_vectors::Error>,
    },
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
        return write_util::ColumnNotFoundSnafu {
            index: index.name().to_string(),
            column: index.embedded_column.clone(),
        }
        .fail()
        .map_err(Error::from);
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
    )?;
    let primary_key = extract_and_format_primary_key(index.name(), &index.primary_key, &record)
        .map_err(|e| Error::from(*e))?;

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
    // Ideally, we can just do `S3VectorPartitionedTable::insert_into` (or similar) with this big boy
    let updated_record = update_embedding_column_in_batch(
        index.name(),
        &record,
        &index.embedded_column,
        &embedding_vectors,
        i32::try_from(table.dimension).unwrap_or_default(),
    )
    .map_err(|e| Error::from(*e))?;

    // Filter out zero vectors to prevent cosine similarity calculation errors
    let (filtered_embeddings, filtered_primary_key, filtered_metadata) =
        filter_zero_vectors(embedding_vectors, primary_key, metadata, index.name());

    let spill_index = index.spill_index().await.context(CannotWriteIndexSnafu {
        index: index.name().to_string(),
    })?;

    table
        .write_data(
            filtered_embeddings,
            filtered_primary_key,
            filtered_metadata,
            spill_index,
        )
        .await
        .context(CannotWriteIndexSnafu {
            index: index.name().to_string(),
        })?;

    // Because of limitations of `DFSchema::logically_equivalent_names_and_types` and its use in
    // `MemTable`, this must be in the same order as outputted by `VectorScanTableProvider`.
    sort_columns_alphabetically(updated_record).map_err(|e| Error::from(*e))
}

pub fn extract_and_format_metadata(
    index_name: &str,
    metadata_columns: &[String],
    record: &RecordBatch,
) -> Result<HashMap<String, Vec<Option<Value>>>, Error> {
    let schema = record.schema();
    let mut metadata_projection = vec![];
    for name in metadata_columns {
        let Some((idx, _)) = schema.column_with_name(name) else {
            return write_util::ColumnNotFoundSnafu {
                index: index_name.to_string(),
                column: name,
            }
            .fail()
            .map_err(Error::from);
        };
        metadata_projection.push(idx);
    }

    let encoder_options = EncoderOptions::default();
    let mut metadata = HashMap::with_capacity(metadata_projection.len());
    for i in metadata_projection {
        let column = record.column(i);
        let field = Arc::new(schema.field(i).clone());
        let name = field.name().clone();
        let mut encoder = make_encoder(&field, column, &encoder_options).context(
            MetadataColumnEncodingSnafu {
                index: index_name.to_string(),
                column: name.clone(),
            },
        )?;

        let mut values = Vec::with_capacity(column.len());
        let mut value = Vec::new();
        for row in 0..column.len() {
            if encoder.is_null(row) {
                values.push(None);
            } else {
                encoder.encode(row, &mut value);
                let metadata_value =
                    serde_json::from_slice(&value).context(MetadataValueJsonSnafu {
                        index: index_name.to_string(),
                        column: name.clone(),
                        row,
                    })?;
                values.push(Some(metadata_value));
                value.clear();
            }
        }

        metadata.insert(name, values);
    }
    Ok(metadata)
}

/// Filter out invalid embedding vectors where all values are either zero or NaN.
///
/// This filters vectors that consist entirely of invalid values (zeros and/or NaNs).
/// A vector with any valid non-zero, non-NaN value is kept.
/// For example:
/// - `[0.0, 0.0]` -> filtered (all zeros)
/// - `[NaN, NaN]` -> filtered (all NaN)
/// - `[0.0, NaN]` -> filtered (all values are either zero or NaN)
/// - `[1.0, 0.0]` -> kept (has a valid non-zero value)
/// - `[1.0, NaN]` -> kept (has a valid non-NaN value)
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
        let Some(embedding) = &embeddings[i] else {
            continue;
        };
        // Single pass: check if all values are zero or NaN (both are invalid embeddings)
        let all_zero_or_nan = embedding.iter().all(|&x| x == 0.0 || x.is_nan());
        // A single NaN or infinity poisons every score computed against the vector, so one
        // bad component disqualifies the whole record too.
        let non_finite = first_non_finite(embedding).is_some();
        if !all_zero_or_nan && !non_finite {
            continue;
        }

        let key_str = primary_keys
            .get(i)
            .and_then(|k| k.as_ref().map(String::as_str))
            .unwrap_or("unknown");
        if all_zero_or_nan {
            tracing::warn!(
                "Skipping record '{key_str}' for S3 Vector index '{index_name}': Embedding vector is all zeroes or contains only invalid values"
            );
        } else {
            tracing::warn!(
                "Skipping record '{key_str}' for S3 Vector index '{index_name}': the embedding contains a NaN or infinite value, so the record is not indexed and vector search will never return it. {}",
                write_util::NON_FINITE_EMBEDDING_REMEDY
            );
        }

        embeddings.remove(i);
        primary_keys.remove(i);
        for values in metadata.values_mut() {
            values.remove(i);
        }
    }

    (embeddings, primary_keys, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, UnionArray};
    use arrow_schema::{DataType, Schema, UnionFields, UnionMode};

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

    /// A vector whose components are mostly real but carry one NaN or infinity has no
    /// defined distance under any metric, so the record must not reach the index. Before
    /// #13089 the filter tested `all`, so only a wholly-invalid vector was caught and
    /// `[NaN, 2.0]` was written.
    #[test]
    fn filter_drops_a_partially_non_finite_vector() {
        use serde_json::Value;
        use std::collections::HashMap;

        let embeddings = vec![
            Some(vec![1.0, 2.0]),               // Keep — finite
            Some(vec![f32::NAN, 2.0]),          // Drop — one NaN among real values
            Some(vec![1.0, f32::INFINITY]),     // Drop — one infinity among real values
            Some(vec![f32::NEG_INFINITY, 1.0]), // Drop — one negative infinity
            Some(vec![0.0, 0.0]),               // Drop — all zero (pre-existing behaviour)
            None,                               // Keep — no embedding to screen
            Some(vec![3.0, 4.0]),               // Keep — finite
        ];
        let keys: Vec<Option<String>> = (1..=7).map(|i| Some(format!("key{i}"))).collect();
        let mut metadata = HashMap::new();
        metadata.insert(
            "test".to_string(),
            (1..=7)
                .map(|i| Some(Value::Number(i.into())))
                .collect::<Vec<_>>(),
        );

        let (filtered_embeddings, filtered_keys, filtered_metadata) =
            filter_zero_vectors(embeddings, keys, metadata, "test_index");

        assert_eq!(filtered_embeddings.len(), 3);
        assert_eq!(filtered_keys.len(), 3);
        assert_eq!(filtered_metadata["test"].len(), 3);
        assert_eq!(filtered_embeddings[0], Some(vec![1.0, 2.0]));
        assert_eq!(filtered_embeddings[1], None);
        assert_eq!(filtered_embeddings[2], Some(vec![3.0, 4.0]));
        assert_eq!(
            filtered_keys,
            vec![
                Some("key1".to_string()),
                Some("key6".to_string()),
                Some("key7".to_string()),
            ]
        );
        // The metadata stays aligned with the rows that survived.
        assert_eq!(
            filtered_metadata["test"],
            vec![
                Some(Value::Number(1.into())),
                Some(Value::Number(6.into())),
                Some(Value::Number(7.into())),
            ]
        );
    }

    /// Test that filter_zero_vectors correctly filters out NaN embeddings.
    #[test]
    fn test_filter_nan_vectors() {
        use serde_json::Value;
        use std::collections::HashMap;

        let embeddings = vec![
            Some(vec![1.0, 2.0]),           // Keep - valid values
            Some(vec![f32::NAN, f32::NAN]), // Filter out (all NaN)
            Some(vec![f32::NAN, 0.0]),      // Filter out (mixed NaN/zero - all invalid)
            Some(vec![3.0, 4.0]),           // Keep - valid values
            Some(vec![0.0, f32::NAN]),      // Filter out (mixed zero/NaN - all invalid)
        ];
        let keys = vec![
            Some("key1".to_string()),
            Some("key2".to_string()),
            Some("key3".to_string()),
            Some("key4".to_string()),
            Some("key5".to_string()),
        ];
        let mut metadata = HashMap::new();
        metadata.insert(
            "test".to_string(),
            vec![
                Some(Value::String("a".to_string())),
                Some(Value::String("b".to_string())),
                Some(Value::String("c".to_string())),
                Some(Value::String("d".to_string())),
                Some(Value::String("e".to_string())),
            ],
        );

        let (filtered_embeddings, filtered_keys, filtered_metadata) =
            filter_zero_vectors(embeddings, keys, metadata, "test_index");

        // Should keep only the 2 valid vectors
        assert_eq!(filtered_embeddings.len(), 2);
        assert_eq!(filtered_keys.len(), 2);
        assert_eq!(filtered_metadata["test"].len(), 2);

        // Check that valid vectors were kept
        assert_eq!(filtered_embeddings[0], Some(vec![1.0, 2.0]));
        assert_eq!(filtered_embeddings[1], Some(vec![3.0, 4.0]));
        assert_eq!(filtered_keys[0], Some("key1".to_string()));
        assert_eq!(filtered_keys[1], Some("key4".to_string()));
    }

    #[test]
    fn configured_metadata_column_with_unsupported_json_type_fails() {
        let union_fields = vec![(
            0_i8,
            Arc::new(Field::new("integer", DataType::Int32, false)),
        )]
        .into_iter()
        .collect::<UnionFields>();
        let union_array = UnionArray::try_new(
            union_fields.clone(),
            vec![0_i8].into(),
            None,
            vec![Arc::new(Int32Array::from(vec![1_i32]))],
        )
        .expect("union array should be valid");
        let field = Field::new(
            "filterable_metadata",
            DataType::Union(union_fields, UnionMode::Sparse),
            false,
        );
        let record = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(union_array)],
        )
        .expect("record batch should be valid");

        let error = extract_and_format_metadata(
            "test_index",
            &["filterable_metadata".to_string()],
            &record,
        )
        .expect_err("unsupported metadata must fail indexing instead of being omitted");

        match error {
            Error::MetadataColumnEncoding { index, column, .. } => {
                assert_eq!(index, "test_index");
                assert_eq!(column, "filterable_metadata");
            }
            other => panic!("expected metadata encoding error, got {other}"),
        }
    }
}
