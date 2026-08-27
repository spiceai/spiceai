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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use qdrant::payload::{PointData, arrow_value_to_qdrant, point_id_from_values};
use snafu::{ResultExt, Snafu};
use spice_table::Index;

use super::QdrantIndex;
use crate::index::embedding_col;
use crate::index::write_util::{
    self, embed_column, extract_and_format_primary_key, update_embedding_column_in_batch,
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(transparent)]
    WriteUtil { source: write_util::Error },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': embedded column '{column}' not found in record batch."
    ))]
    ColumnNotFound { collection: String, column: String },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': embedding dimension mismatch at row {row_index}: expected {expected}, got {actual}."
    ))]
    EmbeddingDimensionMismatch {
        collection: String,
        expected: usize,
        actual: usize,
        row_index: usize,
    },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': computed {actual} embedding(s) for {expected} row(s)."
    ))]
    RowCountMismatch {
        collection: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Failed to write to Qdrant collection '{collection}': {source}"))]
    Qdrant {
        collection: String,
        source: qdrant::Error,
    },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': partition column '{column}' not found in record batch."
    ))]
    PartitionColumnNotFound { collection: String, column: String },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': column '{column}' has a type that cannot be stored as a Qdrant payload value. {source} \
        See: https://spiceai.org/docs/components/vector-databases"
    ))]
    PayloadConversion {
        collection: String,
        column: String,
        source: qdrant::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn write(index: &QdrantIndex, record: RecordBatch) -> Result<RecordBatch> {
    let collection = index.collection.as_str();
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        return ColumnNotFoundSnafu {
            collection: index.collection.clone(),
            column: index.embedded_column.clone(),
        }
        .fail();
    };

    let embedding_vectors = embed_column(
        &record,
        embedded_column_idx,
        Arc::clone(&index.compute_query),
    )
    .await?;

    if embedding_vectors.len() != record.num_rows() {
        return RowCountMismatchSnafu {
            collection: index.collection.clone(),
            expected: record.num_rows(),
            actual: embedding_vectors.len(),
        }
        .fail();
    }

    let primary_keys = extract_and_format_primary_key(index.name(), &index.primary_key, &record)
        .map_err(|e| Error::from(*e))?;

    let points = build_points(index, &record, &embedding_vectors, &primary_keys)?;

    if !points.is_empty() {
        index
            .client
            .upsert(collection, points, index.batch_write_rows)
            .await
            .context(QdrantSnafu {
                collection: index.collection.clone(),
            })?;
    }

    update_embedding_column_in_batch(
        &record,
        &index.embedded_column,
        &embedding_vectors,
        index.dims,
    )
    .map_err(|e| Error::from(*e))
}

fn build_points(
    index: &QdrantIndex,
    record: &RecordBatch,
    embedding_vectors: &[Option<Vec<f32>>],
    primary_keys: &[Option<String>],
) -> Result<Vec<PointData>> {
    const SAMPLE_LIMIT: usize = 5;

    let schema = record.schema();
    let collection = index.collection.as_str();
    let embedding_name = embedding_col(&index.embedded_column);
    let expected_dims = usize::try_from(index.dims.max(0)).unwrap_or(0);

    let mut null_pk_skips: usize = 0;
    let mut null_pk_samples: Vec<usize> = Vec::new();
    let mut zero_or_nan_skips: usize = 0;
    let mut zero_or_nan_samples: Vec<usize> = Vec::new();
    let mut non_finite_skips: usize = 0;
    let mut non_finite_samples: Vec<usize> = Vec::new();
    let mut missing_embedding_skips: usize = 0;

    let mut points: Vec<PointData> = Vec::with_capacity(record.num_rows());

    for row in 0..record.num_rows() {
        let Some(embedding) = embedding_vectors[row].as_ref() else {
            missing_embedding_skips += 1;
            continue;
        };

        if !index.primary_key.is_empty() && primary_keys[row].is_none() {
            null_pk_skips += 1;
            if null_pk_samples.len() < SAMPLE_LIMIT {
                null_pk_samples.push(row);
            }
            continue;
        }

        if embedding.len() != expected_dims {
            return Err(Error::EmbeddingDimensionMismatch {
                collection: collection.to_string(),
                expected: expected_dims,
                actual: embedding.len(),
                row_index: row,
            });
        }

        if embedding.iter().all(|&x| x == 0.0 || x.is_nan()) {
            zero_or_nan_skips += 1;
            if zero_or_nan_samples.len() < SAMPLE_LIMIT {
                zero_or_nan_samples.push(row);
            }
            continue;
        }

        if embedding.iter().any(|x| !x.is_finite()) {
            non_finite_skips += 1;
            if non_finite_samples.len() < SAMPLE_LIMIT {
                non_finite_samples.push(row);
            }
            continue;
        }

        let mut payload: HashMap<String, qdrant::proto::Value> =
            HashMap::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            if field.name() == &embedding_name {
                continue;
            }
            let value = arrow_value_to_qdrant(record.column(i).as_ref(), row).context(
                PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: field.name().clone(),
                },
            )?;
            payload.insert(field.name().clone(), value);
        }

        if let (Some(partition_key), Some(partition_col)) =
            (&index.partition_key, &index.partition_column)
        {
            let Some((idx, _)) = record.schema().column_with_name(partition_col) else {
                return Err(Error::PartitionColumnNotFound {
                    collection: collection.to_string(),
                    column: partition_col.clone(),
                });
            };
            let value = arrow_value_to_qdrant(record.column(idx).as_ref(), row).context(
                PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: partition_col.clone(),
                },
            )?;
            payload.insert(partition_key.clone(), value);
        }

        let id = if index.primary_key.is_empty() {
            None
        } else {
            Some(point_id_from_values(&[primary_keys[row]
                .clone()
                .unwrap_or_default()]))
        };

        points.push(PointData {
            id,
            payload,
            vector: embedding.clone(),
        });
    }

    if null_pk_skips > 0 {
        tracing::warn!(
            "Skipped {null_pk_skips} record(s) for Qdrant collection '{collection}': NULL primary key value(s) (would produce non-idempotent point ids). Sample row indices: {null_pk_samples:?}"
        );
    }
    if zero_or_nan_skips > 0 {
        tracing::warn!(
            "Skipped {zero_or_nan_skips} record(s) for Qdrant collection '{collection}': embedding vector is all zeros or NaN. Sample row indices: {zero_or_nan_samples:?}"
        );
    }
    if non_finite_skips > 0 {
        tracing::warn!(
            "Skipped {non_finite_skips} record(s) for Qdrant collection '{collection}': embedding vector contains non-finite values (NaN or infinity). Sample row indices: {non_finite_samples:?}"
        );
    }
    if missing_embedding_skips > 0 {
        tracing::debug!(
            "Skipped {missing_embedding_skips} record(s) for Qdrant collection '{collection}': no embedding generated."
        );
    }

    Ok(points)
}

pub async fn delete_by_keys(index: &QdrantIndex, keys: &RecordBatch) -> Result<()> {
    let key_strings: Vec<Option<String>> =
        extract_and_format_primary_key(index.name(), &index.primary_key, keys)
            .map_err(|e| Error::from(*e))?;

    let ids: Vec<qdrant::proto::PointId> = key_strings
        .into_iter()
        .filter_map(|key| key.map(|k| point_id_from_values(&[k])))
        .collect();

    if ids.is_empty() {
        return Ok(());
    }

    index
        .client
        .delete_by_ids(&index.collection, ids)
        .await
        .context(QdrantSnafu {
            collection: index.collection.clone(),
        })
}
