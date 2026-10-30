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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use qdrant::payload::{
    DATASET_KEY, GENERATION_KEY, PointData, VECTOR_KEY, arrow_value_to_qdrant, encode_vector,
    payload_column,
};
use qdrant::proto::value::Kind;
use qdrant::proto::{Condition, Filter, PointId, Range};
use snafu::{ResultExt, Snafu};
use spice_table::Index;

use super::QdrantIndex;
use crate::index::write_util::{
    self, embed_column, extract_and_format_primary_key, keys_to_evict,
    update_embedding_column_in_batch,
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
        #[snafu(source(from(qdrant::Error, Box::new)))]
        source: Box<qdrant::Error>,
    },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': column '{column}' cannot be stored as a Qdrant payload value. {source}"
    ))]
    PayloadConversion {
        collection: String,
        column: String,
        #[snafu(source(from(qdrant::Error, Box::new)))]
        source: Box<qdrant::Error>,
    },

    #[snafu(display(
        "Failed to write to Qdrant collection '{collection}': column '{column}' is missing from the rows being written, so their points would have no '{column}' payload and a filter on it would skip them. \
        Keep '{column}' in the dataset, or remove it from the `row_id`, `partition_by`, or vector metadata columns. \
        See: https://spiceai.org/docs/components/vectors"
    ))]
    PayloadColumnMissing { collection: String, column: String },

    #[snafu(display(
        "Failed to delete from Qdrant collection '{collection}': the deleted rows carry none of the key columns {expected:?}, so Spice cannot tell which points to remove. \
        Keep the `row_id` columns in the dataset. See: https://spiceai.org/docs/components/vectors"
    ))]
    KeyColumnsMissing {
        collection: String,
        expected: Vec<String>,
    },

    #[snafu(display(
        "Failed to delete from Qdrant collection '{collection}': key column '{column}' holds a list, which Qdrant cannot match by value. \
        Use a scalar column in `row_id`. See: https://spiceai.org/docs/components/vectors"
    ))]
    UnsupportedKeyType { collection: String, column: String },

    #[snafu(display(
        "Failed to update the search index '{collection}' (qdrant): the vectors stored for the records this write could not embed could not be removed, so a search would return them at their previous value. Cause: {source}"
    ))]
    CannotEvictRejectedRecords {
        collection: String,
        #[snafu(source(from(qdrant::Error, Box::new)))]
        source: Box<qdrant::Error>,
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

    // Held until the upsert lands, so a full refresh cannot start a new generation between
    // stamping these points and writing them.
    let generation = index.write_generation.current.read().await;
    let (points, evicted) = build_points(
        index,
        &record,
        &embedding_vectors,
        &primary_keys,
        *generation,
    )?;

    if !evicted.is_empty() {
        let ids: Vec<PointId> = evicted.iter().map(|key| index.point_id(key)).collect();
        delete_ids(index, ids)
            .await
            .context(CannotEvictRejectedRecordsSnafu {
                collection: index.collection.clone(),
            })?;
    }

    if !points.is_empty() {
        index
            .client
            .upsert(collection, points, index.batch_write_rows)
            .await
            .context(QdrantSnafu {
                collection: index.collection.clone(),
            })?;
    }

    drop(generation);

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
    generation: i64,
) -> Result<(Vec<PointData>, Vec<String>)> {
    const SAMPLE_LIMIT: usize = 5;

    let schema = record.schema();
    let collection = index.collection.as_str();
    let expected_dims = usize::try_from(index.dims.max(0)).unwrap_or(0);

    let mut payload_names: HashSet<&str> = HashSet::with_capacity(
        index.primary_key.len() + index.metadata_columns.all_names().len() + 1,
    );
    for field in &index.primary_key {
        payload_names.insert(field.name().as_str());
    }
    let metadata_names = index.metadata_columns.all_names();
    for name in &metadata_names {
        payload_names.insert(name.as_str());
    }
    let column_index: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();
    let mut payload_columns: Vec<(&str, ArrayRef)> = payload_names
        .into_iter()
        .map(|name| {
            let Some(&idx) = column_index.get(name) else {
                return PayloadColumnMissingSnafu {
                    collection: collection.to_string(),
                    column: name.to_string(),
                }
                .fail();
            };
            payload_column(record.column(idx))
                .map(|column| (name, column))
                .context(PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: name.to_string(),
                })
        })
        .collect::<Result<_>>()?;
    payload_columns.sort_unstable_by_key(|(name, _)| *name);
    let store_exact_vector = index.distance_metric == "cosine";

    let mut null_pk_skips: usize = 0;
    let mut null_pk_samples: Vec<usize> = Vec::new();
    let mut zero_or_nan_skips: usize = 0;
    let mut zero_or_nan_samples: Vec<usize> = Vec::new();
    let mut non_finite_skips: usize = 0;
    let mut non_finite_samples: Vec<usize> = Vec::new();
    let mut missing_embedding_skips: usize = 0;

    let mut points: Vec<(Option<String>, PointData)> = Vec::with_capacity(record.num_rows());
    let mut outcomes: Vec<(&str, write_util::RowOutcome)> = Vec::with_capacity(record.num_rows());

    for row in 0..record.num_rows() {
        let Some(embedding) = embedding_vectors[row].as_ref() else {
            missing_embedding_skips += 1;
            if let Some(key) = primary_keys[row].as_ref() {
                outcomes.push((key.as_str(), write_util::RowOutcome::Rejected));
            }
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
            if let Some(key) = primary_keys[row].as_ref() {
                outcomes.push((key.as_str(), write_util::RowOutcome::Rejected));
            }
            continue;
        }

        if embedding.iter().any(|x| !x.is_finite()) {
            non_finite_skips += 1;
            if non_finite_samples.len() < SAMPLE_LIMIT {
                non_finite_samples.push(row);
            }
            if let Some(key) = primary_keys[row].as_ref() {
                outcomes.push((key.as_str(), write_util::RowOutcome::Rejected));
            }
            continue;
        }

        let mut payload: HashMap<String, qdrant::proto::Value> =
            HashMap::with_capacity(payload_columns.len() + 3);
        for (name, column) in &payload_columns {
            let value =
                arrow_value_to_qdrant(column.as_ref(), row).context(PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: (*name).to_string(),
                })?;
            payload.insert((*name).to_string(), value);
        }
        payload.insert(DATASET_KEY.to_string(), index.dataset.clone().into());
        payload.insert(GENERATION_KEY.to_string(), generation.into());
        if store_exact_vector {
            payload.insert(VECTOR_KEY.to_string(), encode_vector(embedding));
        }

        let id = if index.primary_key.is_empty() {
            None
        } else {
            Some(index.point_id(primary_keys[row].as_deref().unwrap_or_default()))
        };

        if let Some(key) = primary_keys[row].as_ref() {
            outcomes.push((key.as_str(), write_util::RowOutcome::Indexed));
        }
        points.push((
            primary_keys[row].clone(),
            PointData {
                id,
                payload,
                vector: embedding.clone(),
            },
        ));
    }

    if null_pk_skips > 0 {
        tracing::warn!(
            "Skipped {null_pk_skips} record(s) for Qdrant collection '{collection}': NULL primary key value(s) (would produce non-idempotent point ids). Sample row indices: {null_pk_samples:?}"
        );
    }
    if zero_or_nan_skips > 0 {
        tracing::warn!(
            "Skipped {zero_or_nan_skips} record(s) for Qdrant collection '{collection}': embedding vector is all zeros or NaN. Any vector already stored for those records is removed, so a search does not return them at their previous value. Sample row indices: {zero_or_nan_samples:?}"
        );
    }
    if non_finite_skips > 0 {
        tracing::warn!(
            "Skipped {non_finite_skips} record(s) for Qdrant collection '{collection}': embedding vector contains non-finite values (NaN or infinity). Any vector already stored for those records is removed, so a search does not return them at their previous value. Sample row indices: {non_finite_samples:?}"
        );
    }
    if missing_embedding_skips > 0 {
        tracing::debug!(
            "Skipped {missing_embedding_skips} record(s) for Qdrant collection '{collection}': no embedding generated (expected when embedded column is NULL). Any vector already stored for those records is removed."
        );
    }

    let evicted = keys_to_evict(outcomes);
    if !evicted.is_empty() {
        let evicted_keys: HashSet<&str> = evicted.iter().map(String::as_str).collect();
        points.retain(|(key, _)| !key.as_deref().is_some_and(|key| evicted_keys.contains(key)));
    }
    Ok((
        points.into_iter().map(|(_, point)| point).collect(),
        evicted,
    ))
}

pub async fn delete_by_keys(index: &QdrantIndex, keys: &RecordBatch) -> Result<()> {
    let schema = keys.schema();
    let present: Vec<&str> = index
        .primary_key
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| schema.column_with_name(name).is_some())
        .collect();
    if present.len() < index.primary_key.len() {
        let groups = key_groups(index, keys, &present)?;
        return delete_groups(
            index,
            groups.into_values().map(|(filter, _)| (filter, vec![])),
        )
        .await;
    }

    let ids: Vec<PointId> = extract_and_format_primary_key(index.name(), &index.primary_key, keys)
        .map_err(|e| Error::from(*e))?
        .into_iter()
        .flatten()
        .map(|key| index.point_id(&key))
        .collect();
    delete_ids(index, ids).await.context(QdrantSnafu {
        collection: index.collection.clone(),
    })
}

/// Deletes the dataset's points among `ids`, a bounded number per request.
async fn delete_ids(index: &QdrantIndex, ids: Vec<PointId>) -> qdrant::Result<()> {
    for chunk in ids.chunks(qdrant::DEFAULT_UPSERT_BATCH_SIZE) {
        let filter = Filter::must([
            index.dataset_condition(),
            Condition::has_id(chunk.iter().cloned()),
        ]);
        index
            .client
            .delete_by_filter(&index.collection, filter)
            .await?;
    }
    Ok(())
}

pub async fn delete_group_remainder(
    index: &QdrantIndex,
    group_columns: &[String],
    members: &RecordBatch,
) -> Result<()> {
    let columns: Vec<&str> = group_columns.iter().map(String::as_str).collect();
    let groups = key_groups(index, members, &columns)?;
    delete_groups(index, groups.into_values()).await
}

/// One part of a key group's identity. A float is keyed by its bits, with `-0.0` folded into
/// `0.0` because both match the same range filter.
#[derive(Debug, PartialEq, Eq, Hash)]
enum KeyPart {
    Str(String),
    Int(i64),
    Bool(bool),
    Float(u64),
}

type KeyGroups = HashMap<Vec<KeyPart>, (Filter, Vec<PointId>)>;

fn key_groups(index: &QdrantIndex, batch: &RecordBatch, columns: &[&str]) -> Result<KeyGroups> {
    let collection = index.collection.as_str();
    let missing = || {
        KeyColumnsMissingSnafu {
            collection: collection.to_string(),
            expected: index
                .primary_key
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        }
        .build()
    };
    if columns.is_empty() {
        return Err(missing());
    }
    let schema = batch.schema();
    let key_columns = columns
        .iter()
        .map(|&name| {
            let Some((idx, _)) = schema.column_with_name(name) else {
                return Err(missing());
            };
            payload_column(batch.column(idx))
                .map(|column| (name, column))
                .context(PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: name.to_string(),
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let ids: Vec<Option<String>> = if index
        .primary_key
        .iter()
        .all(|f| schema.column_with_name(f.name()).is_some())
    {
        extract_and_format_primary_key(index.name(), &index.primary_key, batch)
            .map_err(|e| Error::from(*e))?
    } else {
        vec![None; batch.num_rows()]
    };

    let mut groups = KeyGroups::new();
    'rows: for (row, id) in ids.into_iter().enumerate() {
        let mut key = Vec::with_capacity(key_columns.len());
        let mut conditions = Vec::with_capacity(key_columns.len() + 1);
        for (name, column) in &key_columns {
            let value =
                arrow_value_to_qdrant(column.as_ref(), row).context(PayloadConversionSnafu {
                    collection: collection.to_string(),
                    column: (*name).to_string(),
                })?;
            let (part, condition) = match value.kind {
                Some(Kind::NullValue(_)) | None => continue 'rows,
                Some(Kind::StringValue(v)) => {
                    (KeyPart::Str(v.clone()), Condition::matches(*name, v))
                }
                Some(Kind::IntegerValue(v)) => (KeyPart::Int(v), Condition::matches(*name, v)),
                Some(Kind::BoolValue(v)) => (KeyPart::Bool(v), Condition::matches(*name, v)),
                Some(Kind::DoubleValue(v)) => (
                    KeyPart::Float((v + 0.0).to_bits()),
                    Condition::range(
                        *name,
                        Range {
                            gte: Some(v),
                            lte: Some(v),
                            ..Default::default()
                        },
                    ),
                ),
                Some(_) => {
                    return UnsupportedKeyTypeSnafu {
                        collection: collection.to_string(),
                        column: (*name).to_string(),
                    }
                    .fail();
                }
            };
            key.push(part);
            conditions.push(condition);
        }
        let group = groups
            .entry(key)
            .or_insert_with(|| (Filter::must(conditions), Vec::new()));
        if let Some(id) = id {
            group.1.push(index.point_id(&id));
        }
    }
    Ok(groups)
}

/// Deletes the dataset's points in each group except the ids listed with it, a bounded number
/// of groups per request.
async fn delete_groups(
    index: &QdrantIndex,
    groups: impl IntoIterator<Item = (Filter, Vec<PointId>)>,
) -> Result<()> {
    const GROUPS_PER_REQUEST: usize = 256;
    let groups: Vec<_> = groups.into_iter().collect();
    for chunk in groups.chunks(GROUPS_PER_REQUEST) {
        let keep: Vec<PointId> = chunk
            .iter()
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect();
        let mut filter = Filter::should(chunk.iter().map(|(group, _)| group.clone().into()));
        filter.must.push(index.dataset_condition());
        if !keep.is_empty() {
            filter.must_not.push(Condition::has_id(keep));
        }
        index
            .client
            .delete_by_filter(&index.collection, filter)
            .await
            .context(QdrantSnafu {
                collection: index.collection.clone(),
            })?;
    }
    Ok(())
}
