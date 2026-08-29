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

//! Embedding + bulk-index write path for [`ElasticsearchIndex`].
//!
//! Given a [`RecordBatch`] from the upstream table, this:
//! 1. Embeds the configured search column.
//! 2. Builds one JSON document per row containing every batch column plus the
//!    dense vector under the configured `vector_field`.
//! 3. Bulk-indexes the documents into Elasticsearch, using the primary key as
//!    the document `_id`.

use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListBuilder, Float32Builder, LargeStringArray, RecordBatch, StringArray,
    StringViewArray,
};
use arrow_json::{EncoderOptions, writer::make_encoder};
use arrow_schema::{DataType, Field, Schema};
use llms::embeddings::{Embed, EmbeddingInput};
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use util::{convert_string_arrow_to_iterator, distribute_nulls};

use crate::index::elasticsearch::{ElasticsearchIndex, delete};
use crate::index::embedding_col;
use crate::index::write_util;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to write to Elasticsearch index '{index}': embedded column '{column}' not found in record batch."
    ))]
    ColumnNotFound { index: String, column: String },

    #[snafu(display(
        "Failed to write to Elasticsearch index '{index}': primary key column '{column}' not found in record batch."
    ))]
    PrimaryKeyColumnNotFound { index: String, column: String },

    #[snafu(display("Failed to write to Elasticsearch index '{index}': {source}"))]
    IssueWithArrowProcessing {
        index: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("Failed to write to Elasticsearch index '{index}': {source}"))]
    IssueWithJsonProcessing {
        index: String,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to compute embeddings for Elasticsearch index '{index}': {source}"))]
    FailedToEmbed {
        index: String,
        source: llms::embeddings::Error,
    },

    #[snafu(display(
        "Failed to bulk index documents into Elasticsearch index '{index}': {source}"
    ))]
    BulkIndex {
        index: String,
        source: elasticsearch::Error,
    },

    #[snafu(display(
        "Elasticsearch bulk index into '{index}' reported {failures} document failure(s). First error: {first}"
    ))]
    BulkIndexItemErrors {
        index: String,
        failures: usize,
        first: String,
    },

    #[snafu(display(
        "Failed to write to Elasticsearch index '{index}': embedding dimension mismatch at row {row_index}: expected {expected}, got {actual}."
    ))]
    EmbeddingDimensionMismatch {
        index: String,
        expected: usize,
        actual: usize,
        row_index: usize,
    },

    #[snafu(display(
        "Failed to write to Elasticsearch index '{index}': embedded column '{column}' has non-string type {data_type}; expected a Utf8/LargeUtf8/Utf8View column to generate embeddings."
    ))]
    EmbeddedColumnNotString {
        index: String,
        column: String,
        data_type: String,
    },

    #[snafu(display(
        "Failed to write to Elasticsearch index '{index}': source column '{column}' collides with the configured vector_field name. Rename one of the columns so the embedding vector does not silently overwrite a source value."
    ))]
    VectorFieldCollidesWithSourceColumn { index: String, column: String },

    #[snafu(display(
        "Failed to update the search index '{index}' (elasticsearch): the documents stored for the records this write could not embed could not be removed, so a search would return them at their previous value. Cause: {source}"
    ))]
    CannotEvictRejectedRecords {
        index: String,
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Entry point: embed the record batch, bulk-index to Elasticsearch, and return
/// the batch with the embedding column populated (so downstream stages see a
/// consistent schema).
pub async fn write(index: &ElasticsearchIndex, record: RecordBatch) -> Result<RecordBatch> {
    let es_index = index.es_index.as_str();

    // Fail fast if the embedded column is missing: silently returning the batch
    // would turn a mis-projection or upstream schema bug into zero-document
    // indexing while the pipeline reports success, which is silent data loss
    // for the vector store.
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        return ColumnNotFoundSnafu {
            index: es_index.to_string(),
            column: index.embedded_column.clone(),
        }
        .fail();
    };

    let embedding_vectors = embed_column(
        &record,
        embedded_column_idx,
        index.embedded_column.as_str(),
        Arc::clone(&index.compute_query),
        es_index,
    )
    .await?;

    if embedding_vectors.len() != record.num_rows() {
        return Err(Error::IssueWithArrowProcessing {
            index: es_index.to_string(),
            source: arrow::error::ArrowError::ComputeError(format!(
                "embedding count {} does not match row count {}",
                embedding_vectors.len(),
                record.num_rows()
            )),
        });
    }

    // Extract primary key (as document _id).
    let primary_keys =
        extract_primary_key_from_fields(&index.primary_key, &index.es_index, &record)?;

    // Build all documents in a sync block so the arrow-json encoders (which are
    // `!Send`) are dropped before any subsequent `.await`.
    let (docs, evicted): (Vec<(Option<String>, Value)>, Vec<String>) =
        build_documents(index, &record, &embedding_vectors, &primary_keys)?;

    // Before the bulk index, not after it: a key this batch both rejects and indexes is
    // excluded from `evicted`, so deleting first is what lets the two orders agree.
    // No rejected row means no extra request at all.
    if !evicted.is_empty() {
        delete::delete_by_ids(index.client.as_ref(), es_index, &evicted)
            .await
            .map_err(|source| Error::CannotEvictRejectedRecords {
                index: es_index.to_string(),
                source,
            })?;
    }

    if docs.is_empty() {
        tracing::debug!(
            "No documents to index into Elasticsearch index '{es_index}' for this batch."
        );
    } else {
        // Chunk the bulk payload to bound per-request memory/size. `batch_write_rows`
        // is validated > 0 at construction time; still guard defensively.
        let chunk_size = index.batch_write_rows.max(1);
        let total = docs.len();
        for chunk in docs.chunks(chunk_size) {
            let resp = index
                .client
                .bulk_index(es_index, chunk)
                .await
                .context(BulkIndexSnafu {
                    index: es_index.to_string(),
                })?;
            inspect_bulk_response(&resp, es_index)?;
        }
        tracing::debug!(
            "Indexed {total} document(s) into Elasticsearch index '{es_index}' (chunk size: {chunk_size})."
        );
    }

    // Attach the computed embedding column so downstream stages (and IndexTableScanNode's
    // schema check) see the expected dense_vector column.
    update_embedding_column_in_batch(
        &record,
        es_index,
        &index.embedded_column,
        &embedding_vectors,
        index.dims,
    )
}

/// Build ES `_bulk` documents from a record batch + pre-computed embeddings.
///
/// Returns the documents to index and, beside them, the `_id`s of the rows this batch
/// could not index and must therefore delete (see [`write_util::keys_to_evict`]).
///
/// Kept sync so the arrow-json encoders it uses (which are `!Send`) stay off
/// the async state machine.
#[expect(clippy::type_complexity)]
fn build_documents(
    index: &ElasticsearchIndex,
    record: &RecordBatch,
    embedding_vectors: &[Option<Vec<f32>>],
    primary_keys: &[Option<String>],
) -> Result<(Vec<(Option<String>, Value)>, Vec<String>)> {
    // Aggregate per-row skip reasons into batch-level counts (plus a small
    // sample of row indices) to avoid high-volume per-row logs on large
    // batches with many NULLs or invalid embeddings.
    const SAMPLE_LIMIT: usize = 5;

    let es_index = index.es_index.as_str();
    let schema = record.schema();
    let embedding_col_name = embedding_col(&index.embedded_column);

    let encoder_options = EncoderOptions::default();
    let mut column_encoders: Vec<(String, _)> = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        // The derived embedding column is written from `embedding_vectors` directly;
        // skip it here so we don't double-write (and to avoid encoding a NULL-filled
        // column on the initial write).
        if field.name() == &embedding_col_name {
            continue;
        }
        let arr = record.column(i);
        let encoder =
            make_encoder(field, arr, &encoder_options).context(IssueWithArrowProcessingSnafu {
                index: es_index.to_string(),
            })?;
        column_encoders.push((field.name().clone(), encoder));
    }

    // Detect name collision between the configured vector field and any source
    // column. Without this, inserting the embedding vector below would silently
    // overwrite a source value in the document sent to Elasticsearch.
    if column_encoders
        .iter()
        .any(|(name, _)| name == &index.vector_field)
    {
        return VectorFieldCollidesWithSourceColumnSnafu {
            index: es_index.to_string(),
            column: index.vector_field.clone(),
        }
        .fail();
    }

    let mut docs: Vec<(Option<String>, Value)> = Vec::with_capacity(record.num_rows());
    let mut value_buf: Vec<u8> = Vec::with_capacity(256);

    let mut null_pk_skips: usize = 0;
    let mut null_pk_samples: Vec<usize> = Vec::new();
    let mut zero_or_nan_skips: usize = 0;
    let mut zero_or_nan_samples: Vec<usize> = Vec::new();
    let mut non_finite_skips: usize = 0;
    let mut non_finite_samples: Vec<usize> = Vec::new();
    let mut missing_embedding_skips: usize = 0;

    let expected_dims = usize::try_from(index.dims.max(0)).unwrap_or(0);

    // The `_id`s of rows this batch could not index. A document already stored under
    // one of them holds a vector from an earlier value of the row's text, which a
    // search would go on returning — see [`write_util::keys_to_evict`].
    let mut rejected: Vec<String> = Vec::new();

    for row in 0..record.num_rows() {
        let Some(embedding) = embedding_vectors[row].as_ref() else {
            missing_embedding_skips += 1;
            if let Some(id) = primary_keys[row].as_ref() {
                rejected.push(id.clone());
            }
            continue;
        };

        // Skip rows with NULL primary keys when a primary key is configured.
        // Without an `_id`, Elasticsearch would auto-generate one, making
        // re-indexing the same row non-idempotent (producing duplicates on
        // refresh/CDC writes). No `_id` also means no document this write can
        // address, so there is nothing for it to evict.
        if !index.primary_key.is_empty() && primary_keys[row].is_none() {
            null_pk_skips += 1;
            if null_pk_samples.len() < SAMPLE_LIMIT {
                null_pk_samples.push(row);
            }
            continue;
        }

        if embedding.len() != expected_dims {
            return Err(Error::EmbeddingDimensionMismatch {
                index: es_index.to_string(),
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
            if let Some(id) = primary_keys[row].as_ref() {
                rejected.push(id.clone());
            }
            continue;
        }

        if embedding.iter().any(|x| !x.is_finite()) {
            non_finite_skips += 1;
            if non_finite_samples.len() < SAMPLE_LIMIT {
                non_finite_samples.push(row);
            }
            if let Some(id) = primary_keys[row].as_ref() {
                rejected.push(id.clone());
            }
            continue;
        }

        let mut doc = serde_json::Map::with_capacity(column_encoders.len() + 1);
        for (name, encoder) in &mut column_encoders {
            if encoder.is_null(row) {
                doc.insert(name.clone(), Value::Null);
                continue;
            }
            value_buf.clear();
            encoder.encode(row, &mut value_buf);
            // `with_context` keeps the error-message allocation off the happy
            // path — this runs once per cell (rows x columns).
            let v: Value = serde_json::from_slice(&value_buf).with_context(|_| {
                IssueWithJsonProcessingSnafu {
                    index: es_index.to_string(),
                }
            })?;
            doc.insert(name.clone(), v);
        }

        let vec_json = Value::Array(
            embedding
                .iter()
                .map(|&x| {
                    serde_json::Number::from_f64(f64::from(x)).map_or(Value::Null, Value::Number)
                })
                .collect(),
        );
        doc.insert(index.vector_field.clone(), vec_json);

        docs.push((primary_keys[row].clone(), Value::Object(doc)));
    }

    if null_pk_skips > 0 {
        tracing::warn!(
            "Skipped {null_pk_skips} record(s) for Elasticsearch index '{es_index}': NULL primary key value(s) (would produce non-idempotent auto-generated document IDs). Sample row indices: {null_pk_samples:?}"
        );
    }
    if zero_or_nan_skips > 0 {
        tracing::warn!(
            "Skipped {zero_or_nan_skips} record(s) for Elasticsearch index '{es_index}': embedding vector is all zeros or NaN. Any document already stored for those records is removed, so a search does not return them at their previous value. Sample row indices: {zero_or_nan_samples:?}"
        );
    }
    if non_finite_skips > 0 {
        tracing::warn!(
            "Skipped {non_finite_skips} record(s) for Elasticsearch index '{es_index}': embedding vector contains non-finite values (NaN or infinity). Any document already stored for those records is removed, so a search does not return them at their previous value. Sample row indices: {non_finite_samples:?}"
        );
    }
    if missing_embedding_skips > 0 {
        tracing::debug!(
            "Skipped {missing_embedding_skips} record(s) for Elasticsearch index '{es_index}': no embedding generated (expected when embedded column is NULL)."
        );
    }

    let indexed = docs.iter().filter_map(|(id, _)| id.as_deref());
    let evicted = write_util::keys_to_evict(rejected, indexed);
    Ok((docs, evicted))
}

async fn embed_column(
    rb: &RecordBatch,
    column_idx: usize,
    column_name: &str,
    model: Arc<dyn Embed>,
    es_index: &str,
) -> Result<Vec<Option<Vec<f32>>>> {
    let column_arr = rb.column(column_idx);
    let iter_opt: Option<Box<dyn Iterator<Item = Option<&str>> + Send>> =
        convert_string_arrow_to_iterator!(column_arr);
    let Some(data) = iter_opt else {
        // The embedded column is expected to be a string-like Arrow array.
        // Silently returning "no embeddings" would make writes a no-op and
        // corrupt the vector index; fail loudly so the mis-configuration
        // surfaces at the first write.
        return EmbeddedColumnNotStringSnafu {
            index: es_index.to_string(),
            column: column_name.to_string(),
            data_type: column_arr.data_type().to_string(),
        }
        .fail();
    };

    let mut nulls = Vec::new();
    let mut column = Vec::new();
    for (i, o) in data.enumerate() {
        if o.is_none_or(str::is_empty) {
            nulls.push(i);
        } else if let Some(s) = o {
            column.push(s.to_string());
        }
    }

    // If every value is null/empty, skip the model call.
    if column.is_empty() {
        return Ok(vec![None; rb.num_rows()]);
    }

    let embedded = model
        .embed(EmbeddingInput::StringArray(column))
        .await
        .context(FailedToEmbedSnafu {
            index: es_index.to_string(),
        })?;

    Ok(distribute_nulls(embedded, nulls))
}

fn update_embedding_column_in_batch(
    record: &RecordBatch,
    es_index: &str,
    embedded_column_name: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<RecordBatch> {
    let embedding_column_name = embedding_col(embedded_column_name);

    let schema = record.schema();
    let mut columns = record.columns().to_vec();

    let embedding_array = create_embedding_array(es_index, embedding_vectors, dimension)?;

    let target_schema = if let Some((idx, _)) = schema.column_with_name(&embedding_column_name) {
        columns[idx] = embedding_array;
        schema
    } else {
        // Derive the new field's type directly from the embedding array so the
        // schema always matches the actual array (e.g. when `create_embedding_array`
        // falls back to an inferred dimension because `dimension <= 0`).
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            &embedding_column_name,
            embedding_array.data_type().clone(),
            true,
        )));
        columns.push(embedding_array);
        Arc::new(Schema::new(fields))
    };

    RecordBatch::try_new(target_schema, columns).context(IssueWithArrowProcessingSnafu {
        index: es_index.to_string(),
    })
}

#[expect(clippy::cast_sign_loss)]
fn create_embedding_array(
    es_index: &str,
    embedding_vectors: &[Option<Vec<f32>>],
    dimension: i32,
) -> Result<Arc<dyn Array>> {
    let dim = if dimension > 0 {
        dimension
    } else {
        // Fallback — shouldn't happen since dims is validated earlier, but stay safe.
        i32::try_from(
            embedding_vectors
                .iter()
                .find_map(|o| o.as_ref().map(Vec::len))
                .unwrap_or(1),
        )
        .unwrap_or(1)
        .max(1)
    };
    let expected = dim as usize;

    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim);
    let item_field = Field::new_list_field(DataType::Float32, false);
    builder = builder.with_field(item_field);

    for (row, emb) in embedding_vectors.iter().enumerate() {
        match emb {
            Some(v) if v.len() == expected => {
                builder.values().append_slice(v);
                builder.append(true);
            }
            Some(v) => {
                return Err(Error::EmbeddingDimensionMismatch {
                    index: es_index.to_string(),
                    expected,
                    actual: v.len(),
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

    Ok(Arc::new(builder.finish()))
}

/// Bulk-index a [`RecordBatch`] as text documents (no embedding step).
///
/// Used by [`ElasticsearchTextIndex`] to push source rows into Elasticsearch
/// so that BM25 full-text search can operate on them.
pub async fn write_text(
    client: &dyn elasticsearch::Elasticsearch,
    es_index: &str,
    primary_key: &[Field],
    batch_write_rows: usize,
    record: &RecordBatch,
) -> Result<RecordBatch> {
    let primary_keys = extract_primary_key_from_fields(primary_key, es_index, record)?;
    let docs = build_text_documents(es_index, record, &primary_keys)?;

    if docs.is_empty() {
        tracing::debug!(
            "No documents to index into Elasticsearch index '{es_index}' for this batch."
        );
    } else {
        let chunk_size = batch_write_rows.max(1);
        let total = docs.len();
        for chunk in docs.chunks(chunk_size) {
            let resp = client
                .bulk_index(es_index, chunk)
                .await
                .context(BulkIndexSnafu {
                    index: es_index.to_string(),
                })?;
            inspect_bulk_response(&resp, es_index)?;
        }
        tracing::debug!(
            "Indexed {total} document(s) into Elasticsearch index '{es_index}' (chunk size: {chunk_size})."
        );
    }

    Ok(record.clone())
}

/// Build ES `_bulk` documents from a record batch without embeddings.
fn build_text_documents(
    es_index: &str,
    record: &RecordBatch,
    primary_keys: &[Option<String>],
) -> Result<Vec<(Option<String>, serde_json::Value)>> {
    let schema = record.schema();
    let encoder_options = EncoderOptions::default();
    let mut column_encoders: Vec<(String, _)> = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        // Skip vector/embedding columns — they are dense_vector in ES and must not
        // be written to a text index as plain floats, which would cause mapping conflicts.
        if matches!(
            field.data_type(),
            arrow::datatypes::DataType::FixedSizeList(_, _)
                | arrow::datatypes::DataType::LargeList(_)
                | arrow::datatypes::DataType::List(_)
        ) {
            continue;
        }
        let arr = record.column(i);
        let encoder =
            make_encoder(field, arr, &encoder_options).context(IssueWithArrowProcessingSnafu {
                index: es_index.to_string(),
            })?;
        column_encoders.push((field.name().clone(), encoder));
    }

    let mut docs: Vec<(Option<String>, serde_json::Value)> = Vec::with_capacity(record.num_rows());
    let mut null_pk_skips: usize = 0;
    let mut value_buf: Vec<u8> = Vec::with_capacity(256);

    for row in 0..record.num_rows() {
        if !primary_keys.is_empty() && primary_keys[row].is_none() {
            null_pk_skips += 1;
            continue;
        }
        let mut doc = serde_json::Map::with_capacity(column_encoders.len());
        for (name, encoder) in &mut column_encoders {
            if encoder.is_null(row) {
                doc.insert(name.clone(), serde_json::Value::Null);
                continue;
            }
            value_buf.clear();
            encoder.encode(row, &mut value_buf);
            let v: serde_json::Value =
                serde_json::from_slice(&value_buf).context(IssueWithJsonProcessingSnafu {
                    index: es_index.to_string(),
                })?;
            doc.insert(name.clone(), v);
        }
        docs.push((primary_keys[row].clone(), serde_json::Value::Object(doc)));
    }

    if null_pk_skips > 0 {
        tracing::warn!(
            "Skipped {null_pk_skips} record(s) for Elasticsearch index '{es_index}': NULL primary key value(s)."
        );
    }
    Ok(docs)
}

/// Extract primary key strings from a record batch, parameterised on field definitions
/// rather than an [`ElasticsearchIndex`] instance.
///
/// The returned string is the document `_id` under which the row is written. A row whose
/// key is NULL (any component, for a composite key) has no stable identity and yields
/// `None`; such rows are skipped rather than written under a generated `_id`.
///
/// [`super::delete`] derives the `_id`s it deletes with this same function, so an exact-key
/// delete addresses precisely the documents a write produced. Keep it that way: a second,
/// parallel derivation would let the two drift and leave deletes silently matching nothing.
pub(super) fn extract_primary_key_from_fields(
    primary_key: &[Field],
    es_index: &str,
    record: &RecordBatch,
) -> Result<Vec<Option<String>>> {
    let schema = record.schema();
    match primary_key {
        [] => Ok(vec![None; record.num_rows()]),
        [f] => {
            let Some((i, _)) = schema.column_with_name(f.name()) else {
                return PrimaryKeyColumnNotFoundSnafu {
                    index: es_index.to_string(),
                    column: f.name().clone(),
                }
                .fail();
            };
            let c = record.column(i);
            if let Some(iter) = convert_string_arrow_to_iterator!(c) {
                return Ok(iter
                    .map(|o: Option<&str>| o.map(ToString::to_string))
                    .collect());
            }
            let casted = arrow::compute::cast(c, &DataType::Utf8).context(
                IssueWithArrowProcessingSnafu {
                    index: es_index.to_string(),
                },
            )?;
            let iter_opt: Option<Box<dyn Iterator<Item = Option<&str>> + Send>> =
                convert_string_arrow_to_iterator!(casted);
            let Some(iter) = iter_opt else {
                return Err(Error::IssueWithArrowProcessing {
                    index: es_index.to_string(),
                    source: arrow::error::ArrowError::CastError(format!(
                        "could not cast primary key column '{}' to Utf8",
                        f.name()
                    )),
                });
            };
            Ok(iter
                .map(|o: Option<&str>| o.map(ToString::to_string))
                .collect())
        }
        fields => {
            let mut proj = Vec::with_capacity(fields.len());
            for f in fields {
                let Some((i, _)) = schema.column_with_name(f.name()) else {
                    return PrimaryKeyColumnNotFoundSnafu {
                        index: es_index.to_string(),
                        column: f.name().clone(),
                    }
                    .fail();
                };
                proj.push(i);
            }
            let pk = record
                .project(&proj)
                .context(IssueWithArrowProcessingSnafu {
                    index: es_index.to_string(),
                })?;
            let num_rows = pk.num_rows();
            let mut row_is_null: Vec<bool> = vec![false; num_rows];
            for col in pk.columns() {
                for (i, is_null) in row_is_null.iter_mut().enumerate() {
                    if col.is_null(i) {
                        *is_null = true;
                    }
                }
            }
            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer
                .write_batches(&[&pk])
                .context(IssueWithArrowProcessingSnafu {
                    index: es_index.to_string(),
                })?;
            writer.finish().context(IssueWithArrowProcessingSnafu {
                index: es_index.to_string(),
            })?;
            let values: Vec<serde_json::Value> = serde_json::from_reader(
                writer.into_inner().as_slice(),
            )
            .context(IssueWithJsonProcessingSnafu {
                index: es_index.to_string(),
            })?;
            values
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    if row_is_null.get(i).copied().unwrap_or(false) {
                        Ok(None)
                    } else {
                        serde_json::to_string(&v).map(Some)
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(IssueWithJsonProcessingSnafu {
                    index: es_index.to_string(),
                })
        }
    }
}

/// Stands in for a categorical token that does not look like one.
const UNRECOGNIZED_CATEGORY: &str = "<unrecognized>";

/// Longest categorical token accepted; Elasticsearch's longest exception name is well under it.
const MAX_CATEGORY_LEN: usize = 64;

/// Accept `value` only if it has the shape of an Elasticsearch categorical token.
///
/// Elasticsearch derives `error.type` from the exception's class name and its response keys
/// are fixed, so both are short `lower_snake_case` identifiers. Nothing in the response is
/// *guaranteed* to be, though — a proxy or a non-Elasticsearch endpoint can put anything
/// there, which is the same interference [`describe_unexpected_response`] exists to handle.
/// Rather than copy a network-provided string into an error, a token that does not match the
/// expected shape is replaced wholesale (never truncated, so no fragment of it survives).
/// This also keeps the message on one line, as the logging rules require.
pub(super) fn categorical_token(value: &str) -> &str {
    let looks_categorical = !value.is_empty()
        && value.len() <= MAX_CATEGORY_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if looks_categorical {
        value
    } else {
        UNRECOGNIZED_CATEGORY
    }
}

/// Describe a failed `_bulk` item from a whitelist of non-identifying fields.
///
/// The document `_id` is the row's primary key (see [`extract_primary_key_from_fields`]),
/// and Elasticsearch's free-form `error.reason` quotes it — a version conflict reads
/// `[<_id>]: version conflict, document already exists (current version [1])`, and a
/// mapper rejection quotes the offending field *value*. Both this error and the refresh
/// failure that wraps it are logged and recorded in `runtime.task_history`, so anything
/// carried here becomes operational output. Only fixed vocabulary is reported: the HTTP
/// `status`, the exception class names from `error.type` / `error.caused_by.type` (each
/// through [`categorical_token`]), and the item's position within the request — never
/// `reason`, never `_id`, never the item itself.
fn describe_bulk_failure(position: usize, op: &Value) -> String {
    let mut parts = Vec::with_capacity(3);

    if let Some(status) = op.get("status").and_then(Value::as_u64) {
        parts.push(format!("status {status}"));
    }

    let error = op.get("error");
    if let Some(kind) = error.and_then(|e| e.get("type")).and_then(Value::as_str) {
        parts.push(categorical_token(kind).to_string());
    }
    if let Some(cause) = error
        .and_then(|e| e.get("caused_by"))
        .and_then(|c| c.get("type"))
        .and_then(Value::as_str)
    {
        parts.push(format!("caused by {}", categorical_token(cause)));
    }

    if parts.is_empty() {
        // Neither a status nor a typed error: say so rather than falling back to
        // stringifying the item, which would name the document directly.
        parts.push("no status or error type reported".to_string());
    }

    // The position is relative to the `_bulk` request this response answers, which is one
    // chunk of `batch_write_rows` documents — not an offset into the whole record batch.
    format!(
        "document at position {position} in the request ({})",
        parts.join(", ")
    )
}

/// Top-level keys an Elasticsearch write or delete response is allowed to name.
///
/// Covers the `_bulk` response (`took`, `errors`, `items`), the `_delete_by_query` response,
/// the `{"task": …}` handle it returns instead when asked not to wait for completion, and
/// the `{"error": …, "status": …}` envelope they share on failure.
const KNOWN_RESPONSE_KEYS: &[&str] = &[
    "batches",
    "deleted",
    "error",
    "errors",
    "failures",
    "items",
    "noops",
    "requests_per_second",
    "retries",
    "status",
    "task",
    "throttled_millis",
    "throttled_until_millis",
    "timed_out",
    "took",
    "total",
    "version_conflicts",
];

/// Accept `key` only if it is one Elasticsearch itself puts at the top level of a response.
///
/// [`categorical_token`] is the wrong filter for a response key: it admits *any*
/// `lower_snake_case` string, and a document identifier (`customer_123`) has exactly that
/// shape, so it would be copied verbatim into the error and `runtime.task_history`. The set
/// of top-level keys is fixed and small, so match it exactly instead of by shape.
fn response_key_token(key: &str) -> &str {
    if KNOWN_RESPONSE_KEYS.contains(&key) {
        key
    } else {
        UNRECOGNIZED_CATEGORY
    }
}

/// Describe an unexpected `_bulk` response body by its shape alone.
///
/// A successful bulk response contains one item per document, each naming its `_id`, so the
/// body itself can never be reported. Its top-level key names are what actually distinguish
/// the interesting cases (an `{"error": …}` envelope from a proxy versus a truncated
/// response), and each goes through [`response_key_token`] — an unexpected response is
/// exactly the case where the keys are *not* Elasticsearch's own, so a key is reported only
/// when it is one Elasticsearch itself defines.
pub(super) fn describe_unexpected_response(resp: &Value) -> String {
    match resp {
        Value::Object(map) => {
            let mut keys: Vec<&str> = map.keys().map(|k| response_key_token(k)).collect();
            keys.sort_unstable();
            // Several rejected keys collapse onto the same placeholder; report it once.
            keys.dedup();
            if keys.is_empty() {
                "an empty JSON object".to_string()
            } else {
                format!("a JSON object with keys: {}", keys.join(", "))
            }
        }
        Value::Array(items) => format!("a JSON array of {} element(s)", items.len()),
        Value::String(_) => "a JSON string".to_string(),
        Value::Number(_) => "a JSON number".to_string(),
        Value::Bool(_) => "a JSON boolean".to_string(),
        Value::Null => "JSON null".to_string(),
    }
}

/// Check an ES `_bulk` response body and return an error if any items failed.
///
/// The bulk API returns HTTP 200 even when individual documents fail, so we must
/// inspect `errors` and the per-item `error` fields to surface problems. Every
/// reported detail goes through [`describe_bulk_failure`] /
/// [`describe_unexpected_response`], which keep the row's primary key out of the
/// message.
fn inspect_bulk_response(resp: &Value, es_index: &str) -> Result<()> {
    // ES _bulk response MUST include a boolean `errors` field. A missing or
    // non-boolean `errors` indicates an unexpected response shape (e.g. a
    // truncated response or a proxy layer interfering) — treat that as a
    // failure rather than silently succeeding.
    match resp.get("errors").and_then(Value::as_bool) {
        Some(false) => return Ok(()),
        Some(true) => {}
        None => {
            return Err(Error::BulkIndexItemErrors {
                index: es_index.to_string(),
                failures: 1,
                first: format!(
                    "Elasticsearch bulk response is missing a boolean `errors` field; got {}",
                    describe_unexpected_response(resp)
                ),
            });
        }
    }

    let (failures, first_error) = match resp.get("items").and_then(Value::as_array) {
        None => (
            1,
            "the response reported errors but carried no `items` array".to_string(),
        ),
        Some(items) => match scan_failed_items(items) {
            (failures, Some(first)) => (failures, first),
            // `errors: true` with no item carrying an `error` is a contradictory response.
            // Report one failure rather than the scanned zero: a message reading "0 document
            // failure(s)" would claim success by count while returning an error.
            (_, None) => (
                1,
                "the response reported errors but no item carried one".to_string(),
            ),
        },
    };

    Err(Error::BulkIndexItemErrors {
        index: es_index.to_string(),
        failures,
        first: first_error,
    })
}

/// Count the `_bulk` items that carry an `error`, and describe the first of them.
fn scan_failed_items(items: &[Value]) -> (usize, Option<String>) {
    let mut failures = 0usize;
    let mut first = None;
    for (position, item) in items.iter().enumerate() {
        // Each item is a map with one key (index/create/update/delete) → op result.
        let Some(op) = item.as_object().and_then(|m| m.values().next()) else {
            continue;
        };
        if op.get("error").is_some() {
            failures += 1;
            if first.is_none() {
                first = Some(describe_bulk_failure(position, op));
            }
        }
    }
    (failures, first)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        Error, MAX_CATEGORY_LEN, UNRECOGNIZED_CATEGORY, categorical_token, inspect_bulk_response,
    };

    /// A primary-key value of the shape that makes this a data-leak: identifying on its own.
    const SENTINEL_KEY: &str = "ada@example.com";
    /// One component of a composite key, which the write path encodes into the `_id` as JSON.
    const SENTINEL_COMPONENT: &str = "acct-99887766";

    fn describe(resp: &serde_json::Value) -> String {
        let Err(e) = inspect_bulk_response(resp, "idx") else {
            panic!("expected inspect_bulk_response to reject this response");
        };
        e.to_string()
    }

    fn failure_count(resp: &serde_json::Value) -> usize {
        let Err(Error::BulkIndexItemErrors { failures, .. }) = inspect_bulk_response(resp, "idx")
        else {
            panic!("expected a BulkIndexItemErrors rejection");
        };
        failures
    }

    mod eviction {
        use std::sync::Arc;

        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        use super::super::build_documents;
        use crate::index::elasticsearch::{
            ElasticsearchIndex, ElasticsearchIndexWriteMaintenance, unused_client,
        };
        use crate::metadata::MetadataColumns;

        const DIMS: i32 = 2;

        /// `build_documents` neither embeds nor talks to Elasticsearch — it is handed the
        /// embeddings and the `_id`s — so the index's client and embedder only have to exist.
        #[derive(Debug)]
        struct Unused;

        #[async_trait::async_trait]
        impl llms::embeddings::Embed for Unused {
            async fn embed(
                &self,
                _input: llms::embeddings::EmbeddingInput,
            ) -> llms::embeddings::Result<Vec<Vec<f32>>> {
                panic!("build_documents must not embed");
            }

            fn size(&self) -> i32 {
                DIMS
            }
        }

        fn index() -> ElasticsearchIndex {
            let source_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("content", DataType::Utf8, true),
            ]));
            ElasticsearchIndex {
                client: Arc::new(unused_client::UnusedClient),
                es_index: "idx".to_string(),
                embedded_column: "content".to_string(),
                vector_field: "content_vector".to_string(),
                text_fields: vec![],
                primary_key: vec![Field::new("id", DataType::Int64, false)],
                compute_query: Arc::new(Unused),
                dims: DIMS,
                similarity: "cosine".to_string(),
                source_schema,
                metadata_columns: MetadataColumns::none(),
                batch_write_rows: 100,
                write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::default()),
            }
        }

        fn record(ids: &[i64]) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("content", DataType::Utf8, true),
            ]));
            let contents: Vec<String> = ids.iter().map(|id| format!("row {id}")).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(contents)),
                ],
            )
            .expect("valid test batch")
        }

        fn keys(ids: &[i64]) -> Vec<Option<String>> {
            ids.iter().map(|id| Some(id.to_string())).collect()
        }

        fn evicted(ids: &[i64], embeddings: &[Option<Vec<f32>>]) -> Vec<String> {
            let index = index();
            let (_docs, evicted) = build_documents(&index, &record(ids), embeddings, &keys(ids))
                .expect("documents build");
            let mut evicted = evicted;
            evicted.sort();
            evicted
        }

        /// Regression test for #13503. A row rewritten from an indexable embedding to a
        /// rejected one is left out of the `_bulk` body, which only ever carries `index`
        /// actions — so the document stored under its `_id` survives untouched and search
        /// keeps returning it at the vector its previous text produced.
        #[test]
        fn an_all_zero_or_nan_embedding_evicts_its_document() {
            assert_eq!(
                evicted(
                    &[1, 2, 3],
                    &[
                        Some(vec![1.0, 2.0]),
                        Some(vec![0.0, 0.0]),
                        Some(vec![f32::NAN, f32::NAN]),
                    ],
                ),
                vec!["2".to_string(), "3".to_string()],
            );
        }

        /// Elasticsearch is the one backend that already rejected a *partially* non-finite
        /// vector, so this class is live on it today.
        #[test]
        fn a_partially_non_finite_embedding_evicts_its_document() {
            assert_eq!(
                evicted(
                    &[1, 2, 3],
                    &[
                        Some(vec![1.0, 2.0]),
                        Some(vec![1.0, f32::NAN]),
                        Some(vec![1.0, f32::INFINITY]),
                    ],
                ),
                vec!["2".to_string(), "3".to_string()],
            );
        }

        /// No embedding at all — a NULL or empty search text — leaves the same stale
        /// document behind as a rejected one.
        #[test]
        fn a_row_with_no_embedding_evicts_its_document() {
            assert_eq!(
                evicted(&[1, 2], &[Some(vec![1.0, 2.0]), None]),
                vec!["2".to_string()],
            );
        }

        #[test]
        fn a_batch_that_indexes_every_row_evicts_nothing() {
            assert!(
                evicted(&[1, 2], &[Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])]).is_empty(),
                "the happy path must issue no delete request at all"
            );
        }
    }

    #[test]
    fn a_clean_bulk_response_is_accepted() {
        let resp = json!({ "errors": false, "items": [{ "index": { "_id": SENTINEL_KEY } }] });
        inspect_bulk_response(&resp, "idx").expect("a response with errors:false must succeed");
    }

    /// Regression test for #12370: a version conflict names the document by `_id` in its
    /// `reason`, and `_id` is the row's primary key.
    #[test]
    fn a_version_conflict_does_not_report_the_primary_key() {
        let resp = json!({
            "errors": true,
            "items": [{
                "index": {
                    "_index": "idx",
                    "_id": SENTINEL_KEY,
                    "status": 409,
                    "error": {
                        "type": "version_conflict_engine_exception",
                        "reason": format!("[{SENTINEL_KEY}]: version conflict, document already exists (current version [1])"),
                        "index_uuid": "xIB-tPZlQm2rXwRDbhBGmA",
                        "shard": "0",
                        "index": "idx"
                    }
                }
            }]
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "primary key leaked into the error: {message}"
        );
        assert!(
            !message.contains("version conflict, document already exists"),
            "the free-form reason (which quotes the key) leaked: {message}"
        );
        assert!(
            message.contains("status 409") && message.contains("version_conflict_engine_exception"),
            "the failure must stay diagnosable by status and type: {message}"
        );
        assert!(
            message.contains("position 0"),
            "the failing document must still be locatable by position: {message}"
        );
    }

    /// A mapper rejection quotes the offending *value*, not just the `_id`.
    #[test]
    fn a_mapper_rejection_does_not_report_the_offending_value() {
        let resp = json!({
            "errors": true,
            "items": [{
                "index": {
                    "_id": SENTINEL_KEY,
                    "status": 400,
                    "error": {
                        "type": "document_parsing_exception",
                        "reason": format!("failed to parse field [email] of type [long] in document with id '{SENTINEL_KEY}'. Preview of field's value: '{SENTINEL_KEY}'"),
                        "caused_by": {
                            "type": "illegal_argument_exception",
                            "reason": format!("For input string: \"{SENTINEL_KEY}\"")
                        }
                    }
                }
            }]
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "the offending field value leaked into the error: {message}"
        );
        assert!(
            message.contains("status 400")
                && message.contains("document_parsing_exception")
                && message.contains("caused by illegal_argument_exception"),
            "the exception classes must survive redaction: {message}"
        );
    }

    /// A composite primary key is encoded into the `_id` as JSON, so every component is in it.
    #[test]
    fn a_composite_key_does_not_reach_the_error() {
        let composite = json!([SENTINEL_KEY, SENTINEL_COMPONENT]).to_string();
        let resp = json!({
            "errors": true,
            "items": [{
                "create": {
                    "_id": composite,
                    "status": 409,
                    "error": {
                        "type": "version_conflict_engine_exception",
                        "reason": format!("[{composite}]: version conflict")
                    }
                }
            }]
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY) && !message.contains(SENTINEL_COMPONENT),
            "a composite key component leaked into the error: {message}"
        );
    }

    /// The pre-fix code stringified the whole item when it carried no `error` object, and the
    /// item names the document directly.
    #[test]
    fn an_item_without_a_typed_error_is_not_stringified() {
        let resp = json!({
            "errors": true,
            "items": [{
                "index": { "_id": SENTINEL_KEY, "error": { "reason": format!("[{SENTINEL_KEY}] rejected") } }
            }]
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "the item leaked into the error: {message}"
        );
        assert!(
            message.contains("no status or error type reported"),
            "an untyped failure must say so rather than dumping the item: {message}"
        );
    }

    /// The pre-fix code interpolated the entire response body, which carries every `_id`.
    #[test]
    fn an_unexpected_response_is_reported_by_shape_only() {
        let resp = json!({
            "took": 3,
            "items": [{ "index": { "_id": SENTINEL_KEY, "status": 201 } }]
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "the response body leaked every document id: {message}"
        );
        assert!(
            message.contains("missing a boolean `errors` field")
                && message.contains("a JSON object with keys: items, took"),
            "the response shape must stay diagnosable: {message}"
        );
    }

    #[test]
    fn a_non_object_unexpected_response_is_reported_by_shape_only() {
        let resp = json!([{ "index": { "_id": SENTINEL_KEY } }]);
        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "the response body leaked: {message}"
        );
        assert!(
            message.contains("a JSON array of 1 element(s)"),
            "expected the array shape to be named: {message}"
        );
    }

    #[test]
    fn every_failure_is_counted_and_the_first_one_is_located() {
        let ok_item = json!({ "index": { "_id": "ok", "status": 201 } });
        let failed = |id: &str, status: u16| {
            json!({ "index": {
                "_id": id,
                "status": status,
                "error": { "type": "version_conflict_engine_exception", "reason": format!("[{id}]") }
            }})
        };
        let resp = json!({
            "errors": true,
            "items": [ok_item, failed(SENTINEL_KEY, 409), failed("second", 429)]
        });

        assert_eq!(
            failure_count(&resp),
            2,
            "both failing items must be counted"
        );

        let message = describe(&resp);
        assert!(
            message.contains("position 1"),
            "the first failure's position must be its index in the request, not 0: {message}"
        );
        assert!(
            !message.contains(SENTINEL_KEY),
            "primary key leaked into the error: {message}"
        );
    }

    #[test]
    fn errors_true_without_an_items_array_is_still_an_error() {
        let resp = json!({ "errors": true });
        let message = describe(&resp);
        assert!(
            message.contains("carried no `items` array"),
            "expected the missing-items case to be named: {message}"
        );
    }

    #[test]
    fn errors_true_with_no_failing_item_is_still_an_error() {
        let resp = json!({ "errors": true, "items": [{ "index": { "_id": SENTINEL_KEY, "status": 201 } }] });
        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "primary key leaked into the error: {message}"
        );
        assert!(
            message.contains("no item carried one"),
            "expected the contradictory-response case to be named: {message}"
        );
        assert_eq!(
            failure_count(&resp),
            1,
            "a contradictory response must not report 0 failures while returning an error"
        );
    }

    /// `error.type` is a network-provided string, not a code-enforced constant: a proxy or a
    /// non-Elasticsearch endpoint can put row data or a newline there.
    #[test]
    fn a_type_field_that_is_not_a_categorical_token_is_replaced_wholesale() {
        for hostile in [
            SENTINEL_KEY,
            SENTINEL_COMPONENT,
            "version_conflict\nada@example.com",
            "Version_Conflict_Engine_Exception",
            &"a".repeat(MAX_CATEGORY_LEN + 1),
            "",
        ] {
            let resp = json!({
                "errors": true,
                "items": [{ "index": {
                    "status": 409,
                    "error": { "type": hostile, "caused_by": { "type": hostile } }
                }}]
            });

            let message = describe(&resp);
            assert!(
                !message.contains(SENTINEL_KEY) && !message.contains(SENTINEL_COMPONENT),
                "a hostile `type` reached the error: {message}"
            );
            assert!(
                !message.contains('\n') && !message.contains('\r'),
                "the error must stay on one line: {message:?}"
            );
            assert!(
                message.contains(UNRECOGNIZED_CATEGORY),
                "a rejected token must be named as unrecognized: {message}"
            );
            assert!(
                message.contains("status 409"),
                "the status must survive a rejected type: {message}"
            );
        }
    }

    /// The keys of an *unexpected* response are, by definition, not Elasticsearch's own.
    #[test]
    fn hostile_response_keys_are_replaced_and_reported_once() {
        let resp = json!({
            "took": 3,
            SENTINEL_KEY: 1,
            SENTINEL_COMPONENT: 2,
            "bad\nkey": 3
        });

        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY) && !message.contains(SENTINEL_COMPONENT),
            "a hostile response key reached the error: {message}"
        );
        assert!(
            !message.contains('\n') && !message.contains('\r'),
            "the error must stay on one line: {message:?}"
        );
        assert_eq!(
            message.matches(UNRECOGNIZED_CATEGORY).count(),
            1,
            "the three rejected keys must collapse to one placeholder: {message}"
        );
        assert!(
            message.contains("took"),
            "a legitimate key must still be reported: {message}"
        );
    }

    /// A document identifier is `lower_snake_case` too, so shape alone cannot reject it.
    #[test]
    fn an_identifier_shaped_response_key_is_replaced() {
        let resp = json!({ "took": 3, "customer_123": 1, "order_2026_08_08": 2 });

        let message = describe(&resp);
        assert!(
            !message.contains("customer_123") && !message.contains("order_2026_08_08"),
            "an identifier-shaped response key reached the error: {message}"
        );
        assert!(
            message.contains("took"),
            "a legitimate key must still be reported: {message}"
        );
    }

    #[test]
    fn a_real_exception_name_is_accepted_unchanged() {
        assert_eq!(
            categorical_token("version_conflict_engine_exception"),
            "version_conflict_engine_exception"
        );
        assert_eq!(categorical_token("http_2_error"), "http_2_error");
        assert_eq!(categorical_token("ada@example.com"), UNRECOGNIZED_CATEGORY);
        assert_eq!(categorical_token("acct-99887766"), UNRECOGNIZED_CATEGORY);
    }

    #[test]
    fn a_non_boolean_errors_field_is_treated_as_an_unexpected_shape() {
        let resp = json!({ "errors": "true", "items": [{ "index": { "_id": SENTINEL_KEY } }] });
        let message = describe(&resp);
        assert!(
            !message.contains(SENTINEL_KEY),
            "primary key leaked into the error: {message}"
        );
        assert!(
            message.contains("missing a boolean `errors` field"),
            "expected the non-boolean `errors` case to be rejected by shape: {message}"
        );
    }
}
