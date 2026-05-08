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

use crate::index::elasticsearch::ElasticsearchIndex;
use crate::index::embedding_col;

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
    let primary_keys = extract_primary_key(index, &record)?;

    // Build all documents in a sync block so the arrow-json encoders (which are
    // `!Send`) are dropped before any subsequent `.await`.
    let docs: Vec<(Option<String>, Value)> =
        build_documents(index, &record, &embedding_vectors, &primary_keys)?;

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
/// Kept sync so the arrow-json encoders it uses (which are `!Send`) stay off
/// the async state machine.
fn build_documents(
    index: &ElasticsearchIndex,
    record: &RecordBatch,
    embedding_vectors: &[Option<Vec<f32>>],
    primary_keys: &[Option<String>],
) -> Result<Vec<(Option<String>, Value)>> {
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

    for row in 0..record.num_rows() {
        let Some(embedding) = embedding_vectors[row].as_ref() else {
            missing_embedding_skips += 1;
            continue;
        };

        // Skip rows with NULL primary keys when a primary key is configured.
        // Without an `_id`, Elasticsearch would auto-generate one, making
        // re-indexing the same row non-idempotent (producing duplicates on
        // refresh/CDC writes).
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
            continue;
        }

        if embedding.iter().any(|x| !x.is_finite()) {
            non_finite_skips += 1;
            if non_finite_samples.len() < SAMPLE_LIMIT {
                non_finite_samples.push(row);
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
            let v: Value =
                serde_json::from_slice(&value_buf).context(IssueWithJsonProcessingSnafu {
                    index: es_index.to_string(),
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
            "Skipped {zero_or_nan_skips} record(s) for Elasticsearch index '{es_index}': embedding vector is all zeros or NaN. Sample row indices: {zero_or_nan_samples:?}"
        );
    }
    if non_finite_skips > 0 {
        tracing::warn!(
            "Skipped {non_finite_skips} record(s) for Elasticsearch index '{es_index}': embedding vector contains non-finite values (NaN or infinity). Sample row indices: {non_finite_samples:?}"
        );
    }
    if missing_embedding_skips > 0 {
        tracing::debug!(
            "Skipped {missing_embedding_skips} record(s) for Elasticsearch index '{es_index}': no embedding generated (expected when embedded column is NULL)."
        );
    }

    Ok(docs)
}

fn extract_primary_key(
    index: &ElasticsearchIndex,
    record: &RecordBatch,
) -> Result<Vec<Option<String>>> {
    let schema = record.schema();

    match index.primary_key.as_slice() {
        [] => Ok(vec![None; record.num_rows()]),
        [f] => {
            let Some((i, _)) = schema.column_with_name(f.name()) else {
                return PrimaryKeyColumnNotFoundSnafu {
                    index: index.es_index.clone(),
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
                    index: index.es_index.clone(),
                },
            )?;
            let iter_opt: Option<Box<dyn Iterator<Item = Option<&str>> + Send>> =
                convert_string_arrow_to_iterator!(casted);
            let Some(iter) = iter_opt else {
                return Err(Error::IssueWithArrowProcessing {
                    index: index.es_index.clone(),
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
            // Composite key: JSON-encode the projected key columns and use that string as _id.
            let mut proj = Vec::with_capacity(fields.len());
            for f in fields {
                let Some((i, _)) = schema.column_with_name(f.name()) else {
                    return PrimaryKeyColumnNotFoundSnafu {
                        index: index.es_index.clone(),
                        column: f.name().clone(),
                    }
                    .fail();
                };
                proj.push(i);
            }
            let pk = record
                .project(&proj)
                .context(IssueWithArrowProcessingSnafu {
                    index: index.es_index.clone(),
                })?;

            // A row with any NULL component in a composite key has no stable
            // identity. Mark those rows as `None` so the null-PK skip logic
            // (which already handles single-column keys) applies here too.
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
                    index: index.es_index.clone(),
                })?;
            writer.finish().context(IssueWithArrowProcessingSnafu {
                index: index.es_index.clone(),
            })?;

            let values: Vec<Value> = serde_json::from_reader(writer.into_inner().as_slice())
                .context(IssueWithJsonProcessingSnafu {
                    index: index.es_index.clone(),
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
                    index: index.es_index.clone(),
                })
        }
    }
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
        if o.is_none() || o.is_some_and(str::is_empty) {
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
                builder.values().append_nulls(expected);
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
fn extract_primary_key_from_fields(
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

/// Check an ES `_bulk` response body and return an error if any items failed.
///
/// The bulk API returns HTTP 200 even when individual documents fail, so we must
/// inspect `errors` and the per-item `error` fields to surface problems.
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
                    "Elasticsearch bulk response is missing a boolean `errors` field; got: {resp}"
                ),
            });
        }
    }

    let items = resp.get("items").and_then(Value::as_array);
    let (failures, first_error) = items.map_or((1usize, "unknown".to_string()), |arr| {
        let mut failures = 0usize;
        let mut first: Option<String> = None;
        for item in arr {
            // Each item is a map with one key (index/create/update/delete) → op result.
            let Some(op) = item.as_object().and_then(|m| m.values().next()) else {
                continue;
            };
            if op.get("error").is_some() {
                failures += 1;
                if first.is_none() {
                    first = Some(
                        op.get("error")
                            .map_or_else(|| op.to_string(), ToString::to_string),
                    );
                }
            }
        }
        (failures, first.unwrap_or_else(|| "unknown".to_string()))
    });

    Err(Error::BulkIndexItemErrors {
        index: es_index.to_string(),
        failures,
        first: first_error,
    })
}
