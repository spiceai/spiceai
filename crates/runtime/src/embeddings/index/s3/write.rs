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

use arrow::array::{LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_json::{EncoderOptions, writer::make_encoder};
use arrow_schema::Field;
use async_openai::types::EmbeddingInput;
use itertools::Itertools;
use runtime_datafusion_index::Index;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;

use crate::{
    convert_string_arrow_to_iterator,
    embeddings::index::{VectorIndex, s3::S3Vector},
    model::EmbeddingModelStore,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Embedding model '{model_name}' was not found"))]
    EmbeddingModelNotFound { model_name: String },

    #[snafu(display("{source}"))]
    FailedToEmbed { source: llms::embeddings::Error },

    #[snafu(display(
        "Failed to update '{index}' index. An error occurred embedding the underlying dataset column '{column}'. Error: '{source}'."
    ))]
    FailedToEmbedColumn {
        index: String,
        column: String,
        source: Box<Error>,
    },

    #[snafu(display("Cannot write to '{index}' index, data does not have column '{column}'."))]
    ColumnNotFound { index: String, column: String },

    #[snafu(display("Cannot write to '{index}' index, index has no primary key field(s)."))]
    NoPrimaryKeyField { index: String },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing arrow records: {source}."
    ))]
    ArrowError {
        index: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing JSON values: {source}."
    ))]
    JsonError {
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
}

/// Extra index data from the raw table batches, embedded required column and write to [`S3VectorsTable`].
#[allow(clippy::too_many_lines)]
pub async fn write(index: &S3Vector, record: &RecordBatch) -> Result<(), Error> {
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        tracing::warn!(
            "Cannot write to '{}' index, data does not have column '{}'.",
            index.name(),
            index.embedded_column
        );
        return Ok(());
    };

    let embedding_vectors = embed_column(
        record,
        embedded_column_idx,
        index.model_name.as_str(),
        Arc::clone(&index.embedding_models),
    )
    .await?;

    let metadata =
        extract_and_format_metadata(index.name(), &index.metadata_columns.all_names(), record)?;
    let primary_key = extract_and_format_primary_key(index.name(), &index.primary_key, record)?;

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

    index
        .table
        .write_data(embedding_vectors, primary_key, metadata)
        .await
        .context(CannotWriteIndexSnafu {
            index: index.name().to_string(),
        })
}

/// Given a [`RecordBatch`] of data from a [`VectorIndex`]'s associated [`TableProvider`], extract and format the primary key, so as to be ready for indexing into `S3Vectors`.
///
/// Formatting is:
///  - When there is a single [`Field`] in `primary_key`, the relevant [`ArrayRef`] is cast to a [`StringArray`] via [`arrow::compute::cast`].
///  - Otherwise, consider the [`Field`] as a sub-[`RecordBatch`] and convert to a string via [`arrow_json`].
pub fn extract_and_format_primary_key(
    index_name: &str,
    primary_key: &[Field],
    record: &RecordBatch,
) -> Result<Vec<Option<String>>, Error> {
    let schema = record.schema();
    match primary_key {
        [f] => {
            let Some((i, _)) = schema.column_with_name(f.name().as_str()) else {
                return ColumnNotFoundSnafu {
                    index: index_name.to_string(),
                    column: f.name().clone(),
                }
                .fail();
            };
            let c = record.column(i);

            // If already string like, continue
            if let Some(data) = convert_string_arrow_to_iterator!(c) {
                return Ok(to_string_vec(data));
            }

            // Otherwise cast to UTF8.
            let string_arr =
                arrow::compute::cast(&c, &arrow_schema::DataType::Utf8).context(ArrowSnafu {
                    index: index_name.to_string(),
                })?;
            let Some(data) = convert_string_arrow_to_iterator!(string_arr) else {
                return Err(Error::FailedToSerializePrimaryKey {
                    index: index_name.to_string(),
                    source: Box::from(format!(
                        "could not cast a '{}' column (column '{}') into string type",
                        f.data_type(),
                        f.name()
                    )),
                });
            };
            Ok(to_string_vec(data))
        }
        [] => Err(Error::NoPrimaryKeyField {
            index: index_name.to_string(),
        }),
        _ => {
            let mut primary_key_projection = vec![];
            for field in primary_key {
                let Some((idx, _)) = schema.column_with_name(field.name().as_str()) else {
                    return ColumnNotFoundSnafu {
                        index: index_name.to_string(),
                        column: field.name().clone(),
                    }
                    .fail();
                };
                primary_key_projection.push(idx);
            }
            let pk = record
                .project(&primary_key_projection)
                .context(ArrowSnafu {
                    index: index_name.to_string(),
                })?;

            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer.write_batches(&[&pk]).context(ArrowSnafu {
                index: index_name.to_string(),
            })?;
            writer.finish().context(ArrowSnafu {
                index: index_name.to_string(),
            })?;

            let values = serde_json::from_reader::<_, Vec<Value>>(writer.into_inner().as_slice())
                .context(JsonSnafu {
                index: index_name.to_string(),
            })?;

            values
                .into_iter()
                .map(|v| serde_json::to_string(&v).map(Some))
                .collect::<Result<Vec<_>, _>>()
                .context(JsonSnafu {
                    index: index_name.to_string(),
                })
        }
    }
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
            return ColumnNotFoundSnafu {
                index: index_name.to_string(),
                column: name,
            }
            .fail();
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
    model_name: &str,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
) -> Result<Vec<Option<Vec<f32>>>, Error> {
    let Some(data) = convert_string_arrow_to_iterator!(rb.column(column_idx)) else {
        return Ok(vec![]);
    };

    let embedding_guard = embedding_models.read().await;
    let Some(model) = embedding_guard.get(model_name) else {
        return EmbeddingModelNotFoundSnafu {
            model_name: model_name.to_string(),
        }
        .fail();
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

    let mut result: Vec<Option<Vec<f32>>> = vec![];
    let mut value_ptr = 0;
    let mut null_ptr = 0;

    while value_ptr < embedded_data.len() || null_ptr < nulls.len() {
        while null_ptr < nulls.len() && nulls[null_ptr] == result.len() {
            result.push(None);
            null_ptr += 1;
        }
        if value_ptr < embedded_data.len() {
            result.push(Some(embedded_data[value_ptr].clone()));
            value_ptr += 1;
        }
    }

    Ok(result)
}
