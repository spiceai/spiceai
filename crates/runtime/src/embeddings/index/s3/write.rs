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
use datafusion::catalog::TableProvider;
use runtime_datafusion_index::Index;
use serde_json::Value;
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::{
    convert_string_arrow_to_iterator,
    embeddings::index::{
        IndexEmbeddingConfig, s3::S3VectorIndex,
    },
    model::EmbeddingModelStore,
};

/// Extra index data from the raw table batches, embedded required column and write to [`S3VectorsTable`].
#[allow(clippy::too_many_lines)]
pub async fn write(index: &S3VectorIndex, cfg: &IndexEmbeddingConfig, record: &RecordBatch) {
    let Some((embedded_column_idx, _)) = record
        .schema()
        .column_with_name(index.embedded_column.as_str())
    else {
        tracing::warn!(
            "Cannot write to '{}' index, data does not have column '{}'.",
            index.name(),
            index.embedded_column
        );
        return;
    };

    let embedding_vectors = match embed_column(
        record,
        embedded_column_idx,
        cfg.model_name.as_str(),
        Arc::clone(&cfg.embedding_models),
    )
    .await
    {
        Ok(vectors) => vectors,
        Err(e) => {
            tracing::error!(
                "Failed to update '{}' index. An error occurred embedding the underlying dataset column '{}'. Error: '{e}'.",
                index.name(),
                index.embedded_column
            );
            return;
        }
    };
    let metadata = match extract_and_format_metadata(&index.metadata_columns.all_names(), record) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(
                "When writing to vector index '{}', failed to prepare metadata: {e}",
                index.name()
            );
            return;
        }
    };
    let primary_key = match extract_and_format_primary_key(&index.primary_key, record) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(
                "When writing to vector index '{}', failed to prepare primary key: {e}",
                index.name()
            );
            return;
        }
    };
    if primary_key.len() != embedding_vectors.len() {
        tracing::error!(
            "When writing to vector index '{}', incompatible number of unique rows ({}) and embedding vectors ({}).",
            index.name(),
            primary_key.len(),
            embedding_vectors.len(),
        );
        return;
    }
    for (name, v) in &metadata {
        if v.len() != embedding_vectors.len() {
            tracing::error!(
                "When writing to vector index '{}', incompatible number of unique rows ({}) and rows of '{}' metadata ({}).",
                index.name(),
                primary_key.len(),
                name,
                embedding_vectors.len(),
            );
            return;
        }
    }

    if let Err(e) = index
        .table
        .write_data(embedding_vectors, primary_key, metadata)
        .await
    {
        tracing::error!("Cannot write to '{}' index: {e}", index.name());
    }
}

/// Given a [`RecordBatch`] of data from a [`VectorIndex`]'s associated [`TableProvider`], extract and format the primary key, so as to be ready for indexing into `S3Vectors`.
///
/// Formatting is:
///  - When there is a single [`Field`] in `primary_key`, the relevant [`ArrayRef`] is cast to a [`StringArray`] via [`arrow::compute::cast`].
///  - Otherwise, consider the [`Field`] as a sub-[`RecordBatch`] and convert to a string via [`arrow_json`].
pub fn extract_and_format_primary_key(
    primary_key: &[Field],
    record: &RecordBatch,
) -> Result<Vec<Option<String>>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = record.schema();
    match primary_key {
        [f] => {
            let Some((i, _)) = schema.column_with_name(f.name().as_str()) else {
                return Err(Box::from(format!(
                    "data does not have primary key column '{}'.",
                    f.name()
                )));
            };
            let c = record.column(i);

            // If already string like, continue
            if let Some(data) = convert_string_arrow_to_iterator!(c) {
                return Ok(to_string_vec(data));
            }

            // Otherwise cast to UTF8.
            let str_array = arrow::compute::cast(&c, &arrow_schema::DataType::Utf8).boxed()?;
            let Some(data) = convert_string_arrow_to_iterator!(str_array) else {
                return Err(Box::from(format!(
                    "primary key '{}' in data could not be serialized",
                    f.name()
                )));
            };
            Ok(to_string_vec(data))
        }
        [] => Err(Box::from(
            "data does not have a primary key column".to_string(),
        )),
        _ => {
            let mut primary_key_projection = vec![];
            for field in primary_key {
                let Some((idx, _)) = schema.column_with_name(field.name().as_str()) else {
                    return Err(Box::from(format!(
                        "data does not have primary key column '{}'.",
                        field.name()
                    )));
                };
                primary_key_projection.push(idx);
            }
            let pk = record.project(&primary_key_projection).boxed()?;

            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer.write_batches(&[&pk]).boxed()?;
            writer.finish().boxed()?;
            serde_json::from_reader::<_, Vec<Option<String>>>(writer.into_inner().as_slice())
                .boxed()
        }
    }
}

pub fn extract_and_format_metadata(
    metadata_columns: &[String],
    record: &RecordBatch,
) -> Result<HashMap<String, Vec<Option<Value>>>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = record.schema();
    let mut metadata_projection = vec![];
    for name in metadata_columns {
        let Some((idx, _)) = schema.column_with_name(name) else {
            return Err(Box::from(format!(
                "data does not have metadata column '{name}'.",
            )));
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
) -> Result<Vec<Option<Vec<f32>>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(data) = convert_string_arrow_to_iterator!(rb.column(column_idx)) else {
        return Ok(vec![]);
    };

    let embedding_guard = embedding_models.read().await;
    let Some(model) = embedding_guard.get(model_name) else {
        return Err(Box::from(format!(
            "Embedding model '{model_name}' was not found"
        )));
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
        .boxed()?;

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
