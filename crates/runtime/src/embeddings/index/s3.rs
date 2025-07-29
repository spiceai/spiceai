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

use std::{any::Any, collections::HashMap, str::FromStr, sync::Arc};

use arrow::{
    array::{LargeStringArray, RecordBatch, StringArray, StringViewArray},
    datatypes::SchemaRef,
};
use arrow_json::{EncoderOptions, writer::make_encoder};
use arrow_schema::Field;
use async_openai::types::EmbeddingInput;
use data_components::s3_vectors::{
    MetadataColumn, MetadataColumns, S3VectorIdentifier, S3VectorTableResult, S3VectorsTable,
};
use datafusion::{catalog::TableProvider, sql::TableReference};
use llms::embeddings::get_or_infer_size;
use runtime_datafusion_index::Index;
use s3_vectors::{Client, S3Vectors};
use serde_json::Value;
use snafu::ResultExt;
use spicepod::{
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig},
    vector::VectorStore,
};
use tokio::sync::RwLock;

use crate::{
    convert_string_arrow_to_iterator,
    dataconnector::parameters::aws::load_config,
    embeddings::index::{IndexEmbeddingConfig, S3Vector, retry_client::S3VectorRetryClientBuilder},
    get_params_with_secrets,
    model::EmbeddingModelStore,
    parameters::{ParameterSpec, Parameters},
    search::util::get_primary_keys,
    secrets::Secrets,
};

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("bucket")
        .description("The S3 bucket name to use for the S3 Vectors index.")
        .secret(),
    ParameterSpec::component("arn")
        .description("The S3 Vectors bucket ARN to use for the S3 Vectors index.")
        .secret(),
    ParameterSpec::component("index")
        .description("The S3 Vectors index name to use within the bucket.")
        .secret(),
    ParameterSpec::component("aws_region")
        .description("The AWS region to use.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID to use.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key to use.")
        .secret(),
    ParameterSpec::component("aws_session_token")
        .description("The AWS session token to use.")
        .secret(),
];

/// Extra index data from the raw table batches, embedded required column and write to [`S3VectorsTable`].
#[allow(clippy::too_many_lines)]
pub async fn write(index: &S3VectorIndex, cfg: &IndexEmbeddingConfig, record: &RecordBatch) {
    let schema = record.schema();
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

    let mut primary_key_projection = vec![];
    for field in &index.primary_key {
        let Some((idx, _)) = schema.column_with_name(field.name().as_str()) else {
            tracing::error!(
                "Cannot write to '{}' index, data does not have column '{}'.",
                index.name(),
                field.name()
            );
            return;
        };
        primary_key_projection.push(idx);
    }

    let mut metadata_projection = vec![];
    for field in index.metadata_columns.iter() {
        let Some((idx, _)) = schema.column_with_name(field.name()) else {
            tracing::error!(
                "Cannot write to '{}' index, data does not have column '{}'.",
                index.name(),
                field.name()
            );
            return;
        };
        metadata_projection.push(idx);
    }

    // Happy to clone arrays here and consume them below in `table.write_data`.
    let mut primary_keys: HashMap<String, Vec<Option<String>>> = primary_key_projection
        .iter()
        .filter_map(|i| {
            let c = record.column(*i);
            let name = schema.field(*i).name();

            // If already string like, continue
            if let Some(data) = convert_string_arrow_to_iterator!(c) {
                return Some((name.clone(), to_string_vec(data)));
            }

            // Otherwise cast to UTF8.
            let str_array = arrow::compute::cast(&c, &arrow_schema::DataType::Utf8).ok()?;
            let data = convert_string_arrow_to_iterator!(str_array)?;
            Some((name.clone(), to_string_vec(data)))
        })
        .collect();

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

    // Check all columns are of same length. Check all are same as vector.
    let num_vectors = embedding_vectors.len();

    if let Some((name, values)) = primary_keys.iter().find(|(_, v)| v.len() != num_vectors) {
        tracing::error!(
            "Cannot write to '{}' index, as provided data has mismatch lengths. embedding column has {embed} rows, whilst primary key column '{name}' has {len} rows. {embed} != {len}.",
            index.name(),
            embed = num_vectors,
            len = values.len()
        );
        return;
    }

    if let Some((name, values)) = metadata.iter().find(|(_, v)| v.len() != num_vectors) {
        tracing::error!(
            "Cannot write to '{}' index, as provided data has mismatch lengths. embedding column has {embed} rows, whilst metadata column '{name}' has {len} rows. {embed} != {len}.",
            index.name(),
            embed = num_vectors,
            len = values.len()
        );
        return;
    }

    // Currently, we only support when there is one primary key column.
    if primary_keys.len() > 1 {
        tracing::debug!("primary_keys.len() > 1");
        return;
    }
    if primary_keys.is_empty() {
        tracing::debug!("primary_keys.is_empty()");
        return;
    }

    let Some(pk_field) = index.primary_key.first() else {
        tracing::debug!("primary_keys.is_empty()");
        return;
    };
    if let Some(key) = primary_keys.remove(pk_field.name()) {
        if let Err(e) = index
            .table
            .write_data(embedding_vectors, key, metadata)
            .await
        {
            tracing::error!("Cannot write to '{}' index: {e}", index.name());
        }
    } else {
        tracing::warn!(
            "Cannot write to '{}' index, no primary key was specified",
            index.name()
        );
    }
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

/// Attempt to construct a  S3 `VectorIndex` for the provided dataset on the given column.
#[allow(clippy::too_many_arguments)]
pub async fn try_from_dataset(
    ds_name: &TableReference,
    column: String,
    config: ColumnLevelEmbeddingConfig,
    vector_store_config: &VectorStore,
    underlying: Arc<dyn TableProvider>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    dataset_columns: Vec<Column>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<S3Vector, Box<dyn std::error::Error + Send + Sync>> {
    // Primary key. Use override from spicepod, fallback to underlying [`TableProvider`].
    let pks_from_table = get_primary_keys(&underlying).await.boxed()?;
    let inner_schema = underlying.schema();
    let primary_key: Vec<_> = config
        .row_ids
        .clone()
        .unwrap_or(pks_from_table)
        .into_iter()
        .filter_map(|c| {
            let (_, f) = inner_schema.column_with_name(c.as_str())?;
            Some(f.clone())
        })
        .collect();

    let metadata_columns = s3_vector_metadata_columns(&dataset_columns, &inner_schema);

    tracing::debug!("s3 vector index metadata columns: {metadata_columns:?}");

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;

    let table = try_vector_table(
        metadata_columns.clone(),
        params,
        format!("{}-{}-{}", ds_name, column, config.model)
            .replace('_', "-")
            .as_str(),
        Arc::clone(&embedding_models),
        config.model.as_str(),
    )
    .await?;

    Ok(S3Vector::new(
        S3VectorIndex {
            table,
            embedded_column: column.clone(),
            primary_key,
            metadata_columns,
        },
        super::IndexEmbeddingConfig {
            model_name: config.model.clone(),
            embedding_models,
        },
    ))
}

#[allow(clippy::cast_sign_loss)]
async fn embedding_vector_size(
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    model_name: &str,
) -> Option<usize> {
    let model_guard = embedding_models.read().await;
    let model = model_guard.get(model_name)?;
    get_or_infer_size(model).await.ok().map(|i| i as usize)
}

// Attempt to construct a S3 vector table from user-provided parameters.
//
// If no index name provided (either explicitly, or in ARN), use `default_s3_index_name`.
#[allow(clippy::cast_possible_wrap)]
async fn try_vector_table(
    columns: MetadataColumns,
    params: Parameters,
    default_s3_index_name: &str,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    model_name: &str,
) -> Result<S3VectorsTable, Box<dyn std::error::Error + Send + Sync>> {
    let s3_vectors_arn = string_from_params(&params, "arn");
    let s3_vectors_bucket = string_from_params(&params, "bucket");
    let s3_vectors_index = string_from_params(&params, "index");

    let id = match (s3_vectors_arn, s3_vectors_bucket, s3_vectors_index) {
        (Some(_), Some(_), Some(_)) => Err("Cannot specify both 's3_vectors_arn' and 's3_vectors_bucket'.".to_string()),
        (Some(arn), None, None) => Ok(S3VectorIdentifier::IndexArn(arn.to_string())),
        (None, Some(bucket), Some(index)) => Ok(S3VectorIdentifier::Index {
            bucket_name: bucket.to_string(),
            index_name: index.to_string(),
        }),
        (None, Some(bucket), None) => Ok(S3VectorIdentifier::Index {
            bucket_name: bucket.to_string(),
            index_name: default_s3_index_name.to_string(),
        }),
        (None, None, Some(_)) => Err("'s3_vectors_index' provided without associated 's3_vectors_bucket'.".to_string()),
        (Some(_), None, Some(_)) | (Some(_), Some(_), None) => {
            Err("'s3_vectors_arn' cannot be used with either 's3_vectors_bucket' or 's3_vectors_index'.".to_string())
        }
        (None, None, None) => Err("For S3, one of 's3_vectors_arn' or 's3_vectors_bucket' must be provided as a vector parameter".to_string()),
    }
    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        Box::from(format!("Invalid S3 Vectors bucket defined: {e}"))
    })?;

    let config = load_config(
        "S3Vectors",
        "aws_region",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        &params,
    )
    .await?;

    let s3_vector_client = Client::new(&config);

    let s3_vector_client = Arc::new(S3VectorRetryClientBuilder::new(s3_vector_client).build())
        as Arc<dyn S3Vectors + Send + Sync>;

    // See if the index already exists and return early if so.
    if let S3VectorTableResult::Table(vector_table) =
        S3VectorsTable::try_new_table(id.clone(), Arc::clone(&s3_vector_client), columns.clone())
            .await
            .boxed()?
    {
        return Ok(vector_table);
    }

    let Some(dimension) = embedding_vector_size(embedding_models, model_name).await else {
        return Err(Box::from(
            "S3 Vectors index does not exist. Could not be created because the embedding dimension could not be inferred.".to_string()
        ));
    };

    let Some(vector_table) =
        S3VectorsTable::try_create_new_table(id, s3_vector_client, dimension as i64, columns)
            .await?
    else {
        return Err(Box::from(
            "S3 Vectors index does not exist. After it was created, it still does not exist. Unexpected.".to_string()
        ));
    };
    Ok(vector_table)
}

// Attempt to get a certain string-value from the parameter.
//
// Returns `None` if the key does not exist
fn string_from_params<'a>(p: &'a Parameters, key: &str) -> Option<&'a str> {
    p.get(key).expose().ok()
}

/// Convert raw params configuration to parameters with secret support
async fn get_store_params(
    vector_store_config: &VectorStore,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Parameters, Box<dyn std::error::Error + Send + Sync>> {
    let params = vector_store_config
        .params
        .as_ref()
        .map(Params::as_string_map)
        .unwrap_or_default();

    let params_with_secrets = get_params_with_secrets(Arc::clone(&secrets), &params).await;

    let params = Parameters::try_new(
        "AWS S3 Vectors store",
        params_with_secrets.into_iter().collect(),
        "s3_vectors",
        Arc::clone(&secrets),
        PARAMETERS,
    )
    .await?;

    Ok(params)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum S3VectorMetadataColumn {
    Filterable,
    NonFilterable,
}

impl FromStr for S3VectorMetadataColumn {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "filterable" => Ok(S3VectorMetadataColumn::Filterable),
            "non-filterable" | "non_filterable" => Ok(S3VectorMetadataColumn::NonFilterable),
            _ => Err(format!("Invalid S3 vector metadata column type: {s}")),
        }
    }
}

/// Gets the columns that should be added as metadata to the S3 vector index.
fn s3_vector_metadata_columns(columns: &[Column], schema: &SchemaRef) -> MetadataColumns {
    let metadata_columns: Vec<MetadataColumn> = columns
        .iter()
        .filter_map(|c| {
            // Note: This needs to be documented. DOCS_REQUIRED
            c.metadata
                .get("vectors")
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<S3VectorMetadataColumn>().ok())
                })
                .and_then(|metadata_column_type| {
                    let Some(field) = schema.field_with_name(&c.name).ok() else {
                        tracing::warn!("Column '{}' not found in schema.", c.name);
                        return None;
                    };
                    Some(match metadata_column_type {
                        S3VectorMetadataColumn::Filterable => {
                            MetadataColumn::Filterable(Arc::new(field.clone()))
                        }
                        S3VectorMetadataColumn::NonFilterable => {
                            MetadataColumn::NonFilterable(Arc::new(field.clone()))
                        }
                    })
                })
        })
        .collect();
    metadata_columns.into()
}

#[derive(Debug, Clone)]
pub struct S3VectorIndex {
    pub table: S3VectorsTable,

    /// The name of the column in the associated [`TableProvider`] that produces the `data` column in [`S3VectorsTable`].
    pub embedded_column: String,

    /// The ordered fields that comprise the underlying unique `key` in [`S3VectorsTable`]
    pub primary_key: Vec<Field>,

    /// Additional columns to add as metadata to the S3 vector index from the original dataset columns.
    pub metadata_columns: MetadataColumns,
}

impl S3VectorIndex {
    #[must_use]
    pub fn primary_key_columns(&self) -> Vec<String> {
        self.primary_key.iter().map(|f| f.name().clone()).collect()
    }
}

impl Index for S3VectorIndex {
    fn name(&self) -> &'static str {
        "s3_vector_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut pks: Vec<_> = self
            .primary_key
            .iter()
            .map(arrow_schema::Field::name)
            .cloned()
            .collect();
        pks.push(self.embedded_column.clone());
        pks.extend(self.metadata_columns.iter().map(|c| c.name().to_string()));

        pks
    }
}
