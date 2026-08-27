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

use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use qdrant::QdrantStore as _;
use search::generation::util::get_primary_keys;
use search::index::chunking::ChunkedSearchIndex;
pub(crate) use search::index::qdrant::QdrantIndex;
use search::metadata::{MetadataColumn, MetadataColumns};
use spicepod::{
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig, MetadataType},
    vector::VectorStore,
};
use tokio::sync::RwLock;

use crate::model::EmbeddingModelStore;
use runtime_parameters_typed::TypedParams as _;
use runtime_search::store_params::qdrant::{
    QdrantDistanceMetric, QdrantVectorParams, client_from_params,
};
use runtime_secrets::{Secrets, get_params_with_secrets};

#[expect(clippy::too_many_arguments)]
pub async fn try_from_table(
    ds_name: &TableReference,
    column: String,
    config: ColumnLevelEmbeddingConfig,
    vector_store_config: &VectorStore,
    inner_table_provider: &Arc<dyn TableProvider>,
    index_schema: SchemaRef,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    dataset_columns: Vec<Column>,
    secrets: Arc<RwLock<Secrets>>,
    partition_by: Vec<Expr>,
) -> Result<QdrantIndex, Box<dyn std::error::Error + Send + Sync>> {
    let inner_schema = index_schema;
    let primary_key = derive_primary_keys(&inner_schema, inner_table_provider, ds_name, &config)?;

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;

    let client = client_from_params(&params)?;

    let collection = params.collection.clone().unwrap_or_else(|| {
        format!("{ds_name}-{column}-{}", config.model)
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    });

    let model = {
        let model_read = embedding_models.read().await;
        let Some(model) = model_read.get(&config.model) else {
            return Err(Box::from(format!(
                "Cannot create Qdrant vector index for table '{ds_name}'. No embedding model named: '{}'.",
                config.model
            )));
        };
        Arc::clone(model)
    };

    let dims = llms::embeddings::get_or_infer_size(&model)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        as i32;

    let batch_write_rows = if params.batch_write_rows > 0 {
        params.batch_write_rows
    } else {
        DEFAULT_BATCH_WRITE_ROWS
    };

    let metadata_columns = qdrant_metadata_columns(&dataset_columns, &inner_schema, &column);

    let (partition_key, partition_column) = qdrant_partition(&partition_by)?;

    ensure_collection(
        client.as_ref(),
        &collection,
        dims,
        params
            .distance_metric
            .unwrap_or(QdrantDistanceMetric::Cosine)
            .distance(),
    )
    .await?;

    if let Some(partition_key) = &partition_key {
        client
            .create_field_index(
                &collection,
                partition_key,
                qdrant::proto::FieldType::Keyword,
            )
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    }

    Ok(QdrantIndex {
        client,
        collection,
        embedded_column: column,
        primary_key,
        compute_query: model,
        dims,
        distance_metric: params
            .distance_metric
            .unwrap_or(QdrantDistanceMetric::Cosine)
            .as_str()
            .to_string(),
        metadata_columns,
        batch_write_rows,
        partition_key: partition_key.clone(),
        partition_column,
    })
}

fn qdrant_partition(
    partition_by: &[Expr],
) -> Result<(Option<String>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    match partition_by.first() {
        None => Ok((None, None)),
        Some(partition_expr) => {
            if partition_by.len() > 1 {
                return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Qdrant partition_by supports at most one expression, but {} were provided",
                    partition_by.len()
                )));
            }
            let name = expr_column_name(partition_expr).ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    "Qdrant partition_by must reference a single column".into()
                },
            )?;
            Ok((Some(format!("spice_partition_{name}")), Some(name)))
        }
    }
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Alias(alias) => Some(alias.name.clone()),
        _ => None,
    }
}

const DEFAULT_BATCH_WRITE_ROWS: usize = 1000;

fn qdrant_metadata_columns(
    columns: &[Column],
    schema: &SchemaRef,
    embedded_column: &str,
) -> MetadataColumns {
    let cols: Vec<MetadataColumn> = columns
        .iter()
        .filter(|c| c.name != embedded_column)
        .filter_map(|c| {
            let kind = c.as_vector_metadata()?;
            let field = schema.field_with_name(&c.name).ok()?.clone();
            Some(match kind {
                MetadataType::Filterable => MetadataColumn::Filterable(Arc::new(field)),
                MetadataType::NonFilterable => MetadataColumn::NonFilterable(Arc::new(field)),
            })
        })
        .collect();
    cols.into()
}

#[expect(clippy::cast_sign_loss)]
async fn ensure_collection(
    client: &dyn qdrant::QdrantStore,
    collection: &str,
    dims: i32,
    distance: qdrant::proto::Distance,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if dims <= 0 {
        return Err(format!(
            "Failed to prepare Qdrant collection '{collection}': embedding dimension must be positive, got {dims}. Check the embedding model's output dimension."
        )
        .into());
    }

    client
        .ensure_collection(collection, dims as u64, distance)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
}

async fn get_store_params(
    vector_store_config: &VectorStore,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<QdrantVectorParams, Box<dyn std::error::Error + Send + Sync>> {
    let params = vector_store_config
        .params
        .as_ref()
        .map(Params::as_string_map)
        .unwrap_or_default();

    if params.iter().any(|(k, _)| k == "qdrant_spill_writes") {
        return Err(
            "`qdrant_spill_writes` is not supported for the Qdrant vector engine. Remove the parameter."
                .into(),
        );
    }

    let params_with_secrets = get_params_with_secrets(Arc::clone(&secrets), &params).await;

    Ok(
        QdrantVectorParams::try_from_params("Qdrant vector store", params_with_secrets, &secrets)
            .await?,
    )
}

fn derive_primary_keys(
    inner_schema: &Schema,
    inner_table_provider: &Arc<dyn TableProvider>,
    ds_name: &TableReference,
    config: &ColumnLevelEmbeddingConfig,
) -> Result<Vec<arrow_schema::Field>, Box<dyn std::error::Error + Send + Sync>> {
    let primary_keys: Vec<String> = match &config.row_ids {
        Some(row_ids) => row_ids.clone(),
        None => get_primary_keys(inner_table_provider)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    };

    if primary_keys.is_empty() {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "Failed to resolve primary key columns for dataset {ds_name}: no primary key columns were configured or derived."
        )));
    }

    let primary_key: Vec<_> = primary_keys
        .iter()
        .map(|c| {
            let (_, f) = inner_schema.column_with_name(c.as_str()).ok_or_else(|| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Failed to configure primary key for dataset {ds_name}: column '{c}' does not exist in the dataset schema."
                ))
            })?;
            if f.data_type() == &DataType::LargeUtf8 {
                Ok(arrow_schema::Field::new(
                    f.name(),
                    DataType::Utf8,
                    f.is_nullable(),
                ))
            } else {
                Ok(f.clone())
            }
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

    let chunked = config.chunking.as_ref().is_some_and(|c| c.enabled);
    if chunked {
        Ok(ChunkedSearchIndex::augment_primary_key(primary_key))
    } else {
        Ok(primary_key)
    }
}

#[cfg(test)]
mod tests {
    use super::qdrant_partition;
    use datafusion::prelude::col;

    #[test]
    fn qdrant_partition_behavior() {
        let (key, column) = qdrant_partition(&[col("tenant_id")]).expect("partition");
        assert_eq!(key.as_deref(), Some("spice_partition_tenant_id"));
        assert_eq!(column.as_deref(), Some("tenant_id"));

        let (key, column) = qdrant_partition(&[]).expect("partition");
        assert!(key.is_none());
        assert!(column.is_none());

        assert!(qdrant_partition(&[col("a"), col("b")]).is_err());
    }
}
