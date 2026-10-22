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

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::prelude::Expr;
use qdrant::QdrantStore as _;
use search::generation::util::get_primary_keys;
use search::index::chunking::ChunkedSearchIndex;
use search::index::compound::CompoundVectorIndex;
pub(crate) use search::index::qdrant::QdrantIndex;
use search::metadata::{MetadataColumn, MetadataColumns};
use spice_table::{Index, LayerWalk};
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
pub async fn try_from_table<S: std::hash::BuildHasher>(
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
    collections_in_use: &HashMap<String, String, S>,
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

    if let Some(other_column) = collections_in_use.get(&collection) {
        return Err(shared_collection_error(
            ds_name,
            &collection,
            other_column,
            &column,
        ));
    }

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

    let partition = qdrant_partition(ds_name, &partition_by, &inner_schema)?;

    let metadata_columns = qdrant_metadata_columns(
        &dataset_columns,
        &inner_schema,
        &column,
        partition
            .as_ref()
            .map(|(field, _)| field)
            .filter(|field| !primary_key.iter().any(|pk| pk.name() == field.name())),
    );

    let metadata_fields: Vec<_> = metadata_columns.iter().map(MetadataColumn::field).collect();
    if let Some(field) = primary_key
        .iter()
        .chain(metadata_fields.iter().map(AsRef::as_ref))
        .find(|field| !qdrant::payload::is_storable(field.data_type()))
    {
        return Err(config_error(
            ds_name,
            &format!(
                "Column '{}' has type {}, which Qdrant cannot store as a payload value. Remove it from `row_id` or the vector metadata columns, or cast it to a supported type in a view.",
                field.name(),
                field.data_type()
            ),
        ));
    }

    if let Some(name) = primary_key
        .iter()
        .chain(metadata_fields.iter().map(AsRef::as_ref))
        .map(|field| field.name().as_str())
        .find(|name| qdrant::payload::RESERVED_KEYS.contains(name))
    {
        return Err(config_error(
            ds_name,
            &format!(
                "Column '{name}' uses a payload key Spice writes on every Qdrant point ({}). Rename the column in a view, or remove it from `row_id` or the vector metadata columns.",
                qdrant::payload::RESERVED_KEYS.join(", ")
            ),
        ));
    }

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

    // Every search filters on the dataset, so it is indexed. The generation is read only by the
    // one cleanup delete per full refresh, which the dataset index already narrows, so it is
    // left unindexed rather than paying for index upkeep on every upsert.
    let indexed_fields = std::iter::once((
        qdrant::payload::DATASET_KEY,
        qdrant::proto::FieldType::Keyword,
    ))
    .chain(
        partition
            .as_ref()
            .map(|(field, field_type)| (field.name().as_str(), *field_type)),
    );
    for (field_name, field_type) in indexed_fields {
        client
            .create_field_index(&collection, field_name, field_type)
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
        dataset: ds_name.to_string(),
        write_generation: Arc::default(),
    })
}

pub(crate) fn collections_in_use(provider: &Arc<dyn TableProvider>) -> HashMap<String, String> {
    fn qdrant_index(index: &dyn Index) -> Option<&QdrantIndex> {
        let any = index.as_any();
        if let Some(chunked) = any.downcast_ref::<ChunkedSearchIndex>() {
            return qdrant_index(chunked.inner().as_ref());
        }
        if let Some(compound) = any.downcast_ref::<CompoundVectorIndex>() {
            return qdrant_index(compound.secondary().as_ref());
        }
        any.downcast_ref::<QdrantIndex>()
    }

    spice_table::nodes(provider.as_ref(), LayerWalk::Index)
        .flat_map(|node| node.indexes().iter())
        .filter_map(|index| qdrant_index(index.as_ref()))
        .map(|index| (index.collection.clone(), index.embedded_column.clone()))
        .collect()
}

fn config_error(
    ds_name: &TableReference,
    detail: &str,
) -> Box<dyn std::error::Error + Send + Sync> {
    format!(
        "Failed to register dataset {ds_name} (qdrant): {detail} See: https://spiceai.org/docs/components/vectors"
    )
    .into()
}

fn shared_collection_error(
    ds_name: &TableReference,
    collection: &str,
    other_column: &str,
    column: &str,
) -> Box<dyn std::error::Error + Send + Sync> {
    config_error(
        ds_name,
        &format!(
            "Columns '{other_column}' and '{column}' would write to the same Qdrant collection '{collection}' and overwrite each other's vectors. Remove `qdrant_collection` so each column gets its own collection."
        ),
    )
}

fn qdrant_partition(
    ds_name: &TableReference,
    partition_by: &[Expr],
    schema: &Schema,
) -> Result<Option<(Field, qdrant::proto::FieldType)>, Box<dyn std::error::Error + Send + Sync>> {
    let [partition_expr] = partition_by else {
        if partition_by.is_empty() {
            return Ok(None);
        }
        return Err(config_error(
            ds_name,
            &format!(
                "`partition_by` supports one column, but {} expressions were provided.",
                partition_by.len()
            ),
        ));
    };
    let Expr::Column(column) = partition_expr else {
        return Err(config_error(
            ds_name,
            &format!("`partition_by` must be a column name, but found '{partition_expr}'."),
        ));
    };
    let Ok(field) = schema.field_with_name(&column.name) else {
        return Err(config_error(
            ds_name,
            &format!(
                "`partition_by` names column '{}', which the dataset does not have. Use one of the dataset's columns.",
                column.name
            ),
        ));
    };
    let field_type = match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            qdrant::proto::FieldType::Keyword
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => qdrant::proto::FieldType::Integer,
        DataType::Float32 | DataType::Float64 => qdrant::proto::FieldType::Float,
        DataType::Boolean => qdrant::proto::FieldType::Bool,
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            _,
        ) => qdrant::proto::FieldType::Datetime,
        other if qdrant::payload::is_string_encoded(other) => qdrant::proto::FieldType::Keyword,
        other => {
            return Err(config_error(
                ds_name,
                &format!(
                    "`partition_by` column '{}' has type {other}, which Qdrant cannot index. Use a string, number, boolean, decimal, date, time, or timestamp column.",
                    column.name
                ),
            ));
        }
    };
    Ok(Some((field.clone(), field_type)))
}

const DEFAULT_BATCH_WRITE_ROWS: usize = 1000;

fn qdrant_metadata_columns(
    columns: &[Column],
    schema: &SchemaRef,
    embedded_column: &str,
    partition_column: Option<&Field>,
) -> MetadataColumns {
    let mut cols: Vec<MetadataColumn> = columns
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
    if let Some(field) = partition_column
        && field.name() != embedded_column
        && !cols.iter().any(|c| c.name() == field.name())
    {
        cols.push(MetadataColumn::Filterable(Arc::new(field.clone())));
    }
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
    use super::{qdrant_metadata_columns, qdrant_partition, shared_collection_error};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::prelude::{Expr, col, lit};
    use qdrant::proto::FieldType;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, true),
            Field::new("year", DataType::Int32, true),
            Field::new("created_at", DataType::Date32, true),
            Field::new("blob", DataType::Binary, true),
        ])
    }

    fn partition(partition_by: &[Expr]) -> Result<Option<(String, FieldType)>, String> {
        qdrant_partition(&TableReference::bare("docs"), partition_by, &schema())
            .map(|p| p.map(|(field, field_type)| (field.name().clone(), field_type)))
            .map_err(|e| e.to_string())
    }

    #[test]
    fn qdrant_partition_resolves_an_indexable_column() {
        assert_eq!(partition(&[]), Ok(None));
        assert_eq!(
            partition(&[col("tenant_id")]),
            Ok(Some(("tenant_id".to_string(), FieldType::Keyword)))
        );
        assert_eq!(
            partition(&[col("year")]),
            Ok(Some(("year".to_string(), FieldType::Integer)))
        );
        assert_eq!(
            partition(&[col("year"), col("tenant_id")]),
            Err("Failed to register dataset docs (qdrant): `partition_by` supports one column, but 2 expressions were provided. See: https://spiceai.org/docs/components/vectors".to_string())
        );
        assert_eq!(
            partition(&[col("year") + lit(1)]),
            Err("Failed to register dataset docs (qdrant): `partition_by` must be a column name, but found 'year + Int32(1)'. See: https://spiceai.org/docs/components/vectors".to_string())
        );
        assert_eq!(
            partition(&[col("created_at")]),
            Ok(Some(("created_at".to_string(), FieldType::Datetime)))
        );
        assert_eq!(
            partition(&[col("tenant")]),
            Err("Failed to register dataset docs (qdrant): `partition_by` names column 'tenant', which the dataset does not have. Use one of the dataset's columns. See: https://spiceai.org/docs/components/vectors".to_string())
        );
        assert_eq!(
            partition(&[col("blob")]),
            Err("Failed to register dataset docs (qdrant): `partition_by` column 'blob' has type Binary, which Qdrant cannot index. Use a string, number, boolean, decimal, date, time, or timestamp column. See: https://spiceai.org/docs/components/vectors".to_string())
        );
    }

    #[test]
    fn partition_column_is_a_payload_column_once() {
        let schema = std::sync::Arc::new(schema());
        let tenant = schema
            .field_with_name("tenant_id")
            .expect("tenant_id")
            .clone();
        let declared = spicepod::semantic::Column::new("tenant_id").with_metadata(
            [(
                "vectors".to_string(),
                serde_json::Value::String("non-filterable".to_string()),
            )]
            .into(),
        );
        for columns in [vec![], vec![declared]] {
            assert_eq!(
                qdrant_metadata_columns(&columns, &schema, "body", Some(&tenant)).all_names(),
                vec!["tenant_id".to_string()]
            );
        }
    }

    #[test]
    fn shared_collection_error_names_both_columns_and_the_fix() {
        assert_eq!(
            shared_collection_error(&TableReference::bare("docs"), "shared", "title", "body")
                .to_string(),
            "Failed to register dataset docs (qdrant): Columns 'title' and 'body' would write to the same Qdrant collection 'shared' and overwrite each other's vectors. Remove `qdrant_collection` so each column gets its own collection. See: https://spiceai.org/docs/components/vectors"
        );
    }
}
