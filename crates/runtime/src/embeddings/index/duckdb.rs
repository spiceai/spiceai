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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use data_components::poly::PolyTableProvider;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_table_providers::{
    duckdb::{TableDefinition, write::DuckDBTableWriter},
    sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
use runtime_datafusion_index::{Index, IndexedTableProvider};
use runtime_secrets::{Secrets, get_params_with_secrets};
use search::{generation::util::get_primary_keys, index::duckdb::DuckDBVectorIndex};
use spicepod::{
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig},
    vector::VectorStore,
};
use tokio::sync::RwLock;

use crate::{
    model::EmbeddingModelStore,
    parameters::{ParameterSpec, Parameters},
};

use search::index::duckdb::{DuckDBDistanceMetric, DuckDBHnswOptions};

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("distance_metric")
        .description(
            "Vector similarity metric for DuckDB VSS. One of: cosine | l2 | inner_product.",
        )
        .one_of(&["cosine", "l2", "inner_product"]),
    ParameterSpec::component("metric")
        .description("Alias for distance_metric. One of: cosine | l2 | inner_product."),
    ParameterSpec::component("hnsw_m")
        .description("DuckDB VSS HNSW graph parameter m (links per node)."),
    ParameterSpec::component("m").description("Alias for hnsw_m."),
    ParameterSpec::component("hnsw_ef_construction")
        .description("DuckDB VSS HNSW build parameter ef_construction."),
    ParameterSpec::component("ef_construction").description("Alias for hnsw_ef_construction."),
    ParameterSpec::component("hnsw_ef_search")
        .description("DuckDB VSS query-time ef_search setting."),
    ParameterSpec::component("ef_search").description("Alias for hnsw_ef_search."),
    ParameterSpec::component("index_name")
        .description("Optional DuckDB VSS index name to use for transient HNSW searches."),
    ParameterSpec::component("install_vss")
        .description("Whether Spice should run INSTALL vss before LOAD vss. Default: true."),
    ParameterSpec::component("partition_by")
        .description("Not yet supported for the DuckDB vector engine."),
    ParameterSpec::component("spill_writes")
        .description("Not supported for the DuckDB vector engine."),
];

#[expect(clippy::too_many_arguments)]
pub(crate) async fn try_from_table(
    ds_name: &TableReference,
    column: String,
    config: ColumnLevelEmbeddingConfig,
    vector_store_config: &VectorStore,
    inner_table_provider: &Arc<dyn TableProvider>,
    index_schema: SchemaRef,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<DuckDBVectorIndex, Box<dyn std::error::Error + Send + Sync>> {
    if config
        .chunking
        .as_ref()
        .is_some_and(|chunking| chunking.enabled)
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "Chunking is not yet supported for the DuckDB vector engine.",
        ));
    }

    let primary_keys: Vec<String> = match config.row_ids.clone() {
        Some(row_ids) => row_ids,
        None => get_primary_keys(inner_table_provider)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    };

    if primary_keys.is_empty() {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "Failed to resolve primary key columns for dataset {ds_name}: no primary key columns were configured or derived."
        )));
    }

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;
    if string_from_params(&params, "partition_by").is_some()
        || !vector_store_config.partition_by.is_empty()
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`partition_by` is not yet supported for the DuckDB vector engine.",
        ));
    }
    if string_from_params(&params, "spill_writes")
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`spill_writes` is not supported for the DuckDB vector engine.",
        ));
    }

    let model = {
        let model_read = embedding_models.read().await;
        let Some(model) = model_read.get(&config.model) else {
            return Err(Box::from(format!(
                "Cannot create DuckDB vector index for table '{}'. No embedding model named: '{}'.",
                ds_name, config.model
            )));
        };
        Arc::clone(model)
    };

    let dims = llms::embeddings::get_or_infer_size(&model)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        as i32;

    let source_schema = schema_with_embedding_column(index_schema, &column, dims);
    let primary_key: Vec<_> = primary_keys
        .iter()
        .map(|pk| {
            source_schema
                .field_with_name(pk)
                .cloned()
                .map_err(|_| {
                    Box::<dyn std::error::Error + Send + Sync>::from(format!(
                        "Failed to configure primary key for dataset {ds_name}: column '{pk}' does not exist in the dataset schema."
                    ))
                })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

    let hnsw = parse_hnsw_options(&params)?;

    Ok(DuckDBVectorIndex::new(
        column,
        primary_key,
        model,
        dims,
        source_schema,
        hnsw,
    ))
}

pub(crate) async fn wrap_accelerator_with_duckdb_vector_indexes(
    tbl: &TableReference,
    columns: &[Column],
    vector_store: &VectorStore,
    accelerator_provider: Arc<dyn TableProvider>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let Some((pool, table_definition)) = duckdb_writer_context(&accelerator_provider) else {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "DuckDB vector engine for table {tbl} requires a DuckDB accelerator provider."
        )));
    };

    let embedding_columns: Vec<_> = columns
        .iter()
        .filter_map(|c| {
            c.embeddings
                .first()
                .map(|embed| (c.name.clone(), embed.clone()))
        })
        .collect();

    let mut provider = if let Some(indexed) = accelerator_provider
        .as_any()
        .downcast_ref::<IndexedTableProvider>()
    {
        indexed.clone()
    } else {
        IndexedTableProvider::new(Arc::clone(&accelerator_provider))
    };

    for (column, config) in embedding_columns {
        let vector_index = try_from_table(
            tbl,
            column,
            config,
            vector_store,
            &accelerator_provider,
            accelerator_provider.schema(),
            Arc::clone(&embedding_models),
            Arc::clone(&secrets),
        )
        .await?
        .with_query_context(Arc::clone(&pool), Arc::clone(&table_definition));

        provider = provider.add_index(Arc::new(vector_index) as Arc<dyn Index>);
    }

    Ok(Arc::new(provider))
}

fn duckdb_writer_context(
    provider: &Arc<dyn TableProvider>,
) -> Option<(Arc<DuckDbConnectionPool>, Arc<TableDefinition>)> {
    if let Some(indexed) = provider.as_any().downcast_ref::<IndexedTableProvider>() {
        return duckdb_writer_context(&indexed.underlying);
    }

    if let Some(poly) = provider.as_any().downcast_ref::<PolyTableProvider>() {
        let writer = poly.writer();
        return duckdb_writer_context(&writer);
    }

    provider
        .as_any()
        .downcast_ref::<DuckDBTableWriter>()
        .map(|writer| (writer.pool(), writer.table_definition()))
}

fn schema_with_embedding_column(schema: SchemaRef, column: &str, dims: i32) -> SchemaRef {
    let embedding_column_name = format!("{column}_embedding");
    if schema.column_with_name(&embedding_column_name).is_some() {
        return schema;
    }

    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        &embedding_column_name,
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), dims),
        true,
    )));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn parse_hnsw_options(
    params: &Parameters,
) -> Result<DuckDBHnswOptions, Box<dyn std::error::Error + Send + Sync>> {
    let mut options = DuckDBHnswOptions::default();

    if let Some(metric) = string_from_params(params, "distance_metric")
        .or_else(|| string_from_params(params, "metric"))
    {
        options.metric = DuckDBDistanceMetric::try_from(metric)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
    }

    options.hnsw_m = parse_u32_param(params, "hnsw_m")?.or(parse_u32_param(params, "m")?);
    options.hnsw_ef_construction = parse_u32_param(params, "hnsw_ef_construction")?
        .or(parse_u32_param(params, "ef_construction")?);
    options.hnsw_ef_search =
        parse_u32_param(params, "hnsw_ef_search")?.or(parse_u32_param(params, "ef_search")?);
    options.index_name = string_from_params(params, "index_name").map(ToString::to_string);
    if let Some(install_vss) = string_from_params(params, "install_vss") {
        options.install_vss = parse_bool_param("install_vss", install_vss)?;
    }

    Ok(options)
}

fn parse_u32_param(
    params: &Parameters,
    key: &str,
) -> Result<Option<u32>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(s) = string_from_params(params, key) else {
        return Ok(None);
    };
    s.parse::<u32>().map(Some).map_err(|e| {
        format!("Invalid value for DuckDB vector parameter '{key}': '{s}': {e}").into()
    })
}

fn parse_bool_param(
    key: &str,
    value: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!(
            "Invalid value for DuckDB vector parameter '{key}': '{value}'. Expected true or false."
        )
        .into()),
    }
}

fn string_from_params<'a>(p: &'a Parameters, key: &str) -> Option<&'a str> {
    p.get(key).expose().ok()
}

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
        "DuckDB vector store",
        params_with_secrets.into_iter().collect(),
        "duckdb",
        Arc::clone(&secrets),
        PARAMETERS,
    )
    .await?;

    Ok(params)
}
