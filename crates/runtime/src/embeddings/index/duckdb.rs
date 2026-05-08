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

use std::{collections::HashMap, sync::Arc};

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
use snafu::prelude::*;
use spicepod::{param::Params, semantic::ColumnLevelEmbeddingConfig, vector::VectorStore};
use tokio::sync::RwLock;

use crate::{
    model::EmbeddingModelStore,
    parameters::{ParameterSpec, Parameters},
};

use search::index::duckdb::{DuckDBDistanceMetric, DuckDBHnswOptions};

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunking is not yet supported for the DuckDB vector engine."))]
    ChunkingNotSupported,

    #[snafu(display("Failed to resolve primary key columns for dataset {dataset}: {source}"))]
    GetPrimaryKeys {
        dataset: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to resolve primary key columns for dataset {dataset}: no primary key columns were configured or derived."
    ))]
    NoPrimaryKeys { dataset: String },

    #[snafu(display("Failed to load vector store parameters: {source}"))]
    GetStoreParams {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("`partition_by` is not yet supported for the DuckDB vector engine."))]
    PartitionByNotSupported,

    #[snafu(display("`spill_writes` is not supported for the DuckDB vector engine."))]
    SpillWritesNotSupported,

    #[snafu(display(
        "Cannot create DuckDB vector index for table '{table}'. No embedding model named: '{model}'."
    ))]
    EmbeddingModelNotFound { table: String, model: String },

    #[snafu(display("Failed to determine embedding dimensions: {source}"))]
    GetEmbeddingDimensions {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to configure primary key for dataset {dataset}: column '{column}' does not exist in the dataset schema."
    ))]
    PrimaryKeyColumnMissing { dataset: String, column: String },

    #[snafu(display("Invalid distance metric '{metric}': {reason}"))]
    InvalidDistanceMetric { metric: String, reason: String },

    #[snafu(display("Invalid value for DuckDB vector parameter '{key}': '{value}': {source}"))]
    InvalidParameter {
        key: String,
        value: String,
        source: std::num::ParseIntError,
    },
}

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
    ParameterSpec::component("hnsw_ef_construction")
        .description("DuckDB VSS HNSW build parameter ef_construction."),
    ParameterSpec::component("hnsw_ef_search")
        .description("DuckDB VSS query-time ef_search setting."),
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
) -> Result<DuckDBVectorIndex> {
    if config
        .chunking
        .as_ref()
        .is_some_and(|chunking| chunking.enabled)
    {
        return ChunkingNotSupportedSnafu.fail();
    }

    let primary_keys: Vec<String> = match config.row_ids.clone() {
        Some(row_ids) => row_ids,
        None => get_primary_keys(inner_table_provider)
            .boxed()
            .context(GetPrimaryKeysSnafu {
                dataset: ds_name.to_string(),
            })?,
    };

    if primary_keys.is_empty() {
        return NoPrimaryKeysSnafu {
            dataset: ds_name.to_string(),
        }
        .fail();
    }

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;
    if partition_by_configured(&params, vector_store_config) {
        return PartitionByNotSupportedSnafu.fail();
    }
    if spill_writes_enabled(&params) {
        return SpillWritesNotSupportedSnafu.fail();
    }

    let model = {
        let model_read = embedding_models.read().await;
        let Some(model) = model_read.get(&config.model) else {
            return EmbeddingModelNotFoundSnafu {
                table: ds_name.to_string(),
                model: config.model.clone(),
            }
            .fail();
        };
        Arc::clone(model)
    };

    let dims = llms::embeddings::get_or_infer_size(&model)
        .await
        .boxed()
        .context(GetEmbeddingDimensionsSnafu)? as i32;

    let source_schema = schema_with_embedding_column(index_schema, &column, dims);
    let primary_key: Vec<_> = primary_keys
        .iter()
        .map(|pk| {
            source_schema
                .field_with_name(pk)
                .cloned()
                .map_err(|_| Error::PrimaryKeyColumnMissing {
                    dataset: ds_name.to_string(),
                    column: pk.clone(),
                })
        })
        .collect::<Result<Vec<_>>>()?;

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
    embedding_columns: Vec<(String, ColumnLevelEmbeddingConfig)>,
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

    // Exclude Spice-managed HNSW indexes from the DuckDB writer's index drift check.
    // These indexes are created externally (after each refresh) and are not registered
    // in the TableDefinition configuration.
    table_definition.add_ignored_index_prefix("__spice_vss_");

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

        // For CDC/append datasets the HNSW index is created here at init time so it exists
        // before any CDC writes arrive. DuckDB VSS then auto-maintains it on each insert.
        // For full-refresh (overwrite) datasets this may be a no-op (the table may be empty
        // or not yet exist); the index is (re)created after each refresh via `on_write_complete`.
        if let Err(e) = vector_index.on_write_complete().await {
            tracing::debug!(
                table = %tbl,
                column = %vector_index.embedded_column,
                "HNSW index not created at init time: {e}. Will be created after the first refresh."
            );
        }

        provider = provider.add_index(Arc::new(vector_index) as Arc<dyn Index>);
    }

    Ok(Arc::new(provider))
}

#[must_use]
pub(crate) fn vector_store_from_embedding_params(
    params: &HashMap<String, String>,
) -> Option<VectorStore> {
    let hnsw_params = params
        .iter()
        .filter_map(|(key, value)| {
            normalized_duckdb_vector_param_name(key)
                .map(|normalized| (format!("duckdb_{normalized}"), value.clone()))
        })
        .collect::<HashMap<_, _>>();

    if hnsw_params.is_empty() {
        return None;
    }

    Some(VectorStore {
        enabled: true,
        engine: Some("duckdb".to_string()),
        partition_by: Vec::new(),
        params: Some(Params::from_string_map(hnsw_params)),
    })
}

fn normalized_duckdb_vector_param_name(key: &str) -> Option<&'static str> {
    match key.strip_prefix("duckdb_").unwrap_or(key) {
        "distance_metric" => Some("distance_metric"),
        "metric" => Some("metric"),
        "hnsw_m" => Some("hnsw_m"),
        "hnsw_ef_construction" => Some("hnsw_ef_construction"),
        "hnsw_ef_search" => Some("hnsw_ef_search"),
        "partition_by" => Some("partition_by"),
        "spill_writes" => Some("spill_writes"),
        _ => None,
    }
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

fn parse_hnsw_options(params: &Parameters) -> Result<DuckDBHnswOptions> {
    let mut options = DuckDBHnswOptions::default();

    if let Some(metric) = string_from_params(params, "distance_metric")
        .or_else(|| string_from_params(params, "metric"))
    {
        options.metric = DuckDBDistanceMetric::try_from(metric).map_err(|reason| {
            Error::InvalidDistanceMetric {
                metric: metric.to_string(),
                reason,
            }
        })?;
    }

    options.hnsw_m = parse_u32_param(params, "hnsw_m")?;
    options.hnsw_ef_construction = parse_u32_param(params, "hnsw_ef_construction")?;
    options.hnsw_ef_search = parse_u32_param(params, "hnsw_ef_search")?;

    Ok(options)
}

fn partition_by_configured(params: &Parameters, vector_store_config: &VectorStore) -> bool {
    string_from_params(params, "partition_by").is_some()
        || !vector_store_config.partition_by.is_empty()
}

fn spill_writes_enabled(params: &Parameters) -> bool {
    string_from_params(params, "spill_writes")
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
}

fn parse_u32_param(params: &Parameters, key: &str) -> Result<Option<u32>> {
    let Some(s) = string_from_params(params, key) else {
        return Ok(None);
    };
    s.parse::<u32>()
        .map(Some)
        .map_err(|source| Error::InvalidParameter {
            key: key.to_string(),
            value: s.to_string(),
            source,
        })
}

fn string_from_params<'a>(p: &'a Parameters, key: &str) -> Option<&'a str> {
    p.get(key).expose().ok()
}

async fn get_store_params(
    vector_store_config: &VectorStore,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Parameters> {
    let params = vector_store_config
        .params
        .as_ref()
        .map(Params::as_string_map)
        .unwrap_or_default();

    let params_with_secrets = get_params_with_secrets(Arc::clone(&secrets), &params).await;

    Parameters::try_new(
        "DuckDB vector store",
        params_with_secrets.into_iter().collect(),
        "duckdb",
        Arc::clone(&secrets),
        PARAMETERS,
    )
    .await
    .context(GetStoreParamsSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    async fn duckdb_params(values: &[(&str, &str)]) -> Parameters {
        Parameters::try_new(
            "DuckDB vector store",
            values
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
                .collect(),
            "duckdb",
            Arc::new(RwLock::new(Secrets::default())),
            PARAMETERS,
        )
        .await
        .expect("DuckDB vector parameters should be valid")
    }

    #[tokio::test]
    async fn parse_hnsw_options_accepts_aliases() {
        let params = duckdb_params(&[
            ("duckdb_metric", "ip"),
            ("duckdb_hnsw_m", "16"),
            ("duckdb_hnsw_ef_construction", "64"),
            ("duckdb_hnsw_ef_search", "20"),
        ])
        .await;

        let options = parse_hnsw_options(&params).expect("HNSW options should parse");

        assert_eq!(options.metric, DuckDBDistanceMetric::InnerProduct);
        assert_eq!(options.hnsw_m, Some(16));
        assert_eq!(options.hnsw_ef_construction, Some(64));
        assert_eq!(options.hnsw_ef_search, Some(20));
    }

    #[tokio::test]
    async fn parse_hnsw_options_prefers_canonical_names() {
        let params = duckdb_params(&[
            ("duckdb_distance_metric", "l2"),
            ("duckdb_metric", "inner_product"),
            ("duckdb_hnsw_m", "32"),
            ("duckdb_hnsw_ef_construction", "128"),
            ("duckdb_hnsw_ef_search", "40"),
        ])
        .await;

        let options = parse_hnsw_options(&params).expect("HNSW options should parse");

        assert_eq!(options.metric, DuckDBDistanceMetric::L2);
        assert_eq!(options.hnsw_m, Some(32));
        assert_eq!(options.hnsw_ef_construction, Some(128));
        assert_eq!(options.hnsw_ef_search, Some(40));
    }

    #[tokio::test]
    async fn parse_hnsw_options_rejects_invalid_numeric_values() {
        let params = duckdb_params(&[("duckdb_hnsw_m", "large")]).await;
        let err = parse_hnsw_options(&params).expect_err("invalid hnsw_m should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for DuckDB vector parameter 'hnsw_m'")
        );
    }

    #[tokio::test]
    async fn unsupported_options_are_detected() {
        let params = duckdb_params(&[
            ("duckdb_partition_by", "bucket(10, id)"),
            ("duckdb_spill_writes", "yes"),
        ])
        .await;

        assert!(partition_by_configured(&params, &VectorStore::default()));
        assert!(spill_writes_enabled(&params));
    }

    #[test]
    fn vector_store_from_embedding_params_keeps_hnsw_params() {
        let store = vector_store_from_embedding_params(&HashMap::from([
            ("duckdb_hnsw_m".to_string(), "32".to_string()),
            ("hnsw_ef_search".to_string(), "20".to_string()),
            ("duckdb_file".to_string(), "data.duckdb".to_string()),
        ]))
        .expect("HNSW params should create a DuckDB vector store config");

        assert_eq!(store.engine.as_deref(), Some("duckdb"));
        let params = store
            .params
            .expect("DuckDB vector store config should include params")
            .as_string_map();
        assert_eq!(params.get("duckdb_hnsw_m").map(String::as_str), Some("32"));
        assert_eq!(
            params.get("duckdb_hnsw_ef_search").map(String::as_str),
            Some("20")
        );
        assert!(!params.contains_key("duckdb_file"));
    }

    #[test]
    fn vector_store_from_embedding_params_ignores_unrelated_params() {
        assert!(
            vector_store_from_embedding_params(&HashMap::from([(
                "duckdb_file".to_string(),
                "data.duckdb".to_string()
            )]))
            .is_none()
        );
    }
}
