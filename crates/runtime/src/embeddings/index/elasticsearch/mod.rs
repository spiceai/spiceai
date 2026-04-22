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

//! Runtime integration for Elasticsearch as a vector engine.
//!
//! Constructs [`ElasticsearchIndex`] instances from dataset configuration
//! and wires them into the [`IndexedTableProvider`] pipeline.

use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use elasticsearch::{Client, Elasticsearch};
use search::generation::util::get_primary_keys;
pub(crate) use search::index::elasticsearch::ElasticsearchIndex;
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
use runtime_secrets::{Secrets, get_params_with_secrets};

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .description("Elasticsearch cluster URL (e.g., https://localhost:9200)."),
    ParameterSpec::component("user")
        .description("Username for Elasticsearch authentication.")
        .secret(),
    ParameterSpec::component("pass")
        .description("Password for Elasticsearch authentication.")
        .secret(),
    ParameterSpec::component("index").description("Elasticsearch index name for storing vectors."),
    ParameterSpec::component("vector_field").description(
        "Name of the dense_vector field in Elasticsearch. Defaults to the embedding column name.",
    ),
    ParameterSpec::component("distance_metric")
        .description(
            "Vector similarity metric for kNN search. One of: cosine | l2_norm | dot_product | max_inner_product.",
        )
        .one_of(&["cosine", "l2_norm", "dot_product", "max_inner_product"]),
    ParameterSpec::component("hnsw_m").description(
        "HNSW graph parameter m (links per node). Higher = better recall, more memory. ES default: 16.",
    ),
    ParameterSpec::component("hnsw_ef_construction").description(
        "HNSW graph build parameter ef_construction (candidate list size at build time). ES default: 100.",
    ),
    ParameterSpec::runtime("client_timeout").description(
        "Total request timeout for the Elasticsearch HTTP client, in time unit format (e.g. 30s, 1m). Default: 30s.",
    ),
    ParameterSpec::runtime("connect_timeout").description(
        "Connect timeout for the Elasticsearch HTTP client, in time unit format (e.g. 10s). Default: 10s.",
    ),
    ParameterSpec::component("max_retries").description(
        "Maximum number of retry attempts for transient Elasticsearch errors (HTTP 429 / 5xx). Default: 3.",
    ),
    ParameterSpec::component("retry_initial_backoff").description(
        "Initial backoff duration between retries, in time unit format (e.g. 100ms, 1s). Default: 200ms.",
    ),
    ParameterSpec::component("batch_write_rows").description(
        "Maximum number of rows to include in a single Elasticsearch _bulk request. Used to control memory usage and payload size during writes. Default: 1000.",
    ),
    ParameterSpec::component("partition_by")
        .description("Not yet supported for the Elasticsearch vector engine."),
    ParameterSpec::component("spill_writes")
        .description("Not yet supported for the Elasticsearch vector engine."),
];

/// Attempt to construct an [`ElasticsearchIndex`] for the provided dataset/view on the given column.
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
) -> Result<ElasticsearchIndex, Box<dyn std::error::Error + Send + Sync>> {
    let inner_schema = index_schema;
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

    // Normalize LargeUtf8 → Utf8 for primary key fields: the Elasticsearch HTTP client
    // always returns string data as Arrow Utf8 (StringArray), so the schema must match.
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

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;

    // Surface explicit "not yet supported" errors for params that require
    // significant new infrastructure (per-partition index routing / spill
    // queues). Better to fail loudly than silently ignore the config.
    if string_from_params(&params, "partition_by").is_some()
        || !vector_store_config.partition_by.is_empty()
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`partition_by` is not yet supported for the Elasticsearch vector engine. Remove the parameter or use the S3 Vectors engine for partitioned workloads.",
        ));
    }
    if string_from_params(&params, "spill_writes").is_some_and(|v| {
        matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes")
    }) {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`spill_writes` is not yet supported for the Elasticsearch vector engine.",
        ));
    }

    let endpoint = string_from_params(&params, "endpoint").ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from(
            "Missing required parameter 'endpoint' for Elasticsearch vector engine.",
        )
    })?;

    let user = string_from_params(&params, "user");
    let pass = string_from_params(&params, "pass");

    let client_options = build_client_options(&params)?;
    let client: Arc<dyn Elasticsearch> = Arc::new(
        Client::new_with_options(endpoint, user, pass, &client_options)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    );

    let es_index = string_from_params(&params, "index").map_or_else(
        || {
            format!("{}-{}-{}", ds_name, column, config.model)
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
        },
        ToString::to_string,
    );

    let vector_field = string_from_params(&params, "vector_field")
        .map_or_else(|| format!("{column}_embedding"), ToString::to_string);

    // Determine text fields for full-text search from the dataset columns.
    // Match both Utf8 and LargeUtf8 since accelerated tables may use LargeUtf8.
    let text_fields: Vec<String> = dataset_columns
        .iter()
        .filter(|c| {
            inner_schema
                .field_with_name(&c.name)
                .is_ok_and(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        })
        .map(|c| c.name.clone())
        .collect();

    // Get the embedding model to determine dimension.
    // Clone the Arc before dropping the read lock to avoid holding it across .await.
    let model = {
        let model_read = embedding_models.read().await;
        let Some(model) = model_read.get(&config.model) else {
            return Err(Box::from(format!(
                "Cannot create Elasticsearch vector index for table '{}'. No embedding model named: '{}'.",
                ds_name, config.model
            )));
        };
        Arc::clone(model)
    };

    let dims = llms::embeddings::get_or_infer_size(&model)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        as i32;

    // Normalize the source schema to match what the Elasticsearch HTTP client actually produces.
    // Accelerated tables (e.g. DuckDB) may store columns with types that differ from what ES
    // returns, causing RecordBatch::try_new to fail when schemas are compared:
    //   - LargeUtf8 → Utf8  (ES always returns StringArray / Utf8)
    //   - FixedSizeList inner field → named "item", non-null Float32
    //     (build_dense_vector_array always uses Field::new("item", Float32, false))
    // Also, if the base table does not have the vector field (common when ES is the vector store),
    // explicitly append it so knn_hits_to_batch can extract it from ES _source.
    let mut source_fields: Vec<arrow_schema::FieldRef> = inner_schema
        .fields()
        .iter()
        .map(|f| {
            let normalized_type = normalize_es_data_type(f.data_type());
            if &normalized_type == f.data_type() {
                Arc::clone(f)
            } else {
                Arc::new(arrow_schema::Field::new(
                    f.name(),
                    normalized_type,
                    f.is_nullable(),
                ))
            }
        })
        .collect();

    // Append vector_field if not already present in the base schema.
    if inner_schema.field_with_name(&vector_field).is_err() {
        source_fields.push(Arc::new(arrow_schema::Field::new(
            &vector_field,
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::Float32, false)),
                dims,
            ),
            true,
        )));
    }

    let source_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        source_fields,
        inner_schema.metadata().clone(),
    ));

    // Ensure the Elasticsearch index exists with the correct dense_vector mapping
    // for our vector field. This makes the ES vector engine "bring-your-own" friendly:
    // if the user has already created and populated the index, we leave it alone;
    // otherwise we create it so writes during refresh succeed.
    ensure_index_with_mapping(
        client.as_ref(),
        &es_index,
        &vector_field,
        dims,
        &text_fields,
    )
    .await?;

    Ok(ElasticsearchIndex {
        client,
        es_index,
        embedded_column: column,
        vector_field,
        text_fields,
        primary_key,
        compute_query: model,
        dims,
        source_schema,
    })
}

/// Create the ES index with a `dense_vector` mapping for `vector_field` if the index
/// does not already exist. If the index already exists, attempt to add or update the
/// mapping so the vector field is searchable via kNN. Mapping updates for existing
/// indices are best-effort: if `put_mapping` fails, the error is logged and the
/// function continues, which may leave the existing mapping unchanged.
async fn ensure_index_with_mapping(
    client: &dyn Elasticsearch,
    es_index: &str,
    vector_field: &str,
    dims: i32,
    text_fields: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if dims <= 0 {
        return Err(format!(
            "Failed to prepare Elasticsearch index '{es_index}': embedding dimension must be positive, got {dims}. Check that the embedding model reports a valid output dimension."
        )
        .into());
    }

    let mut properties = serde_json::Map::new();
    properties.insert(
        vector_field.to_string(),
        serde_json::json!({
            "type": "dense_vector",
            "dims": dims,
            "index": true,
            "similarity": "cosine",
        }),
    );
    for t in text_fields {
        // Skip the vector field if a text column happens to share the name.
        if t == vector_field {
            continue;
        }
        properties.insert(
            t.clone(),
            serde_json::json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } },
            }),
        );
    }

    let exists = client
        .index_exists(es_index)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    let mapping_body = serde_json::json!({ "properties": properties });

    if exists {
        if let Err(e) = client.put_mapping(es_index, &mapping_body).await {
            // If the field already exists with an incompatible mapping, ES returns 400.
            // Surface the error but don't panic — a user may have pre-created the index
            // with a specific mapping they want preserved; log and proceed.
            tracing::warn!(
                "Elasticsearch index '{es_index}' exists but mapping update failed (continuing; \
                existing mapping will be used): {e}"
            );
        }
        return Ok(());
    }

    let create_body = serde_json::json!({ "mappings": { "properties": properties } });
    match client.create_index(es_index, &create_body).await {
        Ok(_) => {
            tracing::info!(
                "Created Elasticsearch index '{es_index}' with dense_vector field '{vector_field}' (dims={dims})."
            );
            Ok(())
        }
        // TOCTOU: another runtime instance may have created the index between our
        // `index_exists` check and `create_index` call. Treat that as success and
        // best-effort apply the mapping update to match the `exists` branch above.
        Err(e) if is_index_already_exists_error(&e) => {
            tracing::info!(
                "Elasticsearch index '{es_index}' was created concurrently by another runtime instance; applying mapping updates."
            );
            if let Err(mapping_error) = client.put_mapping(es_index, &mapping_body).await {
                tracing::warn!(
                    "Elasticsearch index '{es_index}' exists after concurrent creation but mapping update failed (continuing; existing mapping will be used): {mapping_error}"
                );
            }
            Ok(())
        }
        Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
    }
}

/// Check whether an error from `create_index` indicates the index already exists
/// (e.g. because another runtime instance created it concurrently).
fn is_index_already_exists_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        let message = err.to_string();
        if message.contains("resource_already_exists_exception")
            || message.contains("already exists")
        {
            return true;
        }
        current = err.source();
    }
    false
}

/// Normalize an Arrow [`DataType`] to match what the Elasticsearch HTTP client produces.
///
/// - `LargeUtf8` → `Utf8`: ES always deserializes strings as `StringArray` (Utf8).
/// - `FixedSizeList` with any inner field → `FixedSizeList` with `Field::new("item", Float32, false)`:
///   `build_dense_vector_array` always produces this exact inner field.
fn normalize_es_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 => DataType::Utf8,
        DataType::FixedSizeList(_, dim) => DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new("item", DataType::Float32, false)),
            *dim,
        ),
        other => other.clone(),
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
        "Elasticsearch vector store",
        params_with_secrets.into_iter().collect(),
        "elasticsearch",
        Arc::clone(&secrets),
        PARAMETERS,
    )
    .await?;

    Ok(params)
}
