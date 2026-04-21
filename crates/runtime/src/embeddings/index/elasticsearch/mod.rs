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

use arrow_schema::DataType;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use elasticsearch::{Client, Elasticsearch};
use search::generation::util::get_primary_keys;
use search::index::elasticsearch::ElasticsearchIndex;
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
];

/// Attempt to construct an [`ElasticsearchIndex`] for the provided dataset/view on the given column.
#[expect(clippy::too_many_arguments)]
pub async fn try_from_table(
    ds_name: &TableReference,
    column: String,
    config: ColumnLevelEmbeddingConfig,
    vector_store_config: &VectorStore,
    inner_table_provider: &Arc<dyn TableProvider>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    dataset_columns: Vec<Column>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<ElasticsearchIndex, Box<dyn std::error::Error + Send + Sync>> {
    let inner_schema = inner_table_provider.schema();
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

    let endpoint = string_from_params(&params, "endpoint").ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from(
            "Missing required parameter 'endpoint' for Elasticsearch vector engine.",
        )
    })?;

    let user = string_from_params(&params, "user");
    let pass = string_from_params(&params, "pass");

    let client: Arc<dyn Elasticsearch> = Arc::new(
        Client::new(endpoint, user, pass)
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
/// does not already exist. If the index exists, add/update the mapping so the vector
/// field is searchable via kNN. This is idempotent.
async fn ensure_index_with_mapping(
    client: &dyn Elasticsearch,
    es_index: &str,
    vector_field: &str,
    dims: i32,
    text_fields: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let exists = client.index_exists(es_index).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) },
    )?;

    if exists {
        let body = serde_json::json!({ "properties": properties });
        if let Err(e) = client.put_mapping(es_index, &body).await {
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

    let body = serde_json::json!({ "mappings": { "properties": properties } });
    client.create_index(es_index, &body).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) },
    )?;
    tracing::info!(
        "Created Elasticsearch index '{es_index}' with dense_vector field '{vector_field}' (dims={dims})."
    );
    Ok(())
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
