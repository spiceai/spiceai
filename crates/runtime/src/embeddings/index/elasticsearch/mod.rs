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
        .description("Elasticsearch cluster URL (e.g., https://localhost:9200).")
        .secret(),
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
    let primary_keys: Vec<String> = config
        .row_ids
        .clone()
        .unwrap_or_else(|| get_primary_keys(inner_table_provider).unwrap_or_default());

    let primary_key: Vec<_> = primary_keys
        .iter()
        .filter_map(|c| {
            let (_, f) = inner_schema.column_with_name(c.as_str())?;
            Some(f.clone())
        })
        .collect();

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
                .replace(|c: char| !c.is_ascii_alphanumeric() && c != '-', "-")
        },
        ToString::to_string,
    );

    let vector_field = string_from_params(&params, "vector_field")
        .map_or_else(|| format!("{column}_embedding"), ToString::to_string);

    // Determine text fields for full-text search from the dataset columns.
    let text_fields: Vec<String> = dataset_columns
        .iter()
        .filter(|c| {
            inner_schema
                .field_with_name(&c.name)
                .is_ok_and(|f| matches!(f.data_type(), arrow_schema::DataType::Utf8))
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

    Ok(ElasticsearchIndex {
        client,
        es_index,
        embedded_column: column,
        vector_field,
        text_fields,
        primary_key,
        compute_query: model,
        dims,
        source_schema: inner_schema,
    })
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
