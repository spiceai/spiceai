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
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use runtime_datafusion_index::{Index, IndexedTableProvider};
use snafu::ResultExt;
use spicepod::semantic::{Column, IndexStore, MetadataType};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::component::column::full_text_search_config;
use crate::component::dataset::FullTextSearchDatasetConfig;
use crate::make_spice_data_sub_directory;

use search::generation::text_search::index::FullTextDatabaseIndex;

/// Adds a [`FullTextDatabaseIndex`] to a [`TableProvider`].
///
/// Expects at least one [`Column`] to have a full text search column configured.
pub(crate) fn add_full_text_search_to_table(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[Column],
    tbl: &TableReference,
) -> Result<IndexedTableProvider, Box<dyn std::error::Error + Send + Sync>> {
    let schema = inner_table_provider.schema();
    for c in columns {
        if schema.column_with_name(&c.name).is_none() {
            tracing::warn!(
                "The table {} is configured with column {} in the spicepod, but the column is not in the table's schema",
                tbl.to_string(),
                c.name
            );
        }
    }
    let Some(FullTextSearchDatasetConfig {
        index_store,
        index_path,
        search_fields,
        primary_key,
    }) = full_text_search_config(columns, tbl)
    else {
        return Err(Box::from(format!(
            "Attempted to add full text search functionality to '{tbl}', but configuration not available"
        )));
    };

    let directory = if index_store == IndexStore::File {
        if let Some(path) = index_path {
            Some(PathBuf::from_str(path.as_str()).boxed()?)
        } else {
            // Default case. Example `.spice/data/fts/catalog/schema/table/`.
            Some(
                make_spice_data_sub_directory(
                    [vec!["fts".to_string()], tbl.to_vec()].concat().as_slice(),
                )
                .boxed()?,
            )
        }
    } else {
        None
    };

    let store_fields = columns
        .iter()
        .filter_map(|c| {
            if c.as_vector_metadata() == Some(MetadataType::NonFilterable) {
                return Some(c.name.clone());
            }
            None
        })
        .collect::<Vec<_>>();

    let index = FullTextDatabaseIndex::try_new(
        Arc::clone(&inner_table_provider),
        search_fields,
        Some(primary_key),
        directory,
        &store_fields,
    )
    .boxed()?;

    let tbl: IndexedTableProvider = if let Some(idx_tbl) = inner_table_provider
        .as_any()
        .downcast_ref::<IndexedTableProvider>()
    {
        idx_tbl.clone()
    } else {
        IndexedTableProvider::new(inner_table_provider)
    };

    Ok(tbl.add_index(Arc::new(index) as Arc<dyn Index + Send + Sync>))
}

/// Adds an [`ElasticsearchTextIndex`] to a [`TableProvider`] for each FTS-enabled column.
///
/// Unlike the Tantivy path, this function builds a live Elasticsearch client and
/// creates one index per search field. Elasticsearch is queried live at search time.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn add_elasticsearch_fts_to_table(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &crate::search::full_text::elasticsearch::ElasticsearchFtsParams,
) -> Result<IndexedTableProvider, Box<dyn std::error::Error + Send + Sync>> {
    use crate::component::column::full_text_search_config;
    use crate::component::dataset::FullTextSearchDatasetConfig;
    use crate::embeddings::index::elasticsearch::{
        ensure_index_with_text_mapping, get_fts_client, normalize_es_data_type,
    };
    use arrow_schema::{DataType, Field};
    use search::index::elasticsearch::ElasticsearchTextIndex;

    let Some(FullTextSearchDatasetConfig {
        search_fields,
        primary_key,
        ..
    }) = full_text_search_config(columns, tbl)
    else {
        return Err(Box::from(format!(
            "Attempted to add Elasticsearch FTS to '{tbl}', but no FTS column configuration found"
        )));
    };

    let client = get_fts_client(&fts_params.params)?;

    // Normalize LargeUtf8 → Utf8 in the source schema (ES always returns Utf8).
    let raw_schema = inner_table_provider.schema();
    let normalized_fields: Vec<Arc<arrow_schema::Field>> = raw_schema
        .fields()
        .iter()
        .map(|f| {
            let normalized = normalize_es_data_type(f.data_type());
            if &normalized == f.data_type() {
                Arc::clone(f)
            } else {
                Arc::new(Field::new(f.name(), normalized, f.is_nullable()))
            }
        })
        .collect();
    let source_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        normalized_fields,
        raw_schema.metadata().clone(),
    ));

    // Resolve primary key fields from schema, normalizing types.
    let pk_fields: Vec<Field> = primary_key
        .iter()
        .map(|name| {
            raw_schema
                .field_with_name(name)
                .map(|f| {
                    let dt = normalize_es_data_type(f.data_type());
                    Field::new(f.name(), dt, f.is_nullable())
                })
                .unwrap_or_else(|_| Field::new(name, DataType::Utf8, true))
        })
        .collect();

    // Ensure the ES index exists with text mappings for all search fields.
    ensure_index_with_text_mapping(client.as_ref(), &fts_params.es_index, &search_fields).await?;

    let mut provider: IndexedTableProvider = if let Some(idx_tbl) = inner_table_provider
        .as_any()
        .downcast_ref::<IndexedTableProvider>(
    ) {
        idx_tbl.clone()
    } else {
        IndexedTableProvider::new(Arc::clone(&inner_table_provider))
    };

    // Create one ElasticsearchTextIndex per search field.
    for search_field in &search_fields {
        let index = Arc::new(ElasticsearchTextIndex {
            client: Arc::clone(&client),
            es_index: fts_params.es_index.clone(),
            search_column_name: search_field.clone(),
            search_fields: vec![search_field.clone()],
            primary_key: pk_fields.clone(),
            source_schema: Arc::clone(&source_schema),
            batch_write_rows: 1000,
        });
        provider = provider.add_index(index as Arc<dyn Index + Send + Sync>);
    }

    Ok(provider)
}
