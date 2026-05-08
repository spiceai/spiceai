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

/// Adds a single [`ElasticsearchTextIndex`] to a [`TableProvider`] covering all FTS-enabled columns.
///
/// A single index instance is created with all `search_fields` so that one `_bulk` write
/// per batch indexes every FTS column as fields of the same ES document — the correct
/// Elasticsearch model. At query time `call_with_es_indexes` in the UDTF dispatcher
/// selects the requested column from `search_fields` on that shared instance.
///
/// The index is registered via [`IndexedTableProvider::add_index`] so it is visible to the
/// query optimizer and can be discovered by `find_index_in_table_provider` for `text_search()`
/// queries. For the accelerator-side path, indexes are automatically discovered from the
/// federated provider chain — no manual registration is needed.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn add_elasticsearch_fts_to_table(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &crate::search::full_text::elasticsearch::ElasticsearchFtsParams,
) -> Result<IndexedTableProvider, Box<dyn std::error::Error + Send + Sync>> {
    use runtime_datafusion_index::Index;
    let index =
        build_elasticsearch_text_index(Arc::clone(&inner_table_provider), columns, tbl, fts_params)
            .await?;
    let mut provider: IndexedTableProvider = if let Some(idx_tbl) = inner_table_provider
        .as_any()
        .downcast_ref::<IndexedTableProvider>(
    ) {
        idx_tbl.clone()
    } else {
        IndexedTableProvider::new(Arc::clone(&inner_table_provider))
    };
    provider = provider.add_index(index as Arc<dyn Index + Send + Sync>);
    Ok(provider)
}

/// Builds (but does not register) an [`ElasticsearchTextIndex`] for all FTS-enabled columns.
///
/// The returned index is added to the federated provider chain via
/// [`IndexedTableProvider::add_index`] in [`add_elasticsearch_fts_to_table`]. On the
/// accelerator write path, indexes are automatically discovered from the federated provider
/// chain by [`RefreshTaskBuilder::build`] — no manual sink_index plumbing is needed.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn build_elasticsearch_text_index(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &crate::search::full_text::elasticsearch::ElasticsearchFtsParams,
) -> Result<
    Arc<search::index::elasticsearch::ElasticsearchTextIndex>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use crate::component::column::full_text_search_config;
    use crate::component::dataset::FullTextSearchDatasetConfig;
    use crate::embeddings::index::elasticsearch::{
        ElasticsearchIndexWriteMaintenance, ensure_index_with_text_mapping, get_fts_client,
        normalize_es_data_type, parse_index_settings_from_map, parse_write_options_from_map,
    };
    use arrow_schema::Field;
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
    // Also mark all fields as nullable — ES text search results may not include
    // every field (e.g. dense_vector embedding columns), and Arrow will reject
    // null values in non-nullable fields.
    let raw_schema = inner_table_provider.schema();
    let normalized_fields: Vec<Arc<arrow_schema::Field>> = raw_schema
        .fields()
        .iter()
        .filter(|f| {
            !matches!(
                f.data_type(),
                arrow::datatypes::DataType::FixedSizeList(_, _)
                    | arrow::datatypes::DataType::LargeList(_)
                    | arrow::datatypes::DataType::List(_)
            )
        })
        .map(|f| {
            let normalized = normalize_es_data_type(f.data_type());
            Arc::new(Field::new(f.name(), normalized, true))
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
                .map_err(|_| {
                    let valid_columns = raw_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    Box::<dyn std::error::Error + Send + Sync>::from(format!(
                        "Failed to configure Elasticsearch full-text search for dataset {tbl}: row_id column '{name}' does not exist in the dataset schema. Valid columns: {valid_columns}."
                    ))
                })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

    let index_settings = parse_index_settings_from_map(&fts_params.params)?;
    let write_options = parse_write_options_from_map(&fts_params.params)?;
    let write_maintenance = Arc::new(ElasticsearchIndexWriteMaintenance::new(write_options));

    // Ensure the ES index exists with text mappings for all search fields.
    ensure_index_with_text_mapping(
        client.as_ref(),
        &fts_params.es_index,
        &search_fields,
        index_settings.as_ref(),
    )
    .await?;

    // Create a single ElasticsearchTextIndex covering all FTS columns so that one _bulk
    // write per batch indexes every column as fields of the same ES document.
    // search_column_name is set to the first field as a fallback for single-column
    // text_search() calls that omit the column argument; call_with_es_indexes resolves
    // the actual column from search_fields at query time.
    let first_field = search_fields
        .first()
        .cloned()
        .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from(
            format!("Attempted to add Elasticsearch FTS to '{tbl}', but search_fields is empty after configuration")
        ))?;
    Ok(Arc::new(ElasticsearchTextIndex {
        client: Arc::clone(&client),
        es_index: fts_params.es_index.clone(),
        search_column_name: first_field,
        search_fields,
        primary_key: pk_fields,
        source_schema: Arc::clone(&source_schema),
        batch_write_rows: fts_params.batch_write_rows,
        write_maintenance: Arc::clone(&write_maintenance),
    }))
}

#[cfg(all(test, feature = "elasticsearch"))]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use spicepod::semantic::{Column, FullTextSearchConfig};

    use crate::search::full_text::elasticsearch::ElasticsearchFtsParams;

    #[tokio::test]
    async fn add_elasticsearch_fts_errors_when_row_id_missing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let table =
            Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
                as Arc<dyn TableProvider>;
        let columns = vec![
            Column::new("body")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("missing_id")),
        ];
        let fts_params = ElasticsearchFtsParams {
            params: HashMap::from([("endpoint".to_string(), "http://localhost:9200".to_string())]),
            es_index: "docs".to_string(),
            batch_write_rows: 1000,
        };
        let table_ref = datafusion::sql::TableReference::parse_str("docs");

        let err = add_elasticsearch_fts_to_table(table, &columns, &table_ref, &fts_params)
            .await
            .expect_err("missing row_id column should fail before indexing");

        assert!(
            err.to_string()
                .contains("row_id column 'missing_id' does not exist in the dataset schema"),
            "unexpected error: {err}"
        );
    }
}
