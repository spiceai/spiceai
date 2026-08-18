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
use snafu::ResultExt;
use spice_table::{Index, IndexLayer, SpiceTable};
use spicepod::semantic::{Column, IndexStore};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::component::column::full_text_search_config;
use crate::component::dataset::FullTextSearchDatasetConfig;
use crate::make_spice_data_sub_directory;
use spicepod::semantic::MetadataType;

use search::generation::text_search::index::FullTextDatabaseIndex;

/// Builds (but does not register) a [`FullTextDatabaseIndex`] over `inner_table_provider`.
///
/// `store_fields_override` replaces the store-fields set derived from the columns' vector
/// metadata. The compound warm-tier caller supplies the Elasticsearch tier's metadata fields so
/// both query plans have identical schemas; the plain full-text caller passes `None` to keep the
/// metadata-derived set.
///
/// Expects at least one [`Column`] to have a full text search column configured.
pub(crate) fn build_full_text_database_index(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[Column],
    tbl: &TableReference,
    store_fields_override: Option<&[String]>,
) -> Result<FullTextDatabaseIndex, Box<dyn std::error::Error + Send + Sync>> {
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

    // `derived_store_fields` outlives the borrow below only when no override is supplied.
    let derived_store_fields;
    let store_fields: &[String] = if let Some(store_fields) = store_fields_override {
        store_fields
    } else {
        derived_store_fields = columns
            .iter()
            .filter_map(|c| {
                // Both metadata kinds are only about vector-search's own metadata filter;
                // either one still means the column should be projectable and, per its
                // tantivy type, filterable through the FTS index too.
                c.as_vector_metadata()?;
                let (_, field) = schema.column_with_name(&c.name)?;
                if !FullTextDatabaseIndex::is_field_type_supported(field.data_type()) {
                    // e.g. `Date32`/`Date64`/`Timestamp`: a valid `Filterable` metadata type for
                    // other index backends (Elasticsearch), but not yet representable in the
                    // local FTS schema. Skip it here rather than fail index construction.
                    tracing::warn!(
                        "Column {} on table {} has vector-search metadata but its type ({}) is not supported by the full text search index; it will not be filterable there",
                        c.name,
                        tbl,
                        field.data_type()
                    );
                    return None;
                }
                Some(c.name.clone())
            })
            .collect::<Vec<_>>();
        derived_store_fields.as_slice()
    };

    FullTextDatabaseIndex::try_new(
        inner_table_provider,
        search_fields,
        Some(primary_key),
        directory,
        store_fields,
    )
    .boxed()
}

/// Registers `index` on `inner_table_provider`, reusing an index layer already at
/// the top of the stack so several indexes compose onto one layer rather than
/// stacking a layer apiece.
fn register_index(
    inner_table_provider: &Arc<dyn TableProvider>,
    index: Arc<dyn Index + Send + Sync>,
) -> Arc<SpiceTable> {
    if let Some(table) = inner_table_provider.downcast_ref::<SpiceTable>()
        && !table.indexes().is_empty()
    {
        let mut indexes = table.indexes().to_vec();
        indexes.push(index);
        return SpiceTable::over(
            Arc::new(IndexLayer::with_indexes(indexes)),
            Arc::clone(table.below()),
        );
    }

    SpiceTable::over(
        Arc::new(IndexLayer::with_indexes(vec![index])),
        Arc::clone(inner_table_provider),
    )
}

/// Adds a [`FullTextDatabaseIndex`] to a [`TableProvider`].
///
/// Expects at least one [`Column`] to have a full text search column configured.
pub(crate) fn add_full_text_search_to_table(
    inner_table_provider: &Arc<dyn TableProvider>,
    columns: &[Column],
    tbl: &TableReference,
) -> Result<Arc<SpiceTable>, Box<dyn std::error::Error + Send + Sync>> {
    let index =
        build_full_text_database_index(Arc::clone(inner_table_provider), columns, tbl, None)?;
    Ok(register_index(
        inner_table_provider,
        Arc::new(index) as Arc<dyn Index + Send + Sync>,
    ))
}

/// Adds a single [`ElasticsearchTextIndex`] to a [`TableProvider`] covering all FTS-enabled columns.
///
/// A single index instance is created with all `search_fields` so that one `_bulk` write
/// per batch indexes every FTS column as fields of the same ES document — the correct
/// Elasticsearch model. At query time `call_with_es_indexes` in the UDTF dispatcher
/// selects the requested column from `search_fields` on that shared instance.
///
/// The index is registered via [`IndexLayer::add_index`] so it is visible to the
/// query optimizer and can be discovered by `find_index_in_table_provider` for `text_search()`
/// queries. For the accelerator-side path, indexes are automatically discovered from the
/// federated provider chain — no manual registration is needed.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn add_elasticsearch_fts_to_table(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &runtime_search::store_params::elasticsearch::ElasticsearchFtsConfig,
) -> Result<Arc<SpiceTable>, Box<dyn std::error::Error + Send + Sync>> {
    let index =
        build_elasticsearch_text_index(Arc::clone(&inner_table_provider), columns, tbl, fts_params)
            .await?;
    Ok(register_index(
        &inner_table_provider,
        index as Arc<dyn Index + Send + Sync>,
    ))
}

/// Adds a compound (write-through + fallback) full-text search index to `inner_table_provider`:
/// a warm local Tantivy tier in front of the external Elasticsearch tier.
///
/// Writes fan out to both tiers; reads serve from the warm Tantivy index, falling back to
/// Elasticsearch on empty results only when `on_zero_results` is
/// [`ZeroResultsAction::UseSource`] (mirrors the accelerator→source fallback setting). Only the
/// compound index is registered, so discovery resolves to it ahead of the concrete indexes.
///
/// Scope and graceful degradation:
///  - **Multi-column full-text search** is out of scope: one [`CompoundSearchIndex`] keys on a
///    single search column, and building one per field would multiply write-through work. Such
///    datasets register the Elasticsearch index alone (today's behavior, no warm tier, no
///    regression). Tracked in <https://github.com/spiceai/spiceai/issues/11963>.
///  - The Elasticsearch tier is the user-configured store, so failing to build it is fatal.
///  - Building or composing the warm Tantivy tier must **never** fail dataset load: on error the
///    Elasticsearch index is registered alone and a warning is logged (mirrors the vector warm
///    index). A common cause is a primary key whose type Elasticsearch normalizes (e.g.
///    `LargeUtf8` → `Utf8`), which trips the compound compatibility check.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn add_compound_fts_to_table(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &runtime_search::store_params::elasticsearch::ElasticsearchFtsConfig,
    on_zero_results: &crate::component::dataset::acceleration::ZeroResultsAction,
) -> Result<Arc<SpiceTable>, Box<dyn std::error::Error + Send + Sync>> {
    use crate::component::dataset::acceleration::ZeroResultsAction;
    use search::index::SearchIndex;
    use search::index::compound::{CompoundReadMode, CompoundSearchIndex};

    // Multi-column full-text search through the compound is out of scope (#11963): a single
    // compound keys on one search column, and one compound per field would multiply the
    // write-through work. Keep the Elasticsearch-only behavior for these datasets.
    let search_field_count = full_text_search_config(columns, tbl)
        .map(|config| config.search_fields.len())
        .unwrap_or_default();
    if search_field_count > 1 {
        tracing::info!(
            "Dataset {tbl} is configured with {search_field_count} full-text search columns; the write-through warm index is single-column only, so full-text searches are served by Elasticsearch directly. Multi-column warm indexing is tracked in https://github.com/spiceai/spiceai/issues/11963."
        );
        return add_elasticsearch_fts_to_table(inner_table_provider, columns, tbl, fts_params)
            .await;
    }

    // The Elasticsearch tier is the user-configured store; a failure here is a configuration error.
    let es_index =
        build_elasticsearch_text_index(Arc::clone(&inner_table_provider), columns, tbl, fts_params)
            .await?;

    // Store the same metadata fields in the warm Tantivy tier so both sides of the empty-result
    // fallback expose identical `[primary key…, metadata…, _score]` schemas.
    let metadata_fields: Vec<String> = es_index
        .metadata_columns
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let warm_index = match build_full_text_database_index(
        Arc::clone(&inner_table_provider),
        columns,
        tbl,
        Some(&metadata_fields),
    ) {
        Ok(index) => index,
        Err(source) => {
            tracing::warn!(
                "Not adding a warm full-text search index for dataset {tbl}: {source}. Full-text searches will be served by Elasticsearch directly."
            );
            return Ok(register_index(
                &inner_table_provider,
                es_index as Arc<dyn Index + Send + Sync>,
            ));
        }
    };

    // Mirrors the accelerator→source `on_zero_results` setting: only fall back to the
    // Elasticsearch secondary on an empty warm-tier result when the dataset opted in.
    let read_mode = match on_zero_results {
        ZeroResultsAction::ReturnEmpty => CompoundReadMode::PrimaryOnly,
        ZeroResultsAction::UseSource => CompoundReadMode::FallbackToSecondary,
    };

    let compound = match CompoundSearchIndex::try_new(
        Arc::new(warm_index) as Arc<dyn SearchIndex>,
        Arc::clone(&es_index) as Arc<dyn SearchIndex>,
        read_mode,
    ) {
        Ok(compound) => compound,
        Err(source) => {
            tracing::warn!(
                "Not adding a warm full-text search index for dataset {tbl}: {source}. Full-text searches will be served by Elasticsearch directly."
            );
            return Ok(register_index(
                &inner_table_provider,
                es_index as Arc<dyn Index + Send + Sync>,
            ));
        }
    };

    Ok(register_index(
        &inner_table_provider,
        Arc::new(compound) as Arc<dyn Index + Send + Sync>,
    ))
}

/// Builds (but does not register) an [`ElasticsearchTextIndex`] for all FTS-enabled columns.
///
/// The returned index is added to the federated provider chain via
/// [`IndexLayer::add_index`] in [`add_elasticsearch_fts_to_table`]. On the
/// accelerator write path, indexes are automatically discovered from the federated provider
/// chain by [`RefreshTaskBuilder::build`] — no manual `sink_index` plumbing is needed.
#[cfg(feature = "elasticsearch")]
pub(crate) async fn build_elasticsearch_text_index(
    inner_table_provider: Arc<dyn TableProvider>,
    columns: &[spicepod::semantic::Column],
    tbl: &datafusion::sql::TableReference,
    fts_params: &runtime_search::store_params::elasticsearch::ElasticsearchFtsConfig,
) -> Result<
    Arc<search::index::elasticsearch::ElasticsearchTextIndex>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use crate::component::column::full_text_search_config;
    use crate::component::dataset::FullTextSearchDatasetConfig;
    use crate::embeddings::index::elasticsearch::{
        ElasticsearchIndexWriteMaintenance, ensure_index_with_text_mapping, es_metadata_columns,
        normalize_es_data_type,
    };
    use arrow_schema::Field;
    use runtime_search::store_params::elasticsearch::{
        build_client_options, build_write_options, merge_index_settings,
    };
    use search::index::elasticsearch::ElasticsearchTextIndex;
    use secrecy::ExposeSecret;

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

    let endpoint = fts_params.params.endpoint.as_deref().ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from(
            "Missing required parameter 'endpoint' for Elasticsearch FTS.",
        )
    })?;
    let client_options = build_client_options(
        fts_params.params.client_timeout,
        fts_params.params.connect_timeout,
        None,
        None,
    );
    let client: Arc<dyn elasticsearch::Elasticsearch> = Arc::new(
        elasticsearch::Client::new_with_options(
            endpoint,
            fts_params
                .params
                .user
                .as_ref()
                .map(ExposeSecret::expose_secret),
            fts_params
                .params
                .pass
                .as_ref()
                .map(ExposeSecret::expose_secret),
            &client_options,
        )
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    );

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

    let index_settings = merge_index_settings(
        fts_params.params.index_settings.as_ref(),
        fts_params.params.number_of_shards,
        fts_params.params.number_of_replicas,
        fts_params.params.refresh_interval.as_deref(),
    );
    let write_options = build_write_options(
        fts_params.params.bulk_load_refresh_interval.as_deref(),
        fts_params.params.force_merge_after_write,
        fts_params.params.force_merge_segments,
    )?;
    let write_maintenance = Arc::new(ElasticsearchIndexWriteMaintenance::new(write_options));

    // Resolve spicepod `vectors: filterable | non-filterable` hints into metadata columns,
    // mirroring the vector-index path: filterable columns get `index: true` (usable in query
    // filters), non-filterable columns are stored in `_source` only.
    let search_field_names: Vec<&str> = search_fields.iter().map(String::as_str).collect();
    let metadata_columns = es_metadata_columns(columns, &source_schema, &search_field_names);

    // Ensure the ES index exists with text mappings for all search fields.
    ensure_index_with_text_mapping(
        client.as_ref(),
        &fts_params.es_index,
        &search_fields,
        &metadata_columns,
        index_settings.as_ref(),
    )
    .await?;

    // `ensure_index_with_text_mapping` is best-effort for a pre-existing incompatible index (it
    // logs a warning and continues on a `put_mapping` failure), so the mapping it just applied
    // may not match what Spice asked for. Read the real mapping back so the filter-pushdown
    // schema reflects what Elasticsearch actually indexed, not what Spice assumed — the index was
    // just confirmed to exist, so a `get_mapping` failure here is a real error worth surfacing,
    // not one to mask with a fallback.
    let filter_schema = crate::embeddings::index::elasticsearch::fetch_filter_schema(
        client.as_ref(),
        &fts_params.es_index,
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
        metadata_columns,
        batch_write_rows: fts_params.params.batch_write_rows,
        write_maintenance: Arc::clone(&write_maintenance),
        filter_schema,
    }))
}

#[cfg(all(test, feature = "elasticsearch"))]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use runtime_parameters_typed::TypedParams as _;
    use runtime_search::store_params::elasticsearch::{
        ElasticsearchFtsConfig, ElasticsearchFtsParams,
    };
    use secrecy::SecretString;
    use spicepod::semantic::{Column, FullTextSearchConfig};
    use tokio::sync::RwLock;

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
        let params = ElasticsearchFtsParams::try_from_params(
            "Elasticsearch full-text search test",
            HashMap::from([(
                "elasticsearch_endpoint".to_string(),
                SecretString::from("http://localhost:9200".to_string()),
            )]),
            &Arc::new(RwLock::new(runtime_secrets::Secrets::default())),
        )
        .await
        .expect("FTS parameters should be valid");
        let fts_params = ElasticsearchFtsConfig {
            params,
            es_index: "docs".to_string(),
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

    /// The compound's warm tier must expose the same metadata fields as Elasticsearch so its
    /// empty-result fallback has one stable schema.
    #[tokio::test]
    async fn metadata_store_fields_are_exposed_with_primary_key_and_score() {
        use search::index::SearchIndex;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, true),
        ]));
        let table =
            Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
                as Arc<dyn TableProvider>;
        let columns = vec![
            Column::new("body")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
            Column::new("category").with_metadata(HashMap::from([(
                "vectors".to_string(),
                serde_json::json!("filterable"),
            )])),
        ];
        let table_ref = datafusion::sql::TableReference::parse_str("docs");
        let metadata_fields = vec!["category".to_string()];

        let index =
            build_full_text_database_index(table, &columns, &table_ref, Some(&metadata_fields))
                .expect("warm index builds");

        let plan = index
            .query_table_provider("hello")
            .expect("query plan builds");
        let field_names: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        assert!(
            field_names.iter().any(|f| f == "id"),
            "the primary key must be exposed: {field_names:?}"
        );
        assert!(
            field_names
                .iter()
                .any(|f| f == search::SEARCH_SCORE_COLUMN_NAME),
            "the score column must be exposed: {field_names:?}"
        );
        assert!(
            field_names.iter().any(|f| f == "category"),
            "filterable metadata must be exposed: {field_names:?}"
        );
    }
}
