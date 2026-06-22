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

#![allow(clippy::missing_errors_doc)]

use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

use super::embeddings::table::EmbeddingTable;
use crate::candidate::vector::ChunkedNonIndexVectorGeneration;
use crate::candidate::vector_udtf::VectorUDTFGeneration;
use crate::error::{DataFusionSnafu, Error, FormattingSnafu, Result, SearchPipelineSnafu};
use crate::table_provider_explorer::TableProviderExplorer;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";
use arrow::array::RecordBatch;
use async_stream::stream;
use cache::key::{CacheKey, RawCacheKey, SearchKey};
use cache::result::CacheStatus;
use cache::result::query::CachedStream;
use cache::result::search::{CachedAggregationResult, CachedSearchResult};
use cache::{Sizeable, TabledCacheProvider};
use datafusion::catalog::TableProvider;
use datafusion::common::{Column, DFSchema, SchemaError};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_expr::Expr;
use datafusion_expr::sqlparser::ast;
use futures::StreamExt;
use itertools::Itertools;
#[cfg(feature = "models")]
use runtime_datafusion_udfs::embed::EMBED_UDF_NAME;
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use runtime_query_engine::query_engine::QueryEngine;
use runtime_request_context::{AsyncMarker, CacheControl, CacheKeyType, RequestContext};
use search::index::SearchIndex;
use search::index::chunking::ChunkedSearchIndex;
#[cfg(feature = "duckdb")]
use search::index::duckdb::DuckDBVectorIndex;
use search::index::native_vector::NativeVectorIndex;
#[cfg(feature = "s3_vectors")]
use search::index::s3_vectors::S3Vector;
use search::{
    aggregation::{AggregationResult, reciprocal_rank::ReciprocalRankFusion},
    generation::CandidateGeneration,
    pipeline::SearchPipeline,
};
use snafu::ResultExt;
use tracing::{Instrument, Span};

use crate::request::SearchRequest;
use crate::types::VectorSearchResult;

/// A Component that can perform search operations.
pub struct SearchEngine<E: TableProviderExplorer> {
    pub df: Arc<dyn QueryEngine>,

    explicit_primary_keys: HashMap<TableReference, Vec<String>>,

    search_cache: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,

    explorer: E,
}

impl<E: TableProviderExplorer> SearchEngine<E> {
    pub fn new(
        df: Arc<dyn QueryEngine>,
        explicit_primary_keys: HashMap<TableReference, Vec<String>>,
        explorer: E,
    ) -> Self {
        SearchEngine {
            df,
            explicit_primary_keys,
            search_cache: None,
            explorer,
        }
    }

    #[must_use]
    pub fn with_search_cache(
        mut self,
        cache: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,
    ) -> Self {
        self.search_cache = cache;
        self
    }

    #[must_use]
    pub fn search_cache(
        &self,
    ) -> Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>> {
        self.search_cache.clone()
    }

    async fn user_tables_that_can_search(&self) -> Result<Vec<TableReference>> {
        let mut searchable_tables = Vec::new();
        for t in self.df.get_user_table_names() {
            if self
                .embedding_columns_from_table(&t)
                .await
                .is_some_and(|cols| !cols.is_empty())
            {
                searchable_tables.push(t);
                continue;
            }
            if self
                .full_text_search_candidates(&t)
                .await
                .is_some_and(|fts_res| fts_res.is_ok_and(|c| !c.is_empty()))
            {
                searchable_tables.push(t);
            }
        }
        Ok(searchable_tables)
    }

    async fn embedding_columns_from_table(&self, tbl: &TableReference) -> Option<Vec<String>> {
        let table_provider = self.df.get_table(tbl).await?;
        let mut embedding_columns: HashSet<String> = HashSet::default();

        if let Some(embedding_table) = self
            .explorer
            .find_concrete::<EmbeddingTable>(&table_provider)
        {
            for c in embedding_table.get_embedding_columns() {
                embedding_columns.insert(c);
            }
        }

        #[cfg(feature = "s3_vectors")]
        {
            use search::index::s3_vectors::S3Vector;
            if let Some((indexes, _)) = self.explorer.find_index::<S3Vector>(&table_provider) {
                embedding_columns.extend(indexes.iter().map(|i| i.search_column()));
            }
        }

        if let Some((indexes, _)) = self
            .explorer
            .find_index::<ChunkedSearchIndex>(&table_provider)
        {
            embedding_columns.extend(indexes.iter().map(|i| i.search_column()));
        }

        #[cfg(feature = "elasticsearch")]
        {
            use search::index::elasticsearch::ElasticsearchIndex;
            if let Some((indexes, _)) = self
                .explorer
                .find_index::<ElasticsearchIndex>(&table_provider)
            {
                embedding_columns.extend(indexes.iter().map(|i| i.search_column()));
            }
        }

        #[cfg(feature = "duckdb")]
        {
            use search::index::duckdb::DuckDBVectorIndex;
            if let Some((indexes, _)) = self
                .explorer
                .find_index::<DuckDBVectorIndex>(&table_provider)
            {
                embedding_columns.extend(indexes.iter().map(|i| i.search_column()));
            }
        }

        Some(embedding_columns.into_iter().collect())
    }

    async fn full_text_search_candidates(
        &self,
        tbl: &TableReference,
    ) -> Option<Result<Vec<Arc<dyn CandidateGeneration>>>> {
        use crate::full_text::as_candidate_generations;
        #[cfg(feature = "elasticsearch")]
        use crate::full_text::as_es_text_candidate_generations;
        use runtime_datafusion_index::IndexedTableProvider;
        use search::generation::text_search::index::FullTextDatabaseIndex;

        let base_table_provider = self.df.get_table(tbl).await?;

        let Some(indexed_table) = self
            .explorer
            .find_concrete::<IndexedTableProvider>(&base_table_provider)
        else {
            return Some(Ok(vec![]));
        };

        if let Some(fts) = indexed_table.get_index::<FullTextDatabaseIndex>() {
            return Some(
                as_candidate_generations(
                    &fts.with_new_base(Arc::clone(&base_table_provider)),
                    Arc::clone(&self.df),
                    tbl.clone(),
                )
                .await
                .map_err(|source| Error::SearchGenerationError { source }),
            );
        }

        #[cfg(feature = "elasticsearch")]
        {
            use search::index::elasticsearch::ElasticsearchTextIndex;
            let es_indexes = indexed_table.get_indexes::<ElasticsearchTextIndex>();
            if !es_indexes.is_empty() {
                return Some(
                    as_es_text_candidate_generations(es_indexes, Arc::clone(&self.df), tbl.clone())
                        .await
                        .map_err(|source| Error::SearchGenerationError { source }),
                );
            }
        }

        Some(Ok(vec![]))
    }

    fn get_vector_index(
        &self,
        tbl: &Arc<dyn TableProvider>,
        embedding_column: &str,
    ) -> Option<Arc<dyn SearchIndex>> {
        if let Some((indexes, _)) = self.explorer.find_index::<ChunkedSearchIndex>(tbl)
            && let Some(index) = indexes
                .into_iter()
                .find(|idx| idx.search_column() == embedding_column)
        {
            return Some(Arc::new(index.clone()) as Arc<dyn SearchIndex>);
        }

        #[cfg(feature = "s3_vectors")]
        if let Some((indexes, _)) = self.explorer.find_index::<S3Vector>(tbl)
            && let Some(index) = indexes
                .into_iter()
                .find(|idx| idx.search_column() == embedding_column)
        {
            return Some(Arc::new(index.clone()) as Arc<dyn SearchIndex>);
        }

        #[cfg(feature = "duckdb")]
        if let Some((indexes, _)) = self.explorer.find_index::<DuckDBVectorIndex>(tbl)
            && let Some(index) = indexes
                .into_iter()
                .find(|idx| idx.search_column() == embedding_column)
        {
            return Some(Arc::new(index.clone()) as Arc<dyn SearchIndex>);
        }

        if let Some((indexes, _)) = self.explorer.find_index::<NativeVectorIndex>(tbl)
            && let Some(index) = indexes
                .into_iter()
                .find(|idx| idx.search_column() == embedding_column)
        {
            return Some(Arc::new(index.clone()) as Arc<dyn SearchIndex>);
        }

        None
    }

    // Prepare an individual [`impl search::CandidateGeneration`] (specifically a [`VectorGeneration`]) based on the [`TableReference`].
    pub async fn vector_search_generator(
        &self,
        tbl: &TableReference,
        embedding_column: &str,
        primary_keys: &[String],
    ) -> Result<Arc<dyn CandidateGeneration>> {
        let table_provider = self
            .df
            .get_table(tbl)
            .await
            .ok_or(Error::DataSourcesNotFound {
                data_source: vec![tbl.clone()],
            })?;

        if let Some(vector_index) = self.get_vector_index(&table_provider, embedding_column) {
            let is_chunked = vector_index
                .as_any()
                .downcast_ref::<ChunkedSearchIndex>()
                .is_some();

            Ok(Arc::new(VectorUDTFGeneration::new(
                &self.df,
                tbl,
                embedding_column,
                is_chunked,
            )))
        } else {
            let Some(embedding_table) = self
                .explorer
                .find_concrete::<EmbeddingTable>(&table_provider)
            else {
                return Err(Error::CannotVectorSearchDataset {
                    data_source: tbl.clone(),
                });
            };

            // Use UDTF for scalar non-chunked `EmbeddingTable`. Both
            // chunked-scalar and multi-vector (list-typed) sources need
            // the UNNEST-based non-indexed search path.
            let is_multi_vector = embedding_table.is_multi_vector(embedding_column);
            if !embedding_table.is_chunked(embedding_column) && !is_multi_vector {
                return Ok(Arc::new(VectorUDTFGeneration::new(
                    &self.df,
                    tbl,
                    embedding_column,
                    false,
                )));
            }

            let state = self.df.session_context().state();
            let Some(embed_udf) = state.scalar_functions().get(EMBED_UDF_NAME) else {
                return Err(Error::EmbeddingError {
                    source: Box::from(format!(
                        "Vector search on chunked table '{tbl}' requires missing UDF: '{EMBED_UDF_NAME}'",
                    )),
                });
            };

            let Some(model_name) = embedding_table.get_embedding_model_used_by(embedding_column)
            else {
                return Err(Error::CannotVectorSearchDataset {
                    data_source: tbl.clone(),
                });
            };

            if is_multi_vector {
                let aggregation = embedding_table
                    .multi_vector_aggregation(embedding_column)
                    .unwrap_or_default();
                Ok(Arc::new(ChunkedNonIndexVectorGeneration::new_list_multi(
                    &table_provider,
                    tbl,
                    embed_udf,
                    model_name,
                    primary_keys.to_vec(),
                    embedding_column,
                    aggregation,
                )))
            } else {
                Ok(Arc::new(ChunkedNonIndexVectorGeneration::new(
                    &table_provider,
                    tbl,
                    embed_udf,
                    model_name,
                    primary_keys.to_vec(),
                    embedding_column,
                )))
            }
        }
    }

    pub async fn search_with_cache(
        &self,
        req: &SearchRequest,
        cache_provider: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,
        request_context: Arc<RequestContext>,
    ) -> Result<(VectorSearchResult, CacheStatus)> {
        Ok(if let Some(cache_provider) = cache_provider {
            tracing::trace!("Search cache is enabled");
            let search_key = SearchKey::from(req.clone());
            let cache_control = request_context.cache_control();

            let scoped_user_cache_key =
                if cache_control.cache_key_type() == Some(CacheKeyType::ClientSupplied) {
                    request_context.scoped_client_supplied_cache_key()
                } else {
                    None
                };
            let cache_key = match scoped_user_cache_key.as_deref() {
                Some(cache_key) => CacheKey::ClientSupplied(cache_key),
                _ => CacheKey::Search(&search_key),
            };

            let raw_cache_key = {
                let ns = request_context.cache_namespace();
                let (ns_tag, ns_id) = ns.hash_inputs();
                cache_key.as_raw_key_in_namespace(cache_provider.hasher(), ns_tag, ns_id)
            };

            match (
                cache_control,
                cache_provider.get_raw_key(&raw_cache_key.as_u64()).await,
            ) {
                (CacheControl::NoCache, _) => {
                    tracing::trace!("Search cache bypass");
                    let results = self.search(req).await?;
                    (
                        wrap_cache_to_result(raw_cache_key, results, Arc::clone(&cache_provider)),
                        CacheStatus::CacheBypass,
                    )
                }
                (
                    CacheControl::Cache(_)
                    | CacheControl::MaxStale(_, _)
                    | CacheControl::MinFresh(_, _)
                    | CacheControl::OnlyIfCached(_),
                    None,
                ) => {
                    tracing::trace!("Search cache miss");
                    let results = self.search(req).await?;
                    (
                        wrap_cache_to_result(raw_cache_key, results, Arc::clone(&cache_provider)),
                        CacheStatus::CacheMiss,
                    )
                }
                (
                    CacheControl::Cache(_)
                    | CacheControl::MaxStale(_, _)
                    | CacheControl::MinFresh(_, _)
                    | CacheControl::OnlyIfCached(_),
                    Some(cached_result),
                ) => {
                    tracing::trace!("Search cache hit");
                    // each CachedAggregationResult needs to be re-mapped to an AggregationResult
                    let mut results = HashMap::new();
                    for (table_ref, cached_aggregation_result) in cached_result.results.iter() {
                        let result = AggregationResult {
                            data: Box::pin(CachedStream::new(
                                Arc::clone(&cached_aggregation_result.records),
                                Arc::clone(&cached_aggregation_result.schema),
                            )),
                            primary_key: cached_aggregation_result.primary_keys.clone(),
                            data_columns: cached_aggregation_result.data_columns.clone(),
                            matches: cached_aggregation_result.matches.clone(),
                        };
                        results.insert(table_ref.clone(), result);
                    }

                    (results, CacheStatus::CacheHit)
                }
            }
        } else {
            tracing::trace!("Search cache is disabled");
            (self.search(req).await?, CacheStatus::CacheDisabled)
        })
    }

    pub async fn search(&self, req: &SearchRequest) -> Result<VectorSearchResult> {
        let SearchRequest {
            text: query,
            datasets: data_source_opt,
            limit,
            where_cond,
            additional_columns,
            keywords,
        } = req;
        let explicit_datasets_requested = data_source_opt.is_some();

        let tables = match data_source_opt {
            Some(ts) => ts.iter().map(TableReference::from).collect(),
            None => self.user_tables_that_can_search().await?,
        };

        if tables.is_empty() {
            return Err(Error::NoTablesWithSearchFound {});
        }

        let span = match Span::current() {
            span if matches!(span.metadata(), Some(metadata) if metadata.name() == "search") => {
                span
            }
            _ => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "search", input = query)
            }
        };

        let search_result = async {
            tracing::info!(target: "task_history", tables = tables.iter().join(","), limit = %limit, "labels");
            let table_primary_keys = get_primary_keys_with_overrides(&self.df, &tables, &self.explicit_primary_keys)
                .await?;

            // Search for each table is independent, but done in parallel.
            let response: HashMap<TableReference, AggregationResult> = futures::future::try_join_all(tables.into_iter().map(|tbl| {
                let keywords = keywords.clone();
                let primary_keys = table_primary_keys.get(&tbl).map_or(&[] as &[String], |v| v.as_slice());

                async move {
                    let request_context = RequestContext::current(AsyncMarker::new().await);
                    let embedding_columns = self.embedding_columns_from_table(&tbl).await.unwrap_or_default();
                    let mut generators: Vec<Arc<dyn CandidateGeneration>> = Vec::with_capacity(embedding_columns.len());
                    for (i, col) in embedding_columns.iter().enumerate() {
                        generators.insert(i, self.vector_search_generator(
                                &tbl,
                                col.as_str(),
                                primary_keys,
                            ).await?
                        );
                    };

                    // If the dataset is configured with full text search capabilities, add as generator.
                    if let Some(mut fts) = self.full_text_search_candidates(&tbl).await.transpose()? {
                        telemetry::track_text_search(&request_context.to_dimensions());
                        generators.append(&mut fts);
                    }

                    if explicit_datasets_requested && generators.is_empty() {
                        return Err(Error::CannotSearchDataset {
                            data_source: tbl.clone(),
                        });
                    }

                    // Ensure columns for a specific table aren't used on all tables.
                    let table_cols: Vec<_> = additional_columns
                        .iter()
                        .filter(|&c| c.relation.as_ref().is_none_or(|rel| tbl.clone().resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA) == rel.clone().resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)))
                        .cloned()
                        .map(Expr::Column)
                        .collect();

                    let pipe = SearchPipeline::new(generators, ReciprocalRankFusion, Arc::clone(&self.df));
                    let agg_result = pipe.run(
                        query.clone(),
                        &tbl,
                        get_filter_for_table(&self.df, &tbl, where_cond.as_ref()).await?,
                        table_cols,
                        primary_keys.iter().map(|pk| Column::from_name(pk.clone()) ).collect::<Vec<Column>>(),
                        keywords,
                        *limit
                    ).await.context(SearchPipelineSnafu)?;

                    Ok((tbl.clone(), agg_result))
                }
            }).collect::<Vec<_>>()).await?.into_iter().filter_map(|(tbl, result)| Some((tbl, result?))).collect();

            Ok(response)

        }.instrument(span.clone()).await;

        match search_result {
            Ok(result) => {
                let displayable: HashMap<String, serde_json::Value> = result
                    .iter()
                    .map(|(tbl, agg_result)| (tbl.to_string(), agg_result.display_json()))
                    .collect();
                let captured_output_json = serde_json::to_string(&displayable)
                    .boxed()
                    .context(FormattingSnafu)?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(result)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

async fn get_filter_for_table(
    df: &Arc<dyn QueryEngine>,
    tbl: &TableReference,
    filter_opt: Option<&ast::Expr>,
) -> Result<Option<Expr>, Error> {
    let Some(filter) = filter_opt else {
        return Ok(None);
    };

    let table = df.get_table(tbl).await.ok_or(Error::DataSourcesNotFound {
        data_source: vec![tbl.clone()],
    })?;
    let schema = DFSchema::try_from_qualified_schema(tbl.clone(), &table.schema())
        .context(DataFusionSnafu)?;

    match df
        .session_context()
        .state()
        .create_logical_expr(&filter.to_string(), &schema)
    {
        Ok(f) => Ok(Some(f)),
        Err(e) if is_field_not_found_on_unrelated_table(tbl, &e) => {
            tracing::debug!(
                "Ignoring SQL filter ('{}') on table {tbl:?} for search request as its columns do not reference this table",
                filter
            );
            Ok(None)
        }
        Err(e) => Err(Error::DataFusionError { source: e }),
    }
}

/// Checks if the [`DataFusionError`] is about a specific field not being found in a schema (i.e [`SchemaError`]).
///
/// Returns true iff the error is of this nature AND the field is unrelated (i.e. an explicit
/// [`Column::relation`] to a different table) to the provided `tbl`
fn is_field_not_found_on_unrelated_table(tbl: &TableReference, e: &DataFusionError) -> bool {
    let DataFusionError::Diagnostic(_, inner) = e else {
        return false;
    };
    let DataFusionError::SchemaError(err, _) = &**inner else {
        return false;
    };
    let SchemaError::FieldNotFound { field, .. } = &**err else {
        return false;
    };

    // Unrelated table is only if a different relation is explicit set on the column.
    !field.relation.as_ref().is_none_or(|rel| {
        tbl.clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            == rel
                .clone()
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
    })
}

fn wrap_cache_to_result(
    key: RawCacheKey,
    aggregation_result: HashMap<TableReference, AggregationResult>,
    cache_provider: Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>,
) -> HashMap<TableReference, AggregationResult> {
    // each hashmap entry is an aggregation result which contains a sendable record batch stream
    // for each table reference, we need to wrap the batch stream in another stream to pull out the record batches
    // because these occur on different streams though, we need a channel to centralise the results
    // once the results are collated, we can cache the result
    let mut wrapped_results = HashMap::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(TableReference, CachedAggregationResult)>(100);
    let tx = Arc::new(tx);
    let expected_keys: HashSet<TableReference> =
        aggregation_result.keys().cloned().collect::<HashSet<_>>();

    for (table_ref, aggregation_result) in aggregation_result {
        let tx = Arc::clone(&tx);
        let primary_key = aggregation_result.primary_key.clone();
        let data_columns = aggregation_result.data_columns.clone();
        let matches = aggregation_result.matches.clone();
        let mut stream = aggregation_result.data;
        let schema = stream.schema();

        let cloned_table_ref = table_ref.clone();
        let cloned_primary_key = primary_key.clone();
        let cloned_data_columns = data_columns.clone();
        let cloned_matches = matches.clone();
        let cloned_schema = Arc::clone(&schema);
        let cloned_cache_provider = Arc::clone(&cache_provider);

        let cached_stream = stream! {
            let mut records: Vec<RecordBatch> = Vec::new();
            let mut records_size: usize = 0;
            let cache_max_size = cloned_cache_provider.max_size();

            while let Some(batch_result) = stream.next().await {
                if records_size < cache_max_size
                    && let Ok(batch) = &batch_result {
                        records.push(batch.clone());
                        records_size += batch.get_array_memory_size();
                    }

                yield batch_result;
            }

            if records_size < cache_max_size {
                let cached_result = CachedAggregationResult::new(
                    Arc::new(records),
                    cloned_primary_key,
                    cloned_data_columns,
                    cloned_matches,
                    cloned_schema,
                );
                if tx.send((cloned_table_ref.clone(), cached_result)).await.is_err() {
                    tracing::error!("Failed to send cached search result for {cloned_table_ref}");
                }
            }
        };

        wrapped_results.insert(
            table_ref,
            AggregationResult {
                primary_key,
                data_columns,
                matches,
                data: Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    Box::pin(cached_stream),
                )),
            },
        );
    }

    // start a background task to collect the results and cache them
    // if we try to do this in this function before returning, we never return results so the stream never gets polled
    // if the stream never gets polled, we never get results on the channel
    // deadlock!
    tokio::spawn(async move {
        let mut results = HashMap::new();
        while let Some((table_ref, cached_result)) = rx.recv().await {
            results.insert(table_ref, cached_result);
        }

        if results.is_empty() {
            tracing::trace!("No results to cache for tables: {expected_keys:?}");
            return;
        } else if expected_keys.iter().any(|key| !results.contains_key(key)) {
            tracing::trace!(
                "Not all expected keys were found in the cached results: {expected_keys:?}"
            );
            return;
        }

        tracing::trace!("Caching search results for key: {}", key.as_u64());

        let result = CachedSearchResult {
            results: Arc::new(results),
            input_tables: Arc::new(expected_keys),
        };

        if result.get_memory_size() > cache_provider.max_size() {
            tracing::trace!("Search results exceed cache size, not caching");
            return;
        }

        cache_provider.put_raw_key(&key.as_u64(), result).await;
    });

    wrapped_results
}

async fn get_primary_keys_from_table(
    df: &Arc<dyn QueryEngine>,
    table: &TableReference,
) -> Result<Vec<String>> {
    let tbl_ref = df
        .get_table(table)
        .await
        .ok_or_else(|| Error::DataSourcesNotFound {
            data_source: vec![table.clone()],
        })?;

    search::generation::util::get_primary_keys(&tbl_ref).map_err(|e| Error::DataFusionError {
        source: DataFusionError::from(e),
    })
}

async fn get_primary_keys_with_overrides(
    df: &Arc<dyn QueryEngine>,
    tables: &[TableReference],
    explicit_primary_keys: &HashMap<TableReference, Vec<String>>,
) -> Result<HashMap<TableReference, Vec<String>>> {
    let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

    for tbl in tables {
        let resolved_tbl: TableReference = tbl
            .clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            .into();
        let pks = get_primary_keys_from_table(df, &resolved_tbl).await?;
        if !pks.is_empty() {
            tbl_to_pks.insert(tbl.clone(), pks);
        } else if let Some(explicit_pks) = explicit_primary_keys.get(&resolved_tbl) {
            tbl_to_pks.insert(tbl.clone(), explicit_pks.clone());
        }
    }
    Ok(tbl_to_pks)
}

/// Compute the primary keys for each table in the app.
pub async fn parse_explicit_primary_keys(
    app: Arc<tokio::sync::RwLock<Option<Arc<app::App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        let mut pks = app
            .datasets
            .iter()
            .filter_map(|d| {
                d.primary_key_override().map(|pks| {
                    (
                        TableReference::parse_str(&d.name)
                            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                            .into(),
                        pks,
                    )
                })
            })
            .collect::<HashMap<TableReference, Vec<_>>>();

        pks.extend(app.views.iter().filter_map(|d| {
            d.primary_key_override().map(|pks| {
                (
                    TableReference::parse_str(&d.name)
                        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                        .into(),
                    pks,
                )
            })
        }));
        pks
    })
}
