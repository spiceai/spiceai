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

use std::{collections::HashMap, sync::Arc};

use super::request::SearchRequest;
use super::util::{get_embedding_table, user_tables_with_embeddings};
use super::{Error, Result};
use crate::request::CacheControl;
use crate::search::{
    SearchPipelineSnafu,
    candidate::vector::VectorGeneration,
    util::{embedding_columns_from_table, get_primary_keys_with_overrides},
};
use crate::{datafusion::DataFusion, model::EmbeddingModelStore};
use arrow::array::RecordBatch;
use async_stream::stream;
use cache::key::{CacheKey, RawCacheKey, SearchKey};
use cache::result::CacheStatus;
use cache::result::query::CachedStream;
use cache::result::search::{CachedAggregationResult, CachedSearchResult};
use cache::{CacheProvider, Sizeable};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::{
    TableReference,
    sqlparser::ast::{Expr, Ident},
};
use futures::StreamExt;
use itertools::Itertools;
use search::{
    aggregation::{AggregationResult, reciprocal_rank::ReciprocalRankFusion},
    generation::CandidateGeneration,
    pipeline::SearchPipeline,
};
use snafu::ResultExt;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use super::types::VectorSearchResult;

/// A Component that can perform vector search operations.
pub struct VectorSearch {
    pub df: Arc<DataFusion>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,

    // For tables, explicitly defined primary keys for datasets.
    // Are in [`ResolvedTableReference`] format.
    // Before use, must be resolved with spice defaults, `.resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)`.
    explicit_primary_keys: HashMap<TableReference, Vec<String>>,
}

impl VectorSearch {
    pub fn new(
        df: Arc<DataFusion>,
        embeddings: Arc<RwLock<EmbeddingModelStore>>,
        explicit_primary_keys: HashMap<TableReference, Vec<String>>,
    ) -> Self {
        VectorSearch {
            df,
            embeddings,
            explicit_primary_keys,
        }
    }

    // Prepare an individual [`impl search::CandidateGeneration`] (specifically a [`VectorGeneration`]) based on the [`TableReference`].
    pub async fn vector_search_generator(
        &self,
        tbl: &TableReference,
        embedding_column: &str,
        primary_keys: &[String],
    ) -> Result<VectorGeneration> {
        let table_provider = self
            .df
            .get_table(tbl)
            .await
            .ok_or(Error::DataSourcesNotFound {
                data_source: vec![tbl.clone()],
            })?;

        let Some(embedding_table) = get_embedding_table(&table_provider).await else {
            return Err(Error::CannotVectorSearchDataset {
                data_source: tbl.clone(),
            });
        };

        let Some(model_name) = embedding_table.get_embedding_model_used_by(embedding_column) else {
            return Err(Error::CannotVectorSearchDataset {
                data_source: tbl.clone(),
            });
        };

        let Some(embed) = self
            .embeddings
            .read()
            .await
            .iter()
            .find_map(|(name, model)| {
                if *name == model_name {
                    return Some(Arc::clone(model));
                }
                None
            })
        else {
            return Err(Error::CannotVectorSearchDataset {
                data_source: tbl.clone(),
            });
        };

        Ok(VectorGeneration::new(
            &self.df,
            tbl,
            &embed,
            primary_keys,
            embedding_column,
            embedding_table.is_chunked(embedding_column),
        ))
    }

    pub async fn search_with_cache(
        &self,
        req: &SearchRequest,
        cache_provider: Option<Arc<dyn CacheProvider<CachedSearchResult> + Send + Sync>>,
        cache_control: CacheControl,
    ) -> Result<(VectorSearchResult, CacheStatus)> {
        Ok(if let Some(cache_provider) = cache_provider {
            tracing::trace!("Search cache is enabled");
            if matches!(cache_control, CacheControl::NoCache) {
                tracing::trace!("Search cache bypassed");
                return Ok((self.search(req).await?, CacheStatus::CacheBypass));
            }

            let search_key = SearchKey::from(req.clone());
            let cache_key = CacheKey::Search(&search_key);
            let raw_cache_key = cache_key.as_raw_key(cache_provider.hasher());

            if let Some(cached_result) = cache_provider.get_raw_key(&raw_cache_key.as_u64()).await {
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
            } else {
                tracing::trace!("Search cache miss");
                let results = self.search(req).await?;
                (
                    wrap_cache_to_result(raw_cache_key, results, Arc::clone(&cache_provider)),
                    CacheStatus::CacheMiss,
                )
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

        let tables = match data_source_opt {
            Some(ts) => ts.iter().map(TableReference::from).collect(),
            None => user_tables_with_embeddings(&self.df).await?,
        };

        if tables.is_empty() {
            return Err(Error::NoTablesWithEmbeddingsFound {});
        }

        let span = match Span::current() {
            span if matches!(span.metadata(), Some(metadata) if metadata.name() == "vector_search") => {
                span
            }
            _ => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = query)
            }
        };

        let vector_search_result = async {
            tracing::info!(target: "task_history", tables = tables.iter().join(","), limit = %limit, "labels");
            let table_primary_keys = get_primary_keys_with_overrides(&self.df, &tables, &self.explicit_primary_keys)
                .await?;

            // Search for each table is independent, but done in parallel.
            let response: HashMap<TableReference, AggregationResult> = futures::future::try_join_all(tables.into_iter().map(|tbl| {
                let keywords = keywords.clone();
                let primary_keys = table_primary_keys.get(&tbl).map_or(&[] as &[String], |v| v.as_slice());

                async move {
                    let embedding_columns = embedding_columns_from_table(&self.df, &tbl).await?;
                    let mut generators: Vec<Box<dyn CandidateGeneration>> = Vec::with_capacity(embedding_columns.len());

                    for (i, col) in embedding_columns.iter().enumerate() {
                        generators.insert(i, Box::new(self.vector_search_generator(
                                &tbl,
                                col.as_str(),
                                primary_keys,
                            ).await?)
                        );
                    };

                    let agg_result = SearchPipeline::new(generators, ReciprocalRankFusion).run(
                         query.clone(),
                         where_cond.as_ref().map(|e| vec![e.clone()]).unwrap_or_default(),
                          additional_columns.iter().map(|s| Expr::Identifier(Ident::new(s))).collect(),
                          primary_keys.to_vec(),
                          keywords,
                          *limit
                    ).await.context(SearchPipelineSnafu)?;

                    Ok((tbl.clone(), agg_result))
                }
            }).collect::<Vec<_>>()).await?.into_iter().collect();

            Ok(response)

        }.instrument(span.clone()).await;

        match vector_search_result {
            Ok(result) => {
                tracing::info!(target: "task_history", captured_output = ?result);
                Ok(result)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

fn wrap_cache_to_result(
    key: RawCacheKey,
    aggregation_result: HashMap<TableReference, AggregationResult>,
    cache_provider: Arc<dyn CacheProvider<CachedSearchResult> + Send + Sync>,
) -> HashMap<TableReference, AggregationResult> {
    // each hashmap entry is an aggregation result which contains a sendable record batch stream
    // for each table reference, we need to wrap the batch stream in another stream to pull out the record batches
    // because these occur on different streams though, we need a channel to centralise the results
    // once the results are collated, we can cache the result
    let mut wrapped_results = HashMap::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(TableReference, CachedAggregationResult)>(100);
    let tx = Arc::new(tx);
    let expected_keys = aggregation_result.keys().cloned().collect::<Vec<_>>();

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
                if records_size < cache_max_size {
                    if let Ok(batch) = &batch_result {
                        records.push(batch.clone());
                        records_size += batch.get_array_memory_size();
                    }
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
        } else if !expected_keys
            .iter()
            .filter(|key| !results.contains_key(key))
            .collect::<Vec<_>>()
            .is_empty()
        {
            tracing::trace!(
                "Not all expected keys were found in the cached results: {expected_keys:?}"
            );
            return;
        }

        tracing::trace!("Caching search results for key: {}", key.as_u64());

        let result = CachedSearchResult {
            results: Arc::new(results),
        };

        if result.get_memory_size() > cache_provider.max_size() {
            tracing::trace!("Search results exceed cache size, not caching");
            return;
        }

        cache_provider.put_raw_key(&key.as_u64(), result).await;
    });

    wrapped_results
}
