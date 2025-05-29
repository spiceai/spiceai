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
use crate::search::{
    SearchPipelineSnafu,
    candidate::vector::VectorGeneration,
    util::{embedding_columns_from_table, get_primary_keys_with_overrides},
};
use crate::{datafusion::DataFusion, model::EmbeddingModelStore};
use datafusion::sql::{
    TableReference,
    sqlparser::ast::{Expr, Ident},
};
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
