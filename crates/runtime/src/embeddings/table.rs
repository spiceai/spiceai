/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Constraints, Statistics};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use itertools::Itertools;
use llms::chunking::{Chunker, ChunkingConfig};
use snafu::prelude::*;

use tokio::sync::RwLock;

use crate::embeddings::execution_plan::EmbeddingTableExec;
use crate::model::EmbeddingModelStore;

#[derive(Debug, Snafu)]
pub enum Error {}

/// An [`EmbeddingTable`] is a [`TableProvider`] where some columns are augmented with associated embedding columns
#[derive(Clone)]
pub struct EmbeddingTable {
    base_table: Arc<dyn TableProvider>,

    // A mapping of columns names from [`base_table`] to the embedding's `name` to use.
    embedded_columns: HashMap<String, String>,

    embedding_models: Arc<RwLock<EmbeddingModelStore>>,

    // Precompute to avoid async lock waits from `embedding_models` data structure.
    // Mapping of column name to the expected size of its embedding.
    embedding_sizes: HashMap<String, i32>,

    // Column name -> how to chunk the text for the embedding model.
    // If None, no chunking is needed.
    embedding_chunkers: HashMap<String, Arc<dyn Chunker>>,
}

impl EmbeddingTable {
    pub async fn new(
        base_table: Arc<dyn TableProvider>,
        embedded_columns: HashMap<String, String>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
        embed_chunker_config: HashMap<String, ChunkingConfig>,
    ) -> Self {
        let sizes = Self::precompute_embedding_sizes(&embedded_columns, &embedding_models).await;
        let chunkers = Self::prepare_chunkers(
            embed_chunker_config,
            &embedding_models,
            embedded_columns.clone(),
        )
        .await;

        Self {
            base_table,
            embedded_columns,
            embedding_models,
            embedding_sizes: sizes,
            embedding_chunkers: chunkers,
        }
    }

    /// Get the names of the embedding models used by this table across its columns.
    #[must_use]
    pub fn get_embedding_models_used(&self) -> Vec<String> {
        self.embedded_columns.values().cloned().collect()
    }

    /// Get the names of the columns that are augmented with embeddings.
    #[must_use]
    pub fn get_embedding_columns(&self) -> Vec<String> {
        self.embedded_columns.keys().cloned().collect()
    }

    // Get the schema of the base table.
    #[must_use]
    pub fn get_base_table_schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    /// For a given set of columns that should be chunked (i.e. keys of `cfgs`), prepare the chunkers based on the column's embedding model in [`EmbeddingModelStore`].
    async fn prepare_chunkers(
        cfgs: HashMap<String, ChunkingConfig>,
        embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
        column_to_model: HashMap<String, String>,
    ) -> HashMap<String, Arc<dyn Chunker>> {
        let mut chunkers: HashMap<String, Arc<dyn Chunker>> = HashMap::new();

        for (col, chunk_cfg) in cfgs {
            let Some(model_name) = column_to_model.get(&col) else {
                tracing::debug!(
                    "No model specified for column '{}', skipping chunker setup.",
                    col
                );
                continue;
            };
            let embedding_models_guard = embedding_models.read().await;
            let Some(embed_model) = embedding_models_guard.get(model_name) else {
                // Don't need warn, as we should have already checked/logged this.
                tracing::debug!(
                    "Expected model '{}' for column '{}', but it was not found in the model store.",
                    model_name,
                    col
                );
                continue;
            };

            if let Some(chunker) = embed_model.chunker(chunk_cfg) {
                chunkers.insert(col, chunker);
            } else {
                tracing::warn!("Column '{}' expects to be chunked, but the model '{}' does not support chunking. Ignoring chunking config.", col, model_name);
            }
        }
        chunkers
    }

    async fn precompute_embedding_sizes(
        embedded_columns: &HashMap<String, String>,
        embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    ) -> HashMap<String, i32> {
        let mut model_sizes: HashMap<String, i32> = HashMap::new();
        for (col, model_name) in embedded_columns {
            if let Some(model) = embedding_models.read().await.get(model_name) {
                model_sizes.insert(col.clone(), model.size());
            }
        }
        model_sizes
    }

    /// For a given projection of the entire [`Schema`], find which [`Self::embedded_columns`] need to be computed.
    /// If `projection.is_none()`, all embedding columns are in projection, and therefore needed.
    ///
    /// Any project index (in `projection`) that is greater than the number of columns in the base
    /// table is an embedding column. The relation of underlying column to embedding column is, for example, as follows:
    ///
    /// | projection idx | 0 | 1 | 2 | 3 | 4 | 5 |      6      |      7      |
    /// |  column name   | A | B | C | D | E | F | `B_embedding` | `E_embedding` |
    ///
    ///     - 6 Base columns A, B, C, D, E, F
    ///     - 2 Embedding columns B_embedding, E_embedding
    ///     - Any projection index >=6 is an embedding column.
    ///
    /// The order of embedding columns in [`Self::Schema`] is alphabetical.
    fn columns_to_embed(&self, projection: Option<&Vec<usize>>) -> Vec<String> {
        match projection {
            None => self.embedded_columns.keys().cloned().collect_vec(),
            Some(column_idx) => {
                let base_cols = self.base_table.schema().fields.len();
                let x: Vec<_> = self.embedded_columns.keys().sorted().collect();

                // Order of embedding columns in [`Self::Schema`] is alphabetical.
                column_idx
                    .iter()
                    .filter_map(|&c| {
                        if c >= base_cols {
                            x.get(c - base_cols).copied()
                        } else {
                            None
                        }
                    })
                    .cloned()
                    .collect()
            }
        }
    }

    /// Get the appropriate [`DataType`] for the vectors associated with an embedding column.
    fn embedding_column_type(&self, embedding_column: &str) -> DataType {
        let embedding_size = self
            .embedding_sizes
            .get(embedding_column)
            .copied()
            .unwrap_or_default();

        let vector_type = DataType::new_fixed_size_list(DataType::Float32, embedding_size, false);

        if self.embedding_chunkers.contains_key(embedding_column) {
            DataType::new_list(vector_type, false)
        } else {
            vector_type
        }
    }
}

#[async_trait]
impl TableProvider for EmbeddingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    // fn get_table_definition(&self) -> Option<&str> {
    //     self.base_table.get_table_definition()
    // }

    // fn get_logical_plan(&self) -> Option<&LogicalPlan> {
    //     let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));
    //     let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
    //         .context(UnableToConstructLogicalPlanBuilderSnafu {})?
    //         .build()
    //         .context(UnableToBuildLogicalPlanSnafu {})?;

    //     self.base_table.get_logical_plan()
    // }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.base_table.get_column_default(column)
    }

    fn schema(&self) -> SchemaRef {
        let base_schema = self.base_table.schema();
        let mut base_fields: Vec<_> = (0..base_schema.fields.len())
            .filter_map(|i| base_schema.fields.get(i).cloned())
            .collect();

        let mut embedding_fields: Vec<_> = self
            .embedded_columns
            .keys()
            .sorted() // Important to be kept alphabetical for fast lookup
            .filter_map(|k| {
                base_schema.column_with_name(k).map(|(_, field)| {
                    Arc::new(
                        field
                            .clone()
                            .with_data_type(self.embedding_column_type(k))
                            .with_name(format!("{k}_embedding")),
                    )
                })
            })
            .collect();

        base_fields.append(&mut embedding_fields);

        Arc::new(Schema::new(base_fields))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let columns_to_embed = self.columns_to_embed(projection);
        let num_base_cols = self.base_table.schema().fields.len();

        // No embedding work is needed.
        if columns_to_embed.is_empty() {
            return self
                .base_table
                .scan(
                    state,
                    projection
                        .as_ref()
                        .map(|p| {
                            p.iter()
                                .filter(|&&idx| idx < num_base_cols)
                                .copied()
                                .collect()
                        })
                        .as_ref(),
                    filters,
                    limit,
                )
                .await;
        }

        let schema = &self.schema();
        let scan_embed_columns: HashMap<String, String> = self
            .embedded_columns
            .iter()
            .filter(|(c, _m)| columns_to_embed.contains(c))
            .map(|(c, m)| (c.clone(), m.clone()))
            .collect();

        // Need to ensure base table gets the underlying column for each embedding column specified (as well as everything in the original [`projection`]).
        let projection_for_base_table: Option<Vec<usize>> = match projection.cloned() {
            None => None,
            Some(mut proj) => {
                let mut base_cols = scan_embed_columns
                    .keys()
                    .filter_map(|c| schema.column_with_name(c).map(|(idx, _field)| idx))
                    .collect_vec();
                proj.append(&mut base_cols);
                Some(
                    proj.iter()
                        .unique()
                        .filter(|&&c| c < num_base_cols) // Don't include embedding columns for `base_table`
                        .copied()
                        .collect_vec(),
                )
            }
        };

        let projected_schema = project_schema(&self.schema(), projection)?;
        let base_plan = self
            .base_table
            .scan(state, projection_for_base_table.as_ref(), filters, limit)
            .await?;

        Ok(Arc::new(EmbeddingTableExec::new(
            &projected_schema,
            filters,
            limit,
            base_plan,
            scan_embed_columns,
            Arc::clone(&self.embedding_models),
            self.embedding_chunkers.clone(),
        )) as Arc<dyn ExecutionPlan>)
    }

    /// Any filter in [`filters`] can still be exact
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // TODO: Implement pushdown for filters that can be exact.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.insert_into(state, input, overwrite).await
    }
}
