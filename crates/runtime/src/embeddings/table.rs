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
use datafusion::common::{project_schema, Constraints, Statistics};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use itertools::Itertools;
use llms::embeddings::Embed;
use snafu::prelude::*;

use tokio::sync::RwLock;

use crate::embeddings::execution_plan::EmbeddingTableExec;

#[derive(Debug, Snafu)]
pub enum Error {}

/// An [`EmbeddingTable`] is a [`TableProvider`] where some columns are augmented with associated embedding columns
pub struct EmbeddingTable {
    dataset_name: TableReference,
    base_table: Arc<dyn TableProvider>,

    // A mapping of columns names from [`base_table`] to the embedding's `name` to use.
    embedded_columns: HashMap<String, String>,

    embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,

    // Precompute to avoid async lock waits from `embedding_models` data structure.
    // Mapping of column name to the expected size of its embedding.
    embedding_sizes: HashMap<String, usize>,

    num_base_cols: usize,
}

impl EmbeddingTable {
    pub async fn new(
        dataset_name: TableReference,
        base_table: Arc<dyn TableProvider>,
        embedded_columns: HashMap<String, String>,
        embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
    ) -> Self {
        let sizes = Self::precompute_embedding_sizes(&embedded_columns, &embedding_models).await;
        println!("Sizes: {:#?}", sizes);
        let num_base_cols = base_table.schema().all_fields().len();
        Self {
            dataset_name,
            base_table,
            embedded_columns,
            embedding_models,
            embedding_sizes: sizes,
            num_base_cols,
        }
    }

    async fn precompute_embedding_sizes(
        embedded_columns: &HashMap<String, String>,
        embedding_models: &Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
    ) -> HashMap<String, usize> {
        println!("Precomputing embedding sizes for {:#?}", embedded_columns);

        let mut model_sizes: HashMap<String, usize> = HashMap::new();
        for (col, model) in embedded_columns.iter() {
            if let Some(model_lock) = embedding_models.read().await.get(model) {
                let z = model_lock.read().await.size();
                model_sizes.insert(col.clone(), z);
                println!("Model {} has size {}", model, z);
            } else {
                println!("Model {} not found for column {}", model, col);
            }
        }
        model_sizes
    }

    /// For a given projection of the entire [`Schema`], find which [`Self::embedded_columns`] need to be computed.
    /// If `projection.is_none()`, all embedding columns are in projection, and therefore needed.
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
                        if c > base_cols {
                            None
                        } else {
                            x.get(c - base_cols).cloned()
                        }
                    })
                    .cloned()
                    .collect()
            }
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

    fn get_table_definition(&self) -> Option<&str> {
        self.base_table.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        self.base_table.get_logical_plan()
    }
    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        self.base_table.get_column_default(_column)
    }

    fn schema(&self) -> SchemaRef {
        let base_schema = self.base_table.schema();
        let mut base_fields: Vec<_> = (0..base_schema.fields.len())
            .into_iter()
            .map(|i| base_schema.fields.get(i).cloned())
            .flatten()
            .collect();

        let mut embedding_fields: Vec<_> = self
            .embedded_columns
            .keys()
            .sorted() // Important to be kept alphabetical for fast lookup
            .map(|k| match base_schema.column_with_name(k) {
                Some((_, field)) => {
                    let embedding_size = self
                        .embedding_sizes
                        .get(field.name())
                        .map(|&x| x as i32)
                        .unwrap_or_default();

                    Some(Arc::new(
                        field
                            .clone()
                            .with_data_type(DataType::new_fixed_size_list(
                                DataType::Float32,
                                embedding_size,
                                false,
                            ))
                            .with_name(format!("{}_embedding", field.name())),
                    ))
                }
                None => None,
            })
            .flatten()
            .collect();

        base_fields.append(&mut embedding_fields);

        Arc::new(Schema::new(base_fields))
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        println!(
            "You are indeed in an embeddingtable for {}. Embedding columns are {:#?}.",
            self.dataset_name, self.embedded_columns
        );
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
                                .cloned()
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
                        .cloned()
                        .collect_vec(),
                )
            }
        };

        println!(
            "What im putting into the base table: {:#?}",
            projection_for_base_table
        );

        let projected_schema = project_schema(&self.schema(), projection)?;
        let base_plan = self
            .base_table
            .scan(state, projection_for_base_table.as_ref(), filters, limit)
            .await?;

        println!(
            "in scan. models: {:#?}",
            self.embedding_models.read().await.keys().join(", ")
        );
        Ok(Arc::new(EmbeddingTableExec::new(
            projected_schema,
            filters,
            limit,
            base_plan,
            scan_embed_columns,
            self.embedding_models.clone(),
        )) as Arc<dyn ExecutionPlan>)
    }

    /// Any filter in [`filters`] can still be exact
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // let base_supports = self.base_table.supports_filters_pushdown(filters)?;
        // let embed_keys = HashSet::from_iter(self.embedded_columns.keys().map(|x| format!("{x}_embedding")));

        // Ok(filters.iter().enumerate().map(|(i, &f)| {
        //     let is_standard = f.to_columns().map(|c| c.intersection(embed_keys).count() == 0).ok();
        //     if let Some(true) = is_standard {
        //         return base_supports[i];
        //     }
        //     TableProviderFilterPushDown::Unsupported
        // }).collect_vec())
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
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.insert_into(state, input, overwrite).await
    }
}
