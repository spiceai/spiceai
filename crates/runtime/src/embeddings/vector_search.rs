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

use std::{collections::HashMap, fmt::Display, sync::Arc};

use app::App;
use arrow::array::{RecordBatch, StringArray};
use arrow::error::ArrowError;
use async_openai::types::EmbeddingInput;
use datafusion::{common::Constraint, datasource::TableProvider, sql::TableReference};
use itertools::Itertools;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::{
    accelerated_table::AcceleratedTable, datafusion::DataFusion, model::EmbeddingModelStore,
};

use super::table::EmbeddingTable;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Data source {} does not exist", data_source))]
    DataSourceNotFound { data_source: String },

    #[snafu(display("Error occurred interacting with datafusion: {}", source))]
    DataFusionError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Data source {} does not contain any embedding columns", data_source))]
    NoEmbeddingColumns { data_source: String },

    #[snafu(display("Only one embedding column per table currently supported. Table: {data_source} has {num_embeddings} embeddings"))]
    IncorrectNumberOfEmbeddingColumns {
        data_source: String,
        num_embeddings: usize,
    },

    #[snafu(display("Embedding model {} not found", model_name))]
    EmbeddingModelNotFound { model_name: String },

    #[snafu(display("Error embedding input text: {}", source))]
    EmbeddingError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A Component that can perform vector search operations.
pub struct VectorSearch {
    pub df: Arc<DataFusion>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
    explicit_primary_keys: HashMap<TableReference, Vec<String>>,
}

pub enum RetrievalLimit {
    TopN(usize),
    Threshold(f64),
}
impl Display for RetrievalLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetrievalLimit::TopN(n) => write!(f, "TopN({n})"),
            RetrievalLimit::Threshold(t) => write!(f, "Threshold({t})"),
        }
    }
}

pub type ModelKey = String;

pub struct VectorSearchResult {
    pub retrieved_entries: HashMap<TableReference, Vec<String>>,
    pub retrieved_primary_keys: HashMap<TableReference, Vec<RecordBatch>>,
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

    /// Perform a single SQL query vector search.
    async fn individual_search(
        &self,
        tbl: &TableReference,
        primary_keys: &[String],
        embedding: Vec<f32>,
        embedding_column: &str,
        n: usize,
    ) -> Result<(Vec<std::string::String>, Vec<arrow::array::RecordBatch>)> {
        let projection = if primary_keys.is_empty() {
            embedding_column.to_string()
        } else {
            [primary_keys.join(", "), embedding_column.to_string()].join(", ")
        };

        let query = format!(
            "SELECT {projection} FROM {tbl} ORDER BY array_distance({embedding_column}_embedding, {embedding:?}) LIMIT {n}"
        );

        let batches = self
            .df
            .ctx
            .sql(&query)
            .await
            .boxed()
            .context(DataFusionSnafu)?
            .collect()
            .await
            .boxed()
            .context(DataFusionSnafu)?;

        // Extract the embedding column data
        let embedding_column_data: Vec<String> = batches
            .iter()
            .map(|batch| {
                batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| string_to_boxed_err(
                        format!(
                            "Expected '{embedding_column}' to be the last column of the SQL query and return a String type"
                        ),
                    ))
                    .context(DataFusionSnafu)
                    .map(|array| {
                        array
                            .iter()
                            .map(|value| value.unwrap_or_default().to_string())
                            .collect::<Vec<_>>()
                    })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        // Retrieve the primary key data
        let primary_key_data: Vec<RecordBatch> = batches
            .iter()
            .map(|batch| {
                let indices: Vec<usize> = batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, field)| {
                        if primary_keys.contains(field.name()) {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect();

                batch.project(&indices)
            })
            .collect::<Result<Vec<_>, ArrowError>>()
            .boxed()
            .context(DataFusionSnafu)?;

        Ok((embedding_column_data, primary_key_data))
    }

    pub async fn search(
        &self,
        query: String,
        tables: Vec<TableReference>,
        limit: RetrievalLimit,
    ) -> Result<VectorSearchResult> {
        let span = match Span::current() {
            span if matches!(span.metadata(), Some(metadata) if metadata.name() == "vector_search") => {
                span
            }
            _ => {
                tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = query)
            }
        };
        span.in_scope(|| {
            tracing::info!(name: "labels", target: "task_history", tables = tables.iter().join(","), limit = %limit);
        });

        let n = match limit {
            RetrievalLimit::TopN(n) => n,
            RetrievalLimit::Threshold(_) => unimplemented!(),
        };

        let per_table_embeddings = self
            .calculate_embeddings_per_table(query.clone(), tables.clone())
            .instrument(span.clone())
            .await?;

        let table_primary_keys = self
            .get_primary_keys_with_overrides(&self.explicit_primary_keys, tables.clone())
            .instrument(span.clone())
            .await?;

        let mut response = VectorSearchResult {
            retrieved_entries: HashMap::new(),
            retrieved_primary_keys: HashMap::new(),
        };

        for (tbl, search_vectors) in per_table_embeddings {
            tracing::debug!("Running vector search for table {:#?}", tbl.clone());
            let primary_keys = table_primary_keys.get(&tbl).cloned().unwrap_or(vec![]);

            // Only support one embedding column per table.
            let table_provider = self
                .df
                .get_table(tbl.clone())
                .instrument(span.clone())
                .await
                .ok_or(Error::DataSourceNotFound {
                    data_source: tbl.to_string(),
                })?;

            let embedding_column = get_embedding_table(&table_provider)
                .and_then(|e| e.get_embedding_columns().first().cloned())
                .ok_or(Error::NoEmbeddingColumns {
                    data_source: tbl.to_string(),
                })?;

            if search_vectors.len() != 1 {
                return Err(Error::IncorrectNumberOfEmbeddingColumns {
                    data_source: tbl.to_string(),
                    num_embeddings: search_vectors.len(),
                });
            }
            match search_vectors.first() {
                None => unreachable!(),
                Some(embedding) => {
                    let (outtt, primary_key_data) = self
                        .individual_search(
                            &tbl,
                            &primary_keys,
                            embedding.clone(),
                            &embedding_column,
                            n,
                        )
                        .instrument(span.clone())
                        .await?;
                    response.retrieved_entries.insert(tbl.clone(), outtt);
                    response
                        .retrieved_primary_keys
                        .insert(tbl, primary_key_data);
                }
            };
        }
        tracing::debug!(
            "Relevant data from vector search: {:#?}",
            response.retrieved_entries,
        );
        Ok(response)
    }

    /// For the data sources that assumedly exist in the [`DataFusion`] instance, find the embedding models used in each data source.
    async fn find_relevant_embedding_models(
        &self,
        data_sources: Vec<TableReference>,
    ) -> Result<HashMap<TableReference, Vec<ModelKey>>> {
        let mut embeddings_to_run = HashMap::new();
        for data_source in data_sources {
            let table =
                self.df
                    .get_table(data_source.clone())
                    .await
                    .context(DataSourceNotFoundSnafu {
                        data_source: data_source.to_string(),
                    })?;

            let embedding_models = get_embedding_table(&table)
                .context(NoEmbeddingColumnsSnafu {
                    data_source: data_source.to_string(),
                })?
                .get_embedding_models_used();
            embeddings_to_run.insert(data_source, embedding_models);
        }
        Ok(embeddings_to_run)
    }

    async fn get_primary_keys(&self, table: &TableReference) -> Result<Vec<String>> {
        let tbl_ref = self
            .df
            .get_table(table.clone())
            .await
            .context(DataSourceNotFoundSnafu {
                data_source: table.to_string(),
            })?;

        let constraint_idx = tbl_ref
            .constraints()
            .map(|c| c.iter())
            .unwrap_or_default()
            .find_map(|c| match c {
                Constraint::PrimaryKey(columns) => Some(columns),
                Constraint::Unique(_) => None,
            })
            .cloned()
            .unwrap_or(Vec::new());

        tbl_ref
            .schema()
            .project(&constraint_idx)
            .map(|schema_projection| {
                schema_projection
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            })
            .boxed()
            .context(DataFusionSnafu)
    }

    /// For a set of tables, get their primary keys. Attempt to determine the primary key(s) of the
    /// table from the [`TableProvider`] constraints, and if not provided, use the explicit primary
    /// keys defined in the spicepod configuration.
    async fn get_primary_keys_with_overrides(
        &self,
        explicit_primary_keys: &HashMap<TableReference, Vec<String>>,
        tables: Vec<TableReference>,
    ) -> Result<HashMap<TableReference, Vec<String>>> {
        let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

        for tbl in tables {
            let pks = self.get_primary_keys(&tbl).await?;
            if !pks.is_empty() {
                tbl_to_pks.insert(tbl.clone(), pks);
            } else if let Some(explicit_pks) = explicit_primary_keys.get(&tbl) {
                tbl_to_pks.insert(tbl.clone(), explicit_pks.clone());
            }
        }
        Ok(tbl_to_pks)
    }

    /// Embed the input text using the specified embedding model.
    async fn embed(&self, input: &str, embedding_model: &str) -> Result<Vec<f32>> {
        self.embeddings
            .read()
            .await
            .iter()
            .find_map(|(name, model)| {
                if name.clone() == embedding_model {
                    Some(model)
                } else {
                    None
                }
            })
            .ok_or(Error::EmbeddingModelNotFound {
                model_name: embedding_model.to_string(),
            })?
            .write()
            .await
            .embed(EmbeddingInput::String(input.to_string()))
            .await
            .boxed()
            .context(EmbeddingSnafu)?
            .first()
            .cloned()
            .ok_or(Error::EmbeddingError {
                source: string_to_boxed_err(format!(
                    "No embeddings returned for input text from {embedding_model}"
                )),
            })
    }

    /// For each embedding column that a [`TableReference`] contains, calculate the embeddings vector between the query and the column.
    /// The returned `HashMap` is a mapping of [`TableReference`] to an (alphabetical by column name) in-order vector of embeddings.
    async fn calculate_embeddings_per_table(
        &self,
        query: String,
        data_sources: Vec<TableReference>,
    ) -> Result<HashMap<TableReference, Vec<Vec<f32>>>> {
        // Determine which embedding models need to be run. If a table does not have an embedded column, return an error.
        let embeddings_to_run: HashMap<TableReference, Vec<ModelKey>> =
            self.find_relevant_embedding_models(data_sources).await?;

        // Create embedding(s) for question/statement. `embedded_inputs` model_name -> embedding.
        let mut embedded_inputs: HashMap<ModelKey, Vec<f32>> = HashMap::new();
        for model in embeddings_to_run.values().flatten() {
            let result = self
                .embed(&query, model)
                .await
                .boxed()
                .context(EmbeddingSnafu)?;
            embedded_inputs.insert(model.clone(), result);
        }

        Ok(embeddings_to_run
            .iter()
            .map(|(t, model_names)| {
                let z: Vec<_> = model_names
                    .iter()
                    .filter_map(|m| embedded_inputs.get(m).cloned())
                    .collect();
                (t.clone(), z)
            })
            .collect())
    }
}

/// If a [`TableProvider`] is an [`EmbeddingTable`], return the [`EmbeddingTable`].
/// This includes if the [`TableProvider`] is an [`AcceleratedTable`] with a [`EmbeddingTable`] underneath.
fn get_embedding_table(tbl: &Arc<dyn TableProvider>) -> Option<Arc<EmbeddingTable>> {
    if let Some(embedding_table) = tbl.as_any().downcast_ref::<EmbeddingTable>() {
        return Some(Arc::new(embedding_table.clone()));
    }
    if let Some(accelerated_table) = tbl.as_any().downcast_ref::<AcceleratedTable>() {
        if let Some(embedding_table) = accelerated_table
            .get_federated_table()
            .as_any()
            .downcast_ref::<EmbeddingTable>()
        {
            return Some(Arc::new(embedding_table.clone()));
        }
    }
    None
}

fn string_to_boxed_err(s: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(s)
}

/// Compute the primary keys for each table in the app. Primary Keys can be explicitly defined in the Spicepod.yaml
pub async fn compute_primary_keys(
    app: Arc<RwLock<Option<App>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                d.embeddings
                    .iter()
                    .find_map(|e| e.primary_keys.clone())
                    .map(|pks| (TableReference::parse_str(&d.name), pks))
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}
