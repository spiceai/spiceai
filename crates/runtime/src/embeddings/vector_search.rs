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

use std::iter::zip;
use std::{collections::HashMap, fmt::Display, sync::Arc};

use app::App;
use arrow::array::{RecordBatch, StringArray};
use arrow::error::ArrowError;
use async_openai::types::EmbeddingInput;
use datafusion::common::utils::quote_identifier;
use datafusion::{common::Constraint, datasource::TableProvider, sql::TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::query::write_to_json_string;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::{datafusion::DataFusion, model::EmbeddingModelStore};

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

    #[snafu(display("Error occurred processing Arrow records: {}", source))]
    RecordProcessingError { source: ArrowError },

    #[snafu(display("Could not format search results: {}", source))]
    FormattingError {
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

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[allow(clippy::doc_markdown)]
pub struct SearchRequest {
    /// The text to search documents for similarity
    pub text: String,

    /// The datasets to search for similarity. For available datasets, use the 'list_datasets' tool and ensure `can_search_documents==true`.
    #[serde(default)]
    pub datasets: Vec<String>,

    /// Number of documents to return for each dataset
    #[serde(default = "default_limit")]
    pub limit: usize,

    /// An SQL filter predicate to apply. Format: 'WHERE <where_cond>'.
    #[serde(rename = "where", default)]
    pub where_cond: Option<String>,

    /// Additional columns to return from the dataset.
    #[serde(default)]
    pub additional_columns: Vec<String>,
}

fn default_limit() -> usize {
    3
}

impl SearchRequest {
    #[must_use]
    pub fn new(
        text: String,
        data_source: Vec<String>,
        limit: usize,
        where_cond: Option<String>,
        additional_columns: Vec<String>,
    ) -> Self {
        SearchRequest {
            text,
            datasets: data_source,
            limit,
            where_cond,
            additional_columns,
        }
    }
}

pub type ModelKey = String;

#[derive(Debug)]
pub struct VectorSearchTableResult {
    pub primary_key: Vec<RecordBatch>,
    pub embedded_column: Vec<RecordBatch>, // original data, not the embedding vector.
    pub additional_columns: Vec<RecordBatch>,
}

pub type VectorSearchResult = HashMap<TableReference, VectorSearchTableResult>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Match {
    value: String,
    // score: f64,
    dataset: String,
    primary_key: HashMap<String, serde_json::Value>,
    metadata: HashMap<String, serde_json::Value>,
}

pub fn table_to_matches(
    tbl: &TableReference,
    result: &VectorSearchTableResult,
) -> Result<Vec<Match>> {
    let pks: Vec<HashMap<String, serde_json::Value>> =
        if result.primary_key.first().is_some_and(|p| p.num_rows() > 0) {
            let pk_str = write_to_json_string(&result.primary_key).context(FormattingSnafu)?;
            serde_json::from_str(&pk_str)
                .boxed()
                .context(FormattingSnafu)?
        } else {
            vec![]
        };

    let add_cols: Vec<HashMap<String, serde_json::Value>> = if result
        .additional_columns
        .first()
        .is_some_and(|p| p.num_rows() > 0)
    {
        let col_str = write_to_json_string(&result.additional_columns).context(FormattingSnafu)?;
        serde_json::from_str(&col_str)
            .boxed()
            .context(FormattingSnafu)?
    } else {
        vec![]
    };

    let values: Vec<String> = result
        .embedded_column
        .iter()
        .flat_map(|v| {
            if let Some(col) = v.column(0).as_any().downcast_ref::<StringArray>() {
                col.iter()
                    .map(|v| v.unwrap_or_default().to_string())
                    .collect::<Vec<String>>()
            } else {
                vec![]
            }
        })
        .collect();

    Ok(zip(zip(pks, add_cols), values)
        .map(|((pks, add_cols), value)| Match {
            value,
            dataset: tbl.to_string(),
            primary_key: pks,
            metadata: add_cols,
        })
        .collect::<Vec<Match>>())
}

pub fn to_matches(result: &VectorSearchResult) -> Result<Vec<Match>> {
    let output = result
        .iter()
        .map(|(a, b)| table_to_matches(a, b))
        .collect::<Result<Vec<_>>>()?;

    Ok(output.into_iter().flatten().collect_vec())
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
    #[allow(clippy::too_many_arguments)]
    async fn individual_search(
        &self,
        tbl: &TableReference,
        embedding: Vec<f32>,
        primary_keys: &[String],
        embedding_column: &str,
        additional_columns: &[String],
        where_cond: Option<&str>,
        n: usize,
    ) -> Result<VectorSearchTableResult> {
        let projection: Vec<String> = primary_keys
            .iter()
            .cloned()
            .chain(Some(embedding_column.to_string()))
            .chain(additional_columns.iter().cloned())
            .collect();

        let projection_str = projection.iter().map(|s| quote_identifier(s)).join(", ");

        let where_str = where_cond.map_or_else(String::new, |cond| format!("WHERE ({cond})"));

        let query = format!(
            "SELECT {projection_str} FROM {tbl} {where_str} ORDER BY array_distance({embedding_column}_embedding, {embedding:?}) LIMIT {n}"
        );
        tracing::trace!("running SQL: {query}");

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

        let primary_key_projection = (0..primary_keys.len()).collect_vec();
        let embedding_projection = (primary_keys.len()..=primary_keys.len()).collect_vec();
        let additional_columns_projection =
            (primary_keys.len() + 1..projection.len()).collect_vec();

        let primary_keys_records = batches
            .iter()
            .map(|s| s.project(&primary_key_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;
        let embedding_records = batches
            .iter()
            .map(|s| s.project(&embedding_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;
        let primary_keys = batches
            .iter()
            .map(|s| s.project(&additional_columns_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        Ok(VectorSearchTableResult {
            primary_key: primary_keys_records,
            embedded_column: embedding_records,
            additional_columns: primary_keys,
        })
    }

    pub async fn search(&self, req: &SearchRequest) -> Result<VectorSearchResult> {
        let SearchRequest {
            text: query,
            datasets: data_source,
            limit,
            where_cond,
            additional_columns,
        } = req;

        let tables: Vec<TableReference> = data_source.iter().map(TableReference::from).collect();

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

        let per_table_embeddings = self
            .calculate_embeddings_per_table(query.clone(), tables.clone())
            .instrument(span.clone())
            .await?;

        let table_primary_keys = self
            .get_primary_keys_with_overrides(&self.explicit_primary_keys, tables.clone())
            .instrument(span.clone())
            .await?;

        let mut response: VectorSearchResult = HashMap::new();

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
                    let result = self
                        .individual_search(
                            &tbl,
                            embedding.clone(),
                            &primary_keys,
                            &embedding_column,
                            additional_columns,
                            where_cond.as_deref(),
                            *limit,
                        )
                        .instrument(span.clone())
                        .await?;
                    response.insert(tbl.clone(), result);
                }
            };
        }
        span.in_scope(|| {
            tracing::info!(target: "task_history", truncated_output = ?response);
        });
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

    let tbl = if let Some(adaptor) = tbl.as_any().downcast_ref::<FederatedTableProviderAdaptor>() {
        adaptor.table_provider.clone()?
    } else {
        Arc::clone(tbl)
    };

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
pub async fn parse_explicit_primary_keys(
    app: Arc<RwLock<Option<Arc<App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                d.embeddings
                    .iter()
                    .find_map(|e| e.primary_keys.clone())
                    .map(|pks| {
                        (
                            TableReference::parse_str(&d.name)
                                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                                .into(),
                            pks,
                        )
                    })
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use schemars::schema_for;
    use snafu::ResultExt;

    use crate::embeddings::vector_search::SearchRequest;

    #[tokio::test]
    async fn test_search_request_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        serde_json::to_value(schema_for!(SearchRequest)).boxed()?;
        Ok(())
    }
}
