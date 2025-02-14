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

use std::{collections::HashMap, fmt::Display, sync::Arc};

use app::App;
use arrow::array::{LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow::error::ArrowError;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{Schema, SchemaRef};
use async_openai::types::EmbeddingInput;
use datafusion::common::utils::quote_identifier;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{Expr, SelectItem, TableFactor, TableWithJoins};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion::{common::Constraint, datasource::TableProvider, sql::TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use futures::TryStreamExt;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::query::write_to_json_string;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::{convert_string_arrow_to_iterator, embedding_col, offset_col};
use crate::{datafusion::DataFusion, model::EmbeddingModelStore};

use super::table::EmbeddingTable;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Data sources [{}] does not exist", data_source.iter().map(TableReference::to_quoted_string).join(", ")))]
    DataSourcesNotFound { data_source: Vec<TableReference> },

    #[snafu(display("Failed to find table '{}'. An internal error occurred during vector search.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues", table.to_quoted_string()))]
    DataSourceNotFound { table: TableReference },

    #[snafu(display("Vector search failed: No tables with embeddings are available. Ensure embeddings are configured and try again."))]
    NoTablesWithEmbeddingsFound {},

    #[snafu(display("Vector search cannot be run on {}.", data_source.to_quoted_string()))]
    CannotVectorSearchDataset { data_source: TableReference },

    #[snafu(display("Error occurred interacting with datafusion: {source}"))]
    DataFusionError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error occurred processing Arrow records: {source}"))]
    RecordProcessingError { source: ArrowError },

    #[snafu(display("Could not format search results: {source}"))]
    FormattingError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Data source {} does not contain any embedding columns", data_source.to_string()))]
    NoEmbeddingColumns { data_source: TableReference },

    #[snafu(display("Only one embedding column per table currently supported. Table: {} has {num_embeddings} embeddings", data_source.to_string()))]
    IncorrectNumberOfEmbeddingColumns {
        data_source: TableReference,
        num_embeddings: usize,
    },

    #[snafu(display("Embedding model {model_name} not found"))]
    EmbeddingModelNotFound { model_name: String },

    #[snafu(display("Error embedding input text: {source}"))]
    EmbeddingError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid WHERE condition: {where_cond}"))]
    InvalidWhereCondition { where_cond: String },

    #[snafu(display("An invalid keyword was specified: {keyword}"))]
    InvalidKeyword { keyword: String },

    #[snafu(display("Invalid additional column was specified: {additional_column}"))]
    InvalidAdditionalColumns { additional_column: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A Component that can perform vector search operations.
pub struct VectorSearch {
    pub df: Arc<DataFusion>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,

    // For tables, explicitly defined primary keys for datasets.
    // Are in [`ResolvedTableReference`] format.
    // Before use, must be resolved with spice defaults, `.resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)`.
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

static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestBaseJson {
    /// The text to search documents for similarity
    pub text: String,

    /// The datasets to search for similarity. If None, search across all datasets. For available datasets, use the `list_datasets` tool and ensure `can_search_documents==true`.
    #[serde(default)]
    pub datasets: Option<Vec<String>>,

    /// Number of documents to return for each dataset
    #[serde(default)]
    pub limit: Option<usize>,

    /// An SQL filter predicate to apply. Format: 'WHERE `where_cond`'.
    #[serde(rename = "where", default)]
    pub where_cond: Option<String>,

    /// Additional columns to return from the dataset.
    #[serde(default)]
    pub additional_columns: Vec<String>,
}

/// HTTP request schema is separate from AI requests, so that keywords can be supplied as an optional field for HTTP calls.
/// `schemars` doesn't allow setting `#[serde(default)]` as well as `#[schemars(required)]` - the field does not become required.
/// When the field is not required, the model ignores it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestHTTPJson {
    #[serde(flatten)]
    pub base: SearchRequestBaseJson,

    // A list of optional keywords, to pre-filter on the embedding column in SQL before performing the vector search.
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestAIJson {
    #[serde(flatten)]
    pub base: SearchRequestBaseJson,

    /// At least one keyword should be supplied for a vector search. Keywords should be individual words.
    /// Keywords are used to pre-filter the embedding column, applied as a `WHERE col LIKE '%keyword%'` condition.
    /// Keywords should not contain column names, special characters, or other operators.
    pub keywords: Vec<String>,
}

impl From<SearchRequestHTTPJson> for SearchRequestAIJson {
    fn from(req: SearchRequestHTTPJson) -> Self {
        SearchRequestAIJson {
            base: req.base,
            keywords: req.keywords.unwrap_or_default(),
        }
    }
}

impl TryFrom<SearchRequestAIJson> for SearchRequest {
    type Error = String;

    fn try_from(req: SearchRequestAIJson) -> Result<Self, Self::Error> {
        Ok(SearchRequest::new(
            req.base.text,
            req.base.datasets,
            req.base.limit.unwrap_or(default_limit()),
            req.base
                .where_cond
                .map(|r| SearchRequest::parse_where_cond(r).map_err(|e| e.to_string()))
                .transpose()?,
            SearchRequest::parse_additional_columns(&req.base.additional_columns)
                .map_err(|e| e.to_string())?,
            SearchRequest::parse_keywords(&req.keywords).map_err(|e| e.to_string())?,
        ))
    }
}

#[allow(clippy::doc_markdown)]
pub struct SearchRequest {
    /// The text to search documents for similarity
    pub text: String,

    /// The datasets to search for similarity. If None, search across all datasets. For available datasets, use the 'list_datasets' tool and ensure `can_search_documents==true`.
    pub datasets: Option<Vec<String>>,

    /// Number of documents to return for each dataset
    pub limit: usize,

    /// An SQL filter predicate to apply. Format: 'WHERE <where_cond>'.
    pub where_cond: Option<Expr>,

    /// Additional columns to return from the dataset.
    pub additional_columns: Vec<String>,

    /// Keywords to perform a lexical search and pre-filter the embedding column.
    pub keywords: Vec<String>,
}

#[must_use]
fn default_limit() -> usize {
    3
}

impl SearchRequest {
    /// Create new [`SearchRequest`].
    ///
    /// [`where_cond`] should already be sanitized. For raw WHERE conditions,
    /// use [`TryFrom<SearchRequestJson>`].
    #[must_use]
    pub fn new(
        text: String,
        datasets: Option<Vec<String>>,
        limit: usize,
        where_cond: Option<Expr>,
        additional_columns: Vec<String>,
        keywords: Vec<String>,
    ) -> Self {
        SearchRequest {
            text,
            datasets,
            limit,
            where_cond,
            additional_columns,
            keywords,
        }
    }

    pub fn parse_where_cond(where_cond: String) -> Result<Expr> {
        let parser = Parser::new(&GenericDialect);
        let mut parser = parser.try_with_sql(&where_cond).map_err(|_| {
            InvalidWhereConditionSnafu {
                where_cond: where_cond.clone(),
            }
            .build()
        })?;

        // Parse the WHERE keyword if its there, otherwise ignore it.
        let _ = parser.parse_keyword(Keyword::WHERE);

        // Parse the expression after the WHERE keyword.
        let expr = parser.parse_expr().map_err(|_| {
            InvalidWhereConditionSnafu {
                where_cond: where_cond.clone(),
            }
            .build()
        })?;

        // Ensure the WHERE clause is the last token.
        let next_token = parser.next_token();
        if next_token != Token::EOF {
            return Err(InvalidWhereConditionSnafu { where_cond }.build());
        }

        Ok(expr)
    }

    pub fn parse_additional_columns(additional_columns: &[String]) -> Result<Vec<String>> {
        additional_columns
            .iter()
            .map(|c| {
                let select_statement = format!("{c} FROM testing");
                let parser = Parser::new(&GenericDialect);
                let mut parser = parser.try_with_sql(&select_statement).map_err(|err| {
                    tracing::trace!("vector_search additional column parsing failed. {err}");
                    InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build()
                })?;

                // parse the SELECT
                let expr = parser.parse_select().map_err(|err| {
                    tracing::trace!("vector_search additional column parsing failed. {err}");
                    InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build()
                })?;

                if expr.projection.len() > 1 || expr.from.len() > 1 {
                    tracing::trace!("vector_search additional column parsing failed. expected 1 projection and 1 table, but got {expr:?}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                }

                let Some(SelectItem::UnnamedExpr(Expr::Identifier(sqlparser::ast::Ident {
                    value,
                    ..
                }))) = expr.projection.first()
                else {
                    tracing::trace!("vector_search additional column parsing failed. expected an identifier, but got {expr:?}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                };

                if value != c {
                    tracing::trace!("vector_search additional column parsing failed. expected {c}, but got {value}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                }

                let Some(TableWithJoins { relation, .. }) = expr.from.first() else {
                    tracing::trace!("vector_search additional column parsing failed. expected a table, but got {expr:?}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                };

                let TableFactor::Table { name, .. } = relation else {
                    tracing::trace!("vector_search additional column parsing failed. expected a table, but got {relation:?}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                };

                if name.to_string() != "testing" {
                    tracing::trace!("vector_search additional column parsing failed. expected 'testing', but got {name}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                }

                let next_token = parser.next_token();
                if next_token != Token::EOF {
                    tracing::trace!("vector_search additional column parsing failed. expected EOF, but got {next_token:?}");
                    return Err(InvalidAdditionalColumnsSnafu {
                        additional_column: c.clone(),
                    }
                    .build());
                }

                Ok(c.clone())
            })
            .collect::<Result<Vec<String>>>()
    }

    pub fn parse_keywords(keywords: &[String]) -> Result<Vec<String>> {
        keywords
            .iter()
            .map(|k| {
                let expression = format!("target_column ILIKE \"%{}%\"", k.to_lowercase()); // emulate the use of the keyword in the query.
                let parser = Parser::new(&GenericDialect);
                let mut parser = parser.try_with_sql(&expression).map_err(|err| {
                    tracing::trace!("vector_search keyword parsing failed. {err}");
                    InvalidKeywordSnafu {
                        keyword: k.clone(),
                    }
                    .build()
                })?;

                // The keyword will exist on its own if nothing else is present.
                let expr = parser.parse_expr().map_err(|err| {
                    tracing::trace!("vector_search keyword parsing failed. {err}");
                    InvalidKeywordSnafu {
                        keyword: k.clone(),
                    }
                    .build()
                })?;

                let Expr::ILike { expr, pattern, .. } = expr else {
                    tracing::trace!("vector_search keyword parsing failed. expected ILIKE, but got {expr:?}");
                    return Err(InvalidKeywordSnafu { keyword: k.clone() }.build());
                };

                if let (Expr::Identifier(id), Expr::Identifier(v)) = (*expr.clone(), *pattern.clone()) {
                    if id.value.to_lowercase() != "target_column" {
                        tracing::trace!("vector_search keyword parsing failed. expected 'target_column', but got {}", id.value);
                        return Err(InvalidKeywordSnafu { keyword: k.clone() }.build());
                    }

                    if v.value.to_lowercase() != format!("%{}%", k.to_lowercase()) {
                        tracing::trace!("vector_search keyword parsing failed. expected '%{}%', but got {}", k.to_lowercase(), v.value);
                        return Err(InvalidKeywordSnafu { keyword: k.clone() }.build());
                    }
                } else {
                    tracing::trace!("vector_search keyword parsing failed. expected identifiers, but got {expr:?} - {pattern:?}");
                    return Err(InvalidKeywordSnafu { keyword: k.clone() }.build());
                };

                // Ensure the expression is the last token.
                let next_token = parser.next_token();
                if next_token != Token::EOF {
                    tracing::trace!("vector_search keyword parsing failed. expected EOF, but got {next_token:?}");
                    return Err(InvalidKeywordSnafu { keyword: k.clone() }.build());
                }

                Ok(k.clone())
            })
            .collect::<Result<Vec<String>>>()
    }
}

pub type ModelKey = String;

#[derive(Debug, Default)]
pub struct VectorSearchTableResult {
    pub data: Vec<RecordBatch>,

    pub primary_keys: Vec<String>,
    // original data, not the embedding vector.
    pub embedding_column: String,
    pub additional_columns: Vec<String>,
}

impl VectorSearchTableResult {
    /// Return the underlying [`RecordBatch`]s as a pretty formatted table.
    pub fn to_pretty(&self) -> Result<impl Display, ArrowError> {
        pretty_format_batches(&self.data)
    }

    /// Return the primary keys of the [`VectorSearch::individual_search`] as an array of JSON objects.
    ///
    /// Each element is a mapping of the primary key column to its value.
    pub fn primary_keys_json(&self) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let primary_key_projection = get_projection(&self.schema(), &self.primary_keys);
        let primary_keys_records = self
            .data
            .iter()
            .map(|s| s.project(&primary_key_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        if primary_keys_records
            .first()
            .is_some_and(|p| p.num_rows() > 0)
        {
            let pk_str = write_to_json_string(&primary_keys_records).context(FormattingSnafu)?;
            serde_json::from_str(&pk_str)
                .boxed()
                .context(FormattingSnafu)
        } else {
            Ok(vec![])
        }
    }

    /// Return the additional columns of the [`VectorSearch::individual_search`] as an array of JSON objects.
    ///
    /// Each element is a mapping of the additional column name to its value.
    pub fn addition_columns_json(&self) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let additional_columns_projection =
            get_projection(&self.schema(), &self.additional_columns);
        let additional_columns_records = self
            .data
            .iter()
            .map(|s| s.project(&additional_columns_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        if additional_columns_records
            .first()
            .is_some_and(|p| p.num_rows() > 0)
        {
            let additional_str =
                write_to_json_string(&additional_columns_records).context(FormattingSnafu)?;
            serde_json::from_str(additional_str.as_str())
                .boxed()
                .context(FormattingSnafu)
        } else {
            Ok(vec![])
        }
    }

    /// Return the distance of each search result.
    pub fn distance_values(&self) -> Result<Vec<f64>> {
        let Some(distances) = self
            .data
            .iter()
            .map(|s| s.column_by_name(VECTOR_DISTANCE_COLUMN_NAME).cloned())
            .collect::<Option<Vec<_>>>()
        else {
            return Err(Error::EmbeddingError {
                source: "No distances returned".into(),
            });
        };

        let distances: Option<Vec<_>> = distances
            .iter()
            .flat_map(|v| {
                if let Some(col) = v.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    col.iter().collect::<Vec<Option<f64>>>()
                } else {
                    vec![]
                }
            })
            .collect();
        let Some(distances) = distances else {
            return Err(Error::EmbeddingError {
                source: "Empty embedding distances returned unexpectedly".into(),
            });
        };

        Ok(distances)
    }

    /// Return the input column that was embedded.
    pub fn embedding_columns_list(&self) -> Result<Vec<String>> {
        let embedding_projection = get_projection(&self.schema(), &[self.embedding_column.clone()]);
        let embedding_records = self
            .data
            .iter()
            .map(|s| s.project(&embedding_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        let result = embedding_records
            .iter()
            .flat_map(|v| {
                convert_string_arrow_to_iterator!(v.column(0))
                    .map(|v| v.map(|vv| vv.unwrap_or_default().to_string()).collect_vec())
                    .unwrap_or_default()
            })
            .collect();

        Ok(result)
    }

    /// Retuns the Schema of the full underlying data.
    pub fn schema(&self) -> SchemaRef {
        self.data
            .first()
            .map_or(Schema::empty().into(), RecordBatch::schema)
    }

    pub fn to_matches(&self, table: &TableReference) -> Result<Vec<Match>> {
        // Early exit on no data.
        if self.data.first().is_none_or(|d| d.num_rows() == 0) {
            return Ok(vec![]);
        }
        let primary_keys_json = self.primary_keys_json()?;
        let additional_columns_json = self.addition_columns_json()?;
        let values = self.embedding_columns_list()?;
        let distances = self.distance_values()?;

        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let Some(distance) = distances.get(i) else {
                    return Err(Error::EmbeddingError {
                        source: format!("No distance returned for {i}th result").into(),
                    });
                };

                Ok(Match {
                    value: value.clone(),
                    score: 1.0 - *distance,
                    dataset: table.to_string(),
                    primary_key: primary_keys_json.get(i).cloned().unwrap_or_default(),
                    metadata: additional_columns_json.get(i).cloned().unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<Match>>>()
    }
}

pub type VectorSearchResult = HashMap<TableReference, VectorSearchTableResult>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Match {
    /// The value of the match (e.g., document snippet, identifier, etc.)
    value: String,

    /// The similarity of the match to the query
    score: f64,

    /// The name of the dataset where the match was found
    dataset: String,

    /// Primary key(s) identifying the matched item in the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub primary_key: HashMap<String, serde_json::Value>,

    /// Additional metadata for the match, requested explicitly by the user.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Match {
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn score(&self) -> f64 {
        self.score
    }

    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    #[must_use]
    pub fn primary_key(&self) -> &HashMap<String, serde_json::Value> {
        &self.primary_key
    }

    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, serde_json::Value> {
        &self.metadata
    }
}

pub fn to_matches_sorted(result: &VectorSearchResult, limit: usize) -> Result<Vec<Match>> {
    let output = result
        .iter()
        .map(|(a, b)| b.to_matches(a))
        .collect::<Result<Vec<_>>>()?;

    let mut matches: Vec<_> = output.into_iter().flatten().collect();
    // Sort by score in descending order
    matches.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    matches.truncate(limit);
    Ok(matches)
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

    fn construct_chunk_query_sql(
        primary_keys: &[String],
        projection: &[String],
        embedding_column: &str,
        table_name: &TableReference,
        embedding: &[f32],
        where_cond: &str,
        n: usize,
    ) -> String {
        let pks = if primary_keys.is_empty() {
            // If the dataset has no true primary keys, the only known unique key is the embedding column.
            vec![quote_identifier(embedding_column).to_string()]
        } else {
            primary_keys
                .iter()
                .map(|s| quote_identifier(s).to_string())
                .collect_vec()
        };

        format!(
            "WITH distances as (
                SELECT
                    {pks},
                    unnest({embed_col_offset}) AS offset,
                    cosine_distance(unnest({embed_col_embedding}), {embedding:?}) AS {VECTOR_DISTANCE_COLUMN_NAME}
                FROM {table_name}
                {where_cond}
            ),
            ranks as (
                SELECT
                    {pks},
                    distances.offset,
                    distances.{VECTOR_DISTANCE_COLUMN_NAME},
                    ROW_NUMBER() OVER (PARTITION BY ({pks}) ORDER BY distances.{VECTOR_DISTANCE_COLUMN_NAME} ASC) AS chunk_rank
                FROM distances
            ),
            ranked_docs as (
                select {pks}, ranks.{VECTOR_DISTANCE_COLUMN_NAME}, ranks.offset
                from ranks
                WHERE chunk_rank = 1
                ORDER by {VECTOR_DISTANCE_COLUMN_NAME} ASC
                LIMIT {n}
            )
            SELECT
                substring(t.{embed_col}, rd.offset[1], rd.offset[2] - rd.offset[1]) AS {embed_col}_chunk,
                {projection_str},
                rd.{VECTOR_DISTANCE_COLUMN_NAME}
            FROM ranked_docs rd
            JOIN {table_name} t ON {join_on_conditions}",
                embed_col=quote_identifier(embedding_column).to_string(),
                embed_col_offset=offset_col!(quote_identifier(embedding_column).to_string()),
                embed_col_embedding=embedding_col!(quote_identifier(embedding_column).to_string()),
                pks = pks.iter().join(", "),
                projection_str = projection.iter()
                    .map(|s| format!("t.{s}"))
                    .join(", "),
                join_on_conditions = pks
                    .iter()
                    .map(|pk| format!("rd.{p} = t.{p}", p = quote_identifier(pk)))
                    .join(" AND "),
        )
    }

    /// Perform a single SQL query vector search.
    #[allow(clippy::too_many_arguments)]
    async fn individual_search(
        &self,
        tbl: &TableReference,
        embedding: Vec<f32>,
        primary_keys: &[String],
        embedding_column: &str,
        is_chunked: bool,
        additional_columns: &[String],
        where_cond: Option<&Expr>,
        keywords: &[&str],
        n: usize,
    ) -> Result<VectorSearchTableResult> {
        let projection: Vec<String> = primary_keys
            .iter()
            .cloned()
            .chain(Some(embedding_column.to_string()))
            .chain(additional_columns.iter().cloned())
            .unique()
            .map(|s| quote_identifier(&s).to_string())
            .collect();

        let keywords_filter = keywords
            .iter()
            .map(|k| format!("{embedding_column} ILIKE '%{k}%'"))
            .join(" OR ");

        let where_str = match (where_cond, keywords.is_empty()) {
            (Some(cond), false) => {
                format!("WHERE ({cond}) AND ({keywords_filter})")
            }
            (Some(cond), true) => format!("WHERE ({cond})"),
            (None, false) => format!("WHERE ({keywords_filter})"),
            (None, true) => String::new(),
        };

        let query = if is_chunked {
            Self::construct_chunk_query_sql(
                primary_keys,
                &projection,
                embedding_column,
                tbl,
                &embedding,
                &where_str,
                n,
            )
        } else {
            format!(
                "SELECT * FROM (
                    SELECT
                        {projection_str},
                        cosine_distance({embedding_column}_embedding, {embedding:?}) as {VECTOR_DISTANCE_COLUMN_NAME}
                    FROM {tbl}
                    {where_str}
                ) subq
                WHERE {VECTOR_DISTANCE_COLUMN_NAME} IS NOT NULL
                ORDER BY {VECTOR_DISTANCE_COLUMN_NAME} ASC
                LIMIT {n}", projection_str=projection.iter().join(", ")
            )
        };
        tracing::trace!("running SQL: {query}");

        let batches: Vec<RecordBatch> = self
            .df
            .query_builder(&query)
            .build()
            .run()
            .await
            .boxed()
            .context(DataFusionSnafu)?
            .data
            .try_collect()
            .await
            .boxed()
            .context(DataFusionSnafu)?;

        let embedding_column = if is_chunked {
            format!(
                "{embed_col}_chunk",
                embed_col = quote_identifier(embedding_column)
            )
        } else {
            quote_identifier(embedding_column).to_string()
        };

        Ok(VectorSearchTableResult {
            data: batches,
            primary_keys: primary_keys.to_vec(),
            additional_columns: additional_columns.to_vec(),
            embedding_column,
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
            None => self.user_tables_with_embeddings().await?,
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

            let per_table_embeddings = self
                .calculate_embeddings_per_table(query, &tables)
                .await?;

            let table_primary_keys = self
                .get_primary_keys_with_overrides(&self.explicit_primary_keys, &tables)
                .await?;

            let mut response: VectorSearchResult = HashMap::new();
            let keywords = keywords.iter().map(std::string::String::as_str).collect::<Vec<&str>>();

            for (tbl, search_vectors) in per_table_embeddings {
                tracing::debug!("Running vector search for table {:#?}", tbl);
                let primary_keys = table_primary_keys.get(&tbl).map_or(&[] as &[String], |v| v.as_slice());

                // Only support one embedding column per table.
                let table_provider = self
                    .df
                    .get_table(&tbl)
                    .await
                    .ok_or(Error::DataSourcesNotFound {
                        data_source: vec![tbl.clone()],
                    })?;

                let Some(embedding_table) = get_embedding_table(&table_provider).await else {
                    return Err(Error::CannotVectorSearchDataset {
                        data_source: tbl.clone()
                    });
                };

                let embedding_columns = embedding_table.get_embedding_columns();
                let Some(embedding_column) = embedding_columns.first() else {
                    return Err(Error::NoEmbeddingColumns {
                        data_source: tbl.clone(),
                    });
                };

                if search_vectors.len() != 1 {
                    return Err(Error::IncorrectNumberOfEmbeddingColumns {
                        data_source: tbl.clone(),
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
                                primary_keys,
                                embedding_column,
                                embedding_table.is_chunked(embedding_column),
                                additional_columns,
                                where_cond.as_ref(),
                                &keywords,
                                *limit,
                            )
                            .await?;
                        response.insert(tbl.clone(), result);
                    }
                };
            }
            tracing::info!(target: "task_history", captured_output = ?response);
            Ok(response)
        }.instrument(span.clone()).await;

        match vector_search_result {
            Ok(result) => Ok(result),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    pub async fn user_tables_with_embeddings(&self) -> Result<Vec<TableReference>> {
        let tables = self.df.get_user_table_names();
        let mut tables_with_embeddings = Vec::new();

        for t in tables {
            let table_provider = self
                .df
                .get_table(&t)
                .await
                // we should not fail here, as we are iterating over the tables that we know exist
                .context(DataSourceNotFoundSnafu { table: t.clone() })?;
            if get_embedding_table(&table_provider).await.is_some() {
                tables_with_embeddings.push(t);
            }
        }
        Ok(tables_with_embeddings)
    }

    /// For the data sources that assumedly exist in the [`DataFusion`] instance, find the embedding models used in each data source.
    async fn find_relevant_embedding_models(
        &self,
        data_sources: &[TableReference],
    ) -> Result<HashMap<TableReference, Vec<ModelKey>>> {
        let mut embeddings_to_run = HashMap::new();
        for data_source in data_sources {
            let table = self
                .df
                .get_table(data_source)
                .await
                .context(DataSourcesNotFoundSnafu {
                    data_source: vec![data_source.clone()],
                })?;

            let embedding_models = get_embedding_table(&table)
                .await
                .context(NoEmbeddingColumnsSnafu {
                    data_source: data_source.to_string(),
                })?
                .get_embedding_models_used();
            embeddings_to_run.insert(data_source.clone(), embedding_models);
        }
        Ok(embeddings_to_run)
    }

    async fn get_primary_keys(&self, table: &TableReference) -> Result<Vec<String>> {
        let tbl_ref = self
            .df
            .get_table(table)
            .await
            .context(DataSourcesNotFoundSnafu {
                data_source: vec![table.clone()],
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
        tables: &[TableReference],
    ) -> Result<HashMap<TableReference, Vec<String>>> {
        let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

        for tbl in tables {
            // `explicit_primary_keys` are [`ResolvedTableReference`], must resolve with spice defaults first.
            // Equivalent to using [`TableReference::resolve_eq`] on `explicit_primary_keys` keys.
            let resolved_tbl: TableReference = tbl
                .clone()
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .into();
            let pks = self.get_primary_keys(&resolved_tbl).await?;
            if !pks.is_empty() {
                tbl_to_pks.insert(tbl.clone(), pks);
            } else if let Some(explicit_pks) = explicit_primary_keys.get(&resolved_tbl) {
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
                if name == embedding_model {
                    Some(model)
                } else {
                    None
                }
            })
            .ok_or(Error::EmbeddingModelNotFound {
                model_name: embedding_model.to_string(),
            })?
            .embed(EmbeddingInput::String(input.to_string()))
            .await
            .boxed()
            .context(EmbeddingSnafu)?
            .first()
            .cloned()
            .ok_or(Error::EmbeddingError {
                source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "No embeddings returned for input text from {embedding_model}"
                )),
            })
    }

    /// For each embedding column that a [`TableReference`] contains, calculate the embeddings vector between the query and the column.
    /// The returned `HashMap` is a mapping of [`TableReference`] to an (alphabetical by column name) in-order vector of embeddings.
    async fn calculate_embeddings_per_table(
        &self,
        query: &str,
        data_sources: &[TableReference],
    ) -> Result<HashMap<TableReference, Vec<Vec<f32>>>> {
        // Determine which embedding models need to be run. If a table does not have an embedded column, return an error.
        let embeddings_to_run: HashMap<TableReference, Vec<ModelKey>> =
            self.find_relevant_embedding_models(data_sources).await?;

        // Create embedding(s) for question/statement. `embedded_inputs` model_name -> embedding.
        let mut embedded_inputs: HashMap<ModelKey, Vec<f32>> = HashMap::new();
        for model in embeddings_to_run.values().flatten().unique() {
            let result = self
                .embed(query, model)
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

/// Convert a list of column names to a list of column indices. If a column name is not found in the schema, it is ignored.
fn get_projection(schema: &SchemaRef, column_names: &[String]) -> Vec<usize> {
    column_names
        .iter()
        .filter_map(|name| {
            schema
                .index_of(quote_identifier(name).to_string().as_str())
                .ok()
        })
        .collect_vec()
}

/// If a [`TableProvider`] is an [`EmbeddingTable`], return the [`EmbeddingTable`].
/// This includes if the [`TableProvider`] is an [`AcceleratedTable`] with a [`EmbeddingTable`] underneath.
async fn get_embedding_table(tbl: &Arc<dyn TableProvider>) -> Option<Arc<EmbeddingTable>> {
    if let Some(embedding_table) = tbl.as_any().downcast_ref::<EmbeddingTable>() {
        return Some(Arc::new(embedding_table.clone()));
    }

    let tbl = if let Some(adaptor) = tbl.as_any().downcast_ref::<FederatedTableProviderAdaptor>() {
        adaptor.table_provider.clone()?
    } else {
        Arc::clone(tbl)
    };

    if let Some(accelerated_table) = tbl.as_any().downcast_ref::<AcceleratedTable>() {
        let federated_table = accelerated_table
            .get_federated_table()
            .table_provider()
            .await;
        if let Some(embedding_table) = federated_table.as_any().downcast_ref::<EmbeddingTable>() {
            return Some(Arc::new(embedding_table.clone()));
        }
    }
    None
}

/// Compute the primary keys for each table in the app. Primary Keys can be explicitly defined in the Spicepod.yaml
pub async fn parse_explicit_primary_keys(
    app: Arc<RwLock<Option<Arc<App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                d.embeddings.iter().find_map(|e| {
                    e.primary_keys.as_ref().map(|pks| {
                        (
                            TableReference::parse_str(&d.name)
                                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                                .into(),
                            pks.clone(),
                        )
                    })
                })
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr};
    use schemars::schema_for;
    use snafu::ResultExt;

    #[tokio::test]
    async fn test_search_request_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        serde_json::to_value(schema_for!(SearchRequestAIJson)).boxed()?;
        Ok(())
    }

    #[test]
    fn test_valid_where_conditions() {
        // Test basic comparison
        let result = SearchRequest::parse_where_cond("column = 'value'".to_string());
        assert!(result.is_ok());

        // Test with WHERE keyword
        let result = SearchRequest::parse_where_cond("WHERE column = 'value'".to_string());
        assert!(result.is_ok());

        // Test numeric comparison
        let result = SearchRequest::parse_where_cond("age > 18".to_string());
        assert!(result.is_ok());

        // Test boolean condition
        let result = SearchRequest::parse_where_cond("is_active = true".to_string());
        assert!(result.is_ok());

        // Test AND condition
        let result = SearchRequest::parse_where_cond("age > 18 AND is_active = true".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_malformed_conditions() {
        // Test semicolon injection
        let result =
            SearchRequest::parse_where_cond("column = 'value'; DROP TABLE users;".to_string());
        assert!(result.is_err(), "{}", result.expect("!"));

        // Test UNION injection
        let result = SearchRequest::parse_where_cond(
            "column = 'value' UNION SELECT * FROM users".to_string(),
        );
        assert!(result.is_err());

        // Test multiple statements
        let result =
            SearchRequest::parse_where_cond("column = 'value'; SELECT * FROM users".to_string());
        assert!(result.is_err());

        // Test stacked queries
        let result = SearchRequest::parse_where_cond(
            "column = 'value'); SELECT * FROM users; --".to_string(),
        );
        assert!(result.is_err());

        // Test incomplete expression
        let result = SearchRequest::parse_where_cond("column =".to_string());
        assert!(result.is_err());

        // Test invalid operator
        let result = SearchRequest::parse_where_cond("column === value".to_string());
        assert!(result.is_err());

        // Test unclosed string
        let result = SearchRequest::parse_where_cond("column = 'value".to_string());
        assert!(result.is_err());

        // Test invalid column name
        let result = SearchRequest::parse_where_cond("'column' = 'value'".to_string());
        assert!(result.is_ok()); // Note: This is actually valid SQL syntax

        // Test empty condition
        let result = SearchRequest::parse_where_cond(String::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_valid_conditions() {
        // Test nested AND/OR
        let result = SearchRequest::parse_where_cond(
            "age > 18 AND (is_active = true OR role = 'admin')".to_string(),
        );
        assert!(result.is_ok());

        // Test IN clause
        let result = SearchRequest::parse_where_cond("status IN ('active', 'pending')".to_string());
        assert!(result.is_ok());

        // Test BETWEEN
        let result = SearchRequest::parse_where_cond("age BETWEEN 18 AND 65".to_string());
        assert!(result.is_ok());

        // Test IS NULL
        let result = SearchRequest::parse_where_cond("last_login IS NULL".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_expression_structure() {
        // Test basic equality expression structure
        let result = SearchRequest::parse_where_cond("column = 'value'".to_string())
            .expect("Should parse successfully");

        if let Expr::BinaryOp {
            left: _,
            op,
            right: _,
        } = result
        {
            assert_eq!(op, BinaryOperator::Eq);
        } else {
            panic!("Expected BinaryOp expression");
        }

        // Test AND expression structure
        let result = SearchRequest::parse_where_cond("col1 = 'val1' AND col2 = 'val2'".to_string())
            .expect("Should parse successfully");

        if let Expr::BinaryOp {
            left: _,
            op,
            right: _,
        } = result
        {
            assert_eq!(op, BinaryOperator::And);
        } else {
            panic!("Expected BinaryOp expression");
        }
    }

    #[test]
    fn test_search_request_parse_additional_columns() {
        let additional_columns = vec!["column1".to_string(), "column2".to_string()];
        let result = SearchRequest::parse_additional_columns(&additional_columns);
        assert!(result.is_ok());

        // Test invalid column name
        let additional_columns = vec!["column 1".to_string()];
        let result = SearchRequest::parse_additional_columns(&additional_columns);
        assert!(result.is_err());

        // Test empty column name
        let additional_columns = vec![String::new()];
        let result = SearchRequest::parse_additional_columns(&additional_columns);
        assert!(result.is_err());

        // Test escaping column name
        let additional_columns = vec!["1; DROP TABLE testing; --".to_string()]; // would result in SELECT 1; DROP TABLE testing; -- FROM testing;
        let result = SearchRequest::parse_additional_columns(&additional_columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_performance_of_column_parsing() {
        let mut timings = vec![];

        for _ in 0..3 {
            let mut additional_columns = vec!["column1".to_string()];
            for i in 0..100 {
                let start = std::time::Instant::now();
                let result = SearchRequest::parse_additional_columns(&additional_columns);
                timings.push(start.elapsed());
                assert!(result.is_ok());
                additional_columns.push(format!("column{}", i + 2));
            }
        }

        let total_time: std::time::Duration = timings.iter().sum();
        #[allow(clippy::cast_possible_truncation)]
        let average_time = total_time / (timings.len() as u32);
        let average_time_ns = average_time.as_nanos();
        assert!(
            average_time_ns < 1_000_000,
            "Average time: {average_time_ns}ns"
        ); // less than 1ms
    }

    #[test]
    fn test_search_request_parse_keywords() {
        let keywords = vec!["keyword1".to_string(), "keyword2".to_string()];
        let result = SearchRequest::parse_keywords(&keywords);
        assert!(result.is_ok());

        // Test keyword with a space
        let keywords = vec!["keyword 1".to_string()];
        let result = SearchRequest::parse_keywords(&keywords);
        assert!(result.is_ok());

        // Test empty keyword
        let keywords = vec![String::new()];
        let result = SearchRequest::parse_keywords(&keywords);
        assert!(result.is_ok());

        // Test escaping keyword
        let keywords = vec!["\"); DROP TABLE testing;".to_string()];
        let result = SearchRequest::parse_keywords(&keywords);
        assert!(result.is_err());
    }

    #[test]
    fn test_performance_of_keyword_parsing() {
        let mut timings = vec![];

        for _ in 0..3 {
            let mut keywords = vec!["column1".to_string()];
            for i in 0..100 {
                let start = std::time::Instant::now();
                let result = SearchRequest::parse_keywords(&keywords);
                timings.push(start.elapsed());
                assert!(result.is_ok());
                keywords.push(format!("column{}", i + 2));
            }
        }

        let total_time: std::time::Duration = timings.iter().sum();
        #[allow(clippy::cast_possible_truncation)]
        let average_time = total_time / (timings.len() as u32);
        let average_time_ns = average_time.as_nanos();
        assert!(
            average_time_ns < 1_000_000,
            "Average time: {average_time_ns}ns"
        ); // less than 1ms
    }
}
