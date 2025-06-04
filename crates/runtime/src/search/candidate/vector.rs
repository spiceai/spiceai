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
use std::sync::Arc;

use crate::search::Error as VectorSearchError;
use crate::{embedding_col, offset_col};
use async_openai::types::EmbeddingInput;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use itertools::Itertools;
use llms::embeddings::Embed;
use search::generation::{CandidateGeneration, Error as SearchGenerationError};
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use snafu::ResultExt;

use crate::datafusion::DataFusion;

// Distance column name for the vector search query.
// static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";
// Surrogate unique identifier name to use when no primary keys are provided.
static VSS_TEMP_GEN_ID_COLUMN: &str = "vss_temp_gen_id";
// Temporary table name to provide surrogate unique id for vector search query when no primary keys are provided.
static VSS_TEMP_TABLE_NAME: &str = "vss_temp_table";

pub struct VectorGeneration {
    df: Arc<DataFusion>,
    tbl: TableReference,
    embed: Arc<dyn Embed>,
    primary_keys: Vec<String>,
    embedding_column: String,
    is_chunked: bool,
}

impl VectorGeneration {
    pub fn new(
        df: &Arc<DataFusion>,
        tbl: &TableReference,
        embed: &Arc<dyn Embed>,
        primary_keys: &[String],
        embedding_column: &str,
        is_chunked: bool,
    ) -> Self {
        Self {
            df: Arc::clone(df),
            tbl: tbl.clone(),
            embed: Arc::clone(embed),
            primary_keys: primary_keys.to_vec(),
            embedding_column: embedding_column.to_string(),
            is_chunked,
        }
    }

    /// Embed the input text using the specified embedding model.
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, VectorSearchError> {
        self.embed
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()
            .map_err(|e| VectorSearchError::EmbeddingError { source: e })?
            .first()
            .cloned()
            .ok_or(VectorSearchError::EmbeddingError {
                source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "No embeddings returned for input text '{query}'"
                )),
            })
    }

    fn chunked_sql(
        &self,
        additional_columns: &[&Expr],
        embedding: &[f32],
        opt_filters: &[&Expr],
        n: usize,
    ) -> String {
        let (pks, distances_cte, proj_table) =
            self.score_cte_sql(additional_columns, embedding, opt_filters);
        let projection: Vec<Expr> = self
            .primary_keys
            .iter()
            .map(|s| Expr::Identifier(Ident::new(s)))
            .chain(additional_columns.iter().map(|&e| e.clone()))
            .unique()
            .collect();
        let final_projection_str = if projection.is_empty() {
            String::new()
        } else {
            format!(
                "{},",
                // `t.` refers to the table name alias in SQL below.
                projection.iter().map(|s| format!("t.{s}")).join(", ")
            )
        };

        format!(
                "{distances_cte},
                ranks as (
                    SELECT
                        {pks},
                        scores.offset,
                        scores.score,
                        ROW_NUMBER() OVER (PARTITION BY ({pks}) ORDER BY scores.score DESC) AS chunk_rank
                    FROM scores
                ),
                ranked_docs as (
                    select {pks}, ranks.score, ranks.offset
                    from ranks
                    WHERE chunk_rank = 1
                    ORDER by score DESC
                    LIMIT {n}
                )
                SELECT
                    substring(t.{embed_col}, rd.offset[1], rd.offset[2] - rd.offset[1]) AS {SEARCH_VALUE_COLUMN_NAME},
                    {projection_str}
                    rd.score
                FROM ranked_docs rd
                JOIN {proj_table} t ON {join_on_conditions}",
                embed_col= self.embedding_column,
                pks = pks.iter().join(", "),
                projection_str = final_projection_str,
                join_on_conditions = pks
                    .iter()
                    .map(|pk| format!("rd.{p} = t.{p}", p = datafusion::common::utils::quote_identifier(pk)))
                    .join(" AND "),
            )
    }

    /// Intermediate result of vector search on chunk-based table.
    ///
    /// Returns:
    ///   0: primary keys (could be artificial from temporary table if none exist in underlying table)
    ///   1: SQL query for CTE of scores. Will have at least one CTE of the form: `WITH scores AS ()`.
    ///   2: Where extra columns for the final projection can be found (for table without primary key we must retrieve additional columns instead of depending on the underlying table.)
    fn score_cte_sql(
        &self,
        additional_columns: &[&Expr],
        embedding: &[f32],
        opt_filters: &[&Expr],
    ) -> (Vec<String>, String, String) {
        if self.primary_keys.is_empty() {
            self.score_cte_sql_without_pks(additional_columns, embedding, opt_filters)
        } else {
            let projection: Vec<Expr> = self
                .primary_keys
                .iter()
                .cloned()
                .chain(Some(self.embedding_column.to_string()))
                .map(|s| Expr::Identifier(Ident::new(s)))
                .chain(additional_columns.iter().map(|&c| c.clone()))
                .unique()
                .collect();

            let cte = format!(
                "WITH scores as (
                     SELECT
                         {projection},
                         unnest({embed_col_offset}) AS offset,
                         1.0 - cosine_distance(unnest({embed_col_embedding}), {embedding:?}) AS {SEARCH_SCORE_COLUMN_NAME}
                     FROM {table_name}
                     {where_cond}
                 )",
                projection = projection.iter().map(|e| format!("{}", *e)).join(", "),
                embed_col_offset=Expr::Identifier(Ident::new(offset_col!(self.embedding_column))),
                embed_col_embedding=Expr::Identifier(Ident::new(embedding_col!(self.embedding_column))),
                table_name = self.tbl,
                where_cond = where_and(opt_filters)
            );
            (self.primary_keys.clone(), cte, self.tbl.to_string())
        }
    }

    /// Intermediate result of vector search on chunk-based table that do not have existing primary key(s).
    ///
    /// We use an additional surrogate temp table and a generated primary key.
    /// An alternative approach is using the full content as the primary key, but it is less efficient as primary keys
    /// are duplicated along with unnest, resulting in large memory allocation and inefficient final selection (join condition).
    fn score_cte_sql_without_pks(
        &self,
        additional_columns: &[&Expr],
        embedding: &[f32],
        opt_filters: &[&Expr],
    ) -> (Vec<String>, String, String) {
        // embedding_column is always added so we must filter it out from the projection if it is duplicated in the additional columns.
        let additional_columns = {
            let filtered: Vec<_> = additional_columns
                .iter()
                .filter(|c| {
                    matches!(
                        c,
                        Expr::Identifier(Ident {
                            value,
                            ..
                        }) if *value != *self.embedding_column
                    )
                })
                .collect();

            if filtered.is_empty() {
                String::new()
            } else {
                format!("{},", filtered.iter().join(", "))
            }
        };

        (
            vec![VSS_TEMP_GEN_ID_COLUMN.to_string()],
            format!(
                "WITH {VSS_TEMP_TABLE_NAME} AS (
               SELECT
                   ROW_NUMBER() OVER () AS {VSS_TEMP_GEN_ID_COLUMN},
                   {additional_columns}
                   {embedding_column},
                   {embed_col_offset},
                   {embed_col_embedding}
               FROM {table_name}
               {where_cond}
           ),
           scores as (
               SELECT
                   {VSS_TEMP_GEN_ID_COLUMN},
                   unnest({embed_col_offset}) AS offset,
                   1.0 - cosine_distance(unnest({embed_col_embedding}), {embedding:?}) AS {SEARCH_SCORE_COLUMN_NAME}
               FROM {VSS_TEMP_TABLE_NAME}
           )",
                embedding_column = self.embedding_column,
                embed_col_offset = Expr::Identifier(Ident::new(offset_col!(self.embedding_column))),
                embed_col_embedding =
                    Expr::Identifier(Ident::new(embedding_col!(self.embedding_column))),
                table_name = self.tbl,
                where_cond = where_and(opt_filters)
            ),
            VSS_TEMP_TABLE_NAME.to_string(),
        )
    }
}

#[async_trait::async_trait]
impl CandidateGeneration for VectorGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> search::generation::Result<SendableRecordBatchStream> {
        let embedding = self
            .embed_query(query.as_str())
            .await
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?;

        let query = if self.is_chunked {
            self.chunked_sql(
                addition_projection,
                embedding.as_slice(),
                opt_filters,
                limit,
            )
        } else {
            let projection: Vec<Expr> = self
                .primary_keys
                .iter()
                .cloned()
                .map(|s| Expr::Identifier(Ident::new(s)))
                .chain(Some(Expr::Named {
                    // `embedding_column as 'value'`
                    expr: Box::new(Expr::Identifier(Ident::new(self.embedding_column.clone()))),
                    name: Ident::new(SEARCH_VALUE_COLUMN_NAME),
                }))
                .chain(addition_projection.iter().map(|&e| e.clone()))
                .unique()
                .collect();

            format!(
                "SELECT * FROM (
                        SELECT
                            {projection_str},
                            1.0 - cosine_distance({embedding_column}_embedding, {embedding:?}) as {SEARCH_SCORE_COLUMN_NAME}
                        FROM {tbl}
                        {where_str}
                    ) subq
                    WHERE score IS NOT NULL
                    ORDER BY score DESC
                    LIMIT {limit}",
                projection_str = projection.iter().map(|e| format!("{}", *e)).join(", "),
                embedding_column = self.embedding_column,
                tbl = self.tbl,
                where_str = where_and(opt_filters),
            )
        };
        tracing::trace!("running SQL: {query}");

        Ok(self
            .df
            .query_builder(&query)
            .build()
            .run()
            .await
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?
            .data)
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> Result<Vec<bool>, SearchGenerationError> {
        Ok(vec![])
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, _projection: &[&Expr]) -> Result<Vec<bool>, SearchGenerationError> {
        Ok(vec![])
    }

    fn value_derived_from(&self) -> String {
        self.embedding_column.clone()
    }
}

// Constructs a `WHERE` clause of aggregating ['Expr'] by AND conditions.
//
// Empty string returned for no filters.
#[must_use]
pub fn where_and(filters: &[&Expr]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let combined = filters.iter().map(|e| format!("{}", *e)).join(" AND ");
    format!("WHERE {combined}")
}
