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
use crate::request::{AsyncMarker, RequestContext};
use crate::search::Error as VectorSearchError;
use crate::{embedding_col, offset_col};
use arrow::array::FixedSizeListArray;
use arrow::datatypes::Float32Type;
use async_openai::types::EmbeddingInput;

use datafusion::catalog::TableProvider;
use datafusion::common::{Column, DFSchema, UnnestOptions};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::prelude::{array_element, substring};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Expr as LogicalExpr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarUDF, binary_expr, col, ident, lit,
};
use itertools::Itertools;
use llms::embeddings::Embed;
use runtime_datafusion_udfs::cosine_distance;
use search::generation::{
    CandidateGeneration, Error as SearchGenerationError, InternalSnafu, QuerySnafu,
};
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use snafu::ResultExt;
use std::collections::HashMap;
use std::sync::Arc;

use crate::datafusion::DataFusion;

// Distance column name for the vector search query.
// static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";
// Surrogate unique identifier name to use when no primary keys are provided.
static VSS_TEMP_GEN_ID_COLUMN: &str = "vss_temp_gen_id";

/// A [`CandidateGeneration`] for datasets that have a chunked embedding column, but aren't using a vector index.
pub struct ChunkedNonIndexVectorGeneration {
    df: Arc<DataFusion>,
    tbl: TableReference,
    embed: Arc<dyn Embed>,
    primary_keys: Vec<String>,
    embedding_column: String,
}

impl ChunkedNonIndexVectorGeneration {
    pub fn new(
        df: &Arc<DataFusion>,
        tbl: &TableReference,
        embed: &Arc<dyn Embed>,
        primary_keys: &[String],
        embedding_column: &str,
    ) -> Self {
        Self {
            df: Arc::clone(df),
            tbl: tbl.clone(),
            embed: Arc::clone(embed),
            primary_keys: primary_keys.to_vec(),
            embedding_column: embedding_column.to_string(),
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
        tbl: &Arc<dyn TableProvider>,
        additional_columns: &[LogicalExpr],
        embedding: &[f32],
        opt_filters: &[LogicalExpr],
        n: usize,
    ) -> Result<LogicalPlan, DataFusionError> {
        let (pks, score_table, additional_table) =
            self.score_cte_sql(tbl, embedding, opt_filters)?;

        // First project just the columns we need
        let plan = LogicalPlanBuilder::new(score_table).project(
            [
                pks.iter().map(ident).collect(),
                vec![col("score"), col("offset")],
            ]
            .concat(),
        )?;

        // Filter out primary keys from additional columns if duplicated
        let final_additional_columns: Vec<_> = additional_columns
            .iter()
            .filter(|&c| !self.primary_keys.contains(&c.to_string()))
            .cloned()
            .collect();

        // Then apply the window function in a separate step
        let window_expr = row_number()
            .partition_by(pks.iter().map(col).collect())
            .order_by(vec![col("score").sort(false, false)])
            .build()?
            .alias("chunk_rank");

        plan.window(vec![window_expr])?
            .alias("rank")?
            .filter(col("chunk_rank").eq(lit(1)))?
            .sort(vec![col("rank.score").sort(false, false)])?
            .limit(0, Some(n))?
            .join(
                additional_table,
                JoinType::Left,
                pks.iter()
                    .map(|pk| (Column::from_name(pk), Column::from_name(pk)))
                    .collect(),
                None,
            )?
            .project(
                [
                    final_additional_columns,
                    self.primary_keys
                        .iter()
                        .map(|pk| Column::new(Some("rank"), pk).into())
                        .collect::<Vec<LogicalExpr>>(),
                    vec![
                        substring(
                            ident(self.embedding_column.clone()),
                            array_element(col("rank.offset"), lit(1)),
                            binary_expr(
                                array_element(col("rank.offset"), lit(2)),
                                Operator::Minus,
                                array_element(col("rank.offset"), lit(1)),
                            ),
                        )
                        .alias(SEARCH_VALUE_COLUMN_NAME),
                        col("score"),
                    ],
                ]
                .concat(),
            )?
            .build()
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn score_expr(&self, embedding: &[f32]) -> LogicalExpr {
        binary_expr(
            lit(1.0),
            Operator::Minus,
            LogicalExpr::ScalarFunction(ScalarFunction {
                func: Arc::new(cosine_distance::CosineDistance::new().into()) as Arc<ScalarUDF>,
                args: vec![
                    lit(ScalarValue::FixedSizeList(Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![Some(
                                embedding.iter().copied().map(Some).collect::<Vec<_>>(),
                            )],
                            embedding.len() as i32,
                        ),
                    ))),
                    ident(embedding_col!(self.embedding_column.clone())),
                ],
            }),
        )
        .alias(SEARCH_SCORE_COLUMN_NAME)
    }

    /// Intermediate result of vector search on chunk-based table.
    ///
    /// Returns:
    ///   0: primary keys (could be artificial from temporary table if none exist in underlying table)
    ///   1: [`LogicalPlan`] of the scores. should have score and `match`(?) content.
    ///   2: [`LogicalPlan`] of additional columns. primary keys from 0 should be able to join uniquely between this and 1.
    fn score_cte_sql(
        &self,
        tbl: &Arc<dyn TableProvider>,
        embedding: &[f32],
        filters: &[LogicalExpr],
    ) -> Result<(Vec<String>, LogicalPlan, LogicalPlan), DataFusionError> {
        let mut lp = LogicalPlanBuilder::scan(
            self.tbl.clone(),
            Arc::new(DefaultTableSource::new(Arc::clone(tbl))),
            None,
        )?;

        if self.primary_keys.is_empty() {
            self.score_cte_sql_without_pks(lp, embedding, filters)
        } else {
            if let Some(f) = filters.iter().cloned().reduce(LogicalExpr::and) {
                lp = lp.filter(f)?;
            }

            lp = lp
                .project(
                    [
                        self.primary_keys.iter().map(ident).collect(),
                        vec![
                            ident(self.embedding_column.clone()),
                            ident(offset_col!(self.embedding_column)).alias("offset"),
                            ident(embedding_col!(self.embedding_column.clone())),
                        ],
                    ]
                    .concat(),
                )?
                // Note: `datafusion_expr::builder::unnest` does not work for complex queries
                .unnest_columns_with_options(
                    vec![
                        Column::new_unqualified("offset"),
                        Column::new_unqualified(embedding_col!(self.embedding_column.clone())),
                    ],
                    UnnestOptions::new(),
                )?;

            // Compute score
            let mut cols = lp
                .schema()
                .columns()
                .iter()
                .map(|c| LogicalExpr::Column(c.clone()))
                .collect::<Vec<_>>();
            cols.push(self.score_expr(embedding));
            lp = lp.project(cols)?.alias("scores")?;

            Ok((
                self.primary_keys.clone(),
                lp.build()?,
                LogicalPlanBuilder::scan(
                    self.tbl.clone(),
                    Arc::new(DefaultTableSource::new(Arc::clone(tbl))),
                    None,
                )?
                .build()?,
            ))
        }
    }

    /// Intermediate result of vector search on chunk-based table that do not have existing primary key(s).
    ///
    /// We use an additional surrogate temp table and a generated primary key.
    /// An alternative approach is using the full content as the primary key, but it is less efficient as primary keys
    /// are duplicated along with unnest, resulting in large memory allocation and inefficient final selection (join condition).
    fn score_cte_sql_without_pks(
        &self,
        mut lp: LogicalPlanBuilder,
        embedding: &[f32],
        filters: &[LogicalExpr],
    ) -> Result<(Vec<String>, LogicalPlan, LogicalPlan), DataFusionError> {
        // Apply filters if any
        if let Some(f) = filters.iter().cloned().reduce(LogicalExpr::and) {
            lp = lp.filter(f)?;
        }

        // First, create a plan without the window function
        let lp_cols: Vec<_> = lp
            .schema()
            .columns()
            .clone()
            .into_iter()
            .map(LogicalExpr::Column)
            .collect();

        // Then apply the window function separately
        let window_expr = row_number().alias(VSS_TEMP_GEN_ID_COLUMN);
        let lp = lp.project(lp_cols.clone())?.window(vec![window_expr])?;

        // This is just the table with all the additional columns we may want to join on
        let additional_lp = lp.clone().alias("additional")?.build()?;

        // Process the embedding column and offsets
        let mut base_lp = lp
            .project(vec![
                ident(self.embedding_column.clone()),
                ident(offset_col!(self.embedding_column)).alias("offset"),
                ident(embedding_col!(self.embedding_column)),
                col(VSS_TEMP_GEN_ID_COLUMN),
            ])?
            // Note: `datafusion_expr::builder::unnest` does not work for complex queries
            .unnest_columns_with_options(
                vec![
                    Column::new_unqualified("offset"),
                    Column::new_unqualified(embedding_col!(self.embedding_column.clone())),
                ],
                UnnestOptions::new(),
            )?;

        // Compute score
        let mut cols = base_lp
            .schema()
            .columns()
            .iter()
            .map(|c| LogicalExpr::Column(c.clone()))
            .collect::<Vec<_>>();
        cols.push(self.score_expr(embedding));
        base_lp = base_lp.project(cols)?.alias("scores")?;

        Ok((
            vec![VSS_TEMP_GEN_ID_COLUMN.to_string()],
            base_lp.build()?,
            additional_lp,
        ))
    }
}

#[async_trait::async_trait]
impl CandidateGeneration for ChunkedNonIndexVectorGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> search::generation::Result<SendableRecordBatchStream> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        telemetry::track_vector_search(&request_context.to_dimensions());
        let embedding = self
            .embed_query(query.as_str())
            .await
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?;

        let Some(tbl) = self.df.get_table_sync(&self.tbl) else {
            return Err(search::generation::Error::InternalError {
                source: Box::from(format!(
                    "Could not access table source for dataset '{}'.",
                    self.tbl
                )),
            });
        };
        let schema = Arc::new(
            DFSchema::from_unqualified_fields(tbl.schema().fields.clone(), HashMap::default())
                .context(QuerySnafu)?,
        );
        let filters: Vec<LogicalExpr> = opt_filters
            .iter()
            .map(|f| {
                self.df
                    .ctx
                    .state()
                    .create_logical_expr(&f.to_string(), &schema)
            })
            .collect::<Result<Vec<_>, _>>()
            .context(QuerySnafu)?;

        let projection: Vec<LogicalExpr> = addition_projection
            .iter()
            .map(|f| {
                self.df
                    .ctx
                    .state()
                    .create_logical_expr(&f.to_string(), &schema)
            })
            .collect::<Result<Vec<_>, _>>()
            .context(QuerySnafu)?;

        let plan = self
            .chunked_sql(&tbl, &projection, embedding.as_slice(), &filters, limit)
            .context(QuerySnafu)?;

        tracing::debug!(
            "Generating candidates for non-index, chunked vector dataset with Logical Plan:\n{}\n",
            plan.display_indent()
        );

        let data = self
            .df
            .query_from_logical_plan(&plan)
            .run()
            .await
            .boxed()
            .context(InternalSnafu)?
            .data;
        Ok(data)
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
