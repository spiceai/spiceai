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

use crate::{embedding_col, offset_col};

use datafusion::catalog::TableProvider;
use datafusion::common::{Column, UnnestOptions};
use datafusion::datasource::{DefaultTableSource, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::functions::math::isnan;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::prelude::{array_element, substring};
use datafusion::sql::TableReference;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Expr as LogicalExpr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarUDF, binary_expr, col, ident, lit,
};
use runtime_datafusion_udfs::cosine_distance;
use search::generation::CandidateGeneration;
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use std::sync::Arc;

// Distance column name for the vector search query.
// static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";
// Surrogate unique identifier name to use when no primary keys are provided.
static VSS_TEMP_GEN_ID_COLUMN: &str = "vss_temp_gen_id";

/// A [`CandidateGeneration`] for datasets that have a chunked embedding column, but aren't using a vector index.
pub struct ChunkedNonIndexVectorGeneration {
    table_provider: Arc<dyn TableProvider>,
    tbl: TableReference,
    model: String,
    embed: Arc<ScalarUDF>,
    primary_keys: Vec<String>,
    embedding_column: String,
}

impl ChunkedNonIndexVectorGeneration {
    pub fn new(
        table_provider: &Arc<dyn TableProvider>,
        tbl: &TableReference,
        embed: &Arc<ScalarUDF>,
        model: String,
        primary_keys: Vec<String>,
        embedding_column: &str,
    ) -> Self {
        Self {
            table_provider: Arc::clone(table_provider),
            tbl: tbl.clone(),
            model,
            embed: Arc::clone(embed),
            primary_keys,
            embedding_column: embedding_column.to_string(),
        }
    }

    fn score_expr(&self, query: String) -> LogicalExpr {
        binary_expr(
            lit(1.0),
            Operator::Minus,
            LogicalExpr::ScalarFunction(ScalarFunction {
                func: Arc::new(cosine_distance::CosineDistance::new().into()) as Arc<ScalarUDF>,
                args: vec![
                    LogicalExpr::ScalarFunction(ScalarFunction::new_udf(
                        Arc::clone(&self.embed),
                        vec![lit(query), lit(self.model.clone())],
                    )),
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
        query: String,
        filters: &[LogicalExpr],
    ) -> Result<(Vec<String>, LogicalPlan, LogicalPlan), DataFusionError> {
        let mut lp = LogicalPlanBuilder::scan(
            self.tbl.clone(),
            Arc::new(DefaultTableSource::new(Arc::clone(tbl))),
            None,
        )?;

        if self.primary_keys.is_empty() {
            self.score_cte_sql_without_pks(lp, query, filters)
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
            cols.push(self.score_expr(query));
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
        query: String,
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
            .into_iter()
            .map(LogicalExpr::Column)
            .collect();

        // Then apply the window function separately
        let window_expr = row_number().alias(VSS_TEMP_GEN_ID_COLUMN);
        let lp = lp.project(lp_cols)?.window(vec![window_expr])?;

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
        cols.push(self.score_expr(query));
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
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let (pks, score_table, additional_table) =
            self.score_cte_sql(&self.table_provider, query, &[])?;

        // First project just the columns we need
        let mut plan = LogicalPlanBuilder::new(score_table)
            .project(
                [
                    pks.iter().map(ident).collect(),
                    vec![col(SEARCH_SCORE_COLUMN_NAME), col("offset")],
                ]
                .concat(),
            )?
            .filter(
                LogicalExpr::ScalarFunction(ScalarFunction::new_udf(
                    isnan(),
                    vec![ident(SEARCH_SCORE_COLUMN_NAME)],
                ))
                .is_false(),
            )?;

        // Filter out primary keys from additional columns if duplicated
        let final_additional_columns: Vec<_> = self
            .table_provider
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if self.primary_keys.contains(f.name()) {
                    None
                } else {
                    Some(ident(f.name().clone()))
                }
            })
            .collect();

        // Then apply the window function in a separate step
        let window_expr = row_number()
            .partition_by(pks.iter().map(col).collect())
            .order_by(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])
            .build()?
            .alias("chunk_rank");

        plan = plan
            .window(vec![window_expr])?
            .alias("rank")?
            .filter(col("chunk_rank").eq(lit(1)))?
            .sort(vec![
                LogicalExpr::Column(Column::new(Some("rank"), SEARCH_SCORE_COLUMN_NAME))
                    .sort(false, false),
            ])?
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
                        col(SEARCH_SCORE_COLUMN_NAME),
                    ],
                ]
                .concat(),
            )?;

        Ok(Arc::new(ViewTable::new(plan.build()?, None)))
    }

    fn value_derived_from(&self) -> String {
        self.embedding_column.clone()
    }

    fn value_projection_name(&self) -> String {
        SEARCH_VALUE_COLUMN_NAME.to_string()
    }
}
