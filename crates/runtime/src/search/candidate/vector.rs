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
use datafusion::functions_aggregate::expr_fn::{avg, first_value, max, sum};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::prelude::{array_element, substring};
use datafusion::sql::TableReference;
use datafusion_expr::expr::{ScalarFunction, WindowFunction, WindowFunctionDefinition};
use datafusion_expr::{
    Expr as LogicalExpr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarUDF, binary_expr, col, ident, lit,
};
use runtime_datafusion_udfs::cosine_distance;
use search::generation::CandidateGeneration;
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use spicepod::component::embeddings::EmbeddingAggregation;
use std::sync::Arc;

// Distance column name for the vector search query.
// static VECTOR_DISTANCE_COLUMN_NAME: &str = "dist";
// Surrogate unique identifier name to use when no primary keys are provided.
static VSS_TEMP_GEN_ID_COLUMN: &str = "vss_temp_gen_id";
// Alias used for the source-list column while unnesting ListMulti rows;
// after unnest this column holds one matched element per row.
static LIST_MULTI_MATCH_ELEMENT_ALIAS: &str = "_match_element";

/// Scan mode for the non-indexed vector generation.
///
/// `ChunkedScalar` — the source column is a single string per row that
/// has been chunked at ingest time; an `<col>_offset` column carries
/// character offsets back into the source string.
///
/// `ListMulti` — the source column is a list of strings; each list
/// element was embedded in M2. Single-query `MaxSim` / Mean / Sum over
/// stored elements per row.
///
/// `LateInteraction` — both the query and the stored column are
/// multi-element. Scoring is `SUM_{q in Q} MAX_{d in D} cos(q, d)`
/// (ColBERT-style). The query strings are carried on the generator and
/// each is embedded via the `embed` UDF at query time.
#[derive(Clone, Debug)]
pub enum VectorScanMode {
    ChunkedScalar,
    ListMulti {
        aggregation: EmbeddingAggregation,
    },
    LateInteraction {
        /// All query strings. The first is the "primary" query used for
        /// the `query` field on `VectorSearchTableFuncArgs`; all are
        /// embedded via `embed(literal, model)` in the SQL.
        queries: Vec<String>,
    },
}

/// A [`CandidateGeneration`] for datasets whose embedding column is
/// doubly-nested (`List<FixedSizeList<F32, D>>`) and that do not use a
/// native vector index. Handles both chunked-scalar (content split
/// into chunks at ingest) and list-of-string multi-vector inputs.
pub struct ChunkedNonIndexVectorGeneration {
    table_provider: Arc<dyn TableProvider>,
    tbl: TableReference,
    model: String,
    embed: Arc<ScalarUDF>,
    primary_keys: Vec<String>,
    embedding_column: String,
    mode: VectorScanMode,
}

impl ChunkedNonIndexVectorGeneration {
    /// Chunked-scalar mode constructor. Preserves the pre-multi-vector
    /// public API so existing call sites don't need to change.
    pub fn new(
        table_provider: &Arc<dyn TableProvider>,
        tbl: &TableReference,
        embed: &Arc<ScalarUDF>,
        model: String,
        primary_keys: Vec<String>,
        embedding_column: &str,
    ) -> Self {
        Self::with_mode(
            table_provider,
            tbl,
            embed,
            model,
            primary_keys,
            embedding_column,
            VectorScanMode::ChunkedScalar,
        )
    }

    /// Multi-vector (list-of-strings) mode constructor.
    pub fn new_list_multi(
        table_provider: &Arc<dyn TableProvider>,
        tbl: &TableReference,
        embed: &Arc<ScalarUDF>,
        model: String,
        primary_keys: Vec<String>,
        embedding_column: &str,
        aggregation: EmbeddingAggregation,
    ) -> Self {
        Self::with_mode(
            table_provider,
            tbl,
            embed,
            model,
            primary_keys,
            embedding_column,
            VectorScanMode::ListMulti { aggregation },
        )
    }

    /// Late-interaction (multi-query × multi-element) constructor.
    /// `queries` must be non-empty. If it contains a single element the
    /// generator falls back to single-query `MaxSim` over the stored
    /// elements (semantically equivalent to the `ListMulti` path with
    /// `aggregation = max`).
    pub fn new_late_interaction(
        table_provider: &Arc<dyn TableProvider>,
        tbl: &TableReference,
        embed: &Arc<ScalarUDF>,
        model: String,
        primary_keys: Vec<String>,
        embedding_column: &str,
        queries: Vec<String>,
    ) -> Self {
        Self::with_mode(
            table_provider,
            tbl,
            embed,
            model,
            primary_keys,
            embedding_column,
            VectorScanMode::LateInteraction { queries },
        )
    }

    fn with_mode(
        table_provider: &Arc<dyn TableProvider>,
        tbl: &TableReference,
        embed: &Arc<ScalarUDF>,
        model: String,
        primary_keys: Vec<String>,
        embedding_column: &str,
        mode: VectorScanMode,
    ) -> Self {
        Self {
            table_provider: Arc::clone(table_provider),
            tbl: tbl.clone(),
            model,
            embed: Arc::clone(embed),
            primary_keys,
            embedding_column: embedding_column.to_string(),
            mode,
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

            let (project_cols, unnest_cols) = self.unnest_projection_and_columns();

            lp = lp
                .project([self.primary_keys.iter().map(ident).collect(), project_cols].concat())?
                // Note: `datafusion_expr::builder::unnest` does not work for complex queries
                .unnest_columns_with_options(unnest_cols, UnnestOptions::new())?;

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

    /// Build the per-row projection list and the set of columns that
    /// should be `UNNEST`ed, based on the scan mode.
    ///
    /// For `ChunkedScalar`: keeps the source string intact (the chunked
    /// embeddings and offset arrays are unnested in lockstep and the
    /// matched substring is extracted post-unnest via `substring(src,
    /// offset_start, offset_length)`).
    ///
    /// For `ListMulti`: unnests both the source list-of-strings and the
    /// embedding list together so each resulting row carries a scalar
    /// string paired with its vector.
    fn unnest_projection_and_columns(&self) -> (Vec<LogicalExpr>, Vec<Column>) {
        match &self.mode {
            VectorScanMode::ChunkedScalar => (
                vec![
                    ident(self.embedding_column.clone()),
                    ident(offset_col!(self.embedding_column)).alias("offset"),
                    ident(embedding_col!(self.embedding_column.clone())),
                ],
                vec![
                    Column::new_unqualified("offset"),
                    Column::new_unqualified(embedding_col!(self.embedding_column.clone())),
                ],
            ),
            // Both multi-vector modes unnest the source list and
            // embedding list together, pairing a scalar string with its
            // vector on each unnested row.
            VectorScanMode::ListMulti { .. } | VectorScanMode::LateInteraction { .. } => (
                vec![
                    ident(self.embedding_column.clone()).alias(LIST_MULTI_MATCH_ELEMENT_ALIAS),
                    ident(embedding_col!(self.embedding_column.clone())),
                ],
                vec![
                    Column::new_unqualified(LIST_MULTI_MATCH_ELEMENT_ALIAS),
                    Column::new_unqualified(embedding_col!(self.embedding_column.clone())),
                ],
            ),
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

        // Process the embedding column and offsets / list elements
        let (project_cols, unnest_cols) = self.unnest_projection_and_columns();
        let mut base_lp = lp
            .project([project_cols, vec![col(VSS_TEMP_GEN_ID_COLUMN)]].concat())?
            // Note: `datafusion_expr::builder::unnest` does not work for complex queries
            .unnest_columns_with_options(unnest_cols, UnnestOptions::new())?;

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

    /// Build the aggregate expression used to roll per-element scores up
    /// into a single score per primary key. Applied as a window function
    /// partitioned by pk, so a sibling `row_number() = 1` filter selects
    /// the best-matching element in the same step.
    fn aggregate_score_expr(&self, pks: &[String]) -> Result<LogicalExpr, DataFusionError> {
        let partition: Vec<LogicalExpr> = pks.iter().map(col).collect();
        let score_arg = col(SEARCH_SCORE_COLUMN_NAME);

        let aggregation = match &self.mode {
            VectorScanMode::ListMulti { aggregation } => *aggregation,
            // ChunkedScalar uses Max (single chunk wins per row).
            // Late-interaction builds its own aggregation pipeline and
            // does not go through this helper; `Max` is an inert default.
            VectorScanMode::ChunkedScalar | VectorScanMode::LateInteraction { .. } => {
                EmbeddingAggregation::Max
            }
        };

        let agg = match aggregation {
            EmbeddingAggregation::Max => max(score_arg),
            EmbeddingAggregation::Mean => avg(score_arg),
            EmbeddingAggregation::Sum => sum(score_arg),
        };

        // ExprFunctionExt::partition_by only accepts Expr::WindowFunction.
        // Convert the aggregate to a window expression (AGG() OVER (PARTITION BY ...))
        // before applying the partition clause.
        let LogicalExpr::AggregateFunction(agg_fn) = agg else {
            return datafusion::common::exec_err!(
                "Expected AggregateFunction expression for score aggregation"
            );
        };
        Ok(LogicalExpr::WindowFunction(Box::new(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(agg_fn.func),
            agg_fn.params.args,
        )))
        .partition_by(partition)
        .build()?
        .alias(AGG_SCORE_COLUMN_NAME))
    }
}

// Internal alias for the aggregated per-pk score; distinct from
// `SEARCH_SCORE_COLUMN_NAME` so we can rewrite that column after
// aggregation without colliding with the per-element score.
static AGG_SCORE_COLUMN_NAME: &str = "_agg_score";

#[async_trait::async_trait]
impl CandidateGeneration for ChunkedNonIndexVectorGeneration {
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        match &self.mode {
            VectorScanMode::ChunkedScalar => self.search_chunked_scalar(query),
            VectorScanMode::ListMulti { .. } => self.search_list_multi(query),
            VectorScanMode::LateInteraction { queries } => {
                // Argument `query` carries the primary query string; the
                // full set lives on the generator. Single-element
                // late-interaction collapses to `search_list_multi`.
                let qs = queries.clone();
                if qs.len() <= 1 {
                    self.search_list_multi(query)
                } else {
                    self.search_late_interaction(&qs)
                }
            }
        }
    }

    fn value_derived_from(&self) -> String {
        self.embedding_column.clone()
    }

    fn value_projection_name(&self) -> String {
        SEARCH_VALUE_COLUMN_NAME.to_string()
    }
}

impl ChunkedNonIndexVectorGeneration {
    fn search_chunked_scalar(
        &self,
        query: String,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
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

    /// Multi-vector (`ListMulti`) search: per-list-element cosine similarity
    /// rolled up per primary key with the configured aggregation
    /// (`max` / `mean` / `sum`). The `_match` column is the source list
    /// element that produced the best per-element score.
    fn search_list_multi(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let (pks, score_table, additional_table) =
            self.score_cte_sql(&self.table_provider, query, &[])?;

        // Project primary keys + per-element score + matched element.
        // Both `_score` and `_match_element` are scalar columns after
        // UNNEST in `score_cte_sql`.
        let mut plan = LogicalPlanBuilder::new(score_table)
            .project(
                [
                    pks.iter().map(ident).collect(),
                    vec![
                        col(SEARCH_SCORE_COLUMN_NAME),
                        col(LIST_MULTI_MATCH_ELEMENT_ALIAS),
                    ],
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

        // Two sibling window functions: one to pick the argmax element
        // (`chunk_rank = 1`), one to compute the aggregated per-pk score.
        let rank_window = row_number()
            .partition_by(pks.iter().map(col).collect())
            .order_by(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])
            .build()?
            .alias("chunk_rank");

        let agg_window = self.aggregate_score_expr(&pks)?;

        plan = plan
            .window(vec![rank_window])?
            .window(vec![agg_window])?
            .alias("rank")?
            .filter(col("chunk_rank").eq(lit(1)))?
            .sort(vec![
                LogicalExpr::Column(Column::new(Some("rank"), AGG_SCORE_COLUMN_NAME))
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
                        col(LIST_MULTI_MATCH_ELEMENT_ALIAS).alias(SEARCH_VALUE_COLUMN_NAME),
                        // Surface the aggregated score under the
                        // canonical `_score` name that downstream RRF /
                        // consumers expect.
                        col(AGG_SCORE_COLUMN_NAME).alias(SEARCH_SCORE_COLUMN_NAME),
                    ],
                ]
                .concat(),
            )?;

        Ok(Arc::new(ViewTable::new(plan.build()?, None)))
    }

    /// Late-interaction (ColBERT-style) search over a multi-vector
    /// column with a multi-string query.
    ///
    /// Scoring: for each query string `q_k`, compute the best per-row
    /// cosine similarity against any stored element (`MaxSim`). Sum those
    /// per-query bests into the final row score.
    ///
    /// Implementation: one sub-plan per query (reusing `score_cte_sql`'s
    /// UNNEST + per-element cosine), tagged with `q_idx`, unioned;
    /// then a two-step aggregate collapses per-query bests to a single
    /// per-primary-key row.
    fn search_late_interaction(
        &self,
        queries: &[String],
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        // Reuse the first query to grab the canonical primary-key set
        // and the additional-columns plan for the final join.
        let (pks, _primary_score_table, additional_table) =
            self.score_cte_sql(&self.table_provider, queries[0].clone(), &[])?;

        // Build one tagged sub-plan per query.
        let mut subplans: Vec<LogicalPlan> = Vec::with_capacity(queries.len());
        for (idx, q) in queries.iter().enumerate() {
            let (_, score_table, _) = self.score_cte_sql(&self.table_provider, q.clone(), &[])?;
            let idx_i64 = i64::try_from(idx).unwrap_or(i64::MAX);
            let subplan = LogicalPlanBuilder::new(score_table)
                .project(
                    [
                        pks.iter().map(ident).collect(),
                        vec![
                            col(SEARCH_SCORE_COLUMN_NAME),
                            col(LIST_MULTI_MATCH_ELEMENT_ALIAS),
                            lit(idx_i64).alias("q_idx"),
                        ],
                    ]
                    .concat(),
                )?
                .filter(
                    LogicalExpr::ScalarFunction(ScalarFunction::new_udf(
                        isnan(),
                        vec![ident(SEARCH_SCORE_COLUMN_NAME)],
                    ))
                    .is_false(),
                )?
                .build()?;
            subplans.push(subplan);
        }

        // UNION ALL the per-query sub-plans.
        let first = subplans.remove(0);
        let mut unioned = LogicalPlanBuilder::new(first);
        for p in subplans {
            unioned = unioned.union(p)?;
        }

        // Step 1: per (pk, q_idx) — MAX cosine (best stored element for
        // this query) and FIRST_VALUE(match element ordered by score).
        let step1_group: Vec<LogicalExpr> =
            [pks.iter().map(col).collect(), vec![col("q_idx")]].concat();
        let per_query_best_col = "per_query_best";
        let per_query_match_col = "per_query_match";

        let per_query_sort = vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)];
        let step1 = unioned
            .aggregate(
                step1_group,
                vec![
                    max(col(SEARCH_SCORE_COLUMN_NAME)).alias(per_query_best_col),
                    first_value(col(LIST_MULTI_MATCH_ELEMENT_ALIAS), per_query_sort)
                        .alias(per_query_match_col),
                ],
            )?
            .alias("per_query")?;

        // Step 2: per pk — SUM per-query bests (late-interaction total);
        // pick the match element from whichever query scored highest.
        let step2_group: Vec<LogicalExpr> = pks.iter().map(col).collect();
        let match_sort = vec![col(per_query_best_col).sort(false, false)];
        let aggregated = step1
            .aggregate(
                step2_group,
                vec![
                    sum(col(per_query_best_col)).alias(SEARCH_SCORE_COLUMN_NAME),
                    first_value(col(per_query_match_col), match_sort)
                        .alias(SEARCH_VALUE_COLUMN_NAME),
                ],
            )?
            .alias("agg")?;

        // Assemble final columns: additional columns from the base
        // table + primary keys + _match + _score, ordered by score.
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

        let plan = aggregated
            .sort(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])?
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
                        .map(|pk| Column::new(Some("agg"), pk).into())
                        .collect::<Vec<LogicalExpr>>(),
                    vec![col(SEARCH_VALUE_COLUMN_NAME), col(SEARCH_SCORE_COLUMN_NAME)],
                ]
                .concat(),
            )?;

        Ok(Arc::new(ViewTable::new(plan.build()?, None)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::catalog::TableProvider;
    use datafusion::sql::TableReference;
    use datafusion_expr::expr::WindowFunctionDefinition;
    use spicepod::component::embeddings::EmbeddingAggregation;

    fn make_list_multi_gen(aggregation: EmbeddingAggregation) -> ChunkedNonIndexVectorGeneration {
        use datafusion::logical_expr::{Volatility, create_udf};
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("_score", arrow_schema::DataType::Float32, true),
        ]));
        let provider: Arc<dyn TableProvider> = Arc::new(
            datafusion::datasource::empty::EmptyTable::new(Arc::clone(&schema)),
        );
        let embed = Arc::new(create_udf(
            "embed",
            vec![],
            arrow_schema::DataType::Null,
            Volatility::Volatile,
            Arc::new(|_| unimplemented!()),
        ));
        ChunkedNonIndexVectorGeneration::with_mode(
            &provider,
            &TableReference::bare("t"),
            &embed,
            "model".to_string(),
            vec!["id".to_string()],
            "_score",
            VectorScanMode::ListMulti { aggregation },
        )
    }

    fn unwrap_alias(expr: &LogicalExpr) -> &LogicalExpr {
        match expr {
            LogicalExpr::Alias(a) => &a.expr,
            other => other,
        }
    }

    #[test]
    fn aggregate_score_expr_max_produces_window_function() {
        let candidate = make_list_multi_gen(EmbeddingAggregation::Max);
        let expr = candidate
            .aggregate_score_expr(&["id".to_string()])
            .expect("should not error");
        assert!(
            matches!(
                unwrap_alias(&expr),
                LogicalExpr::WindowFunction(wf)
                    if matches!(wf.fun, WindowFunctionDefinition::AggregateUDF(_))
            ),
            "expected WindowFunction(AggregateUDF), got {expr:?}"
        );
    }

    #[test]
    fn aggregate_score_expr_mean_produces_window_function() {
        let cand = make_list_multi_gen(EmbeddingAggregation::Mean);
        let expr = cand
            .aggregate_score_expr(&["id".to_string()])
            .expect("should not error");
        assert!(
            matches!(unwrap_alias(&expr), LogicalExpr::WindowFunction(_)),
            "expected WindowFunction, got {expr:?}"
        );
    }

    #[test]
    fn aggregate_score_expr_sum_produces_window_function() {
        let cand = make_list_multi_gen(EmbeddingAggregation::Sum);
        let expr = cand
            .aggregate_score_expr(&["id".to_string()])
            .expect("should not error");
        assert!(
            matches!(unwrap_alias(&expr), LogicalExpr::WindowFunction(_)),
            "expected WindowFunction, got {expr:?}"
        );
    }
}
