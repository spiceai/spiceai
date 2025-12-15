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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::aggregation::from_single_input;
use crate::{
    SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, VectorSearchGenerationResult,
    collect_batches,
};

use super::{AggregationResult, CandidateAggregation, DatafusionSnafu};
use super::{Error, Result};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    Expr as LogicalExpr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::logical_expr::{JoinType, Operator, binary_expr, col, lit};
use datafusion::prelude::{SessionContext, coalesce};
use datafusion::sql::TableReference;
use snafu::ResultExt;

/// Reciprocal Rank Fusion (RRF) is a method for combining multiple ranked sets of search results.
/// The underlying score of the search results is not important, only the rank (per stream order).
/// The rank, for a given entry (for some primary key `a`) is converted to a score using the formula:
/// ```text
/// score_a = 1 / (rank_i + offset) + 1 / (rank_j + offset) + ...
/// ```
/// Where `rank_i` is the rank of the i-th stream, and `offset` is a constant (e.g. 60).
pub struct ReciprocalRankFusion;

#[async_trait]
impl CandidateAggregation for ReciprocalRankFusion {
    async fn aggregate(
        &self,
        mut data: Vec<VectorSearchGenerationResult>,
        primary_key: Vec<Column>,
        limit: usize,
    ) -> Result<AggregationResult> {
        let num_inputs = data.len();
        // Handle 0, or 1 candidates.
        if num_inputs <= 1 {
            return data
                .pop()
                .map(|d| from_single_input(d, primary_key))
                .ok_or(Error::NoCandidatesGenerated);
        }

        if primary_key.is_empty() {
            return Err(Error::NoPrimaryKey);
        }

        let schemas = data.iter().map(|d| d.data.schema()).collect::<Vec<_>>();
        let () = verify_schema_compatibility(schemas.as_slice())?;

        let ctx = SessionContext::new();
        let mut table_names: Vec<TableReference> = Vec::with_capacity(num_inputs);

        // Find all additional columns in the schema that are not part of the primary key or the expected
        // search columns.
        let mut additional_columns = HashSet::new();
        let mut matches: HashMap<String, Vec<String>> = HashMap::new();

        // Inefficient, but collect each stream, convert to [`MemTable`].
        let mut i = 0;
        for VectorSearchGenerationResult {
            data: stream,
            derived_from,
        } in data
        {
            let schema = stream.schema();
            additional_columns.extend(additional_columns_of_schema(
                &schema,
                primary_key.as_slice(),
            ));

            let data = collect_batches(stream).await.context(DatafusionSnafu)?;

            // If data is empty, don't use.
            if data.first().is_none_or(|rb| rb.num_rows() == 0) {
                continue;
            }

            // Since we know what the `SEARCH_VALUE_COLUMN_NAME` column for the i'th column will be in the final schema,
            // we can add it to the `matches` map now.
            matches
                .get_mut(derived_from.as_str())
                .map(|v| v.push(ith_search_value_column(i)))
                .unwrap_or_else(|| {
                    matches.insert(derived_from.clone(), vec![ith_search_value_column(i)]);
                });

            let table_name = TableReference::bare(format!("search_candidates_{i}"));
            table_names.push(table_name.clone());
            let table = MemTable::try_new(schema, vec![data]).context(DatafusionSnafu)?;
            let _ = ctx
                .register_table(table_name, Arc::new(table))
                .context(DatafusionSnafu)?;

            i += 1;
        }

        let primary_key_str: Vec<String> = primary_key
            .iter()
            .map(datafusion::prelude::Column::flat_name)
            .collect();

        // Now that we've filtered empty generation data, again check for <=1 inputs.
        if table_names.len() <= 1 {
            let tbl = table_names.pop().ok_or(Error::NoCandidatesGenerated)?;
            let match_keys: Vec<_> = matches.keys().cloned().collect();

            return result_from_table(
                &ctx,
                &tbl,
                match_keys.first().ok_or(Error::NoCandidatesGenerated)?,
                primary_key_str.as_slice(),
            )
            .await;
        }

        let additional_columns = additional_columns.into_iter().collect::<Vec<_>>();

        let plan = reciprocal_rank_fusion_plan(
            &ctx,
            table_names.as_slice(),
            primary_key.as_slice(),
            additional_columns.as_slice(),
            60,
            limit,
        )
        .await
        .context(DatafusionSnafu)?;

        tracing::debug!("Running RRF logical plan: {plan:?}");
        let data = ctx
            .execute_logical_plan(plan)
            .await
            .context(DatafusionSnafu)?
            .execute_stream()
            .await
            .context(DatafusionSnafu)?;

        Ok(AggregationResult {
            data,
            primary_key: primary_key_str,
            data_columns: additional_columns
                .iter()
                .map(datafusion::prelude::Column::flat_name)
                .collect(),
            matches,
        })
    }
}

// Construct a [`AggregationResult`] from a single table in a [`SessionContext`].
async fn result_from_table(
    ctx: &SessionContext,
    tbl: &TableReference,
    match_field: &str,
    primary_key: &[String],
) -> Result<AggregationResult> {
    let df = ctx.table(tbl.clone()).await.context(DatafusionSnafu)?;
    let data_columns = df
        .schema()
        .columns()
        .iter()
        .filter_map(|c| {
            let name = c.name().to_string();
            if primary_key.contains(&name) {
                return None;
            }
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str()) {
                return None;
            }
            Some(name)
        })
        .collect();
    let data = df.execute_stream().await.context(DatafusionSnafu)?;

    Ok(AggregationResult {
        data,
        primary_key: primary_key.to_vec(),
        data_columns,
        matches: [(
            match_field.to_string(),
            vec![SEARCH_VALUE_COLUMN_NAME.to_string()],
        )]
        .into(),
    })
}

/// Returns a list of additional columns in the schema that are not part of the primary key or the expected
/// search columns (i.e. score or underlying value).
fn additional_columns_of_schema(schema: &SchemaRef, primary_key: &[Column]) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let name = f.name();
            let col = Column::from_qualified_name(f.name());
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str())
                || primary_key.contains(&col)
            {
                return None;
            }
            Some(col)
        })
        .collect()
}

/// Verifies that all streams have the same schema and contain the required columns: [`SEARCH_VALUE_COLUMN_NAME`], [`SEARCH_SCORE_COLUMN_NAME`].
fn verify_schema_compatibility(schemas: &[SchemaRef]) -> Result<()> {
    let Some(schema) = schemas.iter().find(|s| !s.fields.is_empty()) else {
        return Ok(());
    };

    for s in schemas {
        if s.fields().is_empty() {
            // Empty schema -> empty data
            continue;
        }
        if s.column_with_name(SEARCH_VALUE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_VALUE_COLUMN_NAME.to_string(),
            });
        }

        if s.column_with_name(SEARCH_SCORE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_SCORE_COLUMN_NAME.to_string(),
            });
        }

        // Check that the schema is the same across all streams (i.e. all same as the first).
        // Ensure each column is in first schema, and equal number of columns.
        let correct_columns = s.fields().iter().any(|f| {
            let Some((_, f2)) = schema.column_with_name(f.name()) else {
                return false;
            };
            f2.data_type() == f.data_type() && f2.is_nullable() == f.is_nullable()
        });
        if schema.fields().len() != s.fields().len() || !correct_columns {
            return Err(Error::InconsistentColumns {
                s1: Arc::clone(schema),
                s2: Arc::clone(s),
            });
        }
    }

    Ok(())
}

fn ith_search_value_column(i: usize) -> String {
    format!("{SEARCH_VALUE_COLUMN_NAME}_{i}")
}

/// Generates the LogicalPlan for the RRF aggregation using LogicalPlanBuilder API.
///
/// This function takes already-registered table names from a SessionContext and builds
/// a logical plan that performs reciprocal rank fusion across them.
#[expect(clippy::cast_precision_loss)]
async fn reciprocal_rank_fusion_plan(
    ctx: &SessionContext,
    tables: &[TableReference],
    primary_key: &[Column],
    additional_columns: &[Column],
    offset: usize,
    limit: usize,
) -> datafusion::error::Result<LogicalPlan> {
    // 1) Build CTEs that add explicit rank per table, ranking by SEARCH_SCORE_COLUMN_NAME
    //    Equivalent to: SELECT *, ROW_NUMBER() OVER (ORDER BY score) AS rank FROM table
    let mut ranked_plans: Vec<(TableReference, LogicalPlan)> = Vec::with_capacity(tables.len());

    for table_name in tables {
        // Get the table from the context
        let table = ctx.table(table_name.clone()).await?;
        let table_provider = table.into_unoptimized_plan();

        // Build: SELECT *, ROW_NUMBER() OVER (ORDER BY score DESC) AS rank FROM table
        let window_expr = row_number()
            .order_by(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])
            .build()?
            .alias("rank");

        let ranked = LogicalPlanBuilder::from(table_provider)
            .window(vec![window_expr])?
            .alias(table_name.clone())?
            .build()?;

        ranked_plans.push((table_name.clone(), ranked));
    }

    // 2) Start with the first table
    let (first_table_name, first_plan) = ranked_plans.first().ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("No tables provided for RRF".to_string())
    })?;

    let mut builder = LogicalPlanBuilder::from(first_plan.clone());

    // 3) FULL OUTER JOIN remaining tables on primary key columns
    for (table_name, plan) in ranked_plans.iter().skip(1) {
        builder = builder.join(
            plan.clone(),
            JoinType::Full,
            (
                primary_key
                    .iter()
                    .map(|pk| pk.clone().with_relation(first_table_name.clone()))
                    .collect(),
                primary_key
                    .iter()
                    .map(|pk| pk.clone().with_relation(table_name.clone()))
                    .collect(),
            ),
            None,
        )?;
    }

    // 4) Build the RRF score: SUM(COALESCE(1.0/(rank + offset), 0)) across all tables
    let rrf_score = ranked_plans
        .iter()
        .map(|(table_name, _)| {
            let rank_col = col(Column::new(Some(table_name.clone()), "rank"));
            let offset_lit = lit(offset as f64);
            let score = binary_expr(
                lit(1.0),
                Operator::Divide,
                binary_expr(rank_col, Operator::Plus, offset_lit),
            );
            coalesce(vec![score, lit(0.0)])
        })
        .reduce(|acc, expr| binary_expr(acc, Operator::Plus, expr))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("No tables to compute RRF score".to_string())
        })?;

    let rrf_score_final = rrf_score.alias(SEARCH_SCORE_COLUMN_NAME);

    // 5) Build value columns: one per input table
    let value_cols: Vec<LogicalExpr> = ranked_plans
        .iter()
        .enumerate()
        .map(|(i, (table_name, _))| {
            col(Column::new(
                Some(table_name.clone()),
                SEARCH_VALUE_COLUMN_NAME,
            ))
            .alias(ith_search_value_column(i))
        })
        .collect();

    // 6) Coalesce primary key and additional columns across all tables
    let coalesced_cols: Vec<LogicalExpr> = [primary_key, additional_columns]
        .concat()
        .iter()
        .map(|col_name| {
            let col_refs: Vec<LogicalExpr> = ranked_plans
                .iter()
                .map(|(table_name, _)| col(col_name.clone().with_relation(table_name.clone())))
                .collect();
            coalesce(col_refs).alias(col_name.to_string())
        })
        .collect();

    // 7) Project: score, value columns, coalesced columns
    let projection: Vec<LogicalExpr> = [vec![rrf_score_final], value_cols, coalesced_cols].concat();

    builder = builder.project(projection)?;

    // 8) Sort by score descending and limit
    builder = builder
        .sort(vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)])?
        .limit(0, Some(limit))?;

    builder.build()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    // Note: The old SQL snapshot tests have been removed as we now use LogicalPlanBuilder.
    // The logical plan is tested through integration tests and runtime behavior verification.
    // If snapshot testing is needed, consider using LogicalPlan's display_indent() or explain methods.

    #[test]
    fn test_additional_columns_of_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Int8, false),
            Field::new(SEARCH_VALUE_COLUMN_NAME, DataType::Int8, false),
            Field::new("pk", DataType::Utf8, false),
            Field::new("additional", DataType::Int8, false),
        ]));
        let primary_keys = vec![Column::from_name("pk")];
        assert_eq!(
            additional_columns_of_schema(&schema, primary_keys.as_slice()),
            vec![Column::from_name("additional")]
        );
    }
}
