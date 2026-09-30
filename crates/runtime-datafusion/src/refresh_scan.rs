/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Builds and executes the scan a refresh reads its source data through.
//!
//! Either projects every column of the source table provider, or plans the
//! dataset's refresh SQL, then applies the refresh's filters and streams the
//! result. Computed columns (e.g. embeddings) are re-attached to a refresh-SQL
//! projection that would otherwise drop them.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DataFusionError};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ident};
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use tracing::Level;

use crate::error::find_datafusion_root;
use crate::refresh_sql::RefreshSQLDistinctOn;

/// Gets data from a table provider and returns it as a stream of `RecordBatch`es.
///
/// # Errors
///
/// Returns a `DataFusionError` if the scan cannot be planned — an invalid
/// refresh `sql`, a projection the source schema cannot satisfy, an
/// unrepresentable filter — or if executing the resulting plan fails.
pub async fn get_data(
    ctx: &mut SessionContext,
    table_name: TableReference,
    table_provider: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
    distinct_on: Option<&RefreshSQLDistinctOn>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let mut df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));

            // Get the columns so we can add projection to the plan. This
            // converts the plan to federated where the correct dialect is
            // applied
            let schema = table_provider.schema();
            let columns: Vec<Expr> = schema.fields().iter().map(|f| ident(f.name())).collect();

            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .map_err(find_datafusion_root)?
                .project(columns)?
                .build()
                .map_err(find_datafusion_root)?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => {
            let session = ctx.state();
            let mut plan = session
                .create_logical_plan(&sql)
                .await
                .map_err(find_datafusion_root)?;

            // If the refresh SQL defines a subset of columns to fetch, computed columns such as embeddings
            // are not included automatically, so we verify their presence and add them manually if needed.
            plan = include_computed_columns(plan, &table_provider.schema())?;

            DataFrame::new(session, plan)
        }
    };

    for filter in filters {
        df = df.filter(filter).map_err(find_datafusion_root)?;
    }

    // DISTINCT ON goes above the refresh filters, so it selects among the rows this
    // refresh reads (the append window, this node's partitions) rather than the
    // whole source.
    if let Some(distinct_on) = distinct_on {
        let on_expr = distinct_on.on.iter().map(|c| ident(&c.value)).collect();
        let select_expr = df
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let sort_expr = distinct_on
            .order_by
            .iter()
            .map(|o| ident(&o.column.value).sort(o.is_ascending(), o.sorts_nulls_first()))
            .collect();
        df = df
            .distinct_on(on_expr, select_expr, Some(sort_expr))
            .map_err(find_datafusion_root)?;
    }

    if tracing::enabled!(Level::TRACE)
        && let Ok(explained) = df.clone().explain(false, false)
        && let Ok(explained) = explained.to_string().await
    {
        tracing::trace!("Data refresh plan for {}:\n{}", table_name, explained);
    }

    let sql = Unparser::default()
        .plan_to_sql(df.logical_plan())
        .map_err(find_datafusion_root)?;
    tracing::info!(target: "task_history", sql = %sql, "labels");

    let record_batch_stream = df.execute_stream().await.map_err(find_datafusion_root)?;
    Ok(record_batch_stream)
}

/// Ensures that the associated computed columns (e.g., embeddings) are included
/// in the `LogicalPlan::Projection` node.
/// If any required computed columns are missing, they are automatically added to the projection.
fn include_computed_columns(
    plan: LogicalPlan,
    source_table_schema: &SchemaRef,
) -> DataFusionResult<LogicalPlan> {
    let plan = plan
        .transform_down(|plan| {
            match plan {
                LogicalPlan::Projection(mut proj) => {
                    for (idx, col) in proj.schema.columns().iter().enumerate() {
                        if let Some(computed_columns) = schema_meta_get_computed_columns(
                            source_table_schema.as_ref(),
                            col.name(),
                        ) {
                            for computed_column in computed_columns {
                                if !proj
                                    .schema
                                    .has_column_with_unqualified_name(computed_column.name())
                                {
                                    proj.expr.push(Expr::Column(Column::new(
                                        proj.schema.qualified_field(idx).0.cloned(),
                                        computed_column.name().clone(),
                                    )));
                                }
                            }
                        }
                    }
                    // The Transformed flag is not used, so we always specify it as transformed for simplicity.
                    Ok(Transformed::yes(LogicalPlan::Projection(proj)))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })?
        .data;

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresh_sql::parse_refresh_sql;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::catalog::MemTable;
    use datafusion::logical_expr::{col, lit};
    use futures::TryStreamExt;

    async fn latest_rows(refresh_sql: &str, filters: Vec<Expr>) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("occurred_at", DataType::Int64, true),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![Some(8), Some(10), Some(5), None])),
                Arc::new(StringArray::from(vec!["old", "new", "only", "null-time"])),
            ],
        )
        .expect("valid batch");
        let provider: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("valid table"),
        );
        let mut ctx = SessionContext::new();
        ctx.register_table("events", Arc::clone(&provider))
            .expect("register table");

        let table = TableReference::parse_str("events");
        let (parsed, _) =
            parse_refresh_sql(table.clone(), refresh_sql, schema).expect("valid refresh SQL");
        let stream = get_data(
            &mut ctx,
            table,
            provider,
            Some(parsed.to_scan_sql()),
            filters,
            parsed.distinct_on(),
        )
        .await
        .expect("refresh scan plans");
        let mut batches: Vec<RecordBatch> = stream.try_collect().await.expect("scan runs");
        batches.retain(|b| b.num_rows() > 0);
        let sorted =
            arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        let order = arrow::compute::sort_to_indices(sorted.column(0), None, None).expect("sort");
        let sorted = arrow::compute::take_record_batch(&sorted, &order).expect("take");
        pretty_format_batches(&[sorted])
            .expect("format")
            .to_string()
    }

    #[tokio::test]
    async fn distinct_on_keeps_the_latest_row_per_key() {
        let out = latest_rows(
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC NULLS LAST",
            vec![],
        )
        .await;
        assert!(out.contains("| 1  | 10          | new  |"), "{out}");
        assert!(out.contains("| 2  | 5           | only |"), "{out}");
        assert_eq!(
            out.lines().filter(|l| l.starts_with("| ")).count(),
            3,
            "{out}"
        );
    }

    // `DESC` with no NULLS clause sorts NULLs first, so the NULL-time row is kept;
    // this is what the registration-time info message warns about.
    #[tokio::test]
    async fn distinct_on_desc_without_nulls_clause_keeps_the_null_row() {
        let out = latest_rows(
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC",
            vec![],
        )
        .await;
        assert!(out.contains("null-time"), "{out}");
    }

    // The refresh's own filters (the append window, partitions) must apply before
    // DISTINCT ON: filtering after it would drop key 1 entirely, because its newest
    // row is outside the filter.
    #[tokio::test]
    async fn distinct_on_runs_after_the_refresh_filters() {
        let out = latest_rows(
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC NULLS LAST",
            vec![col("occurred_at").lt(lit(9_i64))],
        )
        .await;
        assert!(out.contains("| 1  | 8           | old  |"), "{out}");
    }
}
