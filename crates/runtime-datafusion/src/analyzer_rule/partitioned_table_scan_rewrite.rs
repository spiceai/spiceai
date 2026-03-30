/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::{
    cell::Cell,
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        tree_node::{Transformed, TransformedResult},
        Result, ToDFSchema,
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        lit, EmptyRelation, Expr, FetchType, Limit, LogicalPlan, LogicalPlanBuilder, SkipType,
        Sort, TableScan, Union,
    },
    optimizer::AnalyzerRule,
    prelude::SessionContext,
    sql::TableReference,
};
use parking_lot::RwLock;

/// A specific value for partitioning keys.
/// For example, if a table is partitioned by:
///  - "date"
///  - "region"
///
/// Unique `PartitionValue`s might be (i.e. `Vec<PartitionValue>`):
/// ```json
/// {"date": "2024-01-01", "region": "us-east"}
/// {"date": "2024-01-01", "region": "us-west"}
/// {"date": "2024-01-02", "region": "us-east"}
/// ```
pub type PartitionValue = HashMap<String, String>;

/// Define how to get partitions for a given table, and how they are partitioned.
pub trait TablePartitionProvider: Send + Sync + Debug {
    /// Get partitions for a given [`TableReference`].
    ///
    /// `schema`: The schema of the table locally. Expect all returned [`TableProvider`] to conform to this schema.
    /// Returns pairs of [`TableProvider`] and the partition values (as string key-value maps) that
    /// they are responsible for. The analyzer rule converts these into filter [`Expr`]s.
    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)>;

    /// Whether partitioning should be applied to the given table.
    fn should_partition(&self, tbl: &TableScan) -> bool;
}

/// An [`AnalyzerRule`] that rewrites table scans on a single locally registered table as the
/// `UNION ALL` of one or more partitions of this table (possibly from a different source).
///
/// Additionally, when a `Limit -> Sort -> Union` pattern is created by this rewrite, it pushes
/// `Sort(TopK)` into each union leg so that each partition returns at most `skip + fetch` rows.
///
/// For example, suppose we want to partition `sales`. We go from this:
///
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   Projection: sales.order_number, sales.phone, sales.postal_code
///    TableScan: sales projection=[order_number, phone, postal_code], full_filters=[sales.status = Utf8("Disputed")]
/// ```
/// To:
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   Projection: sales.order_number, sales.phone, sales.postal_code
///    Union
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales@executor-1
///        full_filters=[sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x143A...]
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales@executor-2
///        full_filters=[sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x896...]
/// ```
pub struct PartitionedTableScanRewrite {
    partition_provider: Arc<dyn TablePartitionProvider>,
    // Avoid holding a strong reference to SessionState to prevent circular references (SessionState -> AnalyzerRule -> SessionState). We only need it to parse partition expressions.
    session_state: Weak<RwLock<SessionState>>,
}

impl PartitionedTableScanRewrite {
    pub fn new(
        partition_provider: Arc<dyn TablePartitionProvider>,
        session_ctx: &SessionContext,
    ) -> Self {
        Self {
            partition_provider,
            session_state: session_ctx.state_weak_ref(),
        }
    }
}

impl Debug for PartitionedTableScanRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedTableScanRewrite")
            .field("partition_provider", &self.partition_provider)
            .finish_non_exhaustive()
    }
}

impl AnalyzerRule for PartitionedTableScanRewrite {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut rewrite_occurred = false; // Cell::new(false);
        plan.transform_up_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = &plan {
                if !self.partition_provider.should_partition(scan) {
                    return Ok(Transformed::no(plan));
                }

                let schema = scan.source.schema();
                let providers = self
                    .partition_provider
                    .get_partitions(&scan.table_name, &schema);

                tracing::debug!(
                    "PartitionedTableScanRewrite: {} partitions for '{}' table.",
                    providers.len(),
                    scan.table_name
                );

                // Pre-compute DFSchema for partition expression parsing
                let df_schema = schema.to_dfschema()?;

                let mut sub_scans = Vec::with_capacity(providers.len());
                for (provider, partition_values) in providers {
                    let mut filters = scan.filters.clone();

                    // Convert partition values (HashMap<String, String>) to filter Exprs and combine with OR.
                    let partition_exprs: Vec<Expr> = partition_values
                        .iter()
                        .filter_map(|pv| {
                            partition_value_to_expr(pv, &df_schema, &self.session_state).transpose()
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    if let Some(partition_filter) = partition_exprs.into_iter().reduce(Expr::or) {
                        filters.push(partition_filter);
                    }
                    let plan = LogicalPlanBuilder::scan_with_filters(
                        scan.table_name.clone(),
                        Arc::new(DefaultTableSource::new(Arc::clone(&provider))),
                        scan.projection.clone(),
                        filters,
                    )?
                    .build()?;
                    sub_scans.push(Arc::new(plan));
                }

                // If no partitions, return empty relation. This can happen if no partitions match the table (even if we want to partition it).
                let Some(first_scan) = sub_scans.first() else {
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(plan.schema()),
                        },
                    )));
                };
                let first_scan = Arc::unwrap_or_clone(Arc::clone(first_scan));
                let sub_scans = sub_scans.into_iter().skip(1).collect::<Vec<_>>();

                // Single partition: no Union needed, just return the sub-scan directly.
                if sub_scans.is_empty() {
                    return Ok(Transformed::yes(first_scan));
                }

                let mut builder = LogicalPlanBuilder::from(first_scan);
                for scan in sub_scans {
                    builder = builder.union(Arc::unwrap_or_clone(scan))?;
                }
                let result = builder.alias(scan.table_name.clone())?.build()?;
                rewrite_occurred = true;
                return Ok(Transformed::yes(result));
            }

            // Push Sort(TopK) into each Union leg when Limit sits above Sort -> Union.
            // DataFusion's optimizer pushes Limit through Union, but does not push
            // Sort(TopK) through Union. Without this, each executor returns all rows
            // and the scheduler sorts the full merged result.
            // Only attempt this when the partition rewrite created a union above.
            if rewrite_occurred {
                if let LogicalPlan::Limit(limit) = plan {
                    return push_sort_topk_into_union(limit);
                }
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
    }
}

/// When `Limit -> Sort -> [Projection|SubqueryAlias]* -> Union(sub_scans)`, push
/// `Sort(fetch = skip + fetch)` into each union leg. This enables per-executor `TopK`,
/// reducing data transfer from executors to the scheduler.
///
/// The outer `Limit -> Sort` and any intermediate nodes (Projection, SubqueryAlias) are
/// preserved for correct final merge-sort, projection, and limiting.
fn push_sort_topk_into_union(limit: Limit) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };

    // Walk through Projection and SubqueryAlias nodes to find the Union.
    // These are "transparent" nodes that don't affect sort order.
    let mut current = sort.input.as_ref();
    let mut intermediates: Vec<&LogicalPlan> = Vec::new();
    let union_plan = loop {
        match current {
            LogicalPlan::Union(u) => break u,
            LogicalPlan::Projection(p) => {
                intermediates.push(current);
                current = p.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(sa) => {
                intermediates.push(current);
                current = sa.input.as_ref();
            }
            _ => return Ok(Transformed::no(LogicalPlan::Limit(limit))),
        }
    };

    let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };
    let SkipType::Literal(skip) = limit.get_skip_type()? else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };
    let Some(effective_fetch) = skip.checked_add(fetch) else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };

    let new_inputs: Vec<Arc<LogicalPlan>> = union_plan
        .inputs
        .iter()
        .map(|input| {
            Arc::new(LogicalPlan::Sort(Sort {
                expr: sort.expr.clone(),
                input: Arc::clone(input),
                fetch: Some(effective_fetch),
            }))
        })
        .collect();

    // Rebuild: new Union → intermediate nodes (reversed) → Sort → Limit.
    let mut result = LogicalPlan::Union(Union {
        inputs: new_inputs,
        schema: Arc::clone(&union_plan.schema),
    });

    // Re-wrap intermediate nodes in reverse (innermost-first) order.
    for node in intermediates.into_iter().rev() {
        result = match node {
            LogicalPlan::SubqueryAlias(sa) => LogicalPlanBuilder::from(result)
                .alias(sa.alias.clone())?
                .build()?,
            LogicalPlan::Projection(p) => LogicalPlanBuilder::from(result)
                .project(p.expr.clone())?
                .build()?,
            _ => unreachable!("only Projection and SubqueryAlias are collected"),
        };
    }

    Ok(Transformed::yes(LogicalPlan::Limit(Limit {
        skip: limit.skip.clone(),
        fetch: limit.fetch.clone(),
        input: Arc::new(LogicalPlan::Sort(Sort {
            expr: sort.expr.clone(),
            input: Arc::new(result),
            fetch: sort.fetch,
        })),
    })))
}

/// Converts a [`PartitionValue`] (e.g. `{"bucket(3, org_id)": "42"}`) into a filter [`Expr`]
/// (e.g. `bucket(3, org_id) = '42'`). Multiple keys are `AND`ed together.
fn partition_value_to_expr(
    pv: &PartitionValue,
    df_schema: &datafusion::common::DFSchema,
    state: &Weak<RwLock<SessionState>>,
) -> Result<Option<Expr>, DataFusionError> {
    let mut expr: Option<Expr> = None;
    for (partition_expr_str, val) in pv {
        let new_expr = state
            .upgrade()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "SessionState has been dropped, cannot parse partition expression".to_string(),
                )
            })?
            .read()
            .create_logical_expr(partition_expr_str, df_schema)?
            .eq(lit(val.clone()));
        expr = match expr {
            Some(existing) => Some(existing.and(new_expr)),
            None => Some(new_expr),
        };
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::empty::EmptyTable,
        logical_expr::{col, LogicalPlanBuilder, SortExpr},
        prelude::SessionContext,
    };
    use std::collections::HashMap;

    /// A test partition provider that splits any table into two partitions.
    #[derive(Debug)]
    struct TwoPartitionProvider {
        schema: SchemaRef,
    }

    impl TablePartitionProvider for TwoPartitionProvider {
        fn get_partitions(
            &self,
            _table: &TableReference,
            _schema: &SchemaRef,
        ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
            let p1: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
            let p2: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
            vec![
                (
                    p1,
                    vec![HashMap::from([(
                        "partition_id".to_string(),
                        "0".to_string(),
                    )])],
                ),
                (
                    p2,
                    vec![HashMap::from([(
                        "partition_id".to_string(),
                        "1".to_string(),
                    )])],
                ),
            ]
        }

        fn should_partition(&self, _tbl: &TableScan) -> bool {
            true
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("partition_id", DataType::Int32, false),
        ]))
    }

    fn make_rule(schema: &SchemaRef, ctx: &SessionContext) -> PartitionedTableScanRewrite {
        PartitionedTableScanRewrite::new(
            Arc::new(TwoPartitionProvider {
                schema: Arc::clone(schema),
            }),
            ctx,
        )
    }

    fn make_table_scan(schema: &SchemaRef) -> LogicalPlan {
        let source: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(schema)));
        LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(source)),
            None,
        )
        .expect("failed to build scan")
        .build()
        .expect("failed to build plan")
    }

    #[test]
    fn test_table_scan_rewritten_to_union() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);
        let plan = make_table_scan(&schema);

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r#"
        SubqueryAlias: test_table
          Union
            TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
            TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_pushdown_through_union() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_with_offset_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(skip=10, fetch=5) -> Sort(id ASC) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(10, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=10, fetch=5
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=15
                  TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=15
                  TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_pushdown_through_projection_and_union() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> Projection(id, name) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("id"), col("name")])
            .expect("project failed")
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          Sort: test_table.id ASC NULLS LAST
            Projection: test_table.id, test_table.name
              SubqueryAlias: test_table
                Union
                  Sort: test_table.id ASC NULLS LAST, fetch=5
                    TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                  Sort: test_table.id ASC NULLS LAST, fetch=5
                    TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_without_sort_no_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> TableScan (no Sort)
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // No Sort in original plan, so no Sort(TopK) should be pushed into union legs.
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          SubqueryAlias: test_table
            Union
              TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
              TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }
}
