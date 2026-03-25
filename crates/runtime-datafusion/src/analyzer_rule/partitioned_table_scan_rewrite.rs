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
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result, ToDFSchema,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        EmptyRelation, Expr, Limit, LogicalPlan, LogicalPlanBuilder, Sort, SubqueryAlias,
        TableScan, Union, lit,
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
/// After rewriting `TableScan` into `Union(sub-scans...)`, a second pass pushes any `Sort` +
/// `Limit` (or standalone `Limit`) that sits above the `Union` into each union leg. This
/// ensures that remote executors return at most N rows each, while the outer `Sort` + `Limit`
/// still runs on the merged result for correctness.
///
/// For example, suppose we want to do it on `sales`. Then we go from this
///
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   TableScan: sales projection=[order_number, phone, postal_code], full_filters=[sales.status = Utf8("Disputed")]
/// ```
/// To something like this:
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   SubqueryAlias: sales
///    Union
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales
///        projection=[order_number, phone, postal_code]
///        full_filters=[ sales.status = Utf8("Disputed"), partition_id = Utf8("0") ]
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales
///        projection=[order_number, phone, postal_code]
///        full_filters=[ sales.status = Utf8("Disputed"), partition_id = Utf8("1") ]
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
        // Phase 1: Rewrite TableScan nodes into Union of per-partition sub-scans.
        let plan = plan
            .transform_up_with_subqueries(|plan| {
                let LogicalPlan::TableScan(scan) = &plan else {
                    return Ok(Transformed::no(plan));
                };
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
                Ok(Transformed::yes(result))
            })
            .data()?;

        // Phase 2: Push Sort + Limit (or standalone Limit) into each union leg so that
        // remote executors return at most N rows each, reducing data transfer.
        // The outer Sort + Limit is preserved for correctness on the merged result.
        push_limit_into_union_legs(plan)
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
    }
}

/// Attempt to push `Sort` + `Limit` (or a standalone `Limit`) down into the legs of a `Union`
/// produced by the partitioned table scan rewrite. This is a top-down rewrite: we match the
/// pattern at the top and recurse into children for any remaining opportunities.
///
/// Matched patterns (where `SubqueryAlias` is the alias wrapper added by the rewrite):
///
/// 1. `Limit -> Sort -> SubqueryAlias -> Union`
///    Push `Sort(expr, fetch=skip+limit)` into each union leg.
///
/// 2. `Limit -> SubqueryAlias -> Union`
///    Push `Limit(0, skip+limit)` into each union leg.
///
/// 3. `Sort(fetch=Some) -> SubqueryAlias -> Union`
///    Push `Sort(expr, fetch)` into each union leg.
fn push_limit_into_union_legs(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    plan.transform_down(|plan| {
        match &plan {
            // Pattern 1 & 2: Limit -> ...
            LogicalPlan::Limit(limit) => {
                let inner_fetch = resolve_fetch(limit);
                let inner_skip = resolve_skip(limit);
                let Some(fetch) = inner_fetch else {
                    // No fetch means no meaningful limit to push down.
                    return Ok(Transformed::no(plan));
                };
                // The per-leg limit is skip + fetch so that after the outer Limit
                // applies the skip, we still have enough rows.
                let per_leg_fetch = inner_skip.unwrap_or(0).saturating_add(fetch);

                match limit.input.as_ref() {
                    // Pattern 1: Limit -> Sort -> SubqueryAlias -> Union
                    LogicalPlan::Sort(sort) => {
                        if let Some(union_inputs) = unwrap_subquery_alias_union(sort.input.as_ref())
                        {
                            let new_inputs =
                                push_sort_limit_into_legs(union_inputs, &sort.expr, per_leg_fetch)?;
                            let new_union = rebuild_union_alias(sort.input.as_ref(), new_inputs)?;
                            let new_sort = LogicalPlan::Sort(Sort {
                                expr: sort.expr.clone(),
                                input: Arc::new(new_union),
                                fetch: sort.fetch,
                            });
                            let new_plan = LogicalPlan::Limit(Limit {
                                skip: limit.skip.clone(),
                                fetch: limit.fetch.clone(),
                                input: Arc::new(new_sort),
                            });
                            Ok(Transformed::yes(new_plan))
                        } else {
                            Ok(Transformed::no(plan))
                        }
                    }
                    // Pattern 2: Limit -> SubqueryAlias -> Union (no sort)
                    subquery_alias_plan => {
                        if let Some(union_inputs) = unwrap_subquery_alias_union(subquery_alias_plan)
                        {
                            let new_inputs = push_limit_into_legs(union_inputs, per_leg_fetch)?;
                            let new_union = rebuild_union_alias(subquery_alias_plan, new_inputs)?;
                            let new_plan = LogicalPlan::Limit(Limit {
                                skip: limit.skip.clone(),
                                fetch: limit.fetch.clone(),
                                input: Arc::new(new_union),
                            });
                            Ok(Transformed::yes(new_plan))
                        } else {
                            Ok(Transformed::no(plan))
                        }
                    }
                }
            }

            // Pattern 3: Sort(fetch=Some) -> SubqueryAlias -> Union
            // DataFusion can fold Limit into Sort.fetch. Handle that case too.
            LogicalPlan::Sort(sort) => {
                let Some(fetch) = sort.fetch else {
                    return Ok(Transformed::no(plan));
                };
                if let Some(union_inputs) = unwrap_subquery_alias_union(sort.input.as_ref()) {
                    let new_inputs = push_sort_limit_into_legs(union_inputs, &sort.expr, fetch)?;
                    let new_union = rebuild_union_alias(sort.input.as_ref(), new_inputs)?;
                    let new_plan = LogicalPlan::Sort(Sort {
                        expr: sort.expr.clone(),
                        input: Arc::new(new_union),
                        fetch: sort.fetch,
                    });
                    Ok(Transformed::yes(new_plan))
                } else {
                    Ok(Transformed::no(plan))
                }
            }

            _ => Ok(Transformed::no(plan)),
        }
    })
    .data()
}

/// If `plan` is `SubqueryAlias -> Union`, return the union inputs.
fn unwrap_subquery_alias_union(plan: &LogicalPlan) -> Option<&[Arc<LogicalPlan>]> {
    if let LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) = plan
        && let LogicalPlan::Union(Union { inputs, .. }) = input.as_ref()
    {
        return Some(inputs);
    }
    None
}

/// Rebuild a `SubqueryAlias(Union(...))` with new union inputs, preserving the alias.
fn rebuild_union_alias(
    original_plan: &LogicalPlan,
    new_inputs: Vec<Arc<LogicalPlan>>,
) -> Result<LogicalPlan, DataFusionError> {
    if let LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) = original_plan {
        let new_union = LogicalPlan::Union(Union::try_new(new_inputs)?);
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(new_union),
            alias.clone(),
        )?))
    } else {
        Err(DataFusionError::Internal(
            "Expected SubqueryAlias wrapping Union".to_string(),
        ))
    }
}

/// Push `Sort(exprs, fetch=per_leg_fetch)` into each union leg.
fn push_sort_limit_into_legs(
    inputs: &[Arc<LogicalPlan>],
    sort_exprs: &[datafusion::logical_expr::SortExpr],
    per_leg_fetch: usize,
) -> Result<Vec<Arc<LogicalPlan>>, DataFusionError> {
    inputs
        .iter()
        .map(|leg| {
            let sorted = LogicalPlan::Sort(Sort {
                expr: sort_exprs.to_vec(),
                input: Arc::clone(leg),
                fetch: Some(per_leg_fetch),
            });
            Ok(Arc::new(sorted))
        })
        .collect()
}

/// Push `Limit(0, per_leg_fetch)` into each union leg (no sort).
fn push_limit_into_legs(
    inputs: &[Arc<LogicalPlan>],
    per_leg_fetch: usize,
) -> Result<Vec<Arc<LogicalPlan>>, DataFusionError> {
    inputs
        .iter()
        .map(|leg| {
            let limited = LogicalPlan::Limit(Limit {
                skip: None,
                fetch: Some(Box::new(lit(
                    i64::try_from(per_leg_fetch).unwrap_or(i64::MAX)
                ))),
                input: Arc::clone(leg),
            });
            Ok(Arc::new(limited))
        })
        .collect()
}

/// Resolve the `fetch` value from a [`Limit`] node to a concrete `usize`, if possible.
fn resolve_fetch(limit: &Limit) -> Option<usize> {
    match limit.get_fetch_type() {
        Ok(datafusion::logical_expr::FetchType::Literal(v)) => v,
        _ => None,
    }
}

/// Resolve the `skip` value from a [`Limit`] node to a concrete `usize`, if possible.
fn resolve_skip(limit: &Limit) -> Option<usize> {
    match limit.get_skip_type() {
        Ok(datafusion::logical_expr::SkipType::Literal(v)) => {
            if v == 0 {
                None
            } else {
                Some(v)
            }
        }
        _ => None,
    }
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
        datasource::MemTable,
        logical_expr::LogicalPlanBuilder,
        prelude::SessionContext,
    };

    /// A mock partition provider that creates `n` partitions for any table named "test_table".
    #[derive(Debug)]
    struct MockPartitionProvider {
        num_partitions: usize,
        schema: SchemaRef,
    }

    impl TablePartitionProvider for MockPartitionProvider {
        fn get_partitions(
            &self,
            _table: &TableReference,
            _schema: &SchemaRef,
        ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
            (0..self.num_partitions)
                .map(|i| {
                    let provider: Arc<dyn TableProvider> = Arc::new(
                        MemTable::try_new(Arc::clone(&self.schema), vec![vec![]]).unwrap(),
                    );
                    let mut pv = HashMap::new();
                    pv.insert("partition_id".to_string(), i.to_string());
                    (provider, vec![pv])
                })
                .collect()
        }

        fn should_partition(&self, scan: &TableScan) -> bool {
            scan.table_name.to_string() == "test_table"
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("partition_id", DataType::Utf8, true),
        ]))
    }

    async fn setup_ctx(num_partitions: usize) -> (SessionContext, Arc<MockPartitionProvider>) {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
        ctx.register_table("test_table", Arc::new(table)).unwrap();

        let provider = Arc::new(MockPartitionProvider {
            num_partitions,
            schema,
        });
        (ctx, provider)
    }

    fn format_plan(plan: &LogicalPlan) -> String {
        format!("{}", plan.display_indent())
    }

    #[tokio::test]
    async fn test_basic_table_scan_rewrite() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // Should produce a SubqueryAlias wrapping a Union of 2 sub-scans
        assert!(
            formatted.contains("SubqueryAlias: test_table"),
            "Expected SubqueryAlias in plan:\n{formatted}"
        );
        assert!(
            formatted.contains("Union"),
            "Expected Union in plan:\n{formatted}"
        );
    }

    #[tokio::test]
    async fn test_limit_sort_pushdown_into_union() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> TableScan(test_table)
        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // The outer Limit + Sort should still be present
        assert!(
            formatted.contains("Limit:"),
            "Expected outer Limit in plan:\n{formatted}"
        );
        assert!(
            formatted.contains("Sort:"),
            "Expected outer Sort in plan:\n{formatted}"
        );

        // Each union leg should have a Sort with fetch=5 pushed down
        let sort_count = formatted.matches("Sort:").count();
        // Outer sort + 2 inner sorts = 3 total
        assert!(
            sort_count >= 3,
            "Expected at least 3 Sort nodes (1 outer + 2 per-leg), got {sort_count} in plan:\n{formatted}"
        );

        insta::assert_snapshot!("limit_sort_pushdown", formatted);
    }

    #[tokio::test]
    async fn test_limit_sort_with_skip_pushdown() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Build: Limit(skip=10, fetch=5) -> Sort(id ASC) -> TableScan(test_table)
        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .limit(10, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // Per-leg fetch should be skip + fetch = 10 + 5 = 15
        assert!(
            formatted.contains("fetch=15"),
            "Expected fetch=15 in per-leg Sort, got plan:\n{formatted}"
        );

        insta::assert_snapshot!("limit_sort_with_skip_pushdown", formatted);
    }

    #[tokio::test]
    async fn test_limit_without_sort_pushdown() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Build: Limit(fetch=5) -> TableScan(test_table)
        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // The outer Limit should still be present
        assert!(
            formatted.contains("Limit:"),
            "Expected outer Limit in plan:\n{formatted}"
        );

        // Each union leg should have a Limit pushed down
        let limit_count = formatted.matches("Limit:").count();
        // Outer limit + 2 inner limits = 3 total
        assert!(
            limit_count >= 3,
            "Expected at least 3 Limit nodes (1 outer + 2 per-leg), got {limit_count} in plan:\n{formatted}"
        );

        insta::assert_snapshot!("limit_without_sort_pushdown", formatted);
    }

    #[tokio::test]
    async fn test_no_pushdown_without_limit() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Build: Sort(id ASC) -> TableScan(test_table) -- no limit, no pushdown
        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // Only 1 Sort node (the outer one), no pushdown
        let sort_count = formatted.matches("Sort:").count();
        assert_eq!(
            sort_count, 1,
            "Expected exactly 1 Sort node (no pushdown without limit), got {sort_count} in plan:\n{formatted}"
        );

        insta::assert_snapshot!("no_pushdown_without_limit", formatted);
    }

    #[tokio::test]
    async fn test_single_partition_no_union() {
        let (ctx, provider) = setup_ctx(1).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Single partition: no Union, just direct sub-scan
        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // No Union for single partition
        assert!(
            !formatted.contains("Union"),
            "Expected no Union for single partition:\n{formatted}"
        );

        insta::assert_snapshot!("single_partition_no_union", formatted);
    }

    #[tokio::test]
    async fn test_no_partitions_empty_relation() {
        let (ctx, provider) = setup_ctx(0).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        assert!(
            formatted.contains("EmptyRelation"),
            "Expected EmptyRelation for 0 partitions:\n{formatted}"
        );

        insta::assert_snapshot!("no_partitions_empty_relation", formatted);
    }

    #[tokio::test]
    async fn test_non_partitioned_table_unchanged() {
        let (ctx, provider) = setup_ctx(2).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        // Register another table that won't be partitioned
        let schema = test_schema();
        let other_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
        ctx.register_table("other_table", Arc::new(other_table))
            .unwrap();

        let plan = LogicalPlanBuilder::scan(
            "other_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("other_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .limit(0, Some(5))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let before = format_plan(&plan);
        let result = rule.analyze(plan, &config).unwrap();
        let after = format_plan(&result);

        assert_eq!(before, after, "Non-partitioned table should be unchanged");
    }

    #[tokio::test]
    async fn test_three_partitions_pushdown() {
        let (ctx, provider) = setup_ctx(3).await;
        let rule = PartitionedTableScanRewrite::new(provider, &ctx);

        let plan = LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(
                ctx.table_provider("test_table").await.unwrap(),
            )),
            None,
        )
        .unwrap()
        .sort(vec![datafusion::prelude::col("id").sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();

        let config = ConfigOptions::default();
        let result = rule.analyze(plan, &config).unwrap();
        let formatted = format_plan(&result);

        // With 3 partitions, LogicalPlanBuilder::union() creates nested unions:
        // Union(Union(leg0, leg1), leg2). The pushdown adds Sort to each leg of
        // the outermost Union, giving 1 outer + 2 per-outer-union-leg = 3 Sort nodes.
        let sort_count = formatted.matches("Sort:").count();
        assert!(
            sort_count >= 3,
            "Expected at least 3 Sort nodes (1 outer + 2 per outer union leg), got {sort_count} in plan:\n{formatted}"
        );

        insta::assert_snapshot!("three_partitions_pushdown", formatted);
    }
}
