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
        tree_node::{Transformed, TransformedResult},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, TableScan, Union, lit},
    optimizer::AnalyzerRule,
    prelude::SessionContext,
    sql::TableReference,
};
use parking_lot::RwLock;

mod limit_leg_trim;
mod topk_pushdown;

use limit_leg_trim::try_trim_union_legs;
use topk_pushdown::push_sort_topk_into_union;

#[cfg(test)]
mod test_utils;

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
pub type PartitionValue = HashMap<String, Option<String>>;

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
/// On top of the base scan -> `UNION ALL` rewrite, two `Limit`-driven optimizations are applied
/// to the unions this rule produces (see the respective submodules):
///
/// - [`limit_leg_trim`]: for an unordered `Limit N` over a `Union`, drop whole legs when exact
///   statistics prove a subset of legs already holds enough rows.
/// - [`topk_pushdown`]: for an ordered `Limit N` (a `Sort` between the `Limit` and the `Union`),
///   push `Sort(TopK)` into each leg so executors return at most `skip + fetch` rows.
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
        let mut rewrite_occurred = false;
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::TableScan(scan) = &plan else {
                // Apply Limit-driven optimizations to unions produced by this rule. Only attempt
                // them when at least one TableScan has been rewritten to a Union in this plan
                // traversal. Note: this may fire for any matching Limit subtree in the plan, not
                // exclusively for unions produced by this rule. This is okay: the helpers below
                // ensure they only fire on an applicable `Limit`.
                if rewrite_occurred && let LogicalPlan::Limit(limit) = plan {
                    // First, try to drop union legs entirely when exact statistics
                    // guarantee the limit can be satisfied by a subset of legs (no
                    // Sort/Filter between Limit and Union, no filters on kept legs).
                    if let Some(trimmed) = try_trim_union_legs(&limit)? {
                        return Ok(Transformed::yes(trimmed));
                    }
                    // Otherwise, push Sort(TopK) into each leg when a Sort is present.
                    return push_sort_topk_into_union(limit);
                }
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

                if let Some(partition_filter) =
                    util::expr::combine_exprs_balanced(partition_exprs, Expr::or)
                {
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
            if sub_scans.is_empty() {
                return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                    EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::clone(plan.schema()),
                    },
                )));
            }

            let result = LogicalPlanBuilder::new(LogicalPlan::Union(Union {
                inputs: sub_scans,
                schema: Arc::clone(plan.schema()),
            }))
            .alias(scan.table_name.clone())?
            .build()?;

            rewrite_occurred = true;
            Ok(Transformed::yes(result))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
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
        let col_expr = state
            .upgrade()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "SessionState has been dropped, cannot parse partition expression".to_string(),
                )
            })?
            .read()
            .create_logical_expr(partition_expr_str, df_schema)?;
        let new_expr = match val {
            None => col_expr.is_null(),
            Some(v) => col_expr.eq(lit(v.clone())),
        };
        expr = match expr {
            Some(existing) => Some(existing.and(new_expr)),
            None => Some(new_expr),
        };
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::test_utils::{make_rule, make_table_scan, test_schema};
    use datafusion::{config::ConfigOptions, optimizer::AnalyzerRule, prelude::SessionContext};

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
}
