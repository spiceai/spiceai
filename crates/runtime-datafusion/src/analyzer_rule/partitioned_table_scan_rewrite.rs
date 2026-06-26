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
        stats::Precision,
        tree_node::{Transformed, TransformedResult},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider, source_as_provider},
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        EmptyRelation, Expr, FetchType, Limit, LogicalPlan, LogicalPlanBuilder, SkipType, Sort,
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
/// Additionally, when a `Limit -> [Projection|SubqueryAlias]* -> Sort -> [Projection|SubqueryAlias]* -> Union`
/// pattern is created by this rewrite, it pushes `Sort(TopK)` into each union leg so that each
/// partition returns at most `skip + fetch` rows.
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
                // Push Sort(TopK) into each Union leg when Limit sits above Sort -> Union.
                // DataFusion's optimizer pushes Limit through Union, but does not push
                // Sort(TopK) through Union. Without this, each executor returns all rows
                // and the scheduler sorts the full merged result.
                //
                // Only attempt this when at least one TableScan has been rewritten to a
                // Union in this plan traversal. Note: this may fire for any matching
                // Limit -> [Projection|SubqueryAlias]* -> Sort -> [Projection|SubqueryAlias]* -> Union
                // subtree in the plan, not exclusively for unions produced by this rule.
                //
                // This is okay. This is just an optimisation. `push_sort_topk_into_union` will ensure it is an applicable `Limit`.
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

/// When `Limit(skip, fetch) -> [Projection|SubqueryAlias]* -> Union(legs)` (no `Sort` or
/// `Filter` between the `Limit` and the `Union`), try to satisfy the limit using only a subset
/// of legs whose exact row counts already cover `effective_fetch = skip + fetch`.
///
/// Because there is no ordering, `LIMIT N` may be answered by any `N` rows of the union. If a
/// subset of legs is guaranteed (via `Precision::Exact` statistics) to hold at least
/// `effective_fetch` rows, the remaining legs are redundant and can be dropped, removing the
/// need to dispatch a `FlightSqlExec` (or equivalent) to those executors.
///
/// Returns `Some(new_plan)` only when legs are actually dropped; `None` otherwise (caller then
/// falls back to existing behavior, e.g. `push_sort_topk_into_union`).
///
/// Safety conditions (all required):
/// - The `Limit` has a literal `fetch` and `skip`.
/// - There is no `Sort` or `Filter` between the `Limit` and the `Union` (these would change
///   which / how many rows survive, breaking the row-count guarantee).
/// - A kept leg has no `Filter` and its `TableScan` has no `filters` (a filter could reduce the
///   actual row count below the reported `num_rows`).
/// - The kept legs report `num_rows` as `Precision::Exact` and sum to `>= effective_fetch`.
fn try_trim_union_legs(limit: &Limit) -> Result<Option<LogicalPlan>, DataFusionError> {
    let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
        return Ok(None);
    };
    let SkipType::Literal(skip) = limit.get_skip_type()? else {
        return Ok(None);
    };
    let Some(effective_fetch) = skip.checked_add(fetch) else {
        return Ok(None);
    };
    if effective_fetch == 0 {
        return Ok(None);
    }

    // Walk Limit -> [Projection|SubqueryAlias]* -> Union. A Sort or Filter (or anything else)
    // disqualifies trimming.
    let mut current = limit.input.as_ref();
    let mut intermediates: Vec<&LogicalPlan> = Vec::new();
    let union = loop {
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
            _ => return Ok(None),
        }
    };

    if union.inputs.len() < 2 {
        return Ok(None);
    }

    // Collect legs with no filters that report an exact row count.
    let mut eligible: Vec<(usize, usize)> = Vec::new();
    for (i, leg) in union.inputs.iter().enumerate() {
        if let Some(num_rows) = leg_exact_rows(leg)? {
            eligible.push((i, num_rows));
        }
    }
    if eligible.is_empty() {
        return Ok(None);
    }

    // Greedily keep the largest legs first so the minimal number of legs is retained.
    eligible.sort_by(|a, b| b.1.cmp(&a.1));
    let mut keep_indices: Vec<usize> = Vec::new();
    let mut cumulative: usize = 0;
    for (i, num_rows) in eligible {
        keep_indices.push(i);
        cumulative = cumulative.saturating_add(num_rows);
        if cumulative >= effective_fetch {
            break;
        }
    }

    // Exact legs cannot guarantee enough rows, or trimming would drop nothing: fall back.
    if cumulative < effective_fetch || keep_indices.len() >= union.inputs.len() {
        return Ok(None);
    }

    keep_indices.sort_unstable();
    let kept: Vec<Arc<LogicalPlan>> = keep_indices
        .iter()
        .map(|&i| Arc::clone(&union.inputs[i]))
        .collect();

    // When a single leg suffices, drop the Union entirely; otherwise rebuild it with the
    // retained legs. The preserved [Projection|SubqueryAlias]* wrappers (which include the
    // SubqueryAlias added above the Union by this rule) keep qualified references resolvable.
    let node: Arc<LogicalPlan> = if kept.len() == 1 {
        Arc::clone(&kept[0])
    } else {
        Arc::new(LogicalPlan::Union(Union {
            inputs: kept,
            schema: Arc::clone(&union.schema),
        }))
    };

    let mut builder = LogicalPlanBuilder::from(node);
    for node in intermediates.iter().rev() {
        builder = match node {
            LogicalPlan::SubqueryAlias(sa) => builder.alias(sa.alias.clone())?,
            LogicalPlan::Projection(p) => builder.project(p.expr.clone())?,
            _ => unreachable!("only Projection and SubqueryAlias are collected"),
        };
    }

    Ok(Some(LogicalPlan::Limit(Limit {
        skip: limit.skip.clone(),
        fetch: limit.fetch.clone(),
        input: Arc::new(builder.build()?),
    })))
}

/// Returns the exact row count for a union leg if it is eligible for limit-based trimming, i.e.
/// the leg is a `TableScan` (optionally wrapped in row-count-preserving `SubqueryAlias`/
/// `Projection` nodes) with no filters whose provider reports `num_rows` as `Precision::Exact`.
///
/// Returns `None` when the leg has any filter, contains a non-row-preserving node, or the
/// provider does not report an exact row count.
fn leg_exact_rows(leg: &LogicalPlan) -> Result<Option<usize>, DataFusionError> {
    let mut current = leg;
    loop {
        match current {
            LogicalPlan::TableScan(scan) => {
                if !scan.filters.is_empty() {
                    return Ok(None);
                }
                let provider = source_as_provider(&scan.source)?;
                let Some(stats) = provider.statistics() else {
                    return Ok(None);
                };
                return Ok(match stats.num_rows {
                    Precision::Exact(num_rows) => Some(num_rows),
                    Precision::Inexact(_) | Precision::Absent => None,
                });
            }
            LogicalPlan::SubqueryAlias(sa) => current = sa.input.as_ref(),
            LogicalPlan::Projection(p) => current = p.input.as_ref(),
            _ => return Ok(None),
        }
    }
}

/// When `Limit -> [Projection|SubqueryAlias]* -> Sort -> [Projection|SubqueryAlias|Filter]* -> Union(sub_scans)`,
/// push `Sort(fetch = skip + fetch)` into each union leg. This enables per-executor `TopK`,
/// reducing data transfer from executors to the scheduler.
///
/// The outer `Limit -> Sort` and any intermediate `Projection`/`SubqueryAlias` nodes are
/// preserved for correct final merge-sort, projection, and limiting.
///
/// `Filter` nodes between Sort and Union are pushed into each union leg (above the leg's
/// input, below `Sort(TopK)`) so federation can unparse the complete
/// `SELECT ... WHERE ... ORDER BY ... LIMIT` query for each executor.
///
/// Projection nodes may appear between Limit and Sort when the SELECT list differs from
/// the columns required by ORDER BY (e.g. `SELECT id, name ... ORDER BY score`). The
/// planner adds a Projection to drop the sort-only column (`score`) above the Sort.
fn push_sort_topk_into_union(limit: Limit) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    // Walk through Projection and SubqueryAlias to find Sort below Limit.
    // These nodes may appear between Limit and Sort when the SELECT list
    // differs from the columns required by ORDER BY.
    let mut current = limit.input.as_ref();
    let mut above_sort_intermediates: Vec<&LogicalPlan> = Vec::new();
    let sort = loop {
        match current {
            LogicalPlan::Sort(s) => break s,
            LogicalPlan::Projection(p) => {
                above_sort_intermediates.push(current);
                current = p.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(sa) => {
                above_sort_intermediates.push(current);
                current = sa.input.as_ref();
            }
            _ => return Ok(Transformed::no(LogicalPlan::Limit(limit))),
        }
    };

    // Walk through Projection, SubqueryAlias, and Filter nodes to find the Union.
    // These nodes are not dropped: they are cloned into each union leg *before*
    // per-leg TopK sort. This is required for qualified expressions like `p.name`
    // and projection aliases used by ORDER BY.
    //
    // We also preserve any SubqueryAlias wrappers directly above the rebuilt Union
    // so outer qualified refs (e.g. `taxi_trips.col`) remain resolvable after
    // federation rewrites replace union legs with Federated nodes.
    let mut current = sort.input.as_ref();
    let mut between_sort_and_union: Vec<LogicalPlan> = Vec::new();
    let mut union_level_aliases: Vec<TableReference> = Vec::new();
    let union_plan = loop {
        match current {
            LogicalPlan::Union(u) => break u,
            LogicalPlan::SubqueryAlias(sa) => {
                between_sort_and_union.push(LogicalPlan::SubqueryAlias(sa.clone()));
                union_level_aliases.push(sa.alias.clone());
                current = sa.input.as_ref();
            }
            LogicalPlan::Projection(p) => {
                between_sort_and_union.push(LogicalPlan::Projection(p.clone()));
                current = p.input.as_ref();
            }
            LogicalPlan::Filter(f) => {
                between_sort_and_union.push(LogicalPlan::Filter(f.clone()));
                current = f.input.as_ref();
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
            let mut leg = Arc::clone(input);

            // Rebuild the exact subtree that used to sit between Sort and Union,
            // but with this specific union leg as the leaf input.
            for node in between_sort_and_union.iter().rev() {
                leg = Arc::new(match node {
                    LogicalPlan::SubqueryAlias(sa) => LogicalPlanBuilder::from(leg)
                        .alias(sa.alias.clone())?
                        .build()?,
                    LogicalPlan::Projection(p) => LogicalPlanBuilder::from(leg)
                        .project(p.expr.clone())?
                        .build()?,
                    LogicalPlan::Filter(f) => LogicalPlanBuilder::from(leg)
                        .filter(f.predicate.clone())?
                        .build()?,
                    _ => unreachable!("only Projection, SubqueryAlias, and Filter are collected"),
                });
            }

            Ok(Arc::new(LogicalPlan::Sort(Sort {
                expr: sort.expr.clone(),
                input: leg,
                fetch: Some(effective_fetch),
            })))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    // Rebuild: Union(per-leg Sort(TopK over reconstructed leg subtree))
    // -> [preserved SubqueryAlias wrappers] -> original outer Sort
    // -> [Projection|SubqueryAlias]* -> Limit.
    let mut result = LogicalPlan::Union(Union {
        inputs: new_inputs,
        schema: Arc::clone(sort.input.schema()),
    });

    // Preserve SubqueryAlias wrappers that originally sat between Sort and Union.
    // This keeps qualified refs above the Union resolvable.
    for alias in union_level_aliases.into_iter().rev() {
        result = LogicalPlanBuilder::from(result).alias(alias)?.build()?;
    }

    // Re-add the Sort node.
    result = LogicalPlan::Sort(Sort {
        expr: sort.expr.clone(),
        input: Arc::new(result),
        fetch: sort.fetch,
    });

    // Re-wrap nodes between Limit and Sort in reverse (innermost-first) order.
    for node in above_sort_intermediates.into_iter().rev() {
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
        input: Arc::new(result),
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
    use super::*;
    use async_trait::async_trait;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        catalog::Session,
        common::Statistics,
        datasource::{TableType, empty::EmptyTable},
        logical_expr::{LogicalPlanBuilder, SortExpr, col},
        physical_plan::{ExecutionPlan, empty::EmptyExec},
        prelude::SessionContext,
    };
    use std::{any::Any, collections::HashMap};

    /// A test [`TableProvider`] that reports a fixed, exact `num_rows` statistic. Used to
    /// exercise limit-based union-leg trimming. `scan` returns an empty plan (the trimming
    /// rewrite operates purely on statistics, so no data is needed).
    #[derive(Debug)]
    struct StatsTable {
        schema: SchemaRef,
        num_rows: Precision<usize>,
    }

    #[async_trait]
    impl TableProvider for StatsTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            let projected = datafusion::common::project_schema(&self.schema, projection)?;
            Ok(Arc::new(EmptyExec::new(projected)))
        }

        fn statistics(&self) -> Option<Statistics> {
            let mut stats = Statistics::new_unknown(&self.schema);
            stats.num_rows = self.num_rows;
            Some(stats)
        }
    }

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
                        Some("0".to_string()),
                    )])],
                ),
                (
                    p2,
                    vec![HashMap::from([(
                        "partition_id".to_string(),
                        Some("1".to_string()),
                    )])],
                ),
            ]
        }

        fn should_partition(&self, _tbl: &TableScan) -> bool {
            true
        }
    }

    /// A test partition provider that produces one filter-free leg per supplied row count.
    /// Each leg's provider reports the corresponding `num_rows` as `Precision::Exact`, except
    /// where `None` is given (which produces a provider with absent statistics).
    #[derive(Debug)]
    struct StatsPartitionProvider {
        schema: SchemaRef,
        leg_rows: Vec<Option<usize>>,
    }

    impl TablePartitionProvider for StatsPartitionProvider {
        fn get_partitions(
            &self,
            _table: &TableReference,
            _schema: &SchemaRef,
        ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
            self.leg_rows
                .iter()
                .map(|rows| {
                    let num_rows = match rows {
                        Some(n) => Precision::Exact(*n),
                        None => Precision::Absent,
                    };
                    let provider: Arc<dyn TableProvider> = Arc::new(StatsTable {
                        schema: Arc::clone(&self.schema),
                        num_rows,
                    });
                    // Empty partition values => no partition filter is added to the leg.
                    (provider, Vec::new())
                })
                .collect()
        }

        fn should_partition(&self, _tbl: &TableScan) -> bool {
            true
        }
    }

    fn make_stats_rule(
        schema: &SchemaRef,
        ctx: &SessionContext,
        leg_rows: Vec<Option<usize>>,
    ) -> PartitionedTableScanRewrite {
        PartitionedTableScanRewrite::new(
            Arc::new(StatsPartitionProvider {
                schema: Arc::clone(schema),
                leg_rows,
            }),
            ctx,
        )
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
                  SubqueryAlias: test_table
                    TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  SubqueryAlias: test_table
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
                  SubqueryAlias: test_table
                    TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=15
                  SubqueryAlias: test_table
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
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Projection: test_table.id, test_table.name
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Projection: test_table.id, test_table.name
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    /// Regression test for distributed queries with column projections.
    ///
    /// When `SELECT id, name FROM table ORDER BY score DESC LIMIT 3` is planned,
    /// the planner creates `Limit -> Projection(id, name) -> Sort(score) -> TableScan`
    /// because `score` is needed for ordering but not in the output. The Projection
    /// between Limit and Sort must not prevent Sort(TopK) from being pushed into
    /// union legs.
    #[test]
    fn test_limit_projection_above_sort_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=3) -> Projection(id, name) -> Sort(partition_id DESC) -> TableScan
        // This represents: SELECT id, name FROM table ORDER BY partition_id DESC LIMIT 3
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("partition_id"), false, true)])
            .expect("sort failed")
            .project(vec![col("id"), col("name")])
            .expect("project failed")
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Sort(TopK) should be pushed into each union leg despite the Projection
        // between Limit and Sort.
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=3
          Projection: test_table.id, test_table.name
            Sort: test_table.partition_id DESC NULLS FIRST
              SubqueryAlias: test_table
                Union
                  Sort: test_table.partition_id DESC NULLS FIRST, fetch=3
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                  Sort: test_table.partition_id DESC NULLS FIRST, fetch=3
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    /// Test with both Projection above Sort AND Projection below Sort.
    #[test]
    fn test_limit_projection_above_and_below_sort_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> Projection(id) -> Sort(name ASC) -> Projection(id, name) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("id"), col("name")])
            .expect("inner project failed")
            .sort(vec![SortExpr::new(col("name"), true, false)])
            .expect("sort failed")
            .project(vec![col("id")])
            .expect("outer project failed")
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          Projection: test_table.id
            Sort: test_table.name ASC NULLS LAST
              SubqueryAlias: test_table
                Union
                  Sort: test_table.name ASC NULLS LAST, fetch=5
                    Projection: test_table.id, test_table.name
                      SubqueryAlias: test_table
                        TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                  Sort: test_table.name ASC NULLS LAST, fetch=5
                    Projection: test_table.id, test_table.name
                      SubqueryAlias: test_table
                        TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    /// Test with offset and Projection above Sort.
    #[test]
    fn test_limit_with_offset_projection_above_sort_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(skip=5, fetch=3) -> Projection(id, name) -> Sort(partition_id DESC) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("partition_id"), false, true)])
            .expect("sort failed")
            .project(vec![col("id"), col("name")])
            .expect("project failed")
            .limit(5, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // fetch pushed into union legs should be skip + fetch = 5 + 3 = 8
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=5, fetch=3
          Projection: test_table.id, test_table.name
            Sort: test_table.partition_id DESC NULLS FIRST
              SubqueryAlias: test_table
                Union
                  Sort: test_table.partition_id DESC NULLS FIRST, fetch=8
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                  Sort: test_table.partition_id DESC NULLS FIRST, fetch=8
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_filter_pushdown_through_union() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> Filter(name != 'x') -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("name").not_eq(lit("x")))
            .expect("filter failed")
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Sort(TopK) and Filter should both be pushed into each union leg.
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Filter: test_table.name != Utf8("x")
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Filter: test_table.name != Utf8("x")
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_filter_with_offset_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(skip=10, fetch=5) -> Sort(id ASC) -> Filter(name != 'x') -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("name").not_eq(lit("x")))
            .expect("filter failed")
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(10, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // fetch pushed into union legs should be skip + fetch = 10 + 5 = 15
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=10, fetch=5
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=15
                  Filter: test_table.name != Utf8("x")
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=15
                  Filter: test_table.name != Utf8("x")
                    SubqueryAlias: test_table
                      TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_filter_with_projection_pushdown() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> Projection(id, name) -> Filter(partition_id > 0) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("partition_id").gt(lit(0)))
            .expect("filter failed")
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

        // Filter pushed into legs, Projection re-wrapped around Union.
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Projection: test_table.id, test_table.name
                    Filter: test_table.partition_id > Int32(0)
                      SubqueryAlias: test_table
                        TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
                Sort: test_table.id ASC NULLS LAST, fetch=5
                  Projection: test_table.id, test_table.name
                    Filter: test_table.partition_id > Int32(0)
                      SubqueryAlias: test_table
                        TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }

    #[test]
    fn test_limit_sort_pushdown_preserves_qualified_alias_columns() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_rule(&schema, &ctx);

        // Build a query shape like:
        // SELECT p.name
        // FROM test_table AS p
        // WHERE p.partition_id > 0
        // ORDER BY p.name
        // LIMIT 3
        //
        // This ensures qualified references (`p.name`) remain valid after TopK
        // pushdown into union legs.
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .alias("p")
            .expect("alias failed")
            .filter(col("p.partition_id").gt(lit(0)))
            .expect("filter failed")
            .project(vec![col("p.name")])
            .expect("project failed")
            .sort(vec![SortExpr::new(col("p.name"), true, false)])
            .expect("sort failed")
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        let plan_str = result.display_indent().to_string();

        assert!(
            plan_str.contains("Sort: p.name ASC NULLS LAST, fetch=3"),
            "expected TopK sort with qualified alias in each union leg, got:\n{plan_str}"
        );

        assert_eq!(
            plan_str.matches("SubqueryAlias: p").count(),
            3, // each leg, then one above union.
            "expected each union leg to preserve alias 'p', got:\n{plan_str}"
        );
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

    /// A single leg has exact stats `>= fetch`, so the Union and the other leg are dropped.
    #[test]
    fn test_limit_trims_to_single_leg() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        // Leg 0 has 10 exact rows, leg 1 has 2; fetch=3 is satisfied by leg 0 alone.
        let rule = make_stats_rule(&schema, &ctx, vec![Some(10), Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=0, fetch=3
          SubqueryAlias: test_table
            TableScan: test_table
        ");
    }

    /// No single leg covers the fetch, but a subset of exact legs sums to `>= fetch`,
    /// so only that subset is kept (the third leg is dropped).
    #[test]
    fn test_limit_trims_to_subset_of_legs() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        // Legs of 4, 4, 4 rows; fetch=7 needs two legs (4 + 4 >= 7).
        let rule = make_stats_rule(&schema, &ctx, vec![Some(4), Some(4), Some(4)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(7))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Two of the three legs are retained.
        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=0, fetch=7
          SubqueryAlias: test_table
            Union
              TableScan: test_table
              TableScan: test_table
        ");
    }

    /// `effective_fetch = skip + fetch`. With skip, more rows are required, so a single
    /// 10-row leg still suffices for skip=5, fetch=3 (effective_fetch=8).
    #[test]
    fn test_limit_with_offset_uses_effective_fetch() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_stats_rule(&schema, &ctx, vec![Some(10), Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(5, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=5, fetch=3
          SubqueryAlias: test_table
            TableScan: test_table
        ");
    }

    /// When the exact legs cannot guarantee enough rows, no trimming occurs.
    #[test]
    fn test_limit_no_trim_when_rows_insufficient() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        // Total exact rows = 2 + 2 = 4 < fetch=10.
        let rule = make_stats_rule(&schema, &ctx, vec![Some(2), Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(10))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Both legs retained: cannot guarantee 10 rows from a subset.
        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=0, fetch=10
          SubqueryAlias: test_table
            Union
              TableScan: test_table
              TableScan: test_table
        ");
    }

    /// Legs without exact statistics are never counted, so no trimming occurs even though
    /// the absent-stat leg might in fact be large enough.
    #[test]
    fn test_limit_no_trim_without_exact_stats() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        // Leg 0 has absent stats; leg 1 has 2 exact rows; fetch=3 cannot be guaranteed.
        let rule = make_stats_rule(&schema, &ctx, vec![None, Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=0, fetch=3
          SubqueryAlias: test_table
            Union
              TableScan: test_table
              TableScan: test_table
        ");
    }

    /// A Sort between Limit and Union disqualifies trimming; the Sort(TopK) pushdown path
    /// handles that case instead.
    #[test]
    fn test_limit_with_sort_does_not_trim() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_stats_rule(&schema, &ctx, vec![Some(10), Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Sort present: legs are not trimmed; Sort(TopK) is pushed into each leg instead.
        insta::assert_snapshot!(result.display_indent(), @r"
        Limit: skip=0, fetch=3
          Sort: test_table.id ASC NULLS LAST
            SubqueryAlias: test_table
              Union
                Sort: test_table.id ASC NULLS LAST, fetch=3
                  SubqueryAlias: test_table
                    TableScan: test_table
                Sort: test_table.id ASC NULLS LAST, fetch=3
                  SubqueryAlias: test_table
                    TableScan: test_table
        ");
    }

    /// A Filter between Limit and Union disqualifies trimming (it can reduce the surviving
    /// row count below the reported statistics).
    #[test]
    fn test_limit_with_filter_does_not_trim() {
        let schema = test_schema();
        let ctx = SessionContext::new();
        let rule = make_stats_rule(&schema, &ctx, vec![Some(10), Some(2)]);

        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("name").not_eq(lit("x")))
            .expect("filter failed")
            .limit(0, Some(3))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Filter between Limit and Union: no trimming; both legs are retained.
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=3
          Filter: test_table.name != Utf8("x")
            SubqueryAlias: test_table
              Union
                TableScan: test_table
                TableScan: test_table
        "#);
    }
}
