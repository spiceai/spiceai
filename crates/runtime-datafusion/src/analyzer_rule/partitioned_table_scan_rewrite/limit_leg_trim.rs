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
//! Union-leg trimming for unordered `Limit`s over the unions produced by
//! [`PartitionedTableScanRewrite`](super::PartitionedTableScanRewrite).

use std::sync::Arc;

use datafusion::{
    common::{Result, stats::Precision},
    datasource::source_as_provider,
    error::DataFusionError,
    logical_expr::{FetchType, Limit, LogicalPlan, LogicalPlanBuilder, SkipType, Union},
};

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
/// falls back to existing behavior, e.g.
/// [`push_sort_topk_into_union`](super::topk_pushdown::push_sort_topk_into_union)).
///
/// Safety conditions (all required):
/// - The `Limit` has a literal `fetch` and `skip`.
/// - There is no `Sort` or `Filter` between the `Limit` and the `Union` (these would change
///   which / how many rows survive, breaking the row-count guarantee).
/// - A kept leg has no `Filter` and its `TableScan` has no `filters` (a filter could reduce the
///   actual row count below the reported `num_rows`).
/// - The kept legs report `num_rows` as `Precision::Exact` and sum to `>= effective_fetch`.
pub(super) fn try_trim_union_legs(limit: &Limit) -> Result<Option<LogicalPlan>, DataFusionError> {
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

#[cfg(test)]
mod tests {
    use super::super::test_utils::{make_stats_rule, make_table_scan, test_schema};
    use datafusion::{
        config::ConfigOptions,
        logical_expr::{LogicalPlanBuilder, SortExpr, col, lit},
        optimizer::AnalyzerRule,
        prelude::SessionContext,
    };

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
