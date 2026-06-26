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
//! Per-leg TopK pushdown for ordered `Limit`s over the unions produced by
//! [`PartitionedTableScanRewrite`](super::PartitionedTableScanRewrite).

use std::sync::Arc;

use datafusion::{
    common::{Result, tree_node::Transformed},
    error::DataFusionError,
    logical_expr::{FetchType, Limit, LogicalPlan, LogicalPlanBuilder, SkipType, Sort, Union},
    sql::TableReference,
};

/// When `Limit -> [Projection|SubqueryAlias]* -> Sort -> [Projection|SubqueryAlias|Filter]* -> Union(sub_scans)`,
/// push `Sort(fetch = skip + fetch)` into each union leg. This enables per-executor `TopK`,
/// reducing data transfer from executors to the scheduler.
///
/// DataFusion's optimizer pushes `Limit` through a `Union`, but does not push `Sort(TopK)`.
/// Without this, each executor returns all rows and the scheduler sorts the full merged result.
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
pub(super) fn push_sort_topk_into_union(
    limit: Limit,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
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

#[cfg(test)]
mod tests {
    use super::super::test_utils::{make_rule, make_table_scan, test_schema};
    use datafusion::{
        config::ConfigOptions,
        logical_expr::{LogicalPlanBuilder, SortExpr, col, lit},
        optimizer::AnalyzerRule,
        prelude::SessionContext,
    };

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
        // (Leg trimming also does not fire: EmptyTable reports no exact statistics.)
        insta::assert_snapshot!(result.display_indent(), @r#"
        Limit: skip=0, fetch=5
          SubqueryAlias: test_table
            Union
              TableScan: test_table, unsupported_filters=[partition_id = Utf8("0")]
              TableScan: test_table, unsupported_filters=[partition_id = Utf8("1")]
        "#);
    }
}
