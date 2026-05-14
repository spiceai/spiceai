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

//! Logical optimizer rules for Cayenne.
//!
//! The flagship rule here is [`CayennePropagateFilterAcrossEquiJoinKeys`], the
//! plan-time predicate transitive closure used to unblock chbench q21 (see
//! `crates/cayenne/src/optimizer_rules.rs` module docs for the broader
//! no-spill strategy this fits into).
//!
//! DataFusion's stock `infer_join_predicates` (in `push_down_filter`) already
//! propagates predicates that *directly* reference a join-key column:
//! `WHERE nation.n_nationkey = 5 AND nation.n_nationkey = supplier.s_nationkey`
//! is transformed into `WHERE supplier.s_nationkey = 5 AND ...`. That covers
//! the `n_nationkey = $const` shape but misses the q21 shape, where the
//! selective filter is on a *non-key* column (`n_name = 'CHINA'`). The
//! cardinality bound the dim-table filter implies for the equi-joined key
//! column never reaches the fact-table scans, so by the time the planner
//! orders joins from the SQL `FROM` clause, `(supplier, order_line, …)`
//! has already been chosen with no nation filter pushed through.
//!
//! ## What the rule does
//!
//! For every `LogicalPlan::Join` with `JoinType::Inner` and at least one
//! column-only equi-key pair `(left.a, right.b)` whose data types match, the
//! rule inspects each side for a non-trivial `Filter` that references at
//! least one column other than its own join key. If one is found, it wraps
//! the *opposite* side with
//!
//! ```text
//! Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))
//! ```
//!
//! The inserted subquery re-projects the join key through whatever filters
//! already exist on the original side, so DataFusion's
//! `decorrelate_predicate_subquery` and `push_down_filter` can then plant a
//! `LeftSemi` join (or, after pushdown, a partition-pruning predicate) on
//! the fact-table scan. For q21 this turns
//! `nation ⋈ supplier ⋈ order_line` into a shape where `supplier.s_nationkey
//! IN (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA')` is visible
//! while the join graph is being costed.
//!
//! ## Termination
//!
//! Each introduced subquery is wrapped in a `SubqueryAlias` whose name
//! starts with [`PROPAGATED_FILTER_ALIAS_PREFIX`]. Before firing, the rule
//! walks the candidate filter chain and refuses to re-introduce a
//! propagated filter on a side that already contains an alias with that
//! prefix referencing this join's other side. This prevents the rule from
//! oscillating with itself when the optimizer iterates to fixed point.
//!
//! ## Status
//!
//! This is the **scaffolding** described in the q21 design doc. The struct
//! and termination guard are in place; the actual subtree-walk + subquery
//! construction is currently a no-op (`Transformed::no`) and is tracked as
//! the remaining work for the predicate transitive closure workstream. The
//! scaffolding is wired through `crates/runtime/src/datafusion/builder.rs`
//! so that follow-on patches can fill in
//! [`build_propagated_filter`] without touching call sites or the rule
//! ordering.

use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{JoinType, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::Expr;
use datafusion_expr::expr::InSubquery;

/// Prefix for [`datafusion_expr::SubqueryAlias`] names introduced by
/// [`CayennePropagateFilterAcrossEquiJoinKeys`].
///
/// Used both as a sentinel for cycle detection (the rule refuses to fire on a
/// subtree that already contains an alias starting with this prefix referencing
/// the same join partner) and as a marker in explain output so the rewrite is
/// recognizable when reading plans.
pub const PROPAGATED_FILTER_ALIAS_PREFIX: &str = "__cayenne_xclos__";

/// Logical optimizer rule that, for each Inner Join with a simple equi-key
/// `(left.a = right.b)`, introduces
/// `Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))`
/// on the side opposite a non-key filter.
///
/// See the module-level docs for the full design and the q21 motivation.
#[derive(Default)]
pub struct CayennePropagateFilterAcrossEquiJoinKeys;

impl CayennePropagateFilterAcrossEquiJoinKeys {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for CayennePropagateFilterAcrossEquiJoinKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePropagateFilterAcrossEquiJoinKeys")
            .finish()
    }
}

impl OptimizerRule for CayennePropagateFilterAcrossEquiJoinKeys {
    fn name(&self) -> &str {
        "cayenne_propagate_filter_across_equi_join_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // TopDown so that a propagation introduced at an outer join can be
        // observed (and re-propagated through) at inner joins on the next pass.
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };

        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }

        // TODO(lukim/q21): finish the subtree-walk + subquery construction.
        // The current scaffolding ensures the rule is registered, ordered
        // ahead of `push_down_filter`, and unit-tested for the no-op path.
        // The remaining work is to:
        //
        // 1. Identify a candidate `(left.a, right.b)` equi-key pair: both
        //    sides must be `Expr::Column` with matching data types and the
        //    column must resolve through the join's `DFSchema`.
        //
        // 2. For each side, search for a non-trivial `Filter` (i.e. one that
        //    references at least one column other than the join key on that
        //    side). Walk through transparent operators
        //    (`Projection`, `SubqueryAlias`, `Limit`) but bail on operators
        //    that change cardinality semantics (`Distinct`, `Aggregate`,
        //    `Join`, `Union`).
        //
        // 3. Skip the side if it already contains a `SubqueryAlias` whose
        //    name starts with `PROPAGATED_FILTER_ALIAS_PREFIX` and that
        //    alias's subquery resolves to the *other* side of this join —
        //    that means a previous iteration already inserted the filter
        //    and we'd otherwise oscillate.
        //
        // 4. Build a `LogicalPlan::Projection` over the filtered side that
        //    yields just the join-key column, wrap it in `SubqueryAlias`
        //    with a name from `PROPAGATED_FILTER_ALIAS_PREFIX` plus
        //    `OptimizerConfig::alias_generator().next("")`, and box it into
        //    a `Subquery { subquery, outer_ref_columns: vec![], spans: … }`.
        //
        // 5. Replace the *opposite* side with
        //    `LogicalPlan::Filter::try_new(Expr::InSubquery(
        //        InSubquery { expr: Box::new(other_key), subquery, negated: false }),
        //        original_other_side)`.
        //
        // 6. Return `Transformed::yes(rebuilt_join)`.
        //
        // The scaffolding deliberately returns `Transformed::no` so that the
        // rule is harmless to wire up immediately and so that the unit tests
        // below pin down the cycle-detection helper without depending on
        // the full subquery construction landing in the same patch.
        let _ = (&join.on, &join.filter, &join.left, &join.right);

        Ok(Transformed::no(plan))
    }
}

/// Returns `true` if `expr` already contains an [`InSubquery`] whose inner
/// plan starts with a `SubqueryAlias` named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
///
/// Used by [`CayennePropagateFilterAcrossEquiJoinKeys`] to skip filters that
/// the rule itself has already produced, preventing it from oscillating with
/// the optimizer's fixed-point loop.
#[must_use]
pub fn expr_has_propagated_filter(expr: &Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut found = false;
    let _ = expr.apply(|e| {
        if let Expr::InSubquery(InSubquery { subquery, .. }) = e
            && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Placeholder for the eventual subquery-construction helper that wraps
/// `subtree` in `Projection([key])` + `SubqueryAlias("__cayenne_xclos__N")`.
///
/// Kept on the type so it can grow into the real implementation without
/// changing the rule's call shape.
#[allow(dead_code)]
fn build_propagated_filter(_subtree: &LogicalPlan, _key: &Expr) -> Option<LogicalPlan> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn rule() -> CayennePropagateFilterAcrossEquiJoinKeys {
        CayennePropagateFilterAcrossEquiJoinKeys::new()
    }

    #[test]
    fn rule_metadata() {
        let r = rule();
        assert_eq!(r.name(), "cayenne_propagate_filter_across_equi_join_keys");
        assert_eq!(r.apply_order(), Some(ApplyOrder::TopDown));
    }

    #[tokio::test]
    async fn non_inner_join_is_unchanged() -> Result<()> {
        // Build a tiny LEFT JOIN plan and confirm the rule returns
        // `Transformed::no(plan)` without altering it. This pins down the
        // gating predicate on `JoinType::Inner` so a future regression that
        // expanded the match would surface here.
        let ctx = SessionContext::new();

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("x", DataType::Utf8, true),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ]));

        ctx.register_table(
            "t_a",
            Arc::new(MemTable::try_new(Arc::clone(&schema_a), vec![vec![]])?),
        )?;
        ctx.register_table(
            "t_b",
            Arc::new(MemTable::try_new(Arc::clone(&schema_b), vec![vec![]])?),
        )?;

        let plan = ctx
            .sql("SELECT a, b FROM t_a LEFT JOIN t_b ON a = b WHERE x = 'val'")
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let transformed = r.rewrite(plan.clone(), &cfg)?;

        assert!(
            !transformed.transformed,
            "LEFT JOIN must be unchanged by the rule"
        );
        Ok(())
    }

    #[test]
    fn expr_has_propagated_filter_detects_marker_alias() -> Result<()> {
        use datafusion::common::Spans;
        use datafusion::common::TableReference;
        use datafusion_expr::logical_plan::{Subquery, SubqueryAlias};
        use datafusion_expr::{
            LogicalPlanBuilder, builder::table_scan, expr::Expr as ExprMod, lit,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let projection = LogicalPlanBuilder::from(scan)
            .project(vec![ExprMod::Column(datafusion::common::Column::new(
                Some("t"),
                "a",
            ))])?
            .build()?;

        let alias_name = format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1");
        let aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(projection),
            TableReference::bare(alias_name),
        )?);

        let in_subquery = Expr::InSubquery(InSubquery::new(
            Box::new(lit(1i64)),
            Subquery {
                subquery: Arc::new(aliased),
                outer_ref_columns: vec![],
                spans: Spans::default(),
            },
            false,
        ));

        assert!(expr_has_propagated_filter(&in_subquery));

        // A plain literal expression must NOT trigger detection.
        assert!(!expr_has_propagated_filter(&lit(5i64)));
        Ok(())
    }
}
