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
//! walks the candidate side's filter chain and refuses to re-introduce a
//! propagated filter on a side that already contains an alias with that
//! prefix. This prevents the rule from oscillating with itself when the
//! optimizer iterates to fixed point.
//!
//! ## Conservatism
//!
//! The rule only fires when the side providing the filter terminates in a
//! single `TableScan` (possibly behind `SubqueryAlias`, `Projection`,
//! `Filter`, `Limit`). Joining a non-trivial subtree would risk
//! duplicate-executing a large plan inside the subquery, since DataFusion
//! does not currently de-duplicate plan-level common subexpressions across
//! the outer plan and an `InSubquery`. The dim-table-filter shape
//! (`Filter(n_name='CHINA') → TableScan(nation)`) is the q21 case and is
//! cheap to re-execute.

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DataFusionError, Result, Spans, TableReference};
use datafusion::logical_expr::{
    Filter, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Subquery, SubqueryAlias,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::Expr;
use datafusion_expr::expr::InSubquery;
use std::sync::Arc;

/// Prefix for [`SubqueryAlias`] names introduced by
/// [`CayennePropagateFilterAcrossEquiJoinKeys`].
///
/// Used both as a sentinel for cycle detection (the rule refuses to fire on a
/// subtree that already contains an alias starting with this prefix) and as a
/// marker in explain output so the rewrite is recognizable when reading plans.
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
        // TopDown: process outer joins first so the propagation seeds reach
        // inner joins on the next pass.
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let join = match plan {
            LogicalPlan::Join(j) => j,
            other => return Ok(Transformed::no(other)),
        };
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let Some((left_col, right_col)) = pick_equijoin_columns(&join) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };

        let left_is_dim = is_dim_like_subtree(&join.left);
        let right_is_dim = is_dim_like_subtree(&join.right);

        let left_has_nonkey_filter =
            left_is_dim && subtree_has_non_key_filter(&join.left, &left_col);
        let right_has_nonkey_filter =
            right_is_dim && subtree_has_non_key_filter(&join.right, &right_col);

        let already_propagated_on_left = subtree_has_propagated_filter(&join.left);
        let already_propagated_on_right = subtree_has_propagated_filter(&join.right);

        let mut new_left: Arc<LogicalPlan> = Arc::clone(&join.left);
        let mut new_right: Arc<LogicalPlan> = Arc::clone(&join.right);
        let mut changed = false;

        // Propagate the LEFT-side dim filter → the RIGHT side.
        if left_has_nonkey_filter && !already_propagated_on_right {
            let subquery_plan = build_key_projection_subquery(
                Arc::clone(&join.left),
                &left_col,
                config.alias_generator(),
            )?;
            let wrapped = wrap_with_in_subquery_filter(
                Arc::clone(&join.right),
                &right_col,
                subquery_plan,
            )?;
            new_right = Arc::new(wrapped);
            changed = true;
        }

        // Propagate the RIGHT-side dim filter → the LEFT side.
        if right_has_nonkey_filter && !already_propagated_on_left {
            let subquery_plan = build_key_projection_subquery(
                Arc::clone(&join.right),
                &right_col,
                config.alias_generator(),
            )?;
            let wrapped =
                wrap_with_in_subquery_filter(Arc::clone(&join.left), &left_col, subquery_plan)?;
            new_left = Arc::new(wrapped);
            changed = true;
        }

        if !changed {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let new_join = Join {
            left: new_left,
            right: new_right,
            on: join.on,
            filter: join.filter,
            join_type: join.join_type,
            join_constraint: join.join_constraint,
            schema: join.schema,
            null_equality: join.null_equality,
        };

        Ok(Transformed::yes(LogicalPlan::Join(new_join)))
    }
}

/// Return the single column-only equi-key pair from `join.on`, or `None` when
/// no such pair exists (e.g. expression keys, multi-key composite joins).
///
/// Picks the first qualifying pair; q21's nation⋈supplier join has exactly
/// one `(n_nationkey, s_nationkey)` pair, which is the case this rule targets.
fn pick_equijoin_columns(join: &Join) -> Option<(Column, Column)> {
    if join.on.len() != 1 {
        return None;
    }
    let (left, right) = &join.on[0];
    match (left, right) {
        (Expr::Column(l), Expr::Column(r)) => Some((l.clone(), r.clone())),
        _ => None,
    }
}

/// Walks `plan` through transparent operators
/// (`Projection`, `SubqueryAlias`, `Filter`, `Limit`) and returns `true` if
/// it bottoms out in a single `TableScan` with no `Join`, `Aggregate`,
/// `Distinct`, `Union`, or `Window` in between.
///
/// The conservatism here keeps the duplicated subquery cheap: DataFusion will
/// execute it independently of the outer join, so we only fire on subtrees
/// where re-running the scan + filter is cheap.
fn is_dim_like_subtree(plan: &LogicalPlan) -> bool {
    let mut cursor = plan;
    loop {
        match cursor {
            LogicalPlan::TableScan(_) => return true,
            LogicalPlan::Projection(p) => cursor = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(a) => cursor = a.input.as_ref(),
            LogicalPlan::Filter(f) => cursor = f.input.as_ref(),
            LogicalPlan::Limit(l) => cursor = l.input.as_ref(),
            _ => return false,
        }
    }
}

/// Returns `true` if any `LogicalPlan::Filter` reachable through
/// transparent operators in `plan` has a predicate that references at least
/// one column whose name is not `key_col.name()`.
///
/// We compare on column *name* rather than the fully qualified `Column` so a
/// `Filter(n_name = 'CHINA') → TableScan(nation [n_nationkey, n_name])` test
/// fires regardless of whether the column is qualified as
/// `nation.n_name` or bare `n_name`.
fn subtree_has_non_key_filter(plan: &LogicalPlan, key_col: &Column) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| match node {
        LogicalPlan::Filter(f) => {
            if filter_references_non_key_column(&f.predicate, key_col) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        }
        // Don't descend into joins, aggregates, etc. — they break the
        // "dim-like" invariant and we shouldn't honor filters under them
        // anyway (already accounted for via `is_dim_like_subtree`).
        LogicalPlan::Join(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Window(_) => Ok(TreeNodeRecursion::Jump),
        _ => Ok(TreeNodeRecursion::Continue),
    });
    found
}

fn filter_references_non_key_column(predicate: &Expr, key_col: &Column) -> bool {
    let mut others = false;
    let _ = predicate.apply(|e| {
        if let Expr::Column(c) = e
            && c.name != key_col.name
        {
            others = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    others
}

/// Returns `true` if `plan` already contains a `SubqueryAlias` (or an
/// `InSubquery` whose subquery starts with one) whose name begins with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
///
/// This is the cycle guard: once the rule has wrapped a side with the
/// propagated `InSubquery`, a subsequent pass will see this marker and skip.
fn subtree_has_propagated_filter(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table().starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::Filter(f) = node
            && expr_has_propagated_filter(&f.predicate)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Returns `true` if `expr` contains an [`InSubquery`] whose inner plan
/// starts with a [`SubqueryAlias`] named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
#[must_use]
pub fn expr_has_propagated_filter(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|e| {
        if let Expr::InSubquery(InSubquery { subquery, .. }) = e
            && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
            && alias.alias.table().starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Build a `SubqueryAlias(__cayenne_xclos__N, Projection([key_col], subtree))`
/// suitable for use as the inner plan of a [`Subquery`] referenced by an
/// [`InSubquery`] expression.
///
/// The alias name uses [`PROPAGATED_FILTER_ALIAS_PREFIX`] plus a unique id
/// from [`OptimizerConfig::alias_generator`], so each invocation produces a
/// distinct marker. The marker doubles as the cycle-detection sentinel
/// scanned by [`subtree_has_propagated_filter`] / [`expr_has_propagated_filter`].
fn build_key_projection_subquery(
    subtree: Arc<LogicalPlan>,
    key_col: &Column,
    alias_gen: &Arc<datafusion::common::alias::AliasGenerator>,
) -> Result<LogicalPlan> {
    let key_expr = Expr::Column(key_col.clone());
    let projection = LogicalPlanBuilder::from(Arc::unwrap_or_clone(subtree))
        .project(vec![key_expr])?
        .build()?;
    let alias_name = alias_gen.next(PROPAGATED_FILTER_ALIAS_PREFIX);
    let aliased = SubqueryAlias::try_new(Arc::new(projection), TableReference::bare(alias_name))?;
    Ok(LogicalPlan::SubqueryAlias(aliased))
}

/// Wrap `input` with `Filter(other_col IN (subquery))` using the
/// `subquery_plan` (which must already be a `SubqueryAlias` named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`]) as the right-hand side.
fn wrap_with_in_subquery_filter(
    input: Arc<LogicalPlan>,
    other_col: &Column,
    subquery_plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let predicate = Expr::InSubquery(InSubquery::new(
        Box::new(Expr::Column(other_col.clone())),
        Subquery {
            subquery: Arc::new(subquery_plan),
            outer_ref_columns: vec![],
            spans: Spans::default(),
        },
        false,
    ));
    let filter = Filter::try_new(predicate, input)?;
    Ok(LogicalPlan::Filter(filter))
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

    fn make_ctx() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        // dim-like nation table
        let nation_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
        ]));
        // fact-like supplier table
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));
        ctx.register_table(
            "nation",
            Arc::new(MemTable::try_new(Arc::clone(&nation_schema), vec![vec![]])?),
        )?;
        ctx.register_table(
            "supplier",
            Arc::new(MemTable::try_new(
                Arc::clone(&supplier_schema),
                vec![vec![]],
            )?),
        )?;
        Ok(ctx)
    }

    /// Walk a `LogicalPlan` to find the first `Join` and return whichever
    /// side's plan tree contains a `SubqueryAlias` whose name starts with
    /// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
    fn find_propagated_side(plan: &LogicalPlan) -> Option<&'static str> {
        let mut result: Option<&'static str> = None;
        let _ = plan.apply(|node| {
            if let LogicalPlan::Join(j) = node {
                if subtree_has_propagated_filter(j.left.as_ref()) {
                    result = Some("left");
                    return Ok(TreeNodeRecursion::Stop);
                }
                if subtree_has_propagated_filter(j.right.as_ref()) {
                    result = Some("right");
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        result
    }

    #[test]
    fn rule_metadata() {
        assert_eq!(
            rule().name(),
            "cayenne_propagate_filter_across_equi_join_keys"
        );
        assert_eq!(rule().apply_order(), Some(ApplyOrder::TopDown));
    }

    #[tokio::test]
    async fn non_inner_join_is_unchanged() -> Result<()> {
        // Use `IS NULL` on the right side so `eliminate_outer_join` doesn't
        // promote the LEFT JOIN to an INNER JOIN, otherwise we'd be testing
        // the wrong thing.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey WHERE n_name IS NULL",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "LEFT JOIN must be skipped by the rule; plan was:\n{plan}"
        );
        Ok(())
    }

    /// Run the rule against every `LogicalPlan::Join` reachable from `plan`,
    /// returning the transformed plan and a flag indicating whether at least
    /// one invocation made a change.
    ///
    /// Mirrors what DataFusion's optimizer driver does for an
    /// `ApplyOrder::TopDown` rule, but without spinning up the rest of the
    /// rule pipeline — keeps the tests focused on this rule's behavior in
    /// isolation.
    fn apply_rule_to_all_joins(
        rule: &CayennePropagateFilterAcrossEquiJoinKeys,
        plan: LogicalPlan,
        cfg: &datafusion::optimizer::OptimizerContext,
    ) -> Result<(LogicalPlan, bool)> {
        let mut any_changed = false;
        let transformed = plan.transform_down(|node| {
            if matches!(node, LogicalPlan::Join(_)) {
                let r = rule.rewrite(node, cfg)?;
                if r.transformed {
                    any_changed = true;
                }
                Ok(r)
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok((transformed.data, any_changed))
    }

    #[tokio::test]
    async fn inner_join_with_dim_filter_propagates_via_subquery() -> Result<()> {
        // The canonical q21 shape (reduced):
        //   FROM supplier, nation
        //   WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'
        //
        // After PushDownFilter, `n_name = 'CHINA'` lives in a Filter directly
        // above the nation TableScan. The rule should then wrap supplier with
        // `Filter(s_nationkey IN (SELECT n_nationkey FROM nation
        //                          WHERE n_name = 'CHINA'))`.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;

        // Depending on DataFusion's planner the join's `left`/`right` may be
        // either order. We don't care which side gets the InSubquery, only
        // that exactly one of them does, and that it carries the marker.
        let propagated = find_propagated_side(&transformed_plan);
        assert!(
            changed,
            "rule should fire on inner join with dim-side non-key filter; plan was:\n{plan}"
        );
        assert!(
            propagated.is_some(),
            "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
        );

        // Cycle prevention: running the rule a second time on the
        // already-transformed plan must be a no-op.
        let (second_plan, changed2) =
            apply_rule_to_all_joins(&r, transformed_plan.clone(), &cfg)?;
        assert!(
            !changed2,
            "second pass must not re-propagate (cycle guard); plan was:\n{second_plan}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn inner_join_without_filter_is_noop() -> Result<()> {
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "rule must not fire when neither side has a non-key filter; plan was:\n{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn inner_join_with_key_only_filter_is_noop() -> Result<()> {
        // `n_nationkey = 22` references only the join key — DataFusion's
        // stock `infer_join_predicates` already handles this case, so our
        // rule must NOT fire and create a redundant subquery.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_nationkey = 22",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "rule must not fire when filter references only the join key; plan was:\n{plan}"
        );
        Ok(())
    }

    #[test]
    fn expr_has_propagated_filter_detects_marker_alias() -> Result<()> {
        use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let projection = LogicalPlanBuilder::from(scan)
            .project(vec![Expr::Column(Column::new(Some("t"), "a"))])?
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
        assert!(!expr_has_propagated_filter(&lit(5i64)));
        Ok(())
    }

    #[test]
    fn is_dim_like_subtree_handles_simple_scan() -> Result<()> {
        use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("x", DataType::Utf8, true),
        ]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        assert!(is_dim_like_subtree(&scan));

        let filtered = LogicalPlanBuilder::from(scan)
            .filter(Expr::Column(Column::new(Some("t"), "x")).eq(lit("v")))?
            .build()?;
        assert!(is_dim_like_subtree(&filtered));
        Ok(())
    }
}
