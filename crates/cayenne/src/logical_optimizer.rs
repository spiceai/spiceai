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
//! `DataFusion`'s stock `infer_join_predicates` (in `push_down_filter`) already
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
//! For every `LogicalPlan::Join` with `JoinType::Inner`, `JoinType::LeftSemi`,
//! or `JoinType::RightSemi`, default SQL NULL equality (`NULL != NULL`), and one
//! or more equi-key pairs whose data types match, the rule inspects each side
//! for a non-trivial `Filter` that references at least one column other than
//! each candidate join key. If one side is dim-like and has a projectable column
//! key, it wraps the *opposite* side with
//!
//! ```text
//! Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))
//! ```
//!
//! The inserted subquery re-projects the join key through whatever filters
//! already exist on the original side, so `DataFusion`'s
//! `decorrelate_predicate_subquery` and `push_down_filter` can then plant a
//! `LeftSemi` join (or, after pushdown, a partition-pruning predicate) on
//! the fact-table scan. For q21 this turns
//! `nation ⋈ supplier ⋈ order_line` into a shape where `supplier.s_nationkey
//! IN (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA')` is visible
//! while the join graph is being costed.
//!
//! Semi-join coverage is what makes chained propagation work: after
//! `decorrelate_predicate_subquery` rewrites a propagated `InSubquery` into a
//! `LeftSemi` join, the next optimizer pass can keep propagating across
//! adjacent inner joins (e.g. `region → nation → supplier → fact`) instead of
//! halting at the semi-join boundary. Propagation correctness on
//! `LeftSemi`/`RightSemi` follows from the join's existing key-domain
//! semantics: wrapping either input with `IN (SELECT key FROM other_side)`
//! produces a subset of rows that the semi-join would already retain.
//!
//! ## Termination
//!
//! Each introduced subquery is wrapped in a `SubqueryAlias` whose name
//! starts with [`PROPAGATED_FILTER_ALIAS_PREFIX`]. Before firing, the rule
//! walks the candidate side's filter chain and refuses to re-introduce a
//! propagated filter for the same target key. This prevents the rule from
//! oscillating with itself when the optimizer iterates to fixed point, while
//! still allowing composite joins to receive one derived filter per key.
//!
//! ## Conservatism
//!
//! The rule only fires when the side providing the filter is dim-like: a small
//! subtree with at most [`MAX_DIM_LIKE_TABLE_SCANS`] table scans behind
//! identity-preserving operators and inner joins. Joining a non-trivial subtree
//! would risk duplicate-executing a large plan inside the subquery, since
//! `DataFusion` does not currently de-duplicate plan-level common subexpressions
//! across the outer plan and an `InSubquery`. The dim-table-filter shape
//! (`Filter(n_name='CHINA') → TableScan(nation)`) and small dimension snowflakes
//! are cheap to re-execute.

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DataFusionError, NullEquality, Result, Spans, TableReference};
use datafusion::logical_expr::{
    Filter, Join, JoinType, LogicalPlan, Projection, Subquery, SubqueryAlias,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::expr::InSubquery;
use std::{collections::BTreeSet, sync::Arc};

/// Prefix for [`SubqueryAlias`] names introduced by
/// [`CayennePropagateFilterAcrossEquiJoinKeys`].
///
/// Used both as a sentinel for key-scoped cycle detection (the rule refuses to
/// add another propagated filter for a target key that already has one) and as
/// a marker in explain output so the rewrite is recognizable when reading plans.
pub const PROPAGATED_FILTER_ALIAS_PREFIX: &str = "__cayenne_xclos__";

/// Logical optimizer rule that, for each `Inner`, `LeftSemi`, or `RightSemi`
/// join with default SQL NULL equality and a simple equi-key
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
    fn name(&self) -> &'static str {
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
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi,
        ) {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        if join.null_equality != NullEquality::NullEqualsNothing {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let equijoin_keys = matching_equijoin_keys(&join);
        if equijoin_keys.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let mut left_analysis = analyze_logical_side(&join.left);
        let mut right_analysis = analyze_logical_side(&join.right);

        let mut new_left: Arc<LogicalPlan> = Arc::clone(&join.left);
        let mut new_right: Arc<LogicalPlan> = Arc::clone(&join.right);
        let mut changed = false;

        for key in &equijoin_keys {
            match key {
                EquiKey::BothColumns { left, right } => {
                    // Propagate the LEFT-side filtered key domain → the RIGHT side.
                    if left_analysis.is_dim_like
                        && left_analysis.has_non_key_filter(&left.name)
                        && key_preserved_through_summaries(&join.left, left)
                        && !right_analysis.has_propagated_filter_target(&column_expr(right))
                    {
                        let subquery_plan = build_key_projection_subquery(
                            Arc::clone(&join.left),
                            left,
                            config.alias_generator(),
                        )?;
                        let target = column_expr(right);
                        let wrapped = wrap_with_in_subquery_filter_expr(
                            Arc::clone(&new_right),
                            &target,
                            subquery_plan,
                        )?;
                        new_right = Arc::new(wrapped);
                        right_analysis.add_propagated_filter_target(&target);
                        changed = true;
                    }

                    // Propagate the RIGHT-side filtered key domain → the LEFT side.
                    if right_analysis.is_dim_like
                        && right_analysis.has_non_key_filter(&right.name)
                        && key_preserved_through_summaries(&join.right, right)
                        && !left_analysis.has_propagated_filter_target(&column_expr(left))
                    {
                        let subquery_plan = build_key_projection_subquery(
                            Arc::clone(&join.right),
                            right,
                            config.alias_generator(),
                        )?;
                        let target = column_expr(left);
                        let wrapped = wrap_with_in_subquery_filter_expr(
                            Arc::clone(&new_left),
                            &target,
                            subquery_plan,
                        )?;
                        new_left = Arc::new(wrapped);
                        left_analysis.add_propagated_filter_target(&target);
                        changed = true;
                    }
                }
                EquiKey::LeftColumnRightExpr {
                    left_col,
                    right_expr,
                } => {
                    // Only LEFT-dim → RIGHT-expr direction can fire: the right
                    // side has an expression key, so the fact-side filter
                    // target must be that expression. Propagation in the other
                    // direction would require projecting an expression
                    // (potentially referencing fact-side rows) inside the dim
                    // subquery, which would no longer be a cheap re-execution.
                    if left_analysis.is_dim_like
                        && left_analysis.has_non_key_filter(&left_col.name)
                        && key_preserved_through_summaries(&join.left, left_col)
                        && !right_analysis.has_propagated_filter_target(right_expr)
                    {
                        let subquery_plan = build_key_projection_subquery(
                            Arc::clone(&join.left),
                            left_col,
                            config.alias_generator(),
                        )?;
                        let wrapped = wrap_with_in_subquery_filter_expr(
                            Arc::clone(&new_right),
                            right_expr,
                            subquery_plan,
                        )?;
                        new_right = Arc::new(wrapped);
                        right_analysis.add_propagated_filter_target(right_expr);
                        changed = true;
                    }
                }
                EquiKey::LeftExprRightColumn {
                    left_expr,
                    right_col,
                } => {
                    // Symmetric: only RIGHT-dim → LEFT-expr direction.
                    if right_analysis.is_dim_like
                        && right_analysis.has_non_key_filter(&right_col.name)
                        && key_preserved_through_summaries(&join.right, right_col)
                        && !left_analysis.has_propagated_filter_target(left_expr)
                    {
                        let subquery_plan = build_key_projection_subquery(
                            Arc::clone(&join.right),
                            right_col,
                            config.alias_generator(),
                        )?;
                        let wrapped = wrap_with_in_subquery_filter_expr(
                            Arc::clone(&new_left),
                            left_expr,
                            subquery_plan,
                        )?;
                        new_left = Arc::new(wrapped);
                        left_analysis.add_propagated_filter_target(left_expr);
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let new_join = Join::try_new(
            new_left,
            new_right,
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?;

        Ok(Transformed::yes(LogicalPlan::Join(new_join)))
    }
}

#[derive(Default)]
struct SideAnalysis {
    is_dim_like: bool,
    filter_columns: BTreeSet<String>,
    /// Targets of already-propagated `InSubquery` filters on this side, keyed
    /// by the `Display` form of the target expression. Used for cycle
    /// prevention — the same target should not be wrapped twice. Tracks both
    /// pure-column and expression targets uniformly, so the chbench
    /// `ascii(substr(c_state,1,1)) - 65` shape is also cycle-guarded.
    propagated_filter_targets: BTreeSet<String>,
}

impl SideAnalysis {
    fn has_non_key_filter(&self, key_name: &str) -> bool {
        self.filter_columns.iter().any(|column| column != key_name)
    }

    fn has_propagated_filter_target(&self, target: &Expr) -> bool {
        self.propagated_filter_targets.contains(&target.to_string())
    }

    fn add_propagated_filter_target(&mut self, target: &Expr) {
        self.propagated_filter_targets.insert(target.to_string());
    }
}

fn column_expr(column: &Column) -> Expr {
    Expr::Column(column.clone())
}

fn analyze_logical_side(plan: &LogicalPlan) -> SideAnalysis {
    let mut analysis = SideAnalysis {
        is_dim_like: is_dim_like_subtree(plan),
        ..SideAnalysis::default()
    };

    let _ = plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            collect_filter_column_names(&filter.predicate, &mut analysis.filter_columns);
            collect_propagated_filter_targets(
                &filter.predicate,
                &mut analysis.propagated_filter_targets,
            );
        }

        Ok(TreeNodeRecursion::Continue)
    });

    analysis
}

/// An equi-join key from `Join::on`, classified by which sides are pure
/// columns. Propagation requires the *dim* side to be a `Column` so the IN
/// subquery has a cheap, projectable key; the *fact* side may be an arbitrary
/// expression (e.g. the chbench `ascii(substr(c_state,1,1)) - 65` pattern).
enum EquiKey {
    /// Both join keys are columns. The rule may fire in either direction
    /// depending on which side is dim-like.
    BothColumns { left: Column, right: Column },
    /// Left key is a column, right key is an expression. Only the
    /// `LEFT → RIGHT` propagation direction is supported.
    LeftColumnRightExpr { left_col: Column, right_expr: Expr },
    /// Right key is a column, left key is an expression. Only the
    /// `RIGHT → LEFT` propagation direction is supported.
    LeftExprRightColumn { left_expr: Expr, right_col: Column },
}

/// Return the equi-join keys from `join.on` whose data types match. Drops
/// pairs where both sides are expressions (no dim-like column to project) and
/// pairs whose types differ (the `IN` subquery would need an implicit cast we
/// don't insert here).
fn matching_equijoin_keys(join: &Join) -> Vec<EquiKey> {
    join.on
        .iter()
        .filter_map(|(left, right)| {
            if !join_key_types_match(left, right, &join.left, &join.right) {
                return None;
            }

            match (left, right) {
                (Expr::Column(l), Expr::Column(r)) => Some(EquiKey::BothColumns {
                    left: l.clone(),
                    right: r.clone(),
                }),
                (Expr::Column(l), other) => Some(EquiKey::LeftColumnRightExpr {
                    left_col: l.clone(),
                    right_expr: other.clone(),
                }),
                (other, Expr::Column(r)) => Some(EquiKey::LeftExprRightColumn {
                    left_expr: other.clone(),
                    right_col: r.clone(),
                }),
                // Both sides are non-trivial expressions — no cheap projection
                // target on either side, skip.
                _ => None,
            }
        })
        .collect()
}

fn join_key_types_match(
    left: &Expr,
    right: &Expr,
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
) -> bool {
    let Ok(left_type) = left.get_type(left_plan.schema()) else {
        return false;
    };
    let Ok(right_type) = right.get_type(right_plan.schema()) else {
        return false;
    };

    left_type == right_type
}

/// Maximum number of `TableScan` leaves allowed inside a dim-like subtree.
///
/// Chosen to cover the canonical chbench / TPC-H dimension snowflake
/// (`region ⋈ nation ⋈ supplier`, three leaves) without admitting arbitrarily
/// large dim joins whose re-execution under an `InSubquery` would be expensive.
const MAX_DIM_LIKE_TABLE_SCANS: usize = 3;

/// Returns `true` if `plan` is a "dim-like" subtree — a small snowflake of
/// dimensions composed of at most [`MAX_DIM_LIKE_TABLE_SCANS`] `TableScan`s
/// connected through identity-preserving operators (`Projection`,
/// `SubqueryAlias`, `Filter`, `Limit`), inner equi-joins with default SQL
/// NULL equality, `Aggregate`, or `Distinct`.
///
/// The conservatism here keeps the duplicated subquery cheap: `DataFusion` will
/// execute it independently of the outer join, so we only fire on subtrees
/// where re-running the scan(s) + filter(s) is cheap. Unions, windows, sorts,
/// and any non-inner / null-equal join terminate the walk.
///
/// `Aggregate` and `Distinct` are *structurally* allowed here, but the rule's
/// caller must additionally verify the join key is preserved through any
/// aggregations via [`key_preserved_through_summaries`] — an aggregate that
/// does not group by the key does not preserve its domain and cannot be the
/// source of a propagated subquery on that key.
fn is_dim_like_subtree(plan: &LogicalPlan) -> bool {
    count_dim_like_table_scans(plan).is_some_and(|n| n <= MAX_DIM_LIKE_TABLE_SCANS)
}

fn count_dim_like_table_scans(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TableScan(_) => Some(1),
        LogicalPlan::Projection(p) => count_dim_like_table_scans(&p.input),
        LogicalPlan::SubqueryAlias(a) => count_dim_like_table_scans(&a.input),
        LogicalPlan::Filter(f) => count_dim_like_table_scans(&f.input),
        LogicalPlan::Limit(l) => count_dim_like_table_scans(&l.input),
        LogicalPlan::Aggregate(a) => count_dim_like_table_scans(&a.input),
        LogicalPlan::Distinct(d) => count_dim_like_table_scans(distinct_input(d)),
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner
                && j.null_equality == NullEquality::NullEqualsNothing =>
        {
            let l = count_dim_like_table_scans(&j.left)?;
            let r = count_dim_like_table_scans(&j.right)?;
            Some(l + r)
        }
        _ => None,
    }
}

/// Returns the single input plan of a `Distinct` regardless of variant.
fn distinct_input(distinct: &datafusion::logical_expr::Distinct) -> &LogicalPlan {
    use datafusion::logical_expr::Distinct;
    match distinct {
        Distinct::All(input) => input,
        Distinct::On(on) => &on.input,
    }
}

/// Returns `true` if `key` retains its scan-level domain through every
/// `Aggregate` / `Distinct` reachable in `plan`.
///
/// * `Aggregate` preserves a column's domain only when it appears in
///   `group_expr` as a plain `Expr::Column` reference.
/// * `Distinct::All` preserves every projected column (deduplication keeps
///   value identity).
/// * `Distinct::On(distinct_on)` preserves only the columns named in its `on`
///   list; for safety we conservatively require the key to appear there.
///
/// The walk follows only identity-preserving operators plus inner equi-joins —
/// the same shape `is_dim_like_subtree` accepts. Anything outside that vocab
/// (`Sort`, `Window`, etc.) is conservatively rejected by returning `false`.
fn key_preserved_through_summaries(plan: &LogicalPlan, key: &Column) -> bool {
    fn walk(plan: &LogicalPlan, key: &Column) -> bool {
        match plan {
            LogicalPlan::TableScan(_) => true,
            LogicalPlan::Projection(p) => walk(&p.input, key),
            LogicalPlan::SubqueryAlias(a) => walk(&a.input, key),
            LogicalPlan::Filter(f) => walk(&f.input, key),
            LogicalPlan::Limit(l) => walk(&l.input, key),
            LogicalPlan::Aggregate(a) => {
                let key_in_group = a
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(column) if column == key));
                key_in_group && walk(&a.input, key)
            }
            LogicalPlan::Distinct(distinct) => {
                use datafusion::logical_expr::Distinct;
                let key_kept = match distinct {
                    Distinct::All(_) => true,
                    Distinct::On(on) => on
                        .on_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::Column(column) if column == key)),
                };
                key_kept && walk(distinct_input(distinct), key)
            }
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Inner
                    && j.null_equality == NullEquality::NullEqualsNothing =>
            {
                walk(&j.left, key) || walk(&j.right, key)
            }
            _ => false,
        }
    }

    walk(plan, key)
}

fn collect_filter_column_names(expr: &Expr, columns: &mut BTreeSet<String>) {
    let _ = expr.apply(|e| {
        if let Expr::Column(column) = e {
            columns.insert(column.name.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

fn collect_propagated_filter_targets(expr: &Expr, targets: &mut BTreeSet<String>) {
    let _ = expr.apply(|e| {
        if let Expr::InSubquery(InSubquery {
            expr: target_expr,
            subquery,
            ..
        }) = e
            && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
        {
            targets.insert(target_expr.to_string());
            return Ok(TreeNodeRecursion::Jump);
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

/// Returns `true` if `plan` already contains a propagated-filter marker.
#[cfg(test)]
fn subtree_has_propagated_filter(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
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
#[cfg(test)]
fn expr_has_propagated_filter(expr: &Expr) -> bool {
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

/// Build a `SubqueryAlias(__cayenne_xclos__N, Projection([key_col], subtree))`
/// suitable for use as the inner plan of a [`Subquery`] referenced by an
/// [`InSubquery`] expression.
///
/// The alias name uses [`PROPAGATED_FILTER_ALIAS_PREFIX`] plus a unique id
/// from [`OptimizerConfig::alias_generator`], so each invocation produces a
/// distinct marker. The marker doubles as the cycle-detection sentinel
/// scanned by [`analyze_logical_side`].
fn build_key_projection_subquery(
    subtree: Arc<LogicalPlan>,
    key_col: &Column,
    alias_gen: &Arc<datafusion::common::alias::AliasGenerator>,
) -> Result<LogicalPlan> {
    let key_expr = Expr::Column(key_col.clone());
    let projection = LogicalPlan::Projection(Projection::try_new(vec![key_expr], subtree)?);
    let alias_name = alias_gen.next(PROPAGATED_FILTER_ALIAS_PREFIX);
    let aliased = SubqueryAlias::try_new(Arc::new(projection), TableReference::bare(alias_name))?;
    Ok(LogicalPlan::SubqueryAlias(aliased))
}

/// Wrap `input` with `Filter(target IN (subquery))` using the `subquery_plan`
/// (which must already be a `SubqueryAlias` named with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`]) as the right-hand side. `target` may be
/// a column or any expression whose columns all resolve in `input`'s schema —
/// the chbench `ascii(substr(c_state,1,1)) - 65` shape is supported through
/// this entry point.
fn wrap_with_in_subquery_filter_expr(
    input: Arc<LogicalPlan>,
    target: &Expr,
    subquery_plan: LogicalPlan,
) -> Result<LogicalPlan> {
    let predicate = Expr::InSubquery(InSubquery::new(
        Box::new(target.clone()),
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
        // dim-like nation table — gains an `n_regionkey` so the multi-hop
        // `region ⋈ nation` propagation tests can join through it.
        let nation_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
            Field::new("n_regionkey", DataType::Int64, false),
        ]));
        // dim-like region table for multi-hop tests.
        let region_schema = Arc::new(Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, true),
        ]));
        // fact-like supplier table
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));
        // fact-like customer table for expression-equi-key tests
        // (chbench `ascii(substr(c_state, 1, 1)) - 65` nation mapping).
        let customer_schema = Arc::new(Schema::new(vec![
            Field::new("c_id", DataType::Int64, false),
            Field::new("c_state", DataType::Utf8, true),
        ]));
        ctx.register_table(
            "nation",
            Arc::new(MemTable::try_new(Arc::clone(&nation_schema), vec![vec![]])?),
        )?;
        ctx.register_table(
            "region",
            Arc::new(MemTable::try_new(Arc::clone(&region_schema), vec![vec![]])?),
        )?;
        ctx.register_table(
            "supplier",
            Arc::new(MemTable::try_new(
                Arc::clone(&supplier_schema),
                vec![vec![]],
            )?),
        )?;
        ctx.register_table(
            "customer",
            Arc::new(MemTable::try_new(
                Arc::clone(&customer_schema),
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

    fn count_propagated_filter_exprs(plan: &LogicalPlan) -> usize {
        let mut count = 0;
        let _ = plan.apply(|node| {
            if let LogicalPlan::Filter(f) = node {
                let _ = f.predicate.apply(|expr| {
                    if let Expr::InSubquery(InSubquery { subquery, .. }) = expr
                        && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
                        && alias
                            .alias
                            .table()
                            .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
                    {
                        count += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
            }
            Ok(TreeNodeRecursion::Continue)
        });
        count
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
    /// Mirrors what `DataFusion`'s optimizer driver does for an
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

        // Depending on `DataFusion`'s planner the join's `left`/`right` may be
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
        let (second_plan, changed2) = apply_rule_to_all_joins(&r, transformed_plan.clone(), &cfg)?;
        assert!(
            !changed2,
            "second pass must not re-propagate (cycle guard); plan was:\n{second_plan}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn left_semi_join_with_dim_filter_propagates_via_subquery() -> Result<()> {
        // The `IN (subquery)` shape that `decorrelate_predicate_subquery`
        // rewrites into a `LeftSemi` join. The propagation rule must still
        // fire on the resulting semi-join so the dim filter reaches the fact
        // side across chained joins.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier \
                 WHERE s_nationkey IN \
                   (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA')",
            )
            .await?
            .into_optimized_plan()?;

        // Sanity-check that decorrelation produced a semi-join shape; if it
        // didn't, this test is testing the wrong thing.
        let mut semi_seen = false;
        let _ = plan.apply(|node| {
            if let LogicalPlan::Join(j) = node
                && matches!(j.join_type, JoinType::LeftSemi | JoinType::RightSemi)
            {
                semi_seen = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        assert!(
            semi_seen,
            "expected decorrelation to produce a semi-join; plan was:\n{plan}"
        );

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            changed,
            "rule should fire on semi-join with dim-side non-key filter; plan was:\n{plan}"
        );
        assert!(
            find_propagated_side(&transformed_plan).is_some(),
            "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
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
    async fn inner_join_with_expression_fact_key_propagates_dim_filter() -> Result<()> {
        // The canonical chbench Q5/Q7/Q10 shape: a non-trivial expression on
        // the fact side and a pure column on the dim side, with the dim side
        // carrying the selective non-key filter.
        //
        // The rule must fire on `(Column, Expr)` (or `(Expr, Column)`) equi-key
        // pairs even though neither side is a pure column-column join.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT c_id FROM customer, nation \
                 WHERE ascii(substr(c_state, 1, 1)) - 65 = n_nationkey \
                   AND n_name = 'CHINA'",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            changed,
            "rule should fire on expression-vs-column equi-key; plan was:\n{plan}"
        );
        assert!(
            find_propagated_side(&transformed_plan).is_some(),
            "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
        );

        // Cycle prevention: running the rule a second time must be a no-op
        // (the unified Display-keyed cycle guard tracks the InSubquery target
        // expression, not just column targets).
        let (_, changed2) = apply_rule_to_all_joins(&r, transformed_plan.clone(), &cfg)?;
        assert!(
            !changed2,
            "second pass must not re-propagate (cycle guard) on expression target"
        );
        Ok(())
    }

    #[tokio::test]
    async fn multi_hop_dim_subtree_propagates_through_region_nation() -> Result<()> {
        // The canonical Q5 shape: `region ⋈ nation ⋈ supplier` with a
        // selective filter on `region.r_name`. With the multi-hop dim
        // detector the `region ⋈ nation` subtree counts as dim-like, so the
        // rule can propagate the filtered `n_nationkey` domain to `supplier`
        // in a single pass instead of waiting for the optimizer's fixed
        // point.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation, region \
                 WHERE s_nationkey = n_nationkey \
                   AND n_regionkey = r_regionkey \
                   AND r_name = 'ASIA'",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            changed,
            "rule should propagate r_name filter through the multi-hop dim subtree; \
             plan was:\n{plan}"
        );
        assert!(
            find_propagated_side(&transformed_plan).is_some(),
            "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
        );
        Ok(())
    }

    #[test]
    fn key_preserved_through_summaries_accepts_distinct_all() -> Result<()> {
        // `Distinct::All` deduplicates whole rows but preserves every column's
        // values (it can only remove duplicate rows), so any join key survives.
        use datafusion::logical_expr::Distinct;
        use datafusion_expr::builder::table_scan;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let distinct = LogicalPlan::Distinct(Distinct::All(Arc::new(scan)));

        let key_a = Column::new(Some("t"), "a");
        let key_b = Column::new(Some("t"), "b");

        assert!(key_preserved_through_summaries(&distinct, &key_a));
        assert!(key_preserved_through_summaries(&distinct, &key_b));
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_dim_propagates_when_key_is_in_group_by() -> Result<()> {
        // Pre-aggregated dim: `SELECT n_nationkey, count(*) FROM nation
        // WHERE n_name = 'CHINA' GROUP BY n_nationkey` joined against
        // supplier. The aggregate's GROUP BY includes `n_nationkey`, so the
        // key's domain is preserved through the aggregation and the rule
        // should still propagate to supplier.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, \
                 (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA' \
                  GROUP BY n_nationkey) AS n_agg \
                 WHERE s_nationkey = n_nationkey",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            changed,
            "rule should fire when dim has Aggregate(GROUP BY key); plan was:\n{plan}"
        );
        assert!(
            find_propagated_side(&transformed_plan).is_some(),
            "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
        );
        Ok(())
    }

    #[test]
    fn key_preserved_through_summaries_rejects_aggregate_without_key_in_group() -> Result<()> {
        // Sanity-check the helper: an aggregate that does NOT group by `a`
        // must report the key as not preserved.
        use datafusion::logical_expr::Aggregate;
        use datafusion_expr::builder::table_scan;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let agg = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(scan),
            vec![Expr::Column(Column::new(Some("t"), "b"))],
            vec![],
        )?);

        let key_a = Column::new(Some("t"), "a");
        let key_b = Column::new(Some("t"), "b");

        assert!(
            !key_preserved_through_summaries(&agg, &key_a),
            "`a` aggregated away, must not be preserved"
        );
        assert!(
            key_preserved_through_summaries(&agg, &key_b),
            "`b` is in GROUP BY, must be preserved"
        );
        Ok(())
    }

    #[test]
    fn key_preserved_through_summaries_rejects_same_name_different_relation() -> Result<()> {
        use datafusion::logical_expr::{Aggregate, Distinct, DistinctOn};
        use datafusion_expr::builder::table_scan;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let scan = table_scan(Some("t2"), &schema, None)?.build()?;
        let t1_key = Column::new(Some("t1"), "a");
        let t2_key = Column::new(Some("t2"), "a");

        let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(scan.clone()),
            vec![Expr::Column(t2_key.clone())],
            vec![],
        )?);
        assert!(
            !key_preserved_through_summaries(&aggregate, &t1_key),
            "same-name GROUP BY columns from a different relation must not preserve the key"
        );

        let distinct_on = LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
            vec![Expr::Column(t2_key.clone())],
            vec![Expr::Column(t2_key)],
            None,
            Arc::new(scan),
        )?));
        assert!(
            !key_preserved_through_summaries(&distinct_on, &t1_key),
            "same-name DISTINCT ON columns from a different relation must not preserve the key"
        );

        Ok(())
    }

    #[tokio::test]
    async fn inner_join_with_key_only_filter_is_noop() -> Result<()> {
        // `n_nationkey = 22` references only the join key — `DataFusion`'s
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
    fn null_equal_inner_join_is_noop() -> Result<()> {
        use datafusion::logical_expr::JoinConstraint;
        use datafusion_expr::{builder::table_scan, lit};

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

        let left_scan = table_scan(Some("l"), &left_schema, None)?.build()?;
        let left = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("l"), "c")).eq(lit("v")),
            Arc::new(left_scan),
        )?);
        let right = table_scan(Some("r"), &right_schema, None)?.build()?;

        let join = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![(
                Expr::Column(Column::new(Some("l"), "a")),
                Expr::Column(Column::new(Some("r"), "x")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNull,
        )?);

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, join, &cfg)?;

        assert!(
            !changed,
            "rule must not introduce SQL IN filters for null-equal joins"
        );
        Ok(())
    }

    #[test]
    fn composite_join_receives_one_filter_per_non_key_constrained_key() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinConstraint;
        use datafusion_expr::{builder::table_scan, lit};

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));

        let left_scan = table_scan(Some("l"), &left_schema, None)?.build()?;
        let left = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("l"), "c")).eq(lit("v")),
            Arc::new(left_scan),
        )?);
        let right = table_scan(Some("r"), &right_schema, None)?.build()?;

        let join = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![
                (
                    Expr::Column(Column::new(Some("l"), "a")),
                    Expr::Column(Column::new(Some("r"), "x")),
                ),
                (
                    Expr::Column(Column::new(Some("l"), "b")),
                    Expr::Column(Column::new(Some("r"), "y")),
                ),
            ],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, join, &cfg)?;

        assert!(
            changed,
            "rule should fire on composite inner join with side-local non-key filter"
        );
        assert_eq!(
            count_propagated_filter_exprs(&transformed_plan),
            2,
            "each matching composite key should get one propagated filter; plan was:\n{transformed_plan}"
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
