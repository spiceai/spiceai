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
//! The flagship rules here are [`CayennePropagateFilterAcrossEquiJoinKeys`]
//! and [`CayenneReassociateCrossJoin`], the plan-time rewrites that expose
//! selective key domains and avoid preserving expensive join-order shapes.
//!
//! `DataFusion`'s stock `infer_join_predicates` (in `push_down_filter`) already
//! propagates predicates that *directly* reference a join-key column:
//! `WHERE nation.n_nationkey = 5 AND nation.n_nationkey = supplier.s_nationkey`
//! is transformed into `WHERE supplier.s_nationkey = 5 AND ...`. That covers
//! the `n_nationkey = $const` shape but misses the common star/snowflake
//! shape, where the selective filter is on a *non-key* column (`n_name = 'CHINA'`). The
//! cardinality bound the dim-table filter implies for the equi-joined key
//! column never reaches the fact-table scans, so by the time the planner
//! orders joins from the SQL `FROM` clause, `(supplier, order_line, …)`
//! has already been chosen with no nation filter pushed through.
//!
//! ## What the rule does
//!
//! For every `LogicalPlan::Join` with `JoinType::Inner`, `JoinType::LeftSemi`,
//! or `JoinType::RightSemi`, default SQL NULL equality (`NULL != NULL`), and
//! one or more column-vs-column equi-key pairs whose data types match, the rule
//! inspects each side for a non-trivial `Filter` that references at least one
//! column other than each candidate join key. If one side is dim-like, has a
//! projectable column key, and the opposite side is a Cayenne-backed scan
//! subtree, it wraps that opposite side with
//!
//! ```text
//! Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))
//! ```
//!
//! The inserted subquery re-projects the join key through whatever filters
//! already exist on the original side, so `DataFusion`'s
//! `decorrelate_predicate_subquery` and `push_down_filter` can then plant a
//! `LeftSemi` join (or, after pushdown, a partition-pruning predicate) on
//! the fact-table scan. For example, this turns
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
//! Outer joins and expression join keys are excluded. They can be legal to
//! rewrite in narrow cases, but HTAP workloads showed the extra semi-join shape
//! can cost more than it saves outside selective dimension-to-fact pruning.
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
//!
//! Two cardinality gates further suppress propagations that wouldn't pay off at
//! runtime, when the underlying [`TableSource`]s expose row counts via
//! `TableProvider::statistics`:
//!
//! * [`MIN_FACT_ROWS_FOR_PROPAGATION`] — skip when the receiving fact
//!   subtree's known upper-bound row count is below the threshold. Below it
//!   there isn't enough probe-side cardinality for the filter to save
//!   meaningful work, and the plain hash join wins.
//! * [`MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO`] — skip unless the receiving side is
//!   much larger than the filtered side's join-key domain. This keeps
//!   small-domain pruning, while avoiding broad propagation across
//!   similarly sized HTAP joins.
//!
//! Statistics are required before propagation fires: the receiving subtree must
//! have a known row-count upper bound, and the filtered side's join-key domain
//! must be known to be much smaller. Missing cardinality evidence is treated as
//! a no-op because the duplicated subquery and added semi-join only pay off
//! when the fact-to-dim ratio is clear.

use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DataFusionError, NullEquality, Result, Spans, TableReference};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{
    Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection, Subquery, SubqueryAlias,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::ExprSchemable;
use datafusion_expr::expr::InSubquery;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{Expr, Operator, TableSource};
use std::{collections::BTreeSet, sync::Arc};

use crate::provider::CayenneTableProvider;

/// Prefix for [`SubqueryAlias`] names introduced by
/// [`CayennePropagateFilterAcrossEquiJoinKeys`].
///
/// Used both as a sentinel for key-scoped cycle detection (the rule refuses to
/// add another propagated filter for a target key that already has one) and as
/// a marker in explain output so the rewrite is recognizable when reading plans.
pub const PROPAGATED_FILTER_ALIAS_PREFIX: &str = "__cayenne_xclos__";

type TableProviderPredicate = Arc<dyn Fn(&dyn TableProvider) -> bool + Send + Sync>;
type TableSourcePredicate = Arc<dyn Fn(&dyn TableSource) -> bool + Send + Sync>;

/// Logical optimizer rule that, for each `Inner`, `LeftSemi`, or `RightSemi`
/// join with default SQL NULL equality and a simple column equi-key
/// `(left.a = right.b)`, introduces
/// `Filter(other_side.key IN (SELECT this_side.key FROM this_side_subtree))`
/// on the Cayenne-backed side opposite a non-key filter.
///
/// See the module-level docs for the full design and selective join-domain motivation.
pub struct CayennePropagateFilterAcrossEquiJoinKeys {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayennePropagateFilterAcrossEquiJoinKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl CayennePropagateFilterAcrossEquiJoinKeys {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
    ///
    /// Runtime registration uses this to recognize `AcceleratedTable`s whose
    /// inner accelerator is Cayenne, while this crate's default stays scoped to
    /// direct [`CayenneTableProvider`] scans.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a new instance with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
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
        // TopDown: process higher joins first so propagation seeds reach
        // nested joins on the next pass.
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
        if matches!(join.join_type, JoinType::LeftSemi)
            && right_side_carries_propagation_marker(&join.right)
        {
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

        for EquiKey { left, right } in &equijoin_keys {
            // Propagate the LEFT-side filtered key domain → the RIGHT side.
            if contains_cayenne_table_scan_with_column(
                &join.right,
                right,
                &self.is_cayenne_table_source,
            ) && left_analysis.is_dim_like
                && left_analysis.has_selective_non_key_filter(left)
                && key_preserved_through_summaries(&join.left, left)
                && !skip_propagation_by_cardinality(&join.left, &join.right, left)
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
            if contains_cayenne_table_scan_with_column(
                &join.left,
                left,
                &self.is_cayenne_table_source,
            ) && right_analysis.is_dim_like
                && right_analysis.has_selective_non_key_filter(right)
                && key_preserved_through_summaries(&join.right, right)
                && !skip_propagation_by_cardinality(&join.right, &join.left, right)
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
    selective_filter_columns: Vec<Column>,
    /// Targets of already-propagated `InSubquery` filters on this side, keyed
    /// by the `Display` form of the target expression. Used for cycle
    /// prevention — the same target should not be wrapped twice.
    propagated_filter_targets: BTreeSet<String>,
}

impl SideAnalysis {
    fn has_selective_non_key_filter(&self, key: &Column) -> bool {
        self.selective_filter_columns
            .iter()
            .any(|column| !columns_match(column, key))
    }

    fn has_propagated_filter_target(&self, target: &Expr) -> bool {
        self.propagated_filter_targets.contains(&target.to_string())
    }

    fn add_propagated_filter_target(&mut self, target: &Expr) {
        self.propagated_filter_targets.insert(target.to_string());
    }
}

/// Logical optimizer rule that reassociates a left-deep inner join when the
/// SQL `FROM` order leaves an early cross join in front of a later selective
/// join.
///
/// A typical shape is `(A CROSS B) JOIN C`, where the parent join predicates
/// only reference `B` and `C`. This rule rewrites that to `A CROSS (B JOIN C)`
/// while preserving the final output schema order. If the parent join also
/// contains predicates involving `A`, only the `B`/`C` predicates move inward
/// and the `A` predicates remain on the outer join.
pub struct CayenneReassociateCrossJoin {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayenneReassociateCrossJoin {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneReassociateCrossJoin {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
    ///
    /// Runtime registration uses this to recognize `AcceleratedTable`s whose
    /// inner accelerator is Cayenne, while this crate's default stays scoped to
    /// direct [`CayenneTableProvider`] scans.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a new instance with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
    }
}

impl std::fmt::Debug for CayenneReassociateCrossJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneReassociateCrossJoin").finish()
    }
}

impl OptimizerRule for CayenneReassociateCrossJoin {
    fn name(&self) -> &'static str {
        "cayenne_reassociate_cross_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // BottomUp lets a single optimizer pass grow `A CROSS (B JOIN C)` into
        // `A JOIN (B JOIN C JOIN D)` as later joins expose more B/D predicates.
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };

        reassociate_cross_join(join, &self.is_cayenne_table_source)
    }
}

/// Logical optimizer rule that rewrites `column IN (k, k+1, …, k+N-1)` to
/// `column BETWEEN k AND k+N-1` for single-table Cayenne-backed filter inputs
/// when the list contents are integer literals sorted unique and consecutive.
/// BETWEEN is ~50 % faster than IN-list at per-row predicate evaluation.
/// Running this as a logical-plan rule (rather than in `TableProvider::scan`)
/// lets `DataFusion`'s downstream simplification passes treat the result
/// identically to a SQL-parsed `BETWEEN`. See bench
/// `pk_in_list_vs_range_rewrite`.
pub struct CayenneInListToRangeRewrite {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayenneInListToRangeRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneInListToRangeRewrite {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a new instance with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
    }
}

impl std::fmt::Debug for CayenneInListToRangeRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneInListToRangeRewrite").finish()
    }
}

impl OptimizerRule for CayenneInListToRangeRewrite {
    fn name(&self) -> &'static str {
        "cayenne_inlist_to_range_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !is_single_cayenne_table_scan_input(&filter.input, &self.is_cayenne_table_source) {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        let original = filter.predicate.clone();
        let rewritten = original
            .clone()
            .transform_up(|expr| {
                let after =
                    crate::provider::table::rewrite_consecutive_inlist_to_range(expr.clone());
                if after == expr {
                    Ok(Transformed::no(expr))
                } else {
                    Ok(Transformed::yes(after))
                }
            })?
            .data;
        if rewritten == original {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let new_filter = Filter::try_new(rewritten, filter.input)?;
        Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
    }
}

fn is_single_cayenne_table_scan_input(
    plan: &LogicalPlan,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut total_scans = 0_usize;
    let mut cayenne_scans = 0_usize;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            total_scans = total_scans.saturating_add(1);
            if is_cayenne_table_source(scan.source.as_ref()) {
                cayenne_scans = cayenne_scans.saturating_add(1);
            }
            if total_scans > 1 {
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });
    total_scans == 1 && cayenne_scans == 1
}

fn reassociate_cross_join(
    join: Join,
    is_cayenne_table_source: &TableSourcePredicate,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    if !is_reassociable_inner_join(&join) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let LogicalPlan::Join(cross_join) = join.left.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };
    if !is_pure_inner_cross_join(cross_join) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    if !contains_cayenne_table_scan(&cross_join.right, is_cayenne_table_source)
        && !contains_cayenne_table_scan(&join.right, is_cayenne_table_source)
    {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let cross_left = cross_join.left.as_ref();
    let cross_right = cross_join.right.as_ref();
    let join_right = join.right.as_ref();

    let mut inner_on = Vec::new();
    let mut outer_on = Vec::new();
    for (left, right) in &join.on {
        let Some(left_refs) = expr_input_refs(left, cross_left, cross_right, join_right) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        let Some(right_refs) = expr_input_refs(right, cross_left, cross_right, join_right) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };

        if left_refs.only_cross_right() && right_refs.only_join_right() {
            inner_on.push((left.clone(), right.clone()));
        } else if left_refs.only_cross_left() && right_refs.only_join_right() {
            outer_on.push((left.clone(), right.clone()));
        } else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
    }

    if inner_on.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let mut inner_filters = Vec::new();
    let mut outer_filters = Vec::new();
    if let Some(filter) = join.filter.clone() {
        for conjunct in split_conjunction_owned(filter) {
            let Some(refs) = expr_input_refs(&conjunct, cross_left, cross_right, join_right) else {
                return Ok(Transformed::no(LogicalPlan::Join(join)));
            };

            if refs.only_cross_right_and_join_right() {
                inner_filters.push(conjunct);
            } else {
                outer_filters.push(conjunct);
            }
        }
    }

    let inner_join = LogicalPlan::Join(Join::try_new(
        Arc::clone(&cross_join.right),
        Arc::clone(&join.right),
        inner_on,
        conjunction(inner_filters),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
    )?);

    let outer_join = Join::try_new(
        Arc::clone(&cross_join.left),
        Arc::new(inner_join),
        outer_on,
        conjunction(outer_filters),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
    )?;

    Ok(Transformed::yes(LogicalPlan::Join(outer_join)))
}

fn is_reassociable_inner_join(join: &Join) -> bool {
    join.join_type == JoinType::Inner
        && join.join_constraint == JoinConstraint::On
        && join.null_equality == NullEquality::NullEqualsNothing
}

fn is_pure_inner_cross_join(join: &Join) -> bool {
    is_reassociable_inner_join(join) && join.on.is_empty() && join.filter.is_none()
}

#[derive(Default)]
struct JoinInputRefs {
    cross_left: bool,
    cross_right: bool,
    join_right: bool,
}

impl JoinInputRefs {
    fn only_cross_left(&self) -> bool {
        self.cross_left && !self.cross_right && !self.join_right
    }

    fn only_cross_right(&self) -> bool {
        !self.cross_left && self.cross_right && !self.join_right
    }

    fn only_join_right(&self) -> bool {
        !self.cross_left && !self.cross_right && self.join_right
    }

    fn only_cross_right_and_join_right(&self) -> bool {
        !self.cross_left && self.cross_right && self.join_right
    }
}

fn expr_input_refs(
    expr: &Expr,
    cross_left: &LogicalPlan,
    cross_right: &LogicalPlan,
    join_right: &LogicalPlan,
) -> Option<JoinInputRefs> {
    if expr.is_volatile() {
        return None;
    }

    let mut refs = JoinInputRefs::default();
    let mut unknown = false;
    let _ = expr.apply(|node| {
        match node {
            Expr::Column(column) => {
                match (
                    cross_left.schema().has_column(column),
                    cross_right.schema().has_column(column),
                    join_right.schema().has_column(column),
                ) {
                    (true, false, false) => refs.cross_left = true,
                    (false, true, false) => refs.cross_right = true,
                    (false, false, true) => refs.join_right = true,
                    _ => {
                        unknown = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Expr::OuterReferenceColumn(_, _) => {
                unknown = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    });

    if unknown { None } else { Some(refs) }
}

fn contains_cayenne_table_scan(
    plan: &LogicalPlan,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && is_cayenne_table_source(scan.source.as_ref())
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    });
    found
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
            collect_selective_filter_columns(
                &filter.predicate,
                &mut analysis.selective_filter_columns,
            );
            collect_propagated_filter_targets(
                &filter.predicate,
                &mut analysis.propagated_filter_targets,
            );
        }
        // Post-decorrelation cycle detection: `decorrelate_predicate_subquery`
        // rewrites our propagated `InSubquery` into a `LeftSemi` join with the
        // marker `SubqueryAlias` as its right child. Without this branch the
        // rule's cycle guard misses the marker (it only walked Filter
        // predicates), and the optimizer would re-propagate on every iteration
        // until hitting `max_passes`, stacking redundant `LeftSemi` layers.
        if let LogicalPlan::Join(join) = node
            && matches!(join.join_type, JoinType::LeftSemi)
            && right_side_carries_propagation_marker(&join.right)
        {
            for (left_expr, _) in &join.on {
                analysis
                    .propagated_filter_targets
                    .insert(left_expr.to_string());
            }
        }

        Ok(TreeNodeRecursion::Continue)
    });

    analysis
}

fn contains_cayenne_table_scan_with_column(
    plan: &LogicalPlan,
    target_column: &Column,
    is_cayenne_table_source: &TableSourcePredicate,
) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && is_cayenne_table_source(scan.source.as_ref())
            && table_scan_has_column(scan, target_column)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn table_scan_has_column(scan: &datafusion::logical_expr::TableScan, column: &Column) -> bool {
    scan.projected_schema.has_column(column)
        || scan
            .projected_schema
            .qualified_field_with_unqualified_name(&column.name)
            .is_ok()
}

/// Returns `true` if `plan` is — possibly behind a chain of `Projection` or
/// `SubqueryAlias` wrappers added by later optimizer rules — a `SubqueryAlias`
/// whose name starts with [`PROPAGATED_FILTER_ALIAS_PREFIX`].
fn right_side_carries_propagation_marker(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            if alias
                .alias
                .table()
                .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
            {
                return true;
            }
            right_side_carries_propagation_marker(&alias.input)
        }
        LogicalPlan::Projection(p) => right_side_carries_propagation_marker(&p.input),
        _ => false,
    }
}

/// A column-vs-column equi-join key from `Join::on`.
struct EquiKey {
    left: Column,
    right: Column,
}

/// Return the column-vs-column equi-join keys from `join.on` whose data types
/// match. Drops expression keys and pairs whose types differ (the `IN` subquery
/// would need an implicit cast we don't insert here).
fn matching_equijoin_keys(join: &Join) -> Vec<EquiKey> {
    join.on
        .iter()
        .filter_map(|(left, right)| {
            if !join_key_types_match(left, right, &join.left, &join.right) {
                return None;
            }

            match (left, right) {
                (Expr::Column(l), Expr::Column(r)) => Some(EquiKey {
                    left: l.clone(),
                    right: r.clone(),
                }),
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
/// Chosen to cover common dimension snowflakes
/// (`region ⋈ nation ⋈ supplier`, three leaves) without admitting arbitrarily
/// large dim joins whose re-execution under an `InSubquery` would be expensive.
const MAX_DIM_LIKE_TABLE_SCANS: usize = 3;

/// Skip propagation when the receiving fact subtree's known upper-bound row
/// count is below this threshold. Below it there isn't enough probe
/// cardinality for the filter to recoup the propagation overhead.
const MIN_FACT_ROWS_FOR_PROPAGATION: usize = 100_000;

/// Skip propagation unless the receiving fact subtree is at least this many
/// times larger than the dim side's propagated join-key domain.
const MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO: usize = 10;

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

/// Sum of known upper-bound row counts of all `TableScan`s reachable from
/// `plan`. Returns `None` if any reachable `TableScan` is missing stats — the
/// caller falls back to the structural gates in that case.
///
/// The walk follows every `LogicalPlan` child (not just dim-like wrappers) so
/// fact-side subtrees with joins, aggregates, etc. are summed too. The result
/// is a loose *upper bound* — filter selectivity isn't accounted for — which
/// is the right direction for the "skip if known small" gate (a true upper
/// bound below the threshold guarantees the subtree is actually small).
fn subtree_upper_bound_rows(plan: &LogicalPlan) -> Option<usize> {
    let mut total: usize = 0;
    let mut any_unknown = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(n) = table_scan_upper_bound_rows(scan) {
                total = total.saturating_add(n);
            } else {
                any_unknown = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if any_unknown { None } else { Some(total) }
}

fn table_scan_upper_bound_rows(scan: &datafusion::logical_expr::TableScan) -> Option<usize> {
    use datafusion::common::stats::Precision;

    scan.source
        .as_any()
        .downcast_ref::<DefaultTableSource>()
        .and_then(|default| default.table_provider.statistics())
        .and_then(|stats| match stats.num_rows {
            Precision::Exact(n) | Precision::Inexact(n) => Some(n),
            Precision::Absent => None,
        })
}

fn key_for_input_schema(input: &LogicalPlan, key: &Column) -> Option<Column> {
    input
        .schema()
        .qualified_field_with_unqualified_name(&key.name)
        .ok()
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name().clone()))
}

/// Upper bound for the number of rows that can contribute values for `key`.
///
/// This intentionally tracks the key's source domain instead of summing every
/// scan under the dim side. For q17-like aggregates, the filtered side may
/// include a large fact scan, but the propagated key domain is still bounded by
/// the grouped dimension key (for example `item.i_id`).
fn key_domain_upper_bound_rows(plan: &LogicalPlan, key: &Column) -> Option<usize> {
    if !plan.schema().has_column(key) {
        return None;
    }

    match plan {
        LogicalPlan::TableScan(scan) => table_scan_upper_bound_rows(scan),
        LogicalPlan::Filter(filter) => key_domain_upper_bound_rows(&filter.input, key),
        LogicalPlan::Limit(limit) => key_domain_upper_bound_rows(&limit.input, key),
        LogicalPlan::Projection(projection) => {
            let index = projection.schema.maybe_index_of_column(key)?;
            let expr = projection.expr.get(index)?;
            key_domain_upper_bound_rows_for_expr(&projection.input, expr)
        }
        LogicalPlan::SubqueryAlias(alias) => key_for_input_schema(&alias.input, key)
            .and_then(|input_key| key_domain_upper_bound_rows(&alias.input, &input_key)),
        LogicalPlan::Aggregate(aggregate) => {
            let key_in_group = aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::Column(column) if column == key));
            if key_in_group {
                key_domain_upper_bound_rows(&aggregate.input, key)
            } else {
                None
            }
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
            if key_kept {
                key_domain_upper_bound_rows(distinct_input(distinct), key)
            } else {
                None
            }
        }
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner
                && join.null_equality == NullEquality::NullEqualsNothing =>
        {
            let left_rows = if join.left.schema().has_column(key) {
                key_domain_upper_bound_rows(&join.left, key)
            } else {
                None
            };
            let right_rows = if join.right.schema().has_column(key) {
                key_domain_upper_bound_rows(&join.right, key)
            } else {
                None
            };
            match (left_rows, right_rows) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(rows), None) | (None, Some(rows)) => Some(rows),
                (None, None) => None,
            }
        }
        _ => None,
    }
}

fn key_domain_upper_bound_rows_for_expr(input: &LogicalPlan, expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column(column) => key_domain_upper_bound_rows(input, column),
        Expr::Alias(alias) => key_domain_upper_bound_rows_for_expr(input, &alias.expr),
        _ => None,
    }
}

/// `true` when propagation should be skipped based on cardinality.
///
/// Propagation only pays when the receiving side is known large and the
/// filtered key domain is known small. Missing statistics are therefore a
/// skip, not a fallback to shape-only heuristics.
fn skip_propagation_by_cardinality(
    dim_side: &LogicalPlan,
    fact_side: &LogicalPlan,
    dim_key: &Column,
) -> bool {
    let dim_key_domain_rows = key_domain_upper_bound_rows(dim_side, dim_key);

    tracing::debug!(
        dim_key_domain_rows = ?dim_key_domain_rows,
        "CayennePropagateFilterAcrossEquiJoinKeys: dim-side key-domain cardinality"
    );

    let fact_rows = subtree_upper_bound_rows(fact_side);

    tracing::debug!(
        fact_rows = ?fact_rows,
        "CayennePropagateFilterAcrossEquiJoinKeys: fact-side cardinality"
    );

    let Some(fact_rows) = fact_rows else {
        return true;
    };
    if fact_rows < MIN_FACT_ROWS_FOR_PROPAGATION {
        return true;
    }

    let Some(dim_key_domain_rows) = dim_key_domain_rows else {
        return true;
    };

    if dim_key_domain_rows == 0 {
        return false;
    }

    if fact_rows < dim_key_domain_rows.saturating_mul(MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO) {
        return true;
    }

    false
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
            LogicalPlan::TableScan(_) => plan.schema().has_column(key),
            LogicalPlan::Projection(p) => plan.schema().has_column(key) && walk(&p.input, key),
            LogicalPlan::SubqueryAlias(a) => {
                let relation_matches_alias = match key.relation.as_ref() {
                    Some(relation) => relation == &a.alias,
                    None => true,
                };
                relation_matches_alias
                    && key_for_input_schema(&a.input, key)
                        .is_some_and(|input_key| walk(&a.input, &input_key))
            }
            LogicalPlan::Filter(f) => plan.schema().has_column(key) && walk(&f.input, key),
            LogicalPlan::Limit(l) => plan.schema().has_column(key) && walk(&l.input, key),
            LogicalPlan::Aggregate(a) => {
                let key_in_group = a
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::Column(column) if column == key));
                key_in_group && plan.schema().has_column(key) && walk(&a.input, key)
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
                key_kept && plan.schema().has_column(key) && walk(distinct_input(distinct), key)
            }
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Inner
                    && j.null_equality == NullEquality::NullEqualsNothing =>
            {
                plan.schema().has_column(key) && (walk(&j.left, key) || walk(&j.right, key))
            }
            _ => false,
        }
    }

    walk(plan, key)
}

fn collect_selective_filter_columns(expr: &Expr, columns: &mut Vec<Column>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_selective_filter_columns(&binary.left, columns);
            collect_selective_filter_columns(&binary.right, columns);
        }
        Expr::BinaryExpr(binary) if comparison_operator_is_selective(binary.op) => {
            collect_literal_comparison_columns(&binary.left, &binary.right, columns);
            collect_literal_comparison_columns(&binary.right, &binary.left, columns);
        }
        Expr::Between(between) if !between.negated => {
            if expr_is_literal_like(&between.low) && expr_is_literal_like(&between.high) {
                collect_columns_from_expr(&between.expr, columns);
            }
        }
        Expr::InList(in_list) if !in_list.negated && !in_list.list.is_empty() => {
            if in_list.list.iter().all(expr_is_literal_like) {
                collect_columns_from_expr(&in_list.expr, columns);
            }
        }
        Expr::Like(like) if !like.negated && expr_is_literal_like(&like.pattern) => {
            collect_columns_from_expr(&like.expr, columns);
        }
        _ => {}
    }
}

fn comparison_operator_is_selective(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

fn collect_literal_comparison_columns(expr: &Expr, other: &Expr, columns: &mut Vec<Column>) {
    if expr_is_literal_like(other) {
        collect_columns_from_expr(expr, columns);
    }
}

fn expr_is_literal_like(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Cast(cast) => expr_is_literal_like(&cast.expr),
        Expr::TryCast(cast) => expr_is_literal_like(&cast.expr),
        _ => false,
    }
}

fn collect_columns_from_expr(expr: &Expr, columns: &mut Vec<Column>) {
    let _ = expr.apply(|e| {
        if let Expr::Column(column) = e
            && !columns
                .iter()
                .any(|existing| columns_match(existing, column))
        {
            columns.push(column.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    });
}

fn columns_match(left: &Column, right: &Column) -> bool {
    left == right
        || (left.name == right.name && (left.relation.is_none() || right.relation.is_none()))
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
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`]) as the right-hand side.
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
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::{DefaultTableSource, TableProvider};
    use datafusion::prelude::SessionContext;
    use datafusion_common::Statistics;
    use datafusion_expr::LogicalPlanBuilder;
    use std::sync::Arc;

    /// Wrapper around [`MemTable`] that exposes a fixed row count via
    /// [`TableProvider::statistics`]. The cardinality gates in
    /// [`skip_propagation_by_cardinality`] require stats to be present on the
    /// dim side; without this wrapper, test tables backed by `MemTable` would
    /// report `None` and propagation would be skipped.
    #[derive(Debug)]
    struct StatMemTable {
        inner: MemTable,
        num_rows: usize,
    }

    #[derive(Debug)]
    struct NoStatsTable {
        inner: MemTable,
    }

    impl StatMemTable {
        fn try_new(
            schema: Arc<Schema>,
            batches: Vec<Vec<arrow::array::RecordBatch>>,
            num_rows: usize,
        ) -> Result<Self> {
            Ok(Self {
                inner: MemTable::try_new(schema, batches)?,
                num_rows,
            })
        }
    }

    impl NoStatsTable {
        fn try_new(schema: Arc<Schema>, batches: Vec<Vec<RecordBatch>>) -> Result<Self> {
            Ok(Self {
                inner: MemTable::try_new(schema, batches)?,
            })
        }
    }

    #[async_trait::async_trait]
    impl TableProvider for StatMemTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        fn statistics(&self) -> Option<Statistics> {
            Some(Statistics {
                num_rows: Precision::Exact(self.num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            })
        }
    }

    #[async_trait::async_trait]
    impl TableProvider for NoStatsTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        fn statistics(&self) -> Option<Statistics> {
            None
        }
    }

    fn rule() -> CayennePropagateFilterAcrossEquiJoinKeys {
        CayennePropagateFilterAcrossEquiJoinKeys::new_with_table_source_predicate(|_| true)
    }

    fn cross_join_rule() -> CayenneReassociateCrossJoin {
        CayenneReassociateCrossJoin::new_with_table_source_predicate(|_| true)
    }

    /// Build a [`LogicalPlan::TableScan`] backed by a [`StatMemTable`] that
    /// reports `num_rows` via `TableProvider::statistics()`. Use this instead
    /// of `datafusion_expr::builder::table_scan` in tests that need the
    /// cardinality gates in [`skip_propagation_by_cardinality`] to pass.
    fn stat_table_scan(name: &str, schema: &Arc<Schema>, num_rows: usize) -> Result<LogicalPlan> {
        let provider = Arc::new(StatMemTable::try_new(
            Arc::clone(schema),
            vec![vec![]],
            num_rows,
        )?);
        let source = Arc::new(DefaultTableSource::new(provider));
        LogicalPlanBuilder::scan(name, source, None)?.build()
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
        // fact-like customer table for expression-equi-key no-op tests
        // (expression-derived nation mapping).
        let customer_schema = Arc::new(Schema::new(vec![
            Field::new("c_id", DataType::Int64, false),
            Field::new("c_state", DataType::Utf8, true),
        ]));
        // Dim tables use realistic small domains; fact tables are large enough
        // for the fact-to-dim key-domain ratio gate to allow pruning.
        ctx.register_table(
            "nation",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&nation_schema),
                vec![vec![]],
                25,
            )?),
        )?;
        ctx.register_table(
            "region",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&region_schema),
                vec![vec![]],
                5,
            )?),
        )?;
        ctx.register_table(
            "supplier",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&supplier_schema),
                vec![vec![]],
                500_000,
            )?),
        )?;
        ctx.register_table(
            "customer",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&customer_schema),
                vec![vec![]],
                500_000,
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
        assert_eq!(cross_join_rule().name(), "cayenne_reassociate_cross_join");
        assert_eq!(cross_join_rule().apply_order(), Some(ApplyOrder::BottomUp));
    }

    #[tokio::test]
    async fn default_rule_skips_non_cayenne_table_scans() -> Result<()> {
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await?
            .into_optimized_plan()?;

        let r = CayennePropagateFilterAcrossEquiJoinKeys::new();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "default rule must not rewrite non-Cayenne scans; plan was:\n{plan}"
        );
        Ok(())
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

    #[test]
    fn cross_join_reassociation_moves_b_c_join_under_cross() -> Result<()> {
        let supplier_schema = Arc::new(Schema::new(vec![Field::new(
            "su_suppkey",
            DataType::Int64,
            false,
        )]));
        let order_line_schema = Arc::new(Schema::new(vec![
            Field::new("ol_o_id", DataType::Int64, false),
            Field::new("ol_w_id", DataType::Int64, false),
            Field::new("ol_d_id", DataType::Int64, false),
            Field::new("ol_delivery_d", DataType::Int64, false),
        ]));
        let order_schema = Arc::new(Schema::new(vec![
            Field::new("o_id", DataType::Int64, false),
            Field::new("o_w_id", DataType::Int64, false),
            Field::new("o_d_id", DataType::Int64, false),
            Field::new("o_entry_d", DataType::Int64, false),
        ]));

        let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
        let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
        let order = stat_table_scan("oorder", &order_schema, 30_000)?;

        let cross = LogicalPlan::Join(Join::try_new(
            Arc::new(supplier),
            Arc::new(order_line),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let plan = LogicalPlan::Join(Join::try_new(
            Arc::new(cross),
            Arc::new(order),
            vec![
                (
                    Expr::Column(Column::new(Some("l1"), "ol_o_id")),
                    Expr::Column(Column::new(Some("oorder"), "o_id")),
                ),
                (
                    Expr::Column(Column::new(Some("l1"), "ol_w_id")),
                    Expr::Column(Column::new(Some("oorder"), "o_w_id")),
                ),
                (
                    Expr::Column(Column::new(Some("l1"), "ol_d_id")),
                    Expr::Column(Column::new(Some("oorder"), "o_d_id")),
                ),
            ],
            Some(
                Expr::Column(Column::new(Some("oorder"), "o_entry_d"))
                    .lt(Expr::Column(Column::new(Some("l1"), "ol_delivery_d"))),
            ),
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let original_schema = Arc::clone(plan.schema());

        let transformed = cross_join_rule().rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )?;
        assert!(
            transformed.transformed,
            "cross join with later selective predicates should be reassociated; plan was:\n{plan}"
        );
        assert_eq!(
            transformed.data.schema(),
            &original_schema,
            "reassociation must preserve output schema order"
        );

        let LogicalPlan::Join(outer) = &transformed.data else {
            panic!("expected outer join after reassociation")
        };
        assert!(
            outer.on.is_empty(),
            "supplier should remain cross-joined after the selective B/C join"
        );
        assert!(
            outer.filter.is_none(),
            "all parent predicates in this shape should move to the B/C join"
        );
        assert!(plan_is_table_scan(&outer.left, "supplier"));

        let LogicalPlan::Join(inner) = outer.right.as_ref() else {
            panic!("expected order_line/oorder inner join under the outer cross join")
        };
        assert_eq!(inner.on.len(), 3);
        assert!(inner.filter.is_some());
        assert!(plan_is_table_scan(&inner.left, "l1"));
        assert!(plan_is_table_scan(&inner.right, "oorder"));

        Ok(())
    }

    #[test]
    fn cross_join_reassociation_keeps_a_c_predicates_on_outer_join() -> Result<()> {
        let supplier_schema = Arc::new(Schema::new(vec![Field::new(
            "su_suppkey",
            DataType::Int64,
            false,
        )]));
        let order_line_schema = Arc::new(Schema::new(vec![Field::new(
            "ol_i_id",
            DataType::Int64,
            false,
        )]));
        let stock_schema = Arc::new(Schema::new(vec![
            Field::new("s_i_id", DataType::Int64, false),
            Field::new("s_suppkey", DataType::Int64, false),
        ]));

        let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
        let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
        let stock = stat_table_scan("stock", &stock_schema, 100_000)?;

        let cross = LogicalPlan::Join(Join::try_new(
            Arc::new(supplier),
            Arc::new(order_line),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let plan = LogicalPlan::Join(Join::try_new(
            Arc::new(cross),
            Arc::new(stock),
            vec![
                (
                    Expr::Column(Column::new(Some("l1"), "ol_i_id")),
                    Expr::Column(Column::new(Some("stock"), "s_i_id")),
                ),
                (
                    Expr::Column(Column::new(Some("supplier"), "su_suppkey")),
                    Expr::Column(Column::new(Some("stock"), "s_suppkey")),
                ),
            ],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let transformed = cross_join_rule().rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )?;
        assert!(
            transformed.transformed,
            "B/C predicates should move inward while A/C predicates stay outside; plan was:\n{plan}"
        );

        let LogicalPlan::Join(outer) = &transformed.data else {
            panic!("expected outer join after reassociation")
        };
        assert_eq!(outer.on.len(), 1);
        assert!(expr_is_column_named(&outer.on[0].0, "su_suppkey"));

        let LogicalPlan::Join(inner) = outer.right.as_ref() else {
            panic!("expected l1/stock inner join under the outer supplier join")
        };
        assert_eq!(inner.on.len(), 1);
        assert!(expr_is_column_named(&inner.on[0].0, "ol_i_id"));

        Ok(())
    }

    #[test]
    fn cross_join_reassociation_requires_b_c_equi_key() -> Result<()> {
        let supplier_schema = Arc::new(Schema::new(vec![Field::new(
            "su_suppkey",
            DataType::Int64,
            false,
        )]));
        let order_line_schema = Arc::new(Schema::new(vec![Field::new(
            "ol_i_id",
            DataType::Int64,
            false,
        )]));
        let stock_schema = Arc::new(Schema::new(vec![Field::new(
            "s_suppkey",
            DataType::Int64,
            false,
        )]));

        let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
        let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
        let stock = stat_table_scan("stock", &stock_schema, 100_000)?;

        let cross = LogicalPlan::Join(Join::try_new(
            Arc::new(supplier),
            Arc::new(order_line),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let plan = LogicalPlan::Join(Join::try_new(
            Arc::new(cross),
            Arc::new(stock),
            vec![(
                Expr::Column(Column::new(Some("supplier"), "su_suppkey")),
                Expr::Column(Column::new(Some("stock"), "s_suppkey")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let transformed = cross_join_rule().rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )?;
        assert!(
            !transformed.transformed,
            "rule must not reassociate without a B/C equi-key to move inward; plan was:\n{plan}"
        );

        Ok(())
    }

    #[test]
    fn cross_join_reassociation_skips_non_cayenne_subtrees() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let middle_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

        let left = stat_table_scan("a", &left_schema, 10_000)?;
        let middle = stat_table_scan("b", &middle_schema, 300_000)?;
        let right = stat_table_scan("c", &right_schema, 30_000)?;
        let cross = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(middle),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let plan = LogicalPlan::Join(Join::try_new(
            Arc::new(cross),
            Arc::new(right),
            vec![(
                Expr::Column(Column::new(Some("b"), "b")),
                Expr::Column(Column::new(Some("c"), "c")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let rule = CayenneReassociateCrossJoin::new_with_table_source_predicate(|_| false);
        let transformed = rule.rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )?;
        assert!(
            !transformed.transformed,
            "rule must stay scoped to Cayenne-backed matched subtrees; plan was:\n{plan}"
        );

        Ok(())
    }

    #[test]
    fn cross_join_reassociation_skips_when_only_untouched_side_is_cayenne() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let middle_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

        let left = stat_table_scan("a", &left_schema, 10_000)?;
        let middle = stat_table_scan("b", &middle_schema, 300_000)?;
        let right = stat_table_scan("c", &right_schema, 30_000)?;
        let cross = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(middle),
            vec![],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);
        let plan = LogicalPlan::Join(Join::try_new(
            Arc::new(cross),
            Arc::new(right),
            vec![(
                Expr::Column(Column::new(Some("b"), "b")),
                Expr::Column(Column::new(Some("c"), "c")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let rule = CayenneReassociateCrossJoin::new_with_table_provider_predicate(|provider| {
            provider.schema().field_with_name("a").is_ok()
        });
        let transformed = rule.rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )?;
        assert!(
            !transformed.transformed,
            "rule must not reassociate a non-Cayenne B/C branch just because the untouched A side is Cayenne; plan was:\n{plan}"
        );

        Ok(())
    }

    fn plan_is_table_scan(plan: &LogicalPlan, table_name: &str) -> bool {
        matches!(plan, LogicalPlan::TableScan(scan) if scan.table_name.table() == table_name)
    }

    fn expr_is_column_named(expr: &Expr, column_name: &str) -> bool {
        matches!(expr, Expr::Column(column) if column.name == column_name)
    }

    #[tokio::test]
    async fn inner_join_with_dim_filter_propagates_via_subquery() -> Result<()> {
        // Representative large fact/dimension join shape:
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
    async fn stats_less_provider_propagation_is_skipped() -> Result<()> {
        let ctx = SessionContext::new();
        let nation_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
        ]));
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));

        let nation_batch = RecordBatch::try_new(
            Arc::clone(&nation_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("CHINA"), Some("FRANCE"), None])) as ArrayRef,
            ],
        )?;
        let supplier_batch = RecordBatch::try_new(
            Arc::clone(&supplier_schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12, 13])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 1, 4])) as ArrayRef,
            ],
        )?;

        ctx.register_table(
            "nation",
            Arc::new(NoStatsTable::try_new(
                nation_schema,
                vec![vec![nation_batch]],
            )?),
        )?;
        ctx.register_table(
            "supplier",
            Arc::new(NoStatsTable::try_new(
                supplier_schema,
                vec![vec![supplier_batch]],
            )?),
        )?;

        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier JOIN nation \
                 ON s_nationkey = n_nationkey \
                 WHERE n_name = 'CHINA' ORDER BY s_suppkey",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "rule should not propagate without cardinality evidence; plan was:\n{plan}"
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
    async fn left_outer_join_is_unchanged_even_when_preserved_side_has_filter() -> Result<()> {
        // `supplier LEFT JOIN nation ON s_nationkey = n_nationkey WHERE
        // s_name = 'X'`. The LEFT side (supplier) has a non-key filter; it is
        // the preserved side. This could be semantically safe to propagate to
        // the lookup side, but it adds an extra semi-join shape and was too
        // easy to over-apply in HTAP workloads.
        //
        // Note: `eliminate_outer_join` will rewrite the LEFT JOIN to an INNER
        // JOIN only if the WHERE clause forces the right side to be non-null
        // — using a filter on the LEFT side instead preserves the outer
        // semantics, which is what we want for this test.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey WHERE s_suppkey > 5",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "LEFT OUTER joins must stay unchanged by the rule; plan was:\n{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn left_outer_join_blocks_right_to_left_propagation() -> Result<()> {
        // Filter on the RIGHT (lookup) side of a LEFT OUTER must NOT cause
        // propagation onto the LEFT (preserved) side: doing so would drop
        // left rows the outer join should emit as `(left, NULL...)`.
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey \
                 WHERE n_name = 'CHINA' OR n_name IS NULL",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "RIGHT→LEFT propagation must not fire on LEFT OUTER; plan was:\n{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rule_does_not_re_fire_on_post_decorrelation_left_semi() -> Result<()> {
        // Regression test for the cycle-detection bug across optimizer
        // iterations: after Pass 1 wraps the receiving side with an
        // `InSubquery`, `decorrelate_predicate_subquery` rewrites that into a
        // `LeftSemi` join with the marker `SubqueryAlias` as its right child.
        // If the rule's cycle detection only sees `InSubquery` markers (and
        // not the structural `LeftSemi`-with-marker shape), Pass 2 sees no
        // marker on the receiving side and re-propagates, producing nested
        // LeftSemi joins on every subsequent optimizer pass.
        //
        // The fix detects the post-decorrelation shape and records the
        // already-propagated target so the rule's cycle guard short-circuits
        // on subsequent passes.
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinConstraint;
        use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
        ]));
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));

        // Build the dim subquery: `Filter(n_name='CHINA') → TableScan(nation)`
        // wrapped in the propagated-filter alias the rule would have produced.
        let nation_scan = table_scan(Some("nation"), &dim_schema, None)?.build()?;
        let nation_filter = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
            Arc::new(nation_scan),
        )?);
        let nation_projection = LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
            Arc::new(nation_filter),
        )?);
        let dim_subquery_alias = format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1");
        let dim_subquery = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(nation_projection),
            TableReference::bare(dim_subquery_alias),
        )?);

        // Build supplier scan (the receiving fact side).
        let supplier_scan = table_scan(Some("supplier"), &fact_schema, None)?.build()?;

        // Compose the post-decorrelation shape: `LeftSemi(supplier, dim_subquery)`
        // on `s_nationkey = n_nationkey`.
        let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
            .join_with_expr_keys(
                dim_subquery,
                JoinType::LeftSemi,
                (
                    vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                    vec![Expr::Column(Column::new(
                        Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
                        "n_nationkey",
                    ))],
                ),
                None,
            )?
            .build()?;

        // Now build an outer `Inner Join` between the *original* nation_filtered
        // and this `LeftSemi` subtree on the same equi-key — the exact shape an
        // optimizer pass would see after the rule already fired + decorrelated.
        let dim_filter_again_scan = table_scan(Some("nation_outer"), &dim_schema, None)?.build()?;
        let dim_filter_again = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
            Arc::new(dim_filter_again_scan),
        )?);

        let outer_join = LogicalPlan::Join(Join::try_new(
            Arc::new(dim_filter_again),
            Arc::new(semi_join_input),
            vec![(
                Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
                Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
        assert!(
            !changed,
            "rule must not re-fire when the receiving side already contains a \
             post-decorrelation LeftSemi propagation marker; plan was:\n{outer_join}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rule_re_fires_when_receiving_side_has_non_marker_subquery_alias() -> Result<()> {
        // Devil's-advocate edge case: a `LeftSemi` whose right side is a
        // `SubqueryAlias` with a *non-marker* name should NOT block
        // propagation (the marker prefix is the unique signal that this rule
        // already fired). Guards against the cycle guard being too aggressive.
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinConstraint;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
        ]));
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));

        let nation_scan = stat_table_scan("nation", &dim_schema, 5_000)?;
        let nation_filter = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
            Arc::new(nation_scan),
        )?);
        let nation_projection = LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
            Arc::new(nation_filter),
        )?);
        let user_alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(nation_projection),
            TableReference::bare("some_user_alias"),
        )?);

        let supplier_scan = stat_table_scan("supplier", &fact_schema, 500_000)?;
        let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
            .join_with_expr_keys(
                user_alias,
                JoinType::LeftSemi,
                (
                    vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                    vec![Expr::Column(Column::new(
                        Some("some_user_alias".to_string()),
                        "n_nationkey",
                    ))],
                ),
                None,
            )?
            .build()?;

        let outer_dim_scan = stat_table_scan("nation_outer", &dim_schema, 5_000)?;
        let outer_dim_filter = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
            Arc::new(outer_dim_scan),
        )?);

        let outer_join = LogicalPlan::Join(Join::try_new(
            Arc::new(outer_dim_filter),
            Arc::new(semi_join_input),
            vec![(
                Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
                Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
        assert!(
            changed,
            "rule should still fire when the receiving LeftSemi's alias is \
             user-supplied (not the propagation marker); plan was:\n{outer_join}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rule_detects_marker_through_projection_wrapper() -> Result<()> {
        // Subsequent optimizer rules (`MergeProjection`, etc.) may wrap the
        // marker `SubqueryAlias` in a `Projection`. The cycle guard must still
        // detect the marker through this wrapping.
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinConstraint;
        use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, true),
        ]));
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));

        let nation_scan = table_scan(Some("nation"), &dim_schema, None)?.build()?;
        let nation_filter = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
            Arc::new(nation_scan),
        )?);
        let inner_projection = LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
            Arc::new(nation_filter),
        )?);
        let marker_alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(inner_projection),
            TableReference::bare(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
        )?);
        let wrapped_marker = LogicalPlan::Projection(Projection::try_new(
            vec![Expr::Column(Column::new(
                Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
                "n_nationkey",
            ))],
            Arc::new(marker_alias),
        )?);

        let supplier_scan = table_scan(Some("supplier"), &fact_schema, None)?.build()?;
        let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
            .join_with_expr_keys(
                wrapped_marker,
                JoinType::LeftSemi,
                (
                    vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                    vec![Expr::Column(Column::new(
                        Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
                        "n_nationkey",
                    ))],
                ),
                None,
            )?
            .build()?;

        let outer_dim_scan = table_scan(Some("nation_outer"), &dim_schema, None)?.build()?;
        let outer_dim_filter = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
            Arc::new(outer_dim_scan),
        )?);

        let outer_join = LogicalPlan::Join(Join::try_new(
            Arc::new(outer_dim_filter),
            Arc::new(semi_join_input),
            vec![(
                Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
                Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
            )],
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )?);

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
        assert!(
            !changed,
            "cycle guard must detect a marker wrapped in an outer Projection; \
             plan was:\n{outer_join}"
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
    async fn inner_join_with_expression_fact_key_is_unchanged() -> Result<()> {
        // Common expression-key join shape: a non-trivial expression on
        // the fact side and a pure column on the dim side, with the dim side
        // carrying the selective non-key filter.
        //
        // These expression-key joins were valid to rewrite but too easy to
        // over-apply, so the selective key-domain rule leaves them alone.
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
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "rule must not fire on expression-vs-column equi-key; plan was:\n{plan}"
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

    #[tokio::test]
    async fn aggregate_propagates_using_group_key_domain() -> Result<()> {
        // A large outer fact scan can join to an aggregate over a filtered
        // dimension/fact subtree. The aggregate subtree contains a large fact
        // scan, but the propagated `i_id` domain is bounded by `item`, so the
        // ratio gate should still allow the aggregate-domain pruning path.
        let ctx = SessionContext::new();
        let item_schema = Arc::new(Schema::new(vec![
            Field::new("i_id", DataType::Int64, false),
            Field::new("i_data", DataType::Utf8, true),
        ]));
        let order_line_schema = Arc::new(Schema::new(vec![
            Field::new("ol_i_id", DataType::Int64, false),
            Field::new("ol_quantity", DataType::Int64, false),
        ]));
        ctx.register_table(
            "item",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&item_schema),
                vec![vec![]],
                100_000,
            )?),
        )?;
        ctx.register_table(
            "order_line",
            Arc::new(StatMemTable::try_new(
                Arc::clone(&order_line_schema),
                vec![vec![]],
                5_000_000,
            )?),
        )?;

        let plan = ctx
            .sql(
                "SELECT sum(ol_outer.ol_quantity) FROM order_line ol_outer, \
                 (SELECT i_id, avg(ol_inner.ol_quantity) AS a \
                  FROM item, order_line ol_inner \
                  WHERE i_data LIKE '%b' AND ol_inner.ol_i_id = i_id \
                  GROUP BY i_id) t \
                 WHERE ol_outer.ol_i_id = t.i_id AND ol_outer.ol_quantity < t.a",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;

        assert!(
            changed,
            "rule should keep q17-shaped aggregate propagation; plan was:\n{plan}"
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
    fn cardinality_gate_uses_key_domain_and_fact_ratio() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let key = Column::new(Some("dim"), "k");

        // Single scan: row count is reported directly.
        let small = stat_table_scan("dim", &schema, 500)?;
        assert_eq!(subtree_upper_bound_rows(&small), Some(500));
        assert_eq!(key_domain_upper_bound_rows(&small, &key), Some(500));

        // Large fact-to-dim ratio → gate is silent.
        let fact = stat_table_scan("fact", &schema, 1_000_000)?;
        assert!(!skip_propagation_by_cardinality(&small, &fact, &key));

        // Comparable sides → gate fires to avoid adding a semi-join that is
        // unlikely to pay for itself.
        let big_dim = stat_table_scan("dim", &schema, 50_000)?;
        let comparable_fact = stat_table_scan("fact", &schema, 200_000)?;
        assert!(skip_propagation_by_cardinality(
            &big_dim,
            &comparable_fact,
            &key
        ));

        // Below the fact threshold → gate fires from the fact side.
        let tiny_fact = stat_table_scan("fact", &schema, 50_000)?;
        assert!(skip_propagation_by_cardinality(&big_dim, &tiny_fact, &key));

        Ok(())
    }

    #[test]
    fn skip_propagation_by_cardinality_blocks_when_stats_absent() -> Result<()> {
        // MemTable doesn't expose row counts via `TableProvider::statistics()`,
        // so there is no clear evidence that the extra subquery will pay off.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let provider = Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]])?);
        let source = Arc::new(DefaultTableSource::new(provider));
        let scan = LogicalPlanBuilder::scan("t", source, None)?.build()?;
        let key = Column::new(Some("t"), "k");

        assert_eq!(subtree_upper_bound_rows(&scan), None);
        assert!(
            skip_propagation_by_cardinality(&scan, &scan, &key),
            "absent stats must trigger the cardinality gate"
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

    #[tokio::test]
    async fn inner_join_with_non_selective_non_key_filter_is_noop() -> Result<()> {
        let ctx = make_ctx()?;
        let plan = ctx
            .sql(
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name IS NOT NULL",
            )
            .await?
            .into_optimized_plan()?;

        let r = rule();
        let cfg = datafusion::optimizer::OptimizerContext::new();
        let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
        assert!(
            !changed,
            "rule must not fire for broad non-key predicates like IS NOT NULL; plan was:\n{plan}"
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
        use datafusion_expr::lit;

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]));

        let left_scan = stat_table_scan("l", &left_schema, 5_000)?;
        let left = LogicalPlan::Filter(Filter::try_new(
            Expr::Column(Column::new(Some("l"), "c")).eq(lit("v")),
            Arc::new(left_scan),
        )?);
        let right = stat_table_scan("r", &right_schema, 500_000)?;

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

    #[test]
    fn inlist_to_range_rule_rewrites_filter_predicate() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let in_list = Expr::Column(Column::new(Some("t"), "id"))
            .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
        let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(plan, &cfg)?;
        assert!(
            transformed.transformed,
            "rule should transform a Filter whose predicate is a rewritable InList"
        );
        let LogicalPlan::Filter(filter) = transformed.data else {
            panic!("expected Filter after rewrite")
        };
        assert!(
            matches!(filter.predicate, Expr::Between(_)),
            "predicate should be rewritten to Expr::Between, got: {:?}",
            filter.predicate
        );
        Ok(())
    }

    #[test]
    fn inlist_to_range_rule_leaves_sparse_inlist_untouched() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let in_list = Expr::Column(Column::new(Some("t"), "id"))
            .in_list(vec![lit(1_i64), lit(100_i64), lit(1000_i64)], false);
        let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(plan, &cfg)?;
        assert!(
            !transformed.transformed,
            "rule should leave sparse IN-list untouched"
        );
        Ok(())
    }

    #[test]
    fn inlist_to_range_rule_rewrites_nested_inside_and() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Int64, false),
        ]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let in_list = Expr::Column(Column::new(Some("t"), "id"))
            .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
        let combined = in_list.and(Expr::Column(Column::new(Some("t"), "status")).eq(lit(1_i64)));
        let plan = LogicalPlanBuilder::from(scan).filter(combined)?.build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(plan, &cfg)?;
        assert!(
            transformed.transformed,
            "rule should rewrite InList even when nested inside AND"
        );
        Ok(())
    }

    #[test]
    fn inlist_to_range_rule_leaves_short_consecutive_inlist_untouched() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let in_list = Expr::Column(Column::new(Some("t"), "id"))
            .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64)], false);
        let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(plan, &cfg)?;
        assert!(
            !transformed.transformed,
            "rule should leave short consecutive IN-list untouched"
        );
        Ok(())
    }

    #[test]
    fn inlist_to_range_rule_leaves_non_cayenne_filter_untouched() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{LogicalPlanBuilder, lit};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let in_list = Expr::Column(Column::new(Some("t"), "id"))
            .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
        let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| false);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(plan, &cfg)?;
        assert!(
            !transformed.transformed,
            "rule should leave non-Cayenne filter inputs untouched"
        );
        Ok(())
    }

    #[test]
    fn inlist_to_range_rule_leaves_join_filter_untouched() -> Result<()> {
        use datafusion::optimizer::OptimizerContext;
        use datafusion_expr::builder::table_scan;
        use datafusion_expr::{JoinType, LogicalPlanBuilder, lit};

        let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let left = table_scan(Some("c"), &left_schema, None)?.build()?;
        let right = table_scan(Some("p"), &right_schema, None)?.build()?;

        let joined = LogicalPlanBuilder::from(left)
            .join_using(right, JoinType::Inner, vec!["id".into()])?
            .filter(
                Expr::Column(Column::new(Some("p"), "id"))
                    .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false),
            )?
            .build()?;

        let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
        let cfg = OptimizerContext::new();
        let transformed = rule.rewrite(joined, &cfg)?;
        assert!(
            !transformed.transformed,
            "rule should not rewrite join-level filter inputs that span multiple table scans"
        );
        Ok(())
    }
}
