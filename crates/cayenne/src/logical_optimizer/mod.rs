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

//! Logical optimizer rules for Cayenne. Four [`OptimizerRule`]s live here,
//! each in its own submodule:
//!
//! * [`CayennePropagateFilterAcrossEquiJoinKeys`] (`propagate_filter`) — the
//!   flagship rewrite documented in the rest of this header, which exposes
//!   selective dim-table key domains to fact-table scans;
//! * [`CayenneReassociateCrossJoin`] (`reassociate_cross_join`) — reassociates
//!   `FROM`-order cross joins so selective join branches evaluate first;
//! * [`CayennePushDownSemiJoin`] (`pushdown_semi_join`) — re-plants
//!   decorrelated semi-joins directly on the Cayenne scan sourcing their key;
//! * [`CayenneInListToRangeRewrite`] (`inlist_to_range`) — rewrites
//!   consecutive-integer `IN` lists to `BETWEEN` over Cayenne scans.
//!
//! Shared plan-shape helpers and gating constants live in `analysis`.
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

mod analysis;
mod inlist_to_range;
mod propagate_filter;
mod pushdown_semi_join;
mod reassociate_cross_join;

pub use inlist_to_range::CayenneInListToRangeRewrite;
pub use propagate_filter::CayennePropagateFilterAcrossEquiJoinKeys;
pub use pushdown_semi_join::CayennePushDownSemiJoin;
pub use reassociate_cross_join::CayenneReassociateCrossJoin;

#[expect(unused_imports)]
use analysis::{
    EquiKey, MAX_DIM_LIKE_TABLE_SCANS, MIN_FACT_ROWS_FOR_PROPAGATION,
    MIN_FACT_TO_DIM_KEY_DOMAIN_RATIO, analyze_logical_side, build_key_projection_subquery,
    collect_columns_from_expr, collect_literal_comparison_columns,
    collect_propagated_filter_targets, collect_selective_filter_columns, column_expr,
    columns_match, comparison_operator_is_selective, contains_cayenne_table_scan,
    contains_cayenne_table_scan_with_column, count_dim_like_table_scans, distinct_input,
    expr_is_literal_like, is_dim_like_subtree, join_key_types_match, key_domain_upper_bound_rows,
    key_domain_upper_bound_rows_for_expr, key_for_input_schema, key_preserved_through_summaries,
    matching_equijoin_keys, right_side_carries_propagation_marker, skip_propagation_by_cardinality,
    subtree_upper_bound_rows, table_scan_has_column, table_scan_upper_bound_rows,
    wrap_with_in_subquery_filter_expr,
};
#[cfg(test)]
#[allow(unused_imports)]
use analysis::{expr_has_propagated_filter, subtree_has_propagated_filter};
use propagate_filter::SideAnalysis;
use pushdown_semi_join::is_single_cayenne_table_scan_input;
#[expect(unused_imports)]
use reassociate_cross_join::{
    JoinInputRefs, expr_input_refs, is_pure_inner_cross_join, is_reassociable_inner_join,
    reassociate_cross_join,
};

#[cfg(test)]
mod tests;
