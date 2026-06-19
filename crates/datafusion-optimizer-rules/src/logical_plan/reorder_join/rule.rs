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

//! `OptimizerRule` wrapper for join reordering.
//!
//! # Algorithm
//!
//! Cost-based enumeration of **left-deep** join orders via the Ibaraki–Kameda
//! (IK84) algorithm. The inner-join subtree is flattened into a join graph
//! (relations as nodes, equi-conditions as edges; see `join_graph`), then for
//! each relation taken as the chain root IK84 builds a precedence tree, ranks
//! sub-chains by the cost/selectivity ratio, and merges them into one linear
//! chain (see `left_deep_join_plan`). The lowest-cost root wins. Cardinalities
//! and selectivities come from `TableProvider` statistics through `cost`. The
//! algorithm is polynomial (it does not enumerate all orders) and only finds
//! left-deep plans — it never produces bushy trees.
//!
//! # Pipeline position
//!
//! ## Must run AFTER
//!
//! - **`ExtractEquijoinPredicate`** — it lifts equi-conditions into the joins'
//!   `on` clauses, which is what we turn into join-graph edges. Running before
//!   this point leaves the reorder with empty-`on` cross-products and a
//!   disconnected join graph.
//! - **Filter / predicate pushdown** — filters need to be pushed to the scans so
//!   base-table cardinalities are correct and the small side is identifiable.
//! - **Subquery decorrelation, outer→inner simplification, cross-join
//!   elimination** — each enlarges the connected inner-join island that is
//!   reorderable.
//!
//! ## Must run BEFORE
//!
//! - **Projection pushdown / `optimize_projections`** — *critical.* These rules
//!   (`extract_leaf_expressions` / `push_down_leaf_projections` /
//!   `optimize_projections`) insert `Projection` nodes between joins.
//!   `flatten_joins_recursive` absorbs any `Projection`/non-join wrapper between
//!   joins as an **opaque leaf**, so an intervening projection fragments the join
//!   tree
//!
//! # Notes:
//!
//! Build-side selection is a separate, later DF decision:
//!
//! Join *order* (this rule) and *which side to build* the hash table on are
//! distinct decisions made at different stages — the latter is the physical
//! `JoinSelection` pass.
//!
//! Re-firing / multi-pass caveat:
//!
//! This is registered as an ordinary `OptimizerRule`, so `DataFusion` re-runs it
//! once per optimizer pass (up to `max_passes`, default 3) rather than exactly
//! once. Production optimizers (`PostgreSQL`, `DuckDB`, Calcite/Trino) decide
//! join order in a single dedicated phase.

use std::sync::Arc;

use datafusion_common::{Result, tree_node::Transformed};
use datafusion_expr::{JoinType, LogicalPlan};

use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::{
    cost::{DefaultCostEstimator, JoinCostEstimator},
    left_deep_join_plan::optimal_left_deep_join_plan,
};

/// Optimizer-rule wrapper around [`optimal_left_deep_join_plan`].
#[derive(Debug)]
pub struct ReorderJoinRule {
    estimator: Arc<dyn JoinCostEstimator + Send + Sync>,
}

impl ReorderJoinRule {
    pub fn new(estimator: Arc<dyn JoinCostEstimator + Send + Sync>) -> Self {
        Self { estimator }
    }
}

impl Default for ReorderJoinRule {
    fn default() -> Self {
        Self::new(Arc::new(DefaultCostEstimator))
    }
}

impl OptimizerRule for ReorderJoinRule {
    fn name(&self) -> &'static str {
        "reorder_join"
    }

    // `optimal_left_deep_join_plan` does its own top-level traversal and
    // short-circuits when the plan has no joins, so we don't want the
    // framework to walk the tree on our behalf.
    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let before = plan.clone();
        tracing::debug!(
            joins = count_joins(&before),
            cross_joins = count_inner_cross_joins(&before),
            "reorder_join rule invoked"
        );
        // Join reordering is strictly best-effort: spiceai does NOT guarantee
        // every `TableProvider` exposes statistics (the default `statistics()`
        // returns `None`; in such case we trace debug message and pass the original
        // plan through unchanged (no reorder).
        match optimal_left_deep_join_plan(plan, self.estimator.as_ref()) {
            // IK84 is deterministic on a stable graph, so a second pass over an
            // already-optimal plan reproduces the same chain; converge by
            // reporting no change.
            Ok(after) if after == before => {
                tracing::debug!(
                    "reorder_join: no change (already optimal or no reorderable joins)"
                );
                Ok(Transformed::no(after))
            }
            // Accept the reorder only if it passes two checks:
            //   1. Every join `on` key resolves in its own input's schema. A
            //      reconstruction bug that drops an inner edge can emit a
            //      structurally-invalid plan whose `rewrite()` succeeds but then
            //      fails a *downstream* rule (e.g. "No field named …").
            //   2. It introduces no new cross product (Inner `Join` with empty
            //      `on`). The enumerator only sees equi-edges, so a relation
            //      linked to the rest solely by a non-equi predicate gets
            //      stranded and reconnected with a bare cross join, which a
            //      downstream pass turns into a `NestedLoopJoinExec` that can
            //      blow up over large inputs. The native plan keeps such
            //      relations adjacent and applies the predicate as a residual
            //      `Filter` on an equi-join, so falling back to it is safer.
            // Either failure → skip the reorder rather than break/hang the query.
            Ok(after)
                if join_keys_resolve(&after)
                    && count_inner_cross_joins(&after) <= count_inner_cross_joins(&before) =>
            {
                tracing::debug!(
                    cross_joins = count_inner_cross_joins(&after),
                    "reorder_join applied (join order changed)"
                );
                Ok(Transformed::yes(after))
            }
            Ok(after) => {
                // The rule produced a reordered plan but it failed one of the
                // checks above, so fall back to the native plan. This is a
                // genuine miss — the graph was reorderable but the
                // reconstruction came out invalid — so warn rather than debug.
                let reason = if join_keys_resolve(&after) {
                    "reorder introduced a cross join (a relation linked only by a non-equi predicate was stranded)"
                } else {
                    "reordered plan has unresolved join keys (reconstruction dropped an equi-edge)"
                };
                tracing::warn!(
                    reason,
                    cross_joins_before = count_inner_cross_joins(&before),
                    cross_joins_after = count_inner_cross_joins(&after),
                    "unable to apply join reorder; falling back to native plan (plan left unchanged)"
                );
                Ok(Transformed::no(before))
            }
            Err(e) => {
                tracing::debug!("skipping join reorder (plan left unchanged): {e}");
                Ok(Transformed::no(before))
            }
        }
    }
}

/// Returns `false` if any `Join` in `plan` has an `on` key that does not resolve
/// in its corresponding input schema — i.e. the reconstruction produced a
/// structurally-invalid plan. Used as a safety gate so a reorder bug degrades to
/// "no reorder" instead of a failed query.
fn join_keys_resolve(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Join(join) = plan {
        let left_schema = join.left.schema();
        let right_schema = join.right.schema();
        for (left_key, right_key) in &join.on {
            if left_key
                .column_refs()
                .iter()
                .any(|col| !left_schema.has_column(col))
                || right_key
                    .column_refs()
                    .iter()
                    .any(|col| !right_schema.has_column(col))
            {
                return false;
            }
        }
    }
    plan.inputs().iter().all(|input| join_keys_resolve(input))
}

/// Total number of `Join` nodes anywhere in `plan` (any join type). Used only
/// for the entry debug trace, to distinguish a no-op invocation on a join-free
/// plan from a real reorder candidate.
fn count_joins(plan: &LogicalPlan) -> usize {
    let here = usize::from(matches!(plan, LogicalPlan::Join(_)));
    here + plan
        .inputs()
        .iter()
        .map(|input| count_joins(input))
        .sum::<usize>()
}

/// Counts *true* cross products — Inner `Join` nodes with an empty `on` clause
/// **and** no `filter` — in `plan`. The reorder is rejected when it produces
/// *more* of these than the input had: a relation the enumerator could only
/// link by a non-equi predicate gets reconnected with a bare cross join, which
/// a downstream pass turns into a `NestedLoopJoinExec` that can blow up to a
/// near-cartesian over large inputs. See the call site.
fn count_inner_cross_joins(plan: &LogicalPlan) -> usize {
    let here = match plan {
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner && join.on.is_empty() && join.filter.is_none() =>
        {
            1
        }
        _ => 0,
    };
    here + plan
        .inputs()
        .iter()
        .map(|input| count_inner_cross_joins(input))
        .sum::<usize>()
}
