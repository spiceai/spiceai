// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `OptimizerRule` wrapper for join reordering.
//!
//! Append this to an `Optimizer`'s rule list (or to a
//! `SessionStateBuilder` via `with_optimizer_rule`) so the reorder
//! runs *after* `ExtractEquijoinPredicate` has lifted equi-conditions
//! into the joins' `on` clauses. Running it before that point leaves the
//! reorder with empty-`on` cross-products and a disconnected join graph.

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
    fn name(&self) -> &str {
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
        // returns `None`; only Cayenne/accelerated/dataset providers implement
        // it), and the cost model returns `Err` when rows/NDV are absent. For an
        // always-on rule over *all* queries we must never fail a query because
        // we couldn't cost it — so on any error we log and pass the original
        // plan through unchanged (no reorder). Tests assert the positive case so
        // genuine regressions are still caught in CI.
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
            // Validate the reordered plan before accepting it: every join `on`
            // key must resolve in its own input's schema. A reconstruction bug
            // (e.g. q21's anti-join + mod-key mix, where an inner edge is
            // dropped) can otherwise emit a structurally-invalid plan whose
            // `rewrite()` succeeds but fails a *downstream* rule
            // (`simplify_expressions: No field named …`) — failing the query.
            // If the output doesn't validate, skip the reorder rather than
            // break the query.
            //
            // Also reject a reorder that *introduces* a cross/theta join (an
            // Inner `Join` with empty `on`). The enumerator only sees equi-edges,
            // so a relation linked to the rest solely by a non-equi predicate
            // (e.g. chbench q7's `(n1='JAPAN' AND n2='CHINA') OR (…)`) gets
            // stranded: the rebuild connects it with an empty-`on` Inner join and
            // the predicate becomes a `NestedLoopJoinExec` filter. If the reorder
            // happens to place that nested loop over large inputs, the query can
            // blow up to a near-cartesian and never finish. DataFusion's native
            // (un-reordered) plan keeps such relations adjacent and applies the
            // predicate as a residual `Filter` on an equi-join, so falling back
            // to it is strictly safer here.
            Ok(after)
                if join_keys_resolve(&after)
                    && count_inner_cross_joins(&after)
                        <= count_inner_cross_joins(&before) =>
            {
                tracing::debug!(
                    cross_joins = count_inner_cross_joins(&after),
                    "reorder_join applied (join order changed)"
                );
                Ok(Transformed::yes(after))
            }
            Ok(after) => {
                // We produced a reordered plan but it failed validation, so we
                // fall back to the native plan. This is a genuine miss — the rule
                // *could* reorder this join graph but the reconstruction came out
                // invalid (e.g. q7's non-equi `n1`/`n2` OR predicate forcing a
                // cross/theta join, or a dropped equi-edge leaving unresolved
                // keys) — so warn rather than debug to surface it.
                let reason = if !join_keys_resolve(&after) {
                    "reordered plan has unresolved join keys (reconstruction dropped an equi-edge)"
                } else {
                    "reorder introduced a cross/theta join (a relation linked only by a non-equi predicate was stranded)"
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
                tracing::debug!(
                    "skipping join reorder (plan left unchanged): {e}"
                );
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

/// Counts *true* cross products — Inner `Join` nodes with an empty `on` clause
/// **and** no `filter` — in `plan`. The reorder is rejected when it produces
/// *more* of these than the input had, i.e. when reordering stranded a relation
/// that was only linked by a non-equi predicate and had to be reconnected with a
/// bare cross join (which a downstream pass turns into a `NestedLoopJoinExec`).
/// See the call site for why that can make a query never finish.
///
/// A theta join (empty `on` but `filter = Some(..)`, produced by
/// `derive_theta_edges`) is deliberately *not* counted: the soft-edge feature
/// places it between the two small relations the predicate links, so it is a
/// cheap filtered join, not a cartesian blow-up.
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

fn count_inner_cross_joins(plan: &LogicalPlan) -> usize {
    let here = match plan {
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner
                && join.on.is_empty()
                && join.filter.is_none() =>
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
