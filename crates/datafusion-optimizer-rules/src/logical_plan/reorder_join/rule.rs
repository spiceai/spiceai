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

use datafusion_common::{
    Result,
    tree_node::{Transformed, TreeNode},
};
use datafusion_expr::LogicalPlan;

use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::{
    cost::{DefaultCostEstimator, JoinCostEstimator},
    left_deep_join_plan::{ReorderOutcome, optimal_left_deep_join_plan},
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
        let start = std::time::Instant::now();

        // No joins anywhere in the plan: nothing to reorder. Returning the input unchanged.
        if !plan.exists(|p| Ok(matches!(p, LogicalPlan::Join(_))))? {
            tracing::debug!(
                elapsed = ?start.elapsed(),
                "reorder_join: skipped (no joins in plan)"
            );
            return Ok(Transformed::no(plan));
        }

        // Join reordering is strictly best-effort and infallible: spiceai does
        // not guarantee every `TableProvider` exposes statistics, so a query is
        // never failed for being un-costable — it simply isn't reordered.
        Ok(
            match optimal_left_deep_join_plan(plan, self.estimator.as_ref()) {
                ReorderOutcome::Completed(transformed) => {
                    if transformed.transformed {
                        tracing::debug!(
                            elapsed = ?start.elapsed(),
                            "reorder_join applied (join order changed)"
                        );
                    } else {
                        tracing::debug!(
                            elapsed = ?start.elapsed(),
                            "reorder_join: no change (already optimal or no reorderable joins)"
                        );
                    }
                    transformed
                }
                // Every failure here is a plan the optimizer couldn't process (a
                // rejected reconstruction or an internal error)
                ReorderOutcome::Failed { plan, error } => {
                    tracing::warn!(
                        error = %error,
                        elapsed = ?start.elapsed(),
                        "unable to apply join reorder; plan left unchanged"
                    );
                    Transformed::no(plan)
                }
            },
        )
    }
}
