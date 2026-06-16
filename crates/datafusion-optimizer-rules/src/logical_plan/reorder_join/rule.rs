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
//! `SessionStateBuilder` via `with_optimizer_rule`) so the IK84 reorder
//! runs *after* `ExtractEquijoinPredicate` has lifted equi-conditions
//! into the joins' `on` clauses. Running it before that point leaves the
//! reorder with empty-`on` cross-products and a disconnected join graph.

use std::sync::Arc;

use datafusion_common::{Result, tree_node::Transformed};
use datafusion_expr::LogicalPlan;

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
            Ok(after) if after == before => Ok(Transformed::no(after)),
            // Validate the reordered plan before accepting it: every join `on`
            // key must resolve in its own input's schema. A reconstruction bug
            // (e.g. q21's anti-join + mod-key mix, where an inner edge is
            // dropped) can otherwise emit a structurally-invalid plan whose
            // `rewrite()` succeeds but fails a *downstream* rule
            // (`simplify_expressions: No field named …`) — failing the query.
            // If the output doesn't validate, skip the reorder rather than
            // break the query.
            Ok(after) if join_keys_resolve(&after) => Ok(Transformed::yes(after)),
            Ok(_) => {
                tracing::debug!(
                    target: "reorder_join",
                    "skipping join reorder: reordered plan has unresolved join keys (plan left unchanged)"
                );
                Ok(Transformed::no(before))
            }
            Err(e) => {
                tracing::debug!(
                    target: "reorder_join",
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
