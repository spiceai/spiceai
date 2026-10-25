/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Keep checked integer `SUM` / 64-bit `AVG` out of [`EagerAggregation`].
//!
//! The `spiceai/datafusion` rule matches aggregates named `"sum"` and
//! builds `Partial` → `FinalPartitioned` below a join, so
//! [`Accumulator::evaluate`] narrows each join-key group to `Int64` before
//! the outer `SUM`. Join-key groups `[MAX, MAX]` and `[-MAX]` that land in
//! one output group then error even though the true total is `MAX`.
//!
//! `CheckedSum::name()` stays `"sum"` for federation. This wrapper keeps
//! the `DataFusion` rule name so skip lists stay valid, and declines the
//! rewrite when [`complete_preaggregate_would_narrow_early`] is true.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::eager_aggregation::EagerAggregation;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateInputMode};

use super::decomposability::{complete_preaggregate_would_narrow_early, physical_source_arg_types};

pub(crate) const EAGER_AGGREGATION_RULE_NAME: &str = "eager_aggregation";

/// Drop-in replacement for `DataFusion`'s `EagerAggregation`.
#[derive(Debug, Default)]
pub struct CheckedEagerAggregation {
    inner: EagerAggregation,
}

impl CheckedEagerAggregation {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: EagerAggregation::new(),
        }
    }
}

fn plan_has_checked_integer_aggregate(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        // Merge stages keep the original args (`CAST(n AS Int64)`) but sit
        // on the partial-state schema, so resolving those args there can
        // look like `CAST(utf8 AS Int64)` and false-skip `SUM(INT)`.
        && aggregate.mode().input_mode() == AggregateInputMode::Raw
    {
        let input_schema = aggregate.input().schema();
        if aggregate.aggr_expr().iter().any(|expr| {
            complete_preaggregate_would_narrow_early(
                expr.fun().name(),
                &physical_source_arg_types(&expr.expressions(), input_schema.as_ref()),
            )
        }) {
            return true;
        }
    }
    plan.children()
        .into_iter()
        .any(plan_has_checked_integer_aggregate)
}

impl PhysicalOptimizerRule for CheckedEagerAggregation {
    fn name(&self) -> &str {
        EAGER_AGGREGATION_RULE_NAME
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plan_has_checked_integer_aggregate(&plan) {
            return Ok(plan);
        }
        self.inner.optimize(plan, config)
    }
}

/// Replace `DataFusion`'s `eager_aggregation` with the checked-integer
/// guard. Keeps the same rule name so skip lists stay valid.
pub fn install_checked_eager_aggregation(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) {
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.name() == EAGER_AGGREGATION_RULE_NAME)
    {
        *rule = Arc::new(CheckedEagerAggregation::new());
        return;
    }
    rules.push(Arc::new(CheckedEagerAggregation::new()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    #[test]
    fn install_replaces_the_datafusion_rule_in_place() {
        let mut rules = PhysicalOptimizer::new().rules;
        let before = rules
            .iter()
            .filter(|rule| rule.name() == EAGER_AGGREGATION_RULE_NAME)
            .count();
        assert_eq!(
            before, 1,
            "default physical optimizer must include the DF rule"
        );
        install_checked_eager_aggregation(&mut rules);
        let after = rules
            .iter()
            .filter(|rule| rule.name() == EAGER_AGGREGATION_RULE_NAME)
            .count();
        assert_eq!(after, 1, "install must replace, not duplicate");
    }
}
