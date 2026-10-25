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

//! Keep checked integer `SUM` out of `SingleDistinctToGroupBy`.
//!
//! `DataFusion` rewrites `SUM(v), COUNT(DISTINCT k)` into an inner
//! `SUM(v) GROUP BY k` plus an outer `SUM`. That inner `evaluate()` narrows to
//! `Int64` before groups can cancel, so rows `(1, MAX), (1, MAX), (2, -MAX)`
//! error even though the true total is `MAX`. Skip the rewrite when a
//! non-distinct aggregate is a checked integer `SUM`; `COUNT(DISTINCT)` alone
//! and `SUM(DISTINCT)` still rewrite.

use std::sync::Arc;

use datafusion::common::{DFSchema, Result, tree_node::Transformed};
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::{Expr, LogicalPlan, expr::AggregateFunction};
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::is_checked_sum_return_type;

pub(crate) const SINGLE_DISTINCT_RULE_NAME: &str = "single_distinct_aggregation_to_group_by";

/// Drop-in replacement for `DataFusion`'s `SingleDistinctToGroupBy`.
#[derive(Debug, Default)]
pub struct CheckedSingleDistinctToGroupBy {
    inner: SingleDistinctToGroupBy,
}

impl CheckedSingleDistinctToGroupBy {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: SingleDistinctToGroupBy::new(),
        }
    }
}

fn has_checked_integer_sum(aggr_expr: &[Expr], schema: &DFSchema, group_len: usize) -> bool {
    aggr_expr.iter().enumerate().any(|(i, expr)| {
        let Expr::AggregateFunction(AggregateFunction {
            func,
            params: AggregateFunctionParams { distinct, .. },
        }) = expr
        else {
            return false;
        };
        if *distinct || func.name() != "sum" {
            return false;
        }
        let Some(field) = schema.fields().get(group_len.saturating_add(i)) else {
            return false;
        };
        is_checked_sum_return_type(field.data_type())
    })
}

impl OptimizerRule for CheckedSingleDistinctToGroupBy {
    fn name(&self) -> &str {
        SINGLE_DISTINCT_RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Aggregate(aggregate) = &plan
            && has_checked_integer_sum(
                &aggregate.aggr_expr,
                aggregate.schema.as_ref(),
                aggregate.group_expr.len(),
            )
        {
            return Ok(Transformed::no(plan));
        }
        self.inner.rewrite(plan, config)
    }
}

/// Replace `DataFusion`'s `single_distinct_aggregation_to_group_by` with the
/// checked-integer guard. Keeps the same rule name so skip lists stay valid.
pub fn install_checked_single_distinct(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.name() == SINGLE_DISTINCT_RULE_NAME)
    {
        *rule = Arc::new(CheckedSingleDistinctToGroupBy::new());
        return;
    }
    rules.push(Arc::new(CheckedSingleDistinctToGroupBy::new()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::optimizer::Optimizer;

    #[test]
    fn install_replaces_the_datafusion_rule_in_place() {
        let mut rules = Optimizer::new().rules;
        let before = rules
            .iter()
            .filter(|rule| rule.name() == SINGLE_DISTINCT_RULE_NAME)
            .count();
        assert_eq!(before, 1, "default optimizer must include the DF rule");
        install_checked_single_distinct(&mut rules);
        let after = rules
            .iter()
            .filter(|rule| rule.name() == SINGLE_DISTINCT_RULE_NAME)
            .count();
        assert_eq!(after, 1, "install must replace, not duplicate");
    }
}
