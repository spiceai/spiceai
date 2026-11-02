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
//!
//! When the rule does fire, it merges `COUNT` partials and both halves of a
//! `Float64` `AVG` with the built-in `sum_udaf()` (`Int64` partial state for
//! counts). A cluster executor decodes every `sum` by name to [`CheckedSum`]
//! (`Decimal128(38, 0)` partial state), so a merge whose `Partial` and `Final`
//! land in different stages would disagree on the shuffled schema.
//! [`CheckedEagerAggregation::for_distributed`] rewrites those merges to
//! [`CheckedSum`] on the scheduler so both sides plan the same state.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::eager_aggregation::EagerAggregation;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateInputMode};

use super::decomposability::{complete_preaggregate_would_narrow_early, physical_source_arg_types};
use super::{checked_sum_udaf, is_checked_sum_return_type};

pub(crate) const EAGER_AGGREGATION_RULE_NAME: &str = "eager_aggregation";

/// Drop-in replacement for `DataFusion`'s `EagerAggregation`.
#[derive(Debug, Default)]
pub struct CheckedEagerAggregation {
    inner: EagerAggregation,
    /// Rewrite the built-in integer `sum` merges the rule adds to
    /// [`CheckedSum`](super::CheckedSum). Set for a cluster scheduler.
    checked_merges: bool,
}

impl CheckedEagerAggregation {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: EagerAggregation::new(),
            checked_merges: false,
        }
    }

    /// For a scheduler whose plans are decoded by name on cluster executors.
    ///
    /// Single-node plans keep the built-in merge: they are never decoded, and
    /// the built-in keeps its `GroupsAccumulator`.
    #[must_use]
    pub fn for_distributed() -> Self {
        Self {
            inner: EagerAggregation::new(),
            checked_merges: true,
        }
    }
}

/// A built-in `sum` (not [`CheckedSum`](super::CheckedSum)) whose result is
/// `Int64` / `UInt64`: the state an executor would decode differently.
fn is_builtin_checked_type_sum(expr: &AggregateFunctionExpr) -> bool {
    expr.fun().inner().is::<Sum>() && is_checked_sum_return_type(expr.field().data_type())
}

fn with_checked_sum(
    expr: &Arc<AggregateFunctionExpr>,
    input_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<Arc<AggregateFunctionExpr>> {
    if !is_builtin_checked_type_sum(expr) {
        return Ok(Arc::clone(expr));
    }
    let mut builder = AggregateExprBuilder::new(checked_sum_udaf(), expr.expressions())
        .schema(Arc::clone(input_schema))
        .alias(expr.name())
        .return_field(expr.field())
        .order_by(expr.order_bys().to_vec())
        .with_ignore_nulls(expr.ignore_nulls())
        .with_reversed(expr.is_reversed());
    if expr.is_distinct() {
        builder = builder.distinct();
    }
    if let Some(display) = expr.human_display() {
        builder = builder.human_display(display);
        if let Some(alias) = expr.human_display_alias() {
            builder = builder.human_display_alias(alias);
        }
    }
    builder.build().map(Arc::new)
}

/// Replace every built-in integer `sum` in `plan` with [`CheckedSum`](super::CheckedSum).
///
/// The `Partial` and `Final` of one merge each hold the expression, so both
/// are rebuilt; the `Partial`'s output schema becomes the checked state.
fn checked_integer_sum_merges(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(aggregate) = node.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(node));
        };
        if !aggregate
            .aggr_expr()
            .iter()
            .any(|expr| is_builtin_checked_type_sum(expr))
        {
            return Ok(Transformed::no(node));
        }
        let input_schema = aggregate.input_schema();
        let aggr_expr = aggregate
            .aggr_expr()
            .iter()
            .map(|expr| with_checked_sum(expr, &input_schema))
            .collect::<Result<Vec<_>>>()?;
        let rebuilt = AggregateExec::try_new(
            *aggregate.mode(),
            aggregate.group_expr().clone(),
            aggr_expr,
            aggregate.filter_expr().to_vec(),
            Arc::clone(aggregate.input()),
            input_schema,
        )?
        .with_limit_options(aggregate.limit_options());
        Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>))
    })
    .data()
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
        let optimized = if plan_has_checked_integer_aggregate(&plan) {
            plan
        } else {
            self.inner.optimize(plan, config)?
        };
        if !self.checked_merges {
            return Ok(optimized);
        }
        checked_integer_sum_merges(optimized)
    }
}

/// Replace `DataFusion`'s `eager_aggregation` with the checked-integer
/// guard. Keeps the same rule name so skip lists stay valid.
pub fn install_checked_eager_aggregation(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) {
    install(rules, CheckedEagerAggregation::new());
}

/// [`install_checked_eager_aggregation`] for a cluster scheduler: also
/// rewrites the rule's built-in integer `sum` merges to checked `SUM`, the
/// function a cluster executor decodes `sum` to.
pub fn install_checked_eager_aggregation_for_distributed(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) {
    install(rules, CheckedEagerAggregation::for_distributed());
}

fn install(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    replacement: CheckedEagerAggregation,
) {
    if let Some(rule) = rules
        .iter_mut()
        .find(|rule| rule.name() == EAGER_AGGREGATION_RULE_NAME)
    {
        *rule = Arc::new(replacement);
        return;
    }
    rules.push(Arc::new(replacement));
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

    fn sum_merge() -> Arc<dyn ExecutionPlan> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::functions_aggregate::sum::sum_udaf;
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
        ]));
        let sum = |name: &str| {
            AggregateExprBuilder::new(sum_udaf(), vec![col(name, &schema).expect("column")])
                .schema(Arc::clone(&schema))
                .alias(format!("sum({name})"))
                .build()
                .map(Arc::new)
                .expect("sum")
        };
        let aggr_expr = vec![sum("count"), sum("x")];
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let partial: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                aggr_expr.clone(),
                vec![None, None],
                input,
                Arc::clone(&schema),
            )
            .expect("partial"),
        );
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                aggr_expr,
                vec![None, None],
                partial,
                schema,
            )
            .expect("final"),
        )
    }

    fn aggregates(plan: &Arc<dyn ExecutionPlan>) -> Vec<&AggregateExec> {
        let mut out = Vec::new();
        let mut node = Some(plan);
        while let Some(current) = node {
            if let Some(aggregate) = current.downcast_ref::<AggregateExec>() {
                out.push(aggregate);
            }
            node = current.children().into_iter().next();
        }
        out
    }

    /// A cluster executor decodes `sum` to checked `SUM`, so the scheduler
    /// must plan the integer merges with it: both halves, and the `Partial`'s
    /// shuffled state. A `Float64` sum already has the same state either way.
    #[test]
    fn distributed_rule_plans_integer_sum_merges_with_checked_sum() {
        use datafusion::arrow::datatypes::DataType;

        let optimized = CheckedEagerAggregation::for_distributed()
            .optimize(sum_merge(), &ConfigOptions::new())
            .expect("optimize");
        let [final_agg, partial] = aggregates(&optimized)[..] else {
            panic!("expected Final over Partial");
        };
        for aggregate in [final_agg, partial] {
            let [int_sum, float_sum] = aggregate.aggr_expr() else {
                panic!("expected two sums");
            };
            assert!(int_sum.fun().inner().is::<super::super::CheckedSum>());
            assert_eq!(int_sum.field().data_type(), &DataType::Int64);
            assert!(float_sum.fun().inner().is::<Sum>());
        }
        assert_eq!(
            partial.schema().field(0).data_type(),
            &DataType::Decimal128(38, 0),
            "the Partial must shuffle checked SUM's state"
        );
        assert_eq!(final_agg.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(final_agg.schema().field(1).data_type(), &DataType::Float64);
    }

    /// Single-node plans are never decoded and keep the built-in merge.
    #[test]
    fn local_rule_keeps_the_built_in_sum() {
        let optimized = CheckedEagerAggregation::new()
            .optimize(sum_merge(), &ConfigOptions::new())
            .expect("optimize");
        for aggregate in aggregates(&optimized) {
            assert!(
                aggregate
                    .aggr_expr()
                    .iter()
                    .all(|expr| expr.fun().inner().is::<Sum>())
            );
        }
    }
}
