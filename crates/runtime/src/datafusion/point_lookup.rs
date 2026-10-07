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

//! Planning for primary-key point lookups.
//!
//! `SELECT <columns> FROM t WHERE pk = <value>`, with an equality on every primary-key column,
//! plans to a single filtered scan. The session runs about a hundred analyzer, logical and
//! physical rule invocations on every query, and on this shape only a handful can change the plan
//! (`type_coercion`, `simplify_expressions`, `push_down_filter`, `optimize_projections`,
//! `FilterPushdown`, `BytesProcessedPhysicalOptimizer`). The rest rewrite joins, aggregates,
//! subqueries, sorts, windows and unions, and walk the plan without finding one.
//!
//! [`is_point_lookup`] recognises the shape on the unoptimized plan. The rules named in
//! [`SKIPPABLE_RULES`] are wrapped when the session is built; while [`create_physical_plan`] plans a
//! recognised lookup they hand their input back untouched. Every other rule runs as it always
//! does, including any rule a `DataFusion` upgrade adds: this is a list of rules to skip, not a
//! list of rules to keep, so a new rule is never skipped by omission.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Constraint, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableScan};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
use datafusion::physical_plan::ExecutionPlan;

/// Rules that cannot change the plan of a query [`is_point_lookup`] accepts: a projection of
/// plain columns over a filter of column-against-constant comparisons over one table scan, with
/// no join, aggregate, subquery, window, sort, union, distinct or limit anywhere.
///
/// `EnsureRequirements` is here because the shape has no distribution or ordering requirement;
/// that holds only while `LIMIT` stays excluded (a global limit needs a single input partition,
/// which `EnsureRequirements` would provide). Rules that act on a scan
/// of a particular connector (`HttpParamsPushdown`, the `DuckDB` rules) and
/// `propagate_empty_relation` (which shapes the plan of a contradictory predicate) are not here:
/// they cost well under a microsecond and are not worth reasoning about per connector.
pub(crate) const SKIPPABLE_RULES: &[&str] = &[
    // analyzer
    "resolve_grouping_function",
    "spice_ddl_rewrite",
    // logical
    "rewrite_set_comparison",
    "optimize_unions",
    "unions_to_filter",
    "regexp_match_null_check_rewrite",
    "replace_distinct_aggregate",
    "eliminate_join",
    "decorrelate_predicate_subquery",
    "cayenne_push_down_semi_join",
    "scalar_subquery_to_join",
    "decorrelate_lateral_join",
    "extract_equijoin_predicate",
    "eliminate_duplicated_expr",
    "eliminate_cross_join",
    "cayenne_reassociate_cross_join",
    "filter_null_join_keys",
    "eliminate_outer_join",
    "reorder_join",
    "single_distinct_aggregation_to_group_by",
    "eliminate_group_by_constant",
    "common_sub_expression_eliminate",
    "DuckDBAggregatePushdownOptimizerRule",
    "cache_invalidation_optimizer_rule",
    "cayenne_cte_materialization",
    // physical
    "aggregate_statistics",
    "join_selection",
    "eager_aggregation",
    "LimitedDistinctAggregation",
    "EnsureRequirements",
    "CombinePartialFinalAggregate",
    "OptimizeAggregateOrder",
    "WindowTopN",
    "LimitAggregation",
    "LimitPushPastWindows",
    "HashJoinBuffering",
    "TopKRepartition",
    "PushdownSort",
    "EmptyHashJoinExecPhysicalOptimization",
    "CayenneDynamicFilterSharing",
    "CayenneMaintainedAggregateRewriter",
    "CayenneStatsAggregateRewriter",
    "CayenneAntiJoinSortMergeRewriter",
];

tokio::task_local! {
    static PLANNING_POINT_LOOKUP: ();
}

fn planning_point_lookup() -> bool {
    PLANNING_POINT_LOOKUP.try_with(|()| ()).is_ok()
}

fn is_skippable(name: &str) -> bool {
    SKIPPABLE_RULES.contains(&name)
}

/// Plans `plan` physically, as `SessionState::create_physical_plan` does. A point lookup
/// ([`is_point_lookup`]) is planned with the rules in [`SKIPPABLE_RULES`] switched off and with
/// only the logical optimizer passes that can still change it.
///
/// `DataFusion` repeats its logical rules until a pass leaves the plan unchanged, so a plan the
/// first pass finishes still pays for a second pass that only confirms it. One pass finishes a
/// point lookup — it pushes the filter into the scan and prunes the projection — unless it leaves
/// a constant predicate behind: a contradiction such as `id = 5 AND id = 6` folds to `false` only
/// after `eliminate_filter` has run, and it takes another pass to turn that filter into an empty
/// relation. The remaining passes run then, so the plan is always the one full optimization
/// produces. `session` keeps its own pass limit.
pub(crate) async fn create_physical_plan(
    session: &mut SessionState,
    plan: &LogicalPlan,
    point_lookup: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !point_lookup {
        return session.create_physical_plan(plan).await;
    }
    PLANNING_POINT_LOOKUP
        .scope((), async {
            let max_passes = session.config().options().optimizer.max_passes;
            session.config_mut().options_mut().optimizer.max_passes = 1;
            let optimized = session.optimize(plan);
            session.config_mut().options_mut().optimizer.max_passes = max_passes;
            let mut optimized = optimized?;
            if has_constant_predicate(&optimized) {
                optimized = session.optimize(&optimized)?;
            }
            let planner = Arc::clone(session.query_planner());
            planner.create_physical_plan(&optimized, session).await
        })
        .await
}

/// Whether a filter in `plan` still holds a boolean or null constant. The next pass's expression
/// simplification folds it — `id = 5 AND false` to `false` — and `eliminate_filter` then removes
/// the filter or turns it into an empty relation. A filter on a boolean column compared with a
/// literal also counts; it pays for a pass it did not need rather than risk skipping one.
fn has_constant_predicate(plan: &LogicalPlan) -> bool {
    fn holds_constant(predicate: &Expr) -> bool {
        predicate
            .exists(|expr| {
                Ok(matches!(expr, Expr::Literal(value, _)
                    if value.is_null() || matches!(value, ScalarValue::Boolean(_))))
            })
            .unwrap_or(true)
    }
    plan.exists(|node| {
        Ok(match node {
            LogicalPlan::Filter(filter) => holds_constant(&filter.predicate),
            LogicalPlan::TableScan(scan) => scan.filters.iter().any(holds_constant),
            _ => false,
        })
    })
    .unwrap_or(true)
}

/// Whether `plan` (unoptimized) is a primary-key point lookup: plain columns projected from a
/// single table scan through filters made only of column-against-constant comparisons, one of
/// which is an equality on each primary-key column of the scanned table.
pub(crate) fn is_point_lookup(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    let mut predicates: Vec<&Expr> = Vec::new();
    loop {
        match node {
            LogicalPlan::Projection(projection) => {
                if !projection.expr.iter().all(is_plain_column) {
                    return false;
                }
                node = projection.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(alias) => node = alias.input.as_ref(),
            LogicalPlan::Filter(filter) => {
                predicates.push(&filter.predicate);
                node = filter.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => return scan_is_keyed_by(scan, &predicates),
            _ => return false,
        }
    }
}

fn scan_is_keyed_by(scan: &TableScan, predicates: &[&Expr]) -> bool {
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return false;
    }
    let Some(primary_key) = scan.source.constraints().and_then(|constraints| {
        constraints.iter().find_map(|constraint| match constraint {
            Constraint::PrimaryKey(indices) => Some(indices),
            Constraint::Unique(_) => None,
        })
    }) else {
        return false;
    };
    if primary_key.is_empty() {
        return false;
    }
    let conjuncts: Vec<&Expr> = predicates
        .iter()
        .flat_map(|predicate| split_conjunction(predicate))
        .collect();
    if !conjuncts
        .iter()
        .all(|conjunct| is_simple_comparison(conjunct))
    {
        return false;
    }
    let schema = scan.source.schema();
    primary_key.iter().all(|&index| {
        index < schema.fields().len() && {
            let name = schema.field(index).name();
            conjuncts
                .iter()
                .any(|conjunct| is_equality_on(conjunct, name))
        }
    })
}

fn is_plain_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Column(_)),
        _ => false,
    }
}

fn is_constant(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(..) | Expr::Placeholder(_))
}

fn is_simple_comparison(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && ((matches!(left.as_ref(), Expr::Column(_)) && is_constant(right))
                || (is_constant(left) && matches!(right.as_ref(), Expr::Column(_))))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => matches!(inner.as_ref(), Expr::Column(_)),
        _ => false,
    }
}

fn is_equality_on(expr: &Expr, column: &str) -> bool {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    else {
        return false;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), other) | (other, Expr::Column(c)) => {
            c.name == column && is_constant(other)
        }
        _ => false,
    }
}

/// Wraps every [`SKIPPABLE_RULES`] rule a session builder holds. Call before `build()`.
pub(crate) fn wrap_skippable_rules(state: &mut SessionStateBuilder) {
    if let Some(analyzer) = state.analyzer().as_mut() {
        analyzer.rules = std::mem::take(&mut analyzer.rules)
            .into_iter()
            .map(skippable_analyzer_rule)
            .collect();
    }
    if let Some(rules) = state.analyzer_rules().as_mut() {
        *rules = std::mem::take(rules)
            .into_iter()
            .map(skippable_analyzer_rule)
            .collect();
    }
    // `build()` falls back to the default rule lists when none were set; materialize them here
    // (identical rules) so the defaults are wrapped too.
    {
        let optimizer = state.optimizer().get_or_insert_with(Optimizer::new);
        optimizer.rules = std::mem::take(&mut optimizer.rules)
            .into_iter()
            .map(skippable_optimizer_rule)
            .collect();
    }
    if let Some(rules) = state.optimizer_rules().as_mut() {
        *rules = std::mem::take(rules)
            .into_iter()
            .map(skippable_optimizer_rule)
            .collect();
    }
    {
        let physical = state
            .physical_optimizers()
            .get_or_insert_with(PhysicalOptimizer::new);
        physical.rules = std::mem::take(&mut physical.rules)
            .into_iter()
            .map(skippable_physical_rule)
            .collect();
    }
    if let Some(rules) = state.physical_optimizer_rules().as_mut() {
        *rules = std::mem::take(rules)
            .into_iter()
            .map(skippable_physical_rule)
            .collect();
    }
}

/// `rule` itself, or `rule` wrapped so it is skipped while planning a point lookup.
pub(crate) fn skippable_analyzer_rule(
    rule: Arc<dyn AnalyzerRule + Send + Sync>,
) -> Arc<dyn AnalyzerRule + Send + Sync> {
    if is_skippable(rule.name()) {
        Arc::new(SkippableAnalyzerRule(rule))
    } else {
        rule
    }
}

/// `rule` itself, or `rule` wrapped so it is skipped while planning a point lookup.
pub(crate) fn skippable_optimizer_rule(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
) -> Arc<dyn OptimizerRule + Send + Sync> {
    if is_skippable(rule.name()) {
        Arc::new(SkippableOptimizerRule(rule))
    } else {
        rule
    }
}

/// `rule` itself, or `rule` wrapped so it is skipped while planning a point lookup.
pub(crate) fn skippable_physical_rule(
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    if is_skippable(rule.name()) {
        Arc::new(SkippablePhysicalRule(rule))
    } else {
        rule
    }
}

struct SkippableAnalyzerRule(Arc<dyn AnalyzerRule + Send + Sync>);

impl Debug for SkippableAnalyzerRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AnalyzerRule for SkippableAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if planning_point_lookup() {
            return Ok(plan);
        }
        self.0.analyze(plan, config)
    }

    fn name(&self) -> &str {
        self.0.name()
    }
}

struct SkippableOptimizerRule(Arc<dyn OptimizerRule + Send + Sync>);

impl Debug for SkippableOptimizerRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl OptimizerRule for SkippableOptimizerRule {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // A skipped rule is handed the whole plan once and returns it, rather than being driven
        // over every node by the optimizer.
        if planning_point_lookup() {
            None
        } else {
            self.0.apply_order()
        }
    }

    #[expect(
        deprecated,
        reason = "a wrapper forwards every trait method to the rule it wraps"
    )]
    fn supports_rewrite(&self) -> bool {
        self.0.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if planning_point_lookup() {
            return Ok(Transformed::no(plan));
        }
        self.0.rewrite(plan, config)
    }
}

struct SkippablePhysicalRule(Arc<dyn PhysicalOptimizerRule + Send + Sync>);

impl Debug for SkippablePhysicalRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PhysicalOptimizerRule for SkippablePhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if planning_point_lookup() {
            return Ok(plan);
        }
        self.0.optimize(plan, config)
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if planning_point_lookup() {
            return Ok(plan);
        }
        self.0.optimize_with_context(plan, context)
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn schema_check(&self) -> bool {
        self.0.schema_check()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    use super::{create_physical_plan, is_point_lookup, wrap_skippable_rules};

    fn context() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..64)),
                Arc::new(Int32Array::from_iter_values((0..64).map(|i| i % 4))),
                Arc::new(StringArray::from_iter_values(
                    (0..64).map(|i| format!("n{i}")),
                )),
            ],
        )
        .expect("batch");
        let keyed = MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]])
            .expect("table")
            .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]));
        let composite = MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]])
            .expect("table")
            .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![1, 0],
            )]));
        let unkeyed = MemTable::try_new(schema, vec![vec![batch]]).expect("table");

        let mut builder = SessionStateBuilder::new().with_default_features();
        wrap_skippable_rules(&mut builder);
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("keyed", Arc::new(keyed))
            .expect("register");
        ctx.register_table("composite", Arc::new(composite))
            .expect("register");
        ctx.register_table("unkeyed", Arc::new(unkeyed))
            .expect("register");
        ctx
    }

    async fn unoptimized(ctx: &SessionContext, sql: &str) -> datafusion::logical_expr::LogicalPlan {
        ctx.state().create_logical_plan(sql).await.expect("plan")
    }

    #[tokio::test]
    async fn recognises_equality_on_every_key_column() {
        let ctx = context();
        for sql in [
            "SELECT * FROM keyed WHERE id = 5",
            "SELECT id, name FROM keyed WHERE 5 = id",
            "SELECT k.id AS i FROM keyed k WHERE k.id = 5 AND region > 1",
            "SELECT * FROM keyed WHERE id = '5'",
            "SELECT * FROM composite WHERE region = 1 AND id = 5",
            "SELECT * FROM keyed WHERE id = 5 AND name IS NOT NULL",
        ] {
            assert!(is_point_lookup(&unoptimized(&ctx, sql).await), "{sql}");
        }
    }

    #[tokio::test]
    async fn rejects_everything_else() {
        let ctx = context();
        for sql in [
            "SELECT * FROM unkeyed WHERE id = 5",
            "SELECT * FROM keyed WHERE region = 1",
            "SELECT * FROM composite WHERE id = 5",
            "SELECT * FROM keyed WHERE id = 5 LIMIT 1",
            "SELECT * FROM keyed WHERE id = 5 ORDER BY name",
            "SELECT count(*) FROM keyed WHERE id = 5",
            "SELECT upper(name) FROM keyed WHERE id = 5",
            "SELECT * FROM keyed WHERE id = 5 OR id = 6",
            "SELECT * FROM keyed WHERE id IN (5, 6)",
            "SELECT * FROM keyed WHERE id = 5 AND name LIKE 'n%'",
            "SELECT * FROM keyed WHERE id = (SELECT max(id) FROM keyed)",
            "SELECT a.id FROM keyed a JOIN keyed b ON a.id = b.id WHERE a.id = 5",
            "SELECT DISTINCT id FROM keyed WHERE id = 5",
            "SELECT * FROM keyed",
        ] {
            assert!(!is_point_lookup(&unoptimized(&ctx, sql).await), "{sql}");
        }
    }

    /// Neither the rules a point lookup skips nor the optimizer pass it does without may change
    /// its plan: plan every accepted shape both ways and compare the physical plans and rows.
    #[tokio::test]
    async fn lean_planning_leaves_point_lookup_plans_unchanged() {
        let ctx = context();
        for sql in [
            "SELECT * FROM keyed WHERE id = 5",
            "SELECT id, name FROM keyed WHERE 5 = id",
            "SELECT k.id AS i FROM keyed k WHERE k.id = 5 AND region > 1",
            "SELECT * FROM keyed WHERE id = '5'",
            "SELECT * FROM composite WHERE region = 1 AND id = 5",
            "SELECT * FROM keyed WHERE id = 5 AND id = 6",
            "SELECT * FROM keyed WHERE id = 5 AND id < 3",
            "SELECT * FROM keyed WHERE id = 5 AND id >= 5",
            "SELECT * FROM keyed WHERE id = 5 AND region = 1 AND region = 2",
            "SELECT * FROM keyed WHERE id = 5 AND region IS NULL",
            "SELECT * FROM keyed WHERE id = NULL",
            "SELECT * FROM keyed WHERE id = 5 AND name IS NULL",
            "SELECT * FROM keyed WHERE id = 5 AND region > 1 AND region < 0",
            "SELECT * FROM keyed WHERE id = 5 AND id IS NULL",
            "SELECT * FROM keyed WHERE id = 5 AND name = 'n5'",
            "SELECT * FROM keyed WHERE id = 5 AND name IS NOT NULL",
        ] {
            let plan = unoptimized(&ctx, sql).await;
            assert!(is_point_lookup(&plan), "{sql}");
            let full = ctx
                .state()
                .create_physical_plan(&plan)
                .await
                .expect("full plan");
            let lean = create_physical_plan(&mut ctx.state(), &plan, true)
                .await
                .expect("lean plan");
            assert_eq!(
                displayable(full.as_ref()).indent(true).to_string(),
                displayable(lean.as_ref()).indent(true).to_string(),
                "{sql}"
            );
            let full_rows = datafusion::physical_plan::collect(full, ctx.task_ctx())
                .await
                .expect("full rows");
            let lean_rows = datafusion::physical_plan::collect(lean, ctx.task_ctx())
                .await
                .expect("lean rows");
            assert_eq!(full_rows, lean_rows, "{sql}");
        }
    }

    #[tokio::test]
    async fn skipped_rules_still_run_outside_a_point_lookup() {
        let ctx = context();
        // `EnsureRequirements` must still shape a query that needs it.
        let sql = "SELECT region, count(*) FROM keyed GROUP BY region ORDER BY region";
        let plan = unoptimized(&ctx, sql).await;
        assert!(!is_point_lookup(&plan));
        let physical = ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let rows = datafusion::physical_plan::collect(physical, ctx.task_ctx())
            .await
            .expect("rows");
        let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4);
    }
}
