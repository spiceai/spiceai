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

//! Filter pushdown for the federation analyzer that keeps a correlated
//! subquery predicate above a join whose sides it spans.
//!
//! The federation analyzer pushes filters down *before* the optimizer
//! decorrelates subqueries. `DataFusion`'s `PushDownFilter` decides which join
//! side a predicate belongs to from `Expr::column_refs`, which does not see the
//! outer references inside a subquery. For TPC-H Q17's
//! `l_quantity < (SELECT … WHERE l_partkey = p_partkey)` it therefore sees only
//! `l_quantity` and moves the predicate onto the `lineitem` side of the join,
//! out of reach of `part`. When both sides federate to one source the whole
//! join is unparsed back into a single SQL statement and the placement is
//! harmless; when they do not, decorrelation later fails with
//! `Schema error: No field named part.p_partkey`.

use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::{
        Column, Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    logical_expr::{
        Expr, Filter, Join, LogicalPlan, Subquery,
        expr::{Exists, InSubquery, SetComparison},
        utils::{conjunction, find_out_reference_exprs, split_conjunction},
    },
    optimizer::{
        ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule, optimize_unions::OptimizeUnions,
    },
};
use datafusion_federation::{
    FederationAnalyzerForLogicalPlan, FederationAnalyzerRule, FederationProviderRef,
    get_table_source,
    sql::optimizer::{OptimizeProjectionsFederation, PushDownFilterFederation},
};

/// The federation analyzer rule Spice registers.
///
/// This is `datafusion_federation::sql::federation_analyzer_rule()` with its
/// filter pushdown wrapped in [`CorrelatedFilterPushDown`]; keep the remaining
/// rules in step with that function.
#[must_use]
pub fn federation_analyzer_rule() -> FederationAnalyzerRule {
    FederationAnalyzerRule::new().with_optimizer(Optimizer::with_rules(vec![
        Arc::new(OptimizeUnions::new()),
        Arc::new(CorrelatedFilterPushDown::default()),
        Arc::new(OptimizeProjectionsFederation::new()),
    ]))
}

/// [`PushDownFilterFederation`] that holds a correlated subquery predicate
/// above a join when the predicate spans both sides and the join does not
/// federate to a single source. Every other filter is handed to the inner rule
/// unchanged.
#[derive(Default, Debug)]
pub struct CorrelatedFilterPushDown {
    inner: PushDownFilterFederation,
}

impl OptimizerRule for CorrelatedFilterPushDown {
    fn name(&self) -> &'static str {
        "federation_sql_correlated_filter_push_down"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    #[expect(deprecated)]
    fn supports_rewrite(&self) -> bool {
        self.inner.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Some(SplitFilter {
            held,
            pushable,
            join,
            direct,
        }) = split_held_predicates(&plan)?
        else {
            return self.inner.rewrite(plan, config);
        };

        // Nothing below the held predicates can move, so leave the node as it is
        // rather than reporting a change on every optimizer pass.
        if pushable.is_empty() && direct {
            return Ok(Transformed::no(plan));
        }

        let below = match conjunction(pushable) {
            Some(predicate) => {
                let filter = LogicalPlan::Filter(Filter::try_new(predicate, join)?);
                self.inner.rewrite(filter, config)?.data
            }
            None => Arc::unwrap_or_clone(join),
        };

        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            held,
            Arc::new(below),
        )?)))
    }
}

struct SplitFilter {
    /// The conjuncts that must stay above the join, and-ed together.
    held: Expr,
    /// Conjuncts the inner rule may push as usual.
    pushable: Vec<Expr>,
    /// The join beneath the filter (and any filters stacked directly on it).
    join: Arc<LogicalPlan>,
    /// Whether the filter sat directly on the join, with no stacked filters.
    direct: bool,
}

/// Split a filter over a join into the conjuncts that must stay above it and
/// those that may be pushed, or `None` when there is nothing to hold.
///
/// Filters stacked directly on the join are merged in, because the inner rule
/// merges a filter into its child filter and pushes the result in one step.
fn split_held_predicates(plan: &LogicalPlan) -> Result<Option<SplitFilter>> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(None);
    };

    let mut predicates = vec![&filter.predicate];
    let mut input = &filter.input;
    while let LogicalPlan::Filter(child) = input.as_ref() {
        predicates.push(&child.predicate);
        input = &child.input;
    }

    let LogicalPlan::Join(join) = input.as_ref() else {
        return Ok(None);
    };

    let mut held = vec![];
    let mut pushable = vec![];
    for conjunct in predicates.iter().flat_map(|p| split_conjunction(p)) {
        if spans_join_sides(conjunct, join)? {
            held.push(conjunct.clone());
        } else {
            pushable.push(conjunct.clone());
        }
    }

    // When one source accepts the whole filtered join it is unparsed into one
    // statement, where the pushed placement lands back in a single scope.
    let Some(held) = conjunction(held) else {
        return Ok(None);
    };
    if federates_whole(plan)? {
        return Ok(None);
    }

    Ok(Some(SplitFilter {
        held,
        pushable,
        direct: Arc::ptr_eq(input, &filter.input),
        join: Arc::clone(input),
    }))
}

fn subquery_of(expr: &Expr) -> Option<&Subquery> {
    match expr {
        Expr::ScalarSubquery(subquery)
        | Expr::Exists(Exists { subquery, .. })
        | Expr::InSubquery(InSubquery { subquery, .. })
        | Expr::SetComparison(SetComparison { subquery, .. }) => Some(subquery),
        _ => None,
    }
}

/// Whether `predicate` holds a correlated subquery and, once the outer
/// references inside its subqueries are counted, needs columns from both sides
/// of `join` or names a column neither side has. Holding such a predicate is
/// always correct: it stays where the query wrote it.
fn spans_join_sides(predicate: &Expr, join: &Join) -> Result<bool> {
    let mut correlated = false;
    let mut columns: HashSet<Column> = predicate.column_refs().into_iter().cloned().collect();
    predicate.apply(|expr| {
        if let Some(subquery) = subquery_of(expr)
            && !subquery.outer_ref_columns.is_empty()
        {
            correlated = true;
            collect_outer_references(subquery, &mut columns)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !correlated {
        return Ok(false);
    }

    let left = join.left.schema();
    let right = join.right.schema();
    let all_left = columns.iter().all(|column| left.has_column(column));
    let all_right = columns.iter().all(|column| right.has_column(column));
    Ok(!all_left && !all_right)
}

/// Every outer reference inside `subquery`, including those of subqueries
/// nested within it.
fn collect_outer_references(subquery: &Subquery, columns: &mut HashSet<Column>) -> Result<()> {
    let mut insert = |expr: &Expr| {
        for outer in find_out_reference_exprs(expr) {
            if let Expr::OuterReferenceColumn(_, column) = outer {
                columns.insert(column);
            }
        }
    };
    subquery.outer_ref_columns.iter().for_each(&mut insert);
    subquery.subquery.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            insert(expr);
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(())
}

/// Whether one federation provider owns every table the plan reads,
/// subqueries included, and accepts the plan whole.
///
/// Sharing a provider is not enough: a provider that refuses the plan (for
/// example over a function the source cannot run) has the analyzer federate
/// each side separately, and the pushed predicate then reaches the source
/// without the table it references.
fn federates_whole(plan: &LogicalPlan) -> Result<bool> {
    let mut sole: Option<FederationProviderRef> = None;
    let mut single = true;
    plan.apply_with_subqueries(|node| {
        let LogicalPlan::TableScan(scan) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let provider = get_table_source(&scan.source)?.map(|source| source.federation_provider());
        match (&sole, provider) {
            (None, Some(provider)) => sole = Some(provider),
            (Some(existing), Some(provider)) if existing.as_ref() == provider.as_ref() => {}
            _ => {
                single = false;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(single
        && sole.is_some_and(|provider| {
            matches!(
                provider.analyzer(plan),
                Some(FederationAnalyzerForLogicalPlan::With(_))
            )
        }))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        execution::SessionStateBuilder,
        logical_expr::TableSource,
        optimizer::{Analyzer, AnalyzerRule},
        prelude::SessionContext,
    };
    use datafusion_federation::{
        FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
    };

    use super::*;
    use crate::analyzer_rule::AnalyzerRulesBuilder;

    /// A provider that is recognised as federated. One that `accepts` claims
    /// every plan (its analyzer has no rules, so nothing is rewritten and the
    /// plan stays inspectable); one that does not refuses every plan, as a source does over a
    /// function it cannot run, and the analyzer falls back to its children.
    #[derive(Debug)]
    struct StubProvider {
        compute_context: &'static str,
        accepts: bool,
    }

    impl FederationProvider for StubProvider {
        fn name(&self) -> &'static str {
            "stub"
        }

        fn compute_context(&self) -> Option<String> {
            Some(self.compute_context.to_string())
        }

        fn analyzer(&self, _plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
            Some(if self.accepts {
                FederationAnalyzerForLogicalPlan::With(Arc::new(Analyzer::with_rules(vec![])))
            } else {
                FederationAnalyzerForLogicalPlan::Unable
            })
        }
    }

    #[derive(Debug)]
    struct StubSource {
        provider: Arc<dyn FederationProvider>,
        schema: SchemaRef,
    }

    impl TableSource for StubSource {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl FederatedTableSource for StubSource {
        fn federation_provider(&self) -> Arc<dyn FederationProvider> {
            Arc::clone(&self.provider)
        }
    }

    const Q17: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly \
        FROM lineitem, part \
        WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' \
        AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";

    fn federated_table(
        ctx: &SessionContext,
        name: &str,
        compute_context: &'static str,
        accepts: bool,
        fields: Vec<Field>,
    ) {
        let source = Arc::new(StubSource {
            provider: Arc::new(StubProvider {
                compute_context,
                accepts,
            }),
            schema: Arc::new(Schema::new(fields)),
        });
        ctx.register_table(name, Arc::new(FederatedTableProviderAdaptor::new(source)))
            .expect("register the federated table");
    }

    fn context(
        analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
        lineitem_source: &'static str,
        part_source: &'static str,
        accepts: bool,
    ) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        federated_table(
            &ctx,
            "lineitem",
            lineitem_source,
            accepts,
            vec![
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_quantity", DataType::Float64, false),
                Field::new("l_extendedprice", DataType::Float64, false),
            ],
        );
        federated_table(
            &ctx,
            "part",
            part_source,
            accepts,
            vec![
                Field::new("p_partkey", DataType::Int64, false),
                Field::new("p_brand", DataType::Utf8, false),
            ],
        );
        ctx
    }

    async fn analyzed_plan(ctx: &SessionContext) -> LogicalPlan {
        let plan = ctx
            .state()
            .create_logical_plan(Q17)
            .await
            .expect("plan the query");
        ctx.state()
            .analyzer()
            .execute_and_check(plan, ctx.state().config_options(), |_, _| {})
            .expect("analyze the query")
    }

    /// The first join in `plan` whose condition names a column neither of its
    /// inputs provides.
    fn unbound_join_column(plan: &LogicalPlan) -> Option<String> {
        let mut unbound = None;
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Join(join) = node
                && let Some(filter) = &join.filter
            {
                for column in filter.column_refs() {
                    if !join.left.schema().has_column(column)
                        && !join.right.schema().has_column(column)
                    {
                        unbound = Some(column.flat_name());
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk the plan");
        unbound
    }

    async fn optimized_plan(ctx: &SessionContext) -> Result<LogicalPlan> {
        ctx.sql(Q17)
            .await
            .expect("plan the query")
            .into_optimized_plan()
    }

    fn unguarded_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        vec![Arc::new(
            datafusion_federation::sql::federation_analyzer_rule(),
        )]
    }

    /// Q17 whose sides the analyzer federates separately: the unguarded
    /// pushdown has to leave a join that reads `part.p_partkey` without `part`
    /// beneath it (the control), and the guarded one must not.
    async fn assert_split_join_binds(lineitem_source: &'static str, part_source: &'static str) {
        let control = context(unguarded_rules(), lineitem_source, part_source, false);
        let control_plan = optimized_plan(&control)
            .await
            .expect("the unguarded pushdown still returns a plan");
        assert_eq!(
            unbound_join_column(&control_plan).as_deref(),
            Some("part.p_partkey"),
            "the control has to show the defect this rule exists for:\n{}",
            control_plan.display_indent()
        );

        let ctx = context(
            AnalyzerRulesBuilder::new().build(),
            lineitem_source,
            part_source,
            false,
        );
        let plan = optimized_plan(&ctx).await.expect("optimize the query");
        assert_eq!(
            unbound_join_column(&plan),
            None,
            "a correlated predicate over a split join has to stay where it binds:\n{}",
            plan.display_indent()
        );

        let analyzed = analyzed_plan(&ctx).await.display_indent().to_string();
        assert!(
            analyzed.contains("Filter: part.p_brand = Utf8(\"Brand#23\")"),
            "the conjuncts that belong to one side still have to be pushed to it:\n{analyzed}"
        );
    }

    /// Regression test for #8220: TPC-H Q17 over two tables from different
    /// sources decorrelated into a join that reads `part.p_partkey` without
    /// `part` beneath it, which `spiced` reports as
    /// `No field named part.p_partkey`.
    #[tokio::test]
    async fn a_correlated_predicate_over_two_sources_still_binds() {
        assert_split_join_binds("lineitem_source", "part_source").await;
    }

    /// One source that refuses the whole plan is federated side by side, which
    /// is the two-source case again: `SQLite` over a Spice-only function sent
    /// `lineitem` alone with a predicate on `part_sqlite.p_partkey`.
    #[tokio::test]
    async fn a_correlated_predicate_over_a_refused_single_source_still_binds() {
        assert_split_join_binds("one_source", "one_source").await;
    }

    /// A join that one source accepts whole is unparsed into one statement, so
    /// its plan has to stay exactly what the unguarded pushdown produces.
    #[tokio::test]
    async fn a_single_source_join_it_accepts_is_left_to_the_inner_rule() {
        let unguarded = analyzed_plan(&context(
            unguarded_rules(),
            "one_source",
            "one_source",
            true,
        ))
        .await;
        let guarded = analyzed_plan(&context(
            AnalyzerRulesBuilder::new().build(),
            "one_source",
            "one_source",
            true,
        ))
        .await;
        assert_eq!(
            guarded.display_indent().to_string(),
            unguarded.display_indent().to_string()
        );
    }
}
