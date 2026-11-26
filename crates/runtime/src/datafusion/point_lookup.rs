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

//! Planning for point lookups.
//!
//! `SELECT <columns or constants> FROM t WHERE col = <value> [LIMIT n]` plans to a single filtered
//! scan, under a limit when it has one. The session runs about a hundred analyzer, logical and
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
//!
//! [`is_bounded_key_lookup`] recognises the wider family of queries that read a bounded number of
//! rows by primary key — several keys, `IN` lists, a disjunction of whole keys — which the query
//! runtime runs apart from analytical work (see `Query::run`).

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Constraint, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::{split_binary, split_conjunction};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, Projection, TableScan};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
use datafusion::physical_plan::{Distribution, ExecutionPlan};

/// Rules that cannot change the plan of a query [`is_point_lookup`] accepts: a projection of
/// plain columns or constants over a filter of column-against-constant comparisons over one table
/// scan, under at most one `LIMIT` with no offset, and with no join, aggregate, subquery, window,
/// sort, union or distinct anywhere.
///
/// `EnsureRequirements` is here because without a limit the shape has no distribution or ordering
/// requirement. It is skipped only for a lookup that pins a primary key, and only when the plan
/// already meets every requirement (see [`is_lookup_chain`]): a limit over a multi-partition scan
/// still gets the coalescing it needs. An operator a table scan returns must
/// meet its own input requirements, such as coalescing a multi-partition input. Rules that act on a scan
/// of a particular connector (`HttpParamsPushdown`) and `propagate_empty_relation` (which shapes
/// the plan of a contradictory predicate) are not here: they cost well under a microsecond and are
/// not worth reasoning about per connector.
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

/// A plan [`is_point_lookup`] accepts, and whether it pins every primary-key column of its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PointLookup {
    keyed: bool,
}

tokio::task_local! {
    /// Set only while the lookup's own logical plan is optimized, so a provider that plans another
    /// query of its own later (a view planned inside its scan) gets every logical rule.
    static OPTIMIZING_POINT_LOOKUP: PointLookup;
    /// Set while the lookup is planned physically. Nested planning inherits it, so a physical rule
    /// is skipped only when the plan it is handed is itself shaped like a lookup.
    static PLANNING_POINT_LOOKUP: PointLookup;
}

fn optimizing_point_lookup() -> bool {
    OPTIMIZING_POINT_LOOKUP.try_with(|_| ()).is_ok()
}

fn planning_point_lookup() -> Option<PointLookup> {
    PLANNING_POINT_LOOKUP.try_with(|lookup| *lookup).ok()
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
    point_lookup: Option<PointLookup>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(lookup) = point_lookup else {
        return session.create_physical_plan(plan).await;
    };
    let Some(optimized) = optimize_lean(session, plan, lookup)? else {
        return session.create_physical_plan(plan).await;
    };
    let planner = Arc::clone(session.query_planner());
    PLANNING_POINT_LOOKUP
        .scope(lookup, planner.create_physical_plan(&optimized, session))
        .await
}

/// Optimizes a point lookup's logical plan with one pass and its [`SKIPPABLE_RULES`] skipped, then
/// with full optimization when that pass leaves work another pass would do.
///
/// Returns `None` when analysis takes the plan out of the lookup shape, and the plan must be
/// planned in full. [`is_point_lookup`] sees the plan before the analyzer runs, and an analyzer
/// rule may rewrite it into something the skipped rules do act on: on a cluster,
/// `PartitionedTableScanRewrite` turns the scan into the union of its partitions, which with one
/// executor is a union of one input that only `optimize_unions` removes.
pub(crate) fn optimize_lean(
    session: &mut SessionState,
    plan: &LogicalPlan,
    lookup: PointLookup,
) -> Result<Option<LogicalPlan>> {
    let analyzed = OPTIMIZING_POINT_LOOKUP.sync_scope(lookup, || {
        session
            .analyzer()
            .execute_and_check(plan.clone(), session.config_options(), |_, _| {})
    })?;
    if !is_lookup_shaped(&analyzed) {
        return Ok(None);
    }
    let max_passes = session.config().options().optimizer.max_passes;
    session.config_mut().options_mut().optimizer.max_passes = 1;
    let optimized = OPTIMIZING_POINT_LOOKUP.sync_scope(lookup, || {
        session.optimizer().optimize(analyzed, &*session, |_, _| {})
    });
    session.config_mut().options_mut().optimizer.max_passes = max_passes;
    let optimized = optimized?;
    if has_constant_predicate(&optimized) || limit_can_reach_scan(&optimized) {
        return session.optimize(&optimized).map(Some);
    }
    Ok(Some(optimized))
}

/// Whether the analyzed `plan` still has only the nodes a point lookup is made of — limits,
/// projections, aliases and filters over one table scan — and no subquery in any expression.
/// The rules in [`SKIPPABLE_RULES`] act only on nodes and expressions outside that set. The scan
/// may have become the federated plan that federation analysis made of it: a leaf with no
/// expressions, which no logical rule can reach into.
fn is_lookup_shaped(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    loop {
        let holds_subquery = node.expressions().iter().any(|expr| {
            expr.exists(|expr| {
                Ok(matches!(
                    expr,
                    Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::ScalarSubquery(_)
                ))
            })
            .unwrap_or(true)
        });
        if holds_subquery {
            return false;
        }
        node = match node {
            LogicalPlan::Limit(limit) => limit.input.as_ref(),
            LogicalPlan::Projection(projection) => projection.input.as_ref(),
            LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            LogicalPlan::TableScan(_) => return true,
            LogicalPlan::Extension(extension) => {
                return extension
                    .node
                    .as_any()
                    .is::<datafusion_federation::FederatedPlanNode>();
            }
            _ => return false,
        };
    }
}

/// Whether a limit in `plan` sits above a table scan with only projections and aliases between
/// them, and the scan has no fetch yet. Within one pass `push_down_limit` runs before
/// `push_down_filter`, so a filter the provider takes exactly leaves the scan only after the limit
/// stopped at it; the next pass would push the limit into the scan.
fn limit_can_reach_scan(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    let mut below_limit = false;
    loop {
        match node {
            LogicalPlan::Limit(limit) => {
                below_limit = true;
                node = limit.input.as_ref();
            }
            LogicalPlan::Projection(projection) => node = projection.input.as_ref(),
            LogicalPlan::SubqueryAlias(alias) => node = alias.input.as_ref(),
            LogicalPlan::TableScan(scan) => return below_limit && scan.fetch.is_none(),
            _ => return false,
        }
    }
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

/// Whether `plan` is a point lookup: plain columns or constants projected from a single table scan
/// through filters made only of column-against-constant comparisons, at least one of them an
/// equality, optionally with one `LIMIT` with no offset above every filter. On a table with a
/// primary key, every key column must be pinned by an equality.
///
/// The limit may sit above or below the projections: an unoptimized plan carries it on top, and
/// an optimized one (such as the Substrait plan a sidecar sends) below the projection, which the
/// optimizer moves above it. A limit below a filter limits the rows the filter sees, a different
/// query, and is not a point lookup.
///
/// Which rules can change the plan depends only on this shape, not on whether the equality is on
/// a key: an equality on any column plans to the same filtered scan, and pays for the same rules.
pub(crate) fn is_point_lookup(plan: &LogicalPlan) -> Option<PointLookup> {
    let mut node = plan;
    let mut limited = false;
    let mut predicates: Vec<&Expr> = Vec::new();
    loop {
        match node {
            LogicalPlan::Limit(limit) => {
                let no_offset = limit.skip.as_deref().is_none_or(|skip| {
                    matches!(skip, Expr::Literal(ScalarValue::Int64(Some(0)), _))
                });
                let literal_fetch = limit.fetch.as_deref().is_some_and(|fetch| {
                    matches!(fetch, Expr::Literal(ScalarValue::Int64(Some(_)), _))
                });
                if limited || !predicates.is_empty() || !no_offset || !literal_fetch {
                    return None;
                }
                limited = true;
                node = limit.input.as_ref();
            }
            LogicalPlan::Projection(projection) => {
                if !projection.expr.iter().all(is_plain_column_or_constant) {
                    return None;
                }
                node = projection.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(alias) => node = alias.input.as_ref(),
            LogicalPlan::Filter(filter) => {
                predicates.push(&filter.predicate);
                node = filter.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => return scan_is_keyed_by(scan, &predicates),
            _ => return None,
        }
    }
}

fn scan_is_keyed_by(scan: &TableScan, predicates: &[&Expr]) -> Option<PointLookup> {
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return None;
    }
    let conjuncts: Vec<&Expr> = predicates
        .iter()
        .flat_map(|predicate| split_conjunction(predicate))
        .collect();
    if !conjuncts
        .iter()
        .all(|conjunct| is_simple_comparison(conjunct))
    {
        return None;
    }
    let schema = scan.source.schema();
    let primary_key = scan.source.constraints().and_then(|constraints| {
        constraints.iter().find_map(|constraint| match constraint {
            Constraint::PrimaryKey(indices) if !indices.is_empty() => Some(indices),
            _ => None,
        })
    });
    let keyed = match primary_key {
        Some(primary_key) => primary_key.iter().all(|&index| {
            index < schema.fields().len() && {
                let name = schema.field(index).name();
                conjuncts
                    .iter()
                    .any(|conjunct| is_equality_on(conjunct, name))
            }
        }),
        None => false,
    };
    let looked_up = keyed
        || (primary_key.is_none()
            && schema.fields().iter().any(|field| {
                conjuncts
                    .iter()
                    .any(|conjunct| is_equality_on(conjunct, field.name()))
            }));
    looked_up.then_some(PointLookup { keyed })
}

/// The most primary keys a query may name and still count as a [bounded key
/// lookup](is_bounded_key_lookup): enough for a batch of lookups by key, few enough that reading
/// them is a handful of rows whatever the table's size.
pub(crate) const MAX_BOUNDED_LOOKUP_KEYS: usize = 4096;

/// Whether `plan` (unoptimized) reads at most a bounded set of rows by primary key: one table scan
/// under filters that pin every primary-key column of the scanned table to finitely many constants,
/// naming at most [`MAX_BOUNDED_LOOKUP_KEYS`] keys, with nothing above the scan but projections,
/// filters, aliases, a sort or a limit, and no subquery, aggregate or window anywhere.
///
/// A key column is pinned by `column = constant`, by `column IN (constants)`, or by a disjunction of
/// those; a disjunction of conjunctions that each pin the whole key — the shape of a batch of
/// composite-key lookups, `(a = 1 AND b = 2) OR (a = 3 AND b = 4)` — names one key per branch.
/// Other predicates may narrow the rows further and are ignored. Unlike [`is_point_lookup`] this
/// says nothing about which optimizer rules can change the plan; it bounds the rows the query reads.
pub(crate) fn is_bounded_key_lookup(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    let mut predicates: Vec<&Expr> = Vec::new();
    loop {
        match node {
            LogicalPlan::Projection(projection) => {
                // The predicates gathered above name this projection's outputs, so they
                // bound the scan only through columns it passes through unchanged.
                if projection.expr.iter().any(reads_beyond_its_row)
                    || !predicates
                        .iter()
                        .all(|predicate| passes_through(projection, predicate))
                {
                    return false;
                }
                node = projection.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(alias) => node = alias.input.as_ref(),
            LogicalPlan::Filter(filter) => {
                if reads_beyond_its_row(&filter.predicate) {
                    return false;
                }
                predicates.push(&filter.predicate);
                node = filter.input.as_ref();
            }
            LogicalPlan::Sort(sort) => {
                // A sort that keeps only its first rows picks them from its whole input,
                // so a predicate above it bounds nothing the scan reads.
                if sort
                    .expr
                    .iter()
                    .any(|sort| reads_beyond_its_row(&sort.expr))
                    || (sort.fetch.is_some() && !predicates.is_empty())
                {
                    return false;
                }
                node = sort.input.as_ref();
            }
            LogicalPlan::Limit(limit) => {
                // Likewise a limit: a predicate above it filters the rows the limit kept.
                if [&limit.skip, &limit.fetch]
                    .into_iter()
                    .flatten()
                    .any(|expr| reads_beyond_its_row(expr))
                    || !predicates.is_empty()
                {
                    return false;
                }
                node = limit.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => return scan_has_bounded_keys(scan, &predicates),
            _ => return false,
        }
    }
}

/// Whether every column `predicate` reads leaves `projection` as the input column of the same
/// name, unrenamed and uncomputed, so the predicate means the same thing below the projection.
fn passes_through(projection: &Projection, predicate: &Expr) -> bool {
    predicate.column_refs().iter().all(|column| {
        projection.expr.iter().any(|expr| match expr {
            Expr::Column(input) => input.name == column.name,
            Expr::Alias(alias) => {
                alias.name == column.name
                    && matches!(alias.expr.as_ref(), Expr::Column(input) if input.name == column.name)
            }
            _ => false,
        })
    })
}

/// Whether `expr` needs more than the row it is evaluated on: a subquery, an outer reference, an
/// aggregate, a window or an unnest.
fn reads_beyond_its_row(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::ScalarSubquery(_)
                | Expr::OuterReferenceColumn(..)
                | Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::Unnest(_)
                | Expr::GroupingSet(_)
        ))
    })
    .unwrap_or(true)
}

fn scan_has_bounded_keys(scan: &TableScan, predicates: &[&Expr]) -> bool {
    let Some(primary_key) = scan.source.constraints().and_then(|constraints| {
        constraints.iter().find_map(|constraint| match constraint {
            Constraint::PrimaryKey(indices) => Some(indices),
            Constraint::Unique(_) => None,
        })
    }) else {
        return false;
    };
    let schema = scan.source.schema();
    let Some(key_columns) = primary_key
        .iter()
        .map(|&index| (index < schema.fields().len()).then(|| schema.field(index).name().as_str()))
        .collect::<Option<Vec<&str>>>()
    else {
        return false;
    };
    if key_columns.is_empty() {
        return false;
    }
    let conjuncts: Vec<&Expr> = predicates
        .iter()
        .copied()
        .chain(scan.filters.iter())
        .flat_map(split_conjunction)
        .collect();
    keys_named(&conjuncts, &key_columns).is_some_and(|keys| keys <= MAX_BOUNDED_LOOKUP_KEYS)
}

/// How many primary keys the conjunction of `conjuncts` can match at most, or `None` when it leaves
/// some key column unbounded: the product of what each key column is pinned to, or the keys a
/// disjunction among the conjuncts names branch by branch, whichever is fewer.
fn keys_named(conjuncts: &[&Expr], key_columns: &[&str]) -> Option<usize> {
    let by_column = key_columns.iter().try_fold(1_usize, |keys, column| {
        conjuncts
            .iter()
            .filter_map(|conjunct| values_pinned(conjunct, column))
            .min()
            .map(|values| keys.saturating_mul(values))
    });
    let by_disjunction = conjuncts
        .iter()
        .filter_map(|conjunct| {
            let branches = split_binary(conjunct, Operator::Or);
            if branches.len() < 2 {
                return None;
            }
            branches.iter().try_fold(0_usize, |keys, branch| {
                keys_named(&split_conjunction(branch), key_columns)
                    .map(|named| keys.saturating_add(named))
            })
        })
        .min();
    match (by_column, by_disjunction) {
        (Some(by_column), Some(by_disjunction)) => Some(by_column.min(by_disjunction)),
        (by_column, by_disjunction) => by_column.or(by_disjunction),
    }
}

/// How many values `expr` allows `column`: one for `column = constant`, the list's length for
/// `column IN (constants)`, and the sum over its branches for a disjunction of those.
fn values_pinned(expr: &Expr, column: &str) -> Option<usize> {
    let branches = split_binary(expr, Operator::Or);
    if branches.len() > 1 {
        return branches.iter().try_fold(0_usize, |values, branch| {
            values_pinned(branch, column).map(|pinned| values.saturating_add(pinned))
        });
    }
    match expr {
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) if matches!(expr.as_ref(), Expr::Column(c) if c.name == column)
            && list.iter().all(is_constant) =>
        {
            Some(list.len())
        }
        _ => is_equality_on(expr, column).then_some(1),
    }
}

fn is_plain_column_or_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(..) => true,
        Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Column(_) | Expr::Literal(..)),
        _ => false,
    }
}

/// Whether `plan` is shaped like a planned point lookup: a chain of single-input operators, none of
/// which needs a single partition from a child that has several, or a particular partitioning or
/// ordering at all. A plan with a join, an aggregate across partitions, a window or a sort
/// requirement is not, and every physical rule runs on it.
fn is_lookup_chain(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let children = plan.children();
    if children.len() > 1 {
        return false;
    }
    let distributions_met = children
        .iter()
        .zip(plan.input_distribution_requirements().into_per_child())
        .all(|(child, distribution)| match distribution {
            Distribution::UnspecifiedDistribution => true,
            Distribution::SinglePartition => {
                child.properties().output_partitioning().partition_count() == 1
            }
            _ => false,
        });
    distributions_met
        && plan.required_input_ordering().iter().all(Option::is_none)
        && children.into_iter().all(is_lookup_chain)
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
        if optimizing_point_lookup() {
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
        if optimizing_point_lookup() {
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
        if optimizing_point_lookup() {
            return Ok(Transformed::no(plan));
        }
        self.0.rewrite(plan, config)
    }
}

struct SkippablePhysicalRule(Arc<dyn PhysicalOptimizerRule + Send + Sync>);

impl SkippablePhysicalRule {
    /// Whether to hand `plan` back untouched: while planning a point lookup, when the plan it is
    /// handed is shaped like one ([`is_lookup_chain`]). `EnsureRequirements` also adds
    /// repartitioning for parallelism by the size of the input, so it is skipped only for a
    /// lookup that pins a primary key, which reads a handful of rows.
    fn skips(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        match planning_point_lookup() {
            None => false,
            Some(PointLookup { keyed }) => {
                is_lookup_chain(plan) && (keyed || self.0.name() != "EnsureRequirements")
            }
        }
    }
}

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
        if self.skips(&plan) {
            return Ok(plan);
        }
        self.0.optimize(plan, config)
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.skips(&plan) {
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
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    use super::{
        MAX_BOUNDED_LOOKUP_KEYS, create_physical_plan, is_bounded_key_lookup, is_point_lookup,
        optimize_lean, wrap_skippable_rules,
    };

    /// A view hidden behind a provider that plans it inside its own scan, as a provider that wraps
    /// another (an embedding table over a view) does: the lookup's scan starts a nested planning.
    #[derive(Debug)]
    struct HiddenView(Arc<dyn datafusion::catalog::TableProvider>);

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for HiddenView {
        fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
            self.0.schema()
        }
        fn table_type(&self) -> datafusion::logical_expr::TableType {
            datafusion::logical_expr::TableType::Base
        }
        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.0.scan(state, projection, filters, limit).await
        }
    }

    /// A provider that takes every filter exactly, as Spark Connect does. Only planned, never run.
    #[derive(Debug)]
    struct ExactFilters(Arc<Schema>);

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for ExactFilters {
        fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
            Arc::clone(&self.0)
        }
        fn table_type(&self) -> datafusion::logical_expr::TableType {
            datafusion::logical_expr::TableType::Base
        }
        fn supports_filters_pushdown(
            &self,
            filters: &[&datafusion::logical_expr::Expr],
        ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
        {
            Ok(vec![
                datafusion::logical_expr::TableProviderFilterPushDown::Exact;
                filters.len()
            ])
        }
        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[datafusion::logical_expr::Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            datafusion::common::not_impl_err!("planned only")
        }
    }

    /// Registers the tables that need a planned plan: `hidden`, an aggregate over the
    /// four-partition `split` behind a [`HiddenView`].
    async fn register_hidden_view(ctx: &SessionContext) {
        let view = ctx
            .sql("SELECT DISTINCT region, name FROM split")
            .await
            .expect("view")
            .into_unoptimized_plan();
        let view = datafusion::catalog::view::ViewTable::new(view, None);
        ctx.register_table("hidden", Arc::new(HiddenView(Arc::new(view))))
            .expect("register");
    }

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
        let unkeyed =
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]]).expect("table");
        // Four partitions of 16 rows: a limit above this scan needs its partitions coalesced.
        let split = MemTable::try_new(
            Arc::clone(&schema),
            (0..4)
                .map(|part| vec![batch.slice(part * 16, 16)])
                .collect(),
        )
        .expect("table");

        // 40,000 rows in one partition, far past a batch: full planning repartitions a scan this
        // size for parallelism.
        let big_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..40_000)),
                Arc::new(Int32Array::from_iter_values((0..40_000).map(|i| i % 4))),
                Arc::new(StringArray::from_iter_values(
                    (0..40_000).map(|i| format!("n{i}")),
                )),
            ],
        )
        .expect("batch");
        let big = MemTable::try_new(Arc::clone(&schema), vec![vec![big_batch]]).expect("table");
        let exact = ExactFilters(Arc::clone(&schema));

        let config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(8192);
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        wrap_skippable_rules(&mut builder);
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("keyed", Arc::new(keyed))
            .expect("register");
        ctx.register_table("composite", Arc::new(composite))
            .expect("register");
        ctx.register_table("unkeyed", Arc::new(unkeyed))
            .expect("register");
        ctx.register_table("split", Arc::new(split))
            .expect("register");
        ctx.register_table("big", Arc::new(big)).expect("register");
        ctx.register_table("exact", Arc::new(exact))
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
            // Constants in the projection, and a limit with no offset.
            "SELECT 1 AS present FROM keyed WHERE id = 5 LIMIT 1",
            "SELECT * FROM keyed WHERE id = 5 LIMIT 1",
            "SELECT 1 FROM keyed WHERE id = 5",
            // A table without a primary key is looked up by an equality on any column.
            "SELECT * FROM unkeyed WHERE id = 5",
            "SELECT 1 AS present FROM unkeyed WHERE name = 'n5' LIMIT 1",
            "SELECT 1 AS present FROM split WHERE name = 'n5' LIMIT 1",
            // The limit below the projection, as an optimized plan carries it.
            "SELECT 1 AS present FROM (SELECT * FROM keyed WHERE id = 5 LIMIT 1) q",
        ] {
            assert!(
                is_point_lookup(&unoptimized(&ctx, sql).await).is_some(),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_everything_else() {
        let ctx = context();
        for sql in [
            // A table with a primary key needs every key column pinned.
            "SELECT * FROM keyed WHERE region = 1",
            "SELECT * FROM composite WHERE id = 5",
            // A filter with no equality, an offset, or a limit below the projection.
            "SELECT * FROM unkeyed WHERE id > 5",
            "SELECT * FROM keyed WHERE id = 5 LIMIT 1 OFFSET 1",
            // A filter above a limit filters the rows the limit kept, and two limits nest.
            "SELECT * FROM (SELECT * FROM keyed LIMIT 10) q WHERE id = 5",
            "SELECT * FROM (SELECT * FROM keyed WHERE id = 5 LIMIT 3) q LIMIT 1",
            "SELECT id + 1 FROM keyed WHERE id = 5",
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
            assert!(
                is_point_lookup(&unoptimized(&ctx, sql).await).is_none(),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn bounded_key_lookups_name_finitely_many_keys() {
        let ctx = context();
        for sql in [
            "SELECT * FROM keyed WHERE id = 5",
            "SELECT * FROM keyed WHERE id = 5 OR id = 6",
            "SELECT * FROM keyed WHERE id IN (5, 6, 7)",
            "SELECT CAST(id AS VARCHAR), upper(name) FROM keyed WHERE id = 5",
            "SELECT * FROM keyed WHERE id = 5 AND name LIKE 'n%'",
            "SELECT * FROM keyed WHERE id = 5 ORDER BY name LIMIT 1",
            "SELECT * FROM composite WHERE (region = 1 AND id = 5) OR (region = 2 AND id = 6)",
            "SELECT * FROM composite WHERE region IN (1, 2) AND id IN (5, 6)",
            "SELECT * FROM composite WHERE region = 1 AND (id = 5 OR id = 6)",
            // A subquery that passes the key through under its own name.
            "SELECT * FROM (SELECT id, name FROM keyed) q WHERE id = 5",
            "SELECT * FROM (SELECT k.id AS id FROM keyed k) q WHERE id = 5",
        ] {
            assert!(
                is_bounded_key_lookup(&unoptimized(&ctx, sql).await),
                "{sql}"
            );
        }
        for sql in [
            "SELECT * FROM unkeyed WHERE id = 5",
            "SELECT * FROM keyed",
            "SELECT * FROM keyed WHERE id > 5",
            "SELECT * FROM keyed WHERE id NOT IN (5, 6)",
            "SELECT * FROM keyed WHERE id = 5 OR name = 'n6'",
            "SELECT * FROM composite WHERE id = 5",
            "SELECT * FROM composite WHERE (region = 1 AND id = 5) OR id = 6",
            "SELECT count(*) FROM keyed WHERE id = 5",
            "SELECT DISTINCT id FROM keyed WHERE id = 5",
            "SELECT * FROM keyed WHERE id IN (SELECT id FROM keyed)",
            "SELECT * FROM keyed WHERE id = (SELECT max(id) FROM keyed)",
            "SELECT a.id FROM keyed a JOIN keyed b ON a.id = b.id WHERE a.id = 5",
            // An outer `id` that is another column renamed, or a value computed from the
            // key, does not pin the key.
            "SELECT * FROM (SELECT region AS id FROM keyed) q WHERE id = 1",
            "SELECT * FROM (SELECT id + 1 AS id FROM keyed) q WHERE id = 5",
            "SELECT id FROM (SELECT id % 2 AS id FROM keyed) t WHERE id = 1",
            // A predicate above a limit filters the rows the limit kept from the whole table.
            "SELECT * FROM (SELECT * FROM keyed ORDER BY region, id LIMIT 10) q WHERE id = 4",
            "SELECT * FROM (SELECT * FROM keyed LIMIT 10) q WHERE id = 4",
        ] {
            assert!(
                !is_bounded_key_lookup(&unoptimized(&ctx, sql).await),
                "{sql}"
            );
        }
        // The key bound is inclusive: one more key than it allows reads like a scan.
        let in_list = |keys: usize| {
            let values: Vec<String> = (0..keys).map(|key| key.to_string()).collect();
            format!("SELECT * FROM keyed WHERE id IN ({})", values.join(", "))
        };
        assert!(is_bounded_key_lookup(
            &unoptimized(&ctx, &in_list(MAX_BOUNDED_LOOKUP_KEYS)).await
        ));
        assert!(!is_bounded_key_lookup(
            &unoptimized(&ctx, &in_list(MAX_BOUNDED_LOOKUP_KEYS + 1)).await
        ));
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
            "SELECT 1 AS present FROM keyed WHERE id = 5 LIMIT 1",
            "SELECT * FROM keyed WHERE id = 5 LIMIT 1",
            "SELECT 1 AS present FROM keyed WHERE id = 6 AND id = 5 LIMIT 1",
            "SELECT * FROM unkeyed WHERE id = 5",
            "SELECT 1 AS present FROM unkeyed WHERE name = 'n5' LIMIT 1",
            "SELECT 1 AS present FROM unkeyed WHERE name = 'absent' LIMIT 1",
            // Several partitions under a limit: `EnsureRequirements` must still coalesce them.
            "SELECT 1 AS present FROM split WHERE name = 'n5' LIMIT 1",
            "SELECT * FROM split WHERE region = 1 LIMIT 3",
            "SELECT * FROM split WHERE region = 1",
            "SELECT 1 AS present FROM (SELECT * FROM split WHERE name = 'n5' LIMIT 1) q",
            "SELECT 1 AS present FROM (SELECT * FROM keyed WHERE id = 5 LIMIT 1) q",
            // A large unkeyed scan, which full planning repartitions for parallelism.
            "SELECT * FROM big WHERE region = 1 LIMIT 100000",
            "SELECT * FROM big WHERE region = 1",
        ] {
            assert_lean_matches_full(&ctx, sql).await;
        }
    }

    /// Plans `sql` both ways and requires the same physical plan and the same rows.
    async fn assert_lean_matches_full(ctx: &SessionContext, sql: &str) {
        let plan = unoptimized(ctx, sql).await;
        let lookup = is_point_lookup(&plan);
        assert!(lookup.is_some(), "{sql}");
        let full = ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .expect("full plan");
        let lean = create_physical_plan(&mut ctx.state(), &plan, lookup)
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
        // A plan over several partitions returns its rows in whatever order the partitions
        // finish, so the rows are compared as a set; the plans above are compared exactly.
        assert_eq!(sorted_rows(&full_rows), sorted_rows(&lean_rows), "{sql}");
    }

    /// A provider that plans another query inside its own scan (an embedding table over a view):
    /// that nested planning must get every rule, though it runs while the lookup is being planned.
    /// The view's `DISTINCT` cannot be planned at all without `replace_distinct_aggregate`, one of
    /// the rules a lookup skips.
    #[tokio::test]
    async fn planning_nested_inside_a_lookup_scan_gets_every_rule() {
        let ctx = context();
        register_hidden_view(&ctx).await;
        assert_lean_matches_full(&ctx, "SELECT region, name FROM hidden WHERE region = 1").await;
    }

    /// Hands every table to the cluster's scan rewrite as `partitions` partitions, each read
    /// through the table itself and serving the given `region` values (none: the whole table).
    struct Partitions {
        ctx: SessionContext,
        regions: Vec<Vec<i32>>,
    }

    impl std::fmt::Debug for Partitions {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Partitions")
                .field("regions", &self.regions)
                .finish_non_exhaustive()
        }
    }

    impl runtime_datafusion::analyzer_rule::TablePartitionProvider for Partitions {
        fn get_partitions(
            &self,
            table: &datafusion::common::TableReference,
            _schema: &datafusion::arrow::datatypes::SchemaRef,
        ) -> datafusion::error::Result<runtime_datafusion::analyzer_rule::PartitionProviders>
        {
            let provider = futures::executor::block_on(self.ctx.table_provider(table.clone()))?;
            Ok(self
                .regions
                .iter()
                .map(|regions| {
                    let values = regions
                        .iter()
                        .map(|region| {
                            std::collections::HashMap::from([(
                                "region".to_string(),
                                Some(region.to_string()),
                            )])
                        })
                        .collect();
                    (Arc::clone(&provider), values)
                })
                .collect())
        }

        fn should_partition(&self, _scan: &datafusion::logical_expr::TableScan) -> bool {
            true
        }
    }

    /// On a cluster an analyzer rule rewrites the lookup's scan into the union of its partitions,
    /// after the lookup was recognised: with one executor a union of one input, which only
    /// `optimize_unions` (a rule a lookup skips) removes. A plan the analyzer took out of the
    /// lookup shape must get every rule. Regression test for #16856.
    #[tokio::test]
    async fn a_lookup_the_analyzer_partitions_gets_every_rule() {
        for regions in [
            vec![vec![]],
            vec![vec![0, 1, 2, 3]],
            vec![vec![0, 1], vec![2, 3]],
        ] {
            let ctx = context();
            let rule = runtime_datafusion::analyzer_rule::PartitionedTableScanRewrite::new(
                Arc::new(Partitions {
                    ctx: ctx.clone(),
                    regions: regions.clone(),
                }),
                &ctx,
            );
            ctx.add_analyzer_rule(Arc::new(rule));
            for sql in [
                "SELECT * FROM unkeyed WHERE id = 5",
                "SELECT 1 AS present FROM unkeyed WHERE name = 'n5' LIMIT 1",
                "SELECT * FROM keyed WHERE id = 5",
            ] {
                let plan = unoptimized(&ctx, sql).await;
                let lookup = is_point_lookup(&plan);
                // Recognised before analysis, and planned in full after it.
                assert!(
                    optimize_lean(&mut ctx.state(), &plan, lookup.expect("a point lookup"))
                        .expect("analyzed")
                        .is_none(),
                    "{sql} over {regions:?}"
                );
                let full = ctx
                    .state()
                    .create_physical_plan(&plan)
                    .await
                    .unwrap_or_else(|e| panic!("full plan {sql} over {regions:?}: {e}"));
                let lean = create_physical_plan(&mut ctx.state(), &plan, lookup)
                    .await
                    .unwrap_or_else(|e| panic!("lean plan {sql} over {regions:?}: {e}"));
                assert_eq!(
                    displayable(full.as_ref()).indent(true).to_string(),
                    displayable(lean.as_ref()).indent(true).to_string(),
                    "{sql} over {regions:?}"
                );
                let full_rows = datafusion::physical_plan::collect(full, ctx.task_ctx())
                    .await
                    .expect("full rows");
                let lean_rows = datafusion::physical_plan::collect(lean, ctx.task_ctx())
                    .await
                    .expect("lean rows");
                assert_eq!(sorted_rows(&full_rows), sorted_rows(&lean_rows), "{sql}");
                // The header and the one matching row.
                assert_eq!(sorted_rows(&lean_rows).len(), 2, "{sql} over {regions:?}");
            }
        }
    }

    struct NeverPlanned;

    #[async_trait::async_trait]
    impl datafusion_federation::FederationPlanner for NeverPlanned {
        async fn plan_federation(
            &self,
            _node: &datafusion_federation::FederatedPlanNode,
            _session_state: &datafusion::execution::session_state::SessionState,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            datafusion::common::not_impl_err!("never planned")
        }
    }

    /// Federation analysis replaces a federated lookup's scan, or the whole lookup, with a leaf it
    /// plans itself; that is still a lookup. A union, as a cluster makes of a scan, is not.
    #[tokio::test]
    async fn federated_lookups_keep_the_lookup_shape() {
        use datafusion::logical_expr::{Extension, LogicalPlanBuilder};
        let ctx = context();
        let lookup = ctx
            .state()
            .optimize(&unoptimized(&ctx, "SELECT * FROM unkeyed WHERE id = 5").await)
            .expect("optimize");
        let federated = || {
            LogicalPlan::Extension(Extension {
                node: Arc::new(datafusion_federation::FederatedPlanNode::new(
                    lookup.clone(),
                    Arc::new(NeverPlanned),
                )),
            })
        };
        assert!(super::is_lookup_shaped(&lookup));
        assert!(super::is_lookup_shaped(&federated()));
        let projected = LogicalPlanBuilder::from(federated())
            .project(vec![datafusion::prelude::col("id")])
            .and_then(|plan| plan.limit(0, Some(1)))
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        assert!(super::is_lookup_shaped(&projected));
        let union = LogicalPlanBuilder::from(lookup.clone())
            .union(lookup.clone())
            .and_then(LogicalPlanBuilder::build)
            .expect("union");
        assert!(!super::is_lookup_shaped(&union));
    }

    fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
        let table = datafusion::arrow::util::pretty::pretty_format_batches(batches)
            .expect("format")
            .to_string();
        let mut rows: Vec<String> = table
            .lines()
            .filter(|line| line.starts_with('|'))
            .map(str::to_string)
            .collect();
        rows.sort();
        rows
    }

    /// A provider that takes a filter exactly leaves the filter in the scan only after the first
    /// pass, so the limit reaches the scan only in a later one. The lean path must still end with
    /// the limit in the scan, or the provider returns every matching row.
    #[tokio::test]
    async fn lean_optimization_pushes_a_limit_into_a_scan_that_took_the_filter() {
        let ctx = context();
        for sql in [
            "SELECT name FROM exact WHERE region = 1 LIMIT 3",
            "SELECT 1 AS present FROM exact WHERE name = 'n5' LIMIT 1",
        ] {
            let plan = unoptimized(&ctx, sql).await;
            let lookup = is_point_lookup(&plan).expect("a point lookup");
            let full = ctx.state().optimize(&plan).expect("full");
            let lean = optimize_lean(&mut ctx.state(), &plan, lookup)
                .expect("lean")
                .expect("a lookup after analysis");
            assert_eq!(
                full.display_indent().to_string(),
                lean.display_indent().to_string(),
                "{sql}"
            );
            assert!(
                full.display_indent().to_string().contains("fetch="),
                "the limit reaches the scan: {}",
                full.display_indent()
            );
        }
    }

    #[tokio::test]
    async fn skipped_rules_still_run_outside_a_point_lookup() {
        let ctx = context();
        // `EnsureRequirements` must still shape a query that needs it.
        let sql = "SELECT region, count(*) FROM keyed GROUP BY region ORDER BY region";
        let plan = unoptimized(&ctx, sql).await;
        assert!(is_point_lookup(&plan).is_none());
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
