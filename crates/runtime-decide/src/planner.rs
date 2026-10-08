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

//! The planner rule for decision calls.
//!
//! `DataFusion` evaluates an async function only in a projection, a filter, or an
//! aggregate argument. In a filter it runs the function on every row that reaches the
//! filter, before the filter's other predicates, and it evaluates each call on its own,
//! so two calls on the same row are two model requests. [`DecisionPlacement`] fixes all
//! three by computing every decision call in a projection directly below the node that
//! uses it:
//!
//! ```text
//! Filter: status = 'open' AND ai_if(body, 'refund')
//!   TableScan: tickets
//! ```
//!
//! becomes
//!
//! ```text
//! Projection: <tickets columns>
//!   Filter: __decide_1.ai_if_0.probability > 0.5
//!     Projection: <tickets columns>, ai_decide(body, '{"ai_if_0": noul refund}', NULL, 'fail') AS __decide_1
//!       Filter: status = 'open'
//!         TableScan: tickets
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, DataFusionError, NullEquality, Result, plan_err};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::logical_plan::{
    Aggregate, Filter, Join, JoinType, LogicalPlan, Projection, Sort, Window,
};
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, Operator, SortExpr, binary_expr, lit};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use evaluate_api::Question;

use crate::FunctionSet;
use crate::args::{canonical_args, canonical_constants, typed_question};
use crate::functions::Kind;
use crate::output::{CHOICE, IF_THRESHOLD, PROBABILITY, SCORE};

/// The prefix of the columns that carry decision answers inside a plan.
const DECISION_COLUMN_PREFIX: &str = "__decide";

/// Whether `expr` contains a decision call.
fn contains_call(functions: &FunctionSet, expr: &Expr) -> bool {
    expr.exists(|e| Ok(call_kind(functions, e).is_some()))
        .unwrap_or(false)
}

fn call_kind(functions: &FunctionSet, expr: &Expr) -> Option<Kind> {
    match expr {
        // Every decision function is async; one downcast rules out the rest.
        Expr::ScalarFunction(function) if function.func.as_async().is_some() => {
            functions.kind_of(&function.func)
        }
        _ => None,
    }
}

/// Whether any node of `plan`, or of its subqueries, has a decision call.
fn plan_has_calls(functions: &FunctionSet, plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = contains_call(functions, expr);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// Computes decision calls in a projection below the node that uses them; see the
/// module documentation.
#[derive(Debug)]
pub struct DecisionPlacement {
    functions: Arc<FunctionSet>,
}

impl DecisionPlacement {
    pub(crate) fn new(functions: Arc<FunctionSet>) -> Self {
        Self { functions }
    }

    fn has_call(&self, expr: &Expr) -> bool {
        contains_call(&self.functions, expr)
    }

    fn place(
        &self,
        node: LogicalPlan,
        aliases: &AliasGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        match node {
            LogicalPlan::Projection(projection) => self.place_projection(projection, aliases),
            LogicalPlan::Filter(filter) => self.place_filter(filter, aliases),
            LogicalPlan::Sort(sort) => self.place_sort(sort, aliases),
            LogicalPlan::Aggregate(aggregate) => self.place_aggregate(aggregate, aliases),
            LogicalPlan::Window(window) => self.place_window(window, aliases),
            LogicalPlan::Join(join) => self.place_join(join, aliases),
            other => Ok(Transformed::no(other)),
        }
    }

    /// A projection whose only decision calls are whole, distinct `ai_decide`
    /// expressions over call-free arguments is already in its final form. Identical
    /// calls are not: `DataFusion` would run each of them and read only the first, so
    /// they are placed below as one call.
    fn is_placed(&self, projection: &Projection) -> bool {
        let mut decides: Vec<&Expr> = Vec::new();
        projection.expr.iter().all(|expr| {
            let inner = match expr {
                Expr::Alias(alias) => alias.expr.as_ref(),
                other => other,
            };
            match inner {
                Expr::ScalarFunction(function)
                    if call_kind(&self.functions, inner) == Some(Kind::Decide) =>
                {
                    let distinct = !decides.contains(&inner);
                    decides.push(inner);
                    distinct && !function.args.iter().any(|arg| self.has_call(arg))
                }
                other => !self.has_call(other),
            }
        })
    }

    fn place_projection(
        &self,
        projection: Projection,
        aliases: &AliasGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        if self.is_placed(&projection) {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        }
        let Projection { expr, input, .. } = projection;
        let placed = self.decide_below(expr.iter(), input, aliases)?;
        let names = NamePreserver::new_for_projection();
        let expr = expr
            .into_iter()
            .map(|e| {
                let saved = names.save(&e);
                Ok(saved.restore(placed.replace(e)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new(expr, Arc::new(placed.plan))?,
        )))
    }

    fn place_filter(
        &self,
        filter: Filter,
        aliases: &AliasGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        if !self.has_call(&filter.predicate) {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let Filter {
            predicate, input, ..
        } = filter;
        let schema = Arc::clone(input.schema());
        let (decisions, others): (Vec<Expr>, Vec<Expr>) = split_conjunction_owned(predicate)
            .into_iter()
            .partition(|e| self.has_call(e));
        // The other predicates run first, so the model only sees rows that pass them.
        let input = match conjunction(others) {
            Some(predicate) => Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, input)?)),
            None => input,
        };
        let placed = self.decide_below(decisions.iter(), input, aliases)?;
        let predicate = conjunction(
            decisions
                .into_iter()
                .map(|e| placed.replace(e))
                .collect::<Result<Vec<_>>>()?,
        )
        .ok_or_else(|| DataFusionError::Internal("a decision filter lost its predicate".into()))?;
        let filtered = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(placed.plan))?);
        Ok(Transformed::yes(keep_columns(filtered, &schema)?))
    }

    fn place_sort(&self, sort: Sort, aliases: &AliasGenerator) -> Result<Transformed<LogicalPlan>> {
        if !sort.expr.iter().any(|s| self.has_call(&s.expr)) {
            return Ok(Transformed::no(LogicalPlan::Sort(sort)));
        }
        let Sort { expr, input, fetch } = sort;
        let schema = Arc::clone(input.schema());
        let placed = self.decide_below(expr.iter().map(|s| &s.expr), input, aliases)?;
        let expr = expr
            .into_iter()
            .map(|s| {
                Ok(SortExpr {
                    expr: placed.replace(s.expr)?,
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sorted = LogicalPlan::Sort(Sort {
            expr,
            input: Arc::new(placed.plan),
            fetch,
        });
        Ok(Transformed::yes(keep_columns(sorted, &schema)?))
    }

    fn place_aggregate(
        &self,
        aggregate: Aggregate,
        aliases: &AliasGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        if !aggregate
            .group_expr
            .iter()
            .chain(&aggregate.aggr_expr)
            .any(|e| self.has_call(e))
        {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }
        let Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } = aggregate;
        if let Some(Expr::GroupingSet(_)) = group_expr.iter().find(|e| self.has_call(e)) {
            return plan_err!(
                "Decision functions cannot be used in GROUPING SETS, ROLLUP or CUBE yet. Compute them in a subquery and group by its column."
            );
        }
        let placed = self.decide_below(group_expr.iter().chain(&aggr_expr), input, aliases)?;
        let names = NamePreserver::new_for_projection();
        let restore = |e: Expr| -> Result<Expr> {
            let saved = names.save(&e);
            Ok(saved.restore(placed.replace(e)?))
        };
        let group_expr = group_expr
            .into_iter()
            .map(restore)
            .collect::<Result<Vec<_>>>()?;
        let aggr_expr = aggr_expr
            .into_iter()
            .map(restore)
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::yes(LogicalPlan::Aggregate(
            Aggregate::try_new(Arc::new(placed.plan), group_expr, aggr_expr)?,
        )))
    }

    fn place_window(
        &self,
        window: Window,
        aliases: &AliasGenerator,
    ) -> Result<Transformed<LogicalPlan>> {
        if !window.window_expr.iter().any(|e| self.has_call(e)) {
            return Ok(Transformed::no(LogicalPlan::Window(window)));
        }
        let Window {
            input,
            window_expr,
            schema,
        } = window;
        let placed = self.decide_below(window_expr.iter(), input, aliases)?;
        let names = NamePreserver::new_for_projection();
        let window_expr = window_expr
            .into_iter()
            .map(|e| {
                let saved = names.save(&e);
                Ok(saved.restore(placed.replace(e)?))
            })
            .collect::<Result<Vec<_>>>()?;
        let windowed = LogicalPlan::Window(Window::try_new(window_expr, Arc::new(placed.plan))?);
        Ok(Transformed::yes(keep_columns(windowed, &schema)?))
    }

    /// An inner join's decision conditions become a filter above the join, which is
    /// the same result: a pair is kept when every condition holds.
    fn place_join(&self, join: Join, aliases: &AliasGenerator) -> Result<Transformed<LogicalPlan>> {
        let in_filter = join.filter.as_ref().is_some_and(|f| self.has_call(f));
        let in_keys = join
            .on
            .iter()
            .any(|(left, right)| self.has_call(left) || self.has_call(right));
        if !in_filter && !in_keys {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        if join.join_type != JoinType::Inner {
            return plan_err!(
                "Decision functions cannot be used in the condition of a {} join. Use an inner join, or apply the decision in WHERE over the joined rows.",
                join.join_type
            );
        }

        let Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            null_equality,
            null_aware,
            ..
        } = join;
        let mut moved = Vec::new();
        let mut keys = Vec::with_capacity(on.len());
        for (left_key, right_key) in on {
            if self.has_call(&left_key) || self.has_call(&right_key) {
                // A moved key keeps the join's NULL semantics: a join on
                // `IS NOT DISTINCT FROM` matches a NULL key to a NULL key.
                moved.push(match null_equality {
                    NullEquality::NullEqualsNothing => left_key.eq(right_key),
                    NullEquality::NullEqualsNull => {
                        binary_expr(left_key, Operator::IsNotDistinctFrom, right_key)
                    }
                });
            } else {
                keys.push((left_key, right_key));
            }
        }
        let mut kept = Vec::new();
        for predicate in filter.map(split_conjunction_owned).unwrap_or_default() {
            if self.has_call(&predicate) {
                moved.push(predicate);
            } else {
                kept.push(predicate);
            }
        }
        let joined = LogicalPlan::Join(Join::try_new(
            left,
            right,
            keys,
            conjunction(kept),
            join_type,
            join_constraint,
            null_equality,
            null_aware,
        )?);
        let predicate = conjunction(moved).ok_or_else(|| {
            DataFusionError::Internal("a decision join lost its condition".into())
        })?;
        let filter = Filter::try_new(predicate, Arc::new(joined))?;
        Ok(Transformed::yes(self.place_filter(filter, aliases)?.data))
    }

    /// Adds a projection below a node that computes the decision calls in `exprs`:
    /// each distinct input and model gets one `ai_decide` call asking every question
    /// the typed calls on it ask, and identical calls are computed once.
    fn decide_below<'a>(
        &self,
        exprs: impl Iterator<Item = &'a Expr>,
        input: Arc<LogicalPlan>,
        aliases: &AliasGenerator,
    ) -> Result<Placed> {
        let mut calls: Vec<(Expr, Kind)> = Vec::new();
        for expr in exprs {
            expr.apply(|e| {
                if let Some(kind) = call_kind(&self.functions, e) {
                    if !calls.iter().any(|(call, _)| call == e) {
                        calls.push((e.clone(), kind));
                    }
                    return Ok(TreeNodeRecursion::Jump);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }

        let mut groups: Vec<Group> = Vec::new();
        let mut targets: Vec<(Expr, usize, Target)> = Vec::with_capacity(calls.len());
        for (call, kind) in calls {
            let Expr::ScalarFunction(function) = &call else {
                continue;
            };
            if function.args.iter().any(|arg| self.has_call(arg)) {
                return plan_err!(
                    "{}: decision functions cannot be nested. Compute the inner decision in a subquery first.",
                    kind.name()
                );
            }
            let args =
                canonical_args(kind, &function.args)?.unwrap_or_else(|| function.args.clone());
            let model_position = args.len() - 2;
            let key = (
                args[0].clone(),
                args[model_position].clone(),
                args[model_position + 1].clone(),
            );
            if kind == Kind::Decide {
                groups.push(Group::Decide(call.clone()));
                targets.push((call, groups.len() - 1, Target::Whole));
                continue;
            }
            let constants = canonical_constants(kind, &args)?;
            let question = typed_question(kind, &constants)?;
            let group = if let Some(index) = groups.iter().position(|g| g.is_typed_for(&key)) {
                index
            } else {
                groups.push(Group::Typed {
                    key: Box::new(key.clone()),
                    questions: Vec::new(),
                });
                groups.len() - 1
            };
            let id = groups[group].question_id(kind, question)?;
            targets.push((call, group, Target::Field { kind, id }));
        }

        let mut projected: Vec<Expr> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let mut columns = Vec::with_capacity(groups.len());
        for group in groups {
            let alias = aliases.next(DECISION_COLUMN_PREFIX);
            let decision = match group {
                Group::Decide(call) => call,
                Group::Typed { key, questions } => {
                    let (input_expr, model, on_error) = *key;
                    let questions: BTreeMap<String, Question> = questions.into_iter().collect();
                    let json = serde_json::to_string(&questions)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        Arc::clone(&self.functions.decide),
                        vec![input_expr, lit(json), model, on_error],
                    ))
                }
            };
            projected.push(decision.alias(&alias));
            columns.push(Expr::Column(Column::new_unqualified(alias)));
        }

        let mut replacements = HashMap::with_capacity(targets.len());
        for (call, group, target) in targets {
            let column = columns[group].clone();
            let replacement = match target {
                Target::Whole => column,
                Target::Field { kind, id } => {
                    let answer = get_field(column, id);
                    match kind {
                        Kind::If => get_field(answer, PROBABILITY).gt(lit(IF_THRESHOLD)),
                        Kind::Probability => get_field(answer, PROBABILITY),
                        Kind::Classify => get_field(answer, CHOICE),
                        Kind::Score => get_field(answer, SCORE),
                        Kind::Decide => {
                            return plan_err!("ai_decide is placed whole, never as a field");
                        }
                    }
                }
            };
            replacements.insert(call, replacement);
        }

        Ok(Placed {
            plan: LogicalPlan::Projection(Projection::try_new(projected, input)?),
            replacements,
        })
    }
}

/// A projection computing decisions, and what each call it replaced becomes.
struct Placed {
    plan: LogicalPlan,
    replacements: HashMap<Expr, Expr>,
}

impl Placed {
    fn replace(&self, expr: Expr) -> Result<Expr> {
        expr.transform_down(|e| match self.replacements.get(&e) {
            Some(replacement) => Ok(Transformed::new(
                replacement.clone(),
                true,
                TreeNodeRecursion::Jump,
            )),
            None => Ok(Transformed::no(e)),
        })
        .map(|t| t.data)
    }
}

/// The calls of one node that become one `ai_decide` call.
enum Group {
    /// A user's own `ai_decide` call, computed as written.
    Decide(Expr),
    /// Typed calls on the same input, model and error handling, with the distinct
    /// questions they ask.
    Typed {
        key: Box<(Expr, Expr, Expr)>,
        questions: Vec<(String, Question)>,
    },
}

impl Group {
    fn is_typed_for(&self, key: &(Expr, Expr, Expr)) -> bool {
        matches!(self, Self::Typed { key: k, .. } if k.as_ref() == key)
    }

    /// The id of `question` in this group, adding it if no call asked it yet. Ids name
    /// the function that asked (`ai_if_0`), so errors can name it too.
    fn question_id(&mut self, kind: Kind, question: Question) -> Result<String> {
        let Self::Typed { questions, .. } = self else {
            return plan_err!("only typed calls share a decision");
        };
        if let Some((id, _)) = questions.iter().find(|(_, q)| *q == question) {
            return Ok(id.clone());
        }
        let id = format!("{}_{}", kind.name(), questions.len());
        questions.push((id.clone(), question));
        Ok(id)
    }
}

/// What a replaced call reads from its decision column.
enum Target {
    /// The whole struct (`ai_decide`).
    Whole,
    /// One question's answer field (the typed functions).
    Field { kind: Kind, id: String },
}

/// Projects `plan` to the columns of `schema`, dropping the decision columns.
fn keep_columns(plan: LogicalPlan, schema: &DFSchemaRef) -> Result<LogicalPlan> {
    let exprs = schema.columns().into_iter().map(Expr::Column).collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan),
    )?))
}

impl OptimizerRule for DecisionPlacement {
    fn name(&self) -> &'static str {
        "decision_placement"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !plan_has_calls(&self.functions, &plan) {
            return Ok(Transformed::no(plan));
        }
        let aliases = Arc::clone(config.alias_generator());
        plan.transform_up_with_subqueries(|node| self.place(node, &aliases))
    }
}
