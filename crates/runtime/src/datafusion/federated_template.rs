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

//! Physical plans of fully federated queries, planned once per query shape.
//!
//! A sidecar reading every table of a query from one upstream Spice runtime
//! plans the query, optimizes it and encodes it as Substrait for every request,
//! though a sidecar doing single-key lookups sends the same query with only the
//! key changed. Here the literal operands of a filter's comparisons are replaced
//! with typed placeholders (`$N`), and the result, the template, is planned
//! once: its physical plan is a Substrait leaf whose plan holds the
//! placeholders. Each request binds its own literal values to that leaf, and
//! the upstream receives them as the plan's parameter bindings
//! ([`flight_client::parameterized_ticket`]).
//!
//! A template is cached under the query with its literals replaced, so two
//! queries share one only when they differ in nothing but those literals. The
//! entry is checked against the request's template, cache namespace and plans
//! generation before it is used; a hash collision or a plan cached before a
//! catalog change is planned again rather than served.
//!
//! Only literals whose comparison needs no type coercion with a column are
//! replaced (a string literal with a string column, an integer literal with an
//! integer column, a boolean with a boolean): a float literal compared with a
//! decimal, for one, is rewritten by an analyzer rule that reads the literal's
//! value, and a placeholder would skip that rewrite. A plan with a function
//! whose result can change between calls (`now()`) is never a template, since
//! planning folds it into a constant. Anything else is planned per request as
//! it always was.

use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::datatypes::{DataType, Field};
use data_components::flight::{SpiceSubstraitRead, SubstraitTemplate, spice_substrait_read};
use datafusion::{
    common::{
        DFSchema, Result, ScalarValue,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    execution::SessionState,
    logical_expr::{
        BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, Volatility,
        expr::{InList, Placeholder},
    },
    physical_plan::ExecutionPlan,
};
use runtime_request_context::{CacheNamespace, RequestContext};

use super::{DataFusion, PlansGeneration};

/// The approximate bytes the templates may hold: the plans cache's entry
/// budget at 128 KiB a shape. The plans cache bounds entries, not bytes; a
/// template also holds its Substrait and a physical plan, so it is bounded by
/// its weight instead (see [`Entry::weight`]), and expires as a cached plan
/// does.
const MAX_TEMPLATE_BYTES: u64 =
    crate::init::caching::DEFAULT_CACHED_PLANS_MAX_CAPACITY * 128 * 1024;

pub(crate) struct FederatedTemplates {
    cache: moka::sync::Cache<u64, Arc<Entry>>,
    planned: AtomicU64,
    reused: AtomicU64,
    declined: AtomicU64,
}

/// How the template cache answered requests whose query shape it considers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FederatedTemplateStats {
    /// Requests that planned their query shape (a template, or a shape found
    /// not to federate as one).
    pub planned: u64,
    /// Requests that bound their values to a template planned earlier.
    pub reused: u64,
    /// Requests whose shape was found earlier not to federate as a template,
    /// and were planned as before.
    pub declined: u64,
}

struct Entry {
    template: LogicalPlan,
    /// The upstream table each scan of `template` reads, in traversal order.
    /// `TableScan` equality ignores the provider, so two plans over the same
    /// table name from different upstreams, or as different principals, are
    /// told apart only by this.
    reads: Vec<SpiceSubstraitRead>,
    namespace: CacheNamespace,
    generation: u64,
    /// `None` when the template's physical plan is not a single parameterized
    /// Substrait leaf, so requests of this shape are planned as before.
    physical: Option<SubstraitTemplate>,
    weight: u32,
}

impl Entry {
    /// An estimate of the bytes the entry holds: its encoded Substrait, twice
    /// its logical plan's printed length (the template here and the optimized
    /// plan the Substrait leaf keeps for an upstream without parameterized
    /// plans), and 4 KiB for the physical plan and bookkeeping. A rough
    /// measure: it bounds the cache's growth, not its exact footprint.
    fn weight(template: &LogicalPlan, physical: Option<&SubstraitTemplate>) -> u32 {
        let printed = template.display_indent().to_string().len();
        let encoded = physical.map_or(0, SubstraitTemplate::encoded_len);
        u32::try_from(4096 + 2 * printed + encoded).unwrap_or(u32::MAX)
    }
}

impl Default for FederatedTemplates {
    fn default() -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .weigher(|_key, entry: &Arc<Entry>| entry.weight)
                .max_capacity(MAX_TEMPLATE_BYTES)
                .time_to_live(crate::init::caching::CACHED_PLANS_TTL)
                .build(),
            planned: AtomicU64::new(0),
            reused: AtomicU64::new(0),
            declined: AtomicU64::new(0),
        }
    }
}

impl FederatedTemplates {
    pub(crate) fn clear(&self) {
        self.cache.invalidate_all();
    }

    pub(crate) fn stats(&self) -> FederatedTemplateStats {
        FederatedTemplateStats {
            planned: self.planned.load(Ordering::Relaxed),
            reused: self.reused.load(Ordering::Relaxed),
            declined: self.declined.load(Ordering::Relaxed),
        }
    }
}

impl DataFusion {
    /// How the federated template cache has answered, for tests and diagnostics.
    #[must_use]
    pub fn federated_template_stats(&self) -> FederatedTemplateStats {
        self.federated_templates.stats()
    }

    /// The physical plan of `plan`, bound from a template planned for its query
    /// shape, when `plan` reads only tables federated to one upstream Spice
    /// runtime and has a literal to parameterize. `None` means `plan` is to be
    /// planned as it always was.
    pub(crate) async fn federated_template_plan(
        &self,
        session: &SessionState,
        request_context: &RequestContext,
        plan: &LogicalPlan,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // Templates are plans; a runtime without a plans cache plans each query.
        self.plans_cache_provider()?;
        // A stateful Flight session's plans stay off the shared caches.
        if super::query::owned_flight_session(request_context).is_some() {
            return None;
        }
        let reads = upstream_reads(plan)?;
        let (template, values) = match parameterize(plan) {
            Ok(Some(parameterized)) => parameterized,
            Ok(None) => return None,
            Err(error) => {
                tracing::debug!(%error, "Unable to parameterize a federated query");
                return None;
            }
        };

        let namespace = request_context.cache_namespace();
        let generation = session
            .config()
            .get_extension::<PlansGeneration>()
            .map(|generation| generation.0)?;
        let key = {
            let mut hasher = std::hash::DefaultHasher::new();
            namespace.hash(&mut hasher);
            reads.hash(&mut hasher);
            template.hash(&mut hasher);
            hasher.finish()
        };

        let templates = &self.federated_templates;
        let (physical, cached) = match templates.cache.get(&key) {
            Some(entry)
                if entry.generation == generation
                    && entry.namespace == namespace
                    && entry.reads == reads
                    && entry.template == template =>
            {
                (entry.physical.clone(), true)
            }
            _ => {
                templates.planned.fetch_add(1, Ordering::Relaxed);
                let physical = plan_template(session, &template, plan).await;
                // As for the plans cache: a template planned against a session
                // copied before a clear may hold a definition the clear retired.
                let current = self.plans_generation.read().await;
                if *current == generation {
                    let weight = Entry::weight(&template, physical.as_ref());
                    templates.cache.insert(
                        key,
                        Arc::new(Entry {
                            template,
                            reads,
                            namespace,
                            generation,
                            physical: physical.clone(),
                            weight,
                        }),
                    );
                }
                (physical, false)
            }
        };
        let Some(physical) = physical else {
            if cached {
                templates.declined.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        };

        match physical.bind(&values, session) {
            Ok(bound) => {
                if cached {
                    templates.reused.fetch_add(1, Ordering::Relaxed);
                }
                Some(bound)
            }
            Err(error) => {
                tracing::debug!(%error, "Unable to bind a federated query's values");
                None
            }
        }
    }
}

/// Plans `template` and returns its Substrait template, when its physical plan
/// is one, every parameter is bound at its literal's exact type, and its schema
/// is the request's.
async fn plan_template(
    session: &SessionState,
    template: &LogicalPlan,
    plan: &LogicalPlan,
) -> Option<SubstraitTemplate> {
    let physical = match session.create_physical_plan(template).await {
        Ok(physical) => physical,
        Err(error) => {
            tracing::debug!(%error, "A federated query shape does not plan as a template");
            return None;
        }
    };
    let substrait = SubstraitTemplate::of(&physical)?;
    let declared = template.get_parameter_fields().ok()?;
    for (number, data_type) in substrait.parameters() {
        let field = declared.get(&format!("${number}"))?.as_ref()?;
        if field.data_type() != data_type {
            return None;
        }
    }
    (physical.schema().fields() == plan.schema().as_arrow().fields()).then_some(substrait)
}

/// The upstream table each scan of `plan` reads, in traversal order, when
/// every table it reads, at least one, is federated through Substrait to the
/// same upstream as the same principal, and `plan` is a query whose planning
/// does not depend on when it is planned.
fn upstream_reads(plan: &LogicalPlan) -> Option<Vec<SpiceSubstraitRead>> {
    let mut reads: Vec<SpiceSubstraitRead> = Vec::new();
    let mut eligible = true;
    let walk = plan.apply_with_subqueries(|node| {
        let ok = match node {
            LogicalPlan::TableScan(scan) => match spice_substrait_read(&scan.source) {
                Some(read) => {
                    let same_upstream = reads
                        .first()
                        .is_none_or(|first| first.context == read.context);
                    reads.push(read);
                    same_upstream
                }
                None => false,
            },
            LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Extension(_) => false,
            _ => immutable_expressions(node),
        };
        eligible = ok;
        Ok(if ok {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    (walk.is_ok() && eligible && !reads.is_empty()).then_some(reads)
}

/// Whether every function in `node`'s expressions returns the same result for
/// the same arguments. The optimizer folds a stable function such as `now()`
/// into a constant, which a template would then keep for every request.
fn immutable_expressions(node: &LogicalPlan) -> bool {
    let mut immutable = true;
    let _ = node.apply_expressions(|expr| {
        expr.apply(|expr| {
            let volatility = match expr {
                Expr::ScalarFunction(function) => Some(function.func.signature().volatility),
                Expr::AggregateFunction(function) => Some(function.func.signature().volatility),
                Expr::WindowFunction(function) => Some(function.fun.signature().volatility),
                Expr::Placeholder(_) => Some(Volatility::Volatile),
                _ => None,
            };
            if volatility.is_some_and(|volatility| volatility != Volatility::Immutable) {
                immutable = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    });
    immutable
}

/// `plan` with the literal operands of its filters' comparisons replaced by
/// placeholders `$1, $2, …`, typed as the literals are, and the literals, the
/// value of `$N` at index `N - 1`. `None` when there is no literal to replace.
fn parameterize(plan: &LogicalPlan) -> Result<Option<(LogicalPlan, Vec<ScalarValue>)>> {
    let mut values = Vec::new();
    let template = plan.clone().transform_up(|node| {
        if !matches!(node, LogicalPlan::Filter(_)) {
            return Ok(Transformed::no(node));
        }
        let schema = Arc::clone(node.inputs()[0].schema());
        node.map_expressions(|predicate| {
            predicate.transform_up(|expr| Ok(parameterize_comparison(expr, &schema, &mut values)))
        })
    })?;
    if values.is_empty() {
        return Ok(None);
    }
    Ok(Some((template.data, values)))
}

fn parameterize_comparison(
    expr: Expr,
    schema: &DFSchema,
    values: &mut Vec<ScalarValue>,
) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            let (left, right) = match (*left, *right) {
                (column @ Expr::Column(_), Expr::Literal(value, None))
                    if comparable_without_coercion(&value, &column, schema) =>
                {
                    (column, placeholder(value, values))
                }
                (Expr::Literal(value, None), column @ Expr::Column(_))
                    if comparable_without_coercion(&value, &column, schema) =>
                {
                    (placeholder(value, values), column)
                }
                (left, right) => {
                    return Transformed::no(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(left),
                        op,
                        Box::new(right),
                    )));
                }
            };
            Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                op,
                Box::new(right),
            )))
        }
        Expr::InList(InList {
            expr: column,
            list,
            negated,
        }) if matches!(column.as_ref(), Expr::Column(_))
            && !list.is_empty()
            && list.iter().all(|item| {
                matches!(item, Expr::Literal(value, None)
                    if comparable_without_coercion(value, &column, schema))
            }) =>
        {
            let list = list
                .into_iter()
                .map(|item| match item {
                    Expr::Literal(value, _) => placeholder(value, values),
                    other => other,
                })
                .collect();
            Transformed::yes(Expr::InList(InList::new(column, list, negated)))
        }
        other => Transformed::no(other),
    }
}

fn placeholder(value: ScalarValue, values: &mut Vec<ScalarValue>) -> Expr {
    let field = Arc::new(Field::new("", value.data_type(), true));
    values.push(value);
    Expr::Placeholder(Placeholder::new_with_field(
        format!("${}", values.len()),
        Some(field),
    ))
}

/// Whether `value` compares with `column` with no cast of the literal that
/// could depend on its value, and is a non-null value of a type a Substrait
/// parameter binding carries exactly.
fn comparable_without_coercion(value: &ScalarValue, column: &Expr, schema: &DFSchema) -> bool {
    let Ok(column_type) = column.get_type(schema) else {
        return false;
    };
    match value {
        ScalarValue::Utf8(Some(_)) => matches!(
            column_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        ScalarValue::Int64(Some(_)) => matches!(
            column_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ),
        ScalarValue::Boolean(Some(_)) => column_type == DataType::Boolean,
        _ => false,
    }
}
