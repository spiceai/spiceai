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

//! Rewrites the NULL-check idiom over `regexp_match` into `regexp_like`.
//!
//! `regexp_match(x, p) IS NOT NULL` is the PostgreSQL-idiom spelling of "does
//! `p` match anywhere in `x`": the function returns the first match's capture
//! groups, and NULL when there is no match or no input. When only the
//! NULL-ness is consumed, computing the match list is wasted work locally and
//! unfederatable remotely — `BigQuery` has no `regexp_match` at all, and its
//! federation deny-list keeps the call local (see
//! [`crate::function_support::deny_spice_functions_for_bigquery_table_providers`]).
//! `regexp_like` answers the same question as a plain boolean, which the local
//! engine evaluates without building lists and the unparser dialects can
//! render natively (`BigQuery`: `REGEXP_CONTAINS`; `DuckDB`:
//! `regexp_matches`).

use datafusion::common::Result;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::functions::regex;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDFImpl};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Rewrites `regexp_match(…) IS [NOT] NULL` into the equivalent
/// `regexp_like(…) IS [NOT] TRUE`.
///
/// The `IS TRUE` wrapping is what makes the rewrite exact rather than merely
/// filter-equivalent: `IS [NOT] NULL` never returns NULL, while a bare
/// `regexp_like` is NULL whenever the string or the pattern is. The full
/// agreement, held by
/// [`tests::the_rewrite_agrees_with_the_engine_on_every_input`] against a real
/// engine run:
///
/// | input               | `match IS NOT NULL` | `like IS TRUE` |
/// |---------------------|---------------------|----------------|
/// | matches             | true                | true           |
/// | does not match      | false               | false          |
/// | string/pattern NULL | false               | false          |
///
/// The rule is provider-independent. The `BigQuery` federation provider runs
/// it before its capability check. The ordinary logical optimizer runs it
/// after federation analysis, where federated subplans are opaque leaves, so
/// the same rule also optimizes every expression that remains local.
#[derive(Default)]
pub struct RegexpMatchNullCheckRewrite;

impl RegexpMatchNullCheckRewrite {
    /// Create the rewrite rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    fn plan_has_rewritable_expression(plan: &LogicalPlan) -> Result<bool> {
        for expr in plan.expressions() {
            let mut found = false;
            expr.apply(|expr| {
                if matches!(expr, Expr::IsNull(inner) | Expr::IsNotNull(inner)
                    if regexp_match_call(inner).is_some())
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if found {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl std::fmt::Debug for RegexpMatchNullCheckRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexpMatchNullCheckRewrite").finish()
    }
}

impl OptimizerRule for RegexpMatchNullCheckRewrite {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            // Make the overwhelmingly common path cheap: do not clone or
            // rebuild the plan unless this node actually contains the exact
            // regexp NULL-check shape.
            if !Self::plan_has_rewritable_expression(&plan)? {
                return Ok(Transformed::no(plan));
            }

            // A rewritten projection expression renders differently, which
            // would rename its output column; the saved name pins the schema.
            let name_preserver = NamePreserver::new(&plan);
            plan.map_expressions(|expr| {
                let saved_name = name_preserver.save(&expr);
                expr.transform_up(|expr| Ok(rewrite_null_check(expr)))
                    .map(|transformed| transformed.update_data(|expr| saved_name.restore(expr)))
            })
        })
    }

    fn name(&self) -> &'static str {
        "regexp_match_null_check_rewrite"
    }
}

fn rewrite_null_check(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::IsNotNull(inner) => match regexp_like_of(&inner) {
            Some(matches) => Transformed::yes(Expr::IsTrue(Box::new(matches))),
            None => Transformed::no(Expr::IsNotNull(inner)),
        },
        Expr::IsNull(inner) => match regexp_like_of(&inner) {
            Some(matches) => Transformed::yes(Expr::IsNotTrue(Box::new(matches))),
            None => Transformed::no(Expr::IsNull(inner)),
        },
        // The arms above, under a user-written NOT: `NOT regexp_match(…) IS
        // NULL` arrives as Not(IsNull(…)), whose child has already become
        // IsNotTrue(regexp_like(…)) by the time this node is visited
        // (transform_up runs bottom-up). `NOT (x IS NOT TRUE)` is `x IS TRUE`
        // for all of true, false and NULL, and collapsing it keeps the double
        // negation out of the federated SQL. Scoped to `regexp_like` operands
        // so this stays a regexp rewrite, not a general boolean simplifier.
        Expr::Not(inner) => match *inner {
            Expr::IsNotTrue(matches) if is_regexp_like(&matches) => {
                Transformed::yes(Expr::IsTrue(matches))
            }
            Expr::IsTrue(matches) if is_regexp_like(&matches) => {
                Transformed::yes(Expr::IsNotTrue(matches))
            }
            other => Transformed::no(Expr::Not(Box::new(other))),
        },
        other => Transformed::no(other),
    }
}

/// Whether this is a call to `DataFusion`'s `regexp_like` — the only operand
/// the collapse above may have created.
fn is_regexp_like(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call)
        if is_datafusion_udf::<regex::regexplike::RegexpLikeFunc>(call))
}

/// The `regexp_like` call answering whether this `regexp_match` call matches
/// at all, carrying the same arguments — the two share the
/// `(str, pattern[, flags])` signature and the same flag semantics.
///
/// The call is recognized by its implementation *type*, not by name or by Arc
/// identity: a user-registered function that happens to be called
/// `regexp_match` has its own semantics, which this rewrite's equivalence does
/// not cover — and pointer identity misses calls built through `expr_fn`,
/// whose `ScalarUDF::call` wraps a fresh clone of the UDF rather than the
/// registry's singleton.
fn regexp_like_of(expr: &Expr) -> Option<Expr> {
    let call = regexp_match_call(expr)?;
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        regex::regexp_like(),
        call.args.clone(),
    )))
}

fn regexp_match_call(expr: &Expr) -> Option<&ScalarFunction> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    (is_datafusion_udf::<regex::regexpmatch::RegexpMatchFunc>(call)
        && (2..=3).contains(&call.args.len()))
    .then_some(call)
}

/// Whether this call's implementation is `DataFusion`'s own `F`. See
/// [`regexp_like_of`] for why the type, not the name or the Arc, is the key.
fn is_datafusion_udf<F: ScalarUDFImpl>(call: &ScalarFunction) -> bool {
    call.func.inner().downcast_ref::<F>().is_some()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::catalog::MemTable;
    use datafusion::execution::context::SessionState;
    use datafusion::logical_expr::{
        ColumnarValue, Extension, LogicalPlan, LogicalPlanBuilder, TableSource, Volatility,
        builder::LogicalTableSource, create_udf, expr::ScalarFunction,
    };
    use datafusion::optimizer::OptimizerContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionContext, col, lit};
    use datafusion_federation::{FederatedPlanNode, FederationPlanner};

    use super::*;

    fn scan() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let source = Arc::new(LogicalTableSource::new(schema)) as Arc<dyn TableSource>;
        LogicalPlanBuilder::scan("t", source, None).expect("scan the test table")
    }

    fn regexp_match_call() -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            regex::regexp_match(),
            vec![col("v"), lit("^R[0-9]{2}")],
        ))
    }

    fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        RegexpMatchNullCheckRewrite::new()
            .rewrite(plan, &OptimizerContext::new())
            .expect("the rule rewrites the plan")
            .data
    }

    fn null_check_plan() -> LogicalPlan {
        scan()
            .filter(Expr::IsNotNull(Box::new(regexp_match_call())))
            .expect("filter")
            .build()
            .expect("build")
    }

    #[test]
    fn is_not_null_becomes_regexp_like_is_true() {
        let plan = rewrite(null_check_plan());
        let rendered = plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_like(t.v, Utf8(\"^R[0-9]{2}\")) IS TRUE"),
            "the NULL-check must become a boolean regexp_like: {rendered}"
        );
        assert!(
            !rendered.contains("regexp_match"),
            "no regexp_match may remain in the rewritten shape: {rendered}"
        );
    }

    #[derive(Debug)]
    struct UnusedFederationPlanner;

    #[async_trait::async_trait]
    impl FederationPlanner for UnusedFederationPlanner {
        async fn plan_federation(
            &self,
            _node: &FederatedPlanNode,
            _session_state: &SessionState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unreachable!("logical rewrite tests never physically plan the federated node")
        }
    }

    #[test]
    fn a_federated_subplan_is_an_opaque_leaf() {
        let federated = LogicalPlan::Extension(Extension {
            node: Arc::new(FederatedPlanNode::new(
                null_check_plan(),
                Arc::new(UnusedFederationPlanner),
            )),
        });

        let rewritten = rewrite(federated);
        let LogicalPlan::Extension(Extension { node }) = rewritten else {
            panic!("the federated extension node must remain in the plan");
        };
        let federated = node
            .as_any()
            .downcast_ref::<FederatedPlanNode>()
            .expect("the extension must remain a federated node");
        let inner = federated.plan().display_indent().to_string();
        assert!(
            inner.contains("regexp_match") && !inner.contains("regexp_like"),
            "the ordinary optimizer must not rewrite inside a federated subplan: {inner}"
        );
    }

    #[test]
    fn is_null_becomes_regexp_like_is_not_true() {
        let plan = rewrite(
            scan()
                .filter(Expr::IsNull(Box::new(regexp_match_call())))
                .expect("filter")
                .build()
                .expect("build"),
        );
        let rendered = plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_like(t.v, Utf8(\"^R[0-9]{2}\")) IS NOT TRUE"),
            "IS NULL must become IS NOT TRUE, which is what stays true on NULL input: {rendered}"
        );
    }

    #[test]
    fn not_is_null_collapses_to_is_true() {
        // `NOT regexp_match(…) IS NULL` — the customer-idiom spelling — must
        // come out as a single `IS TRUE`, not as `NOT (… IS NOT TRUE)`: both
        // are correct, but the double negation is what every reader of the
        // federated SQL would otherwise puzzle over.
        let plan = rewrite(
            scan()
                .filter(Expr::Not(Box::new(Expr::IsNull(Box::new(
                    regexp_match_call(),
                )))))
                .expect("filter")
                .build()
                .expect("build"),
        );
        let rendered = plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_like(t.v, Utf8(\"^R[0-9]{2}\")) IS TRUE"),
            "the NOT must collapse into the IS TRUE form: {rendered}"
        );
        assert!(
            !rendered.contains("IS NOT TRUE") && !rendered.contains("NOT regexp_like"),
            "no double negation may survive the collapse: {rendered}"
        );
    }

    #[test]
    fn a_bare_regexp_match_is_left_alone() {
        // Only the NULL-check idiom has a boolean equivalent. A consumed match
        // list must keep its list semantics.
        let plan = rewrite(
            scan()
                .project(vec![regexp_match_call()])
                .expect("project")
                .build()
                .expect("build"),
        );
        let rendered = plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_match") && !rendered.contains("regexp_like"),
            "a projected match list is not a NULL-check and must not be rewritten: {rendered}"
        );
    }

    #[test]
    fn a_user_function_named_regexp_match_is_left_alone() {
        // The rewrite's equivalence is DataFusion's regexp_match's, not that of
        // whatever a user registered under the same name.
        let imposter = Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(create_udf(
                "regexp_match",
                vec![DataType::Utf8, DataType::Utf8],
                DataType::Utf8,
                Volatility::Immutable,
                Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
            )),
            vec![col("v"), lit("^R[0-9]{2}")],
        ));
        let plan = rewrite(
            scan()
                .filter(Expr::IsNotNull(Box::new(imposter)))
                .expect("filter")
                .build()
                .expect("build"),
        );
        let rendered = plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_match") && !rendered.contains("regexp_like"),
            "a same-named user function has its own semantics and must not be rewritten: {rendered}"
        );
    }

    #[test]
    fn a_null_check_of_something_else_is_left_alone() {
        let plan = rewrite(
            scan()
                .filter(Expr::IsNotNull(Box::new(col("v"))))
                .expect("filter")
                .build()
                .expect("build"),
        );
        assert!(
            plan.display_indent()
                .to_string()
                .contains("t.v IS NOT NULL"),
            "an unrelated NULL-check must pass through untouched"
        );
    }

    #[test]
    fn the_rewritten_projection_keeps_its_column_name() {
        // The rewrite changes how the expression renders, and an expression's
        // rendering is its output column name. A renamed column is a changed
        // schema, which downstream plans and users would both see.
        let original = scan()
            .project(vec![Expr::IsNotNull(Box::new(regexp_match_call()))])
            .expect("project")
            .build()
            .expect("build");
        let original_name = original.schema().field(0).name().clone();
        let rewritten = rewrite(original);
        assert_eq!(
            rewritten.schema().field(0).name(),
            &original_name,
            "the rewrite must not rename the projected column"
        );
    }

    /// The equivalence claim, held by the engine itself rather than by a
    /// reading of the kernels: both spellings of every shape run through a
    /// real `DataFusion` session over the inputs where they could diverge —
    /// NULL input, empty string, no match, match, a NULL pattern, and a
    /// case-insensitivity flag — and must return identical rows under
    /// identical column names.
    #[tokio::test]
    async fn the_rewrite_agrees_with_the_engine_on_every_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some(""),
                    Some("R01x"),
                    Some("X99"),
                    Some("r05b"),
                    Some("zzR03"),
                ])),
            ],
        )
        .expect("build the test batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("build the mem table");

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table))
            .expect("register the test table");

        let sql = "SELECT v, \
             regexp_match(v, '^R[0-9]{2}') IS NOT NULL AS is_not_null, \
             regexp_match(v, '^R[0-9]{2}') IS NULL AS is_null, \
             NOT regexp_match(v, '^R[0-9]{2}') IS NULL AS not_is_null, \
             regexp_match(v, '^r[0-9]{2}', 'i') IS NOT NULL AS flagged, \
             regexp_match(v, NULL) IS NULL AS null_pattern \
             FROM t ORDER BY id";
        let plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan the query");

        let rewritten_plan = rewrite(plan.clone());
        let rendered = rewritten_plan.display_indent().to_string();
        assert!(
            rendered.contains("regexp_like") && !rendered.contains("regexp_match"),
            "every NULL-check in the query must have been rewritten: {rendered}"
        );

        let baseline = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute the original plan")
            .collect()
            .await
            .expect("collect the original rows");
        let rewritten = ctx
            .execute_logical_plan(rewritten_plan)
            .await
            .expect("execute the rewritten plan")
            .collect()
            .await
            .expect("collect the rewritten rows");

        assert_eq!(
            pretty_format_batches(&baseline)
                .expect("format the original rows")
                .to_string(),
            pretty_format_batches(&rewritten)
                .expect("format the rewritten rows")
                .to_string(),
            "the rewrite must agree with the original on every row and column name"
        );
    }
}
