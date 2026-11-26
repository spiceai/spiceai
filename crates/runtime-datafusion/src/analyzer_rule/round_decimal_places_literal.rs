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

//! Materializes `round`'s `decimal_places` argument to an `Int32` literal
//! before type coercion, so analysis and execution agree on the return type.
//!
//! SQL integer literals are `Int64`. `round`'s signature wants `Int32`, and
//! `DataFusion` type coercion wraps the literal as `CAST(Int64(n) AS Int32)`.
//! `round` only folds `decimal_places` into the decimal return type when that
//! argument is a literal (`ReturnFieldArgs::scalar_arguments`). A `Cast` is
//! not a literal, so analysis reports `Decimal(p+1, s)` while the simplifier
//! later folds the cast and execution produces `Decimal(p, decimal_places)`.
//!
//! That split is user-visible:
//!
//! - Flight `GetFlightInfo` vs `DoGet` disagree (#15652).
//! - `COALESCE(round(decimal, n), 0)` fails because `CASE` cannot unify
//!   `Decimal(p, s)` with `Decimal(p+1, s)` (#15708).
//!
//! Trunk carries the general fix as spiceai/datafusion#273 (a backport of
//! apache/datafusion#23232). This rule is the `round`-specific equivalent for
//! `spiceai-55-release-2.4`, which does not include that commit. It must run
//! *before* type coercion: `COALESCE` fails during that pass, so a later
//! rewrite never sees the plan.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::functions::math::round::RoundFunc;
use datafusion::logical_expr::expr::{Alias, Cast, ScalarFunction, TryCast};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

/// Rewrites `round(value, <integer literal>)` so `decimal_places` is an
/// `Int32` literal. See the module docs.
#[derive(Debug, Default)]
pub struct RoundDecimalPlacesLiteral;

impl RoundDecimalPlacesLiteral {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for RoundDecimalPlacesLiteral {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            plan.map_expressions(|expr| expr.transform_up(|expr| Ok(rewrite_expr(expr))))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "round_decimal_places_literal"
    }
}

fn rewrite_expr(expr: Expr) -> Transformed<Expr> {
    if !matches!(
        &expr,
        Expr::ScalarFunction(call)
            if is_datafusion_round(call)
                && call.args.len() == 2
                && int32_literal(&call.args[1]).is_some_and(|lit| lit != call.args[1])
    ) {
        return Transformed::no(expr);
    }

    // Keep the unanalyzed field name (`Int64(2)` in `round(..., 2)`).
    // `Query::run` checks the unoptimized logical schema against the
    // stream; changing the display name would 400 unaliased projections.
    let original_name = expr.schema_name().to_string();
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Transformed::no(expr);
    };
    if let Some(decimal_places) = int32_literal(&args[1]) {
        args[1] = decimal_places;
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction { func, args }).alias(original_name))
}

fn is_datafusion_round(call: &ScalarFunction) -> bool {
    call.func.inner().downcast_ref::<RoundFunc>().is_some()
}

/// An `Int32` literal equivalent to `expr` when `expr` is an integer literal,
/// possibly wrapped in `CAST` / `TRY_CAST` / alias. `None` when the argument
/// is not a foldable integer (a column, `1 + 1`, a non-integer literal).
fn int32_literal(expr: &Expr) -> Option<Expr> {
    let (value, metadata) = peel_literal(expr)?;
    if !is_integer_scalar(&value) {
        return None;
    }
    let casted = value.cast_to(&DataType::Int32).ok()?;
    matches!(casted, ScalarValue::Int32(_)).then_some(Expr::Literal(casted, metadata))
}

fn peel_literal(expr: &Expr) -> Option<(ScalarValue, Option<FieldMetadata>)> {
    match expr {
        Expr::Literal(value, metadata) => Some((value.clone(), metadata.clone())),
        Expr::Cast(Cast { expr, .. })
        | Expr::TryCast(TryCast { expr, .. })
        | Expr::Alias(Alias { expr, .. }) => peel_literal(expr),
        _ => None,
    }
}

fn is_integer_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Null
    )
}

#[cfg(test)]
mod tests {
    use super::int32_literal;
    use crate::analyzer_rule::AnalyzerRulesBuilder;
    use datafusion::arrow::datatypes::{DataType, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::ScalarValue;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::expr::Cast;
    use datafusion::logical_expr::{Expr, LogicalPlan};
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::prelude::{SessionContext, col, lit};
    use std::sync::Arc;

    const NULL_DECIMAL_COALESCE: &str =
        "SELECT coalesce(round(x,2),0) AS v FROM (VALUES (CAST(NULL AS DECIMAL(25,2)))) t(x)";
    const EMPTY_SUM_COALESCE: &str = "SELECT coalesce(round(sum(x),2),0) FROM (VALUES (CAST(1.5 AS DECIMAL(15,2)))) t(x) WHERE x < 0";
    const SCALAR_NULL_COALESCE: &str = "SELECT coalesce(round(CAST(NULL AS DECIMAL(25,2)),2),0)";

    fn ctx_with(rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        SessionContext::new_with_state(state)
    }

    fn spice_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        AnalyzerRulesBuilder::new()
            .include_federation(false)
            .build()
    }

    fn df_default_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        vec![
            Arc::new(ResolveGroupingFunction::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
            Arc::new(TypeCoercion::new()),
        ]
    }

    fn analyze(
        ctx: &SessionContext,
        plan: LogicalPlan,
    ) -> std::result::Result<LogicalPlan, String> {
        let state = ctx.state();
        state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .map_err(|error| error.to_string())
    }

    async fn collect_sql(
        ctx: &SessionContext,
        sql: &str,
    ) -> std::result::Result<(Schema, String), String> {
        let df = ctx.sql(sql).await.map_err(|error| error.to_string())?;
        let analyzed = analyze(ctx, df.logical_plan().clone())?;
        let planned = analyzed.schema().as_arrow().clone();
        let batches = df.collect().await.map_err(|error| error.to_string())?;
        assert!(
            !batches.is_empty(),
            "expected a result batch for `{sql}` so the schema check is not vacuous"
        );
        let streamed = batches[0].schema();
        let planned_types: Vec<_> = planned
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let streamed_types: Vec<_> = streamed
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        assert_eq!(
            planned_types, streamed_types,
            "planned schema must match the streamed schema for `{sql}`:\nplanned={planned:?}\nstreamed={streamed:?}"
        );
        let pretty = pretty_format_batches(&batches)
            .map_err(|error| error.to_string())?
            .to_string();
        Ok((planned, pretty))
    }

    #[test]
    fn spice_analyzer_rules_materialize_round_literals_before_type_coercion() {
        let rules = AnalyzerRulesBuilder::new()
            .include_federation(false)
            .build();
        let names: Vec<_> = rules.iter().map(|rule| rule.name()).collect();
        let round_at = names
            .iter()
            .position(|name| *name == "round_decimal_places_literal")
            .expect("round_decimal_places_literal is registered");
        let coercion_at = names
            .iter()
            .position(|name| *name == "type_coercion")
            .expect("type_coercion is registered");
        assert!(
            round_at < coercion_at,
            "round must run before type_coercion so COALESCE sees one decimal type: {names:?}"
        );
    }

    #[test]
    fn int32_literal_folds_integer_casts() {
        assert_eq!(
            int32_literal(&lit(2_i64)),
            Some(Expr::Literal(ScalarValue::Int32(Some(2)), None))
        );
        let cast = Expr::Cast(Cast::new(Box::new(lit(2_i64)), DataType::Int64));
        assert_eq!(
            int32_literal(&cast),
            Some(Expr::Literal(ScalarValue::Int32(Some(2)), None))
        );
        assert_eq!(int32_literal(&col("x")), None);
    }

    /// Regression for #15652 / #15708: without this rule, `DataFusion` type
    /// coercion still wraps `2` as `CAST(Int64(2) AS Int32)`, so `COALESCE`
    /// sees two decimal precisions.
    #[tokio::test]
    async fn coalesce_round_decimal_fails_on_datafusion_type_coercion_alone() {
        let ctx = ctx_with(df_default_rules());
        for sql in [
            NULL_DECIMAL_COALESCE,
            EMPTY_SUM_COALESCE,
            SCALAR_NULL_COALESCE,
        ] {
            let error = collect_sql(&ctx, sql)
                .await
                .expect_err("COALESCE(ROUND(decimal)) must fail without the rule");
            assert!(
                error.contains("column types must match schema types")
                    || error.contains("multiple data types"),
                "expected a decimal precision mismatch, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn coalesce_round_decimal_returns_zero_with_spice_analyzer_rules() {
        let ctx = ctx_with(spice_rules());
        for sql in [
            NULL_DECIMAL_COALESCE,
            EMPTY_SUM_COALESCE,
            SCALAR_NULL_COALESCE,
        ] {
            let (schema, pretty) = collect_sql(&ctx, sql)
                .await
                .unwrap_or_else(|error| panic!("`{sql}` should succeed: {error}"));
            assert!(
                pretty.contains("0.00"),
                "`{sql}` should return 0.00, got:\n{pretty}"
            );
            let data_type = schema.field(0).data_type();
            assert!(
                matches!(data_type, DataType::Decimal128(_, 2)),
                "`{sql}` result type should be Decimal128(_, 2), got {data_type:?}"
            );
        }
    }

    /// Unaliased `COALESCE(round(...), 0)` keeps the unanalyzed field name so
    /// `Query::run`'s schema check (unoptimized plan vs stream) still matches.
    #[tokio::test]
    async fn unaliased_coalesce_round_keeps_unanalyzed_field_names() {
        let ctx = ctx_with(spice_rules());
        for sql in [EMPTY_SUM_COALESCE, SCALAR_NULL_COALESCE] {
            let df = ctx.sql(sql).await.expect("plan");
            let unanalyzed_name = df.schema().field(0).name().clone();
            let batches = df.collect().await.expect("collect");
            assert!(
                !batches.is_empty(),
                "expected a result batch for `{sql}` so the name check is not vacuous"
            );
            assert_eq!(
                batches[0].schema().field(0).name(),
                unanalyzed_name.as_str(),
                "stream field name must match the unanalyzed plan for `{sql}`"
            );
        }
    }

    #[tokio::test]
    async fn round_decimal_places_is_an_int32_literal_in_the_analyzed_plan() {
        let ctx = ctx_with(spice_rules());
        let df = ctx
            .sql("SELECT round(x, 2) AS v FROM (VALUES (CAST(1.255 AS DECIMAL(10,3)))) t(x)")
            .await
            .expect("plan");
        let plan = analyze(&ctx, df.logical_plan().clone()).expect("analyze");
        let display = format!("{}", plan.display_indent());
        assert!(
            display.contains("Int32(2)"),
            "decimal_places should be an Int32 literal:\n{display}"
        );
        assert!(
            !display.contains("CAST(Int64(2)"),
            "decimal_places must not remain a CAST of an Int64 literal:\n{display}"
        );
    }

    #[tokio::test]
    async fn round_decimal_return_type_uses_the_literal_scale() {
        let ctx = ctx_with(spice_rules());
        let (schema, pretty) =
            collect_sql(&ctx, "SELECT round(CAST(2.345 AS DECIMAL(10,3)), 2) AS x")
                .await
                .expect("round of a decimal literal should succeed");
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(10, 2));
        assert!(
            pretty.contains("2.35"),
            "round(2.345, 2) should be 2.35, got:\n{pretty}"
        );
    }

    /// An explicit `CAST(2 AS BIGINT)` is still a literal 2. Peeling it here
    /// also covers the `CAST` form of #15708, which spiceai/datafusion#273
    /// leaves as a user-written cast.
    #[tokio::test]
    async fn coalesce_round_accepts_a_cast_integer_literal() {
        let ctx = ctx_with(spice_rules());
        let (_schema, pretty) = collect_sql(
            &ctx,
            "SELECT coalesce(round(CAST(NULL AS DECIMAL(25,2)), CAST(2 AS BIGINT)), 0)",
        )
        .await
        .expect("CAST of an integer literal should fold like a bare 2");
        assert!(pretty.contains("0.00"), "expected 0.00, got:\n{pretty}");
    }
}
