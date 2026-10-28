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

//! The [`SubstraitConsumer`] that lowers `FlightSQL` Substrait plans.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, Result, ScalarValue, TableReference, substrait_err};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::expr::{Cast, Like, ScalarFunction as ScalarFunctionExpr, TryCast};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitLambdaConsumer, SubstraitConsumer, from_scalar_function,
    from_substrait_func_args, name_to_op,
};
use datafusion_substrait::substrait::proto::expression::ScalarFunction;
use datafusion_substrait::substrait::proto::{
    ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Type,
};
use parking_lot::RwLock;

/// Lowers Substrait plans the way `datafusion-substrait`'s
/// `DefaultSubstraitConsumer` does. Spice operator declarations bind to SQL
/// syntax independently of upstream UDF names, and `like` / `ilike` lower to
/// [`Expr::Like`]. Ordinary external function declarations retain the default
/// registry lookup.
///
/// `datafusion-substrait` resolves a function to a session UDF of the same name
/// before its built-in expressions, and the session registers
/// `datafusion-spark`'s `like` and `ilike`. Left alone, a Substrait `like`
/// therefore lowers to a `like(..)` function call, which the unparser sends
/// verbatim to a federated or accelerated source, and `DuckDB` rejects it
/// ("Scalar Function with name like does not exist"). Both Spark functions
/// evaluate with the Arrow `like` / `ilike` kernels that [`Expr::Like`] uses, so
/// the rewrite keeps the result and lets the unparser emit `LIKE` / `ILIKE`.
pub(crate) struct SpiceSubstraitConsumer<'a> {
    extensions: &'a Extensions,
    state: &'a SessionState,
    outer_schemas: RwLock<Vec<Arc<DFSchema>>>,
    lambda_consumer: DefaultSubstraitLambdaConsumer,
}

impl<'a> SpiceSubstraitConsumer<'a> {
    pub(crate) fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        Self {
            extensions,
            state,
            outer_schemas: RwLock::new(Vec::new()),
            lambda_consumer: DefaultSubstraitLambdaConsumer::new(),
        }
    }
}

/// Evaluates a cast of a literal (at any depth) with `DataFusion`'s own cast, so
/// the pattern reaches a federated or accelerated source as the value
/// `DataFusion` would compare against. Left as a cast, the unparser can render
/// the literal in a form the source reads differently: `DuckDB` takes
/// `CAST(X'35305c25' AS VARCHAR)` as the text `x35305c25`, not `50\%`.
fn fold_literal_casts(pattern: Expr) -> Result<Expr> {
    match pattern {
        Expr::Cast(Cast { expr, field }) => match fold_literal_casts(*expr)? {
            Expr::Literal(value, metadata) => {
                Ok(Expr::Literal(value.cast_to(field.data_type())?, metadata))
            }
            expr => Ok(Expr::Cast(Cast {
                expr: Box::new(expr),
                field,
            })),
        },
        Expr::TryCast(TryCast { expr, field }) => match fold_literal_casts(*expr)? {
            Expr::Literal(value, metadata) => {
                let value = match value.cast_to(field.data_type()) {
                    Ok(value) => value,
                    Err(_) => ScalarValue::try_from(field.data_type())?,
                };
                Ok(Expr::Literal(value, metadata))
            }
            expr => Ok(Expr::TryCast(TryCast {
                expr: Box::new(expr),
                field,
            })),
        },
        pattern => Ok(pattern),
    }
}

/// Whether a `LIKE` pattern can contain a backslash. Only a string literal,
/// seen through casts that keep its text, is known not to; a `NULL` pattern
/// matches nothing either way. Any other literal can render as text with a
/// backslash once cast (bytes `35 30 5c 25` read as `50\%`), and any other
/// expression is unknown until it runs.
fn pattern_may_contain_backslash(pattern: &Expr) -> bool {
    match pattern {
        Expr::Literal(
            ScalarValue::Utf8(Some(p))
            | ScalarValue::LargeUtf8(Some(p))
            | ScalarValue::Utf8View(Some(p)),
            _,
        ) => p.contains('\\'),
        Expr::Literal(value, _) => !value.is_null(),
        Expr::Cast(cast) => pattern_may_contain_backslash(&cast.expr),
        Expr::TryCast(cast) => pattern_may_contain_backslash(&cast.expr),
        _ => true,
    }
}

/// Rewrites a call to the Spark `like` / `ilike` function into [`Expr::Like`],
/// taking an optional third argument as the escape character the way
/// `datafusion-substrait` does. Any other expression is returned unchanged.
///
/// With no escape character, `DataFusion` and the Spark functions escape with a
/// backslash, while `DuckDB` and `SQLite` have no default escape, so a pushed-down
/// `a LIKE '50\%'` would match different rows there. A pattern that can carry a
/// backslash therefore names it (`ESCAPE '\'`), which `DataFusion` evaluates
/// exactly as no escape. Any other pattern keeps the plain `LIKE` every engine
/// reads the same way.
fn spark_like_to_like_expr(expr: Expr) -> Result<Expr> {
    let Expr::ScalarFunction(ScalarFunctionExpr { func, args }) = expr else {
        return Ok(expr);
    };
    if !matches!(func.name(), "like" | "ilike") || (args.len() != 2 && args.len() != 3) {
        return Ok(Expr::ScalarFunction(ScalarFunctionExpr { func, args }));
    }
    substrait_like_expr(func.name(), args)
}

fn substrait_like_expr(function_name: &str, args: Vec<Expr>) -> Result<Expr> {
    let case_insensitive = function_name == "ilike";
    let mut args = args.into_iter();
    let (Some(expr), Some(pattern)) = (args.next(), args.next()) else {
        return substrait_err!("Expected two or three arguments for `{}`", function_name);
    };
    let pattern = fold_literal_casts(pattern)?;
    let escape_char = match args.next() {
        None => None,
        Some(Expr::Literal(ScalarValue::Utf8(escape), _)) => escape.and_then(|s| s.chars().next()),
        Some(other) => {
            return substrait_err!(
                "Expected a Utf8 literal for the `{}` escape character, but found {other:?}",
                function_name
            );
        }
    }
    .or_else(|| pattern_may_contain_backslash(&pattern).then_some('\\'));

    Ok(Expr::Like(Like::new(
        false,
        Box::new(expr),
        Box::new(pattern),
        escape_char,
        case_insensitive,
    )))
}

/// These declarations describe SQL syntax bound by the sidecar, rather than a
/// function lookup in the receiving session's user-function registry.
fn lower_spice_operator(name: &str, mut arguments: Vec<Expr>) -> Result<Expr> {
    if let Some(operator) = name_to_op(name) {
        let mut arguments = arguments.into_iter();
        let (Some(left), Some(right), None) =
            (arguments.next(), arguments.next(), arguments.next())
        else {
            return substrait_err!(
                "Spice Substrait operator {name} requires exactly two arguments"
            );
        };
        return Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(left),
                operator,
                Box::new(right),
            ),
        ));
    }
    if matches!(name, "like" | "ilike") {
        if arguments.len() != 2 && arguments.len() != 3 {
            return substrait_err!(
                "Spice Substrait operator {name} requires two or three arguments"
            );
        }
        return substrait_like_expr(name, arguments);
    }
    if arguments.len() != 1 {
        return substrait_err!("Spice Substrait operator {name} requires exactly one argument");
    }
    let argument = Box::new(arguments.remove(0));
    Ok(match name {
        "not" => Expr::Not(argument),
        "is_null" => Expr::IsNull(argument),
        "is_not_null" => Expr::IsNotNull(argument),
        "is_true" => Expr::IsTrue(argument),
        "is_false" => Expr::IsFalse(argument),
        "is_unknown" => Expr::IsUnknown(argument),
        "is_not_true" => Expr::IsNotTrue(argument),
        "is_not_false" => Expr::IsNotFalse(argument),
        "is_not_unknown" => Expr::IsNotUnknown(argument),
        "negate" => Expr::Negative(argument),
        _ => {
            return Err(datafusion::common::DataFusionError::NotImplemented(
                format!("Unsupported Spice Substrait operator: {name}"),
            ));
        }
    })
}

#[async_trait]
impl SubstraitConsumer for SpiceSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table = table_ref.table().to_string();
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        schema.table(&table).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }

    async fn consume_scalar_function(
        &self,
        expr: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        if let Some(operator) = self
            .extensions
            .functions
            .get(&expr.function_reference)
            .and_then(|name| name.strip_prefix("spice.operator."))
        {
            let arguments = from_substrait_func_args(self, &expr.arguments, input_schema).await?;
            return lower_spice_operator(operator, arguments);
        }
        spark_like_to_like_expr(from_scalar_function(self, expr, input_schema).await?)
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.outer_schemas.write().push(schema);
    }

    fn pop_outer_schema(&self) {
        self.outer_schemas.write().pop();
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        let schemas = self.outer_schemas.read();
        // steps_out = 1 is the innermost enclosing schema; 0 and anything deeper
        // than the stack have none.
        schemas
            .len()
            .checked_sub(steps_out)
            .and_then(|idx| schemas.get(idx).cloned())
    }

    async fn consume_extension_leaf(&self, rel: &ExtensionLeafRel) -> Result<LogicalPlan> {
        let Some(detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionLeafRel");
        };
        let node = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&detail.type_url, &detail.value)?;
        Ok(LogicalPlan::Extension(Extension { node }))
    }

    async fn consume_extension_single(&self, rel: &ExtensionSingleRel) -> Result<LogicalPlan> {
        let Some(detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
        };
        let node = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&detail.type_url, &detail.value)?;
        let Some(input) = &rel.input else {
            return substrait_err!(
                "ExtensionSingleRel missing input rel, try using ExtensionLeafRel instead"
            );
        };
        let input = self.consume_rel(input).await?;
        let node = node.with_exprs_and_inputs(node.expressions(), vec![input])?;
        Ok(LogicalPlan::Extension(Extension { node }))
    }

    async fn consume_extension_multi(&self, rel: &ExtensionMultiRel) -> Result<LogicalPlan> {
        let Some(detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionMultiRel");
        };
        let node = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&detail.type_url, &detail.value)?;
        let mut inputs = Vec::with_capacity(rel.inputs.len());
        for input in &rel.inputs {
            inputs.push(self.consume_rel(input).await?);
        }
        let node = node.with_exprs_and_inputs(node.expressions(), inputs)?;
        Ok(LogicalPlan::Extension(Extension { node }))
    }

    fn push_lambda_parameters(
        &self,
        lambda_parameters: &[Type],
        input_schema: &DFSchema,
    ) -> Result<Vec<String>> {
        self.lambda_consumer
            .push_lambda_parameters(self, lambda_parameters, input_schema)
    }

    fn pop_lambda_parameters(&self) {
        self.lambda_consumer.pop_lambda_parameters();
    }

    fn lambda_variable(&self, steps_out: usize, field_idx: usize) -> Result<Expr> {
        self.lambda_consumer.lambda_variable(steps_out, field_idx)
    }
}
