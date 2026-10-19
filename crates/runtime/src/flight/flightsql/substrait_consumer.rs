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
use parking_lot::RwLock;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, Result, ScalarValue, TableReference, substrait_err};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::expr::{Like, ScalarFunction as ScalarFunctionExpr};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitLambdaConsumer, SubstraitConsumer, from_scalar_function,
};
use datafusion_substrait::substrait::proto::expression::ScalarFunction;
use datafusion_substrait::substrait::proto::{
    ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Type,
};

/// Lowers Substrait plans the way `datafusion-substrait`'s
/// `DefaultSubstraitConsumer` does, except that a Substrait `like` or `ilike`
/// always becomes [`Expr::Like`].
///
/// `datafusion-substrait` resolves a function to a session UDF of the same name
/// before its built-in expressions, and the session registers
/// `datafusion-spark`'s `like` and `ilike`. Left alone, a Substrait `like`
/// therefore lowers to a `like(..)` function call, which the unparser sends
/// verbatim to a federated or accelerated source, and DuckDB rejects it
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

/// Rewrites a call to the Spark `like` / `ilike` function into [`Expr::Like`],
/// taking an optional third argument as the escape character the way
/// `datafusion-substrait` does. Any other expression is returned unchanged.
fn spark_like_to_like_expr(expr: Expr) -> Result<Expr> {
    let Expr::ScalarFunction(ScalarFunctionExpr { func, args }) = expr else {
        return Ok(expr);
    };
    let case_insensitive = match func.name() {
        "like" => false,
        "ilike" => true,
        _ => return Ok(Expr::ScalarFunction(ScalarFunctionExpr { func, args })),
    };
    if args.len() != 2 && args.len() != 3 {
        return Ok(Expr::ScalarFunction(ScalarFunctionExpr { func, args }));
    }

    let mut args = args.into_iter();
    let (Some(expr), Some(pattern)) = (args.next(), args.next()) else {
        return substrait_err!("Expected two or three arguments for `{}`", func.name());
    };
    let escape_char = match args.next() {
        None => None,
        Some(Expr::Literal(ScalarValue::Utf8(escape), _)) => {
            escape.and_then(|s| s.chars().next())
        }
        Some(other) => {
            return substrait_err!(
                "Expected a Utf8 literal for the `{}` escape character, but found {other:?}",
                func.name()
            );
        }
    };

    Ok(Expr::Like(Like::new(
        false,
        Box::new(expr),
        Box::new(pattern),
        escape_char,
        case_insensitive,
    )))
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
