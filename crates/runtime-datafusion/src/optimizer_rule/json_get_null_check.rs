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

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_functions_json::udfs::json_get_udf;

pub(crate) const JSON_GET_IS_NULL_NAME: &str = "__spice_json_get_is_null";
runtime_udfs_api::register_spice_function!(JSON_GET_IS_NULL_REGISTRATION, JSON_GET_IS_NULL_NAME);

/// Exposes the nullness of a JSON union as a boolean before `BigQuery`
/// federation. The union itself has no remote SQL representation.
#[derive(Debug, Default)]
pub struct JsonGetNullCheckRewrite;

impl OptimizerRule for JsonGetNullCheckRewrite {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|plan| {
            let names = NamePreserver::new(&plan);
            plan.map_expressions(|expr| {
                let name = names.save(&expr);
                expr.transform_up(|expr| {
                    let Expr::IsNull(inner) = &expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let Expr::ScalarFunction(call) = inner.as_ref() else {
                        return Ok(Transformed::no(expr));
                    };
                    // UDF equality includes the implementation type. A user's
                    // unrelated function with the same name must keep its semantics.
                    if call.func.as_ref() != json_get_udf().as_ref() {
                        return Ok(Transformed::no(expr));
                    }
                    Ok(Transformed::yes(
                        ScalarUDF::new_from_impl(JsonGetIsNull).call(call.args.clone()),
                    ))
                })
                .map(|result| result.update_data(|expr| name.restore(expr)))
            })
        })
    }

    fn name(&self) -> &'static str {
        "json_get_null_check_rewrite"
    }
}

/// Internal expression, constructed by the rewrite rather than registered as
/// a SQL function. Local execution uses the original UDF and Arrow's union
/// null check, including integers that cannot be represented by the union.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetIsNull;

impl ScalarUDFImpl for JsonGetIsNull {
    fn name(&self) -> &str {
        JSON_GET_IS_NULL_NAME
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| json_get_udf().signature().clone());
        &SIGNATURE
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        json_get_udf().return_type(arg_types)?;
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        json_get_udf().return_field_from_args(args)?;
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let json_get = json_get_udf();
        let scalar_arguments = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            })
            .collect::<Vec<_>>();
        args.return_field = json_get.return_field_from_args(ReturnFieldArgs {
            arg_fields: &args.arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        match json_get.invoke_with_args(args)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                datafusion::arrow::compute::is_null(&array)?,
            ))),
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                value.is_null(),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn rewrite_preserves_union_nullness_and_projection_names() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(json_get_udf().as_ref().clone());
        let docs = StringArray::from(vec![
            None,
            Some("{}"),
            Some(r#"{"context":{"iteration":null}}"#),
            Some(r#"{"context":{"iteration":{}}}"#),
            Some(r#"{"context":{"iteration":[]}}"#),
            Some(r#"{"context":{"iteration":false}}"#),
            Some(r#"{"context":{"iteration":"9223372036854775808"}}"#),
            Some(r#"{"context":{"iteration":9223372036854775808}}"#),
            Some(r#"{"context":{"iteration":9223372036854775807}}"#),
            Some(r#"{"context":{"iteration":1e+00}}"#),
            Some(r#"{"context":{"iteration":1.50}}"#),
            Some("invalid"),
        ]);
        ctx.register_batch(
            "docs",
            RecordBatch::try_from_iter([("doc", Arc::new(docs) as _)])?,
        )?;
        let plan = ctx
            .sql("SELECT json_get(doc, 'context', 'iteration') IS NULL FROM docs")
            .await?
            .into_unoptimized_plan();
        let rewritten = JsonGetNullCheckRewrite
            .rewrite(plan.clone(), &OptimizerContext::new())?
            .data;
        assert_eq!(plan.schema(), rewritten.schema());
        assert!(
            rewritten
                .display_indent()
                .to_string()
                .contains(JSON_GET_IS_NULL_NAME)
        );
        let original = ctx.execute_logical_plan(plan).await?.collect().await?;
        let result = ctx.execute_logical_plan(rewritten).await?.collect().await?;
        assert_eq!(original, result);
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean null checks"),
            &BooleanArray::from(vec![
                true, true, true, false, false, false, false, true, false, false, false, true
            ]),
        );
        Ok(())
    }

    #[tokio::test]
    async fn consumed_union_and_unrelated_functions_keep_their_expressions() -> Result<()> {
        use datafusion::logical_expr::{Volatility, create_udf};

        let ctx = SessionContext::new();
        ctx.register_udf(json_get_udf().as_ref().clone());
        let plan = ctx
            .sql("SELECT json_get('{\"key\":1}', 'key') AS value")
            .await?
            .into_unoptimized_plan();
        let rewritten = JsonGetNullCheckRewrite
            .rewrite(plan.clone(), &OptimizerContext::new())?
            .data;
        assert_eq!(plan, rewritten);

        ctx.register_udf(create_udf(
            "json_get",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args| Ok(args[0].clone())),
        ));
        let plan = ctx
            .sql("SELECT json_get('value', 'key') IS NULL")
            .await?
            .into_unoptimized_plan();
        let rewritten = JsonGetNullCheckRewrite
            .rewrite(plan.clone(), &OptimizerContext::new())?
            .data;
        assert_eq!(plan, rewritten);
        Ok(())
    }
}
