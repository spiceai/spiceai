/*
Copyright 2026 The Spice.ai OSS Authors

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

// Re-export the canonical types from datafusion-table-providers so that the
// rest of the codebase has a single definition of FunctionSupport.
pub use datafusion_table_providers::util::supported_functions::{
    FunctionRestriction, FunctionSupport, contains_unsupported_functions,
};

use datafusion::{common::Result, logical_expr::LogicalPlan};
use datafusion_federation::FederatedPlanNode;

/// If `plan` is a `FederatedPlanNode` whose inner plan contains functions that
/// are unsupported according to `function_support`, unwrap it back to the inner
/// plan so it is executed locally rather than being sent to the remote.
pub fn unfederate_plan_with_unsupported_functions(
    plan: LogicalPlan,
    function_support: &FunctionSupport,
) -> Result<LogicalPlan> {
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(federated) = extension.node.as_any().downcast_ref::<FederatedPlanNode>()
        && contains_unsupported_functions(federated.plan(), function_support)?
    {
        return Ok(federated.plan().clone());
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{
        ColumnarValue, LogicalPlanBuilder, ScalarUDF, TableSource, Volatility,
        builder::LogicalTableSource, create_udf, expr::ScalarFunction, expr_fn::in_subquery,
    };
    use datafusion::prelude::{col, lit};

    use super::*;

    fn stub_udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn udf_expr(name: &str) -> datafusion::logical_expr::Expr {
        datafusion::logical_expr::Expr::ScalarFunction(ScalarFunction::new_udf(
            stub_udf(name),
            vec![col("val")],
        ))
    }

    fn scan(table: &str) -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let source = Arc::new(LogicalTableSource::new(schema)) as Arc<dyn TableSource>;
        LogicalPlanBuilder::scan(table, source, None).expect("scan")
    }

    fn deny(names: &[&str]) -> FunctionSupport {
        FunctionSupport::new(
            Some(FunctionRestriction::Deny(
                names.iter().map(|s| (*s).to_string()).collect(),
            )),
            None,
            None,
        )
    }

    #[test]
    fn detects_denied_function_in_root_node() {
        let plan = scan("t")
            .project(vec![udf_expr("json_get_str")])
            .expect("project")
            .build()
            .expect("build");

        assert!(
            contains_unsupported_functions(&plan, &deny(&["json_get_str"])).expect("walk plan"),
            "denied function in the root projection must be detected"
        );
    }

    #[test]
    fn detects_denied_function_below_root_node() {
        let plan = scan("t")
            .filter(udf_expr("json_get_str").eq(lit("x")))
            .expect("filter")
            .project(vec![col("id")])
            .expect("project")
            .build()
            .expect("build");

        assert!(
            contains_unsupported_functions(&plan, &deny(&["json_get_str"])).expect("walk plan"),
            "denied function in a filter below the root must be detected"
        );
    }

    #[test]
    fn detects_denied_function_inside_subquery() {
        let subquery = scan("u")
            .project(vec![udf_expr("json_get_str")])
            .expect("project")
            .build()
            .expect("build");
        let plan = scan("t")
            .filter(in_subquery(col("val"), Arc::new(subquery)))
            .expect("filter")
            .build()
            .expect("build");

        assert!(
            contains_unsupported_functions(&plan, &deny(&["json_get_str"])).expect("walk plan"),
            "denied function inside a subquery must be detected"
        );
    }

    #[test]
    fn allows_plan_without_denied_functions() {
        let plan = scan("t")
            .filter(udf_expr("upper").eq(lit("x")))
            .expect("filter")
            .project(vec![col("id")])
            .expect("project")
            .build()
            .expect("build");

        assert!(
            !contains_unsupported_functions(&plan, &deny(&["json_get_str"])).expect("walk plan"),
            "plan with no denied functions must stay federated"
        );
    }
}
