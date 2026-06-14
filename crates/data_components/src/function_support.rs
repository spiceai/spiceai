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

use datafusion::{
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    logical_expr::{Expr, LogicalPlan},
};
use datafusion_federation::FederatedPlanNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FunctionRestriction {
    Allow(Vec<String>),
    Deny(Vec<String>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[expect(clippy::struct_field_names)]
pub struct FunctionSupport {
    scalar_functions: Option<FunctionRestriction>,
    aggregate_functions: Option<FunctionRestriction>,
    window_functions: Option<FunctionRestriction>,
}

impl FunctionSupport {
    #[must_use]
    pub const fn new(
        scalar_functions: Option<FunctionRestriction>,
        aggregate_functions: Option<FunctionRestriction>,
        window_functions: Option<FunctionRestriction>,
    ) -> Self {
        Self {
            scalar_functions,
            aggregate_functions,
            window_functions,
        }
    }

    #[must_use]
    pub fn supports(&self, expr: &Expr) -> bool {
        let mut supported = true;
        let result = expr.apply(|expr| {
            if !self.supports_node(expr) {
                supported = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        result.is_ok() && supported
    }

    fn supports_node(&self, expr: &Expr) -> bool {
        match expr {
            Expr::ScalarFunction(function) => {
                supports_name(self.scalar_functions.as_ref(), function.name())
            }
            Expr::AggregateFunction(function) => {
                supports_name(self.aggregate_functions.as_ref(), function.func.name())
            }
            Expr::WindowFunction(function) => {
                supports_name(self.window_functions.as_ref(), function.fun.name())
            }
            _ => true,
        }
    }
}

#[must_use]
fn supports_name(restriction: Option<&FunctionRestriction>, name: &str) -> bool {
    match restriction {
        Some(FunctionRestriction::Allow(names)) => {
            names.iter().any(|n| n.eq_ignore_ascii_case(name))
        }
        Some(FunctionRestriction::Deny(names)) => {
            !names.iter().any(|n| n.eq_ignore_ascii_case(name))
        }
        None => true,
    }
}

/// Returns `true` if any expression anywhere in `plan` — including child plan
/// nodes and subquery plans (`IN`/`EXISTS`/scalar subqueries) — references a
/// function the `function_support` restriction rejects.
///
/// The walk must cover the whole plan tree, not just `plan.expressions()` on
/// the root node: callers pass the root of an entire federated subtree (e.g.
/// `Projection → Filter → TableScan`), so a denied function in a `WHERE`
/// predicate or subquery would otherwise escape the check and be unparsed into
/// the remote engine's SQL.
pub fn contains_unsupported_functions(
    plan: &LogicalPlan,
    function_support: &FunctionSupport,
) -> Result<bool> {
    let mut unsupported = false;
    plan.apply_with_subqueries(|plan| {
        for expr in plan.expressions() {
            if !function_support.supports(&expr) {
                unsupported = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(unsupported)
}

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

    fn udf_expr(name: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(stub_udf(name), vec![col("val")]))
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
        // The denied function sits in a `Filter` BELOW the root `Projection`,
        // whose own expressions are clean. A root-only check misses it and the
        // predicate would be unparsed into the remote engine's SQL.
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
        // The denied function lives only inside the `IN (<subquery>)` plan.
        // `Expr::apply` does not descend into subquery plans, so the walk must
        // use `apply_with_subqueries` to see it.
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
