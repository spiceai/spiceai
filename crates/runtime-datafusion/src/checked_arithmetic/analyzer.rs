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

//! Rewrites integer `+` / `-` / `*` into checked kernels after type coercion.
//!
//! Federation has already claimed remote subplans by this point, so remotes
//! still see the original operators. The leftover local expressions cannot be
//! folded by `SimplifyExpressions` with wrapping kernels.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::is_integer_arith_type;
use super::op::{checked_op_udf, checked_udf_operator};

/// Analyzer rule that replaces integer `+` / `-` / `*` with checked kernels.
#[derive(Debug, Default)]
pub struct CheckedIntegerOverflow;

impl CheckedIntegerOverflow {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for CheckedIntegerOverflow {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            // Projection / Filter / Aggregate expressions are typed against
            // the input, not `plan.schema()` (the output). A binary plan
            // (Join) types its predicates against the combined output schema.
            let schema = expression_schema(&plan);
            let name_preserver = NamePreserver::new(&plan);
            plan.map_expressions(|expr| {
                let saved_name = name_preserver.save(&expr);
                expr.transform_up(|expr| Ok(rewrite_integer_arith(expr, schema.as_ref())))
                    .map(|transformed| transformed.update_data(|expr| saved_name.restore(expr)))
            })
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "checked_integer_overflow"
    }
}

fn expression_schema(plan: &LogicalPlan) -> Arc<DFSchema> {
    let inputs = plan.inputs();
    if let [input] = inputs.as_slice() {
        Arc::clone(input.schema())
    } else {
        Arc::clone(plan.schema())
    }
}

fn rewrite_integer_arith(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return Transformed::no(expr);
    };
    let Some(udf) = checked_op_udf(op) else {
        return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
    };
    // Leave the operator alone when a child cannot be typed against this
    // plan's input schema (outer-query columns in a correlated subquery,
    // or a projection output name). A hard error here would reject
    // plans that type-check after the rest of the analyzer pass.
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
    };
    if left_type != right_type || !is_integer_arith_type(&left_type) {
        return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
    }
    Transformed::yes(udf.call(vec![*left, *right]))
}

/// Puts back the integer `+` / `-` / `*` that [`CheckedIntegerOverflow`]
/// replaced with checked kernels.
///
/// This is for an expression that leaves `DataFusion` for a source that
/// evaluates it itself, such as an `UPDATE`'s assignments or a `DELETE`'s
/// filters written through to the federated source. The source does not know
/// the `__spice_checked_*` names and applies its own overflow rules, as it
/// does for a federated read.
///
/// # Errors
///
/// Returns an error when the expression tree cannot be traversed.
pub fn restore_integer_operators(expr: Expr) -> Result<Expr> {
    expr.transform_up(|expr| Ok(restore_integer_operator(expr)))
        .map(|transformed| transformed.data)
}

fn restore_integer_operator(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Transformed::no(expr);
    };
    let Some(op) = checked_udf_operator(&func) else {
        return Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args }));
    };
    match <[Expr; 2]>::try_from(args) {
        Ok([left, right]) => Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            op,
            Box::new(right),
        ))),
        Err(args) => Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args })),
    }
}
