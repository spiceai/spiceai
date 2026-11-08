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
//!
//! A `+` or `-` stays as written when one operand is a non-NULL integer
//! literal, its operands' values keep every result inside its type, and
//! neither operand can raise an error. The wrapping kernel cannot wrap there,
//! so it returns what the checked one would. `DataFusion` simplifies some
//! plain operators by dropping an operand (`A + NULL` to `NULL`, `A * 0` to
//! `0`), which must never drop one with an error to raise. A column can become
//! any expression once `DataFusion` inlines a subquery's projection into this
//! one, including a NULL beside a failing operand; a non-NULL literal stays
//! one, so `A + 1` keeps `A`. `CAST(smallint_column AS BIGINT) + 1` is the
//! common case.
//!
//! Only columns, literals and casts of them are looked into, at most
//! [`MAX_CAST_DEPTH`] casts deep, so the check costs the same at every
//! operator however deep the expression; an operand that is itself an
//! operator keeps the outer one checked.
//!
//! Left a plain `+`, it is still the `SUM(x + 1), SUM(x + 2), ...` shape that
//! `DataFusion` rewrites to one `SUM(x)` and one `COUNT(x)` (see
//! [`CheckedSum`](super::CheckedSum)). A `*` is always checked.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{BinaryExpr, Cast, TryCast};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

use super::is_integer_arith_type;
use super::op::checked_op_udf;

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
    if left_type != right_type
        || !is_integer_arith_type(&left_type)
        || stays_plain(op, &left, &right, &left_type, schema)
    {
        return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
    }
    Transformed::yes(udf.call(vec![*left, *right]))
}

/// How many casts (and aliases) the operand checks look through. Two reach
/// `CAST(CAST(int_column AS INT) AS BIGINT)`; past the cap an operand is
/// unknown and the operator stays checked.
const MAX_CAST_DEPTH: usize = 4;

/// Whether `left op right` can stay the plain operator: it is a `+` or `-`
/// with a non-NULL integer literal on one side, neither operand can raise an
/// error, and it cannot leave `data_type` (see the module documentation).
fn stays_plain(
    op: Operator,
    left: &Expr,
    right: &Expr,
    data_type: &DataType,
    schema: &DFSchema,
) -> bool {
    matches!(op, Operator::Plus | Operator::Minus)
        && (is_integer_literal(left) || is_integer_literal(right))
        && raises_no_error(left, schema, MAX_CAST_DEPTH)
        && raises_no_error(right, schema, MAX_CAST_DEPTH)
        && cannot_overflow(op, left, right, data_type, schema)
}

/// Whether evaluating `expr` cannot fail: a column, a literal, or, within
/// `depth` casts, a `TRY_CAST` of one or a `CAST` of one whose every value
/// fits the target type.
fn raises_no_error(expr: &Expr, schema: &DFSchema, depth: usize) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(..) => true,
        _ if depth == 0 => false,
        Expr::Alias(alias) => raises_no_error(&alias.expr, schema, depth - 1),
        Expr::TryCast(TryCast { expr: operand, .. }) => raises_no_error(operand, schema, depth - 1),
        Expr::Cast(Cast {
            expr: operand,
            field,
        }) => {
            raises_no_error(operand, schema, depth - 1)
                && type_range(field.data_type())
                    .zip(value_range(operand, schema, depth - 1))
                    .is_some_and(|(bounds, range)| bounds.contains(range))
        }
        _ => false,
    }
}

/// Whether `left op right` is a `+` or `-` that stays inside `data_type` for
/// every value its operands can take, so the wrapping operator returns what
/// the checked kernel would.
fn cannot_overflow(
    op: Operator,
    left: &Expr,
    right: &Expr,
    data_type: &DataType,
    schema: &DFSchema,
) -> bool {
    let (Some(left), Some(right), Some(bounds)) = (
        value_range(left, schema, MAX_CAST_DEPTH),
        value_range(right, schema, MAX_CAST_DEPTH),
        type_range(data_type),
    ) else {
        return false;
    };
    op_range(op, left, right).is_some_and(|range| bounds.contains(range))
}

/// The closed range of values an integer expression can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ValueRange {
    min: i128,
    max: i128,
}

impl ValueRange {
    fn point(value: i128) -> Self {
        Self {
            min: value,
            max: value,
        }
    }

    fn contains(self, other: Self) -> bool {
        self.min <= other.min && other.max <= self.max
    }
}

/// Every value of an integer `data_type`; `None` for any other type.
fn type_range(data_type: &DataType) -> Option<ValueRange> {
    let (min, max) = match data_type {
        DataType::Int8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        DataType::Int16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        DataType::Int32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        DataType::Int64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        DataType::UInt8 => (0, i128::from(u8::MAX)),
        DataType::UInt16 => (0, i128::from(u16::MAX)),
        DataType::UInt32 => (0, i128::from(u32::MAX)),
        DataType::UInt64 => (0, i128::from(u64::MAX)),
        _ => return None,
    };
    Some(ValueRange { min, max })
}

/// Whether `expr` is a non-NULL integer literal.
fn is_integer_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if integer_literal(value).is_some())
}

/// The value of a non-null integer literal.
fn integer_literal(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(value) => value.map(i128::from),
        ScalarValue::Int16(value) => value.map(i128::from),
        ScalarValue::Int32(value) => value.map(i128::from),
        ScalarValue::Int64(value) => value.map(i128::from),
        ScalarValue::UInt8(value) => value.map(i128::from),
        ScalarValue::UInt16(value) => value.map(i128::from),
        ScalarValue::UInt32(value) => value.map(i128::from),
        ScalarValue::UInt64(value) => value.map(i128::from),
        _ => None,
    }
}

/// The values an integer `expr` can take: a literal's value, a column's type
/// range, or a cast's target range, narrowed to its operand's when every
/// operand value fits, within `depth` casts. `None` for anything else, which
/// keeps the operator over it checked.
fn value_range(expr: &Expr, schema: &DFSchema, depth: usize) -> Option<ValueRange> {
    match expr {
        Expr::Literal(value, _) => integer_literal(value).map(ValueRange::point),
        Expr::Column(_) => type_range(&expr.get_type(schema).ok()?),
        _ if depth == 0 => None,
        Expr::Alias(alias) => value_range(&alias.expr, schema, depth - 1),
        // A cast that returns a value returns one of its target type (CAST
        // errors and TRY_CAST yields NULL on a value that does not fit).
        Expr::Cast(Cast {
            expr: operand,
            field,
        })
        | Expr::TryCast(TryCast {
            expr: operand,
            field,
        }) => {
            let bounds = type_range(field.data_type())?;
            Some(
                value_range(operand, schema, depth - 1)
                    .filter(|range| bounds.contains(*range))
                    .unwrap_or(bounds),
            )
        }
        _ => None,
    }
}

/// The range of `left op right` over every pair of operand values; `None` when
/// `op` is not `+` or `-` (see the module documentation for `*`).
fn op_range(op: Operator, left: ValueRange, right: ValueRange) -> Option<ValueRange> {
    match op {
        Operator::Plus => Some(ValueRange {
            min: left.min.checked_add(right.min)?,
            max: left.max.checked_add(right.max)?,
        }),
        Operator::Minus => Some(ValueRange {
            min: left.min.checked_sub(right.max)?,
            max: left.max.checked_sub(right.min)?,
        }),
        _ => None,
    }
}
