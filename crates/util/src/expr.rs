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

//! `DataFusion` expression utilities.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use chrono::Utc;
use datafusion::{
    common::DFSchema,
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::Expr,
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
};

/// Simplify an expression by evaluating constant sub-expressions.
///
/// Uses `DataFusion`'s [`ExprSimplifier`] which performs:
/// - **Constant folding**: Evaluates constant expressions at plan time (e.g., `now()` → literal timestamp, `1 + 2` → `3`)
/// - **Cast folding**: Folds nested casts around literals (e.g., `l_created_at < CAST(Utf8("2026-02-01 12:34:56.123456789") AS Timestamp(ns, "UTC"))` -> `l_created_at < TimestampMicrosecond(1769949296123456, Some("UTC"))`)
/// - **Algebraic simplification**: Simplifies boolean logic (e.g., `expr AND true` → `expr`, `!!expr` → `expr`)
/// - **Canonicalization**: Normalizes comparisons (e.g., `5 < x` → `x > 5`)
///
/// # Benefits for retention/refresh filters
///
/// - **Faster filter evaluation**: No function calls or type conversions at runtime
/// - **Works with Vortex**: Which doesn't currently support timestamp casts
/// - **Better predicate pushdown**: Storage engines can use indexes/stats directly
///
/// # Errors
///
/// Returns an error if schema conversion or expression simplification fails.
pub fn simplify_expr(expr: Expr, schema: &SchemaRef) -> Result<Expr, DataFusionError> {
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

    // Set query_execution_start_time so that NOW() and other time-dependent
    // functions can be evaluated during simplification
    let execution_props = ExecutionProps::new().with_query_execution_start_time(Utc::now());
    let simplify_context = SimplifyContext::new(&execution_props).with_schema(Arc::new(df_schema));
    let simplifier = ExprSimplifier::new(simplify_context);

    simplifier.simplify(expr)
}

/// Combine expressions using a balanced binary tree structure.
///
/// Instead of left-nested: `((((a AND b) AND c) AND d) AND e)`  (depth = n)
/// Creates balanced:       `((a AND b) AND (c AND d)) AND e`   (depth = log2(n))
///
/// This prevents stack overflow when evaluating large expression sets recursively.
///
/// # Arguments
/// * `exprs` - Vector of expressions to combine (returns `None` if empty)
/// * `combine_fn` - Binary function to combine two expressions (e.g., `Expr::and`, `Expr::or`)
///
/// # Returns
/// * `None` if `exprs` is empty
/// * `Some(expr)` with the combined expression otherwise
///
/// `DataFusion`'s substrait module has `arg_list_to_binary_op_tree` which does balanced
/// construction, but it's not publicly exported for general use.
pub fn combine_exprs_balanced<F>(mut exprs: Vec<Expr>, combine_fn: F) -> Option<Expr>
where
    F: Fn(Expr, Expr) -> Expr + Copy,
{
    // Empty input returns None
    if exprs.is_empty() {
        return None;
    }

    // Fast path: single element, no combining needed
    if exprs.len() == 1 {
        return exprs.into_iter().next();
    }

    // Repeatedly combine pairs until we have a single expression
    while exprs.len() > 1 {
        let mut next_level = Vec::new();

        let mut iter = exprs.into_iter();
        while let Some(left) = iter.next() {
            if let Some(right) = iter.next() {
                next_level.push(combine_fn(left, right));
            } else {
                // Odd element - carry forward to next level
                next_level.push(left);
            }
        }
        exprs = next_level;
    }

    exprs.into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::{Operator, binary_expr, cast, col, lit};
    use datafusion::scalar::ScalarValue;

    #[test]
    fn test_combine_empty_returns_none() {
        let result = combine_exprs_balanced(vec![], Expr::and);
        assert!(result.is_none());
    }

    #[test]
    fn test_combine_single_expr() {
        let expr = col("a").eq(lit(1));
        let result = combine_exprs_balanced(vec![expr.clone()], Expr::and);
        assert_eq!(result, Some(expr));
    }

    #[test]
    fn test_combine_two_exprs() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));
        let result = combine_exprs_balanced(vec![expr1.clone(), expr2.clone()], Expr::and);
        assert_eq!(result, Some(expr1.and(expr2)));
    }

    #[test]
    fn test_combine_three_exprs() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));
        let expr3 = col("c").eq(lit(3));
        let result =
            combine_exprs_balanced(vec![expr1.clone(), expr2.clone(), expr3.clone()], Expr::and);
        // Should be: (a=1 AND b=2) AND c=3
        assert_eq!(result, Some(expr1.and(expr2).and(expr3)));
    }

    #[test]
    fn test_combine_four_exprs_balanced() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));
        let expr3 = col("c").eq(lit(3));
        let expr4 = col("d").eq(lit(4));
        let result = combine_exprs_balanced(
            vec![expr1.clone(), expr2.clone(), expr3.clone(), expr4.clone()],
            Expr::and,
        );
        // Should be: (a=1 AND b=2) AND (c=3 AND d=4)
        let expected = expr1.and(expr2).and(expr3.and(expr4));
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_combine_with_or() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));
        let result = combine_exprs_balanced(vec![expr1.clone(), expr2.clone()], Expr::or);
        assert_eq!(result, Some(expr1.or(expr2)));
    }

    /// Tests that `simplify_expr` folds casts around timestamp literals.
    #[test]
    fn test_simplify_folds_timestamp_cast_to_column_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        let expr = binary_expr(
            col("l_created_at"),
            Operator::Lt,
            cast(
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(
                        Some(1_620_000_000_000_000_000),
                        Some("UTC".into()),
                    ),
                    None,
                ),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        );

        let simplified = simplify_expr(expr, &schema).expect("simplification should succeed");

        assert_eq!(
            simplified.to_string(),
            r#"l_created_at < TimestampMicrosecond(1620000000000000, Some("UTC"))"#
        );
    }

    /// Tests nanosecond precision handling when casting to microseconds.
    #[test]
    fn test_simplify_timestamp_cast_truncates_nanoseconds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        let expr = binary_expr(
            col("l_created_at"),
            Operator::Lt,
            cast(
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(
                        Some(1_620_000_000_000_000_001),
                        Some("UTC".into()),
                    ),
                    None,
                ),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        );

        let simplified = simplify_expr(expr, &schema).expect("simplification should succeed");

        assert_eq!(
            simplified.to_string(),
            r#"l_created_at < TimestampMicrosecond(1620000000000000, Some("UTC"))"#
        );
    }
}
