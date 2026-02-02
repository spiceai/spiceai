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

//! Expression utilities for `DataFusion` expressions.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::DFSchema,
    error::DataFusionError,
    execution::context::ExecutionProps,
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    prelude::Expr,
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
pub(crate) fn simplify_expr(expr: Expr, schema: &SchemaRef) -> Result<Expr, DataFusionError> {
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

    let execution_props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&execution_props).with_schema(Arc::new(df_schema));
    let simplifier = ExprSimplifier::new(simplify_context);

    simplifier.simplify(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::{Operator, binary_expr, cast, col};
    use datafusion::scalar::ScalarValue;

    /// Tests that `simplify_expr` folds casts around timestamp literals.
    #[test]
    fn test_simplify_folds_timestamp_cast_to_column_type() {
        // Schema with microsecond timestamp column (like `l_created_at Timestamp(µs, "UTC")`)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        // Expression: l_created_at < CAST(TimestampNanosecond(...) AS Timestamp(µs, "UTC"))
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

        // After simplification, the cast should be folded into the literal
        // Result: l_created_at < TimestampMicrosecond(1620000000000000, Some("UTC"))
        assert_eq!(
            simplified.to_string(),
            r#"l_created_at < TimestampMicrosecond(1620000000000000, Some("UTC"))"#
        );
    }

    /// Tests nanosecond precision handling when casting to microseconds.
    ///
    /// Verifies that casts are folded even when the nanosecond timestamp has sub-microsecond
    /// precision.
    #[test]
    fn test_simplify_timestamp_cast_truncates_nanoseconds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));

        // Nanosecond value with sub-microsecond precision: 1_620_000_000_000_000_001 ns
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

        // DataFusion behavior: truncates nanoseconds to microseconds
        // Truncation: 1_620_000_000_000_000_001 ns -> 1_620_000_000_000_000 µs
        assert_eq!(
            simplified.to_string(),
            r#"l_created_at < TimestampMicrosecond(1620000000000000, Some("UTC"))"#
        );
    }
}
