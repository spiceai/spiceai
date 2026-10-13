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


//! Casts a float *literal* to a decimal through its decimal spelling, not its
//! binary value.
//!
//! `WHERE dec_col = 12345678.90` plans the literal as `Float64`, and type coercion
//! then compares both sides as a wide decimal such as `Decimal128(30, 15)`. Arrow's
//! `Float64 -> Decimal` cast scales the binary value, so the literal becomes
//! `12345678.899999999524864` and the filter silently matches nothing — about one
//! cent value in five misses this way (#14970). A bound `$1` with the same number
//! takes the same path once the parameter is substituted.
//!
//! A float literal came from a decimal spelling, and the shortest decimal string
//! that reads back as the same `f64` (Rust's `Display`) recovers it: `12345678.9`.
//! This rule rewrites `CAST(<float literal> AS DECIMAL(p, s))` to the decimal
//! literal parsed exactly from that string, at the cast's precision and scale. It
//! only does so when the string fits the scale without rounding; anything else is
//! left to the cast as before, so overflow and out-of-range behaviour is unchanged.

use datafusion::arrow::datatypes::{DataType, Decimal128Type, Decimal256Type};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{Cast, TryCast};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

/// Rewrites float-literal-to-decimal casts into exact decimal literals. See the
/// module docs.
#[derive(Debug, Default)]
pub struct ExactFloatLiteralDecimalCast;

impl ExactFloatLiteralDecimalCast {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for ExactFloatLiteralDecimalCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            plan.map_expressions(|expr| expr.transform_up(rewrite_expr))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "exact_float_literal_decimal_cast"
    }
}

fn rewrite_expr(expr: Expr) -> Result<Transformed<Expr>> {
    let (inner, data_type) = match &expr {
        Expr::Cast(Cast { expr, data_type }) | Expr::TryCast(TryCast { expr, data_type }) => {
            (expr.as_ref(), data_type)
        }
        _ => return Ok(Transformed::no(expr)),
    };
    let Expr::Literal(value, metadata) = inner else {
        return Ok(Transformed::no(expr));
    };
    match exact_decimal(value, data_type) {
        Some(decimal) => Ok(Transformed::yes(Expr::Literal(decimal, metadata.clone()))),
        None => Ok(Transformed::no(expr)),
    }
}

/// The decimal `value` spells, at `data_type`'s precision and scale, when it is a
/// finite float literal whose shortest round-trip spelling fits that scale.
pub(crate) fn exact_decimal(value: &ScalarValue, data_type: &DataType) -> Option<ScalarValue> {
    let spelling = match value {
        ScalarValue::Float64(Some(v)) if v.is_finite() => v.to_string(),
        ScalarValue::Float32(Some(v)) if v.is_finite() => v.to_string(),
        ScalarValue::Float16(Some(v)) if v.is_finite() => v.to_string(),
        _ => return None,
    };
    let fraction_digits = spelling.split_once('.').map_or(0, |(_, f)| f.len());
    match data_type {
        DataType::Decimal128(precision, scale) => {
            if usize::try_from(*scale).ok()? < fraction_digits {
                return None;
            }
            let parsed = datafusion::arrow::compute::kernels::cast_utils::parse_decimal::<
                Decimal128Type,
            >(&spelling, *precision, *scale)
            .ok()?;
            Some(ScalarValue::Decimal128(Some(parsed), *precision, *scale))
        }
        DataType::Decimal256(precision, scale) => {
            if usize::try_from(*scale).ok()? < fraction_digits {
                return None;
            }
            let parsed = datafusion::arrow::compute::kernels::cast_utils::parse_decimal::<
                Decimal256Type,
            >(&spelling, *precision, *scale)
            .ok()?;
            Some(ScalarValue::Decimal256(Some(parsed), *precision, *scale))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[test]
    fn float_literals_cast_to_the_decimal_they_spell() {
        for (value, precision, scale, expected) in [
            (12_345_678.90_f64, 30_u8, 15_i8, 12_345_678_900_000_000_000_000_i128),
            (4.07, 30, 15, 4_070_000_000_000_000),
            (4.07, 10, 2, 407),
            (0.1, 38, 10, 1_000_000_000),
            (-12_345_678.9, 30, 15, -12_345_678_900_000_000_000_000),
            (100_000.0, 10, 2, 10_000_000),
        ] {
            assert_eq!(
                exact_decimal(
                    &ScalarValue::Float64(Some(value)),
                    &DataType::Decimal128(precision, scale)
                ),
                Some(ScalarValue::Decimal128(Some(expected), precision, scale)),
                "{value} as Decimal128({precision}, {scale})"
            );
        }
    }

    #[test]
    fn literals_that_do_not_fit_the_scale_or_precision_keep_the_cast() {
        // More fraction digits than the scale: the cast's rounding decides.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(1.234)),
                &DataType::Decimal128(10, 2)
            ),
            None
        );
        // Overflows the precision: the cast's error decides.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(123_456.0)),
                &DataType::Decimal128(4, 0)
            ),
            None
        );
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(f64::NAN)),
                &DataType::Decimal128(10, 2)
            ),
            None
        );
        assert_eq!(
            exact_decimal(&ScalarValue::Float64(None), &DataType::Decimal128(10, 2)),
            None
        );
    }

    /// Every cent value up to a large bound must survive the rewrite exactly —
    /// the plain cast gets about one in five of these wrong.
    #[test]
    fn every_cent_value_round_trips() {
        for cents in (0_i128..2_000_000).step_by(7) {
            let spelling = format!("{}.{:02}", cents / 100, cents % 100);
            let value: f64 = spelling.parse().expect("valid float");
            assert_eq!(
                exact_decimal(
                    &ScalarValue::Float64(Some(value)),
                    &DataType::Decimal128(30, 15)
                ),
                Some(ScalarValue::Decimal128(
                    Some(cents * 10_i128.pow(13)),
                    30,
                    15
                )),
                "{spelling}"
            );
        }
    }

    async fn count_matches(sql: &str, params: Option<Vec<ScalarValue>>, scale: i8) -> i64 {
        let precision = 10 + u8::try_from(scale).expect("scale");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dec",
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let unit = 10_i128.pow(u32::try_from(scale).expect("scale"));
        let values = Decimal128Array::from(vec![
            12_345_678 * unit + 9 * unit / 10,
            4 * unit + 7 * unit / 100,
        ])
        .with_precision_and_scale(precision, scale)
        .expect("valid decimal");
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
            .expect("valid batch");

        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(ExactFloatLiteralDecimalCast::new()));
        ctx.register_batch("t", batch).expect("register");
        let mut df = ctx.sql(sql).await.expect("plan");
        if let Some(params) = params {
            df = df.with_param_values(params).expect("bind");
        }
        let batches = df.collect().await.expect("execute");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0)
    }

    #[tokio::test]
    async fn decimal_column_matches_a_fractional_literal_across_scales() {
        for scale in [2_i8, 4, 6, 15] {
            for literal in ["12345678.90", "4.07"] {
                let sql = format!("SELECT count(*) FROM t WHERE dec = {literal}");
                assert_eq!(count_matches(&sql, None, scale).await, 1, "{sql}, scale {scale}");
            }
        }
    }

    #[tokio::test]
    async fn decimal_column_matches_a_bound_float_parameter_across_scales() {
        for scale in [2_i8, 4, 6, 15] {
            for value in [12_345_678.90_f64, 4.07] {
                let count = count_matches(
                    "SELECT count(*) FROM t WHERE dec = $1",
                    Some(vec![ScalarValue::Float64(Some(value))]),
                    scale,
                )
                .await;
                assert_eq!(count, 1, "$1 = {value}, scale {scale}");
            }
        }
    }
}
