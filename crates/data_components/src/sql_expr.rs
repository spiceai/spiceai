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
    error::Result,
    logical_expr::Expr,
    sql::unparser::{Unparser, dialect::DefaultDialect},
};

pub fn to_sql_preserving_precedence(expr_value: &Expr) -> Result<String> {
    match expr_value {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql_preserving_precedence(binary_expr.left.as_ref())?;
            let right = to_sql_preserving_precedence(binary_expr.right.as_ref())?;
            Ok(format!("({left} {} {right})", binary_expr.op))
        }
        Expr::Not(inner) => Ok(format!(
            "(NOT ({}))",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNull(inner) => Ok(format!(
            "({} IS NULL)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotNull(inner) => Ok(format!(
            "({} IS NOT NULL)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsTrue(inner) => Ok(format!(
            "({} IS TRUE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsFalse(inner) => Ok(format!(
            "({} IS FALSE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsUnknown(inner) => Ok(format!(
            "({} IS UNKNOWN)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotTrue(inner) => Ok(format!(
            "({} IS NOT TRUE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotFalse(inner) => Ok(format!(
            "({} IS NOT FALSE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotUnknown(inner) => Ok(format!(
            "({} IS NOT UNKNOWN)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::Negative(inner) => Ok(format!(
            "(-{})",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        _ => {
            let dialect = DefaultDialect {};
            Ok(Unparser::new(&dialect).expr_to_sql(expr_value)?.to_string())
        }
    }
}

/// Encode expressions sent to another Spice runtime using exact Arrow numeric types.
/// Other Flight SQL servers continue to use their own SQL literal conventions.
pub fn to_spice_sql(expr: &Expr) -> Result<String> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    let typed = expr
        .clone()
        .transform_up(|expr| {
            let replacement = match &expr {
                Expr::Literal(value, _) => typed_numeric_literal(value)?,
                Expr::TryCast(cast) if cast.field.data_type().is_numeric() => {
                    return datafusion::common::not_impl_err!(
                        "Executor SQL does not encode exact numeric TRY_CAST targets"
                    );
                }
                Expr::Cast(cast) if cast.field.data_type().is_numeric() => {
                    Some(datafusion::functions::core::arrow_cast().call(vec![
                        cast.expr.as_ref().clone(),
                        datafusion::logical_expr::lit(cast.field.data_type().to_string()),
                    ]))
                }
                Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_)
                | Expr::SetComparison(_) => {
                    return datafusion::common::not_impl_err!(
                        "Executor SQL does not encode literals inside subqueries"
                    );
                }
                _ => None,
            };
            Ok(match replacement {
                Some(replacement) => Transformed::yes(replacement),
                None => Transformed::no(expr),
            })
        })?
        .data;
    to_sql_preserving_precedence(&typed)
}

/// Render an exact numeric literal for a Spice executor, or `None` for other types.
pub fn numeric_literal_to_spice_sql(
    value: &datafusion::scalar::ScalarValue,
) -> Result<Option<String>> {
    typed_numeric_literal(value)?
        .map(|expr| to_sql_preserving_precedence(&expr))
        .transpose()
}

fn typed_numeric_literal(value: &datafusion::scalar::ScalarValue) -> Result<Option<Expr>> {
    use arrow::datatypes::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    };
    use datafusion::{logical_expr::lit, scalar::ScalarValue};
    let data_type = value.data_type();
    if !data_type.is_numeric() {
        return Ok(None);
    }
    // Arrow's string-to-decimal cast rejects negative scales. Retain local
    // evaluation rather than emitting an executor literal it cannot decode.
    if matches!(data_type,
        arrow::datatypes::DataType::Decimal32(_, scale)
        | arrow::datatypes::DataType::Decimal64(_, scale)
        | arrow::datatypes::DataType::Decimal128(_, scale)
        | arrow::datatypes::DataType::Decimal256(_, scale) if scale < 0)
    {
        return datafusion::common::not_impl_err!(
            "Executor SQL cannot encode negative-scale decimal literals"
        );
    }
    let text = if value.is_null() {
        ScalarValue::Utf8(None)
    } else {
        let text = match value {
            ScalarValue::Float32(Some(v)) => {
                if v.is_nan()
                    && v.to_bits() != f32::NAN.to_bits()
                    && v.to_bits() != (-f32::NAN).to_bits()
                {
                    return datafusion::common::not_impl_err!(
                        "Executor SQL cannot preserve this Float32 NaN payload"
                    );
                }
                if v.is_nan() && v.is_sign_negative() {
                    "-NaN".to_string()
                } else {
                    v.to_string()
                }
            }
            ScalarValue::Float64(Some(v)) => {
                if v.is_nan()
                    && v.to_bits() != f64::NAN.to_bits()
                    && v.to_bits() != (-f64::NAN).to_bits()
                {
                    return datafusion::common::not_impl_err!(
                        "Executor SQL cannot preserve this Float64 NaN payload"
                    );
                }
                if v.is_nan() && v.is_sign_negative() {
                    "-NaN".to_string()
                } else {
                    v.to_string()
                }
            }
            ScalarValue::Decimal32(Some(v), p, s) => Decimal32Type::format_decimal(*v, *p, *s),
            ScalarValue::Decimal64(Some(v), p, s) => Decimal64Type::format_decimal(*v, *p, *s),
            ScalarValue::Decimal128(Some(v), p, s) => Decimal128Type::format_decimal(*v, *p, *s),
            ScalarValue::Decimal256(Some(v), p, s) => Decimal256Type::format_decimal(*v, *p, *s),
            ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_) => value.to_string(),
            _ => {
                return datafusion::common::not_impl_err!(
                    "Executor SQL cannot preserve numeric type {data_type}"
                );
            }
        };
        ScalarValue::Utf8(Some(text))
    };
    Ok(Some(
        datafusion::functions::core::arrow_cast().call(vec![lit(text), lit(data_type.to_string())]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{logical_expr::lit, prelude::SessionContext, scalar::ScalarValue};

    #[tokio::test]
    async fn executor_numeric_sql_preserves_arrow_values_and_types() -> Result<()> {
        let mut values = vec![
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::Decimal32(Some(-123), 7, 2),
            ScalarValue::Decimal64(Some(123), 16, 2),
            ScalarValue::Decimal128(Some(i128::MAX / 10), 38, 10),
            ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_i128(i128::MAX)), 60, 10),
            ScalarValue::Float32(None),
            ScalarValue::Decimal128(None, 38, 0),
        ];
        for value in [
            0.0_f32,
            -0.0,
            0.1,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
            -f32::NAN,
            f32::from_bits(1),
            f32::MAX,
        ] {
            values.push(ScalarValue::Float32(Some(value)));
        }
        for value in [
            0.0_f64,
            -0.0,
            0.1,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            -f64::NAN,
            f64::from_bits(1),
            f64::MAX,
        ] {
            values.push(ScalarValue::Float64(Some(value)));
        }
        let context = SessionContext::new();
        for value in values {
            let sql = to_spice_sql(&lit(value.clone()))?;
            let batches = context
                .sql(&format!("SELECT {sql} AS v"))
                .await?
                .collect()
                .await?;
            let actual = ScalarValue::try_from_array(batches[0].column(0), 0)?;
            assert_eq!(actual.data_type(), value.data_type(), "{sql}");
            match (&actual, &value) {
                (ScalarValue::Float32(Some(actual)), ScalarValue::Float32(Some(expected))) => {
                    assert_eq!(actual.to_bits(), expected.to_bits(), "{sql}");
                }
                (ScalarValue::Float64(Some(actual)), ScalarValue::Float64(Some(expected))) => {
                    assert_eq!(actual.to_bits(), expected.to_bits(), "{sql}");
                }
                _ => assert_eq!(actual, value, "{sql}"),
            }
        }
        Ok(())
    }

    #[test]
    fn negative_scale_decimal_literals_remain_local() {
        for value in [
            ScalarValue::Decimal32(Some(123), 7, -2),
            ScalarValue::Decimal64(Some(123), 16, -2),
            ScalarValue::Decimal128(Some(123), 38, -2),
            ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_i128(123)), 60, -2),
        ] {
            let error = to_spice_sql(&lit(value)).expect_err("negative scale must remain local");
            assert!(matches!(
                error,
                datafusion::error::DataFusionError::NotImplemented(_)
            ));
        }
    }

    #[test]
    fn noncanonical_nan_stays_local_and_generic_sql_is_unchanged() {
        let value = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0001)));
        to_spice_sql(&lit(value)).expect_err("noncanonical NaN must remain local");
        let generic = to_sql_preserving_precedence(&lit(0.1_f32)).expect("generic SQL");
        assert!(!generic.contains("arrow_cast"), "{generic}");
    }
}
