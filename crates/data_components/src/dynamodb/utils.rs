/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::dynamodb::table_schema::DynamoDBTableSchema;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, FixedOffset, NaiveDate};
use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use util::time_format::format_datetime;

fn timestamp_to_attribute(
    millis: i64,
    tz_opt: Option<&Arc<str>>,
    time_format: &str,
) -> datafusion::error::Result<AttributeValue> {
    let Some(dt_utc) = DateTime::from_timestamp_millis(millis) else {
        return Err(DataFusionError::Internal(format!(
            "Failed to convert timestamp in millis to DateTime: {millis}"
        )));
    };

    let dt: DateTime<FixedOffset> = match tz_opt {
        Some(tz_str) => {
            let tz = FixedOffset::from_str(tz_str).map_err(|e| {
                DataFusionError::Internal(format!("Failed to parse TimeZone \"{tz_str}\": {e}"))
            })?;
            dt_utc.with_timezone(&tz)
        }
        None => dt_utc.fixed_offset(),
    };

    let Some(formatted) = format_datetime(dt, time_format) else {
        return Err(DataFusionError::Internal(format!(
            "Failed to parse timestamp. Verify format is valid: \"{time_format}\""
        )));
    };

    Ok(AttributeValue::S(formatted))
}

pub fn scalar_to_attribute_value(
    scalar: &ScalarValue,
    time_format: &str,
) -> datafusion::error::Result<AttributeValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(AttributeValue::S(s.clone()))
        }
        ScalarValue::Int8(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int16(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::UInt8(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::UInt16(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::UInt32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::UInt64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Float32(Some(f)) => {
            if f.is_finite() {
                Ok(AttributeValue::N(f.to_string()))
            } else {
                Err(DataFusionError::Execution(format!(
                    "Cannot write non-finite Float32 value ({f}) to DynamoDB"
                )))
            }
        }
        ScalarValue::Float64(Some(f)) => {
            if f.is_finite() {
                Ok(AttributeValue::N(f.to_string()))
            } else {
                Err(DataFusionError::Execution(format!(
                    "Cannot write non-finite Float64 value ({f}) to DynamoDB"
                )))
            }
        }
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            let scale = *scale;
            let s = match scale.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    let scale_u32 = u32::from(scale.unsigned_abs());
                    let divisor = 10_i128.pow(scale_u32);
                    let whole = v / divisor;
                    let frac = (v % divisor).unsigned_abs();
                    // When the value is negative but |v| < divisor, whole is 0 and
                    // the sign would be lost. Emit an explicit "-" prefix.
                    let sign = if *v < 0 && whole == 0 { "-" } else { "" };
                    format!("{sign}{whole}.{frac:0>width$}", width = scale_u32 as usize)
                }
                std::cmp::Ordering::Less => {
                    let abs_scale = u32::from(scale.unsigned_abs());
                    let multiplier = 10_i128.pow(abs_scale);
                    v.checked_mul(multiplier)
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Decimal128 value {v} with scale {scale} overflows on multiplication"
                            ))
                        })?
                        .to_string()
                }
                std::cmp::Ordering::Equal => v.to_string(),
            };
            Ok(AttributeValue::N(s))
        }
        ScalarValue::Decimal256(Some(v), _precision, scale) => {
            let scale = *scale;
            let s = match scale.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    let scale_u32 = u32::from(scale.unsigned_abs());
                    let divisor = arrow::datatypes::i256::from_i128(10_i128.pow(scale_u32));
                    let whole = v.wrapping_div(divisor);
                    let frac = v.wrapping_rem(divisor).wrapping_abs();
                    // Same sign-loss fix as Decimal128.
                    let sign = if v.is_negative() && whole == arrow::datatypes::i256::ZERO {
                        "-"
                    } else {
                        ""
                    };
                    // i256::Display does not forward fill/width, so convert first.
                    let frac_str = format!("{frac}");
                    format!(
                        "{sign}{whole}.{frac_str:0>width$}",
                        width = scale_u32 as usize
                    )
                }
                std::cmp::Ordering::Less => {
                    let abs_scale = u32::from(scale.unsigned_abs());
                    let multiplier = arrow::datatypes::i256::from_i128(10_i128.pow(abs_scale));
                    let result = v.checked_mul(multiplier).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Decimal256 value {v} with scale {scale} overflows on multiplication"
                        ))
                    })?;
                    format!("{result}")
                }
                std::cmp::Ordering::Equal => format!("{v}"),
            };
            Ok(AttributeValue::N(s))
        }
        ScalarValue::Boolean(Some(b)) => Ok(AttributeValue::Bool(*b)),
        ScalarValue::Date32(Some(days)) => {
            match NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|d| d.checked_add_signed(chrono::Duration::days(i64::from(*days))))
            {
                Some(date) => Ok(AttributeValue::S(date.format("%Y-%m-%d").to_string())),
                None => Err(DataFusionError::Execution(format!(
                    "Invalid Date32 value: {days}"
                ))),
            }
        }
        // Date64: ms since 1970-01-01
        ScalarValue::Date64(Some(ms)) => {
            // Use div_euclid to floor toward negative infinity for pre-epoch dates.
            // Truncating division (`/`) would give the wrong day for negative ms
            // values with a non-zero sub-day component (e.g. -1 ms → day 0 instead of -1).
            let days = ms.div_euclid(86_400_000);
            match NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|d| d.checked_add_signed(chrono::Duration::days(days)))
            {
                Some(date) => Ok(AttributeValue::S(date.format("%Y-%m-%d").to_string())),
                None => Err(DataFusionError::Execution(format!(
                    "Invalid Date64 value: {ms}"
                ))),
            }
        }
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => Ok(AttributeValue::B(
            aws_sdk_dynamodb::primitives::Blob::new(b.clone()),
        )),
        ScalarValue::TimestampSecond(Some(s), tz_opt) => {
            let millis = s.checked_mul(1_000).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "TimestampSecond value {s} overflows when converting to milliseconds"
                ))
            })?;
            timestamp_to_attribute(millis, tz_opt.as_ref(), time_format)
        }
        ScalarValue::TimestampMillisecond(Some(ms), tz_opt) => {
            timestamp_to_attribute(*ms, tz_opt.as_ref(), time_format)
        }
        ScalarValue::TimestampMicrosecond(Some(us), tz_opt) => {
            timestamp_to_attribute(*us / 1_000, tz_opt.as_ref(), time_format)
        }
        ScalarValue::TimestampNanosecond(Some(ns), tz_opt) => {
            timestamp_to_attribute(*ns / 1_000_000, tz_opt.as_ref(), time_format)
        }
        ScalarValue::Null => Ok(AttributeValue::Null(true)),
        other if other.is_null() => Ok(AttributeValue::Null(true)),
        _ => Err(DataFusionError::NotImplemented(
            "ScalarValue type not supported".to_string(),
        )),
    }
}

pub struct FilterStringVisitor<'a> {
    schema: &'a DynamoDBTableSchema,
    attribute_values: &'a mut HashMap<String, AttributeValue>,
    value_counter: &'a mut usize,
    pub result_stack: Vec<String>,
    pub error: Option<DataFusionError>,
}

impl<'a> FilterStringVisitor<'a> {
    pub fn new(
        schema: &'a DynamoDBTableSchema,
        attribute_values: &'a mut HashMap<String, AttributeValue>,
        value_counter: &'a mut usize,
    ) -> Self {
        Self {
            schema,
            attribute_values,
            value_counter,
            result_stack: Vec::new(),
            error: None,
        }
    }

    fn get_column_alias(&self, column_name: &str) -> String {
        if self.schema.is_flattened_field(column_name) {
            column_name
                .split('.')
                .map(|segment| format!("#{segment}"))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            format!("#{column_name}")
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for FilterStringVisitor<'_> {
    type Node = Expr;

    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        if self.error.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }

        match node {
            Expr::Column(col) => {
                self.result_stack.push(self.get_column_alias(col.name()));
                Ok(TreeNodeRecursion::Continue)
            }
            Expr::Literal(scalar, _) => {
                let value_key = format!(":v{}", self.value_counter);
                *self.value_counter += 1;

                match scalar_to_attribute_value(scalar, &self.schema.time_format()) {
                    Ok(attr_value) => {
                        self.attribute_values.insert(value_key.clone(), attr_value);
                        self.result_stack.push(value_key);
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Err(e) => {
                        self.error = Some(e);
                        Ok(TreeNodeRecursion::Stop)
                    }
                }
            }
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                let Some(right_str) = self.result_stack.pop() else {
                    self.error = Some(DataFusionError::Internal(
                        "Missing right operand in result stack".to_string(),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                };

                let Some(left_str) = self.result_stack.pop() else {
                    self.error = Some(DataFusionError::Internal(
                        "Missing left operand in result stack".to_string(),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                };

                let op_str = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => {
                        self.error = Some(DataFusionError::NotImplemented(format!(
                            "Operator {op:?} not supported"
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                };

                self.result_stack
                    .push(format!("({left_str} {op_str} {right_str})"));
                Ok(TreeNodeRecursion::Continue)
            }
            _ => {
                self.error = Some(DataFusionError::NotImplemented(
                    "Expression type not supported in filters".to_string(),
                ));
                Ok(TreeNodeRecursion::Stop)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;

    fn assert_number(scalar: &ScalarValue, expected: &str) {
        let result = scalar_to_attribute_value(scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("scalar_to_attribute_value should convert numeric scalars to numbers");
        match result {
            AttributeValue::N(n) => assert_eq!(n, expected, "for scalar {scalar:?}"),
            other => panic!("Expected AttributeValue::N, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Decimal128 regression tests
    // -----------------------------------------------------------------------

    #[test]
    fn decimal128_positive_whole_and_fraction() {
        // 12345 with scale 2 → 123.45
        let scalar = ScalarValue::Decimal128(Some(12345), 10, 2);
        assert_number(&scalar, "123.45");
    }

    #[test]
    fn decimal128_negative_whole_and_fraction() {
        // -12345 with scale 2 → -123.45
        let scalar = ScalarValue::Decimal128(Some(-12345), 10, 2);
        assert_number(&scalar, "-123.45");
    }

    #[test]
    fn decimal128_negative_fraction_only() {
        // Regression: -5 with scale 1 → -0.5 (sign must not be lost)
        let scalar = ScalarValue::Decimal128(Some(-5), 10, 1);
        assert_number(&scalar, "-0.5");
    }

    #[test]
    fn decimal128_negative_small_fraction() {
        // -1 with scale 2 → -0.01
        let scalar = ScalarValue::Decimal128(Some(-1), 10, 2);
        assert_number(&scalar, "-0.01");
    }

    #[test]
    fn decimal128_positive_fraction_only() {
        // 5 with scale 1 → 0.5
        let scalar = ScalarValue::Decimal128(Some(5), 10, 1);
        assert_number(&scalar, "0.5");
    }

    #[test]
    fn decimal128_zero() {
        let scalar = ScalarValue::Decimal128(Some(0), 10, 2);
        assert_number(&scalar, "0.00");
    }

    #[test]
    fn decimal128_no_scale() {
        let scalar = ScalarValue::Decimal128(Some(42), 10, 0);
        assert_number(&scalar, "42");
    }

    // -----------------------------------------------------------------------
    // Decimal256 regression tests
    // -----------------------------------------------------------------------

    #[test]
    fn decimal256_positive_with_scale() {
        // 12345 with scale 2 → 123.45
        let v = arrow::datatypes::i256::from_i128(12345);
        let scalar = ScalarValue::Decimal256(Some(v), 20, 2);
        assert_number(&scalar, "123.45");
    }

    #[test]
    fn decimal256_negative_with_scale() {
        // -12345 with scale 2 → -123.45
        let v = arrow::datatypes::i256::from_i128(-12345);
        let scalar = ScalarValue::Decimal256(Some(v), 20, 2);
        assert_number(&scalar, "-123.45");
    }

    #[test]
    fn decimal256_negative_fraction_only() {
        // -5 with scale 1 → -0.5
        let v = arrow::datatypes::i256::from_i128(-5);
        let scalar = ScalarValue::Decimal256(Some(v), 20, 1);
        assert_number(&scalar, "-0.5");
    }

    #[test]
    fn decimal256_no_scale() {
        // No scale → raw integer
        let v = arrow::datatypes::i256::from_i128(42);
        let scalar = ScalarValue::Decimal256(Some(v), 20, 0);
        assert_number(&scalar, "42");
    }

    #[test]
    fn decimal256_zero_with_scale() {
        let v = arrow::datatypes::i256::from_i128(0);
        let scalar = ScalarValue::Decimal256(Some(v), 20, 3);
        assert_number(&scalar, "0.000");
    }

    // -----------------------------------------------------------------------
    // Negative scale regression tests
    // -----------------------------------------------------------------------

    #[test]
    fn decimal128_negative_scale() {
        // 123 with scale -2 → 12300
        let scalar = ScalarValue::Decimal128(Some(123), 10, -2);
        assert_number(&scalar, "12300");
    }

    #[test]
    fn decimal128_negative_scale_negative_value() {
        // -5 with scale -3 → -5000
        let scalar = ScalarValue::Decimal128(Some(-5), 10, -3);
        assert_number(&scalar, "-5000");
    }

    #[test]
    fn decimal256_negative_scale() {
        let v = arrow::datatypes::i256::from_i128(123);
        let scalar = ScalarValue::Decimal256(Some(v), 20, -2);
        assert_number(&scalar, "12300");
    }

    #[test]
    fn decimal128_negative_scale_overflow_returns_error() {
        let scalar = ScalarValue::Decimal128(Some(i128::MAX), 38, -2);
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(result.is_err(), "overflow should produce an error");
    }

    #[test]
    fn decimal256_negative_scale_overflow_returns_error() {
        let v = arrow::datatypes::i256::from_parts(u128::MAX, i128::MAX);
        let scalar = ScalarValue::Decimal256(Some(v), 76, -2);
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(result.is_err(), "overflow should produce an error");
    }

    // -----------------------------------------------------------------------
    // Date64 pre-epoch regression tests
    // -----------------------------------------------------------------------

    fn assert_date(scalar: &ScalarValue, expected: &str) {
        let result = scalar_to_attribute_value(scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("scalar_to_attribute_value should convert date scalars");
        match result {
            AttributeValue::S(s) => assert_eq!(s, expected, "for scalar {scalar:?}"),
            other => panic!("Expected AttributeValue::S, got {other:?}"),
        }
    }

    #[test]
    fn date64_epoch() {
        let scalar = ScalarValue::Date64(Some(0));
        assert_date(&scalar, "1970-01-01");
    }

    #[test]
    fn date64_pre_epoch_exact_day() {
        // Exactly -1 day = -86_400_000 ms
        let scalar = ScalarValue::Date64(Some(-86_400_000));
        assert_date(&scalar, "1969-12-31");
    }

    #[test]
    fn date64_pre_epoch_sub_day() {
        // -1 ms should floor to the previous day (1969-12-31), not truncate to epoch day
        let scalar = ScalarValue::Date64(Some(-1));
        assert_date(&scalar, "1969-12-31");
    }

    // -----------------------------------------------------------------------
    // Float NaN/inf rejection tests
    // -----------------------------------------------------------------------

    #[test]
    fn float32_nan_rejected() {
        let scalar = ScalarValue::Float32(Some(f32::NAN));
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(result.is_err(), "NaN should be rejected");
    }

    #[test]
    fn float64_infinity_rejected() {
        let scalar = ScalarValue::Float64(Some(f64::INFINITY));
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(result.is_err(), "Infinity should be rejected");
    }

    #[test]
    fn float64_neg_infinity_rejected() {
        let scalar = ScalarValue::Float64(Some(f64::NEG_INFINITY));
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(result.is_err(), "Negative infinity should be rejected");
    }

    #[test]
    fn float64_finite_accepted() {
        let scalar = ScalarValue::Float64(Some(2.719));
        assert_number(&scalar, "2.719");
    }

    // -----------------------------------------------------------------------
    // Typed NULL handling
    // -----------------------------------------------------------------------

    #[test]
    fn typed_null_utf8_produces_dynamodb_null() {
        let scalar = ScalarValue::Utf8(None);
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("typed Utf8 null should produce DynamoDB NULL");
        assert_eq!(result, AttributeValue::Null(true));
    }

    #[test]
    fn typed_null_int32_produces_dynamodb_null() {
        let scalar = ScalarValue::Int32(None);
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("typed Int32 null should produce DynamoDB NULL");
        assert_eq!(result, AttributeValue::Null(true));
    }

    #[test]
    fn untyped_null_produces_dynamodb_null() {
        let scalar = ScalarValue::Null;
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("untyped null should produce DynamoDB NULL");
        assert_eq!(result, AttributeValue::Null(true));
    }

    // -----------------------------------------------------------------------
    // Overflow handling
    // -----------------------------------------------------------------------

    #[test]
    fn timestamp_second_overflow_returns_error() {
        let scalar = ScalarValue::TimestampSecond(Some(i64::MAX), None);
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z");
        assert!(
            result.is_err(),
            "TimestampSecond near i64::MAX should fail on overflow"
        );
    }

    // -----------------------------------------------------------------------
    // Additional type coverage
    // -----------------------------------------------------------------------

    #[test]
    fn int8_accepted() {
        let scalar = ScalarValue::Int8(Some(42));
        assert_number(&scalar, "42");
    }

    #[test]
    fn int16_accepted() {
        let scalar = ScalarValue::Int16(Some(-1000));
        assert_number(&scalar, "-1000");
    }

    #[test]
    fn large_utf8_accepted() {
        let scalar = ScalarValue::LargeUtf8(Some("hello".to_string()));
        let result = scalar_to_attribute_value(&scalar, "%Y-%m-%dT%H:%M:%S%z")
            .expect("LargeUtf8 should be accepted");
        assert_eq!(result, AttributeValue::S("hello".to_string()));
    }
}
