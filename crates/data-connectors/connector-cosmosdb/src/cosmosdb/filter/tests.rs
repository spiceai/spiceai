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

use super::*;
use datafusion::arrow::datatypes::Field;
use datafusion::logical_expr::{col, lit};
use std::sync::Arc;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("first name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("vip", DataType::Boolean, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ])
}

fn translate(expr: &Expr) -> Option<(String, Vec<(String, Value)>)> {
    let schema = schema();
    let mut parameters = Parameters::default();
    let condition = Translator::new(&schema).condition(expr, &mut parameters)?;
    Some((condition, parameters.into_named()))
}

#[test]
fn a_property_reference_reaches_any_name() {
    assert_eq!(property("status"), r#"c["status"]"#);
    assert_eq!(property("first name"), r#"c["first name"]"#);
    assert_eq!(property(r#"a"b\c"#), r#"c["a\"b\\c"]"#);
}

#[test]
fn a_comparison_is_guarded_by_its_type() {
    let (condition, parameters) =
        translate(&col("status").not_eq(lit("active"))).expect("translatable");
    // Null and undefined properties are left out by the type check, however
    // the service compares them; a non-string is kept to fail decoding.
    assert_eq!(
        condition,
        r#"((IS_STRING(c["status"]) AND c["status"] != @p0) OR (IS_DEFINED(c["status"]) AND NOT IS_NULL(c["status"]) AND NOT IS_STRING(c["status"])))"#
    );
    assert_eq!(parameters, vec![("@p0".to_string(), Value::from("active"))]);
}

#[test]
fn an_integer_bound_takes_in_the_fractions_that_truncate_to_it() {
    let (condition, parameters) = translate(&col("age").gt_eq(lit(30_i64))).expect("translatable");
    assert!(condition.contains(r#"c["age"] > @p0"#), "{condition}");
    assert_eq!(parameters[0].1, Value::from(29));
    let (condition, parameters) = translate(&col("age").eq(lit(30_i64))).expect("translatable");
    assert!(
        condition.contains(r#"(c["age"] > @p0 AND c["age"] < @p1)"#),
        "{condition}"
    );
    assert_eq!(
        (parameters[0].1.clone(), parameters[1].1.clone()),
        (Value::from(29), Value::from(31))
    );
    // Beyond 2^53 a bound is not exact as a double.
    assert!(translate(&col("age").eq(lit(9_007_199_254_740_993_i64))).is_none());
}

#[test]
fn a_zero_float_bound_takes_in_both_zeros() {
    let (condition, _) = translate(&col("score").lt(lit(0.0))).expect("translatable");
    assert!(condition.contains(r#"c["score"] <= @p0"#), "{condition}");
    let (condition, parameters) = translate(&col("score").not_eq(lit(0.0))).expect("translatable");
    assert!(condition.contains("AND true)"), "{condition}");
    assert!(parameters.is_empty());
}

#[test]
fn a_string_range_needs_an_ascii_bound() {
    assert!(translate(&col("status").gt(lit("m"))).is_some());
    assert!(translate(&col("status").gt(lit("é"))).is_none());
    // Equality does not depend on the order.
    assert!(translate(&col("status").eq(lit("é"))).is_some());
}

#[test]
fn null_tests_keep_values_the_column_cannot_hold() {
    let (condition, _) = translate(&col("age").is_null()).expect("translatable");
    assert_eq!(
        condition,
        r#"(NOT IS_DEFINED(c["age"]) OR IS_NULL(c["age"]) OR NOT IS_NUMBER(c["age"]))"#
    );
    let (condition, _) = translate(&col("age").is_not_null()).expect("translatable");
    assert_eq!(
        condition,
        r#"(IS_DEFINED(c["age"]) AND NOT IS_NULL(c["age"]))"#
    );
}

#[test]
fn in_lists_and_prefixes() {
    let (condition, parameters) =
        translate(&col("status").in_list(vec![lit("a"), lit("b")], false)).expect("translatable");
    assert!(
        condition.contains(r#"c["status"] IN (@p0, @p1)"#),
        "{condition}"
    );
    assert_eq!(parameters.len(), 2);
    // A NULL element makes NOT IN never true and IN partly NULL; neither is pushed.
    assert!(
        translate(&col("status").in_list(vec![lit("a"), lit(ScalarValue::Utf8(None))], false))
            .is_none()
    );
    let (condition, parameters) =
        translate(&col("status").like(lit("ac\\_t%"))).expect("translatable");
    assert!(
        condition.contains(r#"STARTSWITH(c["status"], @p0, false)"#),
        "{condition}"
    );
    assert_eq!(parameters[0].1, Value::from("ac_t"));
    assert!(translate(&col("status").like(lit("a_%"))).is_none());
    assert!(translate(&col("status").ilike(lit("a%"))).is_none());
}

#[test]
fn only_the_built_in_starts_with_is_pushed() {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

    let built_in = datafusion::functions::string::expr_fn::starts_with(col("status"), lit("ac"));
    assert!(translate(&built_in).is_some());
    let user = create_udf(
        "starts_with",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    );
    assert!(translate(&user.call(vec![col("status"), lit("ac")])).is_none());
}

#[test]
fn what_is_not_pushed_down() {
    assert!(translate(&col("tags").is_null()).is_none());
    assert!(translate(&col("status").eq(col("id"))).is_none());
    assert!(translate(&Expr::Not(Box::new(col("status").eq(lit("a"))))).is_none());
}

#[test]
fn a_disjunction_needs_both_sides_and_a_failed_one_allocates_nothing() {
    let schema = schema();
    let mut parameters = Parameters::default();
    let expr = col("status").eq(lit("a")).or(col("tags").is_null());
    assert!(
        Translator::new(&schema)
            .condition(&expr, &mut parameters)
            .is_none()
    );
    assert!(parameters.into_named().is_empty());
    // A conjunction keeps the side that translates.
    let (condition, parameters) =
        translate(&col("tags").is_null().and(col("vip").eq(lit(true)))).expect("translatable");
    assert!(condition.contains(r#"c["vip"] = @p0"#), "{condition}");
    assert_eq!(parameters, vec![("@p0".to_string(), Value::Bool(true))]);
}

#[test]
fn an_integer_beyond_what_a_double_holds_exactly_is_not_pushed_down() {
    // `i64::MIN` has no absolute value in `i64`, and a bound one past it
    // overflows.
    for k in [i64::MIN, i64::MIN + 1, i64::MAX, 1 << 53, -(1 << 53)] {
        assert!(translate(&col("age").eq(lit(k))).is_none(), "{k}");
        assert!(translate(&col("age").gt_eq(lit(k))).is_none(), "{k}");
    }
    assert!(translate(&col("age").eq(lit((1_i64 << 53) - 1))).is_some());
}

#[test]
fn a_null_disjunct_leaves_the_other_side() {
    // `x IN (1, NULL)` reaches the scan as `x = 1 OR NULL`.
    let null = lit(ScalarValue::Boolean(None));
    let expected = translate(&col("status").eq(lit("active"))).expect("translatable");
    assert_eq!(
        translate(&col("status").eq(lit("active")).or(null.clone())),
        Some(expected.clone())
    );
    assert_eq!(
        translate(&null.or(col("status").eq(lit("active")))),
        Some(expected)
    );
}
