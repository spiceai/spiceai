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
use arrow::datatypes::{Field, Schema};
use datafusion::logical_expr::{col, lit};
use std::collections::HashSet;
use std::sync::Arc;

const TIME_FORMAT: &str = "2006-01-02T15:04:05.000Z07:00";

fn schema() -> DynamoDBTableSchema {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("sk", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("first name", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("vip", DataType::Boolean, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            true,
        ),
        Field::new("day", DataType::Date32, true),
        Field::new("data", DataType::Utf8, true),
    ]));
    DynamoDBTableSchema::new(
        Arc::from("t"),
        schema,
        "pk".to_string(),
        Some("sk".to_string()),
        HashSet::new(),
        TIME_FORMAT,
    )
    .with_key_types(Some(ScalarAttributeType::S), Some(ScalarAttributeType::S))
    .with_catch_all(Some("data".to_string()))
}

/// The condition, its exactness, and the placeholders it defined.
fn translate(expr: &Expr) -> Option<(String, bool, Placeholders)> {
    let schema = schema();
    let mut out = Placeholders::default();
    let condition = Translator::new(&schema).condition(expr, &mut out)?;
    Some((condition.expression, condition.exact, out))
}

fn value(out: &Placeholders, placeholder: &str) -> AttributeValue {
    out.values
        .iter()
        .find(|(p, _)| p == placeholder)
        .map_or_else(|| panic!("no value {placeholder}"), |(_, v)| v.clone())
}

fn n(s: &str) -> AttributeValue {
    AttributeValue::N(s.to_string())
}

fn s(s: &str) -> AttributeValue {
    AttributeValue::S(s.to_string())
}

#[test]
fn any_attribute_name_gets_a_valid_placeholder() {
    let (expression, _, out) = translate(&col("first name").eq(lit("Ann"))).expect("translatable");
    assert_eq!(expression, "(#n0 = :v0 OR attribute_type(#n0, :v1))");
    assert_eq!(
        out.names,
        vec![("#n0".to_string(), "first name".to_string())]
    );
}

#[test]
fn a_string_attribute_also_selects_maps_rendered_as_json() {
    let (expression, exact, out) = translate(&col("name").eq(lit("x"))).expect("translatable");
    assert_eq!(expression, "(#n0 = :v0 OR attribute_type(#n0, :v1))");
    assert!(!exact);
    assert_eq!(value(&out, ":v1"), s("M"));
    // `<>` already holds for a map.
    let (expression, exact, _) = translate(&col("name").not_eq(lit("x"))).expect("translatable");
    assert_eq!(expression, "#n0 <> :v0");
    assert!(!exact);
}

#[test]
fn a_string_key_comparison_is_exact() {
    for expr in [
        col("pk").eq(lit("a")),
        col("sk").gt(lit("a")),
        col("sk").lt_eq(lit("a")),
        col("sk").between(lit("a"), lit("b")),
        col("sk").like(lit("a%")),
        col("pk").in_list(vec![lit("a"), lit("b")], false),
    ] {
        let (_, exact, _) = translate(&expr).expect("translatable");
        assert!(exact, "{expr} should be exact");
    }
}

#[test]
fn integer_equality_is_exact_and_ranges_are_not() {
    let (expression, exact, out) = translate(&col("n").eq(lit(5_i64))).expect("translatable");
    assert_eq!(expression, "#n0 = :v0");
    assert!(exact);
    assert_eq!(value(&out, ":v0"), n("5"));
    // A number that is not an integer reads as NULL, and `> 5` still selects it.
    assert!(!translate(&col("n").gt(lit(5_i64))).expect("translatable").1);
    assert!(
        translate(&col("n").in_list(vec![lit(1_i64), lit(2_i64)], false))
            .expect("translatable")
            .1
    );
}

#[test]
fn a_float_comparison_is_widened_by_an_ulp() {
    let x = 0.1_f64;
    let (expression, exact, out) = translate(&col("score").eq(lit(x))).expect("translatable");
    assert_eq!(expression, "#n0 BETWEEN :v0 AND :v1");
    assert!(!exact);
    assert_eq!(value(&out, ":v0"), n(&x.next_down().to_string()));
    assert_eq!(value(&out, ":v1"), n(&x.next_up().to_string()));

    let (expression, _, out) = translate(&col("score").gt_eq(lit(x))).expect("translatable");
    assert_eq!(expression, "#n0 >= :v0");
    assert_eq!(value(&out, ":v0"), n(&x.next_down().to_string()));
    // Rounding never carries a decimal across `x`, so a strict bound stays.
    let (expression, _, out) = translate(&col("score").gt(lit(x))).expect("translatable");
    assert_eq!(expression, "#n0 > :v0");
    assert_eq!(value(&out, ":v0"), n("0.1"));
    // Beyond what a plain decimal renders, nothing is pushed.
    assert!(translate(&col("score").eq(lit(1e300))).is_none());
    // Only zero itself reads as 0.0.
    let (expression, _, out) = translate(&col("score").eq(lit(0.0))).expect("translatable");
    assert_eq!(expression, "#n0 BETWEEN :v0 AND :v1");
    assert_eq!((value(&out, ":v0"), value(&out, ":v1")), (n("0"), n("0")));
}

#[test]
fn a_reversed_between_and_a_null_test_on_a_key_are_left_to_datafusion() {
    assert!(translate(&col("n").between(lit(5_i64), lit(1_i64))).is_none());
    assert!(translate(&col("sk").is_not_null()).is_none());
    assert!(translate(&col("pk").is_null()).is_none());
}

#[test]
fn a_timestamp_is_compared_as_strings_widened_by_every_offset() {
    // 2024-09-03T12:34:56.155Z
    let ts = lit(ScalarValue::TimestampMillisecond(
        Some(1_725_366_896_155),
        Some("+00:00".into()),
    ));
    let (expression, exact, out) = translate(&col("ts").gt_eq(ts.clone())).expect("translatable");
    assert_eq!(expression, "#n0 >= :v0");
    assert!(!exact);
    // chrono reads any offset under a day.
    assert_eq!(value(&out, ":v0"), s("2024-09-02T12:34:56.155"));
    let (expression, _, out) = translate(&col("ts").eq(ts)).expect("translatable");
    assert_eq!(expression, "#n0 BETWEEN :v0 AND :v1");
    assert_eq!(value(&out, ":v0"), s("2024-09-02T12:34:56.155"));
    assert_eq!(value(&out, ":v1"), s("2024-09-04T12:34:56.155~"));
}

#[test]
fn a_layout_finer_than_the_column_bounds_the_whole_millisecond() {
    // `2024-01-01T00:00:00.123456` reads as `.123`, so a bound on `.123` has
    // to reach every microsecond of it.
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]));
    let schema = DynamoDBTableSchema::new(
        Arc::from("t"),
        schema,
        "pk".to_string(),
        None,
        HashSet::new(),
        "2006-01-02T15:04:05.000000",
    );
    let millis = lit(ScalarValue::TimestampMillisecond(
        Some(1_704_067_200_123),
        None,
    ));
    let mut out = Placeholders::default();
    let condition = Translator::new(&schema)
        .condition(&col("ts").eq(millis), &mut out)
        .expect("translatable");
    assert_eq!(condition.expression, "#n0 BETWEEN :v0 AND :v1");
    assert_eq!(value(&out, ":v0"), s("2024-01-01T00:00:00.123000"));
    assert_eq!(value(&out, ":v1"), s("2024-01-01T00:00:00.123999~"));
}

#[test]
fn a_zero_bound_takes_in_both_zeros() {
    // Arrow orders -0.0 below 0.0; DynamoDB holds them equal and stores
    // neither sign, so every zero it returns reads as 0.0.
    let negative_zero = lit(ScalarValue::Float64(Some(-0.0)));
    let (expression, exact, _) =
        translate(&col("score").gt(negative_zero.clone())).expect("translatable");
    assert_eq!(expression, "#n0 >= :v0");
    assert!(!exact);
    let (expression, _, out) =
        translate(&col("score").not_eq(negative_zero)).expect("translatable");
    assert_eq!(expression, "attribute_type(#n0, :v0)");
    assert_eq!(value(&out, ":v0"), s("N"));
    let (expression, _, _) = translate(&col("score").lt(lit(0.0_f64))).expect("translatable");
    assert_eq!(expression, "#n0 <= :v0");
    let (expression, _, _) = translate(&col("score").gt(lit(1.5_f64))).expect("translatable");
    assert_eq!(expression, "#n0 > :v0");
}

#[test]
fn a_null_disjunct_leaves_the_other_side_as_a_superset() {
    // `x IN (1, NULL)` reaches the scan as `x = 1 OR NULL`.
    let null = lit(ScalarValue::Boolean(None));
    let (expression, exact, _) =
        translate(&col("n").eq(lit(5_i64)).or(null.clone())).expect("translatable");
    assert_eq!(expression, "#n0 = :v0");
    // Exact alone, but a NOT over it would keep the rows `x` rejects.
    assert!(!exact);
    assert!(
        translate(&Expr::Not(Box::new(
            col("n").eq(lit(5_i64)).or(null.clone())
        )))
        .is_none()
    );
    let (expression, _, _) =
        translate(&null.clone().or(col("n").eq(lit(5_i64)))).expect("translatable");
    assert_eq!(expression, "#n0 = :v0");
    let schema = schema();
    assert_eq!(
        Translator::new(&schema).key_predicate(&col("pk").eq(lit("a")).or(null)),
        Some(KeyPredicate::Partition(vec![s("a")]))
    );
}

#[test]
fn a_flattened_column_takes_in_items_whose_map_key_has_a_dot() {
    // Unnesting reads `{"m": {"x.y": 1}}` into `m.x.y` as it does
    // `{"m": {"x": {"y": 1}}}`, and only the second has anything at the path.
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("m.x.y", DataType::Float64, true),
        Field::new("m.tag", DataType::Utf8, true),
    ]));
    let schema = DynamoDBTableSchema::new(
        Arc::from("t"),
        schema,
        "pk".to_string(),
        None,
        HashSet::from(["m.x.y".to_string(), "m.tag".to_string()]),
        TIME_FORMAT,
    )
    .with_unnest_depth(Some(2));
    let translator = Translator::new(&schema);
    for (expr, path) in [
        (col(r#""m.x.y""#).eq(lit(1.0_f64)), "#n0.#n1.#n2"),
        (col(r#""m.x.y""#).is_null(), "#n0.#n1.#n2"),
        (col(r#""m.x.y""#).is_not_null(), "#n0.#n1.#n2"),
        (
            col(r#""m.tag""#).in_list(vec![lit("a"), lit("b")], false),
            "#n0.#n1",
        ),
        (col(r#""m.tag""#).like(lit("a%")), "#n0.#n1"),
    ] {
        let mut out = Placeholders::default();
        let condition = translator.condition(&expr, &mut out).expect("translatable");
        assert!(!condition.exact, "{expr}");
        assert!(
            condition
                .expression
                .ends_with(&format!(" OR attribute_not_exists({path}))")),
            "{expr}: {}",
            condition.expression
        );
    }
}

#[test]
fn filters_pushed_together_fit_one_expression() {
    // 76 prefixes make a filter expression past DynamoDB's 4 KB.
    let prefixes = (0..76)
        .map(|i| {
            datafusion::functions::string::expr_fn::starts_with(
                col("name"),
                lit(format!("p{i:02}")),
            )
        })
        .reduce(Expr::or)
        .expect("prefixes");
    let schema = schema();
    let small = col("n").eq(lit(5_i64));
    assert_eq!(
        Translator::new(&schema).classify(&[&small, &prefixes, &small]),
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
            TableProviderFilterPushDown::Exact,
        ]
    );
}

#[test]
fn an_integer_sort_key_reads_a_fraction_as_null() {
    // An Int64 column reads 9007199254740992.1 as NULL, which meets no
    // predicate, even one it would round into.
    let fraction = n("9007199254740992.1");
    let member = SortPredicate::OneOf(vec![n("9007199254740992")]);
    assert_eq!(
        satisfies(&fraction, &member, KeyReading::Integer),
        Some(false)
    );
    assert_eq!(
        satisfies(
            &fraction,
            &SortPredicate::Except(vec![n("1")]),
            KeyReading::Integer
        ),
        Some(false)
    );
    assert_eq!(
        satisfies(
            &n("9223372036854775808"),
            &SortPredicate::Lower(n("0"), false),
            KeyReading::Integer
        ),
        Some(false)
    );
    assert_eq!(
        satisfies(&n("9007199254740992"), &member, KeyReading::Integer),
        Some(true)
    );
    // Stored, the key is the number itself.
    assert_eq!(
        satisfies(
            &n("9223372036854775808"),
            &SortPredicate::Lower(n("0"), false),
            KeyReading::Stored
        ),
        Some(true)
    );
}

#[test]
fn a_timestamp_layout_that_does_not_sort_as_a_string_is_not_compared() {
    assert!(
        sortable_layout(TIME_FORMAT)
            .is_some_and(|l| l.zoned && l.local == "2006-01-02T15:04:05.000")
    );
    assert!(
        sortable_layout("2006-01-02T15:04:05Z")
            .is_some_and(|l| !l.zoned && l.local == "2006-01-02T15:04:05")
    );
    assert!(sortable_layout("2006-01-02 15:04:05").is_some_and(|l| !l.zoned));
    // Trailing zeros trimmed, a 12-hour clock, a day before the month.
    assert!(sortable_layout("2006-01-02T15:04:05.999Z07:00").is_none());
    assert!(sortable_layout("2006-01-02T03:04:05Z07:00").is_none());
    assert!(sortable_layout("02-01-2006T15:04:05").is_none());
}

#[test]
fn a_date_compares_as_its_iso_string() {
    let (expression, exact, out) =
        translate(&col("day").lt(lit(ScalarValue::Date32(Some(19_723))))).expect("translatable");
    assert_eq!(expression, "#n0 < :v0");
    assert!(!exact);
    assert_eq!(value(&out, ":v0"), s("2024-01-01"));
    // A year past 9999 does not render in four digits.
    assert!(translate(&col("day").lt(lit(ScalarValue::Date32(Some(3_000_000))))).is_none());
}

#[test]
fn null_tests() {
    let (expression, exact, _) = translate(&col("name").is_null()).expect("translatable");
    assert_eq!(
        expression,
        "(NOT (attribute_type(#n0, :v0) OR attribute_type(#n0, :v1)))"
    );
    assert!(exact);
    // A number that is not an integer reads as NULL, which `attribute_type`
    // cannot tell apart.
    assert!(translate(&col("n").is_null()).is_none());
    let (expression, exact, _) = translate(&col("n").is_not_null()).expect("translatable");
    assert_eq!(expression, "attribute_type(#n0, :v0)");
    assert!(!exact);
}

#[test]
fn booleans() {
    let (expression, exact, out) = translate(&col("vip")).expect("translatable");
    assert_eq!(expression, "#n0 = :v0");
    assert!(exact);
    assert_eq!(value(&out, ":v0"), AttributeValue::Bool(true));
    let (_, _, out) = translate(&Expr::Not(Box::new(col("vip")))).expect("translatable");
    assert_eq!(value(&out, ":v0"), AttributeValue::Bool(false));
    let (expression, exact, _) = translate(&col("vip").is_not_true()).expect("translatable");
    assert_eq!(expression, "(NOT (#n0 = :v0))");
    assert!(exact);
}

#[test]
fn not_is_pushed_only_over_an_exact_condition() {
    let (expression, exact, _) =
        translate(&Expr::Not(Box::new(col("n").eq(lit(5_i64))))).expect("translatable");
    assert_eq!(expression, "(NOT (#n0 = :v0))");
    // It keeps an item missing the attribute, which SQL evaluates to NULL.
    assert!(!exact);
    assert!(translate(&Expr::Not(Box::new(col("name").eq(lit("x"))))).is_none());
}

#[test]
fn like_is_pushed_as_a_prefix_only() {
    let (expression, _, out) = translate(&col("name").like(lit("a\\%b%"))).expect("translatable");
    assert_eq!(
        expression,
        "(begins_with(#n0, :v0) OR attribute_type(#n0, :v1))"
    );
    assert_eq!(value(&out, ":v0"), s("a%b"));
    assert!(translate(&col("name").like(lit("a_c%"))).is_none());
    assert!(translate(&col("name").like(lit("%a"))).is_none());
    assert!(translate(&col("name").like(lit("%"))).is_none());
    assert!(translate(&col("name").ilike(lit("a%"))).is_none());
}

#[test]
fn only_the_built_in_starts_with_is_pushed() {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};

    let built_in = datafusion::functions::string::expr_fn::starts_with(col("sk"), lit("ORDER#"));
    let (expression, exact, _) = translate(&built_in).expect("translatable");
    assert_eq!(expression, "begins_with(#n0, :v0)");
    assert!(exact);

    // A function registered under the same name is not DataFusion's.
    let user = create_udf(
        "starts_with",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    );
    assert!(translate(&user.call(vec![col("sk"), lit("ORDER#")])).is_none());
}

#[test]
fn a_catch_all_column_names_no_attribute() {
    assert!(translate(&col("data").eq(lit("x"))).is_none());
    assert!(translate(&col("data").is_null()).is_none());
}

#[test]
fn a_column_to_column_comparison_selects_maps_on_either_side() {
    let (expression, exact, _) = translate(&col("pk").eq(col("name"))).expect("translatable");
    assert_eq!(
        expression,
        "(#n0 = #n1 OR attribute_type(#n0, :v0) OR attribute_type(#n1, :v0))"
    );
    assert!(!exact);
    assert!(translate(&col("score").eq(col("score"))).is_none());
}

#[test]
fn a_failed_half_leaves_no_placeholder() {
    let schema = schema();
    let mut out = Placeholders::default();
    // The right side cannot be translated, so the disjunction is not either.
    let expr = col("name").eq(lit("x")).or(col("score").eq(lit(1e300)));
    assert!(
        Translator::new(&schema)
            .condition(&expr, &mut out)
            .is_none()
    );
    assert!(out.names.is_empty() && out.values.is_empty());
    // A conjunction keeps the side that translates, and only its placeholders.
    let expr = col("score").eq(lit(1e300)).and(col("n").eq(lit(1_i64)));
    let condition = Translator::new(&schema)
        .condition(&expr, &mut out)
        .expect("translatable");
    assert_eq!(condition.expression, "#n0 = :v0");
    assert_eq!(out.names.len(), 1);
    assert_eq!(out.values.len(), 1);
}

#[test]
fn a_key_declared_with_a_type_its_attribute_cannot_hold_is_not_pushed() {
    let schema =
        schema().with_key_types(Some(ScalarAttributeType::N), Some(ScalarAttributeType::S));
    let mut out = Placeholders::default();
    assert!(
        Translator::new(&schema)
            .condition(&col("pk").eq(lit("a")), &mut out)
            .is_none()
    );
}

#[test]
fn key_predicates() {
    let schema = schema();
    let translator = Translator::new(&schema);
    assert_eq!(
        translator.key_predicate(&col("pk").eq(lit("a"))),
        Some(KeyPredicate::Partition(vec![s("a")]))
    );
    assert_eq!(
        translator.key_predicate(&lit("a").eq(col("pk"))),
        Some(KeyPredicate::Partition(vec![s("a")]))
    );
    // A key compared with another column is no key condition.
    assert_eq!(translator.key_predicate(&col("pk").eq(col("name"))), None);
    assert_eq!(
        translator.key_predicate(&col("sk").gt(lit("a"))),
        Some(KeyPredicate::Sort(SortPredicate::Lower(s("a"), false)))
    );
    assert_eq!(
        translator.key_predicate(&col("sk").like(lit("ORDER#%"))),
        Some(KeyPredicate::Sort(SortPredicate::Prefix(
            "ORDER#".to_string()
        )))
    );
    // Checked item by item: no key condition states an exclusion.
    assert_eq!(
        translator.key_predicate(&col("sk").not_eq(lit("a"))),
        Some(KeyPredicate::Sort(SortPredicate::Except(vec![s("a")])))
    );
    assert_eq!(
        translator.key_predicate(&col("pk").not_eq(lit("a"))),
        Some(KeyPredicate::PartitionExcept(vec![s("a")]))
    );
}

#[test]
fn the_end_of_a_prefix_range() {
    assert_eq!(prefix_end("ORDER#").as_deref(), Some("ORDER$"));
    // The last code point has no successor, so the character before it is
    // incremented instead.
    assert_eq!(prefix_end("a\u{10FFFF}").as_deref(), Some("b"));
    // Surrogates are not characters, and are skipped.
    assert_eq!(prefix_end("\u{D7FF}").as_deref(), Some("\u{E000}"));
    assert_eq!(prefix_end("\u{10FFFF}"), None);
}

#[test]
fn a_flattened_column_deeper_than_unnesting_is_not_pushed_down() {
    // With one level of unnesting, `m.x.y` comes from the map key "x.y" of
    // `m`, while the path `m.x.y` reaches `{"m": {"x": {"y": …}}}`, which
    // unnesting leaves as the JSON of `m.x`.
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("m.x.y", DataType::Float64, true),
        Field::new("m.x", DataType::Utf8, true),
    ]));
    let schema = DynamoDBTableSchema::new(
        Arc::from("t"),
        schema,
        "pk".to_string(),
        None,
        HashSet::from(["m".to_string(), "m.x.y".to_string(), "m.x".to_string()]),
        TIME_FORMAT,
    )
    .with_unnest_depth(Some(1));
    let translator = Translator::new(&schema);
    let mut out = Placeholders::default();
    assert!(
        translator
            .condition(&col(r#""m.x.y""#).eq(lit(1.0_f64)), &mut out)
            .is_none()
    );
    assert!(
        translator
            .condition(&col(r#""m.x""#).eq(lit("a")), &mut out)
            .is_some()
    );
}

#[test]
fn a_map_unnesting_flattens_away_leaves_the_column_null() {
    // With unnesting, `{"m": {"x": 1}}` becomes a column `m.x`, and `m` NULL.
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("m", DataType::Utf8, true),
    ]));
    let nested = |depth| {
        DynamoDBTableSchema::new(
            Arc::from("t"),
            Arc::clone(&schema),
            "pk".to_string(),
            None,
            HashSet::from(["m".to_string()]),
            TIME_FORMAT,
        )
        .with_unnest_depth(depth)
    };
    let condition = |schema: &DynamoDBTableSchema, expr: Expr| {
        let mut out = Placeholders::default();
        let condition = Translator::new(schema)
            .condition(&expr, &mut out)
            .expect("translatable");
        (condition.expression, condition.exact, out)
    };
    let (expression, exact, out) = condition(&nested(Some(1)), col("m").is_null());
    assert_eq!(expression, "(NOT (attribute_type(#n0, :v0)))");
    assert!(exact);
    assert_eq!(value(&out, ":v0"), s("S"));
    // Without unnesting a map renders as JSON, so it is a value.
    let (expression, _, _) = condition(&nested(None), col("m").is_null());
    assert_eq!(
        expression,
        "(NOT (attribute_type(#n0, :v0) OR attribute_type(#n0, :v1)))"
    );
}

#[test]
fn a_negation_is_parenthesized_once() {
    // DynamoDB refuses `NOT ((a OR b))` as redundant parentheses.
    let (expression, _, _) = translate(&col("name").is_null()).expect("translatable");
    assert_eq!(
        expression,
        "(NOT (attribute_type(#n0, :v0) OR attribute_type(#n0, :v1)))"
    );
    let (expression, _, _) = translate(&Expr::Not(Box::new(
        col("n").eq(lit(5_i64)).and(col("vip").eq(lit(true))),
    )))
    .expect("translatable");
    assert_eq!(expression, "(NOT (#n0 = :v0 AND #n1 = :v1))");
    let (expression, _, _) =
        translate(&Expr::Not(Box::new(col("n").eq(lit(5_i64))))).expect("translatable");
    assert_eq!(expression, "(NOT (#n0 = :v0))");
    assert!(is_parenthesized("(a AND (b))"));
    assert!(!is_parenthesized("(a) AND (b)"));
    assert!(!is_parenthesized("a"));
}

#[test]
fn a_timestamp_is_read_only_in_the_layout_digit_for_digit() {
    // chrono reads `2024-1-02`, which sorts apart from `2024-01-02`.
    assert!(fits_layout("2024-01-02T00:00:00.000Z", TIME_FORMAT));
    assert!(fits_layout("2024-01-02T00:00:00.000+05:30", TIME_FORMAT));
    assert!(!fits_layout("2024-1-02T00:00:00.000Z", TIME_FORMAT));
    assert!(!fits_layout("2024-01-02T0:00:00.000Z", TIME_FORMAT));
    assert!(!fits_layout("2024-01-02", TIME_FORMAT));
    // A layout whose values do not sort as strings is not pushed down, so
    // chrono reads it as it will.
    assert!(fits_layout("Jan 2, 2024", "Jan 2, 2006"));
}
