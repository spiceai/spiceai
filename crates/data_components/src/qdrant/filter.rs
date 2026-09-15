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

//! Translates `DataFusion` filter [`Expr`]s into Qdrant [`Filter`]s.

use std::collections::HashSet;

use arrow_schema::SchemaRef;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use qdrant::proto::{
    Condition, FieldCondition, Filter, Match, Range, RepeatedIntegers, r#match::MatchValue,
};

/// Returns `true` when `expr` can be pushed to Qdrant exactly.
#[must_use]
pub fn supports_pushdown(schema: &SchemaRef, embedding_column: &str, expr: &Expr) -> bool {
    let payload_columns = payload_column_names(schema, embedding_column);
    expr_to_condition(&payload_columns, expr).is_some()
}

/// Converts pushable filters into a single Qdrant [`Filter`] combining them
/// with `AND`. Unpushable filters are skipped. Returns `None` when no filter
/// could be pushed.
#[must_use]
pub fn convert_filters_to_qdrant(
    schema: &SchemaRef,
    embedding_column: &str,
    filters: &[Expr],
) -> Option<Filter> {
    let payload_columns = payload_column_names(schema, embedding_column);
    let mut conditions = Vec::new();
    for filter in filters {
        if let Some(condition) = expr_to_condition(&payload_columns, filter) {
            conditions.push(condition);
        }
    }
    if conditions.is_empty() {
        None
    } else {
        Some(Filter::must(conditions))
    }
}

fn payload_column_names(schema: &SchemaRef, embedding_column: &str) -> HashSet<String> {
    schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| {
            name != embedding_column && name != super::query_provider::QDRANT_SCORE_COLUMN_NAME
        })
        .collect()
}

fn is_payload_column(payload_columns: &HashSet<String>, expr: &Expr) -> bool {
    match expr {
        Expr::Column(col) => payload_columns.contains(&col.name),
        _ => false,
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(col) => Some(col.name.as_str()),
        _ => None,
    }
}

fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

fn scalar_to_f64(scalar: &ScalarValue) -> Option<f64> {
    match scalar {
        ScalarValue::Float32(Some(v)) if v.is_finite() => Some(f64::from(*v)),
        ScalarValue::Float64(Some(v)) if v.is_finite() => Some(*v),
        _ => {
            let v = scalar_to_i64(scalar)?;
            #[expect(
                clippy::cast_precision_loss,
                reason = "integers that do not survive the round trip are rejected below, so the surviving conversion is exact"
            )]
            let as_f64 = v as f64;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "`as_f64` is integral and within the `i128` range (it came from an `i64`), so the round trip is exact whenever it is equal"
            )]
            let round_trip = as_f64 as i128;
            if i128::from(v) == round_trip {
                Some(as_f64)
            } else {
                None
            }
        }
    }
}

fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

fn keyword_equals(field: &str, value: String) -> Condition {
    Condition {
        condition_one_of: Some(qdrant::proto::condition::ConditionOneOf::Field(
            FieldCondition {
                key: field.to_string(),
                r#match: Some(Match {
                    match_value: Some(MatchValue::Keyword(value)),
                }),
                ..Default::default()
            },
        )),
    }
}

fn equality_condition(field: &str, scalar: &ScalarValue) -> Option<Condition> {
    if let Some(value) = scalar_to_string(scalar) {
        return Some(keyword_equals(field, value));
    }
    if let ScalarValue::Boolean(Some(value)) = scalar {
        return Some(Condition::matches(field, *value));
    }
    if let Some(value) = scalar_to_i64(scalar) {
        return Some(Condition::matches(field, value));
    }
    if let Some(value) = scalar_to_f64(scalar) {
        return Some(Condition::range(
            field,
            Range {
                gte: Some(value),
                lte: Some(value),
                ..Default::default()
            },
        ));
    }
    None
}

fn range_condition(field: &str, op: Operator, scalar: &ScalarValue) -> Option<Condition> {
    let value = scalar_to_f64(scalar)?;
    let range = match op {
        Operator::Lt => Range {
            lt: Some(value),
            ..Default::default()
        },
        Operator::LtEq => Range {
            lte: Some(value),
            ..Default::default()
        },
        Operator::Gt => Range {
            gt: Some(value),
            ..Default::default()
        },
        Operator::GtEq => Range {
            gte: Some(value),
            ..Default::default()
        },
        _ => return None,
    };
    Some(Condition::range(field, range))
}

fn in_list_condition(field: &str, list: &[Expr]) -> Option<Condition> {
    let strings: Option<Vec<String>> = list
        .iter()
        .map(|item| match item {
            Expr::Literal(scalar, _) => scalar_to_string(scalar),
            _ => None,
        })
        .collect();
    if let Some(strings) = strings {
        return Some(Condition::matches(field, strings));
    }
    let integers: Option<Vec<i64>> = list
        .iter()
        .map(|item| match item {
            Expr::Literal(scalar, _) => scalar_to_i64(scalar),
            _ => None,
        })
        .collect();
    Some(Condition::matches(
        field,
        MatchValue::Integers(RepeatedIntegers {
            integers: integers?,
        }),
    ))
}

fn expr_to_condition(payload_columns: &HashSet<String>, expr: &Expr) -> Option<Condition> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Some(
                Filter::must([
                    expr_to_condition(payload_columns, left)?,
                    expr_to_condition(payload_columns, right)?,
                ])
                .into(),
            ),
            Operator::Or => Some(
                Filter::should([
                    expr_to_condition(payload_columns, left)?,
                    expr_to_condition(payload_columns, right)?,
                ])
                .into(),
            ),
            Operator::Eq => {
                if !is_payload_column(payload_columns, left) {
                    return None;
                }
                let Expr::Literal(scalar, _) = right.as_ref() else {
                    return None;
                };
                equality_condition(column_name(left)?, scalar)
            }
            Operator::NotEq => {
                if !is_payload_column(payload_columns, left) {
                    return None;
                }
                let Expr::Literal(scalar, _) = right.as_ref() else {
                    return None;
                };
                let field = column_name(left)?;
                Some(
                    Filter::must([
                        Filter::must_not([equality_condition(field, scalar)?]).into(),
                        Filter::must_not([Condition::is_null(field.to_string())]).into(),
                    ])
                    .into(),
                )
            }
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                if !is_payload_column(payload_columns, left) {
                    return None;
                }
                let Expr::Literal(scalar, _) = right.as_ref() else {
                    return None;
                };
                range_condition(column_name(left)?, *op, scalar)
            }
            _ => None,
        },
        Expr::IsNull(inner) => {
            if !is_payload_column(payload_columns, inner) {
                return None;
            }
            Some(Condition::is_null(column_name(inner)?.to_string()))
        }
        Expr::IsNotNull(inner) => {
            if !is_payload_column(payload_columns, inner) {
                return None;
            }
            Some(Filter::must_not([Condition::is_null(column_name(inner)?.to_string())]).into())
        }
        Expr::InList(in_list) => {
            if in_list.negated
                || in_list.list.is_empty()
                || !is_payload_column(payload_columns, &in_list.expr)
            {
                return None;
            }
            in_list_condition(column_name(&in_list.expr)?, &in_list.list)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::logical_expr::{Operator, col, lit};
    use datafusion::prelude::Expr;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("tag", DataType::Utf8, true),
            Field::new(
                "content_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
                true,
            ),
            Field::new("_score", DataType::Float64, true),
        ]))
    }

    fn supports(expr: &Expr) -> bool {
        supports_pushdown(&schema(), "content_embedding", expr)
    }

    fn convert(expr: &Expr) -> Option<Condition> {
        let payload_columns = payload_column_names(&schema(), "content_embedding");
        expr_to_condition(&payload_columns, expr)
    }

    fn field_key(condition: &Condition) -> &str {
        match condition
            .condition_one_of
            .as_ref()
            .expect("condition has a payload")
        {
            qdrant::proto::condition::ConditionOneOf::Field(field) => field.key.as_str(),
            other => panic!("expected a field condition, got {other:?}"),
        }
    }

    #[test]
    fn equality_on_payload_column_is_supported_with_exact_keyword_match() {
        let expr = col("tag").eq(lit("b"));
        assert!(supports(&expr));
        let Some(condition) = convert(&expr) else {
            panic!("expected a condition");
        };
        assert_eq!(field_key(&condition), "tag");
        match condition.condition_one_of.expect("condition") {
            qdrant::proto::condition::ConditionOneOf::Field(field) => {
                assert_eq!(
                    field.r#match.expect("match").match_value,
                    Some(MatchValue::Keyword("b".to_string()))
                );
            }
            other => panic!("expected a field condition, got {other:?}"),
        }
    }

    #[test]
    fn multi_word_strings_stay_keyword_matches_not_full_text() {
        let Some(condition) = convert(&col("tag").eq(lit("hello world"))) else {
            panic!("expected a condition");
        };
        match condition.condition_one_of.expect("condition") {
            qdrant::proto::condition::ConditionOneOf::Field(field) => {
                assert_eq!(
                    field.r#match.expect("match").match_value,
                    Some(MatchValue::Keyword("hello world".to_string()))
                );
            }
            other => panic!("expected a field condition, got {other:?}"),
        }
    }

    #[test]
    fn embedding_and_score_columns_are_never_pushable() {
        assert!(!supports(&col("content_embedding").eq(lit("x"))));
        assert!(!supports(&col("_score").gt(lit(0.5))));
        assert!(!supports(&col("missing").eq(lit("x"))));
    }

    #[test]
    fn ranges_and_null_checks_convert() {
        assert!(supports(&col("id").gt_eq(lit(2020))));
        let Some(condition) = convert(&col("id").gt_eq(lit(2020))) else {
            panic!("expected a condition");
        };
        assert_eq!(field_key(&condition), "id");

        assert!(supports(&col("tag").is_null()));
        assert!(supports(&col("tag").is_not_null()));
        assert!(convert(&col("tag").is_null()).is_some());
        assert!(convert(&col("tag").is_not_null()).is_some());
    }

    #[test]
    fn string_ranges_and_unsupported_operators_are_rejected() {
        assert!(!supports(&col("tag").gt(lit("m"))));
        assert!(!supports(&col("id").eq(col("tag"))));
        assert!(!supports(&col("id").eq(lit(ScalarValue::Utf8(None)))));
    }

    #[test]
    fn in_list_requires_a_homogeneous_non_empty_list() {
        let strings = Expr::in_list(col("tag"), vec![lit("a"), lit("b")], false);
        assert!(supports(&strings));
        assert!(convert(&strings).is_some());

        let mixed = Expr::in_list(col("tag"), vec![lit("a"), lit(1)], false);
        assert!(!supports(&mixed));

        let negated = Expr::in_list(col("tag"), vec![lit("a"), lit("b")], true);
        assert!(!supports(&negated));

        let empty = Expr::in_list(col("tag"), vec![], false);
        assert!(!supports(&empty));
    }

    #[test]
    fn and_filters_combine_into_one_must_filter() {
        let filters = vec![
            col("tag").eq(lit("b")),
            col("id").gt_eq(lit(2020)),
            col("missing").eq(lit("x")),
        ];
        let Some(filter) = convert_filters_to_qdrant(&schema(), "content_embedding", &filters)
        else {
            panic!("expected a filter");
        };
        assert_eq!(filter.must.len(), 2);
        assert!(convert_filters_to_qdrant(&schema(), "content_embedding", &[]).is_none());
        assert!(
            convert_filters_to_qdrant(
                &schema(),
                "content_embedding",
                &[col("missing").eq(lit("x"))]
            )
            .is_none()
        );
    }

    #[test]
    fn float_equality_uses_a_degenerate_range() {
        let Some(condition) = convert(&col("id").eq(lit(1.5))) else {
            panic!("expected a condition");
        };
        match condition.condition_one_of.expect("condition") {
            qdrant::proto::condition::ConditionOneOf::Field(field) => {
                let range = field.range.expect("range");
                assert_eq!(range.gte, Some(1.5));
                assert_eq!(range.lte, Some(1.5));
            }
            other => panic!("expected a field condition, got {other:?}"),
        }
    }

    #[test]
    fn or_combines_into_should() {
        let expr = col("tag").eq(lit("a")).or(col("tag").eq(lit("b")));
        assert!(supports(&expr));
        let Some(condition) = convert(&expr) else {
            panic!("expected a condition");
        };
        match condition.condition_one_of.expect("condition") {
            qdrant::proto::condition::ConditionOneOf::Filter(filter) => {
                assert_eq!(filter.should.len(), 2);
            }
            other => panic!("expected a nested filter, got {other:?}"),
        }
    }

    #[test]
    fn operator_aliases_still_convert() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("id")),
            Operator::GtEq,
            Box::new(lit(7)),
        ));
        assert!(supports(&expr));
        assert!(convert(&expr).is_some());
    }

    #[test]
    fn not_equal_conjoins_a_null_guard() {
        let expr = col("tag").not_eq(lit("a"));
        assert!(supports(&expr));
        let Some(condition) = convert(&expr) else {
            panic!("expected a condition");
        };
        match condition.condition_one_of.expect("condition") {
            qdrant::proto::condition::ConditionOneOf::Filter(filter) => {
                assert_eq!(filter.must.len(), 2);
            }
            other => panic!("expected a must filter, got {other:?}"),
        }
    }

    #[test]
    fn integers_beyond_f64_precision_are_not_pushed() {
        let huge = col("id").gt(lit(i64::MAX));
        assert!(!supports(&huge));
        assert!(convert(&huge).is_none());

        let exact = col("id").gt(lit(1i64 << 53));
        assert!(supports(&exact));
        assert!(convert(&exact).is_some());
    }
}
