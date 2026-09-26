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
use crate::filter::SortPredicate;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use aws_sdk_dynamodb::types::ScalarAttributeType;
use datafusion::logical_expr::{col, lit};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

fn builder() -> DynamoDBRequestPlanBuilder {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new("sk", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            true,
        ),
    ]));
    DynamoDBRequestPlanBuilder::new(
        DynamoDBTableSchema::new(
            Arc::from("t"),
            schema,
            "pk".to_string(),
            Some("sk".to_string()),
            HashSet::new(),
            "2006-01-02T15:04:05.000Z07:00",
        )
        .with_key_types(Some(ScalarAttributeType::S), Some(ScalarAttributeType::S)),
    )
}

fn projection(fields: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| Field::new(*f, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

fn plan(filters: &[Expr], limit: Option<usize>) -> DynamoDBRequestPlan {
    builder()
        .build_request_plan(filters, &projection(&["pk", "sk"]), limit, None)
        .expect("plan")
}

fn queries(plan: DynamoDBRequestPlan) -> Vec<QueryParams> {
    match plan {
        DynamoDBRequestPlan::Query(queries) => queries,
        other => panic!("expected a Query, got {other:?}"),
    }
}

fn single_query(filters: &[Expr]) -> QueryParams {
    let mut queries = queries(plan(filters, None));
    assert_eq!(queries.len(), 1);
    queries.remove(0)
}

fn values(query: &QueryParams) -> HashMap<String, AttributeValue> {
    query
        .expression_attribute_values
        .clone()
        .unwrap_or_default()
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

#[test]
fn a_partition_key_equality_is_a_query_with_the_limit() {
    let mut queries = queries(plan(&[col("pk").eq(lit("u1"))], Some(10)));
    let query = queries.remove(0);
    assert_eq!(query.key_condition_expression.as_deref(), Some("#n0 = :v0"));
    assert_eq!(query.filter_expression, None);
    assert_eq!(query.limit, Some(10));
    assert_eq!(query.projection_expression.as_deref(), Some("#n0, #n1"));
    assert_eq!(values(&query).get(":v0"), Some(&s("u1")));
}

#[test]
fn a_sort_key_range_stays_in_the_key_condition() {
    // Previously two sort-key filters fell back to scanning the whole table.
    let query = single_query(&[
        col("pk").eq(lit("u1")),
        col("sk").gt_eq(lit("k1")),
        col("sk").lt_eq(lit("k2")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 BETWEEN :v1 AND :v2")
    );
    assert_eq!(query.filter_expression, None);
}

#[test]
fn a_half_open_range_reads_between_its_ends_and_checks_the_open_one() {
    // Both bounds are exact, and a key condition holds one sort-key condition:
    // BETWEEN reads the range, and the exclusive end is checked on each item.
    let query = single_query(&[
        col("pk").eq(lit("u1")),
        col("sk").gt_eq(lit("k1")),
        col("sk").lt(lit("k3")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 BETWEEN :v1 AND :v2")
    );
    assert_eq!(values(&query).get(":v2"), Some(&s("k3")));
    assert!(
        query
            .residual
            .contains(&SortPredicate::Upper(s("k3"), false))
    );
    // The residual needs the sort key, projected or not.
    assert!(
        query
            .projection_expression
            .as_deref()
            .is_some_and(|p| p.contains("#n1"))
    );
}

#[test]
fn a_lone_exclusive_bound_is_its_own_key_condition() {
    let query = single_query(&[col("pk").eq(lit("u1")), col("sk").gt(lit("k1"))]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 > :v1")
    );
    assert!(query.residual.is_empty());
}

#[test]
fn a_prefix_and_between_are_key_conditions() {
    let query = single_query(&[col("pk").eq(lit("u1")), col("sk").like(lit("ORDER#%"))]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND begins_with(#n1, :v1)")
    );
    let query = single_query(&[
        col("pk").eq(lit("u1")),
        col("sk").between(lit("a"), lit("b")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 BETWEEN :v1 AND :v2")
    );
}

#[test]
fn a_key_compared_with_a_column_is_scanned_not_rejected() {
    // Previously `#pk = #other` went into the key condition, which DynamoDB refuses.
    match plan(&[col("pk").eq(col("name"))], None) {
        DynamoDBRequestPlan::Scan(scan) => assert_eq!(
            scan.filter_expression.as_deref(),
            Some("(#n0 = #n1 OR attribute_type(#n0, :v0) OR attribute_type(#n1, :v0))")
        ),
        other => panic!("expected a Scan, got {other:?}"),
    }
    // With a partition key to query, a sort-key comparison with a column is
    // left to DataFusion: a Query's filter expression may not read the key.
    let query = single_query(&[col("pk").eq(lit("u1")), col("sk").eq(col("name"))]);
    assert_eq!(query.key_condition_expression.as_deref(), Some("#n0 = :v0"));
    assert_eq!(query.filter_expression, None);
}

#[test]
fn an_in_list_on_the_partition_key_queries_each_value() {
    let queries = queries(plan(
        &[col("pk").in_list(vec![lit("a"), lit("b"), lit("a")], false)],
        None,
    ));
    assert_eq!(queries.len(), 2);
    assert_eq!(values(&queries[0]).get(":v0"), Some(&s("a")));
    assert_eq!(values(&queries[1]).get(":v0"), Some(&s("b")));
}

#[test]
fn contradictory_key_predicates_read_nothing() {
    for filters in [
        vec![col("pk").eq(lit("a")), col("pk").eq(lit("b"))],
        vec![col("pk").eq(lit(""))],
        vec![
            col("pk").eq(lit("a")),
            col("sk").eq(lit("x")),
            col("sk").eq(lit("y")),
        ],
        vec![
            col("pk").eq(lit("a")),
            col("sk").eq(lit("a")),
            col("sk").gt_eq(lit("b")),
        ],
    ] {
        assert!(
            matches!(plan(&filters, None), DynamoDBRequestPlan::Empty),
            "{filters:?}"
        );
    }
}

#[test]
fn an_equality_that_meets_the_other_bounds_states_them() {
    let query = single_query(&[
        col("pk").eq(lit("a")),
        col("sk").eq(lit("c")),
        col("sk").gt_eq(lit("b")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 = :v1")
    );
    assert_eq!(query.filter_expression, None);
}

#[test]
fn a_prefix_with_a_bound_reads_the_prefix_range_and_checks_both() {
    let query = single_query(&[
        col("pk").eq(lit("a")),
        col("sk").like(lit("ORDER#%")),
        col("sk").gt_eq(lit("ORDER#5")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 BETWEEN :v1 AND :v2")
    );
    assert_eq!(values(&query).get(":v1"), Some(&s("ORDER#5")));
    // The least string past every `ORDER#…` one.
    assert_eq!(values(&query).get(":v2"), Some(&s("ORDER$")));
    assert!(
        query
            .residual
            .contains(&SortPredicate::Prefix("ORDER#".to_string()))
    );
}

#[test]
fn a_sort_key_membership_or_exclusion_is_checked_on_each_item() {
    let query = single_query(&[
        col("pk").eq(lit("a")),
        col("sk").in_list(vec![lit("k2"), lit("k7")], false),
        col("sk").not_eq(lit("k3")),
    ]);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 BETWEEN :v1 AND :v2")
    );
    assert_eq!(query.residual.len(), 2);
    assert_eq!(query.limit, None);
}

#[test]
fn a_partition_key_exclusion_drops_that_partition() {
    let queries = queries(plan(
        &[
            col("pk").in_list(vec![lit("a"), lit("b")], false),
            col("pk").not_eq(lit("a")),
        ],
        None,
    ));
    assert_eq!(queries.len(), 1);
    assert_eq!(values(&queries[0]).get(":v0"), Some(&s("b")));
    assert!(matches!(
        plan(&[col("pk").not_eq(lit("a"))], None),
        DynamoDBRequestPlan::Scan(_)
    ));
}

#[test]
fn other_filters_go_to_the_filter_expression_and_drop_the_limit() {
    let mut queries = queries(plan(
        &[col("pk").eq(lit("u1")), col("n").eq(lit(5_i64))],
        Some(10),
    ));
    let query = queries.remove(0);
    assert_eq!(query.filter_expression.as_deref(), Some("#n1 = :v1"));
    // DynamoDB's `Limit` counts the items read before the filter expression.
    assert_eq!(query.limit, None);
}

#[test]
fn a_timestamp_sort_bound_is_a_widened_superset() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Utf8, false),
        Field::new(
            "sk",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            false,
        ),
    ]));
    let builder = DynamoDBRequestPlanBuilder::new(
        DynamoDBTableSchema::new(
            Arc::from("t"),
            schema,
            "pk".to_string(),
            Some("sk".to_string()),
            HashSet::new(),
            "2006-01-02T15:04:05.000Z07:00",
        )
        .with_key_types(Some(ScalarAttributeType::S), Some(ScalarAttributeType::S)),
    );
    let filters = [
        col("pk").eq(lit("u1")),
        col("sk").gt_eq(lit(ScalarValue::TimestampMillisecond(
            Some(1_725_366_896_155),
            Some("+00:00".into()),
        ))),
    ];
    let plan = builder
        .build_request_plan(&filters, &projection(&["pk"]), None, None)
        .expect("plan");
    let query = queries(plan).remove(0);
    assert_eq!(
        query.key_condition_expression.as_deref(),
        Some("#n0 = :v0 AND #n1 >= :v1")
    );
    assert_eq!(
        values(&query).get(":v1"),
        Some(&s("2024-09-02T12:34:56.155"))
    );
}

#[test]
fn every_placeholder_is_used() {
    // A projection and a filter on the same attribute share its placeholder,
    // and nothing is defined that no expression reads.
    let plan = builder()
        .build_request_plan(
            &[col("name").eq(lit("x"))],
            &projection(&["name", "pk"]),
            None,
            None,
        )
        .expect("plan");
    let DynamoDBRequestPlan::Scan(scan) = plan else {
        panic!("expected a Scan");
    };
    let filter = scan.filter_expression.expect("filter");
    let projection = scan.projection_expression.expect("projection");
    for placeholder in scan.expression_attribute_names.expect("names").keys() {
        assert!(filter.contains(placeholder.as_str()) || projection.contains(placeholder.as_str()));
    }
    for placeholder in scan.expression_attribute_values.expect("values").keys() {
        assert!(filter.contains(placeholder.as_str()));
    }
}

#[test]
fn filters_beyond_one_expression_are_left_to_check_on_the_rows_read() {
    // Seven lists of a hundred integers make a filter expression past 4 KB.
    let schema = Arc::new(Schema::new(
        std::iter::once(Field::new("pk", DataType::Utf8, false))
            .chain((0..7).map(|i| Field::new(format!("a{i}"), DataType::Int64, true)))
            .collect::<Vec<_>>(),
    ));
    let builder = DynamoDBRequestPlanBuilder::new(DynamoDBTableSchema::new(
        Arc::from("t"),
        Arc::clone(&schema),
        "pk".to_string(),
        None,
        HashSet::new(),
        "2006-01-02T15:04:05.000Z07:00",
    ));
    let filters: Vec<Expr> = (0..7)
        .map(|i| {
            col(format!("a{i}")).in_list(
                (0..100).map(|k| lit(i64::from(1000 * i + k))).collect(),
                false,
            )
        })
        .collect();
    let plan = builder
        .build_request_plan(&filters, &schema, None, None)
        .expect("plan");
    assert!(!plan.fits_expression_limits());

    let (remote, local) = builder
        .split_within_limits(&filters, &schema, None)
        .expect("split");
    assert!(!local.is_empty());
    assert_eq!(remote.len() + local.len(), filters.len());
    let plan = builder
        .build_request_plan(&remote, &schema, None, None)
        .expect("plan");
    assert!(plan.fits_expression_limits(), "{plan:?}");
}
