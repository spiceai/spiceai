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

//! `DynamoDB` key-condition, filter, sort and limit pushdown, compared row for
//! row with `DataFusion` evaluating the same items locally.
//!
//! The items hold what `DynamoDB` evaluates differently from SQL over the
//! converted rows: one instant written in five offsets, one of them beyond
//! the ±14 hours of any time zone, a timestamp finer than its column, numbers
//! an integer column reads as NULL, decimals an `f64` rounds, maps a string
//! column renders as JSON, a map key that contains a dot, a date padded with a
//! space, and attribute names no bare placeholder can spell.

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use runtime::Runtime;
use spicepod::acceleration::Acceleration;
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params as DatasetParams;
use spicepod::semantic::Column;

use super::streams::{get_client, start_dynamodb_docker_container};
use crate::pushdown_roundtrip::{Case, Pushed, Tables, assert_round_trips};
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

const PORT: u16 = 8040;
const TABLE: &str = "pushdown_roundtrip";

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn item(
    pk: &str,
    sk: &str,
    attributes: Vec<(&str, AttributeValue)>,
) -> HashMap<String, AttributeValue> {
    let mut item: HashMap<String, AttributeValue> = attributes
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
    item.insert("pk".to_string(), s(pk));
    item.insert("sk".to_string(), s(sk));
    item
}

fn items() -> Vec<HashMap<String, AttributeValue>> {
    let map = AttributeValue::M(HashMap::from([("a".to_string(), n("1"))]));
    vec![
        item(
            "u1",
            "k1",
            vec![
                ("s", s("alice")),
                ("n", n("5")),
                ("f", n("1.5")),
                ("b", AttributeValue::Bool(true)),
                ("ts", s("2024-09-03T12:34:56.155Z")),
                ("d", s("2024-01-01")),
                ("other", s("u1")),
                ("first name", s("Ann")),
                ("user-id", s("a1")),
            ],
        ),
        item(
            "u1",
            "k2",
            vec![
                ("s", s("bob")),
                ("n", n("6")),
                ("f", n("2.5")),
                ("b", AttributeValue::Bool(false)),
                ("ts", s("2024-09-03T12:34:56.155+00:00")),
                ("d", s("2024-01-02")),
                ("other", s("zz")),
            ],
        ),
        item(
            "u1",
            "k3",
            vec![
                ("s", s("5")),
                ("n", n("7")),
                ("f", n("0")),
                ("b", AttributeValue::Bool(true)),
                ("ts", s("2024-09-03T14:34:56.155+02:00")),
                ("d", s("2024-01-03")),
                ("other", s("k3")),
            ],
        ),
        item(
            "u1",
            "k4",
            vec![
                ("s", map),
                ("n", n("5.5")),
                ("f", n("0.10000000000000000001")),
                ("ts", s("2024-09-03T08:34:56.155-04:00")),
                ("d", s("2024-01-01T00:00:00")),
            ],
        ),
        item(
            "u2",
            "k1",
            vec![
                ("s", n("5")),
                ("n", n("99999999999999999999")),
                ("f", n("-1.5")),
                ("b", s("true")),
                ("ts", s("2024-09-04T00:00:00.000Z")),
                ("other", s("u2")),
            ],
        ),
        item(
            "u2",
            "k5",
            vec![
                ("s", AttributeValue::Bool(true)),
                ("n", AttributeValue::Null(true)),
                ("f", AttributeValue::Null(true)),
                ("b", AttributeValue::Null(true)),
                ("ts", AttributeValue::Null(true)),
                ("d", AttributeValue::Null(true)),
                ("other", s("zz")),
            ],
        ),
        item("u2", "k6", vec![]),
        item(
            "u3",
            "kA",
            vec![("s", s("line1\nline2")), ("first name", s("Bo"))],
        ),
        item("u3", "kB", vec![("s", s("a%b")), ("f", n("0.1"))]),
        item("u3", "ORDER#1", vec![("n", n("1"))]),
        item("u3", "ORDER#2", vec![("n", n("2"))]),
        item("u3", "ORDER#10", vec![("n", n("10"))]),
        item("u3", "PAY#1", vec![("n", n("100"))]),
        item(
            "u4",
            "k1",
            vec![
                ("ts", s("2024-09-04T03:34:56.155+15:00")),
                ("d", s("2024- 1-01")),
                ("ts6", s("2024-01-01T00:00:00.123456")),
                (
                    "m",
                    AttributeValue::M(HashMap::from([("x.y".to_string(), n("1"))])),
                ),
            ],
        ),
        // A month chrono reads without its zero, and a map-valued `m` beside a
        // string one.
        item(
            "u4",
            "k3",
            vec![("ts", s("2024-9-03T12:34:56.155Z")), ("m", s("plain"))],
        ),
        item(
            "u4",
            "k2",
            vec![
                ("ts6", s("2024-01-01T00:00:00.123000")),
                (
                    "m",
                    AttributeValue::M(HashMap::from([(
                        "x".to_string(),
                        AttributeValue::M(HashMap::from([("y".to_string(), n("2"))])),
                    )])),
                ),
            ],
        ),
    ]
}

async fn seed(client: &Client) -> Result<(), anyhow::Error> {
    let _ = client.delete_table().table_name(TABLE).send().await;
    let key = |name: &str, key_type: KeyType| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(key_type)
            .build()
    };
    let definition = |name: &str| {
        AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(ScalarAttributeType::S)
            .build()
    };
    client
        .create_table()
        .table_name(TABLE)
        .key_schema(key("pk", KeyType::Hash)?)
        .key_schema(key("sk", KeyType::Range)?)
        .attribute_definitions(definition("pk")?)
        .attribute_definitions(definition("sk")?)
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await?;
    for item in items() {
        client
            .put_item()
            .table_name(TABLE)
            .set_item(Some(item))
            .send()
            .await?;
    }
    Ok(())
}

fn dataset(name: &str, accelerated: bool) -> Dataset {
    // Pin the types a sample of every item would otherwise widen.
    let columns = vec![
        Column::new("n").with_type("bigint"),
        Column::new("b").with_type("boolean"),
        Column::new("d").with_type("date"),
    ];
    dataset_with(name, accelerated, &[], columns)
}

fn dataset_with(
    name: &str,
    accelerated: bool,
    params: &[(&str, &str)],
    columns: Vec<Column>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{TABLE}"), name.to_string());
    let mut all = HashMap::from([
        ("dynamodb_aws_access_key_id".to_string(), "fake".to_string()),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            "fake".to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{PORT}"),
        ),
        // Sample every item, so the inferred types do not depend on scan order.
        ("schema_infer_max_records".to_string(), "100".to_string()),
    ]);
    for (key, value) in params {
        all.insert((*key).to_string(), (*value).to_string());
    }
    dataset.params = Some(DatasetParams::from_string_map(all));
    dataset.columns = columns;
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            ..Acceleration::default()
        });
    }
    dataset
}

fn shows_pushdown(plan: &str) -> bool {
    plan.contains("key_condition_expression: Some")
        || plan.contains("filter_expression: Some")
        || plan.contains("request_plan: Empty")
}

fn cases() -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY pk, sk"
        ))
    };
    let e = |predicate: &str| {
        Case::either(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY pk, sk"
        ))
    };
    vec![
        // The partition key: Queries, an IN list of them, and contradictions.
        p("pk = 'u1'"),
        p("'u1' = pk"),
        p("pk IN ('u1', 'u3')"),
        // DataFusion folds a contradiction on one column away itself.
        e("pk = 'u1' AND pk = 'u2'"),
        p("pk = ''"),
        p("pk = other"),
        p("pk <> 'u1'"),
        p("pk > 'u1'"),
        // The sort key, one key condition at a time.
        p("pk = 'u1' AND sk = 'k2'"),
        p("pk = 'u1' AND sk > 'k1'"),
        p("pk = 'u1' AND sk < 'k3'"),
        p("pk = 'u1' AND sk >= 'k2' AND sk <= 'k3'"),
        p("pk = 'u1' AND sk >= 'k2' AND sk < 'k4'"),
        p("pk = 'u1' AND sk > 'k1' AND sk < 'k4'"),
        p("pk = 'u1' AND sk BETWEEN 'k2' AND 'k3'"),
        p("pk = 'u1' AND sk BETWEEN 'k3' AND 'k2'"),
        p("pk = 'u3' AND sk LIKE 'ORDER#%'"),
        p("pk = 'u3' AND starts_with(sk, 'ORDER#')"),
        p("pk = 'u1' AND sk = other"),
        e("pk = 'u1' AND sk = 'k1' AND sk = 'k2'"),
        p("pk = 'u1' AND sk = 'k3' AND sk >= 'k2'"),
        p("pk = 'u3' AND sk LIKE 'ORDER#%' AND sk >= 'ORDER#2'"),
        p("sk = 'k1'"),
        p("sk LIKE 'ORDER#%'"),
        // Strings, where a map is rendered as JSON and a number or bool is NULL.
        p("s = 'alice'"),
        p("s <> 'alice'"),
        p("s > 'b'"),
        p("s = '5'"),
        p("s = '{\"a\":1}'"),
        p("s IN ('alice', 'bob')"),
        p("s NOT IN ('alice')"),
        p("s BETWEEN 'a' AND 'c'"),
        p("s LIKE 'al%'"),
        p("s LIKE 'a\\%b%'"),
        p("s NOT LIKE 'a%'"),
        p("starts_with(s, 'line1')"),
        p("s IS NULL"),
        p("s IS NOT NULL"),
        e("s LIKE '%b'"),
        e("s ILIKE 'AL%'"),
        // A column whose declared integer type reads 5.5 and 10^20 as NULL.
        p("n = 5"),
        p("n <> 5"),
        p("n > 5"),
        p("n <= 6"),
        p("n IN (5, 7)"),
        p("n NOT IN (5, 7)"),
        p("n BETWEEN 5 AND 6"),
        p("n IS NOT NULL"),
        e("n IS NULL"),
        // Floats: 0.10000000000000000001 reads as 0.1.
        p("f = 0.1"),
        p("f >= 0.1"),
        p("f <= 0.1"),
        p("f > 0.1"),
        p("f < 1.5"),
        p("f <> 0.1"),
        p("f = 0"),
        // Arrow orders -0.0 below 0.0, which DynamoDB holds equal.
        p("f > CAST('-0.0' AS DOUBLE)"),
        p("f <> CAST('-0.0' AS DOUBLE)"),
        p("f < 0.0"),
        p("f IS NULL"),
        p("f IS NOT NULL"),
        e("f IN (0.1, 2.5)"),
        // Booleans; the string "true" reads as NULL.
        p("b"),
        p("NOT b"),
        p("b = true"),
        p("b <> true"),
        p("b IS NOT TRUE"),
        p("b IS NULL"),
        // One instant in four offsets, compared as strings widened by any offset.
        p("ts = TIMESTAMP '2024-09-03T12:34:56.155Z'"),
        p("ts >= TIMESTAMP '2024-09-03T12:34:56.155Z'"),
        p("ts > TIMESTAMP '2024-09-03T12:34:56.155Z'"),
        p("ts < TIMESTAMP '2024-09-03T13:00:00Z'"),
        p("ts <= TIMESTAMP '2024-09-03T12:34:56.155Z'"),
        p("ts BETWEEN TIMESTAMP '2024-09-03T00:00:00Z' AND TIMESTAMP '2024-09-03T23:00:00Z'"),
        p("ts <> TIMESTAMP '2024-09-03T12:34:56.155Z'"),
        // Dates; a string that is not YYYY-MM-DD reads as NULL.
        p("d = DATE '2024-01-01'"),
        p("d > DATE '2024-01-01'"),
        p("d <= DATE '2024-01-02'"),
        p("d IN (DATE '2024-01-01', DATE '2024-01-03')"),
        // Attribute names that need a generated placeholder.
        p("\"first name\" = 'Ann'"),
        p("\"user-id\" = 'a1'"),
        // A short IN list with a NULL reaches the scan as `x = 1 OR NULL`.
        p("pk IN ('u1', NULL)"),
        p("n IN (5, NULL)"),
        // More prefixes than one 4 KB filter expression holds.
        e(&(0..76)
            .map(|i| format!("starts_with(s, 'p{i:02}')"))
            .collect::<Vec<_>>()
            .join(" OR ")),
        // More filters than one expression holds, offered over several passes.
        e(&(0..7)
            .map(|i| {
                let list = std::iter::once(5).chain((0..99).map(|k| 1_000 * (i + 1) + k));
                format!(
                    "n IN ({})",
                    list.map(|k| k.to_string()).collect::<Vec<_>>().join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")),
        // Compound predicates.
        p("(s = 'alice' OR n = 7) AND pk = 'u1'"),
        p("NOT (n = 5)"),
        p("NOT (n = 5 AND b)"),
        p("s = 'bob' OR f = 0.1"),
        p("pk = 'u1' AND (n > 5 OR s = 'alice')"),
        // A Spice function is evaluated by DataFusion, never by DynamoDB.
        e("bucket(4, n) = 1"),
    ]
}

fn order_and_limit_cases() -> Vec<Case> {
    vec![
        // A Query of one partition reads in sort-key order.
        Case::pushed("SELECT pk, sk FROM {t} WHERE pk = 'u1' ORDER BY sk DESC LIMIT 2"),
        Case::pushed("SELECT pk, sk FROM {t} WHERE pk = 'u3' ORDER BY sk LIMIT 3"),
        Case::pushed(
            "SELECT pk, sk FROM {t} WHERE pk = 'u3' AND sk LIKE 'ORDER#%' ORDER BY sk DESC",
        ),
        Case::pushed("SELECT pk, sk FROM {t} WHERE pk IN ('u1', 'u3') ORDER BY sk, pk"),
        Case::pushed("SELECT pk, sk FROM {t} WHERE pk = 'u1' AND n > 5 ORDER BY sk DESC"),
        Case::either("SELECT pk, sk FROM {t} ORDER BY sk, pk LIMIT 4"),
        Case::limited("SELECT pk, sk FROM {t} WHERE pk = 'u1'", 2, Pushed::Yes),
        Case::limited(
            "SELECT pk, sk FROM {t} WHERE pk = 'u1' AND sk >= 'k2'",
            1,
            Pushed::Yes,
        ),
        Case::limited("SELECT pk, sk FROM {t}", 3, Pushed::Either),
        // Filters past one expression are checked on the rows read, before the limit.
        Case::limited(
            format!(
                "SELECT pk, sk FROM {{t}} WHERE {}",
                (0..7)
                    .map(|i| {
                        let list = std::iter::once(5).chain((0..99).map(|k| 1_000 * (i + 1) + k));
                        format!(
                            "n IN ({})",
                            list.map(|k| k.to_string()).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            1,
            Pushed::Either,
        ),
        Case::either("SELECT count(*) FROM {t} WHERE n <> 5"),
    ]
}

#[tokio::test]
async fn dynamodb_pushdown_round_trips() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = start_dynamodb_docker_container(PORT).await?;
            seed(&get_client(PORT, "fake", "fake")).await?;

            // A layout finer than a millisecond, and maps flattened into columns.
            let micro = [("time_format", "2006-01-02T15:04:05.000000")];
            let micro_columns = || vec![Column::new("ts6").with_type("Timestamp(Millisecond)")];
            let unnested = [("unnest_depth", "2")];
            let shallow = [("unnest_depth", "1")];
            let app = AppBuilder::new("dynamodb_pushdown_round_trips")
                .with_dataset(dataset("federated", false))
                .with_dataset(dataset("local", true))
                .with_dataset(dataset_with("micro", false, &micro, micro_columns()))
                .with_dataset(dataset_with("micro_local", true, &micro, micro_columns()))
                .with_dataset(dataset_with("unnested", false, &unnested, Vec::new()))
                .with_dataset(dataset_with("unnested_local", true, &unnested, Vec::new()))
                .with_dataset(dataset_with("shallow", false, &shallow, Vec::new()))
                .with_dataset(dataset_with("shallow_local", true, &shallow, Vec::new()))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let tables = Tables {
                federated: "federated",
                local: "local",
                shows_pushdown,
            };
            let mut all = cases();
            all.extend(order_and_limit_cases());
            assert_round_trips(&rt, &tables, &all).await?;

            let p = |predicate: &str| Case::pushed(format!("SELECT * FROM {{t}} WHERE {predicate} ORDER BY pk, sk"));
            let micro = Tables {
                federated: "micro",
                local: "micro_local",
                shows_pushdown,
            };
            let micro_cases = [
                // `.123456` reads as `.123`.
                p("ts6 = TIMESTAMP '2024-01-01T00:00:00.123'"),
                p("ts6 <= TIMESTAMP '2024-01-01T00:00:00.123'"),
                p("ts6 > TIMESTAMP '2024-01-01T00:00:00.122'"),
                p("ts6 BETWEEN TIMESTAMP '2024-01-01T00:00:00' AND TIMESTAMP '2024-01-01T00:00:00.123'"),
            ];
            assert_round_trips(&rt, &micro, &micro_cases).await?;

            let unnested = Tables {
                federated: "unnested",
                local: "unnested_local",
                shows_pushdown,
            };
            let unnested_cases = [
                // `m.x.y` is 1 from the map key "x.y" and 2 from the nested map.
                p("\"m.x.y\" = 1"),
                p("\"m.x.y\" IN (1, 2)"),
                p("\"m.x.y\" IS NOT NULL"),
                // A number that is not an integer reads as NULL too.
                Case::either("SELECT * FROM {t} WHERE \"m.x.y\" IS NULL ORDER BY pk, sk"),
                p("\"m.x.y\" > 0"),
                // A map-valued `m` is flattened away, which leaves `m` NULL.
                p("m IS NULL"),
                p("m IS NOT NULL"),
                p("m = 'plain'"),
            ];
            assert_round_trips(&rt, &unnested, &unnested_cases).await?;

            let shallow = Tables {
                federated: "shallow",
                local: "shallow_local",
                shows_pushdown,
            };
            let shallow_cases = [
                // One level down, `m.x.y` is the map key "x.y", and the path
                // `m.x.y` reaches the map left whole as the JSON of `m.x`.
                Case::either("SELECT * FROM {t} WHERE \"m.x.y\" = 1 ORDER BY pk, sk"),
                // `m.x` holds the map left whole at the depth limit, a struct.
                Case::either("SELECT * FROM {t} WHERE \"m.x\" IS NOT NULL ORDER BY pk, sk"),
                p("m IS NULL"),
            ];
            assert_round_trips(&rt, &shallow, &shallow_cases).await
        })
        .await
}
