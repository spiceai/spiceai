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

//! `MongoDB` filter pushdown, compared row for row with `DataFusion` evaluating
//! the same documents locally.
//!
//! The collection's first documents fix the inferred column types; the rest
//! hold what `MongoDB` matches differently from SQL: null and missing fields,
//! values of other BSON types, arrays, `ObjectId`s, symbols, NaN of either
//! sign, -0.0, integers beyond 2^53, BSON timestamps, dates beyond the
//! nanosecond range, and strings a LIKE translation gets wrong. Other
//! collections cover a case-insensitive default collation, on a collection
//! and on a view, and a field whose name contains a dot.

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Array, Int32Array};
use mongodb::bson::{Bson, DateTime as BsonDateTime, Document, Timestamp, doc, oid::ObjectId};
use runtime::Runtime;
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::param::Params as DatasetParams;
use spicepod::semantic::Column;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use super::common::{get_mongodb_client, make_mongodb_dataset, start_mongodb_docker_container};
use crate::pushdown_roundtrip::{Case, Pushed, Tables, assert_round_trips};
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const PORT: u16 = 27041;

/// The documents schema inference samples, before the ones that stray from
/// the inferred types.
const SAMPLED: usize = 4;

fn oid(n: u8) -> ObjectId {
    ObjectId::from_bytes([0x65, 0xf0, 0, 0, 0, 0, 0, 0, 0, 0, 0, n])
}

fn hex(n: u8) -> String {
    oid(n).to_hex()
}

fn date(rfc3339: &str) -> Bson {
    Bson::DateTime(
        BsonDateTime::parse_rfc3339_str(rfc3339).unwrap_or_else(|_| {
            panic!("valid date {rfc3339}");
        }),
    )
}

fn documents() -> Vec<Document> {
    let sampled = vec![
        doc! { "_id": oid(1), "i32": 1, "i64": 10_i64, "f64": 1.5, "s": "alice", "b": true,
        "ts": date("2024-01-01T10:00:00.123Z"), "d": date("2024-01-01T00:00:00Z"),
        "oid": oid(101), "nested": { "city": "x" }, "tags": ["a", "b"] },
        doc! { "_id": oid(2), "i32": 30, "i64": 20_i64, "f64": 2.5, "s": "bob", "b": false,
        "ts": date("2024-01-02T00:00:00Z"), "d": date("2024-01-02T00:00:00Z"),
        "oid": oid(102), "nested": { "city": "y" }, "tags": ["b"] },
        doc! { "_id": oid(3), "i32": -5, "i64": 9_007_199_254_740_993_i64, "f64": -2.0, "s": "line1\nline2",
        "b": true, "ts": date("2023-12-31T23:59:59.999Z"), "d": date("2023-12-31T00:00:00Z"),
        "oid": oid(103), "nested": { "city": "z" }, "tags": [] },
        doc! { "_id": oid(4), "i32": 2_147_483_647, "i64": -1_i64, "f64": 0.0, "s": "abc", "b": false,
        "ts": date("1969-12-31T23:59:59.999Z"), "d": date("1970-01-01T00:00:00Z"),
        "oid": oid(104), "nested": { "city": "x" }, "tags": ["c"] },
    ];
    assert_eq!(sampled.len(), SAMPLED);
    let strays = vec![
        // Null and missing everywhere.
        doc! { "_id": oid(5), "i32": Bson::Null, "i64": Bson::Null, "f64": Bson::Null, "s": Bson::Null,
        "b": Bson::Null, "ts": Bson::Null, "d": Bson::Null, "oid": Bson::Null, "nested": Bson::Null,
        "tags": Bson::Null },
        doc! { "_id": oid(6) },
        // Other BSON types than the column's.
        doc! { "_id": oid(7), "i32": 7_i64, "i64": 5, "f64": 5, "s": 5, "b": 1, "ts": "2024-01-01",
        "d": date("2024-01-01T10:00:00Z"), "oid": hex(101), "nested": "flat", "tags": "a" },
        doc! { "_id": oid(8), "i32": 5_000_000_000_i64, "i64": 5.0, "f64": 9_007_199_254_740_993_i64,
        "s": { "a": 1 }, "b": "true", "ts": Bson::Timestamp(Timestamp { time: 1_704_067_200, increment: 5 }),
        "d": Bson::Timestamp(Timestamp { time: 1_704_067_200, increment: 1 }), "oid": 5 },
        doc! { "_id": oid(9), "i32": 2.5, "f64": "3.5", "s": true, "b": [true] },
        doc! { "_id": oid(10), "i32": "7", "f64": [1.5], "s": ["alice"], "b": Bson::Null },
        doc! { "_id": oid(11), "i32": [1, 30], "s": oid(1) },
        // Floats Arrow orders apart from how MongoDB compares them.
        doc! { "_id": oid(12), "f64": f64::NAN, "s": "abc\n" },
        doc! { "_id": oid(13), "f64": f64::from_bits(0xFFF8_0000_0000_0000), "s": "a%b" },
        doc! { "_id": oid(14), "f64": -0.0, "s": "a\\%b" },
        doc! { "_id": oid(15), "f64": f64::INFINITY, "s": "\u{212A}elvin" },
        doc! { "_id": oid(16), "f64": f64::NEG_INFINITY, "s": "" },
        doc! { "_id": oid(17), "s": "ab" },
        // A symbol compares as the string it spells and renders as
        // `Symbol("alice")`; the date overflows a nanosecond timestamp.
        doc! { "_id": oid(18), "s": Bson::Symbol("alice".to_string()),
        "ts": Bson::DateTime(BsonDateTime::from_millis(FIRST_BEYOND_NANOSECONDS)) },
        // `_id`s of other types render into the `Utf8` column.
        doc! { "_id": "string-id", "i32": 30, "s": "alice" },
        doc! { "_id": 42, "i32": 1, "s": "bob" },
    ];
    sampled.into_iter().chain(strays).collect()
}

/// The first millisecond past the instants a nanosecond `i64` can hold.
const FIRST_BEYOND_NANOSECONDS: i64 = i64::MAX / 1_000_000 + 1;

/// Strings a case-insensitive collation holds equal and orders apart from SQL.
fn collated_documents() -> Vec<Document> {
    ["X", "a", "x", "b", "B", "Z"]
        .iter()
        .zip(1..)
        .map(|(s, id)| doc! { "_id": id, "s": *s })
        .chain([doc! { "_id": 7, "s": Bson::Null }, doc! { "_id": 8 }])
        .collect()
}

/// A field whose name contains a dot, which unnesting a whole document reads
/// into the dotted column, next to the embedded field of the same path.
fn dotted_documents() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "a.b": 1, "c": 1 },
        doc! { "_id": 2, "a": { "b": 1 }, "c": 2 },
        doc! { "_id": 3, "a": { "b": 2 }, "c": 3 },
        doc! { "_id": 4, "c": 4 },
    ]
}

/// Dates for a column declared as a nanosecond timestamp, two of them beyond
/// the range it holds.
fn distant_documents() -> Vec<Document> {
    [
        FIRST_BEYOND_NANOSECONDS,
        0,
        -FIRST_BEYOND_NANOSECONDS,
        FIRST_BEYOND_NANOSECONDS - 1,
        946_684_799_999,
    ]
    .iter()
    .zip(1..)
    .map(|(millis, id)| doc! { "_id": id, "t": BsonDateTime::from_millis(*millis) })
    .collect()
}

/// Nested documents for `unnest_depth: 1`: `address.city` is a column, and an
/// array of addresses, which `MongoDB`'s dotted paths traverse, is not flattened.
fn nested_documents() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "address": { "city": "Paris", "zip": 75001 } },
        doc! { "_id": 2, "address": { "city": "Oslo", "zip": 150 } },
        doc! { "_id": 3, "address": [{ "city": "Paris", "zip": 1 }] },
        doc! { "_id": 4, "address": "no address" },
        doc! { "_id": 5, "address": { "city": { "name": "Paris" } } },
        doc! { "_id": 6 },
    ]
}

async fn seed(port: u16) -> Result<(), anyhow::Error> {
    let client = get_mongodb_client(port).await?;
    let db = client.database("testdb");
    let collation = doc! { "locale": "en", "strength": 2 };
    for name in ["roundtrip_collated_view", "roundtrip_collated"] {
        let _ = db.collection::<Document>(name).drop().await;
    }
    db.run_command(doc! { "create": "roundtrip_collated", "collation": collation.clone() })
        .await?;
    db.run_command(doc! {
        "create": "roundtrip_collated_view",
        "viewOn": "roundtrip_collated",
        "pipeline": [],
        "collation": collation,
    })
    .await?;
    for (name, docs) in [
        ("roundtrip", documents()),
        ("roundtrip_nested", nested_documents()),
        ("roundtrip_dotted", dotted_documents()),
        ("roundtrip_distant", distant_documents()),
    ] {
        let collection = db.collection::<Document>(name);
        let _ = collection.drop().await;
        collection.insert_many(docs).await?;
    }
    db.collection::<Document>("roundtrip_collated")
        .insert_many(collated_documents())
        .await?;
    Ok(())
}

fn dataset(
    collection: &str,
    name: &str,
    accelerated: bool,
    params: &[(&str, &str)],
) -> spicepod::component::dataset::Dataset {
    let mut dataset = make_mongodb_dataset(collection, name, PORT, accelerated);
    let mut all: HashMap<String, String> = dataset
        .params
        .as_ref()
        .map(DatasetParams::as_string_map)
        .unwrap_or_default();
    for (key, value) in params {
        all.insert((*key).to_string(), (*value).to_string());
    }
    dataset.params = Some(DatasetParams::from_string_map(all));
    dataset
}

fn shows_pushdown(plan: &str) -> bool {
    plan.contains("MongoDBExec") && !plan.contains("filters=[{}]")
}

fn row_cases() -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    let e = |predicate: &str| {
        Case::either(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    let (h1, h2) = (hex(1), hex(2));
    vec![
        // Integers, and the BSON types an Int32 column nulls or keeps.
        p("i32 = 30"),
        p("30 = i32"),
        p("i32 <> 30"),
        p("i32 < 30"),
        p("i32 <= 30"),
        p("i32 > 1"),
        p("i32 >= 1"),
        p("i32 = 7"),
        p("i32 IN (1, 30, 7)"),
        p("i32 IN (1, NULL)"),
        e("NOT (i32 IN (1, NULL))"),
        e("i32 = 1 AND NULL"),
        p("i32 NOT IN (1, 30)"),
        e("i32 NOT IN (1, NULL)"),
        p("i32 BETWEEN 1 AND 30"),
        p("i32 NOT BETWEEN 1 AND 29"),
        p("i32 IS NULL"),
        p("i32 IS NOT NULL"),
        p("NOT (i32 > 1)"),
        p("i32 IS DISTINCT FROM 30"),
        p("i32 IS NOT DISTINCT FROM 30"),
        p("CAST(i32 AS BIGINT) > 5000000000"),
        p("CAST(i32 AS DOUBLE) > 2.5"),
        e("CAST(i32 AS VARCHAR) = '30'"),
        p("i64 = 9007199254740993"),
        p("i64 > 10"),
        p("i64 <> 20"),
        // Floats: NaN of either sign, both zeros, infinities, and a long that
        // rounds on its way to an f64.
        p("f64 > 2"),
        p("f64 < 2"),
        p("f64 >= 0"),
        p("f64 <= 0"),
        p("f64 > 0"),
        p("f64 < 0"),
        p("f64 = 0"),
        p("f64 <> 0"),
        p("f64 > CAST('-0' AS DOUBLE)"),
        p("f64 < CAST('-0' AS DOUBLE)"),
        p("f64 = CAST('NaN' AS DOUBLE)"),
        p("f64 > CAST('NaN' AS DOUBLE)"),
        p("f64 = 2.5"),
        p("f64 <> 2.5"),
        p("f64 < CAST('Infinity' AS DOUBLE)"),
        e("f64 = 9007199254740992.0"),
        p("f64 IN (1.5, 2.5)"),
        p("f64 IS NULL"),
        // Strings, and values of other types a Utf8 column renders.
        p("s = 'alice'"),
        p("s <> 'alice'"),
        p("s > 'b'"),
        p("s < 'b'"),
        p("s = '5'"),
        p("s = 'true'"),
        p("s IN ('alice', '5')"),
        p("s NOT IN ('alice')"),
        p("s LIKE 'al%'"),
        p("s LIKE 'line1%'"),
        p("s LIKE 'ab_'"),
        p("s LIKE 'a\\%b'"),
        p("s LIKE '%b%'"),
        p("s NOT LIKE 'a%'"),
        p("s ILIKE 'AL%'"),
        p("s ILIKE 'k%'"),
        p("s IS NULL"),
        p("s IS NOT NULL"),
        p("s IS DISTINCT FROM 'alice'"),
        p("s NOT IN ('alice', 'bob')"),
        e("starts_with(s, 'al')"),
        // An `ObjectId` column matches its hex rendering.
        p(&format!("_id = '{h1}'")),
        p(&format!("_id IN ('{h1}', '{h2}')")),
        p(&format!("_id <> '{h1}'")),
        p(&format!("_id > '{h2}'")),
        p(&format!("_id = '{}'", h1.to_uppercase())),
        p("_id = '42'"),
        p("_id = 'string-id'"),
        p(&format!("oid = '{}'", hex(101))),
        // Booleans.
        p("b"),
        p("NOT b"),
        p("b = true"),
        p("b <> true"),
        p("b IS TRUE"),
        p("b IS NOT TRUE"),
        p("b IS FALSE"),
        p("b IS NOT FALSE"),
        p("b IS NULL"),
        // Instants, with a BSON timestamp read as its seconds.
        p("ts > TIMESTAMP '2024-01-01T10:00:00'"),
        p("ts >= TIMESTAMP '2024-01-01T10:00:00.1235'"),
        p("ts = TIMESTAMP '2024-01-01T10:00:00.123'"),
        p("ts = TIMESTAMP '2024-01-01T00:00:00'"),
        p("ts < TIMESTAMP '1970-01-01T00:00:00'"),
        p("ts BETWEEN TIMESTAMP '2023-12-31T00:00:00' AND TIMESTAMP '2024-01-01T12:00:00'"),
        e("CAST(ts AS DATE) = DATE '2024-01-01'"),
        // A finer unit overflows for the distant date, which TRY_CAST nulls.
        e("TRY_CAST(ts AS TIMESTAMP) IS NULL"),
        e("TRY_CAST(ts AS TIMESTAMP) > TIMESTAMP '2024-01-01T00:00:00'"),
        // Dates, whose column is the UTC day of any time on it.
        p("d = DATE '2024-01-01'"),
        p("d > DATE '2024-01-01'"),
        p("d <= DATE '2024-01-01'"),
        p("d <> DATE '2024-01-01'"),
        p("d BETWEEN DATE '2024-01-01' AND DATE '2024-01-02'"),
        // An embedded document is JSON; an array is a list.
        p("nested IS NULL"),
        p("nested = '{\"city\":\"x\"}'"),
        p("tags IS NULL"),
        e("array_has(tags, 'a')"),
        // Compound predicates, negated through three-valued logic.
        p("(i32 > 1 AND s = 'bob') OR b"),
        p("NOT (i32 > 1 OR s = 'alice')"),
        p("NOT (i32 > 1 AND b)"),
        p("i32 > 1 AND f64 > 2"),
        p("s = 'alice' OR i32 = 30"),
        // A Spice function is evaluated by DataFusion, never by MongoDB.
        e("bucket(4, i32) = 1"),
    ]
}

fn order_and_limit_cases() -> Vec<Case> {
    vec![
        Case::either("SELECT _id, i32 FROM {t} ORDER BY i32 NULLS LAST, _id LIMIT 3"),
        Case::either("SELECT _id, i32 FROM {t} ORDER BY i32 DESC NULLS FIRST, _id LIMIT 3"),
        Case::either("SELECT _id, s FROM {t} ORDER BY s, _id"),
        Case::either("SELECT _id, f64 FROM {t} ORDER BY f64 NULLS FIRST, _id"),
        Case::either("SELECT _id, ts FROM {t} ORDER BY ts DESC, _id LIMIT 2"),
        Case::either("SELECT count(*) FROM {t} WHERE i32 <> 30"),
        Case::limited("SELECT * FROM {t} WHERE i32 > 1", 2, Pushed::Yes),
        Case::limited("SELECT _id FROM {t}", 3, Pushed::Either),
    ]
}

fn nested_cases() -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    vec![
        p("\"address.city\" = 'Paris'"),
        p("\"address.city\" IS NULL"),
        p("\"address.city\" IS NOT NULL"),
        p("\"address.zip\" > 100"),
        p("\"address.zip\" IS NULL"),
    ]
}

/// On a collection whose collation can be replaced, every string predicate is
/// pushed down; on a view, whose cannot, only those a collation cannot narrow.
fn collated_cases(replaceable: bool) -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    let ordered = |predicate: &str| {
        let sql = format!("SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id");
        if replaceable {
            Case::pushed(sql)
        } else {
            Case::either(sql)
        }
    };
    vec![
        ordered("s <> 'x'"),
        ordered("s NOT IN ('x', 'b')"),
        ordered("s > 'Z'"),
        ordered("s < 'b'"),
        ordered("s BETWEEN 'B' AND 'a'"),
        ordered("s IS DISTINCT FROM 'x'"),
        ordered("NOT (s = 'x')"),
        p("s = 'x'"),
        p("s IN ('x', 'b')"),
        p("s LIKE 'x%'"),
        p("s ILIKE 'x%'"),
        p("s IS NULL"),
        ordered("s = 'x' OR s > 'Z'"),
    ]
}

fn dotted_cases() -> Vec<Case> {
    let e = |predicate: &str| {
        Case::either(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    vec![
        // Read from whole documents, the dotted column also holds the field
        // named "a.b", which no query path addresses.
        e("\"a.b\" = 1"),
        e("\"a.b\" IS NULL"),
        e("\"a.b\" IS NOT NULL"),
        Case::pushed("SELECT * FROM {t} WHERE _id > 1 ORDER BY _id"),
    ]
}

fn distant_cases() -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    vec![
        Case::either("SELECT * FROM {t} ORDER BY _id"),
        p("t IS NULL"),
        p("t IS NOT NULL"),
        p("t < TIMESTAMP '2000-01-01T00:00:00'"),
        p("t >= TIMESTAMP '1970-01-01T00:00:00'"),
        p("t <> TIMESTAMP '1970-01-01T00:00:00'"),
    ]
}

fn catch_all_cases() -> Vec<Case> {
    let e = |predicate: &str| {
        Case::either(format!(
            "SELECT * FROM {{t}} WHERE {predicate} ORDER BY _id"
        ))
    };
    vec![
        // The catch-all is assembled from undeclared fields, so it is
        // evaluated locally, never matched against a field named `data`.
        e("data IS NULL"),
        e("data IS NOT NULL"),
        e("data LIKE '%alice%'"),
        Case::pushed("SELECT * FROM {t} WHERE i32 = 30 ORDER BY _id"),
    ]
}

#[tokio::test]
async fn mongodb_pushdown_round_trips() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let _container = start_mongodb_docker_container(PORT).await?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                seed(PORT).await.map_err(RetryError::transient)
            })
            .await?;

            let sampled = SAMPLED.to_string();
            let inference = [("mongodb_schema_infer_max_records", sampled.as_str())];
            let unnested = [("mongodb_unnest_depth", "1")];

            let catch_all_column = || {
                let mut metadata = HashMap::new();
                metadata.insert("json_object".to_string(), serde_json::json!("*"));
                Column::new("data").with_metadata(metadata)
            };
            let mut catch_all = dataset("roundtrip", "catch_all", false, &inference);
            let mut catch_all_local = dataset("roundtrip", "catch_all_local", true, &inference);
            for d in [&mut catch_all, &mut catch_all_local] {
                d.columns = vec![Column::new("_id"), Column::new("i32"), catch_all_column()];
            }
            let mut dotted = dataset("roundtrip_dotted", "dotted", false, &unnested);
            let mut dotted_local = dataset("roundtrip_dotted", "dotted_local", true, &unnested);
            for d in [&mut dotted, &mut dotted_local] {
                d.columns = vec![Column::new("_id"), Column::new("a.b"), catch_all_column()];
            }
            let mut distant = dataset("roundtrip_distant", "distant", false, &[]);
            let mut distant_local = dataset("roundtrip_distant", "distant_local", true, &[]);
            for d in [&mut distant, &mut distant_local] {
                d.columns = vec![Column::new("_id"), Column::new("t").with_type("timestamp")];
            }

            let app = AppBuilder::new("mongodb_pushdown_round_trips")
                .with_dataset(dataset("roundtrip", "federated", false, &inference))
                .with_dataset(dataset("roundtrip", "local", true, &inference))
                .with_dataset(dataset("roundtrip_nested", "nested", false, &unnested))
                .with_dataset(dataset("roundtrip_nested", "nested_local", true, &unnested))
                .with_dataset(catch_all)
                .with_dataset(catch_all_local)
                .with_dataset(dataset("roundtrip_collated", "collated", false, &[]))
                .with_dataset(dataset("roundtrip_collated", "collated_local", true, &[]))
                .with_dataset(dataset(
                    "roundtrip_collated_view",
                    "collated_view",
                    false,
                    &[],
                ))
                .with_dataset(dataset(
                    "roundtrip_collated_view",
                    "collated_view_local",
                    true,
                    &[],
                ))
                .with_dataset(dotted)
                .with_dataset(dotted_local)
                .with_dataset(distant)
                .with_dataset(distant_local)
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
            let mut cases = row_cases();
            cases.extend(order_and_limit_cases());
            assert_round_trips(&rt, &tables, &cases).await?;

            let nested = Tables {
                federated: "nested",
                local: "nested_local",
                shows_pushdown,
            };
            assert_round_trips(&rt, &nested, &nested_cases()).await?;

            let catch_all = Tables {
                federated: "catch_all",
                local: "catch_all_local",
                shows_pushdown,
            };
            assert_round_trips(&rt, &catch_all, &catch_all_cases()).await?;

            for (federated, local, replaceable) in [
                ("collated", "collated_local", true),
                ("collated_view", "collated_view_local", false),
            ] {
                let tables = Tables {
                    federated,
                    local,
                    shows_pushdown,
                };
                assert_round_trips(&rt, &tables, &collated_cases(replaceable)).await?;
            }

            let dotted = Tables {
                federated: "dotted",
                local: "dotted_local",
                shows_pushdown,
            };
            assert_round_trips(&rt, &dotted, &dotted_cases()).await?;

            let distant = Tables {
                federated: "distant",
                local: "distant_local",
                shows_pushdown,
            };
            assert_round_trips(&rt, &distant, &distant_cases()).await?;
            // Both sides share the conversion, so check it directly: a date the
            // unit cannot hold is NULL, not a wrapped-around instant.
            let batches = run_query(
                &rt,
                "SELECT _id FROM distant_local WHERE t IS NULL ORDER BY _id",
            )
            .await?;
            let nulls: Vec<i32> = batches
                .iter()
                .flat_map(|batch| {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("an Int32 _id");
                    (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(nulls, vec![1, 3]);
            Ok(())
        })
        .await
}
