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

//! Azure Cosmos DB projection and filter pushdown against the Linux emulator,
//! compared row for row with `DataFusion` evaluating the same documents
//! locally, and against a container whose partition keys are of two JSON types.
//!
//! The emulator and the service disagree on comparisons with null and with
//! undefined properties, which is why every pushed condition is guarded by the
//! JSON type it compares; a suite that only passed against one of them would
//! prove little.
//!
//! The emulator is a large image, so the suite is ignored by default:
//! `cargo test --features cosmosdb -- --ignored cosmosdb_pushdown_round_trips`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::RecordBatch;
use azure_core::credentials::Secret;
use azure_data_cosmos::CosmosClient;
use azure_data_cosmos::models::ContainerProperties;
use bollard::secret::HealthConfig;
use runtime::Runtime;
use serde_json::{Value, json};
use spicepod::acceleration::Acceleration;
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use crate::pushdown_roundtrip::{Case, Pushed, Tables, assert_round_trips};
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
    to_pretty_display,
};
use crate::{configure_test_datafusion, init_tracing};

const IMAGE: &str = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview";
/// The emulator's well-known account key.
const KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
const DATABASE: &str = "roundtrip";
const CONTAINER: &str = "people";
/// A container one of whose partition keys is a number, which a string column
/// cannot hold.
const MIXED_PARTITION_KEYS: &str = "mixed_partition_keys";

fn endpoint(port: u16) -> String {
    format!("http://localhost:{port}/")
}

async fn start_emulator() -> Result<RunningContainer, anyhow::Error> {
    ContainerRunnerBuilder::new("runtime-integration-test-cosmosdb")
        .image(IMAGE.to_string())
        .publish_port(8081)
        .command(["--enable-explorer", "false"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD".to_string(),
                "curl".to_string(),
                "-sf".to_string(),
                "http://localhost:8081/".to_string(),
            ]),
            interval: Some(2_000_000_000),
            timeout: Some(10_000_000_000),
            retries: Some(60),
            start_period: Some(5_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(5)))
        .await
}

fn documents() -> Vec<Value> {
    vec![
        json!({ "id": "1", "pk": "a", "name": "alice", "age": 30, "score": 1.5, "vip": true, "tags": ["x"],
                "big": i64::MIN }),
        json!({ "id": "2", "pk": "a", "name": "bob", "age": null, "score": 2.5, "vip": false, "big": 5 }),
        json!({ "id": "3", "pk": "b", "name": "carol", "status": null, "score": 0.0 }),
        json!({ "id": "4", "pk": "b", "name": "dave", "age": 40, "score": null, "vip": true }),
        json!({ "id": "5", "pk": "c", "name": null, "age": 25, "score": -0.0, "status": "active" }),
        json!({ "id": "6", "pk": "c", "age": 35, "status": "active", "score": -2.0 }),
        json!({ "id": "7", "pk": "a", "name": "élan", "age": 0, "status": "inactive", "score": 1e-9 }),
        json!({ "id": "8", "pk": "d", "name": "\u{1F4A1} idea", "age": -3, "status": "active" }),
        json!({ "id": "9", "pk": "d", "name": "a%b", "age": 7, "status": "active", "vip": false }),
        json!({ "id": "10", "pk": "e", "name": "ac_t", "age": 7, "score": 7.0 }),
    ]
}

async fn seed(port: u16) -> Result<(), anyhow::Error> {
    let client = CosmosClient::with_key(&endpoint(port), Secret::from(KEY), None)?;
    let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(20)).build();
    // The emulator answers before it serves requests; creating the database
    // is what shows it is up.
    retry(retry_strategy, || async {
        match client.create_database(DATABASE, None).await {
            Ok(_) => Ok(()),
            Err(e) if e.http_status() == Some(azure_core::http::StatusCode::Conflict) => Ok(()),
            Err(e) => Err(RetryError::transient(anyhow::anyhow!(e))),
        }
    })
    .await?;
    let database = client.database_client(DATABASE);
    let _ = database.container_client(CONTAINER).delete(None).await;
    database
        .create_container(
            ContainerProperties {
                id: CONTAINER.into(),
                partition_key: "/pk".into(),
                ..Default::default()
            },
            None,
        )
        .await?;
    let container = database.container_client(CONTAINER);
    for document in documents() {
        let pk = document["pk"].as_str().unwrap_or_default().to_string();
        container.upsert_item(pk, document, None).await?;
    }

    let _ = database
        .container_client(MIXED_PARTITION_KEYS)
        .delete(None)
        .await;
    database
        .create_container(
            ContainerProperties {
                id: MIXED_PARTITION_KEYS.into(),
                partition_key: "/pk".into(),
                ..Default::default()
            },
            None,
        )
        .await?;
    let mixed = database.container_client(MIXED_PARTITION_KEYS);
    mixed
        .upsert_item("a", json!({ "id": "good", "pk": "a" }), None)
        .await?;
    mixed
        .upsert_item("b", json!({ "id": "other", "pk": "b" }), None)
        .await?;
    mixed
        .upsert_item(5, json!({ "id": "wrong-type", "pk": 5 }), None)
        .await?;
    Ok(())
}

fn dataset(port: u16, container: &str, name: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("cosmosdb:{DATABASE}.{container}"), name.to_string());
    dataset.params = Some(Params::from_string_map(HashMap::from([(
        "cosmosdb_connection_string".to_string(),
        format!("AccountEndpoint={};AccountKey={KEY};", endpoint(port)),
    )])));
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("arrow".to_string()),
            ..Acceleration::default()
        });
    }
    dataset
}

fn shows_pushdown(plan: &str) -> bool {
    plan.contains("CosmosDBExec") && plan.contains(" WHERE ")
}

fn cases() -> Vec<Case> {
    let p = |predicate: &str| {
        Case::pushed(format!("SELECT * FROM {{t}} WHERE {predicate} ORDER BY id"))
    };
    let e = |predicate: &str| {
        Case::either(format!("SELECT * FROM {{t}} WHERE {predicate} ORDER BY id"))
    };
    vec![
        // Strings: null and undefined properties, and a range bound past ASCII.
        p("status = 'active'"),
        p("status <> 'active'"),
        p("status > 'b'"),
        p("name < 'c'"),
        e("name > 'é'"),
        p("name = 'élan'"),
        p("status IN ('active', 'inactive')"),
        p("name LIKE 'al%'"),
        p("name LIKE 'a\\%%'"),
        p("name LIKE 'ac\\_%'"),
        e("name LIKE 'a_%'"),
        e("name ILIKE 'AL%'"),
        p("starts_with(name, '\u{1F4A1}')"),
        p("status IS NULL"),
        p("status IS NOT NULL"),
        p("name IS NULL"),
        // Integers and floats, zeros of both signs among them.
        p("age = 30"),
        p("age <> 30"),
        p("age > 7"),
        p("age >= 7"),
        p("age < 0"),
        p("age <= 0"),
        p("age BETWEEN 0 AND 30"),
        p("age IN (7, 30)"),
        p("age IS NULL"),
        // A short IN list with a NULL reaches the scan as `x = 1 OR NULL`.
        p("age IN (30, NULL)"),
        p("pk IN ('a', NULL)"),
        // `i64::MIN` has no `i64` absolute value, and a bound past it overflows.
        e("big = CAST('-9223372036854775808' AS BIGINT)"),
        e("big >= CAST('-9223372036854775808' AS BIGINT)"),
        p("big = 5"),
        p("score > 0"),
        p("score >= 0"),
        p("score < 0"),
        p("score = 0"),
        p("score <> 0"),
        p("score = 7"),
        p("score IN (1.5, 7.0)"),
        // Booleans.
        p("vip"),
        p("NOT vip"),
        p("vip = false"),
        p("vip IS NULL"),
        // The partition key.
        p("pk = 'a'"),
        p("pk = 'a' AND age > 1"),
        p("pk = 'zz'"),
        p("pk IN ('a', 'd')"),
        // Compound predicates.
        p("status = 'active' OR age = 30"),
        p("(pk = 'c' OR pk = 'd') AND status = 'active'"),
        e("NOT (status = 'active')"),
        e("name = status"),
        // A Spice function is evaluated by DataFusion, never by Cosmos DB.
        e("bucket(4, age) = 1"),
    ]
}

fn projection_and_limit_cases() -> Vec<Case> {
    vec![
        Case::either("SELECT name FROM {t} ORDER BY name NULLS LAST"),
        Case::either("SELECT id, age FROM {t} WHERE age > 1 ORDER BY id"),
        Case::either("SELECT count(*) FROM {t}"),
        Case::either("SELECT count(*) FROM {t} WHERE status = 'active'"),
        Case::limited("SELECT id FROM {t}", 3, Pushed::Either),
        Case::limited("SELECT id, name FROM {t} WHERE pk = 'a'", 1, Pushed::Yes),
    ]
}

#[tokio::test]
#[ignore = "starts the Cosmos DB emulator, a large image"]
async fn cosmosdb_pushdown_round_trips() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let container = start_emulator().await?;
            let port = container.host_port(8081)?;
            seed(port).await?;

            let app = AppBuilder::new("cosmosdb_pushdown_round_trips")
                .with_dataset(dataset(port, CONTAINER, "federated", false))
                .with_dataset(dataset(port, CONTAINER, "local", true))
                .with_dataset(dataset(
                    port,
                    MIXED_PARTITION_KEYS,
                    MIXED_PARTITION_KEYS,
                    false,
                ))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
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
            all.extend(projection_and_limit_cases());
            assert_round_trips(&rt, &tables, &all).await?;
            assert_partition_key_of_another_type_fails_decoding(&rt).await
        })
        .await
}

/// A partition key of another JSON type than its column's fails decoding
/// whether or not an equality on the partition key is pushed down. Such a key
/// lies in a logical partition of its own, which reading the one partition the
/// equality names would skip.
async fn assert_partition_key_of_another_type_fails_decoding(
    rt: &Arc<Runtime>,
) -> Result<(), anyhow::Error> {
    let pushed = format!("SELECT id FROM {MIXED_PARTITION_KEYS} WHERE pk = 'a'");
    let plan = to_pretty_display(&run_query(rt, &format!("EXPLAIN {pushed}")).await?)?.to_string();
    anyhow::ensure!(
        shows_pushdown(&plan),
        "expected the filter pushed down, plan:\n{plan}"
    );
    // `||` is not pushed down, so this reads every document's `pk`.
    let unfiltered = format!("SELECT id FROM {MIXED_PARTITION_KEYS} WHERE pk || '' = 'a'");
    for sql in [unfiltered, pushed] {
        match run_query(rt, &sql).await {
            Ok(batches) => anyhow::bail!(
                "{sql} returned {} rows instead of failing to decode the numeric partition key",
                batches.iter().map(RecordBatch::num_rows).sum::<usize>()
            ),
            Err(e) => anyhow::ensure!(
                e.to_string().contains("whilst decoding field 'pk'"),
                "{sql} failed other than on decoding the numeric partition key: {e}"
            ),
        }
    }
    Ok(())
}
