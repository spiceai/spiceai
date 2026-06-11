/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;
use std::time::SystemTime;

#[cfg(feature = "duckdb")]
use arrow::array::{Array, Int32Array, StringArray};
use common::{get_mongodb_client, make_mongodb_dataset, start_mongodb_docker_container};
#[cfg(feature = "duckdb")]
use common::{
    get_mongodb_replica_set_client, make_mongodb_change_stream_dataset,
    make_mongodb_change_stream_dataset_inferred, make_mongodb_extended_inference_dataset,
    start_mongodb_replica_set_docker_container,
};
#[cfg(feature = "duckdb")]
use datafusion::assert_batches_eq;
use mongodb::{Collection, bson::doc};

use chrono::{DateTime, Utc};
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
#[cfg(feature = "duckdb")]
use crate::utils::wait_until_true;
use crate::utils::{register_test_connectors, run_query, test_request_context};

pub mod common;
mod schema_registration;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const MONGODB_PORT1: u16 = 27019;
#[cfg(feature = "duckdb")]
const MONGODB_CHANGE_STREAM_PORT: u16 = 27020;
#[cfg(feature = "duckdb")]
const MONGODB_CHANGE_STREAM_INFERENCE_PORT: u16 = 27035;
#[cfg(feature = "duckdb")]
const MONGODB_INFERENCE_PORT: u16 = 27036;

#[instrument]
async fn init_mongodb_db(port: u16) -> Result<(), anyhow::Error> {
    tracing::debug!("INIT DB: test");
    let client = get_mongodb_client(port).await?;
    let database = client.database("testdb");

    tracing::debug!("DROP COLLECTION test");
    let _ = database
        .collection::<mongodb::bson::Document>("test")
        .drop()
        .await;

    let collection: Collection<mongodb::bson::Document> = database.collection("test");

    let ts = DateTime::parse_from_rfc3339("2019-01-01T00:00:00Z")?.with_timezone(&Utc);

    // Insert test documents
    let test_docs = vec![
        doc! {
            "_id": 1,
            "col_bit": true,
            "col_tiny": 1i32,
            "col_short": 1i32,
            "col_long": 1i64,
            "col_longlong": 1i64,
            "col_float": 1.1f64,
            "col_double": 1.1f64,
            "col_timestamp": mongodb::bson::DateTime::from(SystemTime::from(ts)),
            "col_date": mongodb::bson::DateTime::from(SystemTime::from(ts)),
            "col_time": "12:34:56",
            "col_blob": mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: b"blob".to_vec(),
            },
            "col_string": "string 🚀😊",
            "col_decimal": 1.11f64,
            "col_unsigned_int": 10u32,
            "col_char": "USA",
            "col_set": ["apple", "banana"],
            "col_json": doc! {
                "name": "John",
                "age": 30,
                "is_active": true,
                "balance": 1234.56
            }
        },
        doc! {
            "_id": 2,
            "col_bit": null,
            "col_tiny": null,
            "col_short": null,
            "col_long": null,
            "col_longlong": null,
            "col_float": null,
            "col_double": null,
            "col_timestamp": null,
            "col_date": null,
            "col_time": null,
            "col_blob": null,
            "col_string": null,
            "col_decimal": null,
            "col_unsigned_int": null,
            "col_char": null,
            "col_set": null,
            "col_json": null
        },
    ];

    collection.insert_many(test_docs).await?;
    Ok(())
}

/// Seed an `inventory` collection (documents plus a unique and a plain secondary
/// index) for the non-CDC extended schema-inference test. The secondary indexes
/// exercise the `listIndexes` inference path; the implicit `_id_` index must be
/// dropped as the inferred primary key.
#[cfg(feature = "duckdb")]
async fn init_mongodb_inventory_db(port: u16) -> Result<(), anyhow::Error> {
    let client = get_mongodb_client(port).await?;
    let database = client.database("testdb");
    let _ = database
        .collection::<mongodb::bson::Document>("inventory")
        .drop()
        .await;
    let collection: Collection<mongodb::bson::Document> = database.collection("inventory");
    collection
        .insert_many(vec![
            doc! { "_id": 1, "sku": "A", "name": "Widget", "quantity": 10 },
            doc! { "_id": 2, "sku": "B", "name": "Gadget", "quantity": 5 },
            doc! { "_id": 3, "sku": "C", "name": "Gizmo", "quantity": 7 },
        ])
        .await?;
    database
        .run_command(doc! {
            "createIndexes": "inventory",
            "indexes": [
                { "key": { "sku": 1 }, "name": "uq_sku", "unique": true },
                { "key": { "quantity": 1 }, "name": "idx_quantity" },
            ],
        })
        .await?;
    Ok(())
}

#[instrument]
#[cfg(feature = "duckdb")]
async fn init_mongodb_change_stream_db(port: u16) -> Result<(), anyhow::Error> {
    tracing::debug!("INIT CHANGE STREAM DB: test");
    let client = get_mongodb_replica_set_client(port).await?;
    let database = client.database("testdb");

    let _ = database
        .collection::<mongodb::bson::Document>("change_stream_users")
        .drop()
        .await;

    let collection: Collection<mongodb::bson::Document> =
        database.collection("change_stream_users");
    collection
        .insert_many(vec![
            doc! { "_id": 1, "name": "Ada" },
            doc! { "_id": 2, "name": "Grace" },
        ])
        .await?;

    Ok(())
}

#[cfg(feature = "duckdb")]
async fn change_stream_rows(rt: &Arc<Runtime>) -> Result<Vec<(i32, String)>, anyhow::Error> {
    let batches = run_query(rt, "SELECT _id, name FROM change_stream_users ORDER BY _id").await?;
    let mut rows = Vec::new();

    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("_id column should be Int32"))?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("name column should be Utf8"))?;

        for row_index in 0..batch.num_rows() {
            if names.is_null(row_index) {
                return Err(anyhow::anyhow!("name should not be null"));
            }
            rows.push((ids.value(row_index), names.value(row_index).to_string()));
        }
    }

    Ok(rows)
}

#[tokio::test]
async fn mongodb_integration_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(MONGODB_PORT1)
                .await
                .map_err(|e| {
                    tracing::error!("start_mongodb_docker_container: {e}");
                    e.to_string()
                })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mongodb_db(MONGODB_PORT1).await.map_err(|e| {
                    tracing::error!("Failed transiently  to initialize MongoDB database: {e}");
                    RetryError::transient(e)
                })
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MongoDB database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mongodb_integration_test")
                .with_dataset(make_mongodb_dataset("test", "test", MONGODB_PORT1, false))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let queries: QueryTests = vec![(
                "SELECT * FROM test",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 18, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("MongoDB Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!("mongodb_integration_test", results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mongodb_integration_test_{snapshot_suffix}"),
                    query,
                    false, // can't snapshot this plan
                    validate_result,
                )
                .await?;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}

/// Non-CDC counterpart to `mongodb_change_streams_infer_primary_key`: a `DuckDB`
/// full-refresh dataset with `schema_inference: extended` loads end-to-end against a
/// real `MongoDB`. The catalog query (`listIndexes`/`collStats`) runs on the server and
/// the inferred `_id` primary key, secondary indexes, and `_id` sort order are all
/// accepted by the accelerator — a correct row count proves none of those steps
/// errored. (Precise value-level mapping is covered by unit tests.)
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn mongodb_extended_schema_inference_loads_and_queries() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,connector_mongodb=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(MONGODB_INFERENCE_PORT).await?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mongodb_inventory_db(MONGODB_INFERENCE_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await?;

            let app = AppBuilder::new("mongodb_extended_schema_inference")
                .with_dataset(make_mongodb_extended_inference_dataset(
                    "inventory",
                    "inventory",
                    MONGODB_INFERENCE_PORT,
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!(
                        "Timed out waiting for MongoDB extended-inference dataset to load"
                    ));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            crate::utils::runtime_ready_check(&rt).await;

            // The full extended-inference pipeline must succeed end-to-end: the MongoDB
            // catalog query runs against the real server, and the inferred `_id`
            // primary key, secondary indexes, and `_id` sort are all accepted by the
            // DuckDB accelerator. A correct row count proves the dataset loaded.
            let results = run_query(&rt, "SELECT COUNT(*) AS n FROM inventory").await?;
            assert_batches_eq!(
                &[
                    "+---+", //
                    "| n |", //
                    "+---+", //
                    "| 3 |", //
                    "+---+", //
                ],
                &results
            );

            running_container
                .remove()
                .await
                .map_err(|e| anyhow::anyhow!("failed to remove container: {e}"))?;
            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_change_streams_apply_insert_update_delete() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(MONGODB_CHANGE_STREAM_PORT).await?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mongodb_change_stream_db(MONGODB_CHANGE_STREAM_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await?;

            let app = AppBuilder::new("mongodb_change_streams_apply_insert_update_delete")
                .with_dataset(make_mongodb_change_stream_dataset(
                    "change_stream_users",
                    "change_stream_users",
                    MONGODB_CHANGE_STREAM_PORT,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for MongoDB Change Streams dataset to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            crate::utils::runtime_ready_check(&rt).await;
            let rt = Arc::new(rt);

            let initial_rows_loaded = wait_until_true(std::time::Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move {
                    change_stream_rows(&rt)
                        .await
                        .is_ok_and(|rows| rows == vec![(1, "Ada".to_string()), (2, "Grace".to_string())])
                }
            })
            .await;
            assert!(initial_rows_loaded, "initial MongoDB snapshot should load");

            let client = get_mongodb_replica_set_client(MONGODB_CHANGE_STREAM_PORT).await?;
            let collection: Collection<mongodb::bson::Document> = client
                .database("testdb")
                .collection("change_stream_users");
            collection
                .insert_one(doc! { "_id": 3, "name": "Katherine" })
                .await?;
            collection
                .update_one(
                    doc! { "_id": 2 },
                    doc! { "$set": { "name": "Grace Hopper" } },
                )
                .await?;
            collection.delete_one(doc! { "_id": 1 }).await?;

            let changes_applied = wait_until_true(std::time::Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move {
                    change_stream_rows(&rt).await.is_ok_and(|rows| {
                        rows == vec![
                            (2, "Grace Hopper".to_string()),
                            (3, "Katherine".to_string()),
                        ]
                    })
                }
            })
            .await;

            assert!(
                changes_applied,
                "MongoDB Change Streams should apply insert, update, and delete events"
            );

            rt.shutdown().await;
            running_container.remove().await?;

            Ok(())
        })
        .await
}

/// `MongoDB` Streams (`refresh_mode: changes`) work with `schema_inference: extended`
/// and no explicit `primary_key`/`on_conflict`: inference supplies `_id` as the
/// primary key plus the matching upsert, which the change-stream path requires.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_change_streams_infer_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(MONGODB_CHANGE_STREAM_INFERENCE_PORT)
                    .await?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mongodb_change_stream_db(MONGODB_CHANGE_STREAM_INFERENCE_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await?;

            let app = AppBuilder::new("mongodb_change_streams_infer_primary_key")
                .with_dataset(make_mongodb_change_stream_dataset_inferred(
                    "change_stream_users",
                    "change_stream_users",
                    MONGODB_CHANGE_STREAM_INFERENCE_PORT,
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for MongoDB Change Streams dataset to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            crate::utils::runtime_ready_check(&rt).await;
            let rt = Arc::new(rt);

            // The change stream only starts if `resolve_primary_keys` succeeds, which
            // requires `primary_key: _id` plus an `_id` upsert. Both come from extended
            // schema inference here, not explicit configuration.
            let initial_rows_loaded = wait_until_true(std::time::Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move {
                    change_stream_rows(&rt).await.is_ok_and(|rows| {
                        rows == vec![(1, "Ada".to_string()), (2, "Grace".to_string())]
                    })
                }
            })
            .await;
            assert!(
                initial_rows_loaded,
                "MongoDB change stream should start and snapshot via the inferred `_id` primary key"
            );

            // An UPDATE should upsert in place (inferred on_conflict), not append.
            let client =
                get_mongodb_replica_set_client(MONGODB_CHANGE_STREAM_INFERENCE_PORT).await?;
            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection("change_stream_users");
            collection
                .update_one(doc! { "_id": 2 }, doc! { "$set": { "name": "Grace Hopper" } })
                .await?;

            let update_applied = wait_until_true(std::time::Duration::from_secs(30), || {
                let rt = Arc::clone(&rt);
                async move {
                    change_stream_rows(&rt).await.is_ok_and(|rows| {
                        rows == vec![(1, "Ada".to_string()), (2, "Grace Hopper".to_string())]
                    })
                }
            })
            .await;
            assert!(
                update_applied,
                "the inferred `_id` upsert should apply UPDATE events in place"
            );

            rt.shutdown().await;
            running_container.remove().await?;

            Ok(())
        })
        .await
}
