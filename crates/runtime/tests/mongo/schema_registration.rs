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

//! Integration tests for `MongoDB` connector registration behavior across the
//! combinations of: acceleration mode (federated / CDC change streams) ×
//! initial collection state (not found / empty / has documents).
//!
//! Key `MongoDB` behavior (after the datafusion-table-providers fix):
//!
//! * `read_provider` returns a **retriable** `EmptyCollection` error when the
//!   collection is empty or does not exist.  `load_dataset` retries with
//!   Fibonacci back-off until the collection has at least one document.
//!
//! * This mirrors `DynamoDB`'s `EmptyTable` error — both databases require at
//!   least one document/item at startup so the connector can infer the schema.
//!   Unlike `DynamoDB`, `MongoDB` does not yet support `dataset.columns` declared
//!   schema as a fallback (no declared-schema → registers-immediately path).

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use async_graphql::futures_util::TryStreamExt;
use mongodb::{Collection, bson::doc};
use runtime::Runtime;
use tokio::time::sleep;

use crate::configure_test_datafusion;
use crate::init_tracing;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};

use spicepod::semantic::Column;

use super::common::{get_mongodb_client, make_mongodb_dataset, start_mongodb_docker_container};

#[cfg(feature = "duckdb")]
use super::common::{
    get_mongodb_replica_set_client, make_mongodb_change_stream_dataset,
    start_mongodb_replica_set_docker_container,
};

// Ports 27021-27034 are reserved for this module.
// 27019 and 27020 are used by mod.rs.
const PORT_NO_ACCEL_NO_COLLECTION: u16 = 27021;
const PORT_NO_ACCEL_EMPTY: u16 = 27022;
const PORT_NO_ACCEL_WITH_DOCS: u16 = 27023;
const PORT_NO_ACCEL_SCHEMA_NO_COLLECTION: u16 = 27027;
const PORT_NO_ACCEL_SCHEMA_EMPTY: u16 = 27028;
const PORT_NO_ACCEL_SCHEMA_WITH_DOCS: u16 = 27029;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_NO_COLLECTION: u16 = 27024;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_EMPTY: u16 = 27025;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_WITH_DOCS: u16 = 27026;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_SCHEMA_NO_COLLECTION: u16 = 27030;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_SCHEMA_EMPTY: u16 = 27031;
#[cfg(feature = "duckdb")]
const PORT_CHANGES_SCHEMA_WITH_DOCS: u16 = 27032;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    snapshot_name: &str,
) -> Result<(), anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let batches = result.data.try_collect::<Vec<_>>().await?;
    let formatted = arrow::util::pretty::pretty_format_batches(&batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(snapshot_name, formatted);
    Ok(())
}

/// Insert `{_id, name, version}` documents — same shape used across all tests.
async fn insert_docs(
    collection: &Collection<mongodb::bson::Document>,
    range: std::ops::Range<i32>,
) {
    let docs: Vec<_> = range
        .map(|i| doc! { "_id": i, "name": format!("Item {i}"), "version": i64::from(i) })
        .collect();
    collection.insert_many(docs).await.expect("docs inserted");
}

async fn wait_for_dataset_rows(
    rt: &Runtime,
    collection_name: &str,
    expected: usize,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return false;
        }
        let result = rt
            .datafusion()
            .query_builder(&format!("SELECT COUNT(*) as cnt FROM {collection_name}"))
            .build()
            .run()
            .await;

        if let Ok(r) = result
            && let Ok(batches) = r.data.try_collect::<Vec<_>>().await
            && !batches.is_empty()
            && batches[0].num_rows() > 0
            && let Some(col) = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            && usize::try_from(col.value(0)).unwrap_or(0) >= expected
        {
            return true;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

// ===========================================================================
// Group 1 — Federated (no acceleration), retry until collection has documents
// ===========================================================================

/// Collection does not exist when the runtime starts.  `read_provider` returns
/// a retriable `EmptyCollection` error and `load_dataset` retries until the
/// collection is created and populated.
#[tokio::test]
async fn no_accel_collection_not_found_then_created_with_docs() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "no_schema_no_collection";

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(PORT_NO_ACCEL_NO_COLLECTION)
                .await
                .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_NO_COLLECTION)
                .await
                .map_err(|e| e.to_string())?;

            // Do NOT create the collection — connector must retry.
            let ds = make_mongodb_dataset(
                collection_name,
                collection_name,
                PORT_NO_ACCEL_NO_COLLECTION,
                false,
            );

            let app = AppBuilder::new("no_accel_no_collection")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // After a delay, create the collection and insert documents so
            // the retry loop can succeed.
            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let collection: Collection<mongodb::bson::Document> = client_for_setup
                    .database("testdb")
                    .collection(collection_name);
                insert_docs(&collection, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "no_accel_no_collection_data",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

/// Collection exists but is empty when the runtime starts.  `read_provider`
/// returns a retriable `EmptyCollection` error; the connector retries until
/// documents are inserted.
#[tokio::test]
async fn no_accel_empty_collection_then_docs_added() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "no_schema_empty";

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(PORT_NO_ACCEL_EMPTY)
                .await
                .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_EMPTY)
                .await
                .map_err(|e| e.to_string())?;

            // Create collection but leave it empty — connector must retry.
            client
                .database("testdb")
                .create_collection(collection_name)
                .await
                .map_err(|e| e.to_string())?;

            let ds =
                make_mongodb_dataset(collection_name, collection_name, PORT_NO_ACCEL_EMPTY, false);

            let app = AppBuilder::new("no_accel_empty").with_dataset(ds).build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let collection: Collection<mongodb::bson::Document> = client_for_setup
                    .database("testdb")
                    .collection(collection_name);
                insert_docs(&collection, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "no_accel_empty_data",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

/// Collection has documents when the runtime starts.  Schema is inferred from
/// the sampled documents; the dataset registers immediately.
#[tokio::test]
async fn no_accel_collection_with_documents_registers_immediately() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "no_schema_with_docs";

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(PORT_NO_ACCEL_WITH_DOCS)
                .await
                .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_WITH_DOCS)
                .await
                .map_err(|e| e.to_string())?;

            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            let ds = make_mongodb_dataset(
                collection_name,
                collection_name,
                PORT_NO_ACCEL_WITH_DOCS,
                false,
            );

            let app = AppBuilder::new("no_accel_with_docs")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "no_accel_with_docs_data",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 2 — Changes acceleration (replica set), retry until collection ready
// ===========================================================================

/// Collection does not exist (replica set).  `read_provider` returns a
/// retriable `EmptyCollection` error; the connector retries until the
/// collection is created and populated.  CDC picks up live changes afterward.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_collection_not_found_then_created_with_docs() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_no_collection";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_NO_COLLECTION).await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_NO_COLLECTION).await?;

            // Do NOT create the collection — connector must retry.
            let ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_NO_COLLECTION,
            );

            let app = AppBuilder::new("changes_no_collection")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let collection: Collection<mongodb::bson::Document> = client_for_setup
                    .database("testdb")
                    .collection(collection_name);
                insert_docs(&collection, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "initial snapshot should load 3 documents");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_no_collection_data",
            )
            .await?;

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Collection exists but is empty (replica set).  `read_provider` returns a
/// retriable `EmptyCollection` error; the connector retries until documents are
/// inserted and then starts the change stream from the initial snapshot.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_empty_collection_then_docs_added() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_EMPTY).await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_EMPTY).await?;

            // Create collection but leave it empty — connector must retry.
            client
                .database("testdb")
                .create_collection(collection_name)
                .await?;

            let ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_EMPTY,
            );

            let app = AppBuilder::new("changes_empty").with_dataset(ds).build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let collection: Collection<mongodb::bson::Document> = client_for_setup
                    .database("testdb")
                    .collection(collection_name);
                insert_docs(&collection, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "initial snapshot should load 3 documents");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_empty_data",
            )
            .await?;

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Collection has documents when the runtime starts (replica set).  The full
/// happy path: initial snapshot loads existing documents and live CDC events
/// are applied for subsequent inserts, updates, and deletes.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_collection_with_documents_snapshot_and_cdc() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_with_docs";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_WITH_DOCS).await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_WITH_DOCS).await?;
            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);

            insert_docs(&collection, 0..3).await;

            let ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_WITH_DOCS,
            );

            let app = AppBuilder::new("changes_with_docs")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "initial snapshot should load 3 documents");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_with_docs_initial",
            )
            .await?;

            // Live CDC: insert 2, update 1, delete 1 → expect 4 rows.
            insert_docs(&collection, 3..5).await;
            collection
                .update_one(
                    doc! { "_id": 1 },
                    doc! { "$set": { "name": "Item 1 updated" } },
                )
                .await?;
            collection.delete_one(doc! { "_id": 0 }).await?;

            let applied = wait_for_dataset_rows(&rt, collection_name, 4, 30).await;
            assert!(applied, "CDC should apply insert/update/delete events");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_with_docs_after_cdc",
            )
            .await?;

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 3 — Federated (no acceleration), declared schema via dataset.columns
//
// When dataset.columns are typed, the runtime computes dataset.schema and the
// connector passes it to table_provider_with_schema.  An empty or missing
// collection can then register immediately without any documents being present.
// ===========================================================================

/// Column declarations matching the test document shape.
fn declared_columns() -> Vec<Column> {
    vec![
        Column::new("_id").with_type("int32"),
        Column::new("name").with_type("text"),
        Column::new("version").with_type("bigint"),
    ]
}

/// Collection does not exist, declared schema provided — registers immediately
/// using the declared schema (no retry needed).
#[tokio::test]
async fn no_accel_declared_schema_collection_not_found_registers_immediately() -> Result<(), String>
{
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "schema_no_collection";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_docker_container(PORT_NO_ACCEL_SCHEMA_NO_COLLECTION)
                    .await
                    .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_SCHEMA_NO_COLLECTION)
                .await
                .map_err(|e| e.to_string())?;

            let mut ds = make_mongodb_dataset(
                collection_name,
                collection_name,
                PORT_NO_ACCEL_SCHEMA_NO_COLLECTION,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_no_collection")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // DESCRIBE shows the declared columns even though no documents exist.
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "no_accel_schema_no_collection_schema",
            )
            .await
            .map_err(|e| e.to_string())?;

            // Create collection + insert docs; federated queries go live to MongoDB.
            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "no_accel_schema_no_collection_data",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

/// Collection exists but is empty, declared schema provided — registers
/// immediately without waiting for documents.
#[tokio::test]
async fn no_accel_declared_schema_empty_collection_registers_immediately() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "schema_empty";

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(PORT_NO_ACCEL_SCHEMA_EMPTY)
                .await
                .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_SCHEMA_EMPTY)
                .await
                .map_err(|e| e.to_string())?;

            client
                .database("testdb")
                .create_collection(collection_name)
                .await
                .map_err(|e| e.to_string())?;

            let mut ds = make_mongodb_dataset(
                collection_name,
                collection_name,
                PORT_NO_ACCEL_SCHEMA_EMPTY,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_empty")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "no_accel_schema_empty_schema",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

/// Collection has documents, declared schema provided — schema is merged:
/// declared field types override inferred ones with the same name.
#[tokio::test]
async fn no_accel_declared_schema_collection_with_documents_uses_merged_schema()
-> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    register_test_connectors().await;

    let collection_name = "schema_with_docs";

    test_request_context()
        .scope(async {
            let running_container = start_mongodb_docker_container(PORT_NO_ACCEL_SCHEMA_WITH_DOCS)
                .await
                .map_err(|e| e.to_string())?;
            let client = get_mongodb_client(PORT_NO_ACCEL_SCHEMA_WITH_DOCS)
                .await
                .map_err(|e| e.to_string())?;

            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            let mut ds = make_mongodb_dataset(
                collection_name,
                collection_name,
                PORT_NO_ACCEL_SCHEMA_WITH_DOCS,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_with_docs")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "no_accel_schema_with_docs_schema",
            )
            .await
            .map_err(|e| e.to_string())?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "no_accel_schema_with_docs_data",
            )
            .await
            .map_err(|e| e.to_string())?;

            running_container
                .remove()
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 4 — Changes acceleration (replica set), declared schema
// ===========================================================================

/// Collection not found (replica set), declared schema — registers immediately;
/// CDC picks up documents once they are inserted.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_declared_schema_collection_not_found_registers_immediately()
-> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_schema_no_collection";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_SCHEMA_NO_COLLECTION)
                    .await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_SCHEMA_NO_COLLECTION).await?;

            let mut ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_SCHEMA_NO_COLLECTION,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("changes_schema_no_collection")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "changes_schema_no_collection_schema",
            )
            .await?;

            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "CDC should pick up inserted documents");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_schema_no_collection_data",
            )
            .await?;

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Collection empty (replica set), declared schema — registers immediately;
/// CDC picks up documents once they are inserted.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_declared_schema_empty_collection_registers_immediately() -> anyhow::Result<()>
{
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_schema_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_SCHEMA_EMPTY).await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_SCHEMA_EMPTY).await?;

            client
                .database("testdb")
                .create_collection(collection_name)
                .await?;

            let mut ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_SCHEMA_EMPTY,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("changes_schema_empty")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "changes_schema_empty_schema",
            )
            .await?;

            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "CDC should pick up inserted documents");

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_schema_empty_data",
            )
            .await?;

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Collection has documents (replica set), declared schema — initial snapshot
/// uses merged schema; CDC applies live changes.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn changes_accel_declared_schema_collection_with_documents_uses_merged_schema()
-> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,connector_mongodb=debug,data_components=debug,info",
    ));
    register_test_connectors().await;

    let collection_name = "changes_schema_with_docs";

    test_request_context()
        .scope(async {
            let running_container =
                start_mongodb_replica_set_docker_container(PORT_CHANGES_SCHEMA_WITH_DOCS).await?;
            let client = get_mongodb_replica_set_client(PORT_CHANGES_SCHEMA_WITH_DOCS).await?;
            let collection: Collection<mongodb::bson::Document> =
                client.database("testdb").collection(collection_name);
            insert_docs(&collection, 0..3).await;

            let mut ds = make_mongodb_change_stream_dataset(
                collection_name,
                collection_name,
                PORT_CHANGES_SCHEMA_WITH_DOCS,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("changes_schema_with_docs")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let loaded = wait_for_dataset_rows(&rt, collection_name, 3, 30).await;
            assert!(loaded, "initial snapshot should load 3 documents");

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {collection_name}"),
                "changes_schema_with_docs_schema",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {collection_name} ORDER BY _id"),
                "changes_schema_with_docs_initial",
            )
            .await?;

            insert_docs(&collection, 3..5).await;
            let applied = wait_for_dataset_rows(&rt, collection_name, 5, 30).await;
            assert!(applied, "CDC should apply new inserts");

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}
