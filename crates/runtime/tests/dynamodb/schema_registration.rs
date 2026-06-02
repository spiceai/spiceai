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

//! Integration tests for DynamoDB connector registration behavior across all
//! combinations of: acceleration mode (federated / CDC streams) × schema source
//! (inferred / declared) × initial table state (missing / empty / has rows /
//! streams disabled).

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        StreamSpecification, StreamViewType,
    },
};
use runtime::Runtime;
use spicepod::semantic::Column;

use crate::configure_test_datafusion;
use crate::init_tracing;
use crate::utils::{runtime_ready_check, test_request_context};

use super::streams::{
    create_table, ensure_dataset_rows, get_client, insert_rows, make_dynamodb_dataset,
    run_and_snapshot_query, start_dynamodb_docker_container, wait_for_dataset_rows,
    wait_for_dynamodb_source_rows,
};

// Ports 8020-8030 reserved for this module; 8001-8014 are used by streams.rs / dml.rs.
const PORT_NO_ACCEL_NO_SCHEMA_NO_TABLE: u16 = 8020;
const PORT_NO_ACCEL_NO_SCHEMA_EMPTY: u16 = 8031;
const PORT_NO_ACCEL_SCHEMA_NO_TABLE_EMPTY_RESULT: u16 = 8022;
const PORT_NO_ACCEL_SCHEMA_NO_TABLE_WITH_ROWS: u16 = 8023;
const PORT_NO_ACCEL_SCHEMA_EMPTY_IMMEDIATE: u16 = 8024;
const PORT_STREAMS_NO_SCHEMA_NO_TABLE: u16 = 8025;
const PORT_STREAMS_NO_SCHEMA_NO_STREAMS: u16 = 8026;
const PORT_STREAMS_NO_SCHEMA_EMPTY: u16 = 8027;
const PORT_STREAMS_SCHEMA_NO_TABLE: u16 = 8028;
const PORT_STREAMS_SCHEMA_NO_STREAMS: u16 = 8029;
const PORT_STREAMS_SCHEMA_EMPTY_IMMEDIATE: u16 = 8030;

const ACCESS_KEY: &str = "foo";
const SECRET_KEY: &str = "bar";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Creates a DynamoDB table with a single string hash key named `id`, but
/// **without** enabling DynamoDB Streams.  Used to test the retry path when
/// streams are required but not yet enabled.
async fn create_table_without_streams(client: &Client, table_name: &str) {
    client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("attribute definition"),
        )
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .expect("key schema element"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("table created without streams");
}

/// Enables DynamoDB Streams (`NewAndOldImages`) on an existing table.
async fn enable_streams(client: &Client, table_name: &str) {
    client
        .update_table()
        .table_name(table_name)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .expect("stream specification"),
        )
        .send()
        .await
        .expect("streams enabled");
}

/// Column declarations for the standard `{id, name, version}` test table.
fn declared_columns() -> Vec<Column> {
    vec![
        Column::new("id").with_type("text"),
        Column::new("name").with_type("text"),
        Column::new("version").with_type("bigint"),
    ]
}

// ===========================================================================
// Group 1 — No acceleration, no declared schema
// ===========================================================================

/// Table does not exist when the connector starts.  The connector retries
/// until the table is created with records, then registers successfully.
#[tokio::test(flavor = "multi_thread")]
async fn no_accel_no_schema_table_not_found_then_created_with_rows() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "no_schema_no_table";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_NO_ACCEL_NO_SCHEMA_NO_TABLE).await?;
            let client = get_client(PORT_NO_ACCEL_NO_SCHEMA_NO_TABLE, ACCESS_KEY, SECRET_KEY);

            // Do NOT create the table yet — connector must retry.
            let ds = make_dynamodb_dataset(
                table_name,
                PORT_NO_ACCEL_NO_SCHEMA_NO_TABLE,
                ACCESS_KEY,
                SECRET_KEY,
                false, // no acceleration
            );

            let app = AppBuilder::new("no_accel_no_schema_no_table")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // After a short delay, create the table and insert rows so the
            // connector's retry loop can succeed.
            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                create_table(&client_for_setup, table_name).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "no_accel_no_schema_no_table_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists but is empty when the connector starts (no declared schema, so
/// `EmptyTable` triggers retries).  Connector registers once rows are added.
#[tokio::test(flavor = "multi_thread")]
async fn no_accel_no_schema_empty_table_then_rows_added() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "no_schema_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_NO_ACCEL_NO_SCHEMA_EMPTY).await?;
            let client = get_client(PORT_NO_ACCEL_NO_SCHEMA_EMPTY, ACCESS_KEY, SECRET_KEY);

            // Create table but leave it empty — connector must retry.
            create_table(&client, table_name).await;

            let ds = make_dynamodb_dataset(
                table_name,
                PORT_NO_ACCEL_NO_SCHEMA_EMPTY,
                ACCESS_KEY,
                SECRET_KEY,
                false,
            );

            let app = AppBuilder::new("no_accel_no_schema_empty")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "no_accel_no_schema_empty_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 2 — No acceleration, explicit declared schema
// ===========================================================================

/// Table does not exist at start, but a declared schema is provided.
/// Once the table is created with **no rows**, the connector registers
/// using the declared schema (no rows required for schema inference).
#[tokio::test(flavor = "multi_thread")]
async fn no_accel_declared_schema_table_not_found_then_created_empty() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "schema_no_table_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_NO_ACCEL_SCHEMA_NO_TABLE_EMPTY_RESULT).await?;
            let client = get_client(
                PORT_NO_ACCEL_SCHEMA_NO_TABLE_EMPTY_RESULT,
                ACCESS_KEY,
                SECRET_KEY,
            );

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_NO_ACCEL_SCHEMA_NO_TABLE_EMPTY_RESULT,
                ACCESS_KEY,
                SECRET_KEY,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_no_table_empty")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Create an empty table after a delay — declared schema allows
            // registration without any rows.
            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                create_table(&client_for_setup, table_name).await;
                // No rows inserted.
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "no_accel_schema_no_table_empty_schema",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table does not exist at start, but a declared schema is provided.
/// Once the table is created **with rows**, the connector registers using
/// the merged schema (inferred types take precedence where not declared).
#[tokio::test(flavor = "multi_thread")]
async fn no_accel_declared_schema_table_not_found_then_created_with_rows() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "schema_no_table_rows";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_NO_ACCEL_SCHEMA_NO_TABLE_WITH_ROWS).await?;
            let client = get_client(
                PORT_NO_ACCEL_SCHEMA_NO_TABLE_WITH_ROWS,
                ACCESS_KEY,
                SECRET_KEY,
            );

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_NO_ACCEL_SCHEMA_NO_TABLE_WITH_ROWS,
                ACCESS_KEY,
                SECRET_KEY,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_no_table_rows")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                create_table(&client_for_setup, table_name).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            // Schema should match declared (merged with inferred).
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "no_accel_schema_no_table_rows_schema",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "no_accel_schema_no_table_rows_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists but is empty AND a declared schema is provided.
/// The connector registers **immediately** without waiting for rows.
#[tokio::test(flavor = "multi_thread")]
async fn no_accel_declared_schema_empty_table_registers_immediately() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "schema_empty_immediate";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_NO_ACCEL_SCHEMA_EMPTY_IMMEDIATE).await?;
            let client = get_client(PORT_NO_ACCEL_SCHEMA_EMPTY_IMMEDIATE, ACCESS_KEY, SECRET_KEY);

            // Table exists but empty — declared schema allows immediate registration.
            create_table(&client, table_name).await;

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_NO_ACCEL_SCHEMA_EMPTY_IMMEDIATE,
                ACCESS_KEY,
                SECRET_KEY,
                false,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("no_accel_schema_empty_immediate")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Schema matches declared columns.
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "no_accel_schema_empty_immediate_schema",
            )
            .await?;

            // Insert rows; they should be immediately queryable (federated, no CDC).
            insert_rows(&client, table_name, 0..3).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "no_accel_schema_empty_immediate_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 3 — Changes acceleration (Streams), no declared schema
// ===========================================================================

/// Table does not exist at start (changes acceleration, no declared schema).
/// Connector retries until the table is created with streams enabled and rows
/// are inserted.
#[tokio::test(flavor = "multi_thread")]
async fn streams_no_schema_table_not_found_then_created() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_no_schema_no_table";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_NO_SCHEMA_NO_TABLE).await?;
            let client = get_client(PORT_STREAMS_NO_SCHEMA_NO_TABLE, ACCESS_KEY, SECRET_KEY);

            let ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_NO_SCHEMA_NO_TABLE,
                ACCESS_KEY,
                SECRET_KEY,
                true, // changes acceleration
            );

            let app = AppBuilder::new("streams_no_schema_no_table")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                // create_table enables streams by default.
                create_table(&client_for_setup, table_name).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_no_schema_no_table_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists with rows, but streams are **not** enabled.  The connector
/// should retry until streams are enabled and rows are present.
///
/// Requires the connector to surface a retriable error when streams are absent.
#[tokio::test(flavor = "multi_thread")]
async fn streams_no_schema_streams_not_enabled_then_enabled() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_no_schema_no_streams";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_NO_SCHEMA_NO_STREAMS).await?;
            let client = get_client(PORT_STREAMS_NO_SCHEMA_NO_STREAMS, ACCESS_KEY, SECRET_KEY);

            // Table exists with rows, but no streams.
            create_table_without_streams(&client, table_name).await;
            insert_rows(&client, table_name, 0..3).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            let ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_NO_SCHEMA_NO_STREAMS,
                ACCESS_KEY,
                SECRET_KEY,
                true,
            );

            let app = AppBuilder::new("streams_no_schema_no_streams")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                enable_streams(&client_for_setup, table_name).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_no_schema_no_streams_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists with streams enabled, but is empty (no declared schema).
/// Connector retries until rows are inserted.
#[tokio::test(flavor = "multi_thread")]
async fn streams_no_schema_empty_table_then_rows_added() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_no_schema_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_NO_SCHEMA_EMPTY).await?;
            let client = get_client(PORT_STREAMS_NO_SCHEMA_EMPTY, ACCESS_KEY, SECRET_KEY);

            // Table with streams, but no rows yet.
            create_table(&client, table_name).await;

            let ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_NO_SCHEMA_EMPTY,
                ACCESS_KEY,
                SECRET_KEY,
                true,
            );

            let app = AppBuilder::new("streams_no_schema_empty")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_no_schema_empty_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

// ===========================================================================
// Group 4 — Changes acceleration (Streams), declared schema
// ===========================================================================

/// Table does not exist at start (changes + declared schema).  Connector
/// retries until the table is created with streams and rows; registers with
/// the merged (inferred + declared) schema.
#[tokio::test(flavor = "multi_thread")]
async fn streams_declared_schema_table_not_found_then_created() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_schema_no_table";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_SCHEMA_NO_TABLE).await?;
            let client = get_client(PORT_STREAMS_SCHEMA_NO_TABLE, ACCESS_KEY, SECRET_KEY);

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_SCHEMA_NO_TABLE,
                ACCESS_KEY,
                SECRET_KEY,
                true,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("streams_schema_no_table")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                create_table(&client_for_setup, table_name).await;
                insert_rows(&client_for_setup, table_name, 0..3).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "streams_schema_no_table_schema",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_schema_no_table_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists with rows, but streams are **not** enabled (changes + declared
/// schema).  Connector retries until streams are enabled; registers with
/// merged schema.
///
/// Requires the connector to surface a retriable error when streams are absent.
#[tokio::test(flavor = "multi_thread")]
async fn streams_declared_schema_streams_not_enabled_then_enabled() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_schema_no_streams";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_SCHEMA_NO_STREAMS).await?;
            let client = get_client(PORT_STREAMS_SCHEMA_NO_STREAMS, ACCESS_KEY, SECRET_KEY);

            // Table with rows but no streams.
            create_table_without_streams(&client, table_name).await;
            insert_rows(&client, table_name, 0..3).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_SCHEMA_NO_STREAMS,
                ACCESS_KEY,
                SECRET_KEY,
                true,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("streams_schema_no_streams")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            let client_for_setup = client.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                enable_streams(&client_for_setup, table_name).await;
            });

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "streams_schema_no_streams_schema",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_schema_no_streams_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}

/// Table exists with streams enabled but is **empty** and a declared schema is
/// provided.  The connector registers **immediately** using the declared schema
/// without waiting for rows; CDC picks up rows once they are inserted.
#[tokio::test(flavor = "multi_thread")]
async fn streams_declared_schema_empty_table_registers_immediately() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "streams_schema_empty";

    test_request_context()
        .scope(async {
            let running_container =
                start_dynamodb_docker_container(PORT_STREAMS_SCHEMA_EMPTY_IMMEDIATE).await?;
            let client = get_client(PORT_STREAMS_SCHEMA_EMPTY_IMMEDIATE, ACCESS_KEY, SECRET_KEY);

            // Empty table with streams — declared schema lets the connector
            // initialize without any rows.
            create_table(&client, table_name).await;

            let mut ds = make_dynamodb_dataset(
                table_name,
                PORT_STREAMS_SCHEMA_EMPTY_IMMEDIATE,
                ACCESS_KEY,
                SECRET_KEY,
                true,
            );
            ds.columns = declared_columns();

            let app = AppBuilder::new("streams_schema_empty_immediate")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Schema reflects declared columns.
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "streams_schema_empty_immediate_schema",
            )
            .await?;

            // Insert rows and verify they flow through CDC into the accelerated table.
            insert_rows(&client, table_name, 0..3).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;
            ensure_dataset_rows(&rt, table_name, 3, 60).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "streams_schema_empty_immediate_data",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;
            Ok(())
        })
        .await
}
