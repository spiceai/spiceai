/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

use crate::docker::{ContainerRunnerBuilder, RunningContainer};
use crate::utils::{runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};
use app::AppBuilder;
use async_graphql::futures_util::TryStreamExt;
use aws_config::{BehaviorVersion, Region, SdkConfig, retry::RetryConfig};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType,
    },
};
use bollard::secret::HealthConfig;
use runtime::Runtime;
use spicepod::acceleration::{Mode, RefreshMode};
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::semantic::Column;
use spicepod::{
    acceleration::Acceleration, component::dataset::Dataset, param::Params as DatasetParams,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::instrument;

const DYNAMODB_DOCKER_CONTAINER: &str = "runtime-integration-test-dynamodb";
const PORT1: u16 = 8001;
const PORT2: u16 = 8002;
const PORT3: u16 = 8003;
const PORT4: u16 = 8004;
const PORT5: u16 = 8005;
const PORT6: u16 = 8006;
const PORT7: u16 = 8007;
const PORT8: u16 = 8008;
const DYNAMODB_HOST_READY_TIMEOUT: Duration = Duration::from_secs(60);

#[instrument]
pub async fn start_dynamodb_docker_container(
    port: u16,
) -> Result<RunningContainer<'static>, anyhow::Error> {
    let container_name = format!("{DYNAMODB_DOCKER_CONTAINER}-{port}");
    let container_name: &'static str = Box::leak(container_name.into_boxed_str());
    let running_container = ContainerRunnerBuilder::new(container_name)
        .image("amazon/dynamodb-local:latest".to_string())
        .add_port_binding(8000, port)
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "curl -s http://localhost:8000 | grep -q 'MissingAuthenticationToken' || exit 1"
                    .to_string(),
            ]),
            interval: Some(2_000_000_000), // 2 seconds
            timeout: Some(10_000_000_000), // 10 seconds
            retries: Some(15),
            start_period: Some(10_000_000_000), // 10 seconds
            start_interval: None,
        })
        .build()?
        .run(None)
        .await?;

    wait_for_dynamodb_host_port(port).await?;
    Ok(running_container)
}

async fn wait_for_dynamodb_host_port(port: u16) -> Result<(), anyhow::Error> {
    let client = get_client(port, "foo", "bar");
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= DYNAMODB_HOST_READY_TIMEOUT {
        match client.list_tables().send().await {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error.to_string()),
        }

        sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!(
        "DynamoDB Local container started but host port {port} was not ready within {}s. Last error: {}",
        DYNAMODB_HOST_READY_TIMEOUT.as_secs(),
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

async fn wait_for_dynamodb_source_rows(
    client: &Client,
    table_name: &str,
    expected_rows: usize,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let mut last_error = None;

    while start_time.elapsed() <= Duration::from_secs(timeout_secs) {
        match client.scan().table_name(table_name).send().await {
            Ok(output) => {
                let actual_rows = usize::try_from(output.count()).unwrap_or(0);
                if actual_rows >= expected_rows {
                    return Ok(());
                }
                last_error = Some(format!(
                    "source table has {actual_rows} rows; expected at least {expected_rows}"
                ));
            }
            Err(error) => last_error = Some(error.to_string()),
        }

        sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting for DynamoDB source table {table_name} to have at least {expected_rows} rows within {timeout_secs}s. Last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

async fn ensure_dataset_rows(
    rt: &Runtime,
    table_name: &str,
    expected_rows: usize,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        wait_for_dataset_rows(rt, table_name, expected_rows, timeout_secs).await,
        "Timed out waiting for dataset {table_name} to have at least {expected_rows} rows within {timeout_secs}s"
    );
    Ok(())
}

async fn ensure_exact_dataset_rows(
    rt: &Runtime,
    table_name: &str,
    expected_rows: usize,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    anyhow::ensure!(
        wait_for_exact_row_count(rt, table_name, expected_rows, timeout_secs).await,
        "Timed out waiting for dataset {table_name} to have exactly {expected_rows} rows within {timeout_secs}s"
    );
    Ok(())
}

pub fn make_dynamodb_dataset(
    table_name: &str,
    port: u16,
    access_key: &str,
    secret_key: &str,
    accelerated: bool,
) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{table_name}"), table_name.to_string());
    let params = HashMap::from([
        (
            "dynamodb_aws_access_key_id".to_string(),
            access_key.to_string(),
        ),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            secret_key.to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ]);
    dataset.params = Some(DatasetParams::from_string_map(params));
    if accelerated {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });
    }
    dataset
}

async fn create_table(client: &Client, table_name: &str) {
    client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("Attribute definition created"),
        )
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .expect("Key schema element created"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .expect("Stream specification created"),
        )
        .send()
        .await
        .expect("Table created");
}

fn get_client(port: u16, access_key: &str, secret_key: &str) -> Client {
    let config = SdkConfig::builder()
        .endpoint_url(format!("http://localhost:{port}"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::from_keys(
            access_key, secret_key, None,
        )))
        .retry_config(RetryConfig::standard().with_max_attempts(5))
        .behavior_version(BehaviorVersion::latest())
        .region(Some(Region::from_static("us-east-1")))
        .build();
    Client::new(&config)
}

async fn insert_rows(client: &Client, table_name: &str, range: Range<usize>) {
    for i in range {
        client
            .put_item()
            .table_name(table_name)
            .item("id", AttributeValue::S(format!("id-{i}")))
            .item("name", AttributeValue::S(format!("Item {i}")))
            .item("version", AttributeValue::N(i.to_string()))
            .send()
            .await
            .expect("Failed to insert item");
    }
}

async fn insert_item(client: &Client, table_name: &str, id: &str, name: &str, version: i32) {
    client
        .put_item()
        .table_name(table_name)
        .item("id", AttributeValue::S(id.to_string()))
        .item("name", AttributeValue::S(name.to_string()))
        .item("version", AttributeValue::N(version.to_string()))
        .send()
        .await
        .expect("Failed to insert item");
}

async fn delete_item(client: &Client, table_name: &str, id: &str) {
    client
        .delete_item()
        .table_name(table_name)
        .key("id", AttributeValue::S(id.to_string()))
        .send()
        .await
        .expect("Failed to delete item");
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "test_table";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT2).await?;

            let client = get_client(PORT2, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, "test_table", 0..5).await;
            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            let app = AppBuilder::new("dynamodb_integration_test")
                .with_dataset(make_dynamodb_dataset(
                    table_name, PORT2, access_key, secret_key, true,
                ))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_dataset_rows(&rt, table_name, 5, 30).await?;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test1",
            )
            .await?;

            insert_rows(&client, "test_table", 5..7).await;
            ensure_dataset_rows(&rt, table_name, 7, 30).await?;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test2",
            )
            .await?;

            insert_rows(&client, "test_table", 7..10).await;
            ensure_dataset_rows(&rt, table_name, 10, 30).await?;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "test3",
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

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_delete() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "batch_delete_test";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT1).await?;
            let client = get_client(PORT1, access_key, secret_key);

            create_table(&client, table_name).await;
            for i in 0..5 {
                insert_item(
                    &client,
                    table_name,
                    &format!("id-{i}"),
                    &format!("Item {i}"),
                    i,
                )
                .await;
            }
            for i in 5..8 {
                insert_item(
                    &client,
                    table_name,
                    &format!("id-{i}"),
                    &format!("Item {i}"),
                    i,
                )
                .await;
            }

            delete_item(&client, table_name, "id-5").await;
            delete_item(&client, table_name, "id-6").await;
            delete_item(&client, table_name, "id-7").await;

            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            let app = AppBuilder::new("dynamodb_batch_delete_test")
                .with_dataset(make_dynamodb_dataset(
                    table_name, PORT1, access_key, secret_key, true,
                ))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;
            ensure_exact_dataset_rows(&rt, table_name, 5, 30).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "batch_delete_final_state",
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

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let data = query_result.data.try_collect::<Vec<_>>().await?;

    let formatted = arrow::util::pretty::pretty_format_batches(&data)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}

pub fn make_dynamodb_dataset_with_file_accel(
    table_name: &str,
    port: u16,
    access_key: &str,
    secret_key: &str,
    duckdb_path: &str,
    lag_exceeds_behavior: Option<&str>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{table_name}"), table_name.to_string());
    let mut params = HashMap::from([
        (
            "dynamodb_aws_access_key_id".to_string(),
            access_key.to_string(),
        ),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            secret_key.to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ]);
    if let Some(behavior) = lag_exceeds_behavior {
        params.insert(
            "lag_exceeds_shard_retention_behavior".to_string(),
            behavior.to_string(),
        );
    }
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Changes),
        engine: Some("duckdb".to_string()),
        params: Some(DatasetParams::from_string_map(HashMap::from([(
            "duckdb_file".to_string(),
            duckdb_path.to_string(),
        )]))),
        ..Acceleration::default()
    });
    dataset
}

/// Creates a mock checkpoint with fake shard IDs from scratch.
/// Used when no real checkpoint exists yet (e.g., testing fresh checkpoint error propagation).
fn create_mock_checkpoint(duckdb_path: &str, dataset_name: &str, hours_ago: u64) {
    use duckdb::Connection;
    use std::time::{SystemTime, UNIX_EPOCH};

    let conn = Connection::open(duckdb_path).expect("Failed to open DuckDB file");

    // Create table if not exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS spice_sys_dynamodb_streams (
            dataset_name TEXT PRIMARY KEY,
            checkpoint_data TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )",
        [],
    )
    .expect("Failed to create checkpoint table");

    // Create a properly formatted checkpoint with non-existent shard ID
    let checkpoint_updated_at_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    // Build checkpoint JSON with all required fields matching the Checkpoint struct
    let checkpoint_json = format!(
        r#"{{"shards":{{"fake-nonexistent-shard-id":{{"sequence_number":"00000000000000000000001","parent_id":null,"updated_at":{{"secs_since_epoch":{checkpoint_updated_at_secs},"nanos_since_epoch":0}},"position":"At"}}}}}}"#
    );

    // Calculate timestamp hours_ago for the database updated_at column
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let timestamp_secs = now.as_secs() - (hours_ago * 60 * 60);

    conn.execute(
        &format!(
            "INSERT INTO spice_sys_dynamodb_streams (dataset_name, checkpoint_data, created_at, updated_at)
             VALUES (?, ?, to_timestamp({timestamp_secs}), to_timestamp({timestamp_secs}))"
        ),
        [dataset_name, &checkpoint_json],
    )
    .expect("Failed to insert mock checkpoint");
}

/// Corrupts the checkpoint by replacing shard IDs with fake ones and making the timestamp old.
/// This simulates the scenario where shards have expired (>24h `DynamoDB` retention).
fn corrupt_checkpoint_for_shard_not_found(duckdb_path: &str, dataset_name: &str, hours_ago: u64) {
    use duckdb::Connection;
    use std::time::{SystemTime, UNIX_EPOCH};

    let conn = Connection::open(duckdb_path).expect("Failed to open DuckDB file");

    // Read the existing checkpoint
    let checkpoint_data: String = conn
        .query_row(
            "SELECT checkpoint_data FROM spice_sys_dynamodb_streams WHERE dataset_name = ?",
            [dataset_name],
            |row| row.get(0),
        )
        .expect("Failed to read checkpoint");

    // Parse checkpoint JSON and replace shard IDs with fake ones
    let mut checkpoint: serde_json::Value =
        serde_json::from_str(&checkpoint_data).expect("Failed to parse checkpoint JSON");

    if let Some(shards) = checkpoint.get_mut("shards").and_then(|s| s.as_object_mut()) {
        // Collect existing shard data (collect is needed because we clear the map before re-inserting)
        #[expect(clippy::needless_collect)]
        let shard_values: Vec<_> = shards.values().cloned().collect();

        // Clear and replace with fake shard IDs
        shards.clear();
        for (i, value) in shard_values.into_iter().enumerate() {
            shards.insert(format!("fake-nonexistent-shard-{i}"), value);
        }
    }

    let corrupted_checkpoint =
        serde_json::to_string(&checkpoint).expect("Failed to serialize corrupted checkpoint");

    // Calculate timestamp hours_ago
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let timestamp_secs = now.as_secs() - (hours_ago * 60 * 60);

    // Update checkpoint with corrupted data and old timestamp
    conn.execute(
        &format!(
            "UPDATE spice_sys_dynamodb_streams
             SET checkpoint_data = ?, updated_at = to_timestamp({timestamp_secs})
             WHERE dataset_name = ?"
        ),
        [&corrupted_checkpoint, dataset_name],
    )
    .expect("Failed to update checkpoint");
}

/// Resolves the underlying base table name for an accelerated `DuckDB` table.
///
/// After `InsertOp::Overwrite`, the accelerator exposes data through a view over an internal
/// `__data_<table_name>_<timestamp>` table. Raw `DuckDB` DML must target the latest internal
/// table; if no internal table exists, the definition name is used directly.
fn resolve_acceleration_base_table(conn: &duckdb::Connection, table_name: &str) -> String {
    let prefix = format!("__data_{table_name}_");
    let Ok(mut stmt) =
        conn.prepare("SELECT table_name FROM duckdb_tables() WHERE starts_with(table_name, ?)")
    else {
        return table_name.to_string();
    };

    let Ok(rows) = stmt.query_map([&prefix], |row| row.get::<usize, String>(0)) else {
        return table_name.to_string();
    };

    let mut latest: Option<(String, u64)> = None;
    for row in rows.flatten() {
        let Some(inner) = row.strip_prefix("__data_") else {
            continue;
        };
        let Some((name_part, ts_part)) = inner.rsplit_once('_') else {
            continue;
        };
        if name_part != table_name {
            continue;
        }
        let Ok(ts) = ts_part.parse::<u64>() else {
            continue;
        };
        if latest.as_ref().is_none_or(|(_, prev_ts)| ts > *prev_ts) {
            latest = Some((row, ts));
        }
    }

    latest.map_or_else(|| table_name.to_string(), |(name, _)| name)
}

/// Deletes rows from the accelerated table to verify rebootstrap restores them.
fn delete_rows_from_acceleration(duckdb_path: &str, table_name: &str, ids_to_delete: &[&str]) {
    use duckdb::Connection;

    let conn = Connection::open(duckdb_path).expect("Failed to open DuckDB file");
    let base_table = resolve_acceleration_base_table(&conn, table_name);

    for id in ids_to_delete {
        conn.execute(&format!(r#"DELETE FROM "{base_table}" WHERE id = ?"#), [id])
            .expect("Failed to delete row");
    }
}

/// Gets the row count from the accelerated table.
fn get_acceleration_row_count(duckdb_path: &str, table_name: &str) -> usize {
    use duckdb::Connection;

    let conn = Connection::open(duckdb_path).expect("Failed to open DuckDB file");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |row| {
            row.get(0)
        })
        .unwrap_or(0);
    usize::try_from(count).unwrap_or(0)
}

async fn wait_for_dataset_error(rt: &Runtime, dataset_name: &str, timeout_secs: u64) -> bool {
    use datafusion::sql::TableReference;

    let table_ref = TableReference::bare(dataset_name);
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return false;
        }
        // Check dataset status
        let statuses = rt.datafusion().runtime_status().get_dataset_statuses();
        if let Some(status) = statuses.get(&table_ref)
            && status.is_error()
        {
            return true;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_dataset_rows(
    rt: &Runtime,
    table_name: &str,
    expected_rows: usize,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return false;
        }
        let query_result = rt
            .datafusion()
            .query_builder(&format!("SELECT COUNT(*) as cnt FROM {table_name}"))
            .build()
            .run()
            .await;

        if let Ok(result) = query_result
            && let Ok(batches) = result.data.try_collect::<Vec<_>>().await
            && !batches.is_empty()
            && batches[0].num_rows() > 0
            && let Some(col) = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
            && usize::try_from(col.value(0)).unwrap_or(0) >= expected_rows
        {
            return true;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Inserts phantom rows directly into the `DuckDB` acceleration table, bypassing `DynamoDB`.
///
/// These rows have IDs that do not exist in `DynamoDB`, so no CDC stream events will ever
/// delete them. An `InsertOp::Overwrite` rebootstrap will remove them (the table is
/// replaced atomically); an `InsertOp::Append` rebootstrap leaves them in place.
fn add_phantom_rows_to_acceleration(duckdb_path: &str, table_name: &str, count: usize) {
    use duckdb::Connection;
    let conn = Connection::open(duckdb_path).expect("Failed to open DuckDB file");
    let base_table = resolve_acceleration_base_table(&conn, table_name);
    for i in 0..count {
        let version = i64::try_from(i).expect("phantom row index fits in i64");
        conn.execute(
            &format!(r#"INSERT INTO "{base_table}" (id, name, version) VALUES (?, ?, ?)"#),
            duckdb::params![format!("phantom-{i}"), format!("Phantom {i}"), version],
        )
        .expect("Failed to insert phantom row");
    }
}

/// Like `wait_for_dataset_rows` but requires an exact row count, not just a minimum.
/// Necessary for detecting stale phantom rows that survive an `InsertOp::Append` rebootstrap.
async fn wait_for_exact_row_count(
    rt: &Runtime,
    table_name: &str,
    expected_rows: usize,
    timeout_secs: u64,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            return false;
        }
        let query_result = rt
            .datafusion()
            .query_builder(&format!("SELECT COUNT(*) as cnt FROM {table_name}"))
            .build()
            .run()
            .await;

        if let Ok(result) = query_result
            && let Ok(batches) = result.data.try_collect::<Vec<_>>().await
            && !batches.is_empty()
            && batches[0].num_rows() > 0
            && let Some(col) = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
        {
            let actual = usize::try_from(col.value(0)).unwrap_or(0);
            if actual == expected_rows {
                return true;
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Verifies that rebootstrap removes phantom rows that exist in `DuckDB` but not in `DynamoDB`.
///
/// The test injects rows directly into the `DuckDB` acceleration file with IDs that do not
/// exist in `DynamoDB`.  No CDC delete events are ever generated for these IDs, so the only
/// mechanism that can remove them is an atomic table replacement.
///
/// Fails with `InsertOp::Append`: phantom IDs have no PK match in the rebootstrap scan,
/// so they are left in place → count stays at 8 → timeout.
/// Passes with `InsertOp::Overwrite`: the internal table is replaced atomically with the
/// 5-row scan result → phantoms gone → count = 5.
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_rebootstrap_removes_rows_deleted_from_source() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "rebootstrap_stale_rows";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT7).await?;
            let client = get_client(PORT7, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, table_name, 0..5).await;
            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            let temp_dir = tempfile::tempdir()?;
            let duckdb_path = temp_dir.path().join("test.duckdb");
            let duckdb_path_str = duckdb_path.to_str().expect("path should be valid UTF-8");

            // Phase 1: Bootstrap normally — acceleration and DynamoDB both have 5 rows.
            {
                let app = AppBuilder::new("dynamodb_rebootstrap_stale_phase1")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT7,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_after_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 1: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                runtime_ready_check(&rt).await;
                ensure_exact_dataset_rows(&rt, table_name, 5, 30).await?;

                let initial_count = get_acceleration_row_count(duckdb_path_str, table_name);
                assert_eq!(initial_count, 5, "Phase 1: Should have 5 rows after initial load");
            }

            // Phase 2: While Spice is down, inject 3 phantom rows directly into DuckDB.
            // These IDs do not exist in DynamoDB, so no CDC stream events can remove them.
            // Only an atomic table replacement (InsertOp::Overwrite) can purge them.
            add_phantom_rows_to_acceleration(duckdb_path_str, table_name, 3);

            let pre_rebootstrap_count = get_acceleration_row_count(duckdb_path_str, table_name);
            assert_eq!(
                pre_rebootstrap_count, 8,
                "Phase 2: Acceleration should have 8 rows (5 original + 3 phantoms)"
            );

            // Corrupt checkpoint (20 h ago) to trigger rebootstrap on next startup.
            corrupt_checkpoint_for_shard_not_found(duckdb_path_str, table_name, 20);

            // Phase 3: Restart Spice — rebootstrap must replace the table, not append to it.
            // DynamoDB still has 5 rows; after Overwrite rebootstrap, the 3 phantoms are gone.
            {
                let app = AppBuilder::new("dynamodb_rebootstrap_stale_phase3")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT7,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_after_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 3: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // DynamoDB has 5 rows (id-0..id-4). Phantoms must not survive.
                let has_exact_rows = wait_for_exact_row_count(&rt, table_name, 5, 30).await;
                assert!(
                    has_exact_rows,
                    "Rebootstrap must remove phantom rows that exist only in DuckDB. \
                     Stale phantom rows survived — InsertOp::Append was used instead of Overwrite"
                );

                runtime_ready_check(&rt).await;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_shard_not_found_fresh_checkpoint_propagates_error() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "shard_not_found_fresh";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT3).await?;
            let client = get_client(PORT3, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, table_name, 0..5).await;
            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            // Create temp DuckDB file and insert fake checkpoint with FRESH timestamp (1h ago)
            let temp_dir = tempfile::tempdir()?;
            let duckdb_path = temp_dir.path().join("test.duckdb");
            let duckdb_path_str = duckdb_path.to_str().expect("path should be valid UTF-8");

            // Insert fake checkpoint that's only 1h old (< 18h threshold)
            // This should propagate error regardless of lag_exceeds_behavior setting
            create_mock_checkpoint(duckdb_path_str, table_name, 1);

            let app = AppBuilder::new("dynamodb_fresh_checkpoint_test")
                .with_dataset(make_dynamodb_dataset_with_file_accel(
                    table_name,
                    PORT3,
                    access_key,
                    secret_key,
                    duckdb_path_str,
                    Some("ready_after_load"), // Setting doesn't matter for fresh checkpoint
                ))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(30)) => {}
                () = cloned_rt.load_components() => {}
            }

            // Fresh checkpoint + ShardNotFound should result in error
            let is_error = wait_for_dataset_error(&rt, table_name, 10).await;
            assert!(
                is_error,
                "Dataset should be in error state when fresh checkpoint has ShardNotFound"
            );

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_shard_not_found_expired_checkpoint_ready_after_load() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "shard_not_found_ready_after";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT4).await?;
            let client = get_client(PORT4, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, table_name, 0..5).await;
            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            // Create temp DuckDB file for acceleration
            let temp_dir = tempfile::tempdir()?;
            let duckdb_path = temp_dir.path().join("test.duckdb");
            let duckdb_path_str = duckdb_path.to_str().expect("path should be valid UTF-8");

            // === PHASE 1: Start Spice normally to create real checkpoint ===
            {
                let app = AppBuilder::new("dynamodb_ready_after_load_phase1")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT4,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_after_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 1: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // Wait for data to be loaded and checkpoint created
                runtime_ready_check(&rt).await;
                ensure_exact_dataset_rows(&rt, table_name, 5, 30).await?;

                // Verify initial data is loaded
                let initial_count = get_acceleration_row_count(duckdb_path_str, table_name);
                assert_eq!(initial_count, 5, "Phase 1: Should have 5 rows after initial load");

                // Runtime drops here, stopping Spice
            }

            // === PHASE 2: Corrupt checkpoint, delete some rows, and add new records while Spice is down ===
            // Make checkpoint reference non-existent shards and set timestamp to 20h ago
            corrupt_checkpoint_for_shard_not_found(duckdb_path_str, table_name, 20);

            // Delete some rows from acceleration to verify rebootstrap restores them
            delete_rows_from_acceleration(duckdb_path_str, table_name, &["id-0", "id-1", "id-2"]);
            let count_after_delete = get_acceleration_row_count(duckdb_path_str, table_name);
            assert_eq!(count_after_delete, 2, "Phase 2: Should have 2 rows after deletion");

            // Add new records to DynamoDB while Spice is down
            insert_rows(&client, table_name, 5..8).await;
            wait_for_dynamodb_source_rows(&client, table_name, 8, 30).await?;

            // === PHASE 3: Start Spice again - should detect ShardNotFound and rebootstrap ===
            {
                let app = AppBuilder::new("dynamodb_ready_after_load_phase3")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT4,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_after_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 3: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // With ready_after_load, should rebootstrap and restore all 8 rows (5 original + 3 new)
                let has_rows = wait_for_dataset_rows(&rt, table_name, 8, 30).await;
                assert!(has_rows, "Phase 3: Dataset should have 8 rows after rebootstrap with ready_after_load (5 original + 3 added while down)");

                runtime_ready_check(&rt).await;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_shard_not_found_expired_checkpoint_ready_before_load() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "shard_not_found_ready_before";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT5).await?;
            let client = get_client(PORT5, access_key, secret_key);

            create_table(&client, table_name).await;
            insert_rows(&client, table_name, 0..5).await;
            wait_for_dynamodb_source_rows(&client, table_name, 5, 30).await?;

            // Create temp DuckDB file for acceleration
            let temp_dir = tempfile::tempdir()?;
            let duckdb_path = temp_dir.path().join("test.duckdb");
            let duckdb_path_str = duckdb_path.to_str().expect("path should be valid UTF-8");

            // === PHASE 1: Start Spice normally to create real checkpoint ===
            {
                let app = AppBuilder::new("dynamodb_ready_before_load_phase1")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT5,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_before_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 1: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // Wait for data to be loaded and checkpoint created
                runtime_ready_check(&rt).await;
                ensure_exact_dataset_rows(&rt, table_name, 5, 30).await?;

                // Verify initial data is loaded
                let initial_count = get_acceleration_row_count(duckdb_path_str, table_name);
                assert_eq!(initial_count, 5, "Phase 1: Should have 5 rows after initial load");

                // Runtime drops here, stopping Spice
            }

            // === PHASE 2: Corrupt checkpoint, delete some rows, and add new records while Spice is down ===
            // Make checkpoint reference non-existent shards and set timestamp to 20h ago
            corrupt_checkpoint_for_shard_not_found(duckdb_path_str, table_name, 20);

            // Delete some rows from acceleration to verify rebootstrap restores them
            delete_rows_from_acceleration(duckdb_path_str, table_name, &["id-0", "id-1", "id-2"]);
            let count_after_delete = get_acceleration_row_count(duckdb_path_str, table_name);
            assert_eq!(count_after_delete, 2, "Phase 2: Should have 2 rows after deletion");

            // Add new records to DynamoDB while Spice is down
            insert_rows(&client, table_name, 5..8).await;
            wait_for_dynamodb_source_rows(&client, table_name, 8, 30).await?;

            // === PHASE 3: Start Spice again - should detect ShardNotFound and rebootstrap ===
            {
                let app = AppBuilder::new("dynamodb_ready_before_load_phase3")
                    .with_dataset(make_dynamodb_dataset_with_file_accel(
                        table_name,
                        PORT5,
                        access_key,
                        secret_key,
                        duckdb_path_str,
                        Some("ready_before_load"),
                    ))
                    .with_sql_cache(SQLResultsCacheConfig {
                        enabled: false,
                        ..Default::default()
                    })
                    .build();

                configure_test_datafusion();
                let rt = Runtime::builder().with_app(app).build().await;
                let cloned_rt = Arc::new(rt.clone());

                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(60)) => {
                        return Err(anyhow::Error::msg("Phase 3: Timed out waiting for datasets to load"));
                    }
                    () = cloned_rt.load_components() => {}
                }

                // With ready_before_load, should rebootstrap and restore all 8 rows (5 original + 3 new)
                let has_rows = wait_for_dataset_rows(&rt, table_name, 8, 30).await;
                assert!(has_rows, "Phase 3: Dataset should have 8 rows after rebootstrap with ready_before_load (5 original + 3 added while down)");

                runtime_ready_check(&rt).await;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                anyhow::Error::msg(e.to_string())
            })?;

            Ok(())
        })
        .await
}

pub fn make_dynamodb_dataset_with_cayenne_acceleration(
    table_name: &str,
    port: u16,
    access_key: &str,
    secret_key: &str,
) -> Dataset {
    let mut dataset = Dataset::new(format!("dynamodb:{table_name}"), table_name.to_string());
    let params = HashMap::from([
        (
            "dynamodb_aws_access_key_id".to_string(),
            access_key.to_string(),
        ),
        (
            "dynamodb_aws_secret_access_key".to_string(),
            secret_key.to_string(),
        ),
        ("dynamodb_aws_region".to_string(), "us-east-1".to_string()),
        ("dynamodb_aws_auth".to_string(), "key".to_string()),
        (
            "endpoint_url".to_string(),
            format!("http://localhost:{port}"),
        ),
    ]);
    let temp_dir = tempfile::tempdir()
        .expect("failed to create temp directory")
        .keep();
    let cayenne_path = temp_dir.join("cayenne_data");
    let metadata_dir = temp_dir.join("cayenne_metadata");
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Changes),
        engine: Some("cayenne".to_string()),
        params: Some(DatasetParams::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                cayenne_path
                    .to_str()
                    .expect("cayenne_path should be valid UTF-8")
                    .to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                metadata_dir
                    .to_str()
                    .expect("metadata_dir should be valid UTF-8")
                    .to_string(),
            ),
        ]))),
        ..Acceleration::default()
    });
    dataset
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_cayenne_file_acceleration() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,dynamodb_streams=debug,info",
    ));

    let table_name = "cayenne_created_at_test";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT6).await?;
            let client = get_client(PORT6, access_key, secret_key);

            // Create table with just `id` as hash key (created_at is a non-key attribute)
            create_table(&client, table_name).await;

            // Insert a single record with created_at timestamp
            client
                .put_item()
                .table_name(table_name)
                .item("id", AttributeValue::S("1".to_string()))
                .item(
                    "created_at",
                    AttributeValue::S("2025-10-12T23:37:16.345Z".to_string()),
                )
                .send()
                .await
                .expect("Failed to insert item");

            wait_for_dynamodb_source_rows(&client, table_name, 1, 30).await?;

            let app = AppBuilder::new("dynamodb_duckdb_file_accel_test")
                .with_dataset(make_dynamodb_dataset_with_cayenne_acceleration(
                    table_name, PORT6, access_key, secret_key,
                ))
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;
            ensure_exact_dataset_rows(&rt, table_name, 1, 30).await?;

            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name}"),
                "dynamodb_streams_cayenne_file_acceleration",
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

/// Verifies that a DynamoDB-accelerated dataset with a declared schema initializes
/// successfully when the source table is empty, then becomes queryable once rows
/// are inserted.
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_declared_schema_empty_table() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,data_components=debug,info",
    ));

    let table_name = "declared_schema_test";
    let access_key = "foo";
    let secret_key = "bar";

    test_request_context()
        .scope(async {
            let running_container = start_dynamodb_docker_container(PORT8).await?;
            let client = get_client(PORT8, access_key, secret_key);

            // Create table with streams but do NOT insert any rows yet.
            create_table(&client, table_name).await;

            // Build dataset with explicit column type declarations so the runtime
            // can initialize schema without scanning any rows.
            let mut ds = make_dynamodb_dataset(table_name, PORT8, access_key, secret_key, true);
            ds.columns = vec![
                Column::new("id").with_type("text"),
                Column::new("name").with_type("text"),
                Column::new("version").with_type("bigint"),
            ];

            let app = AppBuilder::new("dynamodb_declared_schema_test")
                .with_dataset(ds)
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..Default::default()
                })
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Expect successful load even though the table is empty.
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Schema should reflect declared columns.
            run_and_snapshot_query(
                &rt,
                &format!("DESCRIBE {table_name}"),
                "declared_schema_empty_table_schema",
            )
            .await?;

            // Insert rows into the source table.
            insert_rows(&client, table_name, 0..3).await;
            wait_for_dynamodb_source_rows(&client, table_name, 3, 30).await?;

            // Rows should become visible via the accelerated dataset.
            ensure_dataset_rows(&rt, table_name, 3, 30).await?;
            run_and_snapshot_query(
                &rt,
                &format!("SELECT * FROM {table_name} ORDER BY id"),
                "declared_schema_empty_table_data",
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
