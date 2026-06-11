#![allow(dead_code, clippy::allow_attributes)]

use crate::postgres::common;
use crate::postgres::common::get_pg_params;
use crate::utils::{register_test_connectors, runtime_ready_check};
use crate::{configure_test_datafusion, configure_test_datafusion_request_context};
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::common::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use futures::StreamExt;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, IndexType, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::{Dataset, TimeFormat};
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn get_acceleration_config_append(
    engine: &str,
    acceleration_params: Option<Params>,
) -> Acceleration {
    let primary_key = Some("id".to_string());
    let on_conflict = [("id".to_string(), OnConflictBehavior::Upsert)]
        .iter()
        .cloned()
        .collect::<HashMap<String, OnConflictBehavior>>();
    let indexes = [("id".to_string(), IndexType::Unique)]
        .iter()
        .cloned()
        .collect::<HashMap<String, IndexType>>();

    Acceleration {
        enabled: true,
        params: acceleration_params,
        engine: Some(engine.to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_sql: Some(
            "select * from test_table where created_at > now() - INTERVAL '10 years'".to_string(),
        ),
        refresh_check_interval: Some("5h".to_string()),
        primary_key,
        on_conflict,
        indexes,
        ..Acceleration::default()
    }
}

pub(crate) fn get_acceleration_config_full(
    engine: &str,
    acceleration_params: Option<Params>,
) -> Acceleration {
    Acceleration {
        enabled: true,
        params: acceleration_params,
        engine: Some(engine.to_string()),
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    }
}

pub(crate) fn get_dataset(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:test_table", "test_table");
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds.time_column = Some("created_at".to_string());
    // Use Timestamp instead of Timestamptz because Arrow reads Postgres TIMESTAMPTZ as Timestamp(Nanosecond, None)
    ds.time_format = Some(TimeFormat::Timestamp);
    ds
}

pub(crate) fn get_dataset_no_time_column(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:test_table", "test_table");
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    // No time_column set - for testing append without constraints
    ds
}

/// Initialize postgres with a test table that uses BIGINT for timestamps (Unix seconds).
pub(crate) async fn initialize_postgres_unix_time(
    port: usize,
) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;

    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at BIGINT
                )",
    )
    .await?;

    execute_ps_sql(
        &db_conn,
        "INSERT INTO test_table (created_at) VALUES (extract(epoch from now())::bigint)",
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
}

/// Get dataset with Unix timestamp column (INT) to work around Vortex v0.52.1 timestamp metadata bug
pub(crate) fn get_dataset_unix_time(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:test_table", "test_table");
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds.time_column = Some("created_at".to_string());
    ds.time_format = Some(TimeFormat::UnixSeconds);
    ds
}

pub(crate) async fn execute_ps_sql(
    db_conn: &PostgresConnection,
    sql: &str,
) -> Result<u64, anyhow::Error> {
    db_conn
        .conn
        .execute(sql, &[])
        .await
        .map_err(|e| anyhow::anyhow!("Error running sql: {e}"))
}

pub(crate) async fn initialize_postgres(port: usize) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;

    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP(3) WITH TIME ZONE
                )",
    )
    .await?;

    execute_ps_sql(
        &db_conn,
        "INSERT INTO test_table (created_at) VALUES (date_trunc('milliseconds', now()))",
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
}

/// Initialize postgres with a test table that includes a `value` column for testing upsert behavior.
/// The table has: id (PK), `created_at` (timestamp), value (text)
pub(crate) async fn initialize_postgres_with_value_column(
    port: usize,
) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;

    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP(3) WITH TIME ZONE,
                    value TEXT
                )",
    )
    .await?;

    execute_ps_sql(
        &db_conn,
        "INSERT INTO test_table (created_at, value) VALUES (date_trunc('milliseconds', now()), 'initial_value')",
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
}

/// Initialize postgres with a test table that uses TEXT for timestamps (ISO8601 format).
/// This exercises the ISO8601 string comparison path instead of native timestamp types.
pub(crate) async fn initialize_postgres_iso8601(
    port: usize,
) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;

    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at TEXT
                )",
    )
    .await?;

    execute_ps_sql(
        &db_conn,
        "INSERT INTO test_table (created_at) VALUES (to_char(now(), 'YYYY-MM-DD\"T\"HH24:MI:SS.US'))",
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
}

/// Get dataset configured for ISO8601 string timestamps.
pub(crate) fn get_dataset_iso8601(port: usize) -> Dataset {
    let mut ds = Dataset::new("postgres:test_table", "test_table");
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    ds.time_column = Some("created_at".to_string());
    ds.time_format = Some(TimeFormat::ISO8601);
    ds
}

pub(crate) async fn start_test_runtime(
    port: usize,
    acceleration: Acceleration,
) -> Result<Arc<Runtime>, anyhow::Error> {
    start_test_runtime_with_dataset(port, acceleration, get_dataset(port)).await
}

pub(crate) async fn start_test_runtime_no_time_column(
    port: usize,
    acceleration: Acceleration,
) -> Result<Arc<Runtime>, anyhow::Error> {
    start_test_runtime_with_dataset(port, acceleration, get_dataset_no_time_column(port)).await
}

pub(crate) async fn start_test_runtime_unix_time(
    port: usize,
    acceleration: Acceleration,
) -> Result<Arc<Runtime>, anyhow::Error> {
    start_test_runtime_with_dataset(port, acceleration, get_dataset_unix_time(port)).await
}

pub(crate) async fn start_test_runtime_iso8601(
    port: usize,
    acceleration: Acceleration,
) -> Result<Arc<Runtime>, anyhow::Error> {
    start_test_runtime_with_dataset(port, acceleration, get_dataset_iso8601(port)).await
}

async fn start_test_runtime_with_dataset(
    _port: usize,
    acceleration: Acceleration,
    mut dataset: Dataset,
) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;

    dataset.acceleration = Some(acceleration);
    let app = AppBuilder::new("test_acceleration_refresh")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    configure_test_datafusion_request_context();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let cloned_rt = Arc::clone(&rt);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
            panic!("Timeout waiting for components to load");
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(rt)
}

pub(crate) async fn execute_rt_sql(
    rt: Arc<Runtime>,
    sql: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(sql).build().run().await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    Ok(results)
}

pub(crate) async fn refresh_table(rt: Arc<Runtime>, table_name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table_name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("Failed to refresh table"))?
        .notified()
        .await;
    Ok(())
}

// ============================================================================
// Shared test bodies for append mode with different time_format values.
// Used by engine-specific test modules to avoid duplication.
// ============================================================================

/// Test append mode with the default `Timestamp` time format (TIMESTAMPTZ source column).
pub(crate) async fn test_append_timestamp_for_engine(
    engine: &str,
    mode: Option<Mode>,
    accel_params: Option<Params>,
) -> Result<(), anyhow::Error> {
    crate::utils::test_request_context()
        .scope(async {
            let port: usize = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let db_conn = initialize_postgres(port).await?;

            let mut config = get_acceleration_config_append(engine, accel_params);
            if let Some(m) = mode {
                config.mode = m;
            }
            let rt = start_test_runtime(port, config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 1, "{engine}/Timestamp: expected 1 row initially");

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (date_trunc('milliseconds', now()));",
            )
            .await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 2, "{engine}/Timestamp: expected 2 rows after append");

            // Refresh again with no new source data — row count must stay at 2
            refresh_table(Arc::clone(&rt), "test_table").await?;
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                count, 2,
                "{engine}/Timestamp: expected 2 rows after no-op refresh"
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Test append mode with `UnixSeconds` time format (BIGINT source column).
pub(crate) async fn test_append_unix_seconds_for_engine(
    engine: &str,
    mode: Option<Mode>,
    accel_params: Option<Params>,
) -> Result<(), anyhow::Error> {
    crate::utils::test_request_context()
        .scope(async {
            let port: usize = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let db_conn = initialize_postgres_unix_time(port).await?;

            let mut config = get_acceleration_config_append(engine, accel_params);
            if let Some(m) = mode {
                config.mode = m;
            }
            config.primary_key = None;
            config.on_conflict = HashMap::new();
            config.indexes = HashMap::new();
            config.refresh_sql = Some(
                "select * from test_table where created_at > extract(epoch from now() - INTERVAL '10 years')::bigint".to_string(),
            );
            let rt = start_test_runtime_unix_time(port, config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 1, "{engine}/UnixSeconds: expected 1 row initially");

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (extract(epoch from now() + interval '1 second')::bigint);",
            )
            .await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 2, "{engine}/UnixSeconds: expected 2 rows after append");

            // Refresh again with no new source data — row count must stay at 2
            refresh_table(Arc::clone(&rt), "test_table").await?;
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                count, 2,
                "{engine}/UnixSeconds: expected 2 rows after no-op refresh"
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Test append mode with `ISO8601` time format (TEXT source column).
pub(crate) async fn test_append_iso8601_for_engine(
    engine: &str,
    mode: Option<Mode>,
    accel_params: Option<Params>,
) -> Result<(), anyhow::Error> {
    crate::utils::test_request_context()
        .scope(async {
            let port: usize = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let db_conn = initialize_postgres_iso8601(port).await?;

            let mut config = get_acceleration_config_append(engine, accel_params);
            if let Some(m) = mode {
                config.mode = m;
            }
            config.primary_key = None;
            config.on_conflict = HashMap::new();
            config.indexes = HashMap::new();
            config.refresh_sql = Some(
                "select * from test_table where created_at > to_char(now() - INTERVAL '10 years', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')".to_string(),
            );
            let rt = start_test_runtime_iso8601(port, config).await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 1, "{engine}/ISO8601: expected 1 row initially");

            execute_ps_sql(
                &db_conn,
                "INSERT INTO test_table (created_at) VALUES (to_char(now() + interval '1 second', 'YYYY-MM-DD\"T\"HH24:MI:SS.US'));",
            )
            .await?;
            refresh_table(Arc::clone(&rt), "test_table").await?;

            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(count, 2, "{engine}/ISO8601: expected 2 rows after append");

            // Refresh again with no new source data — row count must stay at 2
            refresh_table(Arc::clone(&rt), "test_table").await?;
            let results = execute_rt_sql(Arc::clone(&rt), "SELECT * FROM test_table").await?;
            let count: usize = results.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                count, 2,
                "{engine}/ISO8601: expected 2 rows after no-op refresh"
            );

            running_container.remove().await?;
            Ok(())
        })
        .await
}
