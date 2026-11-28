#![allow(dead_code, clippy::allow_attributes)]

use crate::postgres::common;
use crate::postgres::common::get_pg_params;
use crate::utils::{register_test_connectors, runtime_ready_check};
use crate::{configure_test_datafusion, configure_test_datafusion_request_context};
use app::AppBuilder;
use arrow::array::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use datafusion::common::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use futures::StreamExt;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, IndexType, OnConflictBehavior, RefreshMode};
use spicepod::component::caching::SQLResultsCacheConfig;
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

/// Source table for the append `time_format` histories: `created_at` has `created_at_type`,
/// and the other columns are an integer and a nullable string. Inserts no rows.
/// `acceleration` is the database the `PostgreSQL` accelerator connects to.
pub(crate) async fn initialize_postgres_time_format(
    port: usize,
    created_at_type: &str,
) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(&db_conn, "SET TIME ZONE 'UTC'").await?;
    execute_ps_sql(
        &db_conn,
        &format!(
            "CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                created_at {created_at_type} NOT NULL,
                n INTEGER NOT NULL,
                label TEXT
            )"
        ),
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
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

/// UTC midnights used as `created_at` by [`initialize_postgres_labeled`].
///
/// They are inside the append `refresh_sql` window (`now() - INTERVAL '10 years'`) and fall
/// on an exact millisecond, so an epoch-millisecond comparison does not round.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LabeledInstants {
    pub alpha: DateTime<Utc>,
    pub beta: DateTime<Utc>,
    pub probe: DateTime<Utc>,
}

/// Source table whose non-time columns are an integer and a string.
///
/// Inserts the `alpha` row (`n = 10`, `label = 'alpha'`). Callers insert `beta` and `probe`
/// later. `acceleration` is the database the `PostgreSQL` accelerator connects to.
pub(crate) async fn initialize_postgres_labeled(
    port: usize,
) -> Result<(PostgresConnection, LabeledInstants), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {e}"))?;

    execute_ps_sql(&db_conn, "SET TIME ZONE 'UTC'").await?;
    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP(3) WITH TIME ZONE NOT NULL,
                    n INTEGER NOT NULL,
                    label TEXT NOT NULL
                )",
    )
    .await?;

    // `AT TIME ZONE 'UTC'` yields `timestamp without time zone`. Truncating that to a day
    // is a UTC midnight, independent of the session time zone.
    let picked = db_conn
        .conn
        .query_one(
            "SELECT
                (date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) - INTERVAL '2 days'),
                (date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) - INTERVAL '1 day'),
                date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'))",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Error picking refresh instants: {e}"))?;
    let instants = LabeledInstants {
        alpha: utc_midnight(picked.try_get(0)?, "alpha")?,
        beta: utc_midnight(picked.try_get(1)?, "beta")?,
        probe: utc_midnight(picked.try_get(2)?, "probe")?,
    };

    let n = 10_i32;
    let label = "alpha";
    let inserted = db_conn
        .conn
        .execute(
            "INSERT INTO test_table (created_at, n, label) VALUES ($1, $2, $3)",
            &[&instants.alpha, &n, &label],
        )
        .await
        .map_err(|e| anyhow::anyhow!("Error inserting the alpha row: {e}"))?;
    anyhow::ensure!(
        inserted == 1,
        "expected to insert 1 alpha row, inserted {inserted}"
    );

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok((db_conn, instants))
}

fn utc_midnight(naive: NaiveDateTime, name: &str) -> Result<DateTime<Utc>, anyhow::Error> {
    let time = naive.time();
    anyhow::ensure!(
        time.num_seconds_from_midnight() == 0 && time.nanosecond() == 0,
        "{name} instant {naive} is not a UTC midnight"
    );
    Ok(naive.and_utc())
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

pub(crate) async fn start_test_runtime_with_dataset(
    _port: usize,
    acceleration: Acceleration,
    mut dataset: Dataset,
) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;

    dataset.acceleration = Some(acceleration);
    // Refresh leaves cached SQL results in place; reads after one must skip the cache.
    let app = AppBuilder::new("test_acceleration_refresh")
        .with_dataset(dataset)
        .with_sql_cache(SQLResultsCacheConfig {
            enabled: false,
            ..Default::default()
        })
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
        .wait()
        .await;
    Ok(())
}
