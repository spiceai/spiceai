use crate::configure_test_datafusion;
use crate::postgres::common;
use crate::postgres::common::get_pg_params;
use crate::utils::runtime_ready_check;
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::common::TableReference;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use futures::StreamExt;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, IndexType, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::{Dataset, TimeFormat};
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn get_acceleration_config_append(
    engine: &str,
    acceleration_params: Option<Params>,
) -> Acceleration {
    Acceleration {
        enabled: true,
        params: acceleration_params,
        engine: Some(engine.to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_sql: Some(
            "select * from test_table where created_at > now() - INTERVAL '10 years'".to_string(),
        ),
        refresh_check_interval: Some("5h".to_string()),
        primary_key: Some("id".to_string()),
        on_conflict: [("id".to_string(), OnConflictBehavior::Upsert)]
            .iter()
            .cloned()
            .collect::<HashMap<String, OnConflictBehavior>>(),
        indexes: [("id".to_string(), IndexType::Unique)]
            .iter()
            .cloned()
            .collect::<HashMap<String, IndexType>>(),
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
    ds.time_format = Some(TimeFormat::Timestamptz);
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
        .map_err(|e| anyhow::anyhow!("Error running sql: {}", e))
}

pub(crate) async fn initialize_postgres(port: usize) -> Result<PostgresConnection, anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;

    let db_conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("Error connecting: {}", e))?;

    execute_ps_sql(
        &db_conn,
        "
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE
                )",
    )
    .await?;

    execute_ps_sql(
        &db_conn,
        "INSERT INTO test_table (created_at) VALUES (now())",
    )
    .await?;

    execute_ps_sql(&db_conn, "CREATE DATABASE acceleration").await?;

    Ok(db_conn)
}

pub(crate) async fn start_test_runtime(
    port: usize,
    acceleration: Acceleration,
) -> Result<Arc<Runtime>, anyhow::Error> {
    let mut dataset = get_dataset(port);

    dataset.acceleration = Some(acceleration);
    let app = AppBuilder::new("test_acceleration_refresh")
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let cloned_rt = Arc::clone(&rt);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
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
