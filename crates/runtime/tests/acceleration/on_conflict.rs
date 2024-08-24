use std::time::Duration;

use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use runtime::{datafusion::query::Protocol, Runtime};
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};

use crate::{init_tracing, wait_until_true};

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_acceleration_on_conflict() -> Result<(), anyhow::Error> {
    use arrow::array::RecordBatch;
    use spicepod::component::dataset::acceleration::{OnConflictBehavior, RefreshMode};
    use std::collections::HashMap;

    use crate::get_test_datafusion;
    use crate::postgres::common;

    let _tracing = init_tracing(Some("integration=debug,info"));
    let port: usize = 20963;
    let running_container = common::start_postgres_docker_container(port).await?;

    let pool = common::get_postgres_connection_pool(port).await?;
    let db_conn = pool
        .connect_direct()
        .await
        .expect("connection can be established");

    db_conn
        .conn
        .execute(
            "
CREATE TABLE event_logs (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(100),
    event_timestamp TIMESTAMP
);",
            &[],
        )
        .await
        .expect("table is created");

    db_conn
        .conn
        .execute(
            "
INSERT INTO event_logs (event_name, event_timestamp) VALUES
('User Registration', '2023-05-16 10:00:00'),
('Password Change', '2023-05-16 14:30:00'),
('User Login', '2023-05-17 08:45:00'),
('User Logout', '2023-05-17 18:00:00'),
('File Download', '2023-05-17 13:20:00');",
            &[],
        )
        .await
        .expect("inserted data");

    let params = Params::from_string_map(
        vec![
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    // Create dataset with on_conflict:upsert acceleration setting
    let mut on_conflict_upsert = Dataset::new("postgres:event_logs", "event_logs_upsert");

    let mut upsert_hashmap = HashMap::new();
    upsert_hashmap
        .insert("event_id".to_string(), OnConflictBehavior::Upsert)
        .unwrap_or_default();

    on_conflict_upsert.params = Some(params.clone());
    on_conflict_upsert.acceleration = Some(Acceleration {
        params: Some(params.clone()),
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_check_interval: Some("5s".to_string()),
        primary_key: Some("event_id".to_string()),
        on_conflict: upsert_hashmap,
        ..Acceleration::default()
    });
    on_conflict_upsert.time_column = Some("event_timestamp".to_string());

    // Create dataset with on_conflict:drop acceleration setting
    let mut on_conflict_drop = Dataset::new("postgres:event_logs", "event_logs_drop");

    let mut drop_hashmap = HashMap::new();
    drop_hashmap
        .insert("event_id".to_string(), OnConflictBehavior::Drop)
        .unwrap_or_default();

    on_conflict_drop.params = Some(params.clone());
    on_conflict_drop.acceleration = Some(Acceleration {
        params: Some(params.clone()),
        enabled: true,
        engine: Some("postgres".to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_check_interval: Some("5s".to_string()),
        primary_key: Some("event_id".to_string()),
        on_conflict: drop_hashmap,
        ..Acceleration::default()
    });
    on_conflict_drop.time_column = Some("event_timestamp".to_string());

    let df = get_test_datafusion();

    let app = AppBuilder::new("on_conflict_behavior")
        .with_dataset(on_conflict_upsert)
        // .with_dataset(on_conflict_drop)
        .build();

    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .build()
        .await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    let mut original_batches: Vec<RecordBatch> = vec![];
    let mut original_data = rt
        .datafusion()
        .query_builder("SELECT * FROM event_logs_upsert", Protocol::Internal)
        .build()
        .run()
        .await
        .expect("result returned");
    while let Some(batch) = original_data.data.next().await {
        original_batches.push(batch.expect("batch"));
    }

    db_conn
        .conn
        .execute(
            "UPDATE event_logs SET event_name = 'File Upload' WHERE event_id = 5;",
            &[],
        )
        .await
        .expect("inserted data");

    // Wait 10 seconds for result to be refreshed
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;

    let mut upsert_result = rt
        .datafusion()
        .query_builder("SELECT * FROM event_logs_upsert", Protocol::Internal)
        .build()
        .run()
        .await
        .expect("result returned");
    let mut upsert_batches = vec![];
    while let Some(batch) = upsert_result.data.next().await {
        upsert_batches.push(batch.expect("batch"));
    }

    let mut drop_result = rt
        .datafusion()
        .query_builder("SELECT * FROM event_logs_drop", Protocol::Internal)
        .build()
        .run()
        .await
        .expect("result returned");
    let mut drop_batches = vec![];
    while let Some(batch) = drop_result.data.next().await {
        drop_batches.push(batch.expect("batch"));
    }

    assert_ne!(original_batches, drop_batches);
    assert_eq!(original_batches, upsert_batches);

    running_container.remove().await?;
    Ok(())
}
