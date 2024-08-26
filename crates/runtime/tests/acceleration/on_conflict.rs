use crate::{get_test_datafusion, init_tracing, postgres::common, wait_until_true};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use runtime::{datafusion::query::Protocol, Runtime};
use spicepod::component::{
    dataset::{
        acceleration::{Acceleration, OnConflictBehavior, RefreshMode},
        Dataset,
    },
    params::Params,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[allow(clippy::too_many_lines)]
#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_acceleration_on_conflict() -> Result<(), anyhow::Error> {
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

    let on_conflict_upsert = create_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "event_logs_upsert",
        port,
    );
    let on_conflict_drop = create_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "event_logs_drop",
        port,
    );

    let df = get_test_datafusion();

    let app = AppBuilder::new("on_conflict_behavior")
        .with_dataset(on_conflict_upsert)
        .with_dataset(on_conflict_drop)
        .build();

    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_datafusion(df)
            .build()
            .await,
    );

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    assert!(
        wait_until_true(Duration::from_secs(30), || async {
            let mut query_result = rt
                .datafusion()
                .query_builder(
                    "SELECT * FROM event_logs_upsert LIMIT 1",
                    Protocol::Internal,
                )
                .build()
                .run()
                .await
                .expect("result returned");
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch.expect("batch"));
            }
            !batches.is_empty() && batches[0].num_rows() == 1
        })
        .await,
        "Expected 1 rows returned"
    );

    assert!(
        wait_until_true(Duration::from_secs(30), || async {
            let mut query_result = rt
                .datafusion()
                .query_builder("SELECT * FROM event_logs_drop LIMIT 1", Protocol::Internal)
                .build()
                .run()
                .await
                .expect("result returned");
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch.expect("batch"));
            }
            !batches.is_empty() && batches[0].num_rows() == 1
        })
        .await,
        "Expected 1 rows returned"
    );

    // dataset_ready_check(Arc::clone(&rt), "SELECT * FROM event_logs_upsert LIMIT 1").await;
    // dataset_ready_check(Arc::clone(&rt), "SELECT * FROM event_logs_drop LIMIT 1").await;

    db_conn
        .conn
        .execute(
            "
UPDATE event_logs
SET event_name = 'File Accessed',
    event_timestamp = '2024-08-24 15:45:00'
WHERE event_name = 'File Download'
  AND event_timestamp = '2023-05-17 13:20:00';
",
            &[],
        )
        .await
        .expect("inserted data");

    // Wait for result to be refreshed
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    let upsert_data = rt
        .datafusion()
        .ctx
        .sql("SELECT * FROM event_logs_upsert")
        .await?
        .collect()
        .await?;

    let drop_data = rt
        .datafusion()
        .ctx
        .sql("SELECT * FROM event_logs_drop")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----------+-------------------+---------------------+",
            "| event_id | event_name        | event_timestamp     |",
            "+----------+-------------------+---------------------+",
            "| 1        | User Registration | 2023-05-16T10:00:00 |",
            "| 2        | Password Change   | 2023-05-16T14:30:00 |",
            "| 3        | User Login        | 2023-05-17T08:45:00 |",
            "| 4        | User Logout       | 2023-05-17T18:00:00 |",
            "| 5        | File Download     | 2023-05-17T13:20:00 |",
            "+----------+-------------------+---------------------+"
        ],
        &drop_data
    );

    assert_batches_eq!(
        &[
            "+----------+-------------------+---------------------+",
            "| event_id | event_name        | event_timestamp     |",
            "+----------+-------------------+---------------------+",
            "| 1        | User Registration | 2023-05-16T10:00:00 |",
            "| 2        | Password Change   | 2023-05-16T14:30:00 |",
            "| 3        | User Login        | 2023-05-17T08:45:00 |",
            "| 4        | User Logout       | 2023-05-17T18:00:00 |",
            "| 5        | File Accessed     | 2024-08-24T15:45:00 |",
            "+----------+-------------------+---------------------+"
        ],
        &upsert_data
    );

    running_container.remove().await?;
    Ok(())
}

async fn dataset_ready_check(rt: Arc<Runtime>, sql: &str) {
    assert!(
        wait_until_true(Duration::from_secs(30), || async {
            let mut query_result = rt
                .datafusion()
                .query_builder(sql, Protocol::Internal)
                .build()
                .run()
                .await
                .expect("result returned");
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch.expect("batch"));
            }
            !batches.is_empty() && batches[0].num_rows() == 1
        })
        .await,
        "Expected 1 rows returned"
    );
}

fn create_test_dataset(
    on_conflict: OnConflictBehavior,
    from: &str,
    name: &str,
    port: usize,
) -> Dataset {
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

    let mut dataset = Dataset::new(from, name);

    let mut on_conflict_hashmap = HashMap::new();
    on_conflict_hashmap
        .insert("event_id".to_string(), on_conflict)
        .unwrap_or_default();

    dataset.params = Some(params.clone());
    dataset.acceleration = Some(Acceleration {
        params: Some(params.clone()),
        enabled: true,
        engine: Some("postgres".to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_check_interval: Some("1s".to_string()),
        primary_key: Some("event_id".to_string()),
        on_conflict: on_conflict_hashmap,
        ..Acceleration::default()
    });
    dataset.time_column = Some("event_timestamp".to_string());

    dataset
}
