use crate::{get_test_datafusion, init_tracing, postgres::common, wait_until_true};
use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use rand::Rng;
use runtime::{datafusion::query::Protocol, Runtime};
use spicepod::component::{
    dataset::{
        acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
        Dataset,
    },
    params::Params,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

#[allow(clippy::too_many_lines)]
#[cfg(all(feature = "postgres", feature = "duckdb", feature = "sqlite"))]
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

    let pg_on_conflict_upsert = create_postgres_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "pg_on_conflict_upsert",
        port,
    );
    let pg_on_conflict_drop = create_postgres_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "pg_on_conflict_drop",
        port,
    );

    let duckdb_mem_on_conflict_upsert = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "duckdb_mem_on_conflict_upsert",
        None,
        port,
        Mode::Memory,
        "duckdb",
    );

    let duckdb_mem_on_conflict_drop = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "duckdb_mem_on_conflict_drop",
        None,
        port,
        Mode::Memory,
        "duckdb",
    );

    let duckdb_upsert_file_path = random_db_name();
    let duckdb_file_on_conflict_upsert = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "duckdb_file_on_conflict_upsert",
        Some(duckdb_upsert_file_path.clone()),
        port,
        Mode::File,
        "duckdb",
    );

    let duckdb_drop_file_path = random_db_name();
    let duckdb_file_on_conflict_drop = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "duckdb_file_on_conflict_drop",
        Some(duckdb_drop_file_path.clone()),
        port,
        Mode::File,
        "duckdb",
    );

    let sqlite_mem_on_conflict_upsert = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "sql_mem_on_conflict_upsert",
        None,
        port,
        Mode::Memory,
        "sqlite",
    );

    let sqlite_mem_on_conflict_drop = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "sql_mem_on_conflict_drop",
        None,
        port,
        Mode::Memory,
        "sqlite",
    );

    let sqlite_upsert_file_path = random_db_name();
    let sqlite_file_on_conflict_upsert = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Upsert,
        "postgres:event_logs",
        "sql_file_on_conflict_upsert",
        Some(sqlite_upsert_file_path.clone()),
        port,
        Mode::File,
        "sqlite",
    );

    let sqlite_drop_file_path = random_db_name();
    let sqlite_file_on_conflict_drop = create_sqlite_or_duckdb_test_dataset(
        OnConflictBehavior::Drop,
        "postgres:event_logs",
        "sql_file_on_conflict_drop",
        Some(sqlite_drop_file_path.clone()),
        port,
        Mode::File,
        "sqlite",
    );

    let df = get_test_datafusion();

    let app = AppBuilder::new("on_conflict_behavior")
        .with_dataset(pg_on_conflict_upsert)
        .with_dataset(pg_on_conflict_drop)
        .with_dataset(duckdb_mem_on_conflict_upsert)
        .with_dataset(duckdb_mem_on_conflict_drop)
        .with_dataset(duckdb_file_on_conflict_upsert)
        .with_dataset(duckdb_file_on_conflict_drop)
        .with_dataset(sqlite_mem_on_conflict_upsert)
        .with_dataset(sqlite_mem_on_conflict_drop)
        .with_dataset(sqlite_file_on_conflict_upsert)
        .with_dataset(sqlite_file_on_conflict_drop)
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

    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM pg_on_conflict_upsert LIMIT 1",
    )
    .await;
    dataset_ready_check(Arc::clone(&rt), "SELECT * FROM pg_on_conflict_drop LIMIT 1").await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM duckdb_mem_on_conflict_drop LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM duckdb_mem_on_conflict_upsert LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM duckdb_file_on_conflict_upsert LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM duckdb_file_on_conflict_drop LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM sql_mem_on_conflict_drop LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM sql_mem_on_conflict_upsert LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM sql_file_on_conflict_upsert LIMIT 1",
    )
    .await;
    dataset_ready_check(
        Arc::clone(&rt),
        "SELECT * FROM sql_file_on_conflict_drop LIMIT 1",
    )
    .await;

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

    let pg_upsert_data = get_query_result(&rt, "SELECT * FROM pg_on_conflict_upsert").await?;
    let pg_drop_data = get_query_result(&rt, "SELECT * FROM pg_on_conflict_drop").await?;
    let duckdb_mem_upsert_data =
        get_query_result(&rt, "SELECT * FROM duckdb_mem_on_conflict_upsert").await?;
    let duckdb_mem_drop_data =
        get_query_result(&rt, "SELECT * FROM duckdb_mem_on_conflict_drop").await?;
    let duckdb_file_upsert_data =
        get_query_result(&rt, "SELECT * FROM duckdb_file_on_conflict_upsert").await?;
    let duckdb_file_drop_data =
        get_query_result(&rt, "SELECT * FROM duckdb_file_on_conflict_drop").await?;
    let sqlite_mem_upsert_data =
        get_query_result(&rt, "SELECT * FROM sql_mem_on_conflict_upsert").await?;
    let sqlite_mem_drop_data =
        get_query_result(&rt, "SELECT * FROM sql_mem_on_conflict_drop").await?;
    let sqlite_file_upsert_data =
        get_query_result(&rt, "SELECT * FROM sql_file_on_conflict_upsert").await?;
    let sqlite_file_drop_data =
        get_query_result(&rt, "SELECT * FROM sql_file_on_conflict_drop").await?;

    let upsert_expected_result = &[
        "+----------+-------------------+---------------------+",
        "| event_id | event_name        | event_timestamp     |",
        "+----------+-------------------+---------------------+",
        "| 1        | User Registration | 2023-05-16T10:00:00 |",
        "| 2        | Password Change   | 2023-05-16T14:30:00 |",
        "| 3        | User Login        | 2023-05-17T08:45:00 |",
        "| 4        | User Logout       | 2023-05-17T18:00:00 |",
        "| 5        | File Accessed     | 2024-08-24T15:45:00 |",
        "+----------+-------------------+---------------------+",
    ];

    let drop_expected_result = &[
        "+----------+-------------------+---------------------+",
        "| event_id | event_name        | event_timestamp     |",
        "+----------+-------------------+---------------------+",
        "| 1        | User Registration | 2023-05-16T10:00:00 |",
        "| 2        | Password Change   | 2023-05-16T14:30:00 |",
        "| 3        | User Login        | 2023-05-17T08:45:00 |",
        "| 4        | User Logout       | 2023-05-17T18:00:00 |",
        "| 5        | File Download     | 2023-05-17T13:20:00 |",
        "+----------+-------------------+---------------------+",
    ];

    assert_batches_eq!(upsert_expected_result, &pg_upsert_data);
    assert_batches_eq!(drop_expected_result, &pg_drop_data);
    assert_batches_eq!(upsert_expected_result, &duckdb_mem_upsert_data);
    assert_batches_eq!(drop_expected_result, &duckdb_mem_drop_data);
    assert_batches_eq!(upsert_expected_result, &duckdb_file_upsert_data);
    assert_batches_eq!(drop_expected_result, &duckdb_file_drop_data);
    assert_batches_eq!(upsert_expected_result, &sqlite_mem_upsert_data);
    assert_batches_eq!(drop_expected_result, &sqlite_mem_drop_data);
    assert_batches_eq!(upsert_expected_result, &sqlite_file_upsert_data);
    assert_batches_eq!(drop_expected_result, &sqlite_file_drop_data);

    running_container.remove().await?;
    std::fs::remove_file(&duckdb_upsert_file_path).expect("File should be removed");
    std::fs::remove_file(&duckdb_drop_file_path).expect("File should be removed");
    std::fs::remove_file(&sqlite_upsert_file_path).expect("File should be removed");
    std::fs::remove_file(&sqlite_drop_file_path).expect("File should be removed");
    std::fs::remove_file(format!("{sqlite_upsert_file_path}-shm")).expect("File should be removed");
    std::fs::remove_file(format!("{sqlite_upsert_file_path}-wal")).expect("File should be removed");
    std::fs::remove_file(format!("{sqlite_drop_file_path}-shm")).expect("File should be removed");
    std::fs::remove_file(format!("{sqlite_drop_file_path}-wal")).expect("File should be removed");

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
                .unwrap_or_else(|_| panic!("Result should be returned"));
            let mut batches = vec![];
            while let Some(batch) = query_result.data.next().await {
                batches.push(batch.unwrap_or_else(|_| panic!("Batch should be created")));
            }
            !batches.is_empty() && batches[0].num_rows() == 1
        })
        .await,
        "Expected 1 rows returned"
    );
}

async fn get_query_result(
    rt: &Arc<Runtime>,
    sql: &str,
) -> Result<Vec<arrow::array::RecordBatch>, datafusion::error::DataFusionError> {
    rt.datafusion().ctx.sql(sql).await?.collect().await
}

fn create_postgres_test_dataset(
    on_conflict: OnConflictBehavior,
    from: &str,
    name: &str,
    port: usize,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(get_pg_params(port));

    let mut on_conflict_hashmap = HashMap::new();
    on_conflict_hashmap
        .insert("event_id".to_string(), on_conflict)
        .unwrap_or_default();
    dataset.acceleration = Some(Acceleration {
        params: Some(get_pg_params(port)),
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

fn create_sqlite_or_duckdb_test_dataset(
    on_conflict: OnConflictBehavior,
    from: &str,
    name: &str,
    file: Option<String>,
    port: usize,
    mode: Mode,
    engine: &str,
) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(get_pg_params(port));

    let mut on_conflict_hashmap = HashMap::new();
    on_conflict_hashmap
        .insert("event_id".to_string(), on_conflict)
        .unwrap_or_default();

    dataset.acceleration = Some(Acceleration {
        params: get_params(&mode, file, engine),
        mode,
        enabled: true,
        engine: Some(engine.to_string()),
        refresh_mode: Some(RefreshMode::Append),
        refresh_check_interval: Some("1s".to_string()),
        primary_key: Some("event_id".to_string()),
        on_conflict: on_conflict_hashmap,
        ..Acceleration::default()
    });

    dataset.time_column = Some("event_timestamp".to_string());

    dataset
}

fn get_pg_params(port: usize) -> Params {
    Params::from_string_map(
        vec![
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}

fn get_params(mode: &Mode, file: Option<String>, engine: &str) -> Option<Params> {
    let param_name = format!("{engine}_file",);
    if mode == &Mode::File {
        return Some(Params::from_string_map(
            vec![(param_name, file.unwrap_or_default())]
                .into_iter()
                .collect(),
        ));
    }
    None
}

fn random_db_name() -> String {
    let mut rng = rand::thread_rng();
    let mut name = String::new();

    for _ in 0..10 {
        name.push(rng.gen_range(b'a'..=b'z') as char);
    }

    format!("./{name}.db")
}
