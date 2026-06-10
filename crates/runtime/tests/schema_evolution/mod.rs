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

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;
#[cfg(feature = "postgres")]
use datafusion_table_providers::sql::db_connection_pool::dbconnection::postgresconn::PostgresConnection;
use runtime::Runtime;
use runtime::{
    component::dataset::Dataset as RuntimeDataset,
    dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
};
use spicepod::{
    acceleration::{Acceleration, Mode},
    component::dataset::Dataset,
    param::Params,
};

#[cfg(feature = "postgres")]
use crate::postgres::common;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{
        register_test_connectors, run_query, runtime_ready_check, test_request_context,
        to_pretty_display,
    },
};

#[cfg(feature = "postgres")]
const DUCKDB_FILE_PATH: &str = "./schema_evolution.duckdb";
#[cfg(feature = "postgres")]
const DUCKDB_FILE_UPDATE_PATH: &str = "./schema_evolution_file_update.duckdb";

#[cfg(feature = "postgres")]
use spicepod::component::dataset::OnSchemaChange;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_schema_evolution() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    if std::fs::metadata(DUCKDB_FILE_PATH).is_ok() {
        std::fs::remove_file(DUCKDB_FILE_PATH).expect("should remove local database");
    }

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            // Reset the table to the initial state
            reset_pg_table(&db_conn).await;

            let rt = Arc::new(initialize_runtime(port).await?);

            // This query should continue to work across all of the table mutations below.
            let sql = "SELECT id, town, age FROM cham ORDER BY id ASC";
            run_and_verify_query(&rt, sql, "test_schema_evolution_initial").await;

            // Test 1: Add a new column
            rt.shutdown().await;
            drop(rt);
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_add_column").await;

            // Test 2: Drop a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(&db_conn, "ALTER TABLE public.chameleon DROP COLUMN age;").await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_drop_column").await;

            // Test 3: Rename a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon RENAME COLUMN town TO city;",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_rename_column").await;

            // Test 4: Change the data type of a column
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE chameleon
                ALTER COLUMN age
                TYPE TEXT
                USING (age::TEXT);",
            )
            .await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_change_column_type").await;

            // Test 5: Drop the table
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(&db_conn, "DROP TABLE IF EXISTS public.chameleon;").await;
            let rt = Arc::new(initialize_runtime(port).await?);
            run_and_verify_query(&rt, sql, "test_schema_evolution_drop_table").await;

            running_container.remove().await?;

            if std::fs::metadata(DUCKDB_FILE_PATH).is_ok() {
                std::fs::remove_file(DUCKDB_FILE_PATH).expect("should remove local database");
            }

            Ok(())
        })
        .await
}

#[cfg(feature = "postgres")]
#[expect(clippy::expect_used)]
async fn run_and_verify_query(rt: &Arc<Runtime>, sql: &str, snapshot_name: &str) {
    let record_batch = run_query(rt, sql).await.expect("query should succeed");
    insta::assert_snapshot!(
        snapshot_name,
        to_pretty_display(&record_batch).expect("pretty display")
    );
}

#[cfg(feature = "postgres")]
async fn reset_pg_table(db_conn: &PostgresConnection) {
    execute_pg_statement(db_conn, "DROP TABLE IF EXISTS public.chameleon;").await;
    execute_pg_statement(
        db_conn,
        "CREATE TABLE public.chameleon (id varchar NOT NULL, town varchar NULL, age int4 NULL, CONSTRAINT chameleon_pk PRIMARY KEY (id));",
    )
    .await;
    execute_pg_statement(
        db_conn,
        "INSERT INTO public.chameleon (id, town, age) VALUES ('1', 'London', 30), ('2', 'Paris', 25), ('3', 'New York', 35);",
    )
    .await;
}

#[cfg(feature = "postgres")]
#[expect(clippy::expect_used)]
async fn execute_pg_statement(db_conn: &PostgresConnection, sql: &str) {
    db_conn
        .conn
        .execute(sql, &[])
        .await
        .expect("statement can be executed");
}

#[cfg(feature = "postgres")]
async fn initialize_runtime(port: usize) -> Result<Runtime, anyhow::Error> {
    initialize_runtime_with_mode(port, Mode::File, DUCKDB_FILE_PATH).await
}

#[cfg(feature = "postgres")]
async fn initialize_runtime_with_mode(
    port: usize,
    mode: Mode,
    duckdb_file: &str,
) -> Result<Runtime, anyhow::Error> {
    // Re-register connectors in case a previous runtime shutdown cleared them
    register_test_connectors().await;

    let mut ds = Dataset::new("postgres:chameleon", "cham");

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
    ds.params = Some(params.clone());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode,
        params: Some(Params::from_string_map(
            vec![("duckdb_file".to_string(), duckdb_file.to_string())]
                .into_iter()
                .collect(),
        )),
        ..Acceleration::default()
    });

    let ds_clone = ds.clone();

    let app = AppBuilder::new("test_schema_evolution")
        .with_dataset(ds)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let cloned_rt = Arc::clone(&rt);

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    // Wait for checkpoint to be created (checkpoint creation is async after runtime is ready)
    let app_ref = rt.app();
    let app_lock = app_ref.read().await;
    let Some(app) = app_lock.as_ref() else {
        return Err(anyhow::anyhow!("Failed to obtain app from runtime"));
    };

    let runtime_dataset = runtime::component::dataset::builder::DatasetBuilder::try_from(ds_clone)
        .map_err(|e| anyhow::anyhow!("Failed to create dataset builder: {e}"))?
        .with_app(Arc::clone(app))
        .with_runtime(Arc::clone(&rt))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build dataset: {e}"))?;
    wait_for_checkpoint(&runtime_dataset, 30).await?;

    // Drop the app lock before returning
    drop(app_lock);

    // Unwrap the Arc to get ownership of the Runtime
    Ok(Arc::try_unwrap(rt).unwrap_or_else(|arc| (*arc).clone()))
}

async fn wait_for_checkpoint(
    dataset: &RuntimeDataset,
    timeout_secs: u64,
) -> Result<(), anyhow::Error> {
    let checkpoint = DatasetCheckpoint::try_new(dataset, OpenOption::OpenExisting)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create checkpoint: {e}"))?;

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    while !checkpoint.exists().await {
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!("Timed out waiting for checkpoint to exist"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}

/// Tests `Mode::FileUpdate` schema detection: additive changes keep the existing file,
/// while breaking changes (column removed, type changed) trigger table recreation.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_schema_evolution_file_update_mode() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    if std::fs::metadata(DUCKDB_FILE_UPDATE_PATH).is_ok() {
        std::fs::remove_file(DUCKDB_FILE_UPDATE_PATH).expect("should remove local database");
    }

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            // Reset the table and do an initial load with file_update mode
            reset_pg_table(&db_conn).await;

            let rt = Arc::new(
                initialize_runtime_with_mode(port, Mode::FileUpdate, DUCKDB_FILE_UPDATE_PATH)
                    .await?,
            );

            let sql = "SELECT id, town, age FROM cham ORDER BY id ASC";
            run_and_verify_query(&rt, sql, "test_schema_evolution_file_update_initial").await;

            // Additive change: add a column — should NOT trigger recreation
            rt.shutdown().await;
            drop(rt);
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(
                initialize_runtime_with_mode(port, Mode::FileUpdate, DUCKDB_FILE_UPDATE_PATH)
                    .await?,
            );
            run_and_verify_query(&rt, sql, "test_schema_evolution_file_update_add_column").await;

            // Breaking change: drop a column — should trigger table recreation
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(&db_conn, "ALTER TABLE public.chameleon DROP COLUMN age;").await;
            let rt = Arc::new(
                initialize_runtime_with_mode(port, Mode::FileUpdate, DUCKDB_FILE_UPDATE_PATH)
                    .await?,
            );
            run_and_verify_query(
                &rt,
                "SELECT id, town FROM cham ORDER BY id ASC",
                "test_schema_evolution_file_update_drop_column",
            )
            .await;

            // Breaking change: change column type — should trigger table recreation
            rt.shutdown().await;
            drop(rt);
            reset_pg_table(&db_conn).await;
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE chameleon ALTER COLUMN age TYPE TEXT USING (age::TEXT);",
            )
            .await;
            let rt = Arc::new(
                initialize_runtime_with_mode(port, Mode::FileUpdate, DUCKDB_FILE_UPDATE_PATH)
                    .await?,
            );
            run_and_verify_query(&rt, sql, "test_schema_evolution_file_update_change_type").await;

            running_container.remove().await?;

            if std::fs::metadata(DUCKDB_FILE_UPDATE_PATH).is_ok() {
                std::fs::remove_file(DUCKDB_FILE_UPDATE_PATH)
                    .expect("should remove local database");
            }

            Ok(())
        })
        .await
}

// --- CSV-based file_update tests (no Postgres dependency) ---

const CSV_INITIAL: &str = "id,name,age,city\n1,Alice,30,New York\n2,Bob,25,San Francisco\n";
const CSV_ADD_COLUMN: &str =
    "id,name,age,city,lname\n1,Alice,30,New York,Smith\n2,Bob,25,San Francisco,Jones\n";
const CSV_DROP_COLUMN: &str = "id,name,city\n1,Alice,New York\n2,Bob,San Francisco\n";

#[expect(clippy::expect_used)]
async fn csv_run_and_verify_query(rt: &Arc<Runtime>, sql: &str, snapshot_name: &str) {
    let record_batch = run_query(rt, sql).await.expect("query should succeed");
    insta::assert_snapshot!(
        snapshot_name,
        to_pretty_display(&record_batch).expect("pretty display")
    );
}

async fn init_csv_runtime(
    csv_path: &str,
    engine: &str,
    accel_params: HashMap<String, String>,
) -> Result<Runtime, anyhow::Error> {
    register_test_connectors().await;

    let mut ds = Dataset::new(format!("file:{csv_path}"), "sample");
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: Mode::FileUpdate,
        params: Some(Params::from_string_map(accel_params)),
        ..Acceleration::default()
    });

    let app = AppBuilder::new("test_file_update_csv")
        .with_dataset(ds.clone())
        .build();

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    // Wait for the file_update checkpoint to be written so the next phase's schema
    // detection has the previous schema available.  The checkpoint is stored via the
    // accelerator's own metadata mechanism (e.g. SQLite for Cayenne) — if the
    // current engine doesn't support it, skip (best-effort).
    let app_ref = rt.app();
    let app_lock = app_ref.read().await;
    if let Some(app) = app_lock.as_ref()
        && let Ok(runtime_dataset) =
            runtime::component::dataset::builder::DatasetBuilder::try_from(ds)
                .map_err(anyhow::Error::from)
                .and_then(|b| {
                    b.with_app(Arc::clone(app))
                        .with_runtime(Arc::clone(&rt))
                        .build()
                        .map_err(anyhow::Error::from)
                })
    {
        // Ignore errors (e.g. UnsupportedEngine for Cayenne without sqlite feature)
        let _ = wait_for_checkpoint(&runtime_dataset, 30).await;
    }
    drop(app_lock);

    Ok(Arc::try_unwrap(rt).unwrap_or_else(|arc| (*arc).clone()))
}

async fn run_file_update_csv_phases(
    engine: &str,
    accel_params: HashMap<String, String>,
    csv_path: &str,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let test_prefix = format!("test_file_update_csv_{engine}");
    let sql = "SELECT * FROM sample ORDER BY id";

    test_request_context()
        .scope(async {
            // Phase 1: Initial load (4 columns)
            std::fs::write(csv_path, CSV_INITIAL).expect("write csv");
            let rt = Arc::new(init_csv_runtime(csv_path, engine, accel_params.clone()).await?);
            csv_run_and_verify_query(&rt, sql, &format!("{test_prefix}__initial")).await;
            rt.shutdown().await;
            drop(rt);

            // Phase 2: Add column (5 columns) — should trigger recreation
            std::fs::write(csv_path, CSV_ADD_COLUMN).expect("write csv");
            let rt = Arc::new(init_csv_runtime(csv_path, engine, accel_params.clone()).await?);
            csv_run_and_verify_query(&rt, sql, &format!("{test_prefix}__add_column")).await;
            rt.shutdown().await;
            drop(rt);

            // Phase 3: Drop column (3 columns) — should trigger recreation
            std::fs::write(csv_path, CSV_DROP_COLUMN).expect("write csv");
            let rt = Arc::new(init_csv_runtime(csv_path, engine, accel_params.clone()).await?);
            csv_run_and_verify_query(&rt, sql, &format!("{test_prefix}__drop_column")).await;
            rt.shutdown().await;
            drop(rt);

            // Phase 4: No-change restart — should preserve existing data
            let rt = Arc::new(init_csv_runtime(csv_path, engine, accel_params.clone()).await?);
            csv_run_and_verify_query(&rt, sql, &format!("{test_prefix}__no_change_restart")).await;
            rt.shutdown().await;
            drop(rt);

            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_file_update_csv_duckdb() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let accel_file = temp_dir.path().join("sample.duckdb");
    let csv_file = temp_dir.path().join("sample.csv");
    let params = HashMap::from([(
        "duckdb_file".to_string(),
        accel_file.to_string_lossy().to_string(),
    )]);
    run_file_update_csv_phases("duckdb", params, &csv_file.to_string_lossy()).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_file_update_csv_sqlite() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let accel_file = temp_dir.path().join("sample.db");
    let csv_file = temp_dir.path().join("sample.csv");
    let params = HashMap::from([(
        "sqlite_file".to_string(),
        accel_file.to_string_lossy().to_string(),
    )]);
    run_file_update_csv_phases("sqlite", params, &csv_file.to_string_lossy()).await
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_file_update_csv_cayenne() -> Result<(), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");
    let csv_file = temp_dir.path().join("sample.csv");
    let params = HashMap::from([
        (
            "cayenne_file_path".to_string(),
            data_dir.to_string_lossy().to_string(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            metadata_dir.to_string_lossy().to_string(),
        ),
    ]);
    run_file_update_csv_phases("cayenne", params, &csv_file.to_string_lossy()).await
}

// --- Widening schema evolution (`on_schema_change`) tests ---
//
// These exercise the restart-time widening-evolution path (`mode: File`, full refresh)
// across a real source schema change. They contrast with the default `block` policy
// (covered above by `test_schema_evolution`, which serves stale data via a Deferred
// provider): under `sync_all_columns`/`append_new_columns` the accelerator table is
// evolved in place and the new schema becomes queryable instead of the dataset
// deferring forever.
//
// Assertions are explicit (row counts / column presence / widened arrow type) rather
// than insta snapshots so the cases pass on first CI run without pre-accepted `.snap`
// files.

#[cfg(feature = "postgres")]
async fn init_widen_pg_runtime(
    port: usize,
    engine: &str,
    accel_params: HashMap<String, String>,
    on_schema_change: OnSchemaChange,
) -> Result<Runtime, anyhow::Error> {
    register_test_connectors().await;

    let mut ds = Dataset::new("postgres:chameleon", "cham");
    ds.params = Some(Params::from_string_map(
        vec![
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    ds.on_schema_change = on_schema_change;
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: Mode::File,
        params: Some(Params::from_string_map(accel_params)),
        ..Acceleration::default()
    });

    let ds_clone = ds.clone();

    let app = AppBuilder::new("test_schema_evolution_widening")
        .with_dataset(ds)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let app_ref = rt.app();
    let app_lock = app_ref.read().await;
    if let Some(app) = app_lock.as_ref()
        && let Ok(runtime_dataset) =
            runtime::component::dataset::builder::DatasetBuilder::try_from(ds_clone)
                .map_err(anyhow::Error::from)
                .and_then(|b| {
                    b.with_app(Arc::clone(app))
                        .with_runtime(Arc::clone(&rt))
                        .build()
                        .map_err(anyhow::Error::from)
                })
    {
        let _ = wait_for_checkpoint(&runtime_dataset, 30).await;
    }
    drop(app_lock);

    Ok(Arc::try_unwrap(rt).unwrap_or_else(|arc| (*arc).clone()))
}

#[cfg(feature = "postgres")]
#[expect(clippy::expect_used)]
async fn assert_query_row_count(rt: &Arc<Runtime>, sql: &str, expected_rows: usize) {
    let batches = run_query(rt, sql)
        .await
        .expect("query should succeed after evolution");
    let rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(
        rows, expected_rows,
        "query `{sql}` returned {rows} rows, expected {expected_rows}"
    );
}

/// Asserts the accelerated column `column` reports arrow type `arrow_type` (e.g. `Int64`),
/// proving an in-place type widening was applied to the engine table and the registered
/// schema. Reads the type from the result batch's Arrow schema rather than `arrow_typeof`,
/// which is a `DataFusion` scalar function the accelerator engine (`DuckDB`) cannot evaluate
/// when the projection is pushed down.
#[cfg(feature = "postgres")]
#[expect(clippy::expect_used)]
async fn assert_column_arrow_type(rt: &Arc<Runtime>, column: &str, arrow_type: &str) {
    let sql = format!("SELECT {column} FROM cham LIMIT 1");
    let batches = run_query(rt, &sql)
        .await
        .expect("projection query should succeed");
    let schema = batches.first().expect("at least one record batch").schema();
    let field = schema
        .field_with_name(column)
        .expect("queried column present in result schema");
    let actual = format!("{:?}", field.data_type());
    assert!(
        actual.contains(arrow_type),
        "expected `{column}` to be `{arrow_type}` after evolution, got `{actual}`"
    );
}

/// Restart-time widening under `sync_all_columns`: an additive column and a lossless
/// `int4 -> int8` type widening are both adopted in place, and the accelerated dataset
/// keeps serving (rather than deferring on the old schema as `block` would).
#[cfg(all(feature = "postgres", feature = "duckdb"))]
#[tokio::test]
async fn test_schema_evolution_widening_duckdb_sync_all_columns() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let accel_file = temp_dir.path().join("widen.duckdb");
            let params = HashMap::from([(
                "duckdb_file".to_string(),
                accel_file.to_string_lossy().to_string(),
            )]);

            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            reset_pg_table(&db_conn).await;

            // Phase 1: initial load (id, town, age).
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::SyncAllColumns,
                )
                .await?,
            );
            assert_query_row_count(&rt, "SELECT id, town, age FROM cham ORDER BY id ASC", 3).await;
            rt.shutdown().await;
            drop(rt);

            // Phase 2: additive widening — a new nullable column is adopted in place on
            // restart. Existing rows are preserved (with the column backfilled NULL); the
            // restart evolves the schema rather than re-fetching the source, so the row
            // count is unchanged. Under `block` this query would error (country is not in
            // the registered schema); under `sync_all_columns` the column is queryable.
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::SyncAllColumns,
                )
                .await?,
            );
            assert_query_row_count(
                &rt,
                "SELECT id, town, age, country FROM cham ORDER BY id ASC",
                3,
            )
            .await;
            rt.shutdown().await;
            drop(rt);

            // Phase 3: type widening — int4 -> int8 applied in place to the existing rows.
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ALTER COLUMN age TYPE int8;",
            )
            .await;
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::SyncAllColumns,
                )
                .await?,
            );
            assert_query_row_count(
                &rt,
                "SELECT id, town, age, country FROM cham ORDER BY id ASC",
                3,
            )
            .await;
            assert_column_arrow_type(&rt, "age", "Int64").await;
            rt.shutdown().await;
            drop(rt);

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// Restart-time widening on the Cayenne engine under `sync_all_columns` — same scenario
/// as the `DuckDB` case, validating the `Cayenne` metastore schema update + provider swap.
#[cfg(all(feature = "postgres", not(windows)))]
#[tokio::test]
async fn test_schema_evolution_widening_cayenne_sync_all_columns() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    temp_dir.path().join("data").to_string_lossy().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    temp_dir
                        .path()
                        .join("metadata")
                        .to_string_lossy()
                        .to_string(),
                ),
            ]);

            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            reset_pg_table(&db_conn).await;

            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "cayenne",
                    params.clone(),
                    OnSchemaChange::SyncAllColumns,
                )
                .await?,
            );
            assert_query_row_count(&rt, "SELECT id, town, age FROM cham ORDER BY id ASC", 3).await;
            rt.shutdown().await;
            drop(rt);

            // Additive widening on Cayenne: the metastore schema update + provider swap
            // adopt `country` in place on restart; existing rows are preserved (backfilled
            // NULL), so the row count is unchanged and the column becomes queryable.
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "cayenne",
                    params.clone(),
                    OnSchemaChange::SyncAllColumns,
                )
                .await?,
            );
            assert_query_row_count(
                &rt,
                "SELECT id, town, age, country FROM cham ORDER BY id ASC",
                3,
            )
            .await;
            rt.shutdown().await;
            drop(rt);

            running_container.remove().await?;
            Ok(())
        })
        .await
}

/// `append_new_columns` adopts a new nullable column but does NOT apply a type widening:
/// the `int4 -> int8` change falls back to block-equivalent behavior (the dataset keeps
/// serving the prior schema instead of evolving the type).
#[cfg(all(feature = "postgres", feature = "duckdb"))]
#[tokio::test]
async fn test_schema_evolution_append_new_columns_only() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let accel_file = temp_dir.path().join("append.duckdb");
            let params = HashMap::from([(
                "duckdb_file".to_string(),
                accel_file.to_string_lossy().to_string(),
            )]);

            let port = common::get_random_port()?;
            let running_container = common::start_postgres_docker_container(port).await?;
            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            reset_pg_table(&db_conn).await;

            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::AppendNewColumns,
                )
                .await?,
            );
            assert_query_row_count(&rt, "SELECT id, town, age FROM cham ORDER BY id ASC", 3).await;
            rt.shutdown().await;
            drop(rt);

            // Additive column — adopted under append_new_columns.
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ADD COLUMN country varchar NULL;",
            )
            .await;
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::AppendNewColumns,
                )
                .await?,
            );
            assert_query_row_count(
                &rt,
                "SELECT id, town, age, country FROM cham ORDER BY id ASC",
                3,
            )
            .await;
            assert_column_arrow_type(&rt, "age", "Int32").await;
            rt.shutdown().await;
            drop(rt);

            // Type widening is NOT in the append_new_columns set: the dataset stays on the
            // prior schema (block-equivalent) — `age` remains Int32, and the original
            // projection keeps serving.
            execute_pg_statement(
                &db_conn,
                "ALTER TABLE public.chameleon ALTER COLUMN age TYPE int8;",
            )
            .await;
            let rt = Arc::new(
                init_widen_pg_runtime(
                    port,
                    "duckdb",
                    params.clone(),
                    OnSchemaChange::AppendNewColumns,
                )
                .await?,
            );
            assert_query_row_count(&rt, "SELECT id, town, country FROM cham ORDER BY id ASC", 3)
                .await;
            assert_column_arrow_type(&rt, "age", "Int32").await;
            rt.shutdown().await;
            drop(rt);

            running_container.remove().await?;
            Ok(())
        })
        .await
}
