/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
//! Runs federation integration tests for `MySQL`.
//!
//! Expects a Docker daemon to be running.
use crate::{
    acceleration::ACCELERATION_MUTEX,
    docker::RunningContainer,
    get_test_datafusion,
    mysql::common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container},
    utils::{runtime_ready_check, test_request_context},
};
use std::sync::Arc;

use crate::init_tracing;

use anyhow::Context;
use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use futures::TryStreamExt;
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::{spice_data_base_path, status, Runtime};
use spicepod::component::dataset::{
    acceleration::{Acceleration, IndexType, Mode},
    Dataset,
};

use spicepod::component::params::Params as SpicepodParams;

use tracing::instrument;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

#[cfg(feature = "duckdb")]
mod duckdb;

#[cfg(feature = "sqlite")]
mod sqlite;

#[tokio::test]
async fn spill_to_disk_and_rehydration() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _guard = ACCELERATION_MUTEX.lock().await;

    test_request_context().scope(async {
        let running_container = prepare_test_environment()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        let running_container = Arc::new(running_container);

        let config = vec![
            #[cfg(feature = "duckdb")]
            ("duckdb", None),
            #[cfg(feature = "duckdb")]
            ("duckdb", Some("./.spice/my_duckdb.db")),
            #[cfg(feature = "sqlite")]
            ("sqlite", None),
            #[cfg(feature = "sqlite")]
            ("sqlite", Some("./.spice/my_sqlite.db")),
        ];

        for (idx, (engine, db_file_path)) in config.into_iter().enumerate() {
            tracing::info!("Testing spill-to-disk and rehydration with engine: {engine}, db_file_path: {db_file_path:?}");

            if idx > 0 {
                // Ensure the container is running as the tests manipulate it
                running_container
                    .start()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
            }
            execute_spill_to_disk_and_rehydration(Arc::clone(&running_container), engine, db_file_path)
                .await?;
        }

        running_container
            .remove()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        tracing::info!("Spill-to-disk and rehydration tests passed!");

        Ok(())
    }).await
}

/// Validates spill-to-disk and rehydration functionality by simulating runtime restarts
/// and checking data consistency.
///
/// 1. Retrieve the number of rows using a native `MySQL` connection to use as a baseline.
/// 2. Start Spice, retrieve row count and loaded items after acceleration is completed, and compare with the baseline.
/// 3. Restart the runtime and ensure the loaded items remain consistent immediately after the runtime is loaded.
/// 4. Simulate federated dataset access issue after the runtime is restarted, ensure query result remain consistent.
#[expect(clippy::expect_used)]
async fn execute_spill_to_disk_and_rehydration(
    federated_dataset_container: Arc<RunningContainer<'static>>,
    engine: &str,
    db_file_path: Option<&str>,
) -> Result<(), anyhow::Error> {
    // retrieve number of rows using native mysql connection
    // this also ensures that federated dataset is available
    let pool = get_mysql_conn(MYSQL_PORT)?;
    let res: Vec<Row> = pool
        .get_conn()
        .await?
        .exec("SELECT COUNT(*) FROM lineitem", Params::Empty)
        .await?;
    let num_rows: u64 = res[0].get(0).context("Unable to retrieve number of rows")?;
    assert!(num_rows > 0);

    let accelerated_db_file_path = resolve_local_db_file_path(engine, db_file_path)?;
    tracing::debug!(
        "Expected accelerated database location: {}",
        &accelerated_db_file_path
    );

    // clean up: delete local database file if exists before running the test
    for file_path in [
        accelerated_db_file_path.clone(),
        format!("{accelerated_db_file_path}-wal"),
        format!("{accelerated_db_file_path}-shm"),
    ] {
        if std::fs::metadata(&file_path).is_ok() {
            std::fs::remove_file(&file_path).context("should remove local database")?;
        }
    }

    let rt = init_spice_app(engine, db_file_path, false).await?;
    runtime_ready_check(&rt).await;

    if std::fs::metadata(&accelerated_db_file_path).is_err() {
        return Err(anyhow::anyhow!(
            "Accelerated database file not found at path: {accelerated_db_file_path}"
        ));
    }

    let test_query =
        "SELECT l_orderkey, l_linenumber  FROM lineitem ORDER BY l_orderkey, l_linenumber LIMIT 10";

    let original_items = run_query(test_query, &rt).await?;
    let num_rows_loaded: usize = original_items
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    // ensure data has been loaded correctly
    assert_eq!(num_rows_loaded as u64, 10);

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    drop(rt);
    runtime::dataaccelerator::clear_registry().await;
    runtime::dataaccelerator::register_all().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
    retry(retry_strategy, || async {
        let num_persisted_records: usize =
            get_locally_persisted_records(engine, &accelerated_db_file_path, test_query)
                .await
                .map_err(RetryError::transient)?
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
        if num_persisted_records != num_rows_loaded {
            return Err(RetryError::transient(anyhow::anyhow!(
                "Number of persisted records {num_persisted_records} does not match expected number of records {num_rows_loaded}",
            )));
        }
        Ok(())
    })
    .await?;

    // Restart the runtime and ensure the loaded items remain consistent
    let rt = init_spice_app(engine, db_file_path, false).await?;
    // Do request immediately after restart w/o waiting for ready status (dataset is refreshed)
    let restart1_items = run_query(test_query, &rt).await?;
    let restart1_items_pretty =
        arrow::util::pretty::pretty_format_batches(&restart1_items).expect("pretty format");
    insta::assert_snapshot!("records", restart1_items_pretty);

    drop(rt);

    // Restart the runtime with updated app definition that includes primary key and indexes
    let rt = init_spice_app(engine, db_file_path, true).await?;
    let restart2_items = run_query(test_query, &rt).await?;
    let restart2_items_pretty =
        arrow::util::pretty::pretty_format_batches(&restart2_items).expect("pretty format");
    insta::assert_snapshot!("records", restart2_items_pretty);

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    drop(rt);
    runtime::dataaccelerator::clear_registry().await;
    runtime::dataaccelerator::register_all().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Simulate federated dataset access issue after the runtime is restarted, ensure query result remain consistent
    let rt = init_spice_app(engine, db_file_path, false).await?;
    federated_dataset_container.stop().await?;
    let restart3_items = run_query(test_query, &rt).await?;
    let restart3_items_pretty =
        arrow::util::pretty::pretty_format_batches(&restart3_items).expect("pretty format");
    insta::assert_snapshot!("records", restart3_items_pretty);

    Ok(())
}

async fn get_locally_persisted_records(
    engine: &str,
    db_file_path: &str,
    query: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let query_result = match engine {
        "duckdb" => duckdb::query_local_db(db_file_path, query).await?,
        "sqlite" => sqlite::query_local_db(db_file_path, query).await?,
        _ => Err(anyhow::anyhow!("Unsupported engine: {engine}"))?,
    };

    query_result
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::anyhow!("Unable to collect query results: {e}"))
}

fn resolve_local_db_file_path(
    engine: &str,
    db_file_path: Option<&str>,
) -> Result<String, anyhow::Error> {
    if let Some(db_file_path) = db_file_path {
        let working_dir = std::env::current_dir().unwrap_or(".".into());
        return Ok(format!(
            "{}/{db_file_path}",
            working_dir.to_str().context("Unable to get current dir")?
        ));
    }

    Ok(format!(
        "{}/accelerated_{engine}.db",
        spice_data_base_path()
    ))
}

async fn run_query(query: &str, rt: &Runtime) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run query: {:?}", e))?;

    let collected_data = query_result
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect query results: {:?}", e))?;

    Ok(collected_data)
}

async fn init_spice_app(
    acceleration_engine: &str,
    db_file_path: Option<&str>,
    with_pk_and_indexes: bool,
) -> Result<Runtime, anyhow::Error> {
    let ds = create_test_dataset(acceleration_engine, db_file_path, with_pk_and_indexes);

    let app = AppBuilder::new("spiceapp").with_dataset(ds).build();

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .with_runtime_status(status)
        .build()
        .await;

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    Ok(rt)
}

fn create_test_dataset(
    acceleration_engine: &str,
    db_file_path: Option<&str>,
    with_pk_and_indexes: bool,
) -> Dataset {
    let mut ds = make_mysql_dataset("lineitem", "lineitem", MYSQL_PORT, false);

    let mut acceleration = Acceleration {
        enabled: true,
        engine: Some(acceleration_engine.to_string()),
        mode: Mode::File,
        ..Default::default()
    };

    if let Some(db_file_path) = db_file_path {
        let params = SpicepodParams::from_string_map(
            vec![(
                format!("{acceleration_engine}_file",),
                db_file_path.to_string(),
            )]
            .into_iter()
            .collect(),
        );
        acceleration.params = Some(params);
    }

    if with_pk_and_indexes {
        acceleration.primary_key = Some("(l_orderkey, l_linenumber)".to_string());
        acceleration.indexes = vec![("l_shipdate".to_string(), IndexType::Enabled)]
            .into_iter()
            .collect();
    }

    ds.acceleration = Some(acceleration);

    ds
}

const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-rehydration-retry-mysql";
const MYSQL_PORT: u16 = 13337;

#[instrument]
async fn init_mysql_db() -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(MYSQL_PORT)?;
    let mut conn = pool.get_conn().await?;

    tracing::debug!("DROP TABLE IF EXISTS lineitem");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS lineitem", Params::Empty)
        .await?;

    tracing::debug!("Downloading TPCH lineitem...");
    let tpch_lineitem = crate::get_tpch_lineitem().await?;

    let tpch_lineitem_schema = Arc::clone(&tpch_lineitem[0].schema());

    let create_table_stmt = CreateTableBuilder::new(tpch_lineitem_schema, "lineitem").build_mysql();
    tracing::debug!("CREATE TABLE lineitem...");
    let _: Vec<Row> = conn.exec(create_table_stmt, Params::Empty).await?;

    tracing::debug!("INSERT INTO lineitem...");
    let insert_stmt =
        InsertBuilder::new(&TableReference::from("lineitem"), tpch_lineitem).build_mysql(None)?;
    let _: Vec<Row> = conn.exec(insert_stmt, Params::Empty).await?;
    tracing::debug!("MySQL initialized!");

    Ok(())
}

#[instrument]
async fn prepare_test_environment() -> Result<RunningContainer<'static>, String> {
    let running_container = start_mysql_docker_container(MYSQL_DOCKER_CONTAINER, MYSQL_PORT)
        .await
        .map_err(|e| {
            tracing::error!("Failed to start MySQL Docker container: {e}");
            e.to_string()
        })?;
    tracing::debug!("Container started");
    let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
    retry(retry_strategy, || async {
        init_mysql_db().await.map_err(RetryError::transient)
    })
    .await
    .map_err(|e| {
        tracing::error!("Failed to initialize MySQL database: {e}");
        e.to_string()
    })?;

    Ok(running_container)
}
