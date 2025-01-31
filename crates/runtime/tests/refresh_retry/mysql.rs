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
    docker::RunningContainer,
    mysql::common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container},
    utils::test_request_context,
};
use std::{sync::Arc, time::Duration};

use crate::init_tracing;

use app::AppBuilder;
use datafusion::{
    datasource::TableProvider, physical_plan::collect, prelude::SessionContext, sql::TableReference,
};
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::{
    accelerated_table::{refresh::Refresh, refresh_task::RefreshTask, AcceleratedTable},
    Runtime,
};
use spicepod::component::dataset::acceleration::Acceleration;
use tokio::time;
use tracing::instrument;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-refresh-retry-mysql";
const MYSQL_PORT: u16 = 13307;

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
    let _tracing = init_tracing(Some("integration=debug,info"));
    let running_container = start_mysql_docker_container(MYSQL_DOCKER_CONTAINER, MYSQL_PORT)
        .await
        .map_err(|e| {
            tracing::error!("start_mysql_docker_container: {e}");
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

async fn create_refresh_task(
    rt: &Runtime,
    table_name: &str,
) -> Result<(RefreshTask, Refresh), String> {
    let table = rt
        .datafusion()
        .get_accelerated_table_provider(table_name)
        .await
        .map_err(|e| e.to_string())?;

    let accelerated_table = table
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .ok_or("table is not an AcceleratedTable")?;

    Ok((
        RefreshTask::new(
            rt.status(),
            table_name.into(),
            Arc::clone(&accelerated_table.get_federated_table()),
            None,
            accelerated_table.get_accelerator(),
        ),
        accelerated_table.refresh_params().read().await.clone(),
    ))
}

async fn get_accelerator(rt: &Runtime, table_name: &str) -> Result<Arc<dyn TableProvider>, String> {
    let table = rt
        .datafusion()
        .get_accelerated_table_provider(table_name)
        .await
        .map_err(|e| e.to_string())?;

    let accelerated_table = table
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .ok_or("table is not an AcceleratedTable")?;

    Ok(Arc::clone(&accelerated_table.get_accelerator()))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mysql_refresh_retries() -> Result<(), String> {
    test_request_context()
        .scope(async {
            let running_container = prepare_test_environment().await?;
            let running_container = Arc::new(running_container);

            let mut ds_no_retries =
                make_mysql_dataset("lineitem", "lineitem_no_retries", MYSQL_PORT, false);
            ds_no_retries.acceleration = Some(Acceleration {
                enabled: true,
                refresh_retry_enabled: false,
                refresh_sql: Some("SELECT * from lineitem_no_retries limit 1".to_string()),
                ..Default::default()
            });

            let mut ds_default_retries =
                make_mysql_dataset("lineitem", "lineitem_retries", MYSQL_PORT, false);
            ds_default_retries.acceleration = Some(Acceleration {
                enabled: true,
                refresh_sql: Some("SELECT * from lineitem_retries limit 1".to_string()),
                ..Default::default()
            });

            let app = AppBuilder::new("mysql_refresh_retry")
                .with_dataset(ds_no_retries)
                .with_dataset(ds_default_retries)
                .build();

            let status = runtime::status::RuntimeStatus::new();
            let df = crate::get_test_datafusion(Arc::clone(&status));

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .build()
                .await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = rt.load_components() => {}
            }

            let (refresh_task_no_retries, request) =
                create_refresh_task(&rt, "lineitem_no_retries").await?;

            tracing::debug!("Simulating connectivity issue...");
            running_container.stop().await.map_err(|e| {
                tracing::error!("running_container.stop: {e}");
                e.to_string()
            })?;

            // Refresh should fail fast w/o retries
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    return Err("Timed out waiting for dataset refresh result".to_string());
                },
                res = refresh_task_no_retries.run(request) => {
                    tracing::debug!("Refresh task completed. Is error={}", res.is_err());
                    assert!(res.is_err(), "Expected refresh error but got {res:?}");
                }
            };

            let running_container_reference_copy = Arc::clone(&running_container);

            tokio::spawn(async move {
                // restore connectivity after few seconds
                time::sleep(Duration::from_secs(2)).await;
                tracing::debug!("Restoring connectivity...");
                assert!(running_container_reference_copy
                    .start()
                    .await
                    .map_err(|e| {
                        tracing::error!("running_container.start: {e}");
                        e.to_string()
                    })
                    .is_ok());
            });

            // set custom refresh sql to check number of items loaded later
            rt.datafusion()
                .update_refresh_sql(
                    TableReference::parse_str("lineitem_retries"),
                    Some("SELECT * from lineitem_retries limit 10".to_string()),
                )
                .await
                .map_err(|e| e.to_string())?;

            // For this test, must create_refresh request after `rt.datafusion().update_refresh_sql` to avoid getting old `refresh_sql`.
            let (refresh_task_retries, request) =
                create_refresh_task(&rt, "lineitem_retries").await?;
            println!("Running request for 'lineitem_retries' table: {request:?}");

            // Refresh should do retries and succeed
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
                    return Err("Timed out waiting for dataset refresh result".to_string());
                },
                res = refresh_task_retries.run(request) => {
                    tracing::debug!("Refresh task completed. Is error={}", res.is_err());
                    assert!(res.is_ok(), "Expected refresh succeed after retrying but got {res:?}");
                }
            };

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            // check that the accelerated table contains the expected number of rows
            let accelerated_table = get_accelerator(&rt, "lineitem_retries").await?;
            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerated_table
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(10, result.first().expect("result").num_rows());

            Ok(())
        })
        .await
}
