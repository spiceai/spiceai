/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::utils::mysql::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container};
use std::sync::Arc;

use crate::init_tracing;

use app::AppBuilder;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::Runtime;
use tracing::instrument;

const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-frefresh-retry-mysql";

#[instrument]
async fn init_mysql_db() -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn()?;
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
    let insert_stmt = InsertBuilder::new("lineitem", tpch_lineitem).build_mysql(None)?;
    let _: Vec<Row> = conn.exec(insert_stmt, Params::Empty).await?;
    tracing::debug!("MySQL initialized!");

    Ok(())
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_refresh_no_retries_by_default() -> Result<(), String> {
    use runtime::accelerated_table::{refresh_task::RefreshTask, AcceleratedTable};
    use spicepod::component::dataset::acceleration::Acceleration;

    let _tracing = init_tracing(Some("integration=debug,info"));
    let running_container = start_mysql_docker_container(MYSQL_DOCKER_CONTAINER)
        .await
        .map_err(|e| {
            tracing::error!("start_mysql_docker_container: {e}");
            e.to_string()
        })?;
    tracing::debug!("Container started");
    init_mysql_db().await.map_err(|e| {
        tracing::error!("init_mysql_db: {e}");
        e.to_string()
    })?;

    let mut ds = make_mysql_dataset("lineitem", "lineitem");
    ds.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some("SELECT * from lineitem limit 1".to_string()),
        ..Default::default()
    });

    let app = AppBuilder::new("mysql_refresh_no_retry")
        .with_dataset(ds)
        .build();

    let rt = Runtime::new(Some(app), Arc::new(vec![])).await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_datasets() => {}
    }

    let lineitem_table = rt
        .datafusion()
        .ctx
        .table_provider("lineitem")
        .await
        .map_err(|e| e.to_string())?;

    let lineitem_accelerated_table = lineitem_table
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .ok_or("lineitem table is not an AcceleratedTable")?;

    let refresh_task = Arc::new(RefreshTask::new(
        "lineitem".into(),
        Arc::clone(&lineitem_accelerated_table.get_federated_table()),
        lineitem_accelerated_table.refresh_params(),
        lineitem_table,
    ));

    running_container.stop().await.map_err(|e| {
        tracing::error!("running_container.stop: {e}");
        e.to_string()
    })?;

    // Refresh should fail w/o retries
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return Err("Timed out waiting for dataset refresh result".to_string());
        },
        res = refresh_task.get_full_or_incremental_append_update(None) => {
            assert!(res.is_err(), "Expected refresh error but got {res:?}");
        }
    };

    running_container.remove().await.map_err(|e| {
        tracing::error!("running_container.remove: {e}");
        e.to_string()
    })?;

    Ok(())
}
