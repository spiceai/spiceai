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

use std::sync::Arc;

use common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container};
use mysql_async::prelude::Queryable;

use crate::init_tracing;

pub mod common;

use super::*;
use app::AppBuilder;
use mysql_async::{Params, Row};
use runtime::Runtime;
use tracing::instrument;

const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-types-mysql";
const MYSQL_PORT: u16 = 13316;

#[instrument]
async fn init_mysql_db(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;

    tracing::debug!("DROP TABLE IF EXISTS test");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS test", Params::Empty)
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "
CREATE TABLE test (
    id VARCHAR(36) PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO test (id, created_at) VALUES ('5ea5a3ac-07a0-4d4d-b201-faff68d8356c', '2023-05-02 10:30:00-04:00');", Params::Empty
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn mysql_integration_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    let running_container = start_mysql_docker_container(MYSQL_DOCKER_CONTAINER, MYSQL_PORT)
        .await
        .map_err(|e| {
            tracing::error!("start_mysql_docker_container: {e}");
            e.to_string()
        })?;
    tracing::debug!("Container started");
    init_mysql_db(MYSQL_PORT).await.map_err(|e| {
        tracing::error!("init_mysql_db: {e}");
        e.to_string()
    })?;
    let app = AppBuilder::new("mysql_integration_test")
        .with_dataset(make_mysql_dataset("test", "test", MYSQL_PORT, false))
        .build();

    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let mut rt = Runtime::builder()
        .with_app(app)
        .with_datafusion(df)
        .with_runtime_status(status)
        .build()
        .await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_components() => {}
    }

    let queries: QueryTests = vec![(
        "SELECT * FROM test LIMIT 1",
        "select_limit_1",
        Some(Box::new(|result_batches| {
            for batch in &result_batches {
                assert_eq!(batch.num_columns(), 2, "num_cols: {}", batch.num_columns());
                assert_eq!(batch.num_rows(), 1, "num_rows: {}", batch.num_rows());
            }

            // snapshot the values of the results
            let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                .expect("should pretty print result batch");
            insta::with_settings!({
                description => format!("MySQL Integration Test Results"),
                omit_expression => true,
                snapshot_path => "../snapshots"
            }, {
                insta::assert_snapshot!(format!("mysql_integration_test_select_limit_1"), results);
            });
        })),
    )];

    for (query, snapshot_suffix, validate_result) in queries {
        run_query_and_check_results(
            &mut rt,
            &format!("mysql_integration_test_{snapshot_suffix}"),
            query,
            validate_result,
        )
        .await?;
    }

    running_container.remove().await.map_err(|e| {
        tracing::error!("running_container.remove: {e}");
        e.to_string()
    })?;

    Ok(())
}
