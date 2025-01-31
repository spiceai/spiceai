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
    mysql::common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container},
    utils::runtime_ready_check,
};
use std::sync::Arc;

use super::*;
use app::AppBuilder;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use mysql_async::{prelude::Queryable, Params, Row};
use runtime::Runtime;
use tracing::instrument;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

const MYSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-federation-mysql";
const MYSQL_PORT: u16 = 13306;

#[instrument]
async fn init_mysql_db(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
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

#[tokio::test]
async fn mysql_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let running_container =
                start_mysql_docker_container(MYSQL_DOCKER_CONTAINER, MYSQL_PORT)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mysql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mysql_db(MYSQL_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MySQL database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mysql_federation_push_down")
                .with_dataset(make_mysql_dataset("lineitem", "line", MYSQL_PORT, false))
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

            let queries: QueryTests = vec![
                (
                    "SELECT * FROM line LIMIT 10",
                    "select_limit_10",
                    Some(Box::new(|result_batches| {
                        for batch in result_batches {
                            assert_eq!(
                                batch.num_columns(),
                                16,
                                "num_cols: {}",
                                batch.num_columns()
                            );
                            assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                        }
                    })),
                ),
                (
                    "SELECT * FROM line ORDER BY line.l_orderkey DESC LIMIT 10",
                    "select_order_by_limit_10",
                    Some(Box::new(|result_batches| {
                        for batch in result_batches {
                            assert_eq!(
                                batch.num_columns(),
                                16,
                                "num_cols: {}",
                                batch.num_columns()
                            );
                            assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                        }
                    })),
                ),
            ];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mysql_federation_push_down_{snapshot_suffix}"),
                    query,
                    true,
                    validate_result,
                )
                .await?;
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mysql_federation_inner_join_with_acc() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    let mysql_port = 13308;

    test_request_context().scope_retry(3, || async {
        let running_container = start_mysql_docker_container(
            "runtime-integration-test-federation-inner-join-mysql",
            mysql_port,
        )
        .await
        .map_err(|e| {
            tracing::error!("start_mysql_docker_container: {e}");
            e.to_string()
        })?;
        tracing::debug!("Container started");
        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
        retry(retry_strategy, || async {
            init_mysql_db(mysql_port)
                .await
                .map_err(RetryError::transient)
        })
        .await
        .map_err(|e| {
            tracing::error!("Failed to initialize MySQL database: {e}");
            e.to_string()
        })?;
        let app = AppBuilder::new("mysql_federation_inner_join_with_accelerated_dataset")
            .with_dataset(make_mysql_dataset("lineitem", "line", mysql_port, false))
            .with_dataset(make_mysql_dataset("lineitem", "acc_line", mysql_port, true))
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
            () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                return Err("Timed out waiting for datasets to load".to_string());
            }
            () = rt.load_components() => {}
        }

        runtime_ready_check(&rt).await;

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let queries: QueryTests = vec![
            (
                "SELECT * FROM line inner join acc_line on acc_line.l_orderkey = line.l_orderkey LIMIT 10",
                "inner_join_0",
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 32, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }
                })),
            ),
            (
                "SELECT line.* FROM line inner join acc_line on acc_line.l_orderkey = line.l_orderkey LIMIT 10",
                "inner_join_1",
                Some(Box::new(|result_batches| {
                    for batch in result_batches {
                        assert_eq!(batch.num_columns(), 16, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 10, "num_rows: {}", batch.num_rows());
                    }
                })),
            ),
        ];

        for (query, snapshot_suffix, validate_result) in queries {
            run_query_and_check_results(
                &mut rt,
                &format!("mysql_federation_inner_join_with_acc_{snapshot_suffix}"),
                query,
                true,
                validate_result,
            )
            .await?;
        }

        running_container.remove().await.map_err(|e| {
            tracing::error!("running_container.remove: {e}");
            e.to_string()
        })?;

        Ok(())
    }).await
}
