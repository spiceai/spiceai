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
    utils::{register_test_connectors, runtime_ready_check},
};
use std::sync::Arc;

use super::*;
use app::AppBuilder;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use futures::TryStreamExt;
use mysql_async::{Params, Row, prelude::Queryable};

use runtime::Runtime;
use spicepod::component::catalog::Catalog;
use tracing::instrument;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

const MYSQL_PORT1: u16 = 13306;
const MYSQL_PORT2: u16 = 13308;
const MYSQL_PORT3: u16 = 13310;

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
        InsertBuilder::new(&TableReference::from("lineitem"), &tpch_lineitem).build_mysql(None)?;
    let _: Vec<Row> = conn.exec(insert_stmt, Params::Empty).await?;
    tracing::debug!("MySQL initialized!");

    Ok(())
}

async fn wait_for_query_rows(rt: &Runtime, sql: &str, expected_rows: usize) -> Result<(), String> {
    let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
    retry(retry_strategy, || async {
        let query_result = rt
            .datafusion()
            .query_builder(sql)
            .build()
            .run()
            .await
            .map_err(|e| RetryError::transient(anyhow::anyhow!(e)))?;

        let batches = query_result
            .data
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| RetryError::transient(anyhow::anyhow!(e)))?;
        let actual_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        if actual_rows >= expected_rows {
            return Ok(());
        }

        Err(RetryError::transient(anyhow::anyhow!(
            "query returned {actual_rows} rows; expected at least {expected_rows}"
        )))
    })
    .await
    .map_err(|e| e.to_string())
}

#[tokio::test]
async fn mysql_federation_push_down() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mysql_docker_container(MYSQL_PORT1)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mysql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mysql_db(MYSQL_PORT1)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MySQL database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mysql_federation_push_down")
                .with_dataset(make_mysql_dataset("lineitem", "line", MYSQL_PORT1, false))
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
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
async fn mysql_federation_inner_join_with_acc() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context().scope_retry(3, || async {
        let running_container = start_mysql_docker_container(
            MYSQL_PORT2,
        )
        .await
        .map_err(|e| {
            tracing::error!("start_mysql_docker_container: {e}");
            e.to_string()
        })?;
        tracing::debug!("Container started");
        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
        retry(retry_strategy, || async {
            init_mysql_db(MYSQL_PORT2)
                .await
                .map_err(RetryError::transient)
        })
        .await
        .map_err(|e| {
            tracing::error!("Failed to initialize MySQL database: {e}");
            e.to_string()
        })?;
        let app = AppBuilder::new("mysql_federation_inner_join_with_accelerated_dataset")
            .with_dataset(make_mysql_dataset("lineitem", "line", MYSQL_PORT2, false))
            .with_dataset(make_mysql_dataset("lineitem", "acc_line", MYSQL_PORT2, true))
            .build();

        configure_test_datafusion();
        let mut rt = Runtime::builder().with_app(app).build().await;

        let cloned_rt = Arc::new(rt.clone());
        // Set a timeout for the test
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                return Err("Timed out waiting for datasets to load".to_string());
            }
            () = cloned_rt.load_components() => {}
        }

        runtime_ready_check(&rt).await;

        wait_for_query_rows(&rt, "SELECT * FROM acc_line LIMIT 10", 10).await?;

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

/// `trim(col)` resolves to `DataFusion`'s `btrim` UDF and federates under that
/// canonical name, which `MySQL` has no function for — the query fails remotely
/// with `FUNCTION <db>.btrim does not exist` (the `MySQL` arm of issue #13794).
/// `MySQL` cannot be given a rewrite: its `TRIM` has no two-argument form, and
/// `TRIM(BOTH chars FROM str)` strips `chars` as a substring where `btrim`
/// strips any character in it, so a rewrite would trade a failed query for
/// wrong rows. `btrim` is deny-listed instead and evaluates locally.
///
/// Covers **both** registration paths in one container because the deny-list is
/// installed at two independent call sites — `MySQLTableFactory` in the dataset
/// connector, and again in `catalogconnector::mysql` — and nothing ties them
/// together: `with_function_support` is an optional builder step, so omitting it
/// at either site compiles and degrades only against a live `MySQL`. The deny-list
/// unit tests exercise the builder rather than the wiring, so they pass with
/// either call site's install removed; only this test fails.
#[tokio::test]
async fn mysql_btrim_evaluates_locally_on_both_registration_paths() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container = start_mysql_docker_container(MYSQL_PORT3)
                .await
                .map_err(|e| {
                    tracing::error!("start_mysql_docker_container: {e}");
                    e.to_string()
                })?;

            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_trim_table(MYSQL_PORT3)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| e.to_string())?;

            let mut catalog = Catalog::new("mysql".to_string(), "mycat".to_string());
            catalog.params = Some(spicepod::param::Params::from_string_map(
                mysql_connection_params(MYSQL_PORT3),
            ));

            let app = AppBuilder::new("mysql_btrim")
                .with_dataset(make_mysql_dataset("trim_src", "myds", MYSQL_PORT3, false))
                .with_catalog(catalog)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    return Err("Timed out waiting for components to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let run = |query: String| {
                let rt = rt.clone();
                async move {
                    rt.datafusion()
                        .query_builder(&query)
                        .build()
                        .run()
                        .await
                        .map_err(|e| format!("`{query}` failed: {e}"))?
                        .data
                        .try_collect::<Vec<RecordBatch>>()
                        .await
                        .map_err(|e| format!("`{query}` collection failed: {e}"))
                }
            };

            // `myds` is the dataset path, `mycat.mysqldb.trim_src` the catalog
            // path. Both must answer, and answer identically: an un-denied
            // `btrim` makes every one of these fail outright rather than
            // returning a row.
            for table in ["myds", "mycat.mysqldb.trim_src"] {
                for projection in [
                    "trim(name)",
                    "trim(name, 'x')",
                    "btrim(name)",
                    "trim(name, cast(null as varchar))",
                    "length(trim(name))",
                ] {
                    let rows = run(format!(
                        "SELECT {projection} AS v FROM {table} ORDER BY id"
                    ))
                    .await?;
                    assert_eq!(
                        rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        6,
                        "`{projection}` on `{table}` returned the wrong number of rows"
                    );
                }

                // A filter too: filter expressions take their own path through
                // the deny-list, and a pushed-down `WHERE btrim(...)` was the
                // other half of the reported failure.
                let filtered =
                    run(format!("SELECT id FROM {table} WHERE trim(name) = 'alpha'")).await?;
                assert_eq!(
                    filtered.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    1,
                    "a filter on trim(name) over `{table}` should match exactly one row"
                );

                // And the mechanism, not just the outcome: `btrim` must be
                // absent from the SQL sent to MySQL and present above the scan.
                // The first assertion alone would also hold if the scan stopped
                // federating; the second alone would hold if MySQL grew a
                // `btrim`.
                let plan = run(format!("EXPLAIN SELECT trim(name) AS v FROM {table}")).await?;
                let plan = arrow::util::pretty::pretty_format_batches(&plan)
                    .map_err(|e| format!("formatting the plan failed: {e}"))?
                    .to_string();
                let base_sql = plan
                    .split_once("VirtualExecutionPlan name=mysql")
                    .and_then(|(_, rest)| rest.split_once("base_sql="))
                    .map(|(_, sql)| sql)
                    .ok_or_else(|| format!("no MySQL scan in the plan for `{table}`:\n{plan}"))?;
                assert!(
                    !base_sql.contains("btrim("),
                    "btrim must not reach MySQL for `{table}`; the pushed-down SQL was:\n{base_sql}"
                );
                assert!(
                    plan.contains("btrim("),
                    "btrim should be evaluated locally above the scan for `{table}`; plan was:\n{plan}"
                );
            }

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}

/// The connection parameters `make_mysql_dataset` uses, for the catalog
/// component — which takes the same set but as its own `Params`.
fn mysql_connection_params(port: u16) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("mysql_host".to_string(), "localhost".to_string()),
        ("mysql_tcp_port".to_string(), port.to_string()),
        ("mysql_user".to_string(), "root".to_string()),
        (
            "mysql_pass".to_string(),
            crate::mysql::common::MYSQL_ROOT_PASSWORD.to_string(),
        ),
        ("mysql_db".to_string(), "mysqldb".to_string()),
        ("mysql_sslmode".to_string(), "disabled".to_string()),
    ])
}

/// A six-row table whose values make `trim` observable: padded with spaces,
/// padded with `x` so the two-argument character-set form has something to
/// strip, padded on one side only, and three padded with Unicode space
/// separators. Every assertion that counts rows expects all six.
#[instrument]
async fn init_trim_table(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS trim_src", Params::Empty)
        .await?;
    let _: Vec<Row> = conn
        .exec(
            "CREATE TABLE trim_src (id INT, name VARCHAR(64))",
            Params::Empty,
        )
        .await?;
    let _: Vec<Row> = conn
        .exec(
            // Rows 4-6 are padded with Unicode space *separators*, which
            // `btrim` does not strip. Denying the call keeps evaluation local so
            // they cannot diverge; the fixture pins that, and would catch a
            // future rewrite that federated them to a wider remote `trim`.
            "INSERT INTO trim_src VALUES (1, '  alpha  '), (2, 'xxbetaxx'), (3, '  gamma'), \
             (4, '\u{a0}nbsp\u{a0}'), (5, '\u{2003}emsp\u{2003}'), (6, '\u{3000}ideo\u{3000}')",
            Params::Empty,
        )
        .await?;
    Ok(())
}
