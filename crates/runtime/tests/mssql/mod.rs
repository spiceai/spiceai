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

use std::sync::Arc;

use common::{MSSQL_ROOT_PASSWORD, make_mssql_dataset, start_mssql_docker_container};
use data_components::mssql::connection_manager::SqlServerConnectionManager;

use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::utils::{register_test_connectors, test_request_context};

pub mod common;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const MSSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-types-mssql";

#[instrument]
async fn init_mssql_db(port: u16) -> Result<(), anyhow::Error> {
    let mut config = tiberius::Config::new();
    config.host("localhost");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::Off);
    config.authentication(tiberius::AuthMethod::sql_server("sa", MSSQL_ROOT_PASSWORD));

    let manager = SqlServerConnectionManager::create(config).await?;
    let mut connection = manager.get().await?;

    let _ = connection.execute("DROP TABLE IF EXISTS test", &[]).await?;

    let _ = connection
        .execute(
            "
    CREATE TABLE test (
      id UNIQUEIDENTIFIER PRIMARY KEY,
      col_bit BIT,
      col_tiny TINYINT,
      col_short SMALLINT,
      col_long INT,
      col_longlong BIGINT,
      col_float FLOAT,
      col_double REAL,
      col_timestamp TIMESTAMP,
      col_datetime DATETIME,
      col_date DATE,
      col_time TIME,
      col_blob BINARY(50),
      col_varchar VARCHAR(255),
      col_string TEXT,
      col_var_string VARCHAR(255),
      col_decimal DECIMAL(10, 2),
      col_char CHAR(3),
    );",
            &[],
        )
        .await?;

    let _ = connection
        .execute(
            "INSERT INTO test (
      id,
      col_bit,
      col_tiny,
      col_short,
      col_long,
      col_longlong,
      col_float,
      col_double,
      col_timestamp,
      col_datetime,
      col_date,
      col_time,
      col_blob,
      col_varchar,
      col_string,
      col_var_string,
      col_decimal,
      col_char
    ) VALUES (
      '913b78a6-34a4-462d-9900-90cb37388887',
      1,
      1,
      1,
      1,
      1,
      1.1,
      1.1,
      DEFAULT,
      CAST('2019-01-01 00:00:00' AS DATETIME),
      '2019-01-01',
      '12:34:56',
      CAST('blob' AS BINARY),
      'varchar',
      'string',
      'var_string',
      1.11,
      'USA'
    );",
            &[],
        )
        .await?;

    let _ = connection
        .execute(
            "INSERT INTO test (
      id,
      col_bit,
      col_tiny,
      col_short,
      col_long,
      col_longlong,
      col_float,
      col_double,
      col_timestamp,
      col_datetime,
      col_date,
      col_time,
      col_blob,
      col_varchar,
      col_string,
      col_var_string,
      col_decimal,
      col_char
    ) VALUES (
      'b9daf5da-adc2-4eca-b283-b565b442e646',
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    );",
            &[],
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn mssql_integration_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mssql_docker_container(MSSQL_DOCKER_CONTAINER)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mssql_docker_container: {e}");
                        e.to_string()
                    })?;
            let port = running_container.host_port(1433).map_err(|e| e.to_string())?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mssql_db(port)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MSSQL database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mssql_integration_test")
                .with_dataset(make_mssql_dataset("test", "test", port))
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
                    "SELECT * FROM test",
                    "select",
                    Some(Box::new(|result_batches| {
                        for batch in &result_batches {
                            assert_eq!(batch.num_columns(), 18, "num_cols: {}", batch.num_columns());
                            assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                        }

                        // snapshot the values of the results
                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");
                        insta::with_settings!({
                            description => "MSSQL Integration Test Results",
                            omit_expression => true,
                            snapshot_path => "../snapshots"
                        }, {
                            insta::assert_snapshot!("mssql_integration_test_select", results);
                        });
                    })),
                ),
                // Sort pushdown (NULLS LAST = native MSSQL behavior for DESC): Exact pushdown, no SortExec
                (
                    "SELECT col_long, col_varchar, col_decimal FROM test ORDER BY col_long DESC NULLS LAST LIMIT 2",
                    "sort_pushdown_nulls_last",
                    Some(Box::new(|result_batches| {
                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");
                        insta::with_settings!({
                            description => "MSSQL Sort Pushdown DESC NULLS LAST Results",
                            omit_expression => true,
                            snapshot_path => "../snapshots"
                        }, {
                            insta::assert_snapshot!("mssql_integration_test_sort_pushdown_nulls_last_result", results);
                        });
                    })),
                ),
                // Sort pushdown (NULLS FIRST = non-native for DESC): Inexact pushdown, SortExec stays for NULL ordering
                (
                    "SELECT col_long, col_varchar, col_decimal FROM test ORDER BY col_long DESC NULLS FIRST LIMIT 2",
                    "sort_pushdown_nulls_first",
                    Some(Box::new(|result_batches| {
                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");
                        insta::with_settings!({
                            description => "MSSQL Sort Pushdown DESC NULLS FIRST Results",
                            omit_expression => true,
                            snapshot_path => "../snapshots"
                        }, {
                            insta::assert_snapshot!("mssql_integration_test_sort_pushdown_nulls_first_result", results);
                        });
                    })),
                ),
            ];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mssql_integration_test_{snapshot_suffix}"),
                    query,
                    true,
                    validate_result,
                )
                .await?;
            }

            running_container.cleanup().await;

            Ok(())
        })
        .await
}

const MSSQL_FILTERS_DOCKER_CONTAINER: &str = "runtime-integration-test-filters-mssql";

/// `people` has columns named after T-SQL keywords and functions; `words` holds text that a
/// case-insensitive, pad-space collation compares differently from `DataFusion`, and `czech`
/// uses a collation that reads `ch` as one letter.
async fn init_filter_tables(port: u16) -> Result<(), anyhow::Error> {
    let mut config = tiberius::Config::new();
    config.host("localhost");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::Off);
    config.authentication(tiberius::AuthMethod::sql_server("sa", MSSQL_ROOT_PASSWORD));
    let manager = SqlServerConnectionManager::create(config).await?;
    let mut connection = manager.get().await?;
    for statement in [
        "DROP TABLE IF EXISTS people",
        "DROP TABLE IF EXISTS words",
        "DROP TABLE IF EXISTS czech",
        "CREATE TABLE people (id INT PRIMARY KEY, [user] NVARCHAR(50), [my col] INT, [order] INT, created DATE, r REAL)",
        "INSERT INTO people VALUES (1, 'alice', 10, 1, '2024-01-01', 0.1), (2, 'bob', 20, 2, '2024-01-02', 0.2), \
         (3, 'carol', 30, 3, '2024-01-03', 0.3), (4, 'dbo', 40, 4, '2024-01-04', 0.4)",
        "CREATE TABLE words (id INT PRIMARY KEY, s NVARCHAR(20))",
        "INSERT INTO words VALUES (1, 'a'), (2, 'A'), (3, 'a '), (4, 'b'), (5, '[x]'), (6, 'x'), (7, N'日本'), (8, '??'), \
         (9, N'😀ab'), (10, '50%'), (11, 'x\\' + CHAR(10) + 'y')",
        "CREATE TABLE czech (id INT PRIMARY KEY, s NVARCHAR(20) COLLATE Czech_CI_AS)",
        "INSERT INTO czech VALUES (1, 'chata'), (2, 'cesta')",
    ] {
        let _ = connection.execute(statement, &[]).await?;
    }
    Ok(())
}

async fn query_column(rt: &Runtime, sql: &str) -> Result<Vec<String>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("{sql}: {e}"))?;
    let batches: Vec<arrow::array::RecordBatch> = futures::TryStreamExt::try_collect(result.data)
        .await
        .map_err(|e| format!("{sql}: {e}"))?;
    let mut values = Vec::new();
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8)
            .map_err(|e| e.to_string())?;
        let column = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("not a string column")?;
        values.extend(column.iter().map(|v| v.unwrap_or("NULL").to_string()));
    }
    Ok(values)
}

#[tokio::test]
async fn mssql_filters_return_the_rows_their_condition_selects() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container = start_mssql_docker_container(MSSQL_FILTERS_DOCKER_CONTAINER)
                .await
                .map_err(|e| e.to_string())?;
            let port = running_container
                .host_port(1433)
                .map_err(|e| e.to_string())?;
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_filter_tables(port)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| e.to_string())?;

            let app = AppBuilder::new("mssql_filters_test")
                .with_dataset(make_mssql_dataset("people", "people", port))
                .with_dataset(make_mssql_dataset("words", "words", port))
                .with_dataset(make_mssql_dataset("czech", "czech", port))
                .build();
            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            let cases: &[(&str, &[&str])] = &[
                // `user` unquoted is the T-SQL function that returns the session's user, `dbo`.
                (
                    r#"SELECT "user" FROM people ORDER BY id"#,
                    &["alice", "bob", "carol", "dbo"],
                ),
                (r#"SELECT id FROM people WHERE "user" = 'dbo'"#, &["4"]),
                (r#"SELECT id FROM people WHERE "user" = 'alice'"#, &["1"]),
                (r#"SELECT id FROM people WHERE "order" = 3"#, &["3"]),
                (r#"SELECT id FROM people WHERE "my col" = 20"#, &["2"]),
                ("SELECT * FROM people WHERE id = 1", &["1"]),
                (
                    "SELECT id FROM people WHERE created = DATE '2024-01-02'",
                    &["2"],
                ),
                ("SELECT id FROM people WHERE r = CAST(0.1 AS REAL)", &["1"]),
                (
                    "SELECT id FROM people WHERE r > CAST(0.1 AS REAL) ORDER BY id",
                    &["2", "3", "4"],
                ),
                // T-SQL LOG is the natural logarithm.
                ("SELECT id FROM people WHERE log(id) > 0.5", &["4"]),
                ("SELECT id FROM words WHERE s = 'A'", &["2"]),
                (
                    "SELECT id FROM words WHERE s <> 'a' ORDER BY id",
                    &["2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
                ),
                (
                    "SELECT id FROM words WHERE s > 'Z' ORDER BY id",
                    &["1", "3", "4", "5", "6", "7", "9", "11"],
                ),
                ("SELECT id FROM words WHERE s = '日本'", &["7"]),
                // In a T-SQL string, a backslash before a line break is removed with it.
                ("SELECT id FROM words WHERE s = 'x\\\ny'", &["11"]),
                // A T-SQL LIKE pattern reads `[x]` as a class, has no backslash escape and follows
                // the collation's letters; T-SQL SUBSTRING counts UTF-16 code units.
                ("SELECT id FROM words WHERE s LIKE '[x]%'", &["5"]),
                (r"SELECT id FROM words WHERE s LIKE '50\%'", &["10"]),
                (
                    "SELECT id FROM czech WHERE s LIKE 'c%' ORDER BY id",
                    &["1", "2"],
                ),
                ("SELECT id FROM words WHERE substr(s, 3, 1) = 'b'", &["9"]),
                (
                    "SELECT id FROM words WHERE id < 7 ORDER BY s NULLS FIRST LIMIT 2",
                    &["2", "5"],
                ),
            ];
            for (sql, expected) in cases {
                let got = query_column(&rt, sql).await?;
                assert_eq!(got, *expected, "{sql}");
            }

            running_container.cleanup().await;
            Ok(())
        })
        .await
}
