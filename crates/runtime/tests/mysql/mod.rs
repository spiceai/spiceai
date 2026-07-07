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

use common::{get_mysql_conn, make_mysql_dataset, start_mysql_docker_container};
use mysql_async::prelude::Queryable;

use spicepod::component::dataset::Dataset;
use spicepod::param::ParamValue;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};

#[cfg(feature = "duckdb")]
mod comments;
pub mod common;
mod federation;

use super::*;
use app::AppBuilder;
use mysql_async::{Params, Row};
use runtime::Runtime;
use tracing::instrument;

const MYSQL_PORT1: u16 = 13316;
const MYSQL_PORT2: u16 = 13317;
const MYSQL_PORT3: u16 = 13318;
const MYSQL_PORT4: u16 = 13319;

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
  id SERIAL PRIMARY KEY,
  col_bit BIT(1),
  col_tiny TINYINT,
  col_short SMALLINT,
  col_long INT,
  col_longlong BIGINT,
  col_float FLOAT,
  col_double DOUBLE,
  col_timestamp TIMESTAMP,
  col_date DATE,
  col_time TIME,
  col_blob BLOB,
  col_varchar VARCHAR(255),
  col_string TEXT,
  col_var_string VARCHAR(255),
  col_decimal DECIMAL(10, 2),
  col_unsigned_int INT UNSIGNED,
  col_char CHAR(3),
  col_set SET('apple', 'banana', 'cherry'),
  col_json JSON
);",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO test (
  col_bit,
  col_tiny,
  col_short,
  col_long,
  col_longlong,
  col_float,
  col_double,
  col_timestamp,
  col_date,
  col_time,
  col_blob,
  col_varchar,
  col_string,
  col_var_string,
  col_decimal,
  col_unsigned_int,
  col_char,
  col_set,
  col_json
) VALUES (
  1,
  1,
  1,
  1,
  1,
  1.1,
  1.1,
  '2019-01-01 00:00:00',
  '2019-01-01',
  '12:34:56',
  'blob',
  'varchar',
  'string',
  'var_string',
  1.11,
  10,
  'USA',
  'apple,banana',
  '{\"name\": \"John\", \"age\": 30, \"is_active\": true, \"balance\": 1234.56}'
);",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO test (
  col_bit,
  col_tiny,
  col_short,
  col_long,
  col_longlong,
  col_float,
  col_double,
  col_timestamp,
  col_date,
  col_time,
  col_blob,
  col_varchar,
  col_string,
  col_var_string,
  col_decimal,
  col_unsigned_int,
  col_char,
  col_set,
  col_json
) VALUES (
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
  NULL,
  NULL,
  NULL
);",
            Params::Empty,
        )
        .await?;

    Ok(())
}

#[instrument]
async fn init_mysql_utf8mb4_db(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;

    tracing::debug!("DROP TABLE IF EXISTS test_utf8mb4");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS test_utf8mb4", Params::Empty)
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "
CREATE TABLE test_utf8mb4 (
  id SERIAL PRIMARY KEY,
  col_text_utf8mb4 TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  col_varchar_utf8mb4 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  col_normal_text TEXT
);",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO test_utf8mb4 (
  col_text_utf8mb4,
  col_varchar_utf8mb4,
  col_normal_text
) VALUES (
  '🚀 This text contains UTF8MB4 characters that are not in UTF8MB3 😊',
  '🦄 Another UTF8MB4 string with emojis 🎉',
  'Regular text with no special characters'
);",
            Params::Empty,
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn mysql_integration_test() -> Result<(), String> {
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
            let app = AppBuilder::new("mysql_integration_test")
                .with_dataset(make_mysql_dataset("test", "test", MYSQL_PORT1, false))
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

            let queries: QueryTests = vec![(
                "SELECT * FROM test",
                "select",
                Some(Box::new(|result_batches| {
                    for batch in &result_batches {
                        assert_eq!(batch.num_columns(), 20, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("MySQL Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("mysql_integration_test_select"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mysql_integration_test_{snapshot_suffix}"),
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
async fn mysql_character_set_results_test() -> Result<(), String> {
    type QueryTests<'a> = Vec<(&'a str, &'a str, Option<Box<ValidateFn>>)>;
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mysql_docker_container(MYSQL_PORT2)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mysql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mysql_utf8mb4_db(MYSQL_PORT2)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MySQL database: {e}");
                e.to_string()
            })?;

            let app = AppBuilder::new("mysql_character_set_results_test")
                .with_dataset(make_mysql_dataset(
                    "test_utf8mb4",
                    "test_default",
                    MYSQL_PORT2,
                    false,
                ))
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

            let queries: QueryTests = vec![(
                "SELECT * FROM test_default",
                "character_set_results_default",
                Some(Box::new(|result_batches| {
                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");

                    insta::with_settings!({
                        description => format!("MySQL Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("character_set_results_default"), results);
                    });
                })),
            )];

            for (query, snapshot_suffix, validate_result) in queries {
                run_query_and_check_results(
                    &mut rt,
                    &format!("mysql_integration_test_{snapshot_suffix}"),
                    query,
                    false,
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
async fn mysql_timezone_test() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mysql_docker_container(MYSQL_PORT3)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mysql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");

            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mysql_tz_test_db(MYSQL_PORT3)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MySQL timezone database: {e}");
                e.to_string()
            })?;

            let mut ds_system = make_mysql_dataset("tz_test", "tz_system_tbl", MYSQL_PORT3, false);
            set_dataset_time_zone(&mut ds_system, "system")?;

            let mut ds_custom = make_mysql_dataset("tz_test", "tz_custom_tbl", MYSQL_PORT3, false);
            set_dataset_time_zone(&mut ds_custom, "+02:00")?;

            let app = AppBuilder::new("mysql_timezone_test")
                .with_dataset(ds_system)
                .with_dataset(ds_custom)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_query_and_check_results(
                &mut rt,
                "mysql_timezone_test",
                "SELECT * FROM tz_system_tbl ORDER BY id",
                false,
                Some(Box::new(
                    |result_batches: Vec<arrow::array::RecordBatch>| {
                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");

                        insta::assert_snapshot!("system_time_zone", results);
                    },
                )),
            )
            .await?;

            run_query_and_check_results(
                &mut rt,
                "mysql_timezone_test",
                "SELECT * FROM tz_custom_tbl ORDER BY id",
                false,
                Some(Box::new(
                    |result_batches: Vec<arrow::array::RecordBatch>| {
                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");

                        insta::assert_snapshot!("custom_time_zone", results);
                    },
                )),
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}

#[instrument]
async fn init_mysql_tz_test_db(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;

    // Set timezone to UTC
    conn.query_drop("SET time_zone = '+00:00'").await?;

    tracing::debug!("DROP TABLE IF EXISTS tz_test");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS tz_test", Params::Empty)
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "CREATE TABLE tz_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                ts TIMESTAMP
            )",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO tz_test (ts) VALUES 
                ('2024-06-01 08:00:00'),
                ('2024-06-01 12:00:00'),
                ('2024-06-01 16:00:00')",
            Params::Empty,
        )
        .await?;

    Ok(())
}

fn set_dataset_time_zone(ds: &mut Dataset, tz: &str) -> Result<(), String> {
    let Some(params) = ds.params.as_mut() else {
        return Err("Dataset params are missing".to_string());
    };
    params.data.insert(
        "mysql_time_zone".to_string(),
        ParamValue::String(tz.to_string()),
    );
    Ok(())
}

fn set_dataset_zero_date_behavior(ds: &mut Dataset, behavior: &str) -> Result<(), String> {
    let Some(params) = ds.params.as_mut() else {
        return Err("Dataset params are missing".to_string());
    };
    params.data.insert(
        "mysql_zero_date_behavior".to_string(),
        ParamValue::String(behavior.to_string()),
    );
    Ok(())
}

#[instrument]
async fn init_mysql_zero_date_db(port: u16) -> Result<(), anyhow::Error> {
    let pool = get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;

    // Allow inserting MySQL's zero-date sentinel ('0000-00-00' / '0000-00-00 00:00:00').
    // Real-world Aurora/MySQL deployments commonly run with a permissive sql_mode that
    // allows zero dates; we replicate that here on the *server* (not just the session)
    // so the zero-date row survives in the table for the connector to read back.
    conn.query_drop("SET GLOBAL sql_mode = ''").await?;
    conn.query_drop("SET SESSION sql_mode = ''").await?;

    tracing::debug!("DROP TABLE IF EXISTS zero_date_test");
    let _: Vec<Row> = conn
        .exec("DROP TABLE IF EXISTS zero_date_test", Params::Empty)
        .await?;

    // Mirrors the customer-reported schema: NOT NULL date/timestamp columns with
    // the literal zero-date as their DEFAULT, plus an unrelated NOT NULL non-temporal
    // column to verify that nullability for non-temporal columns is *not* widened.
    let _: Vec<Row> = conn
        .exec(
            "CREATE TABLE zero_date_test (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                date_created TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
                date_modified DATE NOT NULL DEFAULT '0000-00-00'
            )",
            Params::Empty,
        )
        .await?;

    let _: Vec<Row> = conn
        .exec(
            "INSERT INTO zero_date_test (id, name, date_created, date_modified) VALUES
                (1, 'alice', '2024-01-15 10:00:00', '2024-01-15'),
                (2, 'bob',   '0000-00-00 00:00:00', '0000-00-00'),
                (3, 'carol', '2024-03-20 14:30:00', '2024-03-20')",
            Params::Empty,
        )
        .await?;

    Ok(())
}

/// Validates the new `mysql_zero_date_behavior` connector parameter for both modes.
///
/// Regression coverage for the customer-reported failure on Aurora `MySQL` tables that have
/// `TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00'` columns containing zero-date rows:
///
/// * Default (`null`) — zero dates coerce to Arrow NULL; the schema reports the temporal
///   columns as nullable so `RecordBatch` validation passes.
/// * `error` — the scan fails with a clear, column-named error referencing the parameter.
/// * `error` + a predicate that filters out the zero-date row succeeds (predicate pushdown
///   into `MySQL` means the bad row never reaches the row decoder).
#[tokio::test]
async fn mysql_zero_date_test() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_mysql_docker_container(MYSQL_PORT4)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mysql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");

            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mysql_zero_date_db(MYSQL_PORT4)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MySQL zero-date database: {e}");
                e.to_string()
            })?;

            // Two datasets pointed at the same underlying MySQL table, distinguished only
            // by the `mysql_zero_date_behavior` param. We reuse the same source table to
            // guarantee both modes see byte-identical row data.
            let ds_null = make_mysql_dataset("zero_date_test", "zd_null", MYSQL_PORT4, false);

            let mut ds_error = make_mysql_dataset("zero_date_test", "zd_error", MYSQL_PORT4, false);
            set_dataset_zero_date_behavior(&mut ds_error, "error")?;

            let app = AppBuilder::new("mysql_zero_date_test")
                .with_dataset(ds_null)
                .with_dataset(ds_error)
                .build();

            configure_test_datafusion();
            let mut rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // (a) Default `null` mode: scan succeeds; zero-date row decoded as NULLs;
            //     temporal columns reported as nullable (snapshot captures the schema +
            //     row payload).
            run_query_and_check_results(
                &mut rt,
                "mysql_zero_date_test",
                "SELECT * FROM zd_null ORDER BY id",
                false,
                Some(Box::new(
                    |result_batches: Vec<arrow::array::RecordBatch>| {
                        assert_eq!(
                            result_batches
                                .iter()
                                .map(arrow::array::RecordBatch::num_rows)
                                .sum::<usize>(),
                            3,
                            "null mode should return all 3 rows"
                        );
                        // Sanity-check that temporal columns were widened to nullable so
                        // the bad row could be returned at all (this is the actual fix).
                        let schema = result_batches[0].schema();
                        assert!(
                            schema
                                .field_with_name("date_created")
                                .expect("date_created field exists in result schema")
                                .is_nullable(),
                            "date_created should be nullable in null mode"
                        );
                        assert!(
                            schema
                                .field_with_name("date_modified")
                                .expect("date_modified field exists in result schema")
                                .is_nullable(),
                            "date_modified should be nullable in null mode"
                        );
                        assert!(
                            !schema
                                .field_with_name("name")
                                .expect("name field exists in result schema")
                                .is_nullable(),
                            "non-temporal NOT NULL columns must NOT be widened"
                        );

                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");
                        insta::with_settings!({
                            description => "null mode: zero dates coerced to NULL, schema widened",
                            omit_expression => true,
                            snapshot_path => "../snapshots"
                        }, {
                            insta::assert_snapshot!("mysql_zero_date_test_null", results);
                        });
                    },
                )),
            )
            .await?;

            // (b) `error` mode: scan must fail with the parameterized error message,
            //     name the offending column, and reference the parameter so an operator
            //     can act on it without reading source.
            let error_query = "SELECT * FROM zd_error ORDER BY id";
            let stream = rt
                .datafusion()
                .query_builder(error_query)
                .build()
                .run()
                .await
                .map_err(|e| format!("plan construction unexpectedly failed: {e}"))?
                .data;
            let err_msg = stream
                .try_collect::<Vec<arrow::array::RecordBatch>>()
                .await
                .err()
                .map(|e| e.to_string())
                .ok_or_else(|| {
                    "error mode query unexpectedly succeeded; expected ZeroDateEncountered"
                        .to_string()
                })?;

            assert!(
                err_msg.contains("zero date"),
                "error message should mention 'zero date'; got: {err_msg}"
            );
            assert!(
                err_msg.contains("date_created"),
                "error message should name the offending column 'date_created'; got: {err_msg}"
            );
            assert!(
                err_msg.contains("mysql_zero_date_behavior"),
                "error message should reference the parameter name; got: {err_msg}"
            );

            // (c) `error` mode + predicate that excludes the zero-date row. Predicate
            //     pushdown (verified separately by federation tests) means MySQL filters
            //     id=2 server-side, so the row decoder never sees the bad row and the
            //     query succeeds.
            run_query_and_check_results(
                &mut rt,
                "mysql_zero_date_test",
                "SELECT * FROM zd_error WHERE id != 2 ORDER BY id",
                false,
                Some(Box::new(
                    |result_batches: Vec<arrow::array::RecordBatch>| {
                        assert_eq!(
                            result_batches
                                .iter()
                                .map(arrow::array::RecordBatch::num_rows)
                                .sum::<usize>(),
                            2,
                            "error-mode + predicate-pushdown should return the 2 non-zero rows"
                        );
                        // In error mode, the schema must honor source NOT NULL exactly.
                        let schema = result_batches[0].schema();
                        assert!(
                            !schema
                                .field_with_name("date_created")
                                .expect("date_created field exists in result schema")
                                .is_nullable(),
                            "date_created should NOT be widened in error mode"
                        );
                        assert!(
                            !schema
                                .field_with_name("date_modified")
                                .expect("date_modified field exists in result schema")
                                .is_nullable(),
                            "date_modified should NOT be widened in error mode"
                        );

                        let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                            .expect("should pretty print result batch");
                        insta::with_settings!({
                            description => "error mode + predicate skipping zero-date row",
                            omit_expression => true,
                            snapshot_path => "../snapshots"
                        }, {
                            insta::assert_snapshot!("mysql_zero_date_test_error_filtered", results);
                        });
                    },
                )),
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}
