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
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

use crate::init_tracing;
use crate::utils::test_request_context;

pub mod common;

mod federation;

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

#[tokio::test]
async fn mysql_integration_test() -> Result<(), String> {
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
