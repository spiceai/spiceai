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

use common::{make_mssql_dataset, start_mssql_docker_container, MSSQL_ROOT_PASSWORD};
use data_components::mssql::connection_manager::SqlServerConnectionManager;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

use crate::init_tracing;
use crate::utils::test_request_context;

pub mod common;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const MSSQL_DOCKER_CONTAINER: &str = "runtime-integration-test-types-mssql";
const MSSQL_PORT: u16 = 11433;

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

    test_request_context()
        .scope(async {
            let running_container =
                start_mssql_docker_container(MSSQL_DOCKER_CONTAINER, MSSQL_PORT)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_mssql_docker_container: {e}");
                        e.to_string()
                    })?;
            tracing::debug!("Container started");
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
            retry(retry_strategy, || async {
                init_mssql_db(MSSQL_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize MSSQL database: {e}");
                e.to_string()
            })?;
            let app = AppBuilder::new("mssql_integration_test")
                .with_dataset(make_mssql_dataset("test", "test", MSSQL_PORT))
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
                        assert_eq!(batch.num_columns(), 18, "num_cols: {}", batch.num_columns());
                        assert_eq!(batch.num_rows(), 2, "num_rows: {}", batch.num_rows());
                    }

                    // snapshot the values of the results
                    let results = arrow::util::pretty::pretty_format_batches(&result_batches)
                        .expect("should pretty print result batch");
                    insta::with_settings!({
                        description => format!("MSSQL Integration Test Results"),
                        omit_expression => true,
                        snapshot_path => "../snapshots"
                    }, {
                        insta::assert_snapshot!(format!("mssql_integration_test_select"), results);
                    });
                })),
            )];

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

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                e.to_string()
            })?;

            Ok(())
        })
        .await
}
