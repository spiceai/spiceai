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

use data_components::oracle::oracle_connector;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::oracle::common::{
    make_oracle_cloud_dataset, make_oracle_dataset, start_oracle_docker_container,
};
use crate::utils::{
    dataset_load_errors, register_test_connectors, runtime_ready_check,
    runtime_ready_check_with_timeout_err, test_request_context, verify_env_secret_exists,
};

pub mod common;

use super::*;
use app::AppBuilder;
use runtime::Runtime;
use tracing::instrument;

const ORACLE_DOCKER_CONTAINER: &str = "runtime-integration-test-oracle";
const ORACLE_PORT: u16 = 15210;

#[instrument]
async fn init_oracle_db(port: u16) -> Result<(), anyhow::Error> {
    let connector = oracle_connector::new(
        common::ORACLE_USERNAME,
        common::ORACLE_ROOT_PASSWORD,
        format!("//localhost:{ORACLE_PORT}/FREEPDB1"),
    );

    let client = connector.connect()?;

    // TIMESTAMP(0) WITH LOCAL TIME ZONE data types rely on the current time zone when inserting data.
    // Set a specific test time zone to ensure consistent results.
    let _ = client.execute("ALTER SESSION SET TIME_ZONE = '-07:00'", &[])?;

    // Oracle does not support DROP IF EXISTS syntax
    let _ = client.execute(
        r"
        BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE TEST_TABLE';
        EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
        END;
        ",
        &[],
    )?;

    // Create test table
    client.execute(
        r#"
            CREATE TABLE "TEST_TABLE" (
                "ID"                        NUMBER PRIMARY KEY,
                "VAL_NUMBER"                NUMBER(10, 2),
                "VAL_INTEGER"               NUMBER(18),
                "VAL_DECIMAL"               DECIMAL(18,4),
                "VAL_FLOAT"                 FLOAT(24),
                "VAL_DOUBLE"                FLOAT(53),
                "VAL_CHAR"                  CHAR(10),
                "VAL_NCHAR"                 NCHAR(10),
                "VAL_VARCHAR2"              VARCHAR2(50),
                "VAL_NVARCHAR2"             NVARCHAR2(50),
                "VAL_CLOB"                  CLOB,
                "VAL_NCLOB"                 NCLOB,
                "VAL_DATE"                  DATE,
                "VAL_TIMESTAMP"             TIMESTAMP(6),
                "VAL_TIMESTAMP_TZ"          TIMESTAMP(6) WITH TIME ZONE,
                "VAL_TIMESTAMP_LOCAL_TZ"    TIMESTAMP(0) WITH LOCAL TIME ZONE,
                "VAL_BINARY_FLOAT"          BINARY_FLOAT,
                "VAL_BINARY_DOUBLE"         BINARY_DOUBLE,
                "VAL_RAW"                   RAW(16),
                "VAL_BLOB"                  BLOB,
                "VAL_BOOL"                  CHAR(1)
            )
            "#,
        &[],
    )?;

    // Insert row 1
    client
        .execute(
            r#"
            INSERT INTO "TEST_TABLE" (
                "ID", "VAL_NUMBER", "VAL_INTEGER", "VAL_DECIMAL", "VAL_FLOAT", "VAL_DOUBLE",
                "VAL_CHAR", "VAL_NCHAR", "VAL_VARCHAR2", "VAL_NVARCHAR2",
                "VAL_CLOB", "VAL_NCLOB", "VAL_DATE", "VAL_TIMESTAMP", "VAL_TIMESTAMP_TZ", "VAL_TIMESTAMP_LOCAL_TZ",
                "VAL_BINARY_FLOAT", "VAL_BINARY_DOUBLE", "VAL_RAW", "VAL_BLOB",
                "VAL_BOOL"
            ) VALUES (
                1, 123.45, 123456789012345678, 555.1234, 3.14, 2.71828,
                'abc', N'def', 'ghi', N'jkl',
                -- VAL_DATE carries a time-of-day: an Oracle DATE is a datetime, and a date-only
                -- mapping would silently truncate this to midnight (regression test for #12096).
                'clobtext', N'nclobtext', TO_DATE('2024-06-27 14:32:11', 'YYYY-MM-DD HH24:MI:SS'), TIMESTAMP '2024-06-27 10:00:00', TIMESTAMP '2024-06-27 10:00:00 -07:00', TIMESTAMP '2024-06-27 10:00:00',
                1.23, 4.56, hextoraw('DEADBEEFDEADBEEFDEADBEEFDEADBEEF'), EMPTY_BLOB(),
                'Y'
            )
            "#,
            &[],
        )?;

    // Insert row 2
    client
        .execute(
            r#"
            INSERT INTO "TEST_TABLE" (
                "ID", "VAL_NUMBER", "VAL_INTEGER", "VAL_DECIMAL", "VAL_FLOAT", "VAL_DOUBLE",
                "VAL_CHAR", "VAL_NCHAR", "VAL_VARCHAR2", "VAL_NVARCHAR2",
                "VAL_CLOB", "VAL_NCLOB", "VAL_DATE", "VAL_TIMESTAMP", "VAL_TIMESTAMP_TZ", "VAL_TIMESTAMP_LOCAL_TZ",
                "VAL_BINARY_FLOAT", "VAL_BINARY_DOUBLE", "VAL_RAW", "VAL_BLOB",
                "VAL_BOOL"
            ) VALUES (
                2, 987.65, -999, 9999.4321, 2.71, 3.14159,
                'xyz', N'pqr', 'stu', N'vwx',
                'clobtext2', N'nclobtext2', DATE '2025-01-01', TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-01 00:00:00 +00:00', TIMESTAMP '2025-01-01 00:00:00',
                9.87, 6.54, hextoraw('11223344556677889900AABBCCDDEEFF'), EMPTY_BLOB(),
                'N'
            )
            "#,
            &[],
        )?;

    // Insert row 3 (NULLs)
    client
        .execute(
            r#"
            INSERT INTO "TEST_TABLE" (
                "ID", "VAL_NUMBER", "VAL_INTEGER", "VAL_DECIMAL", "VAL_FLOAT", "VAL_DOUBLE",
                "VAL_CHAR", "VAL_NCHAR", "VAL_VARCHAR2", "VAL_NVARCHAR2",
                "VAL_CLOB", "VAL_NCLOB", "VAL_DATE", "VAL_TIMESTAMP", "VAL_TIMESTAMP_TZ", "VAL_TIMESTAMP_LOCAL_TZ",
                "VAL_BINARY_FLOAT", "VAL_BINARY_DOUBLE", "VAL_RAW", "VAL_BLOB",
                "VAL_BOOL"
            ) VALUES (
                3, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, EMPTY_BLOB(),
                NULL
            )
            "#,
            &[],
        )?;

    // Commit
    client.execute("COMMIT", &[])?;

    Ok(())
}

#[tokio::test]
async fn oracle_test_direct_connection() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let running_container =
                start_oracle_docker_container(ORACLE_DOCKER_CONTAINER, ORACLE_PORT)
                    .await
                    .map_err(|e| {
                        tracing::error!("start_oracle_docker_container: {e}");
                        e
                    })?;
            tracing::debug!("Container started");

            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();
            retry(retry_strategy, || async {
                init_oracle_db(ORACLE_PORT)
                    .await
                    .map_err(RetryError::transient)
            })
            .await
            .map_err(|e| {
                tracing::error!("Failed to initialize Oracle database: {e}");
                e
            })?;

            let federated_ds = make_oracle_dataset("\"TEST_TABLE\"", "test_tbl", ORACLE_PORT);
            let mut accelerated_ds = make_oracle_dataset("\"TEST_TABLE\"", "test_tbl_accelerated", ORACLE_PORT);
            accelerated_ds.acceleration = Some(spicepod::acceleration::Acceleration::default());

            let app = AppBuilder::new("oracle_integration_test")
                .with_dataset(federated_ds)
                // Verify that accelerated table can be successfully loaded
                .with_dataset(accelerated_ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            run_and_snapshot_query(
                &rt,
                "describe test_tbl",
                "schema",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                r#"select 
                    "ID"::BIGINT as ID,
                    "VAL_NUMBER", 
                    "VAL_DECIMAL", 
                    "VAL_FLOAT", 
                    "VAL_DOUBLE", 
                    "VAL_INTEGER", 
                    "VAL_CHAR", 
                    "VAL_NCHAR", 
                    "VAL_VARCHAR2", 
                    "VAL_NVARCHAR2", 
                    "VAL_CLOB", 
                    "VAL_NCLOB", 
                    "VAL_DATE", 
                    "VAL_TIMESTAMP", 
                    "VAL_TIMESTAMP_TZ", 
                    "VAL_TIMESTAMP_LOCAL_TZ", 
                    "VAL_BINARY_FLOAT", 
                    "VAL_BINARY_DOUBLE", 
                    "VAL_RAW", 
                    "VAL_BLOB", 
                    "VAL_BOOL" 
                 from test_tbl"#,
                "data",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                r#"explain select "VAL_NUMBER", "VAL_DECIMAL", "VAL_FLOAT" from test_tbl where "VAL_NUMBER" = 123.45 AND "VAL_FLOAT" > 2.8 AND "VAL_INTEGER" >= 123456789012345678 AND "VAL_DECIMAL" < 10000 AND "VAL_VARCHAR2" = 'ghi' AND "VAL_BOOL" = 'Y' limit 1"#,
                "filters_pushdown_query_plan",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                r#"select "VAL_NUMBER", "VAL_DECIMAL", "VAL_FLOAT" from test_tbl where "VAL_NUMBER" = 123.45 AND "VAL_FLOAT" > 2.8 AND "VAL_INTEGER" >= 123456789012345678 AND "VAL_DECIMAL" < 10000 AND "VAL_VARCHAR2" = 'ghi' AND "VAL_BOOL" = 'Y' limit 1"#,
                "filters_pushdown_query_result",
            )
            .await?;

            // Sort pushdown: verify ORDER BY is pushed into the federated SQL and SortExec is removed
            run_and_snapshot_query(
                &rt,
                r#"explain select "ID", "VAL_NUMBER", "VAL_VARCHAR2" from test_tbl order by "VAL_NUMBER" desc limit 2"#,
                "sort_pushdown_plan",
            )
            .await?;

            run_and_snapshot_query(
                &rt,
                r#"select "ID", "VAL_NUMBER", "VAL_VARCHAR2" from test_tbl order by "VAL_NUMBER" desc limit 2"#,
                "sort_pushdown_result",
            )
            .await?;

            running_container.remove().await.map_err(|e| {
                tracing::error!("running_container.remove: {e}");
                 e
            })?;

            Ok(())
        })
        .await
}

/// The Oracle server errors that mean the test account's own credential cannot be used,
/// whatever the runtime does with it. Each is paired with the condition to report.
///
/// `ORA-28002` ("the password will expire within N days") is deliberately absent: it accompanies
/// a logon that *succeeded*, so reading it as unusable would skip a test that can still run.
const UNUSABLE_CREDENTIAL_ERRORS: [(&str, &str); 3] = [
    ("ORA-01017", "invalid username or password"),
    ("ORA-28000", "the account is locked"),
    ("ORA-28001", "the password has expired"),
];

/// Returns the credential condition `message` names, if it names one.
///
/// Only the conditions in [`UNUSABLE_CREDENTIAL_ERRORS`] skip the test. Every other failure,
/// a near-expiry warning included, has to keep failing.
fn unusable_oracle_credential(message: &str) -> Option<&'static str> {
    UNUSABLE_CREDENTIAL_ERRORS
        .iter()
        .find(|(code, _)| names_oracle_code(message, code))
        .map(|(_, condition)| *condition)
}

/// Whether `message` names `code` itself, rather than a longer number beginning with it.
///
/// The recorded status text is free-form, so the character after the code is checked: matching
/// `ORA-28001` inside `ORA-280015` would report an unusable credential for a different error.
fn names_oracle_code(message: &str, code: &str) -> bool {
    message.match_indices(code).any(|(index, _)| {
        !message[index + code.len()..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_digit())
    })
}

#[test]
fn expired_password_is_an_unusable_credential() {
    assert_eq!(
        unusable_oracle_credential(
            "Failed to connect to the Oracle Server. Failed to establish connection: \
             OCI Error: ORA-28001: the password has expired"
        ),
        Some("the password has expired")
    );
}

#[test]
fn a_locked_account_and_bad_credentials_are_unusable_credentials() {
    assert_eq!(
        unusable_oracle_credential("OCI Error: ORA-28000: the account is locked"),
        Some("the account is locked")
    );
    assert_eq!(
        unusable_oracle_credential("OCI Error: ORA-01017: invalid username/password; logon denied"),
        Some("invalid username or password")
    );
}

/// `ORA-28002` reports a password nearing expiry on a logon that *succeeded*. The test can still
/// run, so this must not be read as an unusable credential.
#[test]
fn a_password_nearing_expiry_is_not_an_unusable_credential() {
    assert_eq!(
        unusable_oracle_credential("OCI Error: ORA-28002: the password will expire within 7 days"),
        None
    );
}

/// A longer number beginning with a listed code is a different error.
#[test]
fn a_longer_code_sharing_a_prefix_is_not_an_unusable_credential() {
    assert_eq!(unusable_oracle_credential("OCI Error: ORA-280015"), None);
}

/// Every other failure has to keep failing: a connector regression must not be filed away as
/// somebody else's expired password.
#[test]
fn an_unrelated_failure_is_not_a_credential_problem() {
    assert_eq!(
        unusable_oracle_credential("ORA-12541: TNS:no listener"),
        None
    );
    assert_eq!(unusable_oracle_credential("Timed out waiting"), None);
    assert_eq!(unusable_oracle_credential(""), None);
}

#[tokio::test]
async fn oracle_test_cloud_mtls() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    for env_var in [
        "ORACLE_CLOUD_CONNECTION_STRING",
        "ORACLE_CLOUD_USERNAME",
        "ORACLE_CLOUD_PASSWORD",
        "ORACLE_CLOUD_WALLET_SSO_CERT",
    ] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    test_request_context()
        .scope(async {
            let ds = make_oracle_cloud_dataset("\"NATION\"", "nation");

            let app = AppBuilder::new("oracle_cloud_test")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            let loaded = tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => false,
                () = cloned_rt.load_components() => true,
            };

            let ready = loaded
                && runtime_ready_check_with_timeout_err(&rt, std::time::Duration::from_mins(2))
                    .await
                    .is_ok();

            // A dataset the connector could not authenticate never reports ready, and the reason
            // is only in the status the runtime recorded for it. Read that status, so an
            // unusable test credential is told apart from a connector that is actually broken.
            if !ready {
                let errors = dataset_load_errors(&rt);

                if let Some(condition) = errors
                    .iter()
                    .find_map(|(_, message)| unusable_oracle_credential(message))
                {
                    eprintln!(
                        "Skipping oracle_test_cloud_mtls: the ORACLE_CLOUD_* test credential is \
                         unusable ({condition}). Rotate the Oracle Cloud test account's password \
                         and update the CI secret to restore this coverage."
                    );
                    return Ok(());
                }

                let reported = errors
                    .iter()
                    .map(|(dataset, message)| format!("{dataset}: {message}"))
                    .collect::<Vec<_>>()
                    .join("; ");

                return Err(anyhow::Error::msg(format!(
                    "Timed out waiting for datasets to load ({})",
                    if reported.is_empty() {
                        "no dataset recorded an error"
                    } else {
                        &reported
                    }
                )));
            }

            run_and_snapshot_query(
                &rt,
                "select N_NAME, N_COMMENT from nation order by N_NAME limit 5",
                "oracle_cloud_query_result",
            )
            .await?;

            Ok(())
        })
        .await
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let data = query_result.data.try_collect::<Vec<_>>().await?;

    let formatted = arrow::util::pretty::pretty_format_batches(&data)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}
