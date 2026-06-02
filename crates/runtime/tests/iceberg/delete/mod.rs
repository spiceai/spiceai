/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};
use anyhow::Context;
use app::AppBuilder;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::sync::Arc;

/// Test that DELETE FROM works on an Iceberg table via equality delete files.
///
/// This test:
/// 1. Inserts 4 rows with a unique `batch_id` into a read-write Iceberg table
/// 2. Deletes 2 of them with a WHERE filter
/// 3. Verifies only the non-deleted rows remain
/// 4. Deletes the remaining rows
/// 5. Verifies no rows remain for this batch
#[tokio::test]
async fn iceberg_delete_from_table() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let dataset = make_iceberg_dataset("spice_write", "test_table", "test_table")?;

            let app = AppBuilder::new("iceberg-delete").with_dataset(dataset).build();

            configure_test_datafusion();

            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let batch_uuid = uuid::Uuid::new_v4().to_string();

            // Step 1: Insert 4 rows with distinct int_col values (1, 2, 3, 4)
            let insert_sql = format!(
                "INSERT INTO test_table \
                (batch_id, boolean_col, int_col, long_col, float_col, double_col, decimal_col, date_col, timestamp_col, binary_col) \
                VALUES \
                ('{batch_uuid}', TRUE,  1, 100, REAL '1.0', 1.0, DECIMAL '1.0000', DATE '2025-01-01', TIMESTAMP '2025-01-01 00:00:00', X'01'), \
                ('{batch_uuid}', TRUE,  2, 200, REAL '2.0', 2.0, DECIMAL '2.0000', DATE '2025-01-02', TIMESTAMP '2025-01-02 00:00:00', X'02'), \
                ('{batch_uuid}', FALSE, 3, 300, REAL '3.0', 3.0, DECIMAL '3.0000', DATE '2025-01-03', TIMESTAMP '2025-01-03 00:00:00', X'03'), \
                ('{batch_uuid}', FALSE, 4, 400, REAL '4.0', 4.0, DECIMAL '4.0000', DATE '2025-01-04', TIMESTAMP '2025-01-04 00:00:00', X'04');"
            );

            execute_query_and_validate_result(
                &rt,
                &insert_sql,
                "delete_insert_result",
            ).await?;

            // Verify all 4 rows exist
            let select_all_sql = format!(
                "SELECT int_col, boolean_col, long_col FROM test_table \
                WHERE batch_id = '{batch_uuid}' ORDER BY int_col"
            );
            execute_query_and_validate_result(
                &rt,
                &select_all_sql,
                "delete_all_rows_before_delete",
            ).await?;

            // Step 2: Delete rows where boolean_col = FALSE (rows with int_col 3 and 4)
            let delete_sql = format!(
                "DELETE FROM test_table WHERE batch_id = '{batch_uuid}' AND boolean_col = false"
            );
            execute_query_and_validate_result(
                &rt,
                &delete_sql,
                "delete_partial_result",
            ).await?;

            // Step 3: Verify only rows with int_col 1 and 2 remain
            execute_query_and_validate_result(
                &rt,
                &select_all_sql,
                "delete_rows_after_partial_delete",
            ).await?;

            // Step 4: Delete remaining rows
            let delete_all_sql = format!(
                "DELETE FROM test_table WHERE batch_id = '{batch_uuid}'"
            );
            execute_query_and_validate_result(
                &rt,
                &delete_all_sql,
                "delete_remaining_result",
            ).await?;

            // Step 5: Verify no rows remain for this batch
            execute_query_and_validate_result(
                &rt,
                &select_all_sql,
                "delete_rows_after_full_delete",
            ).await?;

            Ok(())
        })
        .await
}

/// Test that DELETE FROM with no matching rows succeeds with count=0.
#[tokio::test]
async fn iceberg_delete_no_matching_rows() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let dataset = make_iceberg_dataset("spice_write", "test_table", "test_table")?;

            let app = AppBuilder::new("iceberg-delete-noop")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();

            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Delete with a batch_id that doesn't exist — should return count=0
            let nonexistent_uuid = uuid::Uuid::new_v4().to_string();
            let delete_sql =
                format!("DELETE FROM test_table WHERE batch_id = '{nonexistent_uuid}'");
            execute_query_and_validate_result(&rt, &delete_sql, "delete_no_match_result").await?;

            Ok(())
        })
        .await
}

async fn execute_query_and_validate_result(
    rt: &Runtime,
    query: &str,
    snapshot_name: &str,
) -> Result<(), anyhow::Error> {
    let query = QueryBuilder::new(query, rt.datafusion()).build();

    let query_result = query
        .run()
        .await
        .map_err(|e| anyhow::Error::msg(format!("Failed to execute query: {e}")))?;

    let records = query_result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::Error::msg(format!("Failed to collect query results: {e}")))?;

    let pretty = arrow::util::pretty::pretty_format_batches(&records)
        .map_err(|e| anyhow::Error::msg(format!("Failed to format record batches: {e}")))?;

    insta::assert_snapshot!(snapshot_name, pretty);

    Ok(())
}

fn make_iceberg_dataset(
    namespace: &str,
    table: &str,
    name: &str,
) -> Result<Dataset, anyhow::Error> {
    let account_id =
        std::env::var("AWS_ICEBERG_ACCOUNT_ID").context("AWS_ICEBERG_ACCOUNT_ID is not set")?;

    let from = format!(
        "iceberg:https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/{account_id}/namespaces/{namespace}/tables/{table}"
    );
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(get_iceberg_params());
    dataset.access = spicepod::component::access::AccessMode::ReadWrite;
    Ok(dataset)
}

fn get_iceberg_params() -> Params {
    Params::from_string_map(
        vec![
            ("iceberg_s3_region".to_string(), "us-east-1".to_string()),
            (
                "iceberg_s3_access_key_id".to_string(),
                "${ env:AWS_ICEBERG_ACCESS_KEY_ID }".to_string(),
            ),
            (
                "iceberg_s3_secret_access_key".to_string(),
                "${ env:AWS_ICEBERG_SECRET_ACCESS_KEY }".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    )
}
