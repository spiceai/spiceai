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

#[tokio::test]
async fn iceberg_insert_into_existing_table() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let dataset = make_iceberg_dataset("spice_write", "test_table", "test_table")?;

            let app = AppBuilder::new("iceberg-write").with_dataset(dataset).build();

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

            // Generate a new UUID for this batch so we can query appended data
            let batch_uuid = uuid::Uuid::new_v4().to_string();
            let append_sql = format!(
                "INSERT INTO test_table \
                  (batch_id, boolean_col, int_col, long_col, float_col, double_col, decimal_col, date_col, timestamp_col, binary_col) \
                VALUES \
                  ('{batch_uuid}', TRUE,  1,  10000000001, REAL '1.5',  2.25, DECIMAL '12345.6789', DATE '2024-01-01', TIMESTAMP '2024-01-01 02:03:04', X'00FFAB'), \
                  ('{batch_uuid}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
            );

            execute_query_and_validate_result(
                &rt,
                &append_sql,
                "iceberg_insert_into_table_result",
            ).await?;

            // Select to validate appended rows. The batch_id is unique per test run, so we exclude it from snapshot validation.
            execute_query_and_validate_result(
                &rt,
                &format!(
                    "SELECT boolean_col, int_col, long_col, float_col, double_col, decimal_col, date_col, timestamp_col, binary_col FROM test_table WHERE batch_id = '{batch_uuid}'",
                ),
                "iceberg_insert_into_table_appended_rows",
            ).await?;

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
    dataset.params = Some(get_params());
    dataset.access = spicepod::component::dataset::AccessMode::ReadWrite;
    Ok(dataset)
}

fn get_params() -> Params {
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
