/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::{
    init_tracing, run_query_and_check_results,
    utils::{runtime_ready_check, test_request_context},
};
use arrow::record_batch::RecordBatch;
use runtime::Runtime;

#[tokio::test]
async fn test_aws_sdk_environment_resolution() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    ensure_aws_profile();

    test_request_context()
        .scope(async {
            let app = test_framework::app_utils::load_app_from_spicepod_str(include_str!(
                "./spicepods/aws_sdk_verify.yaml"
            ))
            .await
            .expect("Should load app from spicepod string");

            let mut rt = Runtime::builder().with_app(app).build().await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::new(rt.clone()).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let tables = [
                "public_taxi",
                "private_taxi",
                "region_delta",
                "region_databricks_delta",
                "iceberg_table",
                "iceberg_table_glue",
            ];

            for table in tables {
                run_query_and_check_results(
                    &mut rt,
                    &format!("SELECT * FROM {table} LIMIT 1"),
                    Some(validate_one_row),
                )
                .await?;
            }

            Ok(())
        })
        .await?;

    Ok(())
}

/// Checks if a `~/.aws/config` file exists, if so assume the profile has been configured.
///
/// If not, configure it by placing a `~/.aws/config` file with a `[default]` profile and add static
/// test credentials for that profile to the `~/.aws/credentials` file.
#[allow(clippy::expect_used)]
fn ensure_aws_profile() {
    let os_home = std::env::var("HOME").expect("HOME environment variable must be set");
    let config_file_str = format!("{os_home}/.aws/config");
    let config_file = std::path::Path::new(&config_file_str);
    let credentials_file_str = format!("{os_home}/.aws/credentials");
    let credentials_file = std::path::Path::new(&credentials_file_str);
    if config_file.exists() {
        // Assume the profile is configured
        tracing::info!(
            "AWS profile already configured at {config_file:?}, skipping configuration."
        );
        return;
    }

    let access_key_id = std::env::var("AWS_DATABRICKS_DELTA_ACCESS_KEY_ID")
        .expect("AWS_DATABRICKS_DELTA_ACCESS_KEY_ID must be set");
    let secret_access_key = std::env::var("AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY")
        .expect("AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY must be set");

    // Create the default profile with the credentials from AWS_DATABRICKS_DELTA_ACCESS_KEY_ID and AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY
    let config_content = "[default]
region = us-east-1
output = json
";
    let credentials_content = format!(
        "[default]
aws_access_key_id = {access_key_id}
aws_secret_access_key = {secret_access_key}
"
    );

    // Create the .aws directory if it doesn't exist
    std::fs::create_dir_all(std::path::Path::new(&format!("{os_home}/.aws")))
        .expect("Failed to create .aws directory");
    std::fs::write(config_file, config_content).expect("Failed to write config file");
    std::fs::write(credentials_file, credentials_content)
        .expect("Failed to write credentials file");
}

#[allow(clippy::needless_pass_by_value)]
fn validate_one_row(results: Vec<RecordBatch>) {
    assert_eq!(results.len(), 1, "Expected one row in the result");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Expected one row in the result batch");
}
