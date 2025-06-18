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

use std::{path::PathBuf, sync::Arc};

use app::AppBuilder;

use crate::utils::{run_query, wait_until_true};
use crate::{
    init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

use runtime::{Runtime, auth::EndpointAuth, config::Config, podswatcher::PodsWatcher};

// Define a static constant for the properly formatted YAML content
static YAML_CONTENT_BEFORE: &str = "version: v1
kind: Spicepod
name: pods_watcher_integration

datasets:
  - from: s3://spiceai-demo-datasets/taxi_trips/2024/
    name: taxi_trips_1
    params:
      file_format: parquet
";

static YAML_CONTENT_AFTER: &str = "version: v1
kind: Spicepod
name: pods_watcher_integration

datasets:
  - from: s3://spiceai-demo-datasets/taxi_trips/2024/
    name: taxi_trips_1
    params:
      file_format: parquet

  - from: s3://spiceai-demo-datasets/taxi_trips/2024/
    name: taxi_trips_2
    params:
      file_format: parquet
";

fn get_test_dir() -> PathBuf {
    std::env::current_dir().unwrap_or_default().join("spicepod")
}

fn write_spicepod_yaml(content: &str) -> Result<(), anyhow::Error> {
    let spicepod_file_path = get_test_dir().join("spicepod.yaml");
    std::fs::write(spicepod_file_path, content)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn podswatcher_integration_test() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let spicepod_dir = get_test_dir();
            std::fs::create_dir_all(&spicepod_dir).expect("Failed to create spicepod directory");

            write_spicepod_yaml(YAML_CONTENT_BEFORE)?;

            let app = AppBuilder::build_from_path(spicepod_dir.clone()).await
                .expect("Failed to build app");
            let pods_watcher = PodsWatcher::new(spicepod_dir.clone());

            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .with_pods_watcher(pods_watcher)
                    .build()
                    .await,
            );

            // Start server
            let api_config = Config::new();
            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            // Set a timeout for the test
            let cloned_rt = Arc::clone(&rt);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Connect to the server
            let http_client = reqwest::Client::builder().build()?;
            tracing::info!("Waiting for servers to start...");
            wait_until_true(std::time::Duration::from_secs(10), || async {
                http_client
                    .get("http://127.0.0.1:8090/ready".to_string())
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            runtime_ready_check(&rt).await;


            let results_before = run_query(
                &rt,
                 "select table_catalog, table_schema, table_name, table_type from information_schema.tables where table_schema == 'public' order by table_name;").await?;
            let pretty_before = arrow::util::pretty::pretty_format_batches(&results_before)
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("pods_watcher_assert_before", pretty_before);

            // Update the spicepod.yaml file to add a new dataset
            write_spicepod_yaml(YAML_CONTENT_AFTER)?;

            // Wait until the change is detected and processed
            let success = wait_until_true(std::time::Duration::from_secs(30), || async {
                let results_after = run_query(
                    &rt,
                    "select * from information_schema.tables where table_schema = 'public' and table_name = 'taxi_trips_2'",
                ).await.expect("Failed to run query");

                !results_after.is_empty()
            }).await;

            if !success {
                return Err(anyhow::anyhow!("Timed out waiting for pods watcher to detect changes"));
            }

            // Confirm that the new dataset is loaded
            let results_after = run_query(
                &rt,
                 "select table_catalog, table_schema, table_name, table_type from information_schema.tables where table_schema == 'public' order by table_name;").await?;
            let pretty_after = arrow::util::pretty::pretty_format_batches(&results_after)
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!("pods_watcher_assert_after", pretty_after);

            // Clean up
            std::fs::remove_dir_all(&spicepod_dir).expect("Failed to remove spicepod directory");

            Ok(())
        })
        .await
}
