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

use crate::utils::wait_until_true;
use app::AppBuilder;

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::{auth::EndpointAuth, config::Config, podswatcher::PodsWatcher};
use spicepod::acceleration::Acceleration;
use spicepod::component::dataset::Dataset;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

fn get_dataset(from: &str, name: &str, cron: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_cron: Some(cron.to_string()),
        ..Acceleration::default()
    });
    dataset
}

const NAMES_CSV: &str = include_str!("data/names.csv");

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_cron_schedule_creates() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            std::fs::write("./test_cron_file.csv", NAMES_CSV).expect("write file");

            let app = AppBuilder::new("test_cron_schedule_creates")
                .with_dataset(get_dataset(
                    "file:test_cron_file.csv",
                    "names",
                    "*/30 * * * * *", // every 30 seconds
                ))
                .build();

            let rt = Arc::new(
                Runtime::builder()
                    .with_app(app)
                    .with_datafusion_configuration_fn(configure_test_datafusion)
                    .build()
                    .await,
            );

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // validate that a scheduler exists in the runtime
            let schedulers_lock = Arc::clone(&rt).schedulers();
            let schedulers = schedulers_lock.read().await;

            // there should only be one `refresh_scheduler` with one schedule for the configured dataset
            assert!(
                schedulers.len() == 1,
                "Expected exactly one scheduler, found: {}",
                schedulers.len()
            );
            let refresh_scheduler = schedulers
                .get("refresh_scheduler")
                .expect("Expected a refresh scheduler to be present");
            let mut rt_schedules = refresh_scheduler.schedules().await;
            assert!(
                rt_schedules.len() == 1,
                "Expected exactly one schedule, found: {}",
                rt_schedules.len()
            );
            let schedule = rt_schedules
                .pop()
                .expect("Expected a schedule to be present");
            assert_eq!(
                schedule.name(),
                "names".into(),
                "Expected schedule name to match dataset name"
            );

            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM names ORDER BY id")
                .build()
                .run()
                .await
                .expect("query is successful")
                .data
                .try_collect()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n";
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_cron_file.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait 30 seconds for at least one run of the cron job
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            let result: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT * FROM names ORDER BY id")
                .build()
                .run()
                .await
                .expect("query is successful")
                .data
                .try_collect()
                .await
                .expect("collects results");

            let pretty = arrow::util::pretty::pretty_format_batches(&result)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            std::fs::remove_file("./test_cron_file.csv").expect("remove file");

            Ok(())
        })
        .await
}

fn get_test_dir() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_default()
        .join("cron_spicepod_test")
}

fn write_spicepod_yaml(content: &str) -> Result<(), anyhow::Error> {
    let spicepod_file_path = get_test_dir().join("spicepod.yaml");
    std::fs::write(spicepod_file_path, content)?;
    Ok(())
}

static YAML_CONTENT_BEFORE: &str = "version: v1
kind: Spicepod
name: cron_reload_integration

datasets:
  - from: https://public-data.spiceai.org/decimal.parquet
    name: decimal
    params:
      file_format: parquet
    acceleration:
      enabled: true
      refresh_cron: \"*/30 * * * * *\" # every 30 seconds
";

static YAML_CONTENT_AFTER: &str = "version: v1
kind: Spicepod
name: cron_reload_integration

datasets:
  - from: https://public-data.spiceai.org/decimal.parquet
    name: decimal
    params:
      file_format: parquet
    acceleration:
      enabled: true
      refresh_cron: \"* * * * * *\" # every minute
";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cron_reload() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let spicepod_dir = get_test_dir();
            std::fs::create_dir_all(&spicepod_dir).expect("Failed to create spicepod directory");

            write_spicepod_yaml(YAML_CONTENT_BEFORE)?;

            let app = AppBuilder::build_from_filesystem_path(spicepod_dir.clone())
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

            // validate that a scheduler exists in the runtime
            let schedulers_lock = Arc::clone(&rt).schedulers();
            let schedulers = schedulers_lock.read().await;

            // there should only be one `refresh_scheduler` with one schedule for the configured dataset
            assert!(
                schedulers.len() == 1,
                "Expected exactly one scheduler, found: {}",
                schedulers.len()
            );
            let refresh_scheduler = schedulers
                .get("refresh_scheduler")
                .expect("Expected a refresh scheduler to be present");
            let mut rt_schedules = refresh_scheduler.schedules().await;
            assert!(
                rt_schedules.len() == 1,
                "Expected exactly one schedule, found: {}",
                rt_schedules.len()
            );
            let schedule = rt_schedules
                .pop()
                .expect("Expected a schedule to be present");
            assert_eq!(
                schedule.name(),
                "decimal".into(),
                "Expected schedule name to match dataset name"
            );

            drop(schedulers);

            // Update the spicepod.yaml file to add a new dataset
            write_spicepod_yaml(YAML_CONTENT_AFTER)?;

            // Wait for the runtime to reload the configuration
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            // there should still only be one scheduler, with one schedule
            let schedulers_lock = Arc::clone(&rt).schedulers();
            let schedulers = schedulers_lock.read().await;
            assert!(
                schedulers.len() == 1,
                "Expected exactly one scheduler, found: {}",
                schedulers.len()
            );
            let refresh_scheduler = schedulers
                .get("refresh_scheduler")
                .expect("Expected a refresh scheduler to be present");
            let mut rt_schedules = refresh_scheduler.schedules().await;
            assert!(
                rt_schedules.len() == 1,
                "Expected exactly one schedule, found: {}",
                rt_schedules.len()
            );
            let schedule = rt_schedules
                .pop()
                .expect("Expected a schedule to be present");
            assert_eq!(
                schedule.name(),
                "decimal".into(),
                "Expected schedule name to match dataset name"
            );

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            // Clean up
            std::fs::remove_dir_all(&spicepod_dir).expect("Failed to remove spicepod directory");

            Ok(())
        })
        .await
}
