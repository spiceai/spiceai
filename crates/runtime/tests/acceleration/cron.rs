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

use crate::utils::{time_till_second, wait_until_true};
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

#[allow(clippy::expect_used)]
async fn snapshot_names_from_runtime(name: &str, rt: &Arc<Runtime>, dataset_name: Option<&str>) {
    let result: Vec<RecordBatch> = rt
        .datafusion()
        .query_builder(
            format!(
                "SELECT id, name, age, city, score FROM {} ORDER BY id",
                dataset_name.unwrap_or("names")
            )
            .as_str(),
        )
        .build()
        .run()
        .await
        .expect("query is successful")
        .data
        .try_collect()
        .await
        .expect("collects results");

    let pretty = arrow::util::pretty::pretty_format_batches(&result)
        .map_err(|e| anyhow::Error::msg(e.to_string()))
        .expect("Should format batches");
    insta::assert_snapshot!(name, pretty);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_cron_schedule_creates() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            std::fs::write("./test_cron_schedule_creates.csv", NAMES_CSV).expect("write file");

            let app = AppBuilder::new("test_cron_schedule_creates")
                .with_dataset(get_dataset(
                    "file:test_cron_schedule_creates.csv",
                    "names",
                    "*/30 * * * * *", // every 30 seconds
                ))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

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

            snapshot_names_from_runtime("test_cron_schedule_creates_before", &rt, None).await;

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n";
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_cron_schedule_creates.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait for the next 30th second, and wait 10 seconds for the job to succeed
            tokio::time::sleep(time_till_second(30, Some(10))).await;

            snapshot_names_from_runtime("test_cron_schedule_creates_after", &rt, None).await;

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            std::fs::remove_file("./test_cron_schedule_creates.csv").expect("remove file");

            Ok(())
        })
        .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_multiple_cron_schedule_creates() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            const DATASET_COUNT: usize = 15;

            std::fs::write("./test_multiple_cron_schedule_creates.csv", NAMES_CSV)
                .expect("write file");

            let mut app = AppBuilder::new("test_multiple_cron_schedule_creates");

            for i in 1..=DATASET_COUNT {
                app = app.with_dataset(get_dataset(
                    "file:test_multiple_cron_schedule_creates.csv",
                    format!("names_{i}").as_str(),
                    "*/10 * * * * *", // every 10 seconds
                ));
            }

            let app = app.build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            // align schedule start to be at least a few seconds before the next 10th second
            tokio::time::sleep(time_till_second(10, Some(2))).await;

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
                rt_schedules.len() == DATASET_COUNT,
                "Expected exactly one schedule, found: {}",
                rt_schedules.len()
            );
            let dataset_names = (1..=DATASET_COUNT)
                .map(|i| format!("names_{i}").into())
                .collect::<Vec<_>>();
            while let Some(schedule) = rt_schedules.pop() {
                assert!(
                    dataset_names.contains(&schedule.name()),
                    "Expected schedule name to match dataset name"
                );
            }

            for dataset_name in dataset_names.clone() {
                snapshot_names_from_runtime(
                    &format!("test_multiple_cron_schedule_creates_before_{dataset_name}"),
                    &rt,
                    Some(&dataset_name),
                )
                .await;
            }

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n";
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_multiple_cron_schedule_creates.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait for the next 10th second, and wait 5 seconds for the job to succeed
            tokio::time::sleep(time_till_second(10, Some(5))).await;

            for dataset_name in dataset_names.clone() {
                snapshot_names_from_runtime(
                    &format!("test_multiple_cron_schedule_creates_after_{dataset_name}"),
                    &rt,
                    Some(&dataset_name),
                )
                .await;
            }

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            std::fs::remove_file("./test_multiple_cron_schedule_creates.csv").expect("remove file");

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

#[allow(clippy::too_many_lines)]
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

            let app = AppBuilder::build_from_path(spicepod_dir.clone())
                .await
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

const NAMES_TIMESTAMPED_CSV: &str = include_str!("data/names_timestamped.csv");

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_append_cron_schedule() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            std::fs::write("./test_append_cron_schedule.csv", NAMES_TIMESTAMPED_CSV)
                .expect("write file");

            let app = test_framework::app_utils::load_app_from_spicepod_str(include_str!(
                "./spicepods/test_append_cron_schedule.yaml"
            ))
            .await
            .expect("Should load app from spicepod string");

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

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

            snapshot_names_from_runtime("test_append_cron_schedule_before", &rt, None).await;

            // Append a new row to the CSV file
            let new_row = format!(
                "11,Spaceman,29,LEO,100,{}\n",
                chrono::Utc::now().to_rfc3339()
            );
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_append_cron_schedule.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait for the next 30th second, and wait 10 seconds for the job to succeed
            tokio::time::sleep(time_till_second(15, Some(5))).await;
            snapshot_names_from_runtime("test_append_cron_schedule_after_one", &rt, None).await;

            // Append a new row to the CSV file
            let new_row = format!(
                "12,Cassian,33,Kenari,100,{}\n",
                chrono::Utc::now().to_rfc3339()
            );
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_append_cron_schedule.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait for the next 30th second, and wait 10 seconds for the job to succeed
            tokio::time::sleep(time_till_second(15, Some(5))).await;
            snapshot_names_from_runtime("test_append_cron_schedule_after_two", &rt, None).await;

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            std::fs::remove_file("./test_append_cron_schedule.csv").expect("remove file");

            Ok(())
        })
        .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_cron_view() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            std::fs::write("./test_cron_view.csv", NAMES_CSV).expect("write file");

            let app = test_framework::app_utils::load_app_from_spicepod_str(include_str!(
                "./spicepods/test_cron_view.yaml"
            ))
            .await
            .expect("Should load app from spicepod string");

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

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
                "names_view".into(),
                "Expected schedule name to match dataset name"
            );

            snapshot_names_from_runtime("test_cron_view", &rt, None).await;

            // Append a new row to the CSV file
            let new_row = "11,Spaceman,29,LEO,100\n".to_string();
            std::fs::OpenOptions::new()
                .append(true)
                .open("./test_cron_view.csv")
                .expect("open file")
                .write_all(new_row.as_bytes())
                .expect("append to file");

            // wait for the next 15th second, and wait 5 seconds for the job to succeed
            tokio::time::sleep(time_till_second(15, Some(5))).await;
            snapshot_names_from_runtime("test_cron_view_after", &rt, None).await;

            rt.shutdown().await;
            drop(rt);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            std::fs::remove_file("./test_cron_view.csv").expect("remove file");

            Ok(())
        })
        .await
}
