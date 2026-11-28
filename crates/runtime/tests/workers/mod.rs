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

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::status::ComponentStatus;
use snafu::ResultExt;
use spicepod::component::dataset::Dataset;
use spicepod::component::model::Model;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, TaskHistory};
use spicepod::component::worker::{LoadBalanceParams, RouterConfig, Worker};
use spicepod::param::Params;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::models::get_tpcds_dataset;
use crate::utils::{
    init_tracing_with_task_history, runtime_ready_check, test_request_context, time_till_second,
    verify_env_secret_exists,
};
use crate::{DEFAULT_TRACING_MODELS, init_tracing};

fn create_loadbalance_worker(name: &str, models: &[&str], cron: &str, prompt: &str) -> Worker {
    let mut params = HashMap::new();
    params.insert("prompt".to_string(), prompt.into());

    Worker {
        name: name.to_string(),
        description: None,
        params,
        load_balance: Some(LoadBalanceParams {
            routing: models
                .iter()
                .map(|m| RouterConfig::RoundRobin {
                    from: (*m).to_string(),
                })
                .collect(),
        }),
        cron: Some(cron.to_string()),
        sql: None,
    }
}

fn create_sql_worker(name: &str, sql: &str, cron: &str) -> Worker {
    Worker {
        name: name.to_string(),
        description: None,
        params: HashMap::new(),
        load_balance: None,
        cron: Some(cron.to_string()),
        sql: Some(sql.to_string()),
    }
}

fn get_openai_model(model: impl Into<String>, name: impl Into<String>) -> Model {
    let mut model = Model::new(format!("openai:{}", model.into()), name);
    model
        .params
        .extend(crate::utils::openai_credential_params());
    model.params.insert("tools".into(), "auto".into());
    model
}

#[tokio::test]
async fn test_worker_with_cron() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope_retry(3, || async {
            verify_env_secret_exists(crate::utils::openai_credential_secret())
                .await
                .map_err(anyhow::Error::msg)?;

            let ds_tpcds_item = get_tpcds_dataset("item", None, None);

            let model = get_openai_model("gpt-4o-mini", "4o-mini");

            let app = AppBuilder::new("test_worker_with_cron")
                .with_dataset(ds_tpcds_item)
                .with_model(model)
                .with_worker(create_loadbalance_worker(
                    "cron_scheduled",
                    &["4o-mini"],
                    "*/30 * * * * *",
                    "Using the SQL tool, count the number of records in the item table.",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_tracing, trace_provider) = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

            // don't startup until we've got some time to load before the next cron job
            tokio::time::sleep(time_till_second(30, Some(2))).await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // wait for the next 30th second, and wait 20 seconds for the job to succeed
            tokio::time::sleep(time_till_second(30, Some(20))).await; // wait for the cron job to run at least once
            let _ = trace_provider.force_flush();

            let data = rt
                .datafusion()
                .query_builder("SELECT task, input, captured_output FROM runtime.task_history WHERE task = 'scheduled_worker'")
                .build()
                .run()
                .await
                .boxed()
                .expect("Failed to collect data")
                .data
                .try_collect::<Vec<_>>()
                .await
                .boxed()
                .expect("Failed to collect data");

            let pretty = arrow::util::pretty::pretty_format_batches(&data)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_sql_worker_with_cron() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope_retry(3, || async {
            verify_env_secret_exists(crate::utils::openai_credential_secret())
                .await
                .map_err(anyhow::Error::msg)?;

            let ds_tpcds_item = get_tpcds_dataset("item", None, None);

            // The snapshot asserts the query's result preview, which the query only
            // builds when `captured_output` is not `none` (the default).
            let app = AppBuilder::new("test_worker_with_cron")
                .with_runtime(SpicepodRuntime {
                    task_history: TaskHistory {
                        captured_output: "truncated".into(),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .with_dataset(ds_tpcds_item)
                .with_worker(create_sql_worker(
                    "sql_scheduled",
                    "SELECT COUNT(*) FROM item",
                    "*/15 * * * * *",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_tracing, trace_provider) = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

            // don't startup until we've got some time to load before the next cron job
            // this avoids an extra task history trace that does nothing, because the task is a no-op while the runtime isn't ready
            tokio::time::sleep(time_till_second(15, Some(2))).await;

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // every 15th second, wait for 2 seconds for the job to succeed
            tokio::time::sleep(time_till_second(15, Some(2))).await; // wait for the cron job to run at least once
            let _ = trace_provider.force_flush();

            let data = rt
                .datafusion()
                .query_builder("SELECT task, input, captured_output FROM runtime.task_history ORDER BY end_time DESC")
                .build()
                .run()
                .await
                .boxed()
                .expect("Failed to collect data")
                .data
                .try_collect::<Vec<_>>()
                .await
                .boxed()
                .expect("Failed to collect data");

            let pretty = arrow::util::pretty::pretty_format_batches(&data)
                .map_err(|e| anyhow::Error::msg(e.to_string()))?;
            insta::assert_snapshot!(pretty);

            Ok(())
        })
        .await
}

/// The SQL that [`test_sql_worker_runs_while_a_dataset_failed_to_load`] schedules.
const RUNS_WHILE_A_DATASET_FAILED_SQL: &str = "SELECT 1141 AS worker_ran";

/// A SQL worker runs on its schedule while a dataset failed to load: a component that
/// failed may never load, so it must not hold the worker back.
#[tokio::test]
async fn test_sql_worker_runs_while_a_dataset_failed_to_load() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let present_csv = dir.path().join("loaded.csv");
            std::fs::write(&present_csv, "id\n1\n")?;
            let csv_dataset = |path: &std::path::Path, name: &str| {
                let mut dataset = Dataset::new(format!("file:{}", path.display()), name);
                dataset.params = Some(Params::from_string_map(HashMap::from([(
                    "file_format".to_string(),
                    "csv".to_string(),
                )])));
                dataset
            };
            let app = AppBuilder::new("test_sql_worker_runs_while_a_dataset_failed_to_load")
                .with_dataset(csv_dataset(&present_csv, "loaded"))
                .with_dataset(csv_dataset(&dir.path().join("missing.csv"), "failed"))
                .with_worker(create_sql_worker(
                    "every_second",
                    RUNS_WHILE_A_DATASET_FAILED_SQL,
                    "* * * * * *",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let (_tracing, trace_provider) =
                init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);
            let loader = tokio::spawn({
                let rt = Arc::clone(&rt);
                async move { rt.load_components().await }
            });

            let status = rt.status();
            let settled = tokio::time::timeout(Duration::from_secs(30), async {
                while !(matches!(
                    status.get_component_status("dataset:failed"),
                    Some(ComponentStatus::Error(_))
                ) && matches!(
                    status.worker_status("every_second"),
                    Some(ComponentStatus::Ready)
                )) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await;
            assert!(
                settled.is_ok(),
                "dataset 'failed' must fail to load and the worker must load; dataset {:?}, worker {:?}",
                status.get_component_status("dataset:failed"),
                status.worker_status("every_second")
            );

            tokio::time::sleep(Duration::from_secs(3)).await;
            let _ = trace_provider.force_flush();
            let data = rt
                .datafusion()
                .query_builder(&format!(
                    "SELECT COUNT(*) AS runs FROM runtime.task_history WHERE task = 'sql_query' AND input = '{RUNS_WHILE_A_DATASET_FAILED_SQL}'"
                ))
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await?;
            let runs: i64 = data
                .iter()
                .map(|batch| batch.column(0).as_primitive::<Int64Type>().value(0))
                .sum();
            assert!(
                runs > 0,
                "the worker must run while dataset 'failed' is in Error"
            );

            loader.abort();
            Ok::<(), anyhow::Error>(())
        })
        .await
}
