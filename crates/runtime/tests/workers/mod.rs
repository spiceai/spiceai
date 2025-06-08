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
use futures::TryStreamExt;
use runtime::Runtime;
use snafu::ResultExt;
use spicepod::component::model::Model;
use spicepod::component::worker::{LoadBalanceParams, RouterConfig, Worker};
use std::collections::HashMap;
use std::sync::Arc;

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
    model.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    model.params.insert("tools".into(), "auto".into());
    model
}

#[tokio::test]
async fn test_worker_with_cron() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope_retry(3, || async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let ds_tpcds_item = get_tpcds_dataset("item", None, None);

            let app = AppBuilder::new("test_worker_with_cron")
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
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
