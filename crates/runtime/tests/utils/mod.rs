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

use std::{
    fmt::Display,
    future::Future,
    sync::{Arc, LazyLock},
    time::Duration,
};

use arrow::array::RecordBatch;
use chrono::Timelike;
use futures::StreamExt;
use runtime::{
    Runtime,
    request::{Protocol, RequestContext, UserAgent},
};

pub(crate) static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> = LazyLock::new(|| {
    Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spiceci/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .build(),
    )
});

pub(crate) async fn runtime_ready_check(rt: &Runtime) {
    runtime_ready_check_with_timeout(rt, Duration::from_secs(120)).await;
}

pub(crate) async fn runtime_ready_check_with_timeout(rt: &Runtime, duration: Duration) {
    assert!(wait_until_true(duration, || async { rt.status().is_ready() }).await);
}

pub(crate) async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

/// Returns the duration until the next occurrence of the nearest second.
/// Optionally, add an overhead to apply to wait for a bit longer after the nearest second is reached.
#[allow(dead_code)]
pub(crate) fn time_till_second(nearest_second: u32, wait: Option<u32>) -> Duration {
    assert!(
        nearest_second < 60,
        "nearest_second must be between 0 and 59"
    );
    let now_second = chrono::Utc::now().second();
    let modulus = now_second % nearest_second;
    let time_until_nearest = if modulus == 0 {
        0
    } else {
        nearest_second - modulus
    };

    Duration::from_secs(u64::from(time_until_nearest + wait.unwrap_or(0)))
}

#[allow(dead_code)]
pub(crate) async fn verify_env_secret_exists(secret_name: &str) -> Result<(), String> {
    let mut secrets = runtime::secrets::Secrets::new();
    // Will automatically load `env` as the default
    secrets
        .load_from(&[])
        .await
        .map_err(|err| err.to_string())?;

    secrets
        .get_secret(secret_name)
        .await
        .map_err(|err| err.to_string())?
        .ok_or_else(|| format!("Secret {secret_name} not found"))?;

    Ok(())
}

pub(crate) fn test_request_context() -> Arc<RequestContext> {
    Arc::clone(&TEST_REQUEST_CONTEXT)
}

#[allow(dead_code)]
pub(crate) async fn run_query(
    rt: &Arc<Runtime>,
    query: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(query).build().run().await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    Ok(results)
}

#[allow(dead_code)]
pub(crate) fn to_pretty_display(batches: &[RecordBatch]) -> Result<impl Display, anyhow::Error> {
    let pretty = arrow::util::pretty::pretty_format_batches(batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;

    Ok(pretty)
}
