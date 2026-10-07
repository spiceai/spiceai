/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Integration tests for `runtime.executor.task_slots`.
//!
//! Each test starts a real `Scheduler` and `Executor` and reads the slot count
//! the executor registered with the scheduler, so the whole wired path is covered:
//! the executor reading its own app, the scheduler's app arriving through
//! `GetAppDefinition`, and the registration the scheduler sums for its parallelism.

use app::{App, AppBuilder};
use spicepod::component::runtime::{Executor, Runtime};
use std::time::Duration;
use tokio::time::{Instant, sleep};

use crate::cluster::harness::ClusterHarness;
use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

/// An app whose `runtime.executor.task_slots` is `task_slots`, or unset for `None`.
fn app_with_task_slots(name: &str, task_slots: Option<u64>) -> App {
    let mut app = AppBuilder::new(name).build();
    app.runtime = Runtime {
        executor: Some(Executor { task_slots }),
        ..Runtime::default()
    };
    app
}

/// Poll until the scheduler's registry reports exactly one executor, then return the
/// slots it advertised.
async fn advertised_slots_of_single_executor(
    harness: &ClusterHarness,
) -> Result<u32, anyhow::Error> {
    harness.wait_for_executors(Duration::from_secs(60)).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let slots = harness.executor_advertised_slots().await?;
        if let [only] = slots.as_slice() {
            return Ok(*only);
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for exactly one executor registration; advertised slots: {slots:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn advertised_slots(scheduler: App, executor: App) -> Result<u32, anyhow::Error> {
    let harness = ClusterHarness::builder()
        .scheduler(scheduler)
        .executor_with_app(executor)
        .start()
        .await?;
    let result = advertised_slots_of_single_executor(&harness).await;
    harness.shutdown().await;
    result
}

/// A slot count that differs from the CPU budget default on any machine.
fn above_cpu_default(extra: u64) -> u64 {
    let cores = u64::try_from(cpu_budget::cpu_budget().cluster_executor_concurrent_tasks())
        .expect("CPU core count should fit in u64");
    cores + extra
}

#[tokio::test(flavor = "multi_thread")]
async fn executor_advertises_scheduler_task_slots() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let configured = above_cpu_default(3);
            let advertised = advertised_slots(
                app_with_task_slots("task_slots_scheduler", Some(configured)),
                app_with_task_slots("task_slots_executor", None),
            )
            .await?;
            assert_eq!(u64::from(advertised), configured);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn executor_task_slots_override_scheduler_task_slots() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let scheduler_slots = above_cpu_default(3);
            let executor_slots = above_cpu_default(7);
            let advertised = advertised_slots(
                app_with_task_slots("task_slots_scheduler", Some(scheduler_slots)),
                app_with_task_slots("task_slots_executor", Some(executor_slots)),
            )
            .await?;
            assert_eq!(u64::from(advertised), executor_slots);
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn executor_advertises_cpu_budget_without_task_slots() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    test_request_context()
        .scope(async {
            configure_test_datafusion();
            let advertised = advertised_slots(
                app_with_task_slots("task_slots_scheduler", None),
                app_with_task_slots("task_slots_executor", None),
            )
            .await?;
            assert_eq!(
                usize::try_from(advertised).expect("slot count should fit in usize"),
                cpu_budget::cpu_budget().cluster_executor_concurrent_tasks()
            );
            Ok(())
        })
        .await
}
