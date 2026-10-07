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

//! Guard that an executor's `task_runner` thread pool is sized from the CPU budget,
//! not from `runtime.executor.task_slots`.
//!
//! This is its own test binary with a single test because it counts the OS threads
//! of the whole process: any other executor in the process would add its own
//! `task_runner` threads to the count.

#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
#![expect(
    clippy::expect_used,
    reason = "test code, and the shared harness it includes, fails fast with descriptive messages"
)]

#[path = "cluster/harness.rs"]
#[expect(
    dead_code,
    reason = "the shared harness has helpers this test does not use"
)]
mod harness;

use app::AppBuilder;
use harness::ClusterHarness;
use spicepod::component::runtime::{Executor, Runtime};
use std::time::Duration;
use tokio::time::{Instant, sleep};

const TASK_RUNNER_THREAD_NAME: &str = "task_runner";

/// Number of threads of this process whose `comm` is exactly `task_runner`.
fn task_runner_thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("/proc/self/task should be readable")
        .filter_map(Result::ok)
        .filter(|entry| {
            std::fs::read_to_string(entry.path().join("comm"))
                .is_ok_and(|comm| comm.trim_end() == TASK_RUNNER_THREAD_NAME)
        })
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn task_runner_pool_is_sized_from_cpu_budget_not_task_slots() {
    let budget = cpu_budget::cpu_budget();
    let expected_threads = budget.cluster_executor_task_runner_threads();
    let task_slots = budget.cluster_executor_concurrent_tasks() + 5;

    let mut scheduler_app = AppBuilder::new("task_runner_threads_scheduler").build();
    scheduler_app.runtime = Runtime {
        executor: Some(Executor {
            task_slots: Some(u64::try_from(task_slots).expect("slot count should fit in u64")),
        }),
        ..Runtime::default()
    };

    let harness = ClusterHarness::builder()
        .scheduler(scheduler_app)
        .executors(1)
        .start()
        .await
        .expect("cluster should start");
    // One scheduler, so one poll loop and one task-runner pool.
    assert_eq!(harness.executors.len(), 1, "expected exactly one executor");

    harness
        .wait_for_executors(Duration::from_secs(60))
        .await
        .expect("executor should register");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let advertised = harness
            .executor_advertised_slots()
            .await
            .expect("scheduler should report executor slots");
        if advertised == [u32::try_from(task_slots).expect("slot count should fit in u32")] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "executor never advertised {task_slots} slots; last seen: {advertised:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    // The pool is created when the poll loop starts; wait for it to appear and settle.
    let mut previous = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    let observed = loop {
        let count = task_runner_thread_count();
        if count > 0 && count == previous {
            break count;
        }
        assert!(
            Instant::now() < deadline,
            "task_runner thread count never settled; last count: {count}"
        );
        previous = count;
        sleep(Duration::from_millis(500)).await;
    };

    harness.shutdown().await;

    assert_eq!(
        observed, expected_threads,
        "executor with {task_slots} task slots runs {observed} task_runner threads; expected {expected_threads} (the CPU budget), not {task_slots} (the slot count)"
    );
    assert_ne!(observed, task_slots);
}
