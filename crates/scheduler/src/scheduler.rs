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

use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{Notify, RwLock, mpsc::Receiver},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{Result, schedule::Schedule, task::TaskRequest};

pub struct NotStarted {
    schedules: Vec<Arc<Schedule>>,
}

pub struct NotificationChannels {
    pub(crate) completion: Arc<Notify>,
    pub(crate) reset: Arc<Notify>,
}

type TaskRequestHandles = Arc<RwLock<HashMap<Arc<str>, Vec<JoinHandle<Result<()>>>>>>;
pub(crate) type TaskRequestChannels =
    Arc<RwLock<HashMap<Arc<str>, Arc<RwLock<Receiver<Arc<TaskRequest>>>>>>>;

type SchedulerHandles = Arc<RwLock<HashMap<Arc<str>, Vec<JoinHandle<Result<()>>>>>>;

pub struct Running {
    schedules: Vec<Arc<Schedule>>,
    request_handles: TaskRequestHandles,
    request_channels: TaskRequestChannels,
    cancellation_token: Arc<CancellationToken>,
    notification_channels: Arc<NotificationChannels>,
    scheduler_handles: SchedulerHandles,
}

pub struct Scheduler<T> {
    state: Arc<T>,
    name: Arc<str>,
}

impl Scheduler<NotStarted> {
    #[must_use]
    pub fn new(name: Arc<str>, schedules: Vec<Arc<Schedule>>) -> Self {
        Self {
            state: Arc::new(NotStarted { schedules }),
            name,
        }
    }

    /// Starts the scheduler
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduler fails to start, due to a task request channel error.
    pub async fn start(self) -> Result<Scheduler<Running>> {
        let cancellation_token = Arc::new(CancellationToken::new());

        let schedules = self.state.schedules.clone();
        let mut request_handles = HashMap::new();
        let mut request_channels = HashMap::new();

        let notification_channels = Arc::new(NotificationChannels {
            completion: Arc::new(Notify::default()),
            reset: Arc::new(Notify::default()),
        });

        for schedule in &schedules {
            // Initialize the request channels for each schedule
            let (tx, rx) = tokio::sync::mpsc::channel::<Arc<TaskRequest>>(5);
            let tx = Arc::new(tx);
            let schedule_id = schedule.id();

            for channel_lock in schedule.triggers() {
                let mut channel = channel_lock.write().await;
                channel.set_task_completion_notification(Arc::clone(
                    &notification_channels.completion,
                ));
                channel.set_cancellation_token(Arc::clone(&cancellation_token));
                channel.set_reset_notification(Arc::clone(&notification_channels.reset));
                channel.set_submission_channel(Arc::clone(&tx));
                let handle = channel.start()?;
                let entry = request_handles
                    .entry(Arc::clone(&schedule_id))
                    .or_insert(Vec::new());
                entry.push(handle);
            }

            request_channels.insert(Arc::clone(&schedule_id), Arc::new(RwLock::new(rx)));
        }

        let request_handles = Arc::new(RwLock::new(request_handles));
        let request_channels = Arc::new(RwLock::new(request_channels));

        Ok(Scheduler {
            state: Arc::new(Running {
                schedules: self.state.schedules.clone(),
                cancellation_token,
                request_handles,
                request_channels,
                notification_channels: Arc::clone(&notification_channels),
                scheduler_handles: Arc::new(RwLock::new(HashMap::new())),
            }),
            name: self.name,
        }
        .start()
        .await)
    }
}

impl Scheduler<Running> {
    pub async fn stop(self) {
        let cancellation_token = Arc::clone(&self.state.cancellation_token);
        cancellation_token.cancel();

        // End the task request channels
        let mut request_handles = self.state.request_handles.write().await;
        for handles in request_handles.values_mut() {
            for handle in handles.drain(..) {
                handle.abort();
                match handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("Task request channel completed successfully");
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Task request channel execution failed: {e}");
                    }
                    Err(e) => {
                        tracing::error!("Task request channel join error: {e}");
                    }
                }
            }
        }

        // End the schedule handlers
        let mut scheduler_handles = self.state.scheduler_handles.write().await;
        for handles in scheduler_handles.values_mut() {
            for handle in handles.drain(..) {
                handle.abort();
                match handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("Scheduler task completed successfully");
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Scheduler task execution failed: {e}");
                    }
                    Err(e) => {
                        tracing::error!("Scheduler task join error: {e}");
                    }
                }
            }
        }

        // Drop the RX channels to ensure they are closed
        let mut request_channels = self.state.request_channels.write().await;
        request_channels.clear();

        // Clear the scheduler handles
        scheduler_handles.clear();
    }

    pub async fn start(self) -> Self {
        let state = Arc::clone(&self.state);
        let cancellation_token = Arc::clone(&self.state.cancellation_token);
        let schedules = self.state.schedules.clone();

        // For each schedule, spawn a task that listens for task requests and executes them
        let scheduler_handles = Arc::clone(&self.state.scheduler_handles);
        let mut scheduler_handles = scheduler_handles.write().await;
        for schedule in &schedules {
            let schedule = Arc::clone(schedule);
            let schedule_id = schedule.id();

            let handle = schedule.start(
                Arc::clone(&state.request_channels),
                Arc::clone(&state.notification_channels),
                Arc::clone(&cancellation_token),
            );

            scheduler_handles
                .entry(schedule_id)
                .or_insert_with(Vec::new)
                .push(handle);
        }

        drop(scheduler_handles);
        self
    }
}

// ========== Tests ==========
#[cfg(test)]
mod test {
    use super::*;
    use crate::channel::interval::IntervalRequestChannel;
    use crate::channel::manual::ManualRequestChannel;
    use crate::schedule::Schedule;
    use crate::task::{ScheduledTask, TaskRequest};
    use async_trait::async_trait;
    use std::{
        sync::LazyLock,
        time::{Duration, Instant},
    };
    use tracing_subscriber::EnvFilter;

    fn init_tracing(default_level: Option<&str>) -> tracing::subscriber::DefaultGuard {
        let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
            (_, Some(log)) => EnvFilter::new(log),
            (Some(level), None) => EnvFilter::new(level),
            _ => EnvFilter::new("DEBUG"),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    static TEST_EXECUTION_COUNT: LazyLock<RwLock<HashMap<Arc<str>, usize>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler"), 0);
        map.insert(Arc::from("test_multi_schedule"), 0);
        map.insert(Arc::from("test_multi_component_schedule"), 0);
        map.insert(Arc::from("test_multi_evaluator"), 0);
        map.insert(Arc::from("test_manual_interrupts"), 0);
        map.insert(Arc::from("test_manual_queued_with_interrupt"), 0);
        map.insert(Arc::from("test_manual_queue_clears_after_immediate"), 0);

        RwLock::new(map)
    });

    static TIMING_MAP: LazyLock<RwLock<HashMap<Arc<str>, Vec<Instant>>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler_timing"), Vec::new());

        RwLock::new(map)
    });

    struct TestComponent {
        name: Arc<str>,
    }

    #[async_trait]
    impl ScheduledTask for TestComponent {
        async fn execute(&self) -> Result<()> {
            let mut map_lock = TEST_EXECUTION_COUNT.write().await;
            let count = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            *count += 1;
            Ok(())
        }
    }

    struct LongComponent {
        name: Arc<str>,
        wait: u64,
    }

    #[async_trait]
    impl ScheduledTask for LongComponent {
        async fn execute(&self) -> Result<()> {
            tokio::time::sleep(std::time::Duration::from_secs(self.wait)).await;
            let mut map_lock = TEST_EXECUTION_COUNT.write().await;
            let count = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            *count += 1;
            Ok(())
        }
    }

    struct TimedComponent {
        name: Arc<str>,
    }

    #[async_trait]
    impl ScheduledTask for TimedComponent {
        async fn execute(&self) -> Result<()> {
            let now = Instant::now();
            let mut map_lock = TIMING_MAP.write().await;
            let timings = map_lock
                .get_mut(self.name.as_ref())
                .expect("To get test execution count");
            timings.push(now);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let schedule = Schedule::new(Arc::new(TestComponent {
            name: Arc::from("test_scheduler"),
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler =
            Scheduler::<NotStarted>::new("test_scheduler".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_scheduler")
            .expect("To get test execution count");
        assert!(
            *count == 4 || *count == 5,
            "Test component should have executed 4 or 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_scheduler_timing() {
        init_tracing(None);
        let schedule = Schedule::new(Arc::new(TimedComponent {
            name: "test_scheduler_timing".into(),
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler = Scheduler::new("test_scheduler_timing".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        scheduler.stop().await;
        let map_lock = TIMING_MAP.read().await;
        let timings = map_lock
            .get("test_scheduler_timing")
            .expect("To get test execution count");
        let mut diffs = Vec::new();
        for i in 1..timings.len() {
            let diff = timings[i].duration_since(timings[i - 1]);
            diffs.push(diff);
        }
        assert!(
            diffs.len() == 8 || diffs.len() == 9,
            "There should be more than 8 or 9 timing differences, but got {diffs:?}"
        );
        for diff in diffs {
            assert!(
                diff.as_millis() >= 990 && diff.as_millis() <= 1010,
                "Timing difference should be around 1 second, but got {diff:?}ms"
            );
        }
    }

    #[tokio::test]
    async fn test_multi_schedule() {
        let schedule_one = Schedule::new(Arc::new(TestComponent {
            name: Arc::from("test_multi_schedule"),
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let schedule_two = Schedule::new(Arc::new(TestComponent {
            name: Arc::from("test_multi_schedule"),
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler = Scheduler::<NotStarted>::new(
            "test_multi_schedule".into(),
            vec![Arc::new(schedule_one), Arc::new(schedule_two)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_schedule")
            .expect("To get test execution count");
        assert!(
            *count == 8 || *count == 10,
            "Test component should have executed 8 or 10 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_multi_evaluator() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(Arc::new(TestComponent {
            name: "test_multi_evaluator".into(),
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))))
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new("test_multi_evaluator".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        tx.send(Some(Arc::new(TaskRequest::default().clears_queue())))
            .await
            .expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_multi_evaluator")
            .expect("To get test execution count");
        assert!(
            *count == 4 || *count == 5,
            "Test component should have executed 4 or 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_interrupts() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(Arc::new(TestComponent {
            name: "test_manual_interrupts".into(),
        }))
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new("test_manual_interrupts".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        tx.send(None).await.expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_interrupts")
            .expect("To get test execution count");
        assert!(
            *count == 3,
            "Test component should have executed 3 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_queued_with_interrupt() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(Arc::new(TestComponent {
            name: "test_manual_queued_with_interrupt".into(),
        }))
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new(
            "test_manual_queued_with_interrupt".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        for _ in 0..5 {
            tx.send(Some(Arc::new(TaskRequest::default())))
                .await
                .expect("To send task request");
        }
        tokio::time::sleep(std::time::Duration::from_secs(7)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_queued_with_interrupt")
            .expect("To get test execution count");
        assert!(
            *count == 5,
            "Test component should have executed 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_manual_queue_clears_after_immediate() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(Arc::new(LongComponent {
            name: "test_manual_queue_clears_after_immediate".into(),
            wait: 5,
        }))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))))
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new(
            "test_manual_queue_clears_after_immediate".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        tx.send(Some(Arc::new(TaskRequest::default().clears_queue())))
            .await
            .expect("To send task request");
        tokio::time::sleep(std::time::Duration::from_secs(8)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_manual_queue_clears_after_immediate")
            .expect("To get test execution count");
        assert!(
            *count == 1,
            "Test component should have executed 1 times, but got {count}"
        );
    }
}
