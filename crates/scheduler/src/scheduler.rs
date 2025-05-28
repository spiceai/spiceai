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
    sync::{
        Notify, RwLock,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{Result, channel::TaskRequestChannel, schedule::Schedule, task::TaskRequest};

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
pub(crate) type TaskSubmissionChannels =
    Arc<RwLock<HashMap<Arc<str>, Arc<Sender<Arc<TaskRequest>>>>>>;

type SchedulerHandles = Arc<RwLock<HashMap<Arc<str>, Vec<JoinHandle<Result<()>>>>>>;

pub struct Running {
    schedules: Arc<RwLock<Vec<Arc<Schedule>>>>,
    request_handles: TaskRequestHandles,
    request_channels: TaskRequestChannels,
    submission_channels: TaskSubmissionChannels,
    cancellation_token: Arc<CancellationToken>,
    notification_channels: Arc<NotificationChannels>,
    scheduler_handles: SchedulerHandles,
}

pub struct SchedulerBuilder {
    name: Arc<str>,
    schedules: Vec<Arc<Schedule>>,
}

impl SchedulerBuilder {
    #[must_use]
    pub fn new(name: Arc<str>) -> Self {
        Self {
            name,
            schedules: Vec::new(),
        }
    }

    #[must_use]
    pub fn add_schedule(mut self, schedule: Arc<Schedule>) -> Self {
        self.schedules.push(schedule);
        self
    }

    /// Builds a new scheduler that has not yet started.
    ///
    /// # Errors
    ///
    /// - If no schedules are specified, or if there are duplicate schedule names.
    pub fn build(self) -> Result<Scheduler<NotStarted>> {
        if self.schedules.is_empty() {
            return Err(crate::Error::NoSchedulesSpecified {
                name: self.name.to_string(),
            });
        }

        self.schedules.iter().try_for_each(|schedule| {
            if self
                .schedules
                .iter()
                .filter(|s| s.name() == schedule.name())
                .count()
                > 1
            {
                return Err(crate::Error::DuplicateScheduleName {
                    name: schedule.name().to_string(),
                });
            }
            Ok(())
        })?;

        Ok(Scheduler::<NotStarted>::new(self.name, self.schedules))
    }
}

pub struct Scheduler<T> {
    state: Arc<T>,
    name: Arc<str>,
}

impl Scheduler<NotStarted> {
    #[must_use]
    pub(crate) fn new(name: Arc<str>, schedules: Vec<Arc<Schedule>>) -> Self {
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

        let notification_channels = Arc::new(NotificationChannels {
            completion: Arc::new(Notify::default()),
            reset: Arc::new(Notify::default()),
        });

        let scheduler = Scheduler {
            state: Arc::new(Running {
                schedules: Arc::new(RwLock::new(Vec::new())),
                cancellation_token: Arc::clone(&cancellation_token),
                request_handles: Arc::new(RwLock::new(HashMap::new())),
                request_channels: Arc::new(RwLock::new(HashMap::new())),
                submission_channels: Arc::new(RwLock::new(HashMap::new())),
                notification_channels: Arc::clone(&notification_channels),
                scheduler_handles: Arc::new(RwLock::new(HashMap::new())),
            }),
            name: self.name,
        };

        for schedule in &self.state.schedules.clone() {
            scheduler.add_schedule(Arc::clone(schedule)).await?;
        }

        Ok(scheduler)
    }
}

impl Scheduler<Running> {
    #[must_use]
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    #[must_use]
    pub async fn schedules(&self) -> Vec<Arc<Schedule>> {
        self.state.schedules.read().await.clone()
    }

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

    /// Adds another trigger to an existing schedule, and starts up the request channel.
    ///
    /// # Errors
    ///
    /// - If the schedule with the specified name does not exist.
    /// - If the request channel fails to start.
    /// - If a submission channel is not found for the schedule.
    pub async fn add_trigger_for_schedule(
        &self,
        schedule_name: Arc<str>,
        request_channel: Arc<RwLock<dyn TaskRequestChannel>>,
    ) -> Result<()> {
        self.schedules()
            .await
            .iter()
            .find(|s| s.name() == schedule_name)
            .ok_or_else(|| crate::Error::DuplicateScheduleName {
                name: schedule_name.to_string(),
            })?;

        let mut channel = request_channel.write().await;
        channel.set_task_completion_notification(Arc::clone(
            &self.state.notification_channels.completion,
        ));
        channel.set_cancellation_token(Arc::clone(&self.state.cancellation_token));
        channel.set_reset_notification(Arc::clone(&self.state.notification_channels.reset));

        let submission_channels_lock = Arc::clone(&self.state.submission_channels);
        let submission_channels = submission_channels_lock.read().await;
        let submission_channel = submission_channels
            .get(&schedule_name)
            .ok_or(crate::Error::SubmissionChannelRequired)?;

        channel.set_submission_channel(Arc::clone(submission_channel));
        let handle = channel.start()?;
        let mut request_handles = self.state.request_handles.write().await;
        let entry = request_handles
            .entry(schedule_name)
            .or_insert_with(Vec::new);
        entry.push(handle);
        Ok(())
    }

    /// Adds a new schedule to the running scheduler.
    ///
    /// # Errors
    ///
    /// - If a schedule with the same name already exists.
    pub async fn add_schedule(&self, schedule: Arc<Schedule>) -> Result<()> {
        let schedule_name = schedule.name();
        if self
            .schedules()
            .await
            .iter()
            .any(|s| s.name() == schedule_name)
        {
            return Err(crate::Error::DuplicateScheduleName {
                name: schedule_name.to_string(),
            });
        }

        let mut schedules = self.state.schedules.write().await;
        schedules.push(Arc::clone(&schedule));
        drop(schedules);

        // Create the submission and request channels for the new schedule
        let (tx, rx) = tokio::sync::mpsc::channel::<Arc<TaskRequest>>(5);
        let tx = Arc::new(tx);
        let schedule_name = schedule.name();
        self.state
            .submission_channels
            .write()
            .await
            .insert(Arc::clone(&schedule_name), Arc::clone(&tx));
        let rx_lock = Arc::new(RwLock::new(rx));
        self.state
            .request_channels
            .write()
            .await
            .insert(Arc::clone(&schedule_name), Arc::clone(&rx_lock));

        // Start the request channels for the new schedule
        let cancellation_token = Arc::clone(&self.state.cancellation_token);
        let notification_channels = Arc::clone(&self.state.notification_channels);

        for trigger_lock in schedule.triggers() {
            let mut trigger = trigger_lock.write().await;
            trigger.set_task_completion_notification(Arc::clone(&notification_channels.completion));
            trigger.set_cancellation_token(Arc::clone(&cancellation_token));
            trigger.set_reset_notification(Arc::clone(&notification_channels.reset));

            trigger.set_submission_channel(Arc::clone(&tx));
            let handle = trigger.start()?;
            let mut request_handles = self.state.request_handles.write().await;
            let entry = request_handles
                .entry(Arc::clone(&schedule_name))
                .or_insert_with(Vec::new);
            entry.push(handle);
        }

        // With request channels set up, we can now start the schedule
        let scheduler_handles = Arc::clone(&self.state.scheduler_handles);
        let mut scheduler_handles = scheduler_handles.write().await;
        let handle = schedule.start(
            Arc::clone(&self.state.request_channels),
            Arc::clone(&self.state.notification_channels),
            Arc::clone(&cancellation_token),
        );
        scheduler_handles
            .entry(schedule_name)
            .or_insert_with(Vec::new)
            .push(handle);

        Ok(())
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
        map.insert(
            Arc::from("test_adding_schedule_while_running_starts_existing"),
            0,
        );
        map.insert(
            Arc::from("test_adding_schedule_while_running_starts_new"),
            0,
        );
        map.insert(Arc::from("test_adding_trigger_to_existing_schedule"), 0);

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
        let schedule = Schedule::new(
            Arc::from("test_scheduler"),
            Arc::new(TestComponent {
                name: Arc::from("test_scheduler"),
            }),
        )
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
        let schedule = Schedule::new(
            Arc::from("test_scheduler_timing"),
            Arc::new(TimedComponent {
                name: "test_scheduler_timing".into(),
            }),
        )
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
        let schedule_one = Schedule::new(
            Arc::from("test_multi_schedule_one"),
            Arc::new(TestComponent {
                name: Arc::from("test_multi_schedule"),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let schedule_two = Schedule::new(
            Arc::from("test_multi_schedule_two"),
            Arc::new(TestComponent {
                name: Arc::from("test_multi_schedule"),
            }),
        )
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
        let schedule = Schedule::new(
            Arc::from("test_multi_evaluator"),
            Arc::new(TestComponent {
                name: "test_multi_evaluator".into(),
            }),
        )
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
        let schedule = Schedule::new(
            Arc::from("test_manual_interrupts"),
            Arc::new(TestComponent {
                name: "test_manual_interrupts".into(),
            }),
        )
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
        let schedule = Schedule::new(
            Arc::from("test_manual_queued_with_interrupt"),
            Arc::new(TestComponent {
                name: "test_manual_queued_with_interrupt".into(),
            }),
        )
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
        let schedule = Schedule::new(
            Arc::from("test_manual_queue_clears_after_immediate"),
            Arc::new(LongComponent {
                name: "test_manual_queue_clears_after_immediate".into(),
                wait: 5,
            }),
        )
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

    #[tokio::test]
    async fn test_adding_schedule_while_running_starts() {
        let schedule = Schedule::new(
            Arc::from("test_adding_schedule_while_running_starts_existing"),
            Arc::new(TestComponent {
                name: Arc::from("test_adding_schedule_while_running_starts_existing"),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler = Scheduler::<NotStarted>::new(
            "test_adding_schedule_while_running_starts".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // add a new schedule while the scheduler has been running for some time
        let new_schedule = Schedule::new(
            Arc::from("test_adding_schedule_while_running_starts_new"),
            Arc::new(TestComponent {
                name: Arc::from("test_adding_schedule_while_running_starts_new"),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));

        scheduler
            .add_schedule(Arc::new(new_schedule))
            .await
            .expect("To add new schedule");
        tokio::time::sleep(Duration::from_secs(5)).await;

        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_adding_schedule_while_running_starts_existing")
            .expect("To get test execution count");
        assert!(
            *count == 9 || *count == 10,
            "Test component should have executed 9 or 10 times, but got {count}"
        );
        let count = map_lock
            .get("test_adding_schedule_while_running_starts_new")
            .expect("To get test execution count");
        assert!(
            *count == 4 || *count == 5,
            "Test component should have executed 4 or 5 times, but got {count}"
        );
    }

    #[tokio::test]
    async fn test_adding_trigger_to_existing_schedule() {
        let schedule = Schedule::new(
            Arc::from("test_adding_trigger_to_existing_schedule"),
            Arc::new(TestComponent {
                name: Arc::from("test_adding_trigger_to_existing_schedule"),
            }),
        );
        let scheduler = Scheduler::<NotStarted>::new(
            "test_adding_trigger_to_existing_schedule".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_secs(5)).await;

        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_adding_trigger_to_existing_schedule")
            .expect("To get test execution count");
        assert!(
            *count == 0,
            "Test component should have executed 0 times, but got {count}"
        );
        drop(map_lock);

        // add a new trigger to the existing schedule
        let new_trigger = Arc::new(RwLock::new(IntervalRequestChannel::new(1)));
        scheduler
            .add_trigger_for_schedule(
                Arc::from("test_adding_trigger_to_existing_schedule"),
                new_trigger,
            )
            .await
            .expect("To add new trigger");

        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.stop().await;
        let map_lock = TEST_EXECUTION_COUNT.read().await;
        let count = map_lock
            .get("test_adding_trigger_to_existing_schedule")
            .expect("To get test execution count");
        assert!(
            *count == 4 || *count == 5,
            "Test component should have executed 4 or 5 times, but got {count}"
        );
    }
}
