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

        let scheduler = Scheduler {
            state: Arc::new(Running {
                schedules: Arc::new(RwLock::new(Vec::new())),
                cancellation_token: Arc::clone(&cancellation_token),
                request_handles: Arc::new(RwLock::new(HashMap::new())),
                request_channels: Arc::new(RwLock::new(HashMap::new())),
                submission_channels: Arc::new(RwLock::new(HashMap::new())),
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
        let schedules = self.schedules().await;
        let schedule = schedules
            .iter()
            .find(|s| s.name() == schedule_name)
            .ok_or_else(|| crate::Error::DuplicateScheduleName {
                name: schedule_name.to_string(),
            })?;

        let mut channel = request_channel.write().await;
        channel.set_task_completion_notification(Arc::clone(
            &schedule.notification_channels.completion,
        ));
        channel.set_cancellation_token(Arc::clone(&self.state.cancellation_token));
        channel.set_reset_notification(Arc::clone(&schedule.notification_channels.reset));

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

        for trigger_lock in schedule.triggers() {
            let mut trigger = trigger_lock.write().await;
            trigger.set_task_completion_notification(Arc::clone(
                &schedule.notification_channels.completion,
            ));
            trigger.set_cancellation_token(Arc::clone(&cancellation_token));
            trigger.set_reset_notification(Arc::clone(&schedule.notification_channels.reset));

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
            Arc::clone(&cancellation_token),
        );
        scheduler_handles
            .entry(schedule_name)
            .or_insert_with(Vec::new)
            .push(handle);

        Ok(())
    }

    /// Removes a running schedule from the scheduler.
    ///
    /// # Errors
    ///
    /// - If the schedule with the specified name does not exist.
    pub async fn remove_schedule(&self, schedule_name: Arc<str>) -> Result<()> {
        let mut schedules = self.state.schedules.write().await;
        if let Some(index) = schedules.iter().position(|s| s.name() == schedule_name) {
            schedules.remove(index);
        } else {
            return Err(crate::Error::ScheduleNotFound {
                name: schedule_name.to_string(),
            });
        }

        // Remove the request handles for the schedule
        let handles = self
            .state
            .request_handles
            .write()
            .await
            .remove(&schedule_name);

        if let Some(handles) = handles {
            for handle in handles {
                handle.abort();
                match handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("Request channel completed successfully");
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Request channel execution failed: {e}");
                    }
                    Err(e) => {
                        if !e.is_cancelled() {
                            // Only log errors that are not due to cancellation
                            // (which is expected when stopping the scheduler)
                            tracing::error!("Request channel join error: {e}");
                        }
                    }
                }
            }
        }

        // Remove the scheduler handles for the schedule
        let handles = self
            .state
            .scheduler_handles
            .write()
            .await
            .remove(&schedule_name);

        if let Some(handles) = handles {
            for handle in handles {
                handle.abort();
                match handle.await {
                    Ok(Ok(())) => {
                        tracing::debug!("Scheduler task completed successfully");
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Scheduler task execution failed: {e}");
                    }
                    Err(e) => {
                        if !e.is_cancelled() {
                            // Only log errors that are not due to cancellation
                            // (which is expected when stopping the scheduler)
                            tracing::error!("Scheduler task join error: {e}");
                        }
                    }
                }
            }
        }

        // Remove the request and submission channels for the schedule
        self.state
            .request_channels
            .write()
            .await
            .remove(&schedule_name);
        self.state
            .submission_channels
            .write()
            .await
            .remove(&schedule_name);

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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{sync::LazyLock, time::Duration};
    use tokio::time::Instant;
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

    static TIMING_MAP: LazyLock<RwLock<HashMap<Arc<str>, Vec<Instant>>>> = LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(Arc::from("test_scheduler_timing"), Vec::new());

        RwLock::new(map)
    });

    /// Counts its executions in a counter owned by the test. A plain atomic,
    /// rather than an async lock shared between tests: on the paused clock,
    /// time auto-advances whenever the runtime is idle, including while a task
    /// waits for a lock another test holds, which would move the timeline.
    struct TestComponent {
        executions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ScheduledTask for TestComponent {
        async fn execute(&self) -> Result<()> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Like [`TestComponent`], but each execution takes `wait` seconds.
    struct LongComponent {
        executions: Arc<AtomicUsize>,
        wait: u64,
    }

    #[async_trait]
    impl ScheduledTask for LongComponent {
        async fn execute(&self) -> Result<()> {
            tokio::time::sleep(std::time::Duration::from_secs(self.wait)).await;
            self.executions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Polls `executions` until it reaches `expected`, failing with the last
    /// count seen if that takes longer than `within`. On the paused clock each
    /// poll interval only advances once every task is idle, so work already
    /// queued always finishes before the next check.
    async fn wait_for_executions(executions: &AtomicUsize, expected: usize, within: Duration) {
        let deadline = Instant::now() + within;
        loop {
            let observed = executions.load(Ordering::SeqCst);
            assert!(
                observed <= expected,
                "executed {observed} times, more than the expected {expected}"
            );
            if observed == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "executed {observed} times, expected {expected} within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
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

    /// Runs on the paused clock: an interval trigger fires once per interval,
    /// with ticks at 1s, 2s, ... after start, so stopping at 5.5s (between two
    /// ticks rather than on one) leaves exactly five runs.
    #[tokio::test(start_paused = true)]
    async fn test_scheduler() {
        let executions = Arc::new(AtomicUsize::new(0));
        let schedule = Schedule::new(
            Arc::from("test_scheduler"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler =
            Scheduler::<NotStarted>::new("test_scheduler".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_millis(5500)).await;
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock: the property under test is that the interval trigger
    /// fires once per interval, which a wall-clock reading on a loaded runner cannot
    /// measure. Ticks land at 1s..=9s, so waking at 9.5s avoids a tie with the tenth.
    #[tokio::test(start_paused = true)]
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
        tokio::time::sleep(Duration::from_millis(9500)).await;
        scheduler.stop().await;
        let map_lock = TIMING_MAP.read().await;
        let timings = map_lock
            .get("test_scheduler_timing")
            .expect("To get test execution count");
        let diffs: Vec<Duration> = timings
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .collect();
        assert_eq!(
            diffs.len(),
            8,
            "There should be 8 timing differences, but got {diffs:?}"
        );
        for diff in diffs {
            assert_eq!(
                diff,
                Duration::from_secs(1),
                "Each interval should be exactly 1 second, but got {diff:?}"
            );
        }
    }

    /// Runs on the paused clock, for the same reason as `test_scheduler`: the
    /// two schedules tick independently, five times each by 5.5s.
    #[tokio::test(start_paused = true)]
    async fn test_multi_schedule() {
        let executions_one = Arc::new(AtomicUsize::new(0));
        let executions_two = Arc::new(AtomicUsize::new(0));
        let schedule_one = Schedule::new(
            Arc::from("test_multi_schedule_one"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions_one),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let schedule_two = Schedule::new(
            Arc::from("test_multi_schedule_two"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions_two),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler = Scheduler::<NotStarted>::new(
            "test_multi_schedule".into(),
            vec![Arc::new(schedule_one), Arc::new(schedule_two)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_millis(5500)).await;
        scheduler.stop().await;
        assert_eq!(executions_one.load(Ordering::SeqCst), 5);
        assert_eq!(executions_two.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock: the interval trigger runs at 1s..=4s, and a
    /// manual request sent at 4.5s runs at once, between two ticks. The wait for
    /// it ends before the 5s tick could stand in for it.
    #[tokio::test(start_paused = true)]
    async fn test_multi_evaluator() {
        let executions = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(
            Arc::from("test_multi_evaluator"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))))
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new("test_multi_evaluator".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_millis(4500)).await;
        assert_eq!(executions.load(Ordering::SeqCst), 4);
        tx.send(Some(Arc::new(TaskRequest::default().clears_queue())))
            .await
            .expect("To send task request");
        wait_for_executions(&executions, 5, Duration::from_millis(400)).await;
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock so the bounded waits are exact: each manual
    /// interrupt runs the task once, before the next one is sent.
    #[tokio::test(start_paused = true)]
    async fn test_manual_interrupts() {
        let executions = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(
            Arc::from("test_manual_interrupts"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
            }),
        )
        .add_trigger(manual_channel_lock);
        let scheduler = Scheduler::new("test_manual_interrupts".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        for expected in 1..=3 {
            tx.send(None).await.expect("To send task request");
            wait_for_executions(&executions, expected, Duration::from_secs(1)).await;
        }
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 3);
    }

    /// Runs on the paused clock: all five queued requests run, and nothing
    /// runs after them.
    #[tokio::test(start_paused = true)]
    async fn test_manual_queued_with_interrupt() {
        let executions = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(
            Arc::from("test_manual_queued_with_interrupt"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
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
        wait_for_executions(&executions, 5, Duration::from_secs(1)).await;
        // Time under test: nothing else may run once the queue is drained.
        tokio::time::sleep(Duration::from_secs(7)).await;
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock, where the 5s component and the 1s interval
    /// keep the same timeline but exactly: the interval's first run takes
    /// 1s..6s, the clearing manual request at 3s arrives while it runs and is
    /// dropped, and the next interval run starts at 7s and ends at 12s.
    /// Stopping at 11.5s separates "dropped" (one run) from "queued behind the
    /// running task" (which would have finished a second run at 11s).
    #[tokio::test(start_paused = true)]
    async fn test_manual_queue_clears_after_immediate() {
        let executions = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let manual_channel = ManualRequestChannel::new(rx);
        let manual_channel_lock = Arc::new(RwLock::new(manual_channel));
        let schedule = Schedule::new(
            Arc::from("test_manual_queue_clears_after_immediate"),
            Arc::new(LongComponent {
                executions: Arc::clone(&executions),
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
        tokio::time::sleep(Duration::from_secs(3)).await;
        tx.send(Some(Arc::new(TaskRequest::default().clears_queue())))
            .await
            .expect("To send task request");
        tokio::time::sleep(Duration::from_millis(8500)).await;
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    /// Runs on the paused clock: the existing schedule ticks at whole seconds,
    /// and the schedule added at 5.5s first ticks one interval later, at 6.5s.
    /// Stopping at 10.75s leaves ten runs of the existing schedule (1s..=10s)
    /// and five of the new one (6.5s..=10.5s), with no tick on a wake-up.
    #[tokio::test(start_paused = true)]
    async fn test_adding_schedule_while_running_starts() {
        let existing_executions = Arc::new(AtomicUsize::new(0));
        let new_executions = Arc::new(AtomicUsize::new(0));
        let schedule = Schedule::new(
            Arc::from("test_adding_schedule_while_running_starts_existing"),
            Arc::new(TestComponent {
                executions: Arc::clone(&existing_executions),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler = Scheduler::<NotStarted>::new(
            "test_adding_schedule_while_running_starts".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_millis(5500)).await;
        assert_eq!(existing_executions.load(Ordering::SeqCst), 5);

        // add a new schedule while the scheduler has been running for some time
        let new_schedule = Schedule::new(
            Arc::from("test_adding_schedule_while_running_starts_new"),
            Arc::new(TestComponent {
                executions: Arc::clone(&new_executions),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));

        scheduler
            .add_schedule(Arc::new(new_schedule))
            .await
            .expect("To add new schedule");
        tokio::time::sleep(Duration::from_millis(5250)).await;

        scheduler.stop().await;
        assert_eq!(existing_executions.load(Ordering::SeqCst), 10);
        assert_eq!(new_executions.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock: a schedule without triggers never runs, and
    /// the trigger added at 5s ticks at 6s..=10s before the stop at 10.5s.
    #[tokio::test(start_paused = true)]
    async fn test_adding_trigger_to_existing_schedule() {
        let executions = Arc::new(AtomicUsize::new(0));
        let schedule = Schedule::new(
            Arc::from("test_adding_trigger_to_existing_schedule"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
            }),
        );
        let scheduler = Scheduler::<NotStarted>::new(
            "test_adding_trigger_to_existing_schedule".into(),
            vec![Arc::new(schedule)],
        );
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            0,
            "a schedule without triggers must not run"
        );

        // add a new trigger to the existing schedule
        let new_trigger = Arc::new(RwLock::new(IntervalRequestChannel::new(1)));
        scheduler
            .add_trigger_for_schedule(
                Arc::from("test_adding_trigger_to_existing_schedule"),
                new_trigger,
            )
            .await
            .expect("To add new trigger");

        tokio::time::sleep(Duration::from_millis(5500)).await;
        scheduler.stop().await;
        assert_eq!(executions.load(Ordering::SeqCst), 5);
    }

    /// Runs on the paused clock: five runs (1s..=5s) before the removal at
    /// 5.5s, and not one more in the five seconds after it.
    #[tokio::test(start_paused = true)]
    async fn test_remove_schedule() {
        let executions = Arc::new(AtomicUsize::new(0));
        let schedule = Schedule::new(
            Arc::from("test_remove_schedule"),
            Arc::new(TestComponent {
                executions: Arc::clone(&executions),
            }),
        )
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(1))));
        let scheduler =
            Scheduler::<NotStarted>::new("test_remove_schedule".into(), vec![Arc::new(schedule)]);
        let scheduler = scheduler.start().await.expect("Scheduler should start");
        tokio::time::sleep(Duration::from_millis(5500)).await;
        let before_removal = executions.load(Ordering::SeqCst);
        assert_eq!(before_removal, 5);

        // remove the schedule
        scheduler
            .remove_schedule(Arc::from("test_remove_schedule"))
            .await
            .expect("To remove schedule");

        tokio::time::sleep(Duration::from_secs(5)).await;

        scheduler.stop().await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            before_removal,
            "a removed schedule must not run again"
        );
    }
}
