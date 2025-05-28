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

use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

use crate::{
    channel::TaskRequestChannel,
    scheduler::{NotificationChannels, TaskRequestChannels},
    task::{RunningTask, ScheduledTask},
};

pub struct Schedule {
    name: Arc<str>,
    triggers: Vec<Arc<RwLock<dyn TaskRequestChannel>>>,
    task: Arc<dyn ScheduledTask>,
}

impl Schedule {
    #[must_use]
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    #[must_use]
    pub fn new(name: Arc<str>, task: Arc<dyn ScheduledTask>) -> Self {
        Self {
            name,
            triggers: Vec::new(),
            task,
        }
    }

    #[must_use]
    pub fn add_trigger(mut self, channel: Arc<RwLock<dyn TaskRequestChannel>>) -> Self {
        self.triggers.push(channel);
        self
    }

    /// Executes the components defined by this schedule.
    pub(crate) fn execute(self: &Arc<Self>, notifier: Arc<Notify>) -> RunningTask {
        let task = Arc::clone(&self.task);
        let handle = tokio::spawn(async move {
            if (task.execute().await).is_err() {
                // Log or handle the error when retry stategy is implemented
            }

            notifier.notify_waiters();

            Ok(())
        });

        RunningTask::new(handle)
    }

    #[must_use]
    pub(crate) fn triggers(&self) -> &Vec<Arc<RwLock<dyn TaskRequestChannel>>> {
        &self.triggers
    }

    async fn finalise_running_task(task: RunningTask) {
        match task.consume_for_handle().await {
            Ok(Ok(())) => {
                tracing::debug!("Task executed successfully");
            }
            Ok(Err(e)) => {
                tracing::error!("Task execution failed: {e}");
            }
            Err(e) => {
                tracing::error!("Task join error: {e}");
            }
        }
    }

    /// Starts the schedule's main loop, returning a `JoinHandle`.
    pub(crate) fn start(
        self: Arc<Self>,
        request_channels: TaskRequestChannels,
        notification_channels: Arc<NotificationChannels>,
        cancellation_token: Arc<tokio_util::sync::CancellationToken>,
    ) -> tokio::task::JoinHandle<crate::Result<()>> {
        let schedule_name = self.name();
        tokio::spawn(async move {
            let rx_lock = {
                let channels = request_channels.read().await;
                channels.get(&schedule_name).cloned()
            };

            if let Some(rx_lock) = rx_lock {
                let mut rx = rx_lock.write().await;
                let mut running_task: Option<RunningTask> = None;
                loop {
                    tokio::select! {
                        () = cancellation_token.cancelled() => {
                            break;
                        }
                        maybe_task = rx.recv() => {
                            match maybe_task {
                                Some(task_request) => {
                                    // clear the queue if requested
                                    if task_request.clear_queue {
                                        for _ in 0..rx.len() {
                                            if let Some(_task) = rx.recv().await {
                                                // Clear the queue
                                            }
                                        }
                                    }

                                    // If there is a running task, check its status or cancel it if required
                                    if let Some(task) = running_task.take() {
                                        match (task.is_finished(), task_request.cancel_running) {
                                            (true, _) => {
                                                Self::finalise_running_task(task).await;
                                            }
                                            (false, true) => {
                                                task.handle.abort();
                                                Self::finalise_running_task(task).await;
                                            }
                                            _ => {
                                                // If the task is still running and not cancelled, put it back
                                                running_task = Some(task);
                                                continue;
                                            }
                                        }
                                    }

                                    // Notify task request channels to reset their clocks if applicable
                                    notification_channels.reset.notify_waiters();

                                    // Execute all components for this schedule
                                    running_task = Some(self.execute(Arc::clone(&notification_channels.completion)));
                                }
                                None => {
                                    // Channel closed, exit loop
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}
