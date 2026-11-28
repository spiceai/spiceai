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

use snafu::ResultExt;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::task::TaskRequest;

pub mod cron;
pub mod interval;
pub mod manual;

pub trait TaskRequestChannel: Send + Sync {
    /// Sets the cancellation token for this task request channel.
    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>);
    /// Sets the notification channel to notify when a task is completed.
    fn set_task_completion_notification(&mut self, _notify: Arc<tokio::sync::Notify>) {}
    /// Sets the reset notification channel to notify when the requestor should reset and wait for the next notification.
    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>);
    /// Sets the submission channel to send the task request.
    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>);
    /// Starts the task request channel and returns a handle to the background task.
    #[expect(clippy::missing_errors_doc)]
    fn start(&self) -> Result<JoinHandle<Result<()>>>;
}

/// How one cycle of a timed trigger ended.
pub(crate) enum Cycle {
    /// The trigger sent a request, and a run ended after that.
    Requested,
    /// Another trigger started a run first, a run ended, and the trigger starts its wait over.
    Reset,
    Cancelled,
}

/// One cycle of a timed trigger: wait for `due`, send a request, and wait for the run to end.
///
/// The schedule notifies `reset` and then starts the run, and `task_completion` wakes only the
/// waiters registered when the run ends. Each wait for a run's end is registered before the run
/// it waits for can start, so a run that ends at once still ends the wait. The wait that follows
/// a reset is registered when the sleep starts, so a run that ended during the sleep also ends it.
pub(crate) async fn timed_request_cycle(
    due: impl Future<Output = ()>,
    cancellation: &CancellationToken,
    reset: &Notify,
    task_completion: &Notify,
    tx: &mpsc::Sender<Arc<TaskRequest>>,
) -> Result<Cycle> {
    {
        let other_run_ended = task_completion.notified();
        tokio::pin!(other_run_ended);
        other_run_ended.as_mut().enable();
        tokio::select! {
            () = cancellation.cancelled() => return Ok(Cycle::Cancelled),
            () = reset.notified() => {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(Cycle::Cancelled),
                    () = &mut other_run_ended => return Ok(Cycle::Reset),
                }
            }
            () = due => {}
        }
    }

    let run_ended = task_completion.notified();
    tokio::pin!(run_ended);
    run_ended.as_mut().enable();
    tx.send(Arc::new(TaskRequest::default()))
        .await
        .context(crate::ChannelSendSnafu)?;
    tokio::select! {
        () = cancellation.cancelled() => Ok(Cycle::Cancelled),
        () = &mut run_ended => Ok(Cycle::Requested),
    }
}
