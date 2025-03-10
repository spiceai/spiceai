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

use std::future::Future;
use std::time::Duration;

use snafu::ResultExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{Error, FailedToExecuteTaskSnafu};

/// A handle for a spawned task that allows external cancellation.
/// 
/// This handle supports both graceful cancellation and forced termination:
/// - If a [`CancellationToken`] is provided, it enables graceful shutdown.
/// - If the task does not exit within the specified timeout after a termination request, it is forcibly aborted.
pub(crate) struct CancellableTaskHandle {
    notify_abort_task: oneshot::Sender<()>,
    cancellation_token: Option<CancellationToken>,
    on_task_completed: oneshot::Receiver<()>,
}

impl CancellableTaskHandle {
    pub async fn cancel(mut self, timeout: Duration) {
        let Some(token) = self.cancellation_token.take() else {
            // The task does not support graceful cancellation, so we abort it.
            // The error is expected if the receiver has already been deallocated, indicating the task has completed or aborted.
            self.notify_abort_task.send(()).ok();
            return;
        };

        // Attempt to gracefully cancel the task and wait for its completion.
        token.cancel();

        tokio::select! {
            () = tokio::time::sleep(timeout) => {
                // If the task hasn't completed within the timeout, we forcefully abort it.
                self.notify_abort_task.send(()).ok();
            }
            // Wait for task completion.
            _ = self.on_task_completed => {}
        };
    }
}

/// Spawns a task that allows external cancellation.
///
/// Returns a future that resolves when the task completes or is canceled,
/// along with a [`CancellableTaskHandle`] for external task control.
pub(crate) fn spawn_cancellable_task<F>(
    task_fn: F,
    task_cancellation: Option<CancellationToken>,
) -> (impl Future<Output = Result<(), Error>>, CancellableTaskHandle)
where
    F: Future<Output = Result<(), Error>> + Send + 'static,
{
    let (notify_abort_task, on_abort_task) = oneshot::channel();
    let (notify_task_completed, on_task_completed) = oneshot::channel();

    let task_handle = CancellableTaskHandle {
        notify_abort_task,
        cancellation_token: task_cancellation,
        on_task_completed,
    };

    let handle: JoinHandle<Result<(), Error>> = tokio::task::spawn(async move {
        let result = tokio::select! {
            res = task_fn => {
                res
            }
            _ = on_abort_task => {
                Ok(())
            }
        };

        notify_task_completed.send(()).ok();

        result
    });

    let task_future = async move {
        match handle.await {
            Ok(result) => result,
            // If task was cancelled (for example during runtime termination), we return Ok (expected behavior).
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(err).context(FailedToExecuteTaskSnafu),
        }
    };

    (task_future, task_handle)
}
