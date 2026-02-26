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

    /// Returns true if the task has already completed, false otherwise.
    pub fn is_finished(&self) -> bool {
        // returns true when the corresponding receiver has been dropped, which happens when the task completes or is aborted.
        self.notify_abort_task.is_closed()
    }
}

/// Spawns a task that allows external cancellation.
///
/// Returns a future that resolves when the task completes or is canceled,
/// along with a [`CancellableTaskHandle`] for external task control.
pub(crate) fn spawn_cancellable_task<F>(
    task_cancellation: Option<CancellationToken>,
    task_fn: F,
) -> (
    impl Future<Output = Result<(), Error>>,
    CancellableTaskHandle,
)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_task_completes_successfully() {
        let task_fn = async { Ok::<(), Error>(()) };
        let (task_future, _handle) = spawn_cancellable_task(None, task_fn);
        let result = task_future.await;
        result.expect("should complete successfully");
    }

    #[tokio::test]
    async fn test_task_fails() {
        // test that correct error is returned
        let task_fn =
            async { Err::<(), Error>(Error::AcceleratedReadWriteTableWithoutReplication {}) };
        let (task_future, _handle) = spawn_cancellable_task(None, task_fn);
        let result = task_future.await;
        assert!(matches!(
            result,
            Err(Error::AcceleratedReadWriteTableWithoutReplication)
        ));
    }

    #[tokio::test]
    async fn test_task_is_cancelled_gracefully() {
        let cancellation_token = CancellationToken::new();
        let (task_future, handle) =
            spawn_cancellable_task(Some(cancellation_token.clone()), async move {
                // Simulate some work
                cancellation_token.cancelled().await;
                Ok::<(), Error>(())
            });

        // cancel the task async after 100 ms
        let cancel_future = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::select! {
                // We expect the task to be cancelled immediately (before timeout)
                () = handle.cancel(Duration::from_secs(5)) => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    panic!("Timed out waiting for task to complete");
                }
            }
        });

        let (task_result, cancel_result) = tokio::join!(task_future, cancel_future);
        task_result.expect("should complete successfully");
        cancel_result.expect("should complete successfully");
    }

    #[tokio::test]
    async fn test_task_is_aborted() {
        let (task_future, handle) = spawn_cancellable_task(None, async move {
            // Simulate some work
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok::<(), Error>(())
        });

        // cancel the task async after 100 ms
        let cancel_future = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::select! {
                // We expect the task to be cancelled immediately (before timeout)
                () = handle.cancel(Duration::from_secs(5)) => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    panic!("Timed out waiting for task to complete");
                }
            }
        });

        let (task_result, cancel_result) = tokio::join!(task_future, cancel_future);
        task_result.expect("should complete successfully");
        cancel_result.expect("should complete successfully");
    }

    #[tokio::test]
    async fn test_task_can_be_force_aborted() {
        // test that the task can be aborted after timeout when cancellation token is provided
        let (task_future, handle) =
            spawn_cancellable_task(Some(CancellationToken::new()), async move {
                // Simulate some work
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok::<(), Error>(())
            });

        // cancel the task async after 100 ms
        let cancel_future = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::select! {
                // the task must be force aborted after timeout
                () = handle.cancel(Duration::from_millis(200)) => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    panic!("Timed out waiting for task to complete");
                }
            }
        });

        let (task_result, cancel_result) = tokio::join!(task_future, cancel_future);
        task_result.expect("should complete successfully");
        cancel_result.expect("should complete successfully");
    }

    #[tokio::test]
    async fn test_cancel_already_completed_task() {
        let cancellation_token = CancellationToken::new();
        let (task_future, handle) = spawn_cancellable_task(Some(cancellation_token), async move {
            // complete the task immediatly
            Ok::<(), Error>(())
        });

        task_future.await.expect("should complete successfully");

        // attempt to cancel already completed task after 100 ms
        let cancel_result = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tokio::select! {
                // We expect the task to be cancelled immediately (before timeout)
                () = handle.cancel(Duration::from_secs(5)) => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    panic!("Timed out waiting for task to complete");
                }
            }
        })
        .await;

        cancel_result.expect("should complete successfully");
    }

    #[tokio::test]
    async fn test_is_completed() {
        let (task_future, handle) = spawn_cancellable_task(None, async move {
            // Finish task after 100 ms
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), Error>(())
        });

        assert!(!handle.is_finished());

        task_future.await.expect("to complete successfully");

        assert!(handle.is_finished());

        // Verify completion check could be used multiple times
        assert!(handle.is_finished());
    }
}
