/*
Copyright 2025 The Spice.ai OSS Authors
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

use std::{future::Future, sync::Arc};

use snafu::prelude::*;
use tokio::{runtime::Handle, sync::Notify};

pub mod cancellable_task;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    RuntimeCreation { source: tokio::io::Error },

    #[snafu(display("Expected a result from the task, but nothing was returned"))]
    TaskExecution,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates a separate Tokio [`Runtime`] to isolate latency-sensitive tasks
///
/// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
/// `Runtime` correctly is somewhat tricky. This structure manages the creation
/// and shutdown of a separate thread.
///
/// # Notes
/// On drop, the thread will wait for all remaining tasks to complete.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
pub struct ManagedTokioRuntime {
    /// Handle is the tokio structure for interacting with a Runtime.
    handle: Handle,
    /// Signal to start shutting down
    notify_shutdown: Arc<Notify>,
    /// When thread is active, is Some
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ManagedTokioRuntime {
    fn drop(&mut self) {
        // Notify the thread to shutdown.
        self.notify_shutdown.notify_one();
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, wait for it to finish
            tracing::debug!("Shutting down Tokio runtime thread...");
            if let Err(e) = thread_join_handle.join() {
                tracing::debug!("Error joining Tokio runtime thread: {e:?}");
            } else {
                tracing::debug!("Tokio runtime thread shutdown successfully.");
            }
        }
    }
}

impl ManagedTokioRuntime {
    /// # Errors
    ///
    /// Returns [`Error::RuntimeCreation`] if the Tokio runtime cannot be constructed.
    pub fn try_new() -> Result<Self> {
        Self::builder().build()
    }

    /// Create a builder for configuring the runtime.
    #[must_use]
    pub fn builder() -> ManagedTokioRuntimeBuilder {
        ManagedTokioRuntimeBuilder::new()
    }

    /// Return a handle suitable for spawning tasks
    #[must_use]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

/// Builder for [`ManagedTokioRuntime`] with configuration options.
pub struct ManagedTokioRuntimeBuilder {
    low_priority: bool,
    thread_name: Option<String>,
}

impl Default for ManagedTokioRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ManagedTokioRuntimeBuilder {
    /// Create a new builder with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            low_priority: false,
            thread_name: None,
        }
    }

    /// Set worker threads to run at lower priority (nice value 10 on Unix).
    /// This is useful for background tasks that shouldn't compete with latency-sensitive work.
    #[must_use]
    pub fn with_low_priority(mut self) -> Self {
        self.low_priority = true;
        self
    }

    /// Set a custom thread name prefix for worker threads.
    #[must_use]
    pub fn with_thread_name(mut self, name: impl Into<String>) -> Self {
        self.thread_name = Some(name.into());
        self
    }

    /// Build the runtime.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeCreation`] if the Tokio runtime cannot be constructed.
    pub fn build(self) -> Result<ManagedTokioRuntime> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            // Reserve one core for the primary Tokio runtime handling HTTP and control-plane work.
            .worker_threads(cpu_budget::cpu_budget().dedicated_runtime_worker_threads())
            .enable_all();

        if let Some(name) = &self.thread_name {
            builder.thread_name(name);
        }

        // Set low priority on worker threads if requested (Unix only)
        #[cfg(unix)]
        if self.low_priority {
            builder.on_thread_start(|| {
                // Set nice value to 10 (lower priority than default 0, range is -20 to 19)
                // SAFETY: setpriority is safe to call with PRIO_PROCESS and 0 (current thread)
                unsafe {
                    libc::setpriority(libc::PRIO_PROCESS, 0, 10);
                }
            });
        }

        let runtime = builder.build()?;
        let handle = runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // Note: runtime is dropped here
        });

        Ok(ManagedTokioRuntime {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }
}

/// Spawns a task on the provided Tokio runtime and collects its result.
///
/// # Errors
///
/// Returns [`Error::TaskExecution`] if the task is cancelled or panics before producing a result.
pub async fn spawn_task_and_collect_results<F>(fut: F, tokio_handle: &Handle) -> Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let join_handle = tokio_handle.spawn(fut);
    match join_handle.await {
        Ok(result) => Ok(result),
        Err(_) => Err(Error::TaskExecution),
    }
}

/// True when a failure is the runtime shutting down under a task on the
/// blocking pool, rather than the operation itself failing.
///
/// A sidecar helper that runs on the blocking pool surfaces a shutdown as a
/// cancelled [`tokio::task::JoinError`] wrapped in [`Error::External`], which
/// callers see only as an opaque `"Acceleration error: task ... was cancelled"` —
/// hence classifying by type rather than by message. The task never started, so
/// there is nothing to retry and nothing an operator can act on; a caller that
/// reports its failures at `warn` should report this one below the default level.
///
/// Prefer this over the `RuntimeStatus::is_shutdown()` guard the refresh task uses
/// for the same purpose: `is_shutdown()` is only *coincidental* — every failure that
/// races a shutdown gets quietened, including real ones — whereas the `JoinError`
/// is a *causal* statement that this specific work did not run.
///
/// The condition it reads is "the task was cancelled", and the shutdown reading
/// holds because a `spawn_blocking` task is cancelled only when the runtime is
/// dropped with the task still queued; nothing here calls `JoinHandle::abort`. A
/// caller that starts aborting sidecar tasks (a per-operation timeout, say) has to
/// revisit that.
///
/// The whole source chain is walked, so it holds however deeply the caller has
/// boxed or wrapped the error. A *panicked* task is deliberately not matched: that
/// is a bug and must stay loud.
#[must_use]
pub fn is_shutdown_cancellation(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |error| std::error::Error::source(*error)).any(|error| {
        error
            .downcast_ref::<tokio::task::JoinError>()
            .is_some_and(tokio::task::JoinError::is_cancelled)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    /// Drop joins the runtime's thread, and that thread drops the runtime, so by
    /// the time `drop` returns every task still pending on it has been cancelled
    /// — the shutdown cancellation `is_shutdown_cancellation` classifies.
    #[test]
    fn dropping_the_runtime_cancels_its_pending_tasks_before_returning() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let pending = runtime.handle().spawn(std::future::pending::<()>());
        assert!(!pending.is_finished(), "the task cannot finish on its own");

        drop(runtime);
        assert!(
            pending.is_finished(),
            "drop returns only once the runtime has shut down and cancelled its tasks"
        );

        let join_error = futures::executor::block_on(pending)
            .expect_err("a task on a dropped runtime cannot complete");
        assert!(
            join_error.is_cancelled(),
            "expected a cancellation, got: {join_error}"
        );
        assert!(is_shutdown_cancellation(&join_error));
    }

    #[test]
    fn test_managed_tokio_runtime_handle() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let handle = runtime.handle();

        // Verify we can spawn a task on the handle
        let future = async { 42 };
        let join_handle = handle.spawn(future);

        // We can't easily block on this in a sync test, but we can verify the handle works
        assert!(!join_handle.is_finished());
    }

    #[tokio::test]
    async fn test_spawn_task_and_collect_results_success() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let handle = runtime.handle();

        let future = async { 42u32 };
        let result = spawn_task_and_collect_results(future, handle).await;

        assert!(result.is_ok());
        assert_eq!(result.expect("Failed to get task result"), 42);
    }

    #[tokio::test]
    async fn test_spawn_task_and_collect_results_async_task() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let handle = runtime.handle();

        let future = async {
            sleep(Duration::from_millis(10)).await;
            "hello world"
        };

        let result = spawn_task_and_collect_results(future, handle).await;

        assert!(result.is_ok());
        assert_eq!(
            result.expect("Failed to get async task result"),
            "hello world"
        );
    }

    #[tokio::test]
    async fn test_spawn_task_and_collect_results_with_different_types() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let handle = runtime.handle();

        // Test with Vec<i32>
        let future = async { vec![1, 2, 3, 4, 5] };
        let result = spawn_task_and_collect_results(future, handle).await;
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Failed to get Vec result"),
            vec![1, 2, 3, 4, 5]
        );

        // Test with Option<String>
        let future = async { Some("test".to_string()) };
        let result = spawn_task_and_collect_results(future, handle).await;
        assert!(result.is_ok());
        assert_eq!(
            result.expect("Failed to get Option result"),
            Some("test".to_string())
        );

        // Test with Result<i32, String>
        let future = async { Ok::<i32, String>(100) };
        let result = spawn_task_and_collect_results(future, handle).await;
        assert!(result.is_ok());
        assert_eq!(result.expect("Failed to get Result result"), Ok(100));
    }

    #[tokio::test]
    async fn test_multiple_concurrent_tasks() {
        let runtime = ManagedTokioRuntime::try_new().expect("Failed to create runtime");
        let handle = runtime.handle();

        // Spawn multiple tasks concurrently
        let futures = (0..5).map(|i| {
            spawn_task_and_collect_results(
                async move {
                    sleep(Duration::from_millis(10)).await;
                    i * 2
                },
                handle,
            )
        });

        let results: Result<Vec<_>, _> = futures::future::try_join_all(futures).await;
        assert!(results.is_ok());

        let results = results.expect("Failed to collect concurrent task results");
        assert_eq!(results, vec![0, 2, 4, 6, 8]);
    }
}
