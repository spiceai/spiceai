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
        let cpu_cores = num_cpus::get();
        let worker_threads = std::cmp::max(cpu_cores.saturating_sub(1), 1);

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            // Reserve one core for the primary Tokio runtime handling HTTP and control-plane work.
            .worker_threads(worker_threads)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::task::JoinSet;
    use tokio::time::sleep;

    /// Waits for all tasks in the `JoinSet` to complete and reports any errors that
    /// occurred.
    ///
    /// If we don't do this, any errors that occur in the task (such as IO errors)
    /// are not reported.
    async fn drain_join_set(mut join_set: JoinSet<Result<()>>) {
        // retrieve any errors from the tasks
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {}                                   // task completed successfully
                Ok(Err(e)) => tracing::debug!("Task failed: {e}"), // task failed
                Err(e) => tracing::debug!("JoinSet error: {e}"),   // JoinSet error
            }
        }
    }

    #[test]
    fn test_managed_tokio_runtime_creation() {
        let runtime = ManagedTokioRuntime::try_new();
        assert!(runtime.is_ok());

        let _runtime = runtime.expect("Failed to create ManagedTokioRuntime");
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

    #[tokio::test]
    async fn test_drain_join_set_with_successful_tasks() {
        let mut join_set = JoinSet::new();

        // Add some successful tasks
        for i in 0..3 {
            join_set.spawn(async move {
                sleep(Duration::from_millis(i * 10)).await;
                Ok(()) as Result<()>
            });
        }

        // This should complete without panicking
        drain_join_set(join_set).await;
    }

    #[tokio::test]
    async fn test_drain_join_set_with_failed_tasks() {
        let mut join_set = JoinSet::new();

        // Add a mix of successful and failed tasks
        join_set.spawn(async { Ok(()) as Result<()> });
        join_set.spawn(async { Err(Error::TaskExecution) });
        join_set.spawn(async { Ok(()) as Result<()> });

        // This should complete without panicking, even with failed tasks
        drain_join_set(join_set).await;
    }
}
