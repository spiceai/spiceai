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
use tokio::{
    runtime::Handle,
    sync::{Notify, mpsc},
    task::JoinSet,
};

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
            tracing::debug!("Shutting down CPU runtime thread...");
            if let Err(e) = thread_join_handle.join() {
                tracing::debug!("Error joining CPU runtime thread: {e:?}");
            } else {
                tracing::debug!("CPU runtime thread shutdown successfully.");
            }
        }
    }
}

impl ManagedTokioRuntime {
    pub fn try_new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()?;
        let handle = runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // Note: runtime is dropped here, which will wait for all tasks
            // to complete
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Return a handle suitable for spawning tasks
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

/// Spawns a task on the provided Tokio runtime and collects its result.
pub async fn spawn_task_and_collect_results<F>(fut: F, tokio_handle: &Handle) -> Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(1);

    let mut join_set = JoinSet::new();

    join_set.spawn_on(
        async move {
            let result = fut.await;
            let _ = tx.send(result).await;
            Ok(()) as Result<()>
        },
        tokio_handle,
    );

    let output = rx.recv().await.ok_or_else(|| Error::TaskExecution);

    drain_join_set(join_set).await;

    output
}

/// Waits for all tasks in the JoinSet to complete and reports any errors that
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
