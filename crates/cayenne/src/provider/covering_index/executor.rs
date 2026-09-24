/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Bounded CPU executor for optional covering-index work.

use std::sync::LazyLock;

use rayon::ThreadPool;

use super::{Error, Result};

/// Process-shared Rayon executor for admitted covering-index construction.
///
/// Jobs are accepted only by callers that have already admitted their input
/// and scratch allocations. A dropped oneshot receiver does not cancel its
/// Rayon job: the job owns its reservations and must run to release them.
#[derive(Debug)]
pub(crate) struct CayenneIndexExecutor {
    pool: ThreadPool,
}

impl CayenneIndexExecutor {
    fn new() -> std::result::Result<Self, rayon::ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cpu_budget::cpu_budget().cayenne_index_threads())
            .build()
            .map(|pool| Self { pool })
    }

    /// The single bounded executor, initialized after normal CPU-budget setup.
    pub(crate) fn shared() -> Result<&'static Self> {
        static EXECUTOR: LazyLock<std::result::Result<CayenneIndexExecutor, String>> =
            LazyLock::new(|| CayenneIndexExecutor::new().map_err(|error| error.to_string()));
        EXECUTOR.as_ref().map_err(|error| Error::Unavailable {
            operation: format!("failed to initialize Cayenne index executor: {error}"),
        })
    }

    /// Run an admitted CPU job and return its result through a Tokio oneshot.
    pub(crate) async fn execute<T, F>(&self, job: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            // A receiver cancellation must not stop the build: dropping `job`
            // early would retain its admitted buffers until some unrelated
            // cancellation path eventually observes them.
            let _ = sender.send(job());
        });
        receiver.await.map_err(|error| Error::Unavailable {
            operation: format!("Cayenne index executor stopped before completing a job: {error}"),
        })?
    }
}
