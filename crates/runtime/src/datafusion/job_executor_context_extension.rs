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

//! Request context extension for async job execution.

use crate::jobs::JobExecutor;
use runtime_request_context::{Extension, RequestContext};
use std::sync::Arc;

/// Context extension that provides access to the `JobExecutor` for async query operations.
#[derive(Clone)]
pub struct JobExecutorContextExtension {
    executor: Arc<JobExecutor>,
}

impl JobExecutorContextExtension {
    #[must_use]
    pub fn new(executor: Arc<JobExecutor>) -> Self {
        Self { executor }
    }

    #[must_use]
    pub fn executor(&self) -> Arc<JobExecutor> {
        Arc::clone(&self.executor)
    }
}

impl Extension for JobExecutorContextExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Gets the job executor from the current request context.
///
/// Returns `None` if async jobs are not enabled (not in cluster mode).
pub fn get_job_executor(context: &Arc<RequestContext>) -> Option<Arc<JobExecutor>> {
    context
        .extension::<JobExecutorContextExtension>()
        .map(|ext| ext.executor())
}
