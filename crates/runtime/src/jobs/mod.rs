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

//! Async SQL Query/Jobs API for distributed queries in cluster mode.
//!
//! This module provides a Spark/Databricks/Snowflake-style async query API where:
//! - Queries are submitted and return a job/statement ID immediately
//! - Clients poll for status and retrieve results when ready
//! - Results are chunked and stored in the shared object store
//! - Only works in cluster mode (requires scheduler state location)

mod error;
mod executor;
mod state;
mod store;

use runtime_request_context::{AsyncMarker, Protocol, RequestContext};

pub use error::{Error, Result};
pub use executor::JobExecutor;
pub use state::JobErrorCode;
pub use state::{
    DEFAULT_CHUNK_SIZE, DEFAULT_RESULT_TTL, JobDriver, JobResult, JobResultManifest, JobSchema,
    JobState, JobStatus, PUBLIC_JOB_OWNER,
};
pub use store::JobStore;

/// Who asks about async query jobs: the scope it submits them under, and the jobs it may read
/// and cancel.
///
/// The scope is the storage id of the request's caller namespace
/// (`RequestContext::caller_namespace`), so job ownership follows the principal boundary the
/// caches enforce: every request is `public` when it has no principal, and each principal with
/// a stable id, the local operator included, gets its own opaque scope. A caller reaches the
/// jobs of its own scope; the local operator, which has full access, also reaches the jobs of
/// callers without credentials (`public`), among them every job submitted before it had a
/// scope of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCaller {
    owner: String,
    reaches_public: bool,
}

impl JobCaller {
    /// A caller that reaches only the jobs it owns, under `owner`.
    #[must_use]
    pub fn owning(owner: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            reaches_public: false,
        }
    }

    /// The local operator: its own jobs, and those of callers without credentials.
    #[must_use]
    pub fn local_operator() -> Self {
        Self {
            owner: runtime_auth::anonymous::LOCAL_OPERATOR_ID.to_string(),
            reaches_public: true,
        }
    }

    /// The scope a job this caller submits is owned by.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Whether this caller may read and cancel `job`.
    #[must_use]
    pub fn may_access(&self, job: &JobState) -> bool {
        job.is_owned_by(&self.owner) || (self.reaches_public && job.is_owned_by(PUBLIC_JOB_OWNER))
    }
}

/// The calling request, as a [`JobCaller`].
pub async fn current_job_caller() -> JobCaller {
    let context = RequestContext::current(AsyncMarker::new().await);
    let local_operator = !matches!(context.protocol(), Protocol::Internal)
        && runtime_auth::AuthRequestContext::auth_principal(context.as_ref()).is_some_and(
            |principal| runtime_auth::anonymous::is_local_operator(principal.as_ref()),
        );
    if local_operator {
        JobCaller::local_operator()
    } else {
        JobCaller::owning(context.caller_namespace().storage_id())
    }
}
