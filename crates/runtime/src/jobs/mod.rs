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

pub use error::{Error, Result};
pub use executor::JobExecutor;
pub use state::{DEFAULT_CHUNK_SIZE, JobResult, JobResultManifest, JobSchema, JobState, JobStatus};
pub use store::JobStore;
