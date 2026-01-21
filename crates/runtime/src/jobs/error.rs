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

use arrow::error::ArrowError;
use object_store::Error as ObjectStoreError;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Job {job_id} not found"))]
    JobNotFound { job_id: String },

    #[snafu(display("Job {job_id} results have expired"))]
    JobResultsExpired { job_id: String },

    #[snafu(display("Result chunk {chunk_index} not found for job {job_id}"))]
    ChunkNotFound { job_id: String, chunk_index: usize },

    #[snafu(display("Job {job_id} is not yet complete (status: {status})"))]
    JobNotComplete { job_id: String, status: String },

    #[snafu(display("Jobs API requires cluster mode with scheduler.state_location configured"))]
    ClusterModeRequired,

    #[snafu(display("Failed to read job state from object store: {source}"))]
    ObjectStoreRead { source: ObjectStoreError },

    #[snafu(display("Failed to write job state to object store: {source}"))]
    ObjectStoreWrite { source: ObjectStoreError },

    #[snafu(display("Failed to delete job state from object store: {source}"))]
    ObjectStoreDelete { source: ObjectStoreError },

    #[snafu(display("Failed to serialize job state: {source}"))]
    SerializeState { source: serde_json::Error },

    #[snafu(display("Failed to deserialize job state: {source}"))]
    DeserializeState { source: serde_json::Error },

    #[snafu(display("Failed to serialize result chunk: {source}"))]
    SerializeChunk { source: ArrowError },

    #[snafu(display("Failed to deserialize result chunk: {source}"))]
    DeserializeChunk { source: ArrowError },

    #[snafu(display("Failed to execute query: {message}"))]
    QueryExecution { message: String },

    #[snafu(display("Job {job_id} was cancelled"))]
    JobCancelled { job_id: String },

    #[snafu(display("Invalid job ID format: {job_id}"))]
    InvalidJobId { job_id: String },

    #[snafu(display("Integer overflow while calculating {field}: {left_value} + {right_value}"))]
    IntegerOverflow {
        field: String,
        left_value: usize,
        right_value: usize,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
