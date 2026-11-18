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

    #[snafu(display("Job {job_id} completed with no rows returned"))]
    NoRowsReturned { job_id: String },

    #[snafu(display("Job {job_id} is not yet complete (status: {status})"))]
    JobNotComplete { job_id: String, status: String },

    #[snafu(display("Jobs API requires cluster mode with scheduler.state_location configured"))]
    ClusterModeRequired,

    #[snafu(display("Failed to read job state from object store: {source}"))]
    ObjectStoreRead { source: ObjectStoreError },

    #[snafu(display("Failed to write job state to object store: {source}"))]
    ObjectStoreWrite {
        source: object_store_occ::ConditionalWriteError,
    },

    #[snafu(display("Failed to delete job state from object store: {source}"))]
    ObjectStoreDelete { source: ObjectStoreError },

    #[snafu(display("Failed to list objects in object store: {source}"))]
    ObjectStoreList { source: ObjectStoreError },

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

    #[snafu(display(
        "Failed to delete distributed job '{job_id}': failed to delete {failed_deletions} of {total_chunks} data chunks."
    ))]
    PartialChunkDeletion {
        job_id: String,
        failed_deletions: usize,
        total_chunks: usize,
    },

    #[snafu(display("Failed to read batch from result stream: {source}"))]
    StreamRead {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display(
        "The results of query '{job_id}' are larger than {}, so the query failed and its results were discarded. Select fewer rows or columns, or aggregate in SQL. See https://spiceai.org/docs/api/http/queries",
        size_limit_text(*maximum_size)
    ))]
    MaximumJobSizeExceeded { job_id: String, maximum_size: u64 },

    #[snafu(display(
        "`maximum_size` is {requested} bytes, above {} (100 GiB), the most an async query can store. Omit it to use 100 GiB, or set a smaller value. See https://spiceai.org/docs/api/http/queries",
        super::ledger::MAX_JOB_RESULT_BYTES
    ))]
    MaximumSizeAboveLimit { requested: u64 },

    #[snafu(display(
        "Failed to submit the async query: you already have {pending} async queries waiting to run, the most one caller may queue. Wait for some to start, or cancel one with POST /v1/queries/{{query_id}}/cancel, then retry. See https://spiceai.org/docs/api/http/queries"
    ))]
    QueueFull { pending: usize },

    #[snafu(display(
        "Failed to submit the async query: your stored query results total {}, the most one caller may keep. {} See https://spiceai.org/docs/api/http/queries",
        byte_size_text(*stored_bytes),
        expiry_text(*oldest_expiry_ms)
    ))]
    ResultBudgetFull {
        stored_bytes: u64,
        oldest_expiry_ms: Option<u64>,
    },

    #[snafu(display("Failed to read the async query budget of a caller: {source}"))]
    DeserializeLedger { source: serde_json::Error },

    #[snafu(display(
        "Concurrent modification detected for job {job_id}. Another scheduler modified the job state."
    ))]
    ConcurrentModification { job_id: String },

    #[snafu(display("Job {job_id} already finished (status: {status}), so it is not run again"))]
    JobAlreadyFinished { job_id: String, status: String },

    #[snafu(display(
        "Job {job_id} is driven by another scheduler, so this scheduler stopped driving it"
    ))]
    JobTakenOver { job_id: String },

    #[snafu(display(
        "`timeout_seconds` is {requested}, above {limit} seconds, this runtime's `runtime.query.timeout` and the longest an async query may run. Omit it to use {limit} seconds, or set a smaller value. See https://spiceai.org/docs/api/http/queries"
    ))]
    TimeoutAboveQueryTimeout { requested: u64, limit: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The size limit a job failed on, for its error: the product limit, or the `maximum_size` the
/// query set.
fn size_limit_text(maximum_size: u64) -> String {
    if maximum_size == super::ledger::MAX_JOB_RESULT_BYTES {
        "100 GiB, the most an async query can store".to_string()
    } else {
        format!(
            "{}, the `maximum_size` the query set",
            byte_size_text(maximum_size)
        )
    }
}

/// `bytes` in the largest binary unit it reaches, to two decimal places without trailing zeros:
/// `100 GiB`, `6.25 TiB`, `1.5 MiB`, `300 bytes`.
pub(crate) fn byte_size_text(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
    ];
    for (unit, size) in UNITS {
        if bytes >= size {
            let hundredths = u128::from(bytes) * 100 / u128::from(size);
            let (whole, fraction) = (hundredths / 100, hundredths % 100);
            return match fraction {
                0 => format!("{whole} {unit}"),
                f if f % 10 == 0 => format!("{whole}.{} {unit}", f / 10),
                f => format!("{whole}.{f:02} {unit}"),
            };
        }
    }
    format!("{bytes} bytes")
}

/// When space frees up for a caller at its stored-results budget.
fn expiry_text(oldest_expiry_ms: Option<u64>) -> String {
    match oldest_expiry_ms.and_then(|ms| {
        chrono::DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX))
    }) {
        Some(at) => format!(
            "Space frees as results expire; the oldest expires at {}.",
            at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
        None => "Space frees as results expire, 12 hours after each query finishes.".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_messages() {
        assert_eq!(
            Error::MaximumSizeAboveLimit {
                requested: 214_748_364_800
            }
            .to_string(),
            "`maximum_size` is 214748364800 bytes, above 107374182400 (100 GiB), the most an async query can store. Omit it to use 100 GiB, or set a smaller value. See https://spiceai.org/docs/api/http/queries"
        );
        assert_eq!(
            Error::QueueFull { pending: 1_000 }.to_string(),
            "Failed to submit the async query: you already have 1000 async queries waiting to run, the most one caller may queue. Wait for some to start, or cancel one with POST /v1/queries/{query_id}/cancel, then retry. See https://spiceai.org/docs/api/http/queries"
        );
        assert_eq!(
            Error::ResultBudgetFull {
                stored_bytes: 6_871_947_673_600,
                oldest_expiry_ms: Some(1_794_949_440_000),
            }
            .to_string(),
            "Failed to submit the async query: your stored query results total 6.25 TiB, the most one caller may keep. Space frees as results expire; the oldest expires at 2026-11-17T21:04:00Z. See https://spiceai.org/docs/api/http/queries"
        );
        assert_eq!(
            Error::MaximumJobSizeExceeded {
                job_id: "01ABC-DEF-456-7890AB".to_string(),
                maximum_size: super::super::ledger::MAX_JOB_RESULT_BYTES,
            }
            .to_string(),
            "The results of query '01ABC-DEF-456-7890AB' are larger than 100 GiB, the most an async query can store, so the query failed and its results were discarded. Select fewer rows or columns, or aggregate in SQL. See https://spiceai.org/docs/api/http/queries"
        );
        assert_eq!(
            Error::MaximumJobSizeExceeded {
                job_id: "01ABC".to_string(),
                maximum_size: 1 << 30,
            }
            .to_string(),
            "The results of query '01ABC' are larger than 1 GiB, the `maximum_size` the query set, so the query failed and its results were discarded. Select fewer rows or columns, or aggregate in SQL. See https://spiceai.org/docs/api/http/queries"
        );
    }
}
