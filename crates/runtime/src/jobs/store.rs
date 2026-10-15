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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, UpdateVersion};
use object_store_occ::lease::{Fenced, FencedPutError, fenced_put};
use object_store_occ::{
    Attempt, ConditionalWriteError, ConflictRetry, Expected, RetryOnConflictError, conditional_put,
    retry_on_conflict,
};
use parking_lot::Mutex;
use snafu::prelude::*;
use uuid::Uuid;

use crate::http::v1::queries::SubmitQueryRequest;
use crate::jobs::state::JobErrorCode;

use super::error::{
    DeserializeChunkSnafu, DeserializeStateSnafu, JobAlreadyFinishedSnafu, ObjectStoreDeleteSnafu,
    ObjectStoreListSnafu, ObjectStoreReadSnafu, Result, SerializeChunkSnafu, SerializeStateSnafu,
};
use super::state::{
    ColumnSchema, DEFAULT_CHUNK_SIZE, DEFAULT_RESULT_TTL, JobDriver, JobResult, JobResultManifest,
    JobSchema, JobState, JobStatus,
};

/// Stores job state and results in the shared object store.
///
/// Layout:
/// ```text
/// {base_prefix}/
/// ├── jobs/
/// │   ├── {job_id}.json          # Job state
/// │   └── {job_id}/
/// │       ├── chunk_0.arrow      # Result chunk 0
/// │       ├── chunk_1.arrow      # Result chunk 1
/// │       └── ...
/// ```
pub struct JobStore {
    store: Arc<dyn ObjectStore>,
    base_prefix: String,
    node_id: String,
    result_ttl: Duration,
    chunk_size: usize,
}

impl std::fmt::Debug for JobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStore")
            .field("base_prefix", &self.base_prefix)
            .field("node_id", &self.node_id)
            .field("result_ttl", &self.result_ttl)
            .field("chunk_size", &self.chunk_size)
            .finish_non_exhaustive()
    }
}

impl JobStore {
    /// Creates a new `JobStore` with the given object store and configuration.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base_prefix: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            base_prefix: base_prefix.into(),
            node_id: node_id.into(),
            result_ttl: DEFAULT_RESULT_TTL,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Sets the result TTL.
    #[must_use]
    pub fn with_result_ttl(mut self, ttl: Duration) -> Self {
        self.result_ttl = ttl;
        self
    }

    /// Sets the chunk size.
    #[must_use]
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Generates a new unique job ID.
    ///
    /// Returns a Databricks-style formatted ID like "01ABC-DEF-456-7890AB".
    /// Uses `UUIDv7` which contains a millisecond timestamp plus random bits.
    #[must_use]
    pub fn generate_job_id() -> String {
        // Format: 01ABC-DEF-456-7890AB style (Databricks-like)
        // UUIDv7 hex structure (32 chars):
        //   0-11:  48-bit ms timestamp
        //   12:    version nibble (always '7')
        //   13-15: 12-bit random
        //   16:    variant nibble (always '8'-'b', only 2 random bits)
        //   17-31: 60-bit random
        //
        // For uniqueness we use timestamp prefix + random suffix from chars 17+
        let uuid = Uuid::now_v7();
        let hex = uuid.simple().to_string();
        format!(
            "{}-{}-{}-{}",
            &hex[0..5],   // timestamp
            &hex[5..8],   // timestamp
            &hex[8..11],  // timestamp
            &hex[17..23]  // 6 chars (24 bits) of pure randomness
        )
        .to_uppercase()
    }

    /// Creates a new pending job and stores it.
    pub async fn create_job(
        &self,
        request: SubmitQueryRequest,
        read_only: bool,
        owner: String,
    ) -> Result<JobState> {
        let job_id = Self::generate_job_id();
        let mut state = JobState::new_pending(job_id, request.sql, request.parameters)
            .with_owner(owner)
            .with_timeout_seconds(request.timeout_seconds)
            .with_maximum_size(request.maximum_size);
        state.read_only = read_only;
        self.write_job_state(&mut state).await?;
        Ok(state)
    }

    /// Gets the current state of a job, refusing one whose results expired.
    pub async fn get_job(&self, job_id: &str) -> Result<JobState> {
        let state = self.get_job_ignoring_expiry(job_id).await?;
        if state.is_expired() {
            return Err(super::error::Error::JobResultsExpired {
                job_id: job_id.to_string(),
            });
        }
        Ok(state)
    }

    /// Gets the current state of a job without applying the expiry check.
    ///
    /// Callers that authorize by owner use this so ownership is resolved
    /// before expiry: otherwise an expired job answers a non-owner with
    /// `JobResultsExpired` while a missing one answers `JobNotFound`, which
    /// tells a caller that someone else's job id exists.
    pub async fn get_job_ignoring_expiry(&self, job_id: &str) -> Result<JobState> {
        let path = self.job_state_path(job_id);
        let result = self.store.get(&path).await.map_err(|e| match e {
            ObjectStoreError::NotFound { .. } => super::error::Error::JobNotFound {
                job_id: job_id.to_string(),
            },
            other => super::error::Error::ObjectStoreRead { source: other },
        })?;

        // Extract version information for conditional writes
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };

        let bytes = result.bytes().await.context(ObjectStoreReadSnafu)?;
        let mut state: JobState = serde_json::from_slice(&bytes).context(DeserializeStateSnafu)?;

        // Store the version for conditional writes
        state.version = Some(version);

        Ok(state)
    }

    /// Updates the job state with conditional write for consistency.
    pub async fn update_job(&self, state: &mut JobState) -> Result<()> {
        self.write_job_state(state).await
    }

    /// Marks a job as running, driven by this store's node under the next
    /// generation.
    ///
    /// A job that already finished is not run again: a cancel that lands between
    /// submission and the executor starting the job would otherwise be flipped back to
    /// running, and the executor, seeing the cancellation, would leave it running forever.
    pub async fn set_job_running(&self, job_id: &str) -> Result<JobState> {
        let mut state = self.get_job(job_id).await?;
        ensure!(
            !state.is_terminal(),
            JobAlreadyFinishedSnafu {
                job_id,
                status: state.status.to_string(),
            }
        );
        state.set_running(self.node_id.clone());
        self.write_job_state(&mut state).await?;
        Ok(state)
    }

    /// Takes over a running job whose driver is gone: this store's node drives
    /// it under the next generation. The takeover lands only while the job is
    /// still running and driven as in `observed`, so of the schedulers that
    /// recover the same job one takes it over and the others get `None`.
    pub async fn take_over_job(&self, observed: &JobState) -> Result<Option<JobState>> {
        let (state, taken) = self
            .modify_job(&observed.job_id, |state| {
                if state.status != JobStatus::Running
                    || state.scheduler_node != observed.scheduler_node
                    || state.generation != observed.generation
                {
                    return Ok(false);
                }
                state.set_running(self.node_id.clone());
                Ok(true)
            })
            .await?;
        Ok(taken.then_some(state))
    }

    /// Marks a job as cancelled. A job that already finished keeps its status.
    pub async fn cancel_job(&self, job_id: &str) -> Result<JobState> {
        self.modify_job(job_id, |state| {
            if state.is_terminal() {
                return Ok(false);
            }
            state.set_cancelled();
            Ok(true)
        })
        .await
        .map(|(state, _)| state)
    }

    /// Writes result chunks for a completed job from a stream of record batches.
    ///
    /// Streams `RecordBatch`es and writes them as Arrow IPC chunks as they arrive,
    /// avoiding loading all results into memory at once. Chunks are flushed when
    /// the configured `chunk_size` row threshold is reached.
    ///
    /// Each chunk is written only while `driver` still drives the job, over a
    /// chunk a former driver may have left at its index.
    ///
    /// Returns the job result manifest.
    pub async fn write_result_chunks_from_stream(
        &self,
        job_id: &str,
        driver: &JobDriver,
        mut stream: SendableRecordBatchStream,
    ) -> Result<JobResult> {
        let state = self.get_job(job_id).await?;

        let schema: SchemaRef = stream.schema();

        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let mut chunk_indices = Vec::new();
        // Cumulative starting row offset of each flushed chunk (rows in all prior chunks).
        let mut chunk_row_offsets = Vec::new();

        // Buffer for accumulating batches until we reach chunk_size
        let mut current_chunk_batches: Vec<RecordBatch> = Vec::new();
        let mut current_chunk_rows = 0usize;
        let mut chunk_index = 0usize;
        // Rows already committed to prior chunks; the starting offset of the next chunk.
        let mut committed_rows = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| super::error::Error::StreamRead {
                source: Box::new(e),
            })?;

            let batch_rows = batch.num_rows();
            total_rows = total_rows.checked_add(batch_rows).ok_or_else(|| {
                super::error::Error::IntegerOverflow {
                    field: "total_row_count".to_string(),
                    left_value: total_rows,
                    right_value: batch_rows,
                }
            })?;

            current_chunk_batches.push(batch);
            current_chunk_rows = current_chunk_rows.checked_add(batch_rows).ok_or_else(|| {
                super::error::Error::IntegerOverflow {
                    field: "chunk_row_count".to_string(),
                    left_value: current_chunk_rows,
                    right_value: batch_rows,
                }
            })?;

            // Flush chunk if we've reached the chunk size
            if current_chunk_rows >= self.chunk_size {
                let bytes = self
                    .write_chunk(job_id, driver, chunk_index, &current_chunk_batches)
                    .await?;
                total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                    super::error::Error::IntegerOverflow {
                        field: "total_byte_count".to_string(),
                        left_value: total_bytes,
                        right_value: bytes,
                    }
                })?;

                if let Some(maximum_size) = state.maximum_size
                    && (total_bytes as u64) > maximum_size
                {
                    return Err(super::error::Error::MaximumJobSizeExceeded { maximum_size });
                }

                chunk_indices.push(chunk_index);
                chunk_row_offsets.push(committed_rows);
                committed_rows =
                    committed_rows
                        .checked_add(current_chunk_rows)
                        .ok_or_else(|| super::error::Error::IntegerOverflow {
                            field: "chunk_row_offset".to_string(),
                            left_value: committed_rows,
                            right_value: current_chunk_rows,
                        })?;
                chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
                    super::error::Error::IntegerOverflow {
                        field: "chunk_index".to_string(),
                        left_value: chunk_index,
                        right_value: 1,
                    }
                })?;
                current_chunk_batches.clear();
                current_chunk_rows = 0;
            }
        }

        // Flush remaining batches
        if !current_chunk_batches.is_empty() {
            let bytes = self
                .write_chunk(job_id, driver, chunk_index, &current_chunk_batches)
                .await?;
            total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                super::error::Error::IntegerOverflow {
                    field: "total_byte_count".to_string(),
                    left_value: total_bytes,
                    right_value: bytes,
                }
            })?;

            if let Some(maximum_size) = state.maximum_size
                && (total_bytes as u64) > maximum_size
            {
                return Err(super::error::Error::MaximumJobSizeExceeded { maximum_size });
            }

            chunk_indices.push(chunk_index);
            chunk_row_offsets.push(committed_rows);
        }

        Ok(Self::build_job_result(
            &schema,
            total_rows,
            total_bytes,
            chunk_indices.len(),
            chunk_indices,
            chunk_row_offsets,
        ))
    }

    /// Builds a `JobResult` from the given schema and result statistics.
    fn build_job_result(
        schema: &SchemaRef,
        total_rows: usize,
        total_bytes: usize,
        total_chunks: usize,
        chunk_indices: Vec<usize>,
        chunk_row_offsets: Vec<usize>,
    ) -> JobResult {
        // Build schema info - use Display instead of Debug for stable type names
        let columns: Vec<ColumnSchema> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                // Extract precision and scale for decimal types
                let (type_precision, type_scale) = match field.data_type() {
                    arrow::datatypes::DataType::Decimal128(precision, scale)
                    | arrow::datatypes::DataType::Decimal256(precision, scale) => {
                        (Some(u32::from(*precision)), Some(i32::from(*scale)))
                    }
                    _ => (None, None),
                };
                ColumnSchema {
                    name: field.name().clone(),
                    type_name: field.data_type().to_string(),
                    type_precision,
                    type_scale,
                    nullable: field.is_nullable(),
                    position: i,
                }
            })
            .collect();

        JobResult {
            manifest: JobResultManifest {
                format: "ARROW_IPC".to_string(),
                schema: JobSchema {
                    column_count: columns.len(),
                    columns,
                },
                total_row_count: total_rows,
                total_chunk_count: total_chunks,
                total_byte_count: total_bytes,
            },
            chunk_indices,
            chunk_row_offsets,
        }
    }

    /// Writes a single chunk to the object store while `driver` drives the job.
    async fn write_chunk(
        &self,
        job_id: &str,
        driver: &JobDriver,
        chunk_index: usize,
        batches: &[RecordBatch],
    ) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }

        let schema = batches[0].schema();
        let mut buffer = Vec::new();

        {
            let mut writer =
                StreamWriter::try_new(&mut buffer, &schema).context(SerializeChunkSnafu)?;

            for batch in batches {
                writer.write(batch).context(SerializeChunkSnafu)?;
            }

            writer.finish().context(SerializeChunkSnafu)?;
        }

        let path = self.chunk_path(job_id, chunk_index);
        let bytes_len = buffer.len();

        let written = fenced_put(
            self.store.as_ref(),
            &path,
            buffer.into(),
            &ConflictRetry::default(),
            || async {
                Ok(self
                    .get_job_ignoring_expiry(job_id)
                    .await?
                    .is_driven_by(driver))
            },
        )
        .await
        .map_err(|e| match e {
            FencedPutError::Check(e) => e,
            FencedPutError::Store(source) => super::error::Error::ObjectStoreWrite { source },
            FencedPutError::Contention { .. } => super::error::Error::ConcurrentModification {
                job_id: job_id.to_string(),
            },
        })?;
        match written {
            Fenced::Written(_) => Ok(bytes_len),
            Fenced::Refused => Err(super::error::Error::JobTakenOver {
                job_id: job_id.to_string(),
            }),
        }
    }

    /// Reads a result chunk from the object store.
    pub async fn read_chunk(&self, job_id: &str, chunk_index: usize) -> Result<Vec<RecordBatch>> {
        let path = self.chunk_path(job_id, chunk_index);

        let result = self.store.get(&path).await.map_err(|e| match e {
            ObjectStoreError::NotFound { .. } => super::error::Error::ChunkNotFound {
                job_id: job_id.to_string(),
                chunk_index,
            },
            other => super::error::Error::ObjectStoreRead { source: other },
        })?;

        let bytes = result.bytes().await.context(ObjectStoreReadSnafu)?;
        let cursor = std::io::Cursor::new(bytes.as_ref());
        let reader = StreamReader::try_new(cursor, None).context(DeserializeChunkSnafu)?;

        // Collect all batches, propagating any errors that occur during deserialization
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(DeserializeChunkSnafu)?;

        Ok(batches)
    }

    /// Marks a job as succeeded with the given results, while `driver` drives it.
    ///
    /// The first terminal state wins: a job that was cancelled or failed while its
    /// results were being written stays that way.
    pub async fn complete_job(
        &self,
        job_id: &str,
        driver: &JobDriver,
        result: JobResult,
    ) -> Result<JobState> {
        self.modify_job(job_id, |state| {
            if state.is_terminal() {
                tracing::debug!(
                    job_id,
                    status = %state.status,
                    "Job already finished; not marking it succeeded"
                );
                return Ok(false);
            }
            ensure_driven_by(state, driver)?;
            state.set_succeeded(result.clone(), self.result_ttl);
            Ok(true)
        })
        .await
        .map(|(state, _)| state)
    }

    /// Marks a job as failed with the given error, while `driver` drives it.
    ///
    /// The first terminal state wins: a job that already succeeded or was cancelled
    /// keeps that status.
    pub async fn fail_job(
        &self,
        job_id: &str,
        driver: &JobDriver,
        error_code: JobErrorCode,
        message: impl Into<String>,
    ) -> Result<JobState> {
        let error = super::state::JobError {
            error_code,
            message: message.into(),
            sql_state: None,
        };
        self.modify_job(job_id, |state| {
            if state.is_terminal() {
                tracing::debug!(
                    job_id,
                    status = %state.status,
                    "Job already finished; not marking it failed"
                );
                return Ok(false);
            }
            ensure_driven_by(state, driver)?;
            state.set_failed(error.clone());
            Ok(true)
        })
        .await
        .map(|(state, _)| state)
    }

    /// Deletes a job and all its result chunks.
    ///
    /// This method attempts to delete all chunks before deleting the job state.
    /// If any chunk deletions fail, the operation returns an error without
    /// deleting the job state to avoid orphaning chunks.
    pub async fn delete_job(&self, job_id: &str) -> Result<()> {
        // Delete all chunks first
        let chunks_prefix = self.job_chunks_prefix(job_id);
        let mut stream = self.store.list(Some(&chunks_prefix));

        let mut total_chunks = 0usize;
        let mut failed_deletions = 0usize;

        while let Some(entry) = stream.next().await {
            let meta = entry.context(ObjectStoreListSnafu)?;
            total_chunks = total_chunks.saturating_add(1);

            if let Err(err) = self.store.delete(&meta.location).await
                && !matches!(err, ObjectStoreError::NotFound { .. })
            {
                tracing::warn!(
                    job_id,
                    path = %meta.location,
                    error = %err,
                    "Failed to delete job chunk from object store"
                );
                failed_deletions = failed_deletions.saturating_add(1);
            }
        }

        // If any chunks failed to delete, return an error to avoid orphaning data
        if failed_deletions > 0 {
            return Err(super::error::Error::PartialChunkDeletion {
                job_id: job_id.to_string(),
                failed_deletions,
                total_chunks,
            });
        }

        // Delete job state. Another scheduler's cleanup may have deleted it first.
        let path = self.job_state_path(job_id);
        match self.store.delete(&path).await {
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            result => result.context(ObjectStoreDeleteSnafu),
        }
    }

    /// Lists all jobs, optionally filtered by status.
    ///
    /// Returns an error if any job state file fails to be read or deserialized.
    pub async fn list_jobs(&self, status_filter: Option<JobStatus>) -> Result<Vec<JobState>> {
        let jobs_prefix = self.jobs_prefix();
        let mut stream = self.store.list(Some(&jobs_prefix));
        let mut jobs = Vec::new();

        while let Some(entry) = stream.next().await {
            let meta = entry.context(ObjectStoreListSnafu)?;

            // Only process .json files (job state, not chunks)
            if !std::path::Path::new(meta.location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                continue;
            }

            let result = self
                .store
                .get(&meta.location)
                .await
                .context(ObjectStoreReadSnafu)?;
            let bytes = result.bytes().await.context(ObjectStoreReadSnafu)?;
            let state: JobState = serde_json::from_slice(&bytes).context(DeserializeStateSnafu)?;

            // Apply status filter
            if status_filter.is_some_and(|filter| state.status != filter) {
                continue;
            }

            // Skip expired jobs
            if state.is_expired() {
                continue;
            }

            jobs.push(state);
        }

        // Sort by created_at descending (newest first)
        jobs.sort_by_key(|b| std::cmp::Reverse(b.created_at_ms));

        Ok(jobs)
    }

    /// Cleans up expired jobs and their results.
    ///
    /// This method uses best-effort cleanup. For listing errors, individual job state
    /// retrieval errors, and deletion errors, the operation logs warnings and continues
    /// to clean up as many expired jobs as possible.
    ///
    /// Returns the count of successfully deleted jobs.
    pub async fn cleanup_expired_jobs(&self) -> Result<usize> {
        let jobs_prefix = self.jobs_prefix();
        let mut stream = self.store.list(Some(&jobs_prefix));
        let mut deleted_count = 0usize;

        while let Some(entry) = stream.next().await {
            let meta = match entry {
                Ok(m) => m,
                Err(e) => {
                    // Log and continue to process remaining entries in the stream
                    tracing::warn!(
                        "Failed to list distributed job during expired job cleanup: {e}. This job will be skipped."
                    );
                    continue;
                }
            };

            // Only process .json files
            if !std::path::Path::new(meta.location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                continue;
            }

            let result = match self.store.get(&meta.location).await {
                Ok(r) => r,
                Err(ObjectStoreError::NotFound { .. }) => {
                    // Job was deleted between list and get - this is expected
                    tracing::debug!(path = %meta.location, "Job state file not found during cleanup (likely deleted during list operation), skipping");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to read distributed job state at path '{path}' during cleanup: {e}. This job will be skipped.",
                        path = meta.location
                    );
                    continue;
                }
            };

            let bytes = match result.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read distributed job state bytes at path '{path}' during cleanup: {e}. This job will be skipped.",
                        path = meta.location
                    );
                    continue;
                }
            };

            let state = match serde_json::from_slice::<JobState>(&bytes) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        "Failed to read distributed job state at path '{path}' during cleanup: {e}. This job will be skipped.",
                        path = meta.location
                    );
                    continue;
                }
            };

            if state.is_expired() {
                match self.delete_job(&state.job_id).await {
                    Ok(()) => {
                        // Saturate at MAX on overflow - cleanup count is informational only
                        deleted_count = deleted_count.saturating_add(1);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to delete expired distributed job '{job_id}' during cleanup: {e}",
                            job_id = state.job_id
                        );
                    }
                }
            }
        }

        Ok(deleted_count)
    }

    fn jobs_prefix(&self) -> Path {
        join_path(&self.base_prefix, "jobs")
    }

    fn job_state_path(&self, job_id: &str) -> Path {
        join_path(&self.base_prefix, &format!("jobs/{job_id}.json"))
    }

    fn job_chunks_prefix(&self, job_id: &str) -> Path {
        join_path(&self.base_prefix, &format!("jobs/{job_id}"))
    }

    fn chunk_path(&self, job_id: &str, chunk_index: usize) -> Path {
        join_path(
            &self.base_prefix,
            &format!("jobs/{job_id}/chunk_{chunk_index}.arrow"),
        )
    }

    /// Applies `change` to the job's current state and writes it back, starting
    /// again from a fresh read when another writer changed the job first.
    /// `change` returns `false` to leave the job as it is; the result says
    /// whether the change was written.
    async fn modify_job(
        &self,
        job_id: &str,
        change: impl FnMut(&mut JobState) -> Result<bool> + Send,
    ) -> Result<(JobState, bool)> {
        let change = Mutex::new(change);
        retry_on_conflict(&ConflictRetry::default(), || async {
            let mut state = self.get_job(job_id).await?;
            if !(*change.lock())(&mut state)? {
                return Ok(Attempt::Done((state, false)));
            }
            match self.write_job_state(&mut state).await {
                Ok(()) => Ok(Attempt::Done((state, true))),
                Err(super::error::Error::ConcurrentModification { .. }) => Ok(Attempt::Conflict),
                Err(e) => Err(e),
            }
        })
        .await
        .map_err(|err| match err {
            RetryOnConflictError::ConflictsExhausted { .. } => {
                super::error::Error::ConcurrentModification {
                    job_id: job_id.to_string(),
                }
            }
            RetryOnConflictError::Failed(err) => err,
        })
    }

    /// Writes the job's state if it is still at the version it was read at, or
    /// creates it when it has no version yet.
    async fn write_job_state(&self, state: &mut JobState) -> Result<()> {
        let path = self.job_state_path(&state.job_id);
        let payload = serde_json::to_vec(state).context(SerializeStateSnafu)?;

        let expected = match &state.version {
            Some(version) => Expected::at_version(version.clone()).ok_or_else(|| {
                super::error::Error::ObjectStoreWrite {
                    source: ConditionalWriteError::Unsupported {
                        path: path.to_string(),
                        source: ObjectStoreError::NotSupported {
                            source: "the store reported neither an ETag nor a version".into(),
                        },
                    },
                }
            })?,
            None => Expected::Absent,
        };

        let result = conditional_put(self.store.as_ref(), &path, payload.into(), &expected)
            .await
            .map_err(|e| match e {
                ConditionalWriteError::Conflict { .. } => {
                    super::error::Error::ConcurrentModification {
                        job_id: state.job_id.clone(),
                    }
                }
                source => super::error::Error::ObjectStoreWrite { source },
            })?;

        // Update the version for future writes
        state.version = Some(UpdateVersion::from(result));

        Ok(())
    }
}

/// Refuses a write by `driver` to a job that another scheduler took over.
fn ensure_driven_by(state: &JobState, driver: &JobDriver) -> Result<()> {
    if state.is_driven_by(driver) {
        Ok(())
    } else {
        Err(super::error::Error::JobTakenOver {
            job_id: state.job_id.clone(),
        })
    }
}

fn join_path(prefix: &str, suffix: &str) -> Path {
    if prefix.is_empty() {
        Path::from(suffix)
    } else {
        Path::from(format!("{prefix}/{suffix}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;

    use crate::http::v1::queries::SubmitQueryRequest;

    fn test_owner() -> String {
        crate::jobs::PUBLIC_JOB_OWNER.to_string()
    }

    /// Starts the job, and returns the driver its writes go through.
    async fn start(job_store: &JobStore, job_id: &str) -> JobDriver {
        job_store
            .set_job_running(job_id)
            .await
            .expect("to start the job")
            .driver()
            .expect("a started job has a driver")
    }

    fn make_request(sql: impl Into<String>) -> SubmitQueryRequest {
        SubmitQueryRequest {
            sql: sql.into(),
            parameters: None,
            timeout_seconds: None,
            maximum_size: None,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_job() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        let state = job_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job");

        assert_eq!(state.status, JobStatus::Pending);
        assert_eq!(state.sql, "SELECT 1");

        let retrieved = job_store.get_job(&state.job_id).await.expect("to get job");

        assert_eq!(retrieved.job_id, state.job_id);
        assert_eq!(retrieved.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_job_lifecycle() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        // Create job
        let state = job_store
            .create_job(make_request("SELECT * FROM test"), false, test_owner())
            .await
            .expect("to create job");
        let job_id = state.job_id.clone();

        // Set running
        let running = job_store
            .set_job_running(&job_id)
            .await
            .expect("to set running");
        assert_eq!(running.status, JobStatus::Running);
        assert_eq!(running.scheduler_node.as_deref(), Some("node-1"));

        let driver = running.driver().expect("a running job has a driver");
        let completed = job_store
            .complete_job(&job_id, &driver, empty_result())
            .await
            .expect("to complete job");
        assert_eq!(completed.status, JobStatus::Succeeded);
        assert!(completed.expires_at_ms.is_some());
    }

    fn empty_result() -> JobResult {
        JobResult {
            manifest: JobResultManifest {
                format: "ARROW_IPC".to_string(),
                schema: JobSchema {
                    column_count: 0,
                    columns: vec![],
                },
                total_row_count: 0,
                total_chunk_count: 0,
                total_byte_count: 0,
            },
            chunk_indices: vec![],
            chunk_row_offsets: vec![],
        }
    }

    /// A cancel that lands before the executor starts the job must stick: starting it
    /// would flip it back to running, and the executor, seeing the cancellation, would
    /// then leave it running forever.
    #[tokio::test]
    async fn a_cancelled_job_is_not_started() {
        let job_store = JobStore::new(Arc::new(InMemory::new()), "test", "node-1");
        let job_id = job_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        job_store.cancel_job(&job_id).await.expect("to cancel");

        let err = job_store
            .set_job_running(&job_id)
            .await
            .expect_err("a cancelled job must not start");
        assert!(
            matches!(err, super::super::error::Error::JobAlreadyFinished { .. }),
            "{err}"
        );
        let state = job_store.get_job(&job_id).await.expect("to get job");
        assert_eq!(state.status, JobStatus::Cancelled);
    }

    /// The first terminal state wins: completing or failing a finished job keeps the
    /// status it finished with.
    #[tokio::test]
    async fn a_finished_job_keeps_its_first_terminal_state() {
        let job_store = JobStore::new(Arc::new(InMemory::new()), "test", "node-1");

        let cancelled = job_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        let driver = start(&job_store, &cancelled).await;
        job_store.cancel_job(&cancelled).await.expect("to cancel");
        let state = job_store
            .complete_job(&cancelled, &driver, empty_result())
            .await
            .expect("completing a cancelled job is a no-op");
        assert_eq!(state.status, JobStatus::Cancelled);

        let succeeded = job_store
            .create_job(make_request("SELECT 2"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        let driver = start(&job_store, &succeeded).await;
        job_store
            .complete_job(&succeeded, &driver, empty_result())
            .await
            .expect("to complete");
        let state = job_store
            .fail_job(&succeeded, &driver, JobErrorCode::Internal, "late failure")
            .await
            .expect("failing a succeeded job is a no-op");
        assert_eq!(state.status, JobStatus::Succeeded);
        assert!(state.error.is_none());
    }

    /// Schedulers that finish one job at the same time on one local directory
    /// all succeed and agree on the one terminal state that landed first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_finishers_agree_on_the_first_terminal_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store_occ::LocalConditionalPut::new(dir.path()).expect("local store"));
        let stores: Vec<_> = (0..3)
            .map(|n| {
                Arc::new(JobStore::new(
                    Arc::clone(&store),
                    "test",
                    format!("node-{n}"),
                ))
            })
            .collect();
        for round in 0..10 {
            let job_id = stores[0]
                .create_job(make_request(format!("SELECT {round}")), false, test_owner())
                .await
                .expect("to create job")
                .job_id;
            let driver = start(&stores[0], &job_id).await;

            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let finishers: Vec<_> = stores
                .iter()
                .enumerate()
                .map(|(n, job_store)| {
                    let (job_store, job_id, barrier, driver) = (
                        Arc::clone(job_store),
                        job_id.clone(),
                        Arc::clone(&barrier),
                        driver.clone(),
                    );
                    tokio::spawn(async move {
                        barrier.wait().await;
                        match n {
                            0 => {
                                job_store
                                    .complete_job(&job_id, &driver, empty_result())
                                    .await
                            }
                            1 => {
                                job_store
                                    .fail_job(&job_id, &driver, JobErrorCode::Internal, "failed")
                                    .await
                            }
                            _ => job_store.cancel_job(&job_id).await,
                        }
                    })
                })
                .collect();
            let mut statuses = Vec::new();
            for finisher in finishers {
                statuses.push(
                    finisher
                        .await
                        .expect("finisher task")
                        .expect("a lost race is not an error")
                        .status,
                );
            }
            let landed = stores[0].get_job(&job_id).await.expect("to get job").status;
            assert!(
                statuses.iter().all(|status| *status == landed),
                "round {round}: every finisher must report the stored status {landed:?}: {statuses:?}"
            );
        }
    }

    /// Expired jobs are deleted together with their result chunks; live jobs stay.
    #[tokio::test]
    async fn cleanup_deletes_expired_jobs_and_their_chunks() {
        let store = Arc::new(InMemory::new());
        let expired_store =
            JobStore::new(Arc::clone(&store) as Arc<dyn ObjectStore>, "test", "node-1")
                .with_result_ttl(Duration::ZERO);
        let live_store =
            JobStore::new(Arc::clone(&store) as Arc<dyn ObjectStore>, "test", "node-1");

        let expired = expired_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        let expired_driver = start(&expired_store, &expired).await;
        store
            .put(
                &expired_store.chunk_path(&expired, 0),
                b"chunk".to_vec().into(),
            )
            .await
            .expect("write a chunk");
        expired_store
            .complete_job(&expired, &expired_driver, empty_result())
            .await
            .expect("to complete");

        let live = live_store
            .create_job(make_request("SELECT 2"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        let live_driver = start(&live_store, &live).await;
        live_store
            .complete_job(&live, &live_driver, empty_result())
            .await
            .expect("to complete");

        let deleted = live_store
            .cleanup_expired_jobs()
            .await
            .expect("cleanup succeeds");
        assert_eq!(deleted, 1);

        let remaining: Vec<_> = futures::TryStreamExt::try_collect(store.list(None))
            .await
            .expect("list");
        let remaining: Vec<String> = remaining.iter().map(|m| m.location.to_string()).collect();
        assert!(
            remaining
                .iter()
                .all(|path| !path.contains(expired.as_str())),
            "the expired job and its chunks are gone: {remaining:?}"
        );
        assert!(
            remaining.iter().any(|path| path.contains(live.as_str())),
            "the live job stays: {remaining:?}"
        );
    }

    /// Two schedulers' cleanups can both try to delete the same job.
    #[tokio::test]
    async fn deleting_an_already_deleted_job_succeeds() {
        let job_store = JobStore::new(Arc::new(InMemory::new()), "test", "node-1");
        let job_id = job_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job")
            .job_id;
        job_store.delete_job(&job_id).await.expect("first delete");
        // The first delete removed the job, so the second runs against an
        // absent object.
        let gone = job_store.get_job(&job_id).await;
        assert!(
            matches!(
                &gone,
                Err(super::super::error::Error::JobNotFound { job_id: missing }) if *missing == job_id
            ),
            "the first delete must remove the job, got {gone:?}"
        );
        job_store
            .delete_job(&job_id)
            .await
            .expect("a second delete is not an error");
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        let state = job_store
            .create_job(make_request("SELECT 1"), false, test_owner())
            .await
            .expect("to create job");

        let cancelled = job_store
            .cancel_job(&state.job_id)
            .await
            .expect("to cancel job");

        assert_eq!(cancelled.status, JobStatus::Cancelled);
    }

    #[test]
    fn test_generate_job_id() {
        // Generate multiple IDs to verify uniqueness
        let ids: std::collections::HashSet<String> =
            (0..100).map(|_| JobStore::generate_job_id()).collect();

        // All 100 IDs should be unique
        assert_eq!(ids.len(), 100, "All generated job IDs should be unique");

        // Verify format of generated IDs
        for id in &ids {
            assert_eq!(id.len(), 20, "Job ID should be 20 characters"); // 5 + 1 + 3 + 1 + 3 + 1 + 6
            assert_eq!(id.matches('-').count(), 3, "Job ID should have 3 dashes");
        }
    }

    mod write_result_chunks_from_stream {
        use super::*;
        use crate::jobs::Error;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;

        fn create_test_stream(
            schema: SchemaRef,
            batches: Vec<RecordBatch>,
        ) -> SendableRecordBatchStream {
            let batch_stream = stream::iter(batches.into_iter().map(Ok));
            Box::pin(RecordBatchStreamAdapter::new(schema, batch_stream))
        }

        #[tokio::test]
        async fn test_empty_stream() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            let state = job_store
                .create_job(
                    make_request("SELECT * FROM test WHERE 1=0"),
                    false,
                    test_owner(),
                )
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let stream = create_test_stream(Arc::clone(&schema), vec![]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write empty stream");

            assert_eq!(result.manifest.total_row_count, 0);
            assert_eq!(result.manifest.total_chunk_count, 0);
            assert!(result.chunk_indices.is_empty());
            assert!(result.chunk_row_offsets.is_empty());

            // Schema should be preserved even with empty stream
            assert_eq!(result.manifest.schema.column_count, 2);
            assert_eq!(result.manifest.schema.columns[0].name, "id");
            assert_eq!(result.manifest.schema.columns[1].name, "value");
        }

        #[tokio::test]
        async fn test_single_batch() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(100);

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write single batch stream");

            assert_eq!(result.manifest.total_row_count, 3);
            assert_eq!(result.manifest.total_chunk_count, 1);
            assert_eq!(result.chunk_indices, vec![0]);
            assert_eq!(result.chunk_row_offsets, vec![0]);

            // Verify chunk can be read back
            let chunks = job_store
                .read_chunk(&state.job_id, 0)
                .await
                .expect("to read chunk");
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].num_rows(), 3);
        }

        #[tokio::test]
        async fn test_multiple_batches_single_chunk() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(100);

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .expect("to create batch1");

            let batch2 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![3, 4, 5]))],
            )
            .expect("to create batch2");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch1, batch2]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream");

            assert_eq!(result.manifest.total_row_count, 5);
            // All batches fit in one chunk since chunk_size is 100
            assert_eq!(result.manifest.total_chunk_count, 1);
            assert_eq!(result.chunk_indices, vec![0]);
            assert_eq!(result.chunk_row_offsets, vec![0]);
        }

        #[tokio::test]
        async fn test_multiple_chunks() {
            let store = Arc::new(InMemory::new());
            // Set chunk size to 2 rows to force multiple chunks
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(2);

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .expect("to create batch1");

            let batch2 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![3, 4]))],
            )
            .expect("to create batch2");

            let batch3 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![5]))],
            )
            .expect("to create batch3");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch1, batch2, batch3]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream");

            assert_eq!(result.manifest.total_row_count, 5);
            // chunk_size=2, so:
            // - chunk 0: batch1 (2 rows) -> flushed
            // - chunk 1: batch2 (2 rows) -> flushed
            // - chunk 2: batch3 (1 row) -> flushed at end
            assert_eq!(result.manifest.total_chunk_count, 3);
            assert_eq!(result.chunk_indices, vec![0, 1, 2]);
            // Each batch is exactly chunk_size rows, so offsets are 0, 2, 4.
            assert_eq!(result.chunk_row_offsets, vec![0, 2, 4]);

            // Verify all chunks can be read
            for i in 0..3 {
                let chunks = job_store
                    .read_chunk(&state.job_id, i)
                    .await
                    .expect("to read chunk");
                assert!(!chunks.is_empty());
            }
        }

        #[tokio::test]
        async fn test_chunk_row_offsets_with_oversized_batches() {
            // Regression test for #11271: chunks are flushed once the row threshold is
            // reached but contain whole batches, so a chunk routinely holds MORE than
            // chunk_size rows. The recorded offset must be the cumulative row count of
            // prior chunks, not `chunk_index * chunk_size`.
            let store = Arc::new(InMemory::new());
            // chunk_size=2 but each batch is 3 rows, so every chunk overshoots.
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(2);

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let make_batch = |vals: Vec<i32>| {
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vals))])
                    .expect("to create batch")
            };

            // Three 3-row batches: each one alone trips the threshold and flushes.
            let batches = vec![
                make_batch(vec![1, 2, 3]),
                make_batch(vec![4, 5, 6]),
                make_batch(vec![7, 8, 9]),
            ];

            let stream = create_test_stream(Arc::clone(&schema), batches);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream");

            assert_eq!(result.manifest.total_row_count, 9);
            assert_eq!(result.manifest.total_chunk_count, 3);
            assert_eq!(result.chunk_indices, vec![0, 1, 2]);
            // True cumulative offsets are 0, 3, 6 — NOT the 0, 2, 4 that the old
            // `chunk_index * chunk_size` formula would have produced.
            assert_eq!(result.chunk_row_offsets, vec![0, 3, 6]);
        }

        #[tokio::test]
        async fn test_schema_with_decimal_types() {
            use arrow::datatypes::DataType;

            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Decimal128(10, 2), false),
                Field::new("rate", DataType::Decimal256(20, 5), true),
            ]));

            let stream = create_test_stream(Arc::clone(&schema), vec![]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream");

            // Verify decimal precision and scale are captured
            assert_eq!(result.manifest.schema.columns[0].type_precision, Some(10));
            assert_eq!(result.manifest.schema.columns[0].type_scale, Some(2));
            assert_eq!(result.manifest.schema.columns[1].type_precision, Some(20));
            assert_eq!(result.manifest.schema.columns[1].type_scale, Some(5));
        }

        #[tokio::test]
        async fn test_total_byte_count() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            let state = job_store
                .create_job(make_request("SELECT * FROM test"), false, test_owner())
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch]);

            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream");

            // Verify byte count is tracked
            assert!(
                result.manifest.total_byte_count > 0,
                "Expected non-zero byte count"
            );
        }

        fn make_request_with_maximum_size(
            sql: impl Into<String>,
            maximum_size: Option<u64>,
        ) -> SubmitQueryRequest {
            SubmitQueryRequest {
                sql: sql.into(),
                parameters: None,
                timeout_seconds: None,
                maximum_size,
            }
        }

        #[tokio::test]
        async fn test_maximum_size_allows_within_limit() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            // Create job with a generous maximum size limit
            let state = job_store
                .create_job(
                    make_request_with_maximum_size("SELECT * FROM test", Some(10_000)),
                    false,
                    test_owner(),
                )
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch]);

            // Should succeed since results are within the maximum size limit
            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream within maximum size limit");

            assert_eq!(result.manifest.total_row_count, 3);
            assert!(result.manifest.total_byte_count > 0);
            assert!(result.manifest.total_byte_count <= 10_000);
        }

        #[tokio::test]
        async fn test_maximum_size_exceeds_limit() {
            let store = Arc::new(InMemory::new());
            // Use small chunk size to force chunk flush and trigger the size check
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(2);

            // Create job with a very small maximum size limit (1 byte)
            let state = job_store
                .create_job(
                    make_request_with_maximum_size("SELECT * FROM test", Some(1)),
                    false,
                    test_owner(),
                )
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            // Create batches that will exceed the 1 byte limit
            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .expect("to create batch");

            let batch2 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![3, 4]))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch1, batch2]);

            // Should fail because results exceed the maximum size limit
            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await;

            assert!(
                matches!(result, Err(Error::MaximumJobSizeExceeded { .. })),
                "Expected MaximumJobSizeExceeded error, got: {result:?}"
            );
        }

        #[tokio::test]
        async fn test_maximum_size_exceeds_on_final_flush() {
            let store = Arc::new(InMemory::new());
            // Large chunk size to ensure flush happens at the end
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(100);

            // Create job with a very small maximum size limit
            let state = job_store
                .create_job(
                    make_request_with_maximum_size("SELECT * FROM test", Some(1)),
                    false,
                    test_owner(),
                )
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch]);

            // Should fail on final flush because results exceed the maximum size limit
            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await;

            assert!(
                matches!(result, Err(Error::MaximumJobSizeExceeded { .. })),
                "Expected MaximumJobSizeExceeded error, got: {result:?}"
            );
        }

        #[tokio::test]
        async fn test_maximum_size_none_allows_any_size() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            // Create job without a maximum size limit
            let state = job_store
                .create_job(
                    make_request_with_maximum_size("SELECT * FROM test", None),
                    false,
                    test_owner(),
                )
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

            // Create a larger batch
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from((0..1000).collect::<Vec<_>>()))],
            )
            .expect("to create batch");

            let stream = create_test_stream(Arc::clone(&schema), vec![batch]);

            // Should succeed since there's no maximum size limit
            let result = job_store
                .write_result_chunks_from_stream(
                    &state.job_id,
                    &start(&job_store, &state.job_id).await,
                    stream,
                )
                .await
                .expect("to write stream without maximum size limit");

            assert_eq!(result.manifest.total_row_count, 1000);
            assert!(result.manifest.total_byte_count > 0);
        }
    }

    mod conditional_writes {
        use super::*;
        use crate::jobs::Error;

        #[tokio::test]
        async fn test_create_job_sets_version() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            let mut state = job_store
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job");

            // The version create_job returns is the stored object's: a write
            // carrying it is accepted. (A state with no version is written as
            // a create, which the existing object refuses.)
            job_store
                .update_job(&mut state)
                .await
                .expect("a write carrying the creation version should succeed");

            // Once another writer has moved the job on, that version is stale,
            // and the conditional write is refused instead of overwriting the
            // newer state.
            job_store
                .set_job_running(&state.job_id)
                .await
                .expect("another writer sets the job running");
            let err = job_store
                .update_job(&mut state)
                .await
                .expect_err("a write carrying a stale version must be refused");
            assert!(
                matches!(&err, Error::ConcurrentModification { job_id } if *job_id == state.job_id),
                "expected ConcurrentModification for {}, got {err:?}",
                state.job_id
            );
            let stored = job_store.get_job(&state.job_id).await.expect("to get job");
            assert_eq!(stored.status, JobStatus::Running);
        }

        #[tokio::test]
        async fn test_update_preserves_version_tracking() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            // Create job
            let created = job_store
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job");
            let job_id = created.job_id.clone();

            // Get the job (which retrieves the version)
            let retrieved = job_store.get_job(&job_id).await.expect("to get job");
            let original_version = retrieved.version.clone();
            assert!(original_version.is_some(), "Version should be set");

            // Update via set_job_running
            let updated = job_store
                .set_job_running(&job_id)
                .await
                .expect("to set running");

            // Version should be updated after write
            assert!(updated.version.is_some(), "Version should still be set");
            // The version should have changed
            assert_ne!(
                updated.version, original_version,
                "Version should change after update"
            );
        }

        #[tokio::test]
        async fn test_version_not_serialized() {
            // Verify that version is not included in serialized JSON
            let state = JobState::new_pending("test-123".to_string(), "SELECT 1".to_string(), None);

            let json = serde_json::to_string(&state).expect("to serialize");
            // Check that the UpdateVersion-related fields are not serialized
            // Note: "schema_version" is a valid field, so we check for the specific pattern
            assert!(
                !json.contains("\"version\""),
                "version field should not be serialized: {json}"
            );
            assert!(
                !json.contains("\"e_tag\""),
                "e_tag field should not be serialized: {json}"
            );
        }

        #[tokio::test]
        async fn test_concurrent_modification_detection() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let job_store = JobStore::new(Arc::clone(&store), "test", "node-1");

            // Create job
            let created = job_store
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job");
            let job_id = created.job_id.clone();

            // Simulate two schedulers getting the job concurrently
            let mut state1 = job_store.get_job(&job_id).await.expect("to get job");
            let mut state2 = job_store.get_job(&job_id).await.expect("to get job");

            // First update should succeed
            state1.set_running("node-1".to_string());
            job_store
                .update_job(&mut state1)
                .await
                .expect("first update should succeed");

            // Second update should fail with concurrent modification
            state2.set_running("node-2".to_string());
            let result = job_store.update_job(&mut state2).await;

            assert!(
                matches!(result, Err(Error::ConcurrentModification { .. })),
                "Expected ConcurrentModification error, got: {result:?}"
            );
        }

        #[tokio::test]
        async fn test_sequential_updates_succeed() {
            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            // Create job
            let created = job_store
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job");
            let job_id = created.job_id.clone();

            // Get and update - should succeed
            let mut state = job_store.get_job(&job_id).await.expect("to get job");
            state.set_running("node-1".to_string());
            job_store
                .update_job(&mut state)
                .await
                .expect("first update should succeed");

            // Get again with new version and update - should succeed
            let mut state2 = job_store.get_job(&job_id).await.expect("to get job");
            state2.set_cancelled();
            job_store
                .update_job(&mut state2)
                .await
                .expect("second update should succeed with fresh version");

            // Verify final state
            let final_state = job_store.get_job(&job_id).await.expect("to get job");
            assert_eq!(final_state.status, JobStatus::Cancelled);
        }

        fn int_stream(values: Vec<i32>) -> SendableRecordBatchStream {
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            use futures::stream;

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(values))],
            )
            .expect("to create batch");
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::iter(vec![Ok(batch)]),
            ))
        }

        async fn chunk_values(job_store: &JobStore, job_id: &str, index: usize) -> Vec<i32> {
            job_store
                .read_chunk(job_id, index)
                .await
                .expect("to read chunk")
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("an Int32 column")
                        .values()
                        .to_vec()
                })
                .collect()
        }

        /// Another scheduler takes a job over from its driver; the former
        /// driver comes back and is refused at each write it makes for the job.
        #[tokio::test]
        async fn a_driver_whose_job_was_taken_over_is_refused_at_its_next_write() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let former = JobStore::new(Arc::clone(&store), "test", "node-a").with_chunk_size(2);
            let successor = JobStore::new(Arc::clone(&store), "test", "node-b").with_chunk_size(2);
            let job_id = former
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job")
                .job_id;
            let started = former.set_job_running(&job_id).await.expect("to start");
            let former_driver = started.driver().expect("a driver");

            let taken = successor
                .take_over_job(&started)
                .await
                .expect("to take over")
                .expect("the job is still driven as observed");
            assert_eq!(taken.scheduler_node.as_deref(), Some("node-b"));
            assert_eq!(taken.generation, Some(2));
            let successor_driver = taken.driver().expect("a driver");

            let err = former
                .write_result_chunks_from_stream(&job_id, &former_driver, int_stream(vec![7, 7]))
                .await
                .expect_err("the former driver must not write results");
            assert!(matches!(err, Error::JobTakenOver { .. }), "{err:?}");
            assert!(
                store.head(&former.chunk_path(&job_id, 0)).await.is_err(),
                "the refused chunk must not be written"
            );
            for err in [
                former
                    .complete_job(&job_id, &former_driver, empty_result())
                    .await
                    .expect_err("the former driver must not complete the job"),
                former
                    .fail_job(&job_id, &former_driver, JobErrorCode::Internal, "late")
                    .await
                    .expect_err("the former driver must not fail the job"),
            ] {
                assert!(matches!(err, Error::JobTakenOver { .. }), "{err:?}");
            }
            assert_eq!(
                former.get_job(&job_id).await.expect("to get job").status,
                JobStatus::Running
            );

            let result = successor
                .write_result_chunks_from_stream(&job_id, &successor_driver, int_stream(vec![1, 2]))
                .await
                .expect("the driver writes results");
            let done = successor
                .complete_job(&job_id, &successor_driver, result)
                .await
                .expect("the driver completes the job");
            assert_eq!(done.status, JobStatus::Succeeded);
            assert_eq!(chunk_values(&successor, &job_id, 0).await, vec![1, 2]);
        }

        /// A chunk the former driver wrote before the takeover is replaced by the
        /// driver's own, so the results match the driver's manifest.
        #[tokio::test]
        async fn the_driver_replaces_a_chunk_its_former_driver_left() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let former = JobStore::new(Arc::clone(&store), "test", "node-a").with_chunk_size(2);
            let successor = JobStore::new(Arc::clone(&store), "test", "node-b").with_chunk_size(2);
            let job_id = former
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job")
                .job_id;
            let started = former.set_job_running(&job_id).await.expect("to start");
            former
                .write_result_chunks_from_stream(
                    &job_id,
                    &started.driver().expect("a driver"),
                    int_stream(vec![9, 9, 9]),
                )
                .await
                .expect("the driver writes while it drives");

            let taken = successor
                .take_over_job(&started)
                .await
                .expect("to take over")
                .expect("the job is still driven as observed");
            let result = successor
                .write_result_chunks_from_stream(
                    &job_id,
                    &taken.driver().expect("a driver"),
                    int_stream(vec![1, 2]),
                )
                .await
                .expect("the new driver writes over the former driver's chunk");
            assert_eq!(result.chunk_indices, vec![0]);
            assert_eq!(chunk_values(&successor, &job_id, 0).await, vec![1, 2]);
        }

        /// Two schedulers that recover the same orphaned job: one takes it over,
        /// the other finds it already taken and does not drive it.
        #[tokio::test]
        async fn two_schedulers_recovering_one_job_take_it_over_once() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let lost = JobStore::new(Arc::clone(&store), "test", "node-lost");
            let b = JobStore::new(Arc::clone(&store), "test", "node-b");
            let c = JobStore::new(Arc::clone(&store), "test", "node-c");
            let job_id = lost
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job")
                .job_id;
            let orphaned = lost.set_job_running(&job_id).await.expect("to start");

            let (by_b, by_c) = tokio::join!(b.take_over_job(&orphaned), c.take_over_job(&orphaned));
            let takers: Vec<_> = [by_b.expect("b"), by_c.expect("c")]
                .into_iter()
                .flatten()
                .collect();
            assert_eq!(takers.len(), 1, "{takers:?}");
            let stored = b.get_job(&job_id).await.expect("to get job");
            assert_eq!(stored.scheduler_node, takers[0].scheduler_node);
            assert_eq!(stored.generation, Some(2));

            // A recoverer that read the job before the takeover finds it taken.
            assert!(
                c.take_over_job(&orphaned)
                    .await
                    .expect("to take over")
                    .is_none()
            );
        }

        /// A job rewritten by a version that does not record the generation
        /// counts as taken over: no driver holds a job without a generation, so
        /// its former driver is refused, and the next takeover starts a new
        /// generation under a new driver.
        #[tokio::test]
        async fn a_rewrite_that_drops_the_generation_is_a_new_owner() {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let former = JobStore::new(Arc::clone(&store), "test", "node-a");
            let next = JobStore::new(Arc::clone(&store), "test", "node-c");
            let job_id = former
                .create_job(make_request("SELECT 1"), false, test_owner())
                .await
                .expect("to create job")
                .job_id;
            let former_driver = former
                .set_job_running(&job_id)
                .await
                .expect("to start")
                .driver()
                .expect("a driver");

            // An older scheduler takes the job over: it writes its own node and
            // drops the field it does not know.
            let path = former.job_state_path(&job_id);
            let mut stored: serde_json::Value = serde_json::from_slice(
                &store
                    .get(&path)
                    .await
                    .expect("to read job")
                    .bytes()
                    .await
                    .expect("to read bytes"),
            )
            .expect("job JSON");
            let fields = stored.as_object_mut().expect("an object");
            assert!(fields.remove("generation").is_some());
            fields.insert("scheduler_node".to_string(), "node-trunk".into());
            store
                .put(&path, serde_json::to_vec(&stored).expect("JSON").into())
                .await
                .expect("the older scheduler's rewrite");

            let err = former
                .complete_job(&job_id, &former_driver, empty_result())
                .await
                .expect_err("the former driver must not complete the job");
            assert!(matches!(err, Error::JobTakenOver { .. }), "{err:?}");

            let rewritten = next.get_job(&job_id).await.expect("to get job");
            assert_eq!(rewritten.generation, None);
            let taken = next
                .take_over_job(&rewritten)
                .await
                .expect("to take over")
                .expect("the job is still as observed");
            assert_eq!(taken.scheduler_node.as_deref(), Some("node-c"));
            assert_eq!(taken.generation, Some(1));
            assert!(
                former
                    .fail_job(&job_id, &former_driver, JobErrorCode::Internal, "late")
                    .await
                    .is_err_and(|err| matches!(err, Error::JobTakenOver { .. })),
                "the former driver stays refused after the next takeover"
            );
        }
    }
}
