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
use object_store::{Error as ObjectStoreError, ObjectStore};
use snafu::prelude::*;
use uuid::Uuid;

use super::error::{
    DeserializeChunkSnafu, DeserializeStateSnafu, ObjectStoreDeleteSnafu, ObjectStoreListSnafu,
    ObjectStoreReadSnafu, ObjectStoreWriteSnafu, Result, SerializeChunkSnafu, SerializeStateSnafu,
};
use super::state::{
    ColumnSchema, DEFAULT_CHUNK_SIZE, DEFAULT_RESULT_TTL, JobResult, JobResultManifest, JobSchema,
    JobState, JobStatus,
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
        sql: String,
        parameters: Option<serde_json::Value>,
    ) -> Result<JobState> {
        let job_id = Self::generate_job_id();
        let state = JobState::new_pending(job_id, sql, parameters);
        self.write_job_state(&state).await?;
        Ok(state)
    }

    /// Gets the current state of a job.
    pub async fn get_job(&self, job_id: &str) -> Result<JobState> {
        let path = self.job_state_path(job_id);
        let result = self.store.get(&path).await.map_err(|e| match e {
            ObjectStoreError::NotFound { .. } => super::error::Error::JobNotFound {
                job_id: job_id.to_string(),
            },
            other => super::error::Error::ObjectStoreRead { source: other },
        })?;

        let bytes = result.bytes().await.context(ObjectStoreReadSnafu)?;
        let state: JobState = serde_json::from_slice(&bytes).context(DeserializeStateSnafu)?;

        // Check if expired
        if state.is_expired() {
            return Err(super::error::Error::JobResultsExpired {
                job_id: job_id.to_string(),
            });
        }

        Ok(state)
    }

    /// Updates the job state with conditional write for consistency.
    pub async fn update_job(&self, state: &JobState) -> Result<()> {
        self.write_job_state(state).await
    }

    /// Marks a job as running.
    pub async fn set_job_running(&self, job_id: &str) -> Result<JobState> {
        let mut state = self.get_job(job_id).await?;
        state.set_running(self.node_id.clone());
        self.write_job_state(&state).await?;
        Ok(state)
    }

    /// Marks a job as cancelled.
    pub async fn cancel_job(&self, job_id: &str) -> Result<JobState> {
        let mut state = self.get_job(job_id).await?;
        if state.is_terminal() {
            // Already in terminal state, return as-is
            return Ok(state);
        }
        state.set_cancelled();
        self.write_job_state(&state).await?;
        Ok(state)
    }

    /// Writes result chunks for a completed job.
    ///
    /// Takes a schema and an iterator of `RecordBatch`es and writes them as Arrow IPC chunks.
    /// The schema is used for the result manifest even if no batches are provided,
    /// ensuring empty result sets still have valid schema information.
    ///
    /// Returns the job result manifest.
    pub async fn write_result_chunks(
        &self,
        job_id: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<JobResult> {
        if batches.is_empty() {
            // Empty result set - still include the schema
            return Ok(Self::build_job_result(&schema, 0, 0, 0, vec![]));
        }
        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let mut chunk_indices = Vec::new();

        // Group batches into chunks based on row count
        let mut current_chunk_batches: Vec<RecordBatch> = Vec::new();
        let mut current_chunk_rows = 0usize;
        let mut chunk_index = 0usize;

        for batch in batches {
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
                    .write_chunk(job_id, chunk_index, &current_chunk_batches)
                    .await?;
                total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                    super::error::Error::IntegerOverflow {
                        field: "total_byte_count".to_string(),
                        left_value: total_bytes,
                        right_value: bytes,
                    }
                })?;
                chunk_indices.push(chunk_index);
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
                .write_chunk(job_id, chunk_index, &current_chunk_batches)
                .await?;
            total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                super::error::Error::IntegerOverflow {
                    field: "total_byte_count".to_string(),
                    left_value: total_bytes,
                    right_value: bytes,
                }
            })?;
            chunk_indices.push(chunk_index);
        }

        Ok(Self::build_job_result(
            &schema,
            total_rows,
            total_bytes,
            chunk_indices.len(),
            chunk_indices,
        ))
    }

    /// Writes result chunks for a completed job from a stream of record batches.
    ///
    /// Streams `RecordBatch`es and writes them as Arrow IPC chunks as they arrive,
    /// avoiding loading all results into memory at once. Chunks are flushed when
    /// the configured `chunk_size` row threshold is reached.
    ///
    /// Returns the job result manifest.
    pub async fn write_result_chunks_from_stream(
        &self,
        job_id: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<JobResult> {
        let schema: SchemaRef = stream.schema();

        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let mut chunk_indices = Vec::new();

        // Buffer for accumulating batches until we reach chunk_size
        let mut current_chunk_batches: Vec<RecordBatch> = Vec::new();
        let mut current_chunk_rows = 0usize;
        let mut chunk_index = 0usize;

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
                    .write_chunk(job_id, chunk_index, &current_chunk_batches)
                    .await?;
                total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                    super::error::Error::IntegerOverflow {
                        field: "total_byte_count".to_string(),
                        left_value: total_bytes,
                        right_value: bytes,
                    }
                })?;
                chunk_indices.push(chunk_index);
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
                .write_chunk(job_id, chunk_index, &current_chunk_batches)
                .await?;
            total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                super::error::Error::IntegerOverflow {
                    field: "total_byte_count".to_string(),
                    left_value: total_bytes,
                    right_value: bytes,
                }
            })?;
            chunk_indices.push(chunk_index);
        }

        Ok(Self::build_job_result(
            &schema,
            total_rows,
            total_bytes,
            chunk_indices.len(),
            chunk_indices,
        ))
    }

    /// Builds a `JobResult` from the given schema and result statistics.
    fn build_job_result(
        schema: &SchemaRef,
        total_rows: usize,
        total_bytes: usize,
        total_chunks: usize,
        chunk_indices: Vec<usize>,
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
                truncated: false,
                total_byte_count: Some(total_bytes),
            },
            chunk_indices,
        }
    }

    /// Writes a single chunk to the object store.
    async fn write_chunk(
        &self,
        job_id: &str,
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

        self.store
            .put(&path, buffer.into())
            .await
            .context(ObjectStoreWriteSnafu)?;

        Ok(bytes_len)
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

    /// Marks a job as succeeded with the given results.
    pub async fn complete_job(&self, job_id: &str, result: JobResult) -> Result<JobState> {
        let mut state = self.get_job(job_id).await?;
        state.set_succeeded(result, self.result_ttl);
        self.write_job_state(&state).await?;
        Ok(state)
    }

    /// Marks a job as failed with the given error.
    pub async fn fail_job(
        &self,
        job_id: &str,
        error_code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<JobState> {
        let mut state = self.get_job(job_id).await?;
        state.set_failed(super::state::JobError {
            error_code: error_code.into(),
            message: message.into(),
            sql_state: None,
        });
        self.write_job_state(&state).await?;
        Ok(state)
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

            if let Err(err) = self.store.delete(&meta.location).await {
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

        // Delete job state
        let path = self.job_state_path(job_id);
        self.store
            .delete(&path)
            .await
            .context(ObjectStoreDeleteSnafu)?;

        Ok(())
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
        jobs.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));

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

    async fn write_job_state(&self, state: &JobState) -> Result<()> {
        let path = self.job_state_path(&state.job_id);
        let payload = serde_json::to_vec(state).context(SerializeStateSnafu)?;

        self.store
            .put(&path, payload.into())
            .await
            .context(ObjectStoreWriteSnafu)?;

        Ok(())
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

    #[tokio::test]
    async fn test_create_and_get_job() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        let state = job_store
            .create_job("SELECT 1".to_string(), None)
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
            .create_job("SELECT * FROM test".to_string(), None)
            .await
            .expect("to create job");
        let job_id = state.job_id.clone();

        // Set running
        let running = job_store
            .set_job_running(&job_id)
            .await
            .expect("to set running");
        assert_eq!(running.status, JobStatus::Running);
        assert_eq!(running.executor_node.as_deref(), Some("node-1"));

        // Complete with empty results
        let result = JobResult {
            manifest: JobResultManifest {
                format: "ARROW_IPC".to_string(),
                schema: JobSchema {
                    column_count: 0,
                    columns: vec![],
                },
                total_row_count: 0,
                total_chunk_count: 0,
                truncated: false,
                total_byte_count: Some(0),
            },
            chunk_indices: vec![],
        };

        let completed = job_store
            .complete_job(&job_id, result)
            .await
            .expect("to complete job");
        assert_eq!(completed.status, JobStatus::Succeeded);
        assert!(completed.expires_at_ms.is_some());
    }

    #[tokio::test]
    async fn test_write_and_read_chunks() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(2);

        let state = job_store
            .create_job("SELECT * FROM test".to_string(), None)
            .await
            .expect("to create job");

        // Create test batches
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

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

        let result = job_store
            .write_result_chunks(&state.job_id, Arc::clone(&schema), vec![batch1, batch2])
            .await
            .expect("to write chunks");

        assert_eq!(result.manifest.total_row_count, 4);
        assert_eq!(result.manifest.total_chunk_count, 2);

        // Read chunks back
        let chunk0 = job_store
            .read_chunk(&state.job_id, 0)
            .await
            .expect("to read chunk 0");
        assert!(!chunk0.is_empty());

        let chunk1 = job_store
            .read_chunk(&state.job_id, 1)
            .await
            .expect("to read chunk 1");
        assert!(!chunk1.is_empty());
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        let state = job_store
            .create_job("SELECT 1".to_string(), None)
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

    #[tokio::test]
    async fn test_write_result_chunks_empty_with_schema() {
        let store = Arc::new(InMemory::new());
        let job_store = JobStore::new(store, "test", "node-1");

        let state = job_store
            .create_job("SELECT * FROM test WHERE 1=0".to_string(), None)
            .await
            .expect("to create job");

        // Create a schema but no batches - simulating an empty result set
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let result = job_store
            .write_result_chunks(&state.job_id, Arc::clone(&schema), vec![])
            .await
            .expect("to write empty result with schema");

        // Should have 0 rows but valid schema
        assert_eq!(result.manifest.total_row_count, 0);
        assert_eq!(result.manifest.total_chunk_count, 0);
        assert!(result.chunk_indices.is_empty());

        // Schema should be preserved even with no rows
        assert_eq!(result.manifest.schema.column_count, 2);
        assert_eq!(result.manifest.schema.columns.len(), 2);
        assert_eq!(result.manifest.schema.columns[0].name, "id");
        assert_eq!(result.manifest.schema.columns[0].type_name, "Int32");
        assert!(!result.manifest.schema.columns[0].nullable);
        assert_eq!(result.manifest.schema.columns[1].name, "name");
        assert_eq!(result.manifest.schema.columns[1].type_name, "Utf8");
        assert!(result.manifest.schema.columns[1].nullable);
    }

    mod write_result_chunks_from_stream {
        use super::*;
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
                .create_job("SELECT * FROM test WHERE 1=0".to_string(), None)
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let stream = create_test_stream(Arc::clone(&schema), vec![]);

            let result = job_store
                .write_result_chunks_from_stream(&state.job_id, stream)
                .await
                .expect("to write empty stream");

            assert_eq!(result.manifest.total_row_count, 0);
            assert_eq!(result.manifest.total_chunk_count, 0);
            assert!(result.chunk_indices.is_empty());

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
                .create_job("SELECT * FROM test".to_string(), None)
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
                .write_result_chunks_from_stream(&state.job_id, stream)
                .await
                .expect("to write single batch stream");

            assert_eq!(result.manifest.total_row_count, 3);
            assert_eq!(result.manifest.total_chunk_count, 1);
            assert_eq!(result.chunk_indices, vec![0]);

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
                .create_job("SELECT * FROM test".to_string(), None)
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
                .write_result_chunks_from_stream(&state.job_id, stream)
                .await
                .expect("to write stream");

            assert_eq!(result.manifest.total_row_count, 5);
            // All batches fit in one chunk since chunk_size is 100
            assert_eq!(result.manifest.total_chunk_count, 1);
            assert_eq!(result.chunk_indices, vec![0]);
        }

        #[tokio::test]
        async fn test_multiple_chunks() {
            let store = Arc::new(InMemory::new());
            // Set chunk size to 2 rows to force multiple chunks
            let job_store = JobStore::new(store, "test", "node-1").with_chunk_size(2);

            let state = job_store
                .create_job("SELECT * FROM test".to_string(), None)
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
                .write_result_chunks_from_stream(&state.job_id, stream)
                .await
                .expect("to write stream");

            assert_eq!(result.manifest.total_row_count, 5);
            // chunk_size=2, so:
            // - chunk 0: batch1 (2 rows) -> flushed
            // - chunk 1: batch2 (2 rows) -> flushed
            // - chunk 2: batch3 (1 row) -> flushed at end
            assert_eq!(result.manifest.total_chunk_count, 3);
            assert_eq!(result.chunk_indices, vec![0, 1, 2]);

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
        async fn test_schema_with_decimal_types() {
            use arrow::datatypes::DataType;

            let store = Arc::new(InMemory::new());
            let job_store = JobStore::new(store, "test", "node-1");

            let state = job_store
                .create_job("SELECT * FROM test".to_string(), None)
                .await
                .expect("to create job");

            let schema = Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Decimal128(10, 2), false),
                Field::new("rate", DataType::Decimal256(20, 5), true),
            ]));

            let stream = create_test_stream(Arc::clone(&schema), vec![]);

            let result = job_store
                .write_result_chunks_from_stream(&state.job_id, stream)
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
                .create_job("SELECT * FROM test".to_string(), None)
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
                .write_result_chunks_from_stream(&state.job_id, stream)
                .await
                .expect("to write stream");

            // Verify byte count is tracked
            assert!(
                result.manifest.total_byte_count.is_some_and(|b| b > 0),
                "Expected non-zero byte count"
            );
        }
    }
}
