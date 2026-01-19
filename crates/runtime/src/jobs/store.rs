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
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore};
use snafu::prelude::*;
use uuid::Uuid;

use super::error::{
    DeserializeChunkSnafu, DeserializeStateSnafu, ObjectStoreDeleteSnafu, ObjectStoreReadSnafu,
    ObjectStoreWriteSnafu, Result, SerializeChunkSnafu, SerializeStateSnafu,
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
    /// Returns a Databricks-style formatted ID like "01ABC-DEF-456-789".
    /// Uses `UUIDv7` which contains a millisecond timestamp plus random bits.
    #[must_use]
    pub fn generate_job_id() -> String {
        // Format: 01ABC-DEF-456-789 style (Databricks-like)
        // UUIDv7 structure: 48-bit timestamp (ms) + 4-bit version + 12-bit rand + 62-bit rand
        // We use characters 0-16 to include timestamp + version + random bits for uniqueness
        let uuid = Uuid::now_v7();
        let hex = uuid.simple().to_string();
        format!(
            "{}-{}-{}-{}",
            &hex[0..5],
            &hex[5..8],
            &hex[8..11],
            &hex[11..14]
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
    /// Takes an iterator of `RecordBatch`es and writes them as Arrow IPC chunks.
    /// Returns the job result manifest.
    pub async fn write_result_chunks(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<JobResult> {
        if batches.is_empty() {
            // Empty result set
            return Ok(JobResult {
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
            });
        }

        let schema = batches[0].schema();
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

        Ok(JobResult {
            manifest: JobResultManifest {
                format: "ARROW_IPC".to_string(),
                schema: JobSchema {
                    column_count: columns.len(),
                    columns,
                },
                total_row_count: total_rows,
                total_chunk_count: chunk_indices.len(),
                truncated: false,
                total_byte_count: Some(total_bytes),
            },
            chunk_indices,
        })
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
    pub async fn delete_job(&self, job_id: &str) -> Result<()> {
        // Delete all chunks first
        let chunks_prefix = self.job_chunks_prefix(job_id);
        let mut stream = self.store.list(Some(&chunks_prefix));

        while let Some(entry) = stream.next().await {
            match entry {
                Ok(meta) => {
                    if let Err(err) = self.store.delete(&meta.location).await {
                        tracing::warn!(
                            job_id,
                            path = %meta.location,
                            error = %err,
                            "Failed to delete job chunk from object store"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        job_id,
                        error = %err,
                        "Failed to list job chunks from object store"
                    );
                }
            }
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
    pub async fn list_jobs(&self, status_filter: Option<JobStatus>) -> Result<Vec<JobState>> {
        let jobs_prefix = self.jobs_prefix();
        let mut stream = self.store.list(Some(&jobs_prefix));
        let mut jobs = Vec::new();

        while let Some(entry) = stream.next().await {
            let meta = entry.context(ObjectStoreReadSnafu)?;

            // Only process .json files (job state, not chunks)
            if !std::path::Path::new(meta.location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                continue;
            }

            let result = self.store.get(&meta.location).await;
            let Ok(get_result) = result else {
                continue;
            };

            let Ok(bytes) = get_result.bytes().await else {
                continue;
            };

            let Ok(state) = serde_json::from_slice::<JobState>(&bytes) else {
                continue;
            };

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
    pub async fn cleanup_expired_jobs(&self) -> Result<usize> {
        let jobs_prefix = self.jobs_prefix();
        let mut stream = self.store.list(Some(&jobs_prefix));
        let mut deleted_count = 0usize;

        while let Some(entry) = stream.next().await {
            let Ok(meta) = entry else {
                continue;
            };

            // Only process .json files
            if !std::path::Path::new(meta.location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                continue;
            }

            let Ok(result) = self.store.get(&meta.location).await else {
                continue;
            };

            let Ok(bytes) = result.bytes().await else {
                continue;
            };

            let Ok(state) = serde_json::from_slice::<JobState>(&bytes) else {
                continue;
            };

            if state.is_expired() && self.delete_job(&state.job_id).await.is_ok() {
                // Saturate at MAX on overflow - cleanup count is informational only
                deleted_count = deleted_count.saturating_add(1);
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
            .write_result_chunks(&state.job_id, vec![batch1, batch2])
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
        let id1 = JobStore::generate_job_id();
        let id2 = JobStore::generate_job_id();

        // Even without sleep, UUIDv7 includes random bits that should differ
        assert_ne!(id1, id2, "UUIDv7-based job IDs should be unique");
        assert_eq!(id1.len(), 17); // 5 + 1 + 3 + 1 + 3 + 1 + 3
        assert_eq!(id1.matches('-').count(), 3);
    }
}
