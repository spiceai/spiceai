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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Version for job state schema, incremented on breaking changes
pub const JOB_SCHEMA_VERSION: u32 = 1;

/// Default time-to-live for job results (12 hours, matching Databricks)
pub const DEFAULT_RESULT_TTL: Duration = Duration::from_secs(12 * 60 * 60);

/// Default chunk size for results (10,000 rows)
pub const DEFAULT_CHUNK_SIZE: usize = 10_000;

/// The current status of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    /// Job is queued but not yet running
    Pending,
    /// Job is actively executing
    Running,
    /// Job completed successfully, results available
    Succeeded,
    /// Job execution failed
    Failed,
    /// Job was cancelled by user
    Cancelled,
    /// Job results have been cleaned up / expired
    Closed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Closed => write!(f, "CLOSED"),
        }
    }
}

/// Error details when a job fails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobError {
    /// Error code categorizing the failure
    pub error_code: String,
    /// Human-readable error message
    pub message: String,
    /// SQL state code if applicable
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
}

/// Schema information for a column in the result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Arrow data type name
    pub type_name: String,
    /// Type precision for numeric types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_precision: Option<u32>,
    /// Type scale for decimal types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_scale: Option<i32>,
    /// Whether the column can contain nulls
    pub nullable: bool,
    /// Column position (0-indexed)
    pub position: usize,
}

/// Schema information for the result set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSchema {
    /// Number of columns
    pub column_count: usize,
    /// Column definitions
    pub columns: Vec<ColumnSchema>,
}

/// Manifest describing the complete result set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultManifest {
    /// Data format (always `ARROW_IPC` for now)
    pub format: String,
    /// Result schema
    pub schema: JobSchema,
    /// Total number of rows across all chunks
    pub total_row_count: usize,
    /// Total number of chunks
    pub total_chunk_count: usize,
    /// Whether results were truncated due to limits
    pub truncated: bool,
    /// Total size in bytes (approximate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_byte_count: Option<usize>,
}

/// Result information for a completed job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    /// The result manifest with schema and counts
    pub manifest: JobResultManifest,
    /// List of chunk indices available
    pub chunk_indices: Vec<usize>,
}

/// Complete state of a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
    /// Schema version for forward compatibility
    pub schema_version: u32,
    /// Unique job identifier
    pub job_id: String,
    /// Current status
    pub status: JobStatus,
    /// The SQL statement being executed
    pub sql: String,
    /// Query parameters if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    /// Node that is executing this job
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_node: Option<String>,
    /// Error details if status is Failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JobError>,
    /// Result information if status is Succeeded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JobResult>,
    /// When the job was created (Unix timestamp ms)
    pub created_at_ms: u64,
    /// When execution started (Unix timestamp ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// When execution completed (Unix timestamp ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    /// When results will expire (Unix timestamp ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Custom labels/metadata
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
}

impl JobState {
    /// Creates a new pending job state.
    #[must_use]
    pub fn new_pending(job_id: String, sql: String, parameters: Option<serde_json::Value>) -> Self {
        let now_ms = now_ms_or_zero();
        Self {
            schema_version: JOB_SCHEMA_VERSION,
            job_id,
            status: JobStatus::Pending,
            sql,
            parameters,
            executor_node: None,
            error: None,
            result: None,
            created_at_ms: now_ms,
            started_at_ms: None,
            completed_at_ms: None,
            expires_at_ms: None,
            labels: HashMap::new(),
        }
    }

    /// Transitions job to running state.
    pub fn set_running(&mut self, executor_node: String) {
        self.status = JobStatus::Running;
        self.executor_node = Some(executor_node);
        self.started_at_ms = Some(now_ms_or_zero());
    }

    /// Transitions job to succeeded state with results.
    pub fn set_succeeded(&mut self, result: JobResult, result_ttl: Duration) {
        let now = now_ms_or_zero();
        self.status = JobStatus::Succeeded;
        self.result = Some(result);
        self.completed_at_ms = Some(now);
        // Convert TTL to milliseconds, saturating at u64::MAX for extremely large TTLs
        // (effectively "never expires" - over 500 million years)
        let ttl_ms = u64::try_from(result_ttl.as_millis()).unwrap_or(u64::MAX);
        // Saturate at u64::MAX if overflow would occur (effectively "never expires")
        self.expires_at_ms = Some(now.saturating_add(ttl_ms));
    }

    /// Transitions job to failed state with error.
    pub fn set_failed(&mut self, error: JobError) {
        self.status = JobStatus::Failed;
        self.error = Some(error);
        self.completed_at_ms = Some(now_ms_or_zero());
    }

    /// Transitions job to cancelled state.
    pub fn set_cancelled(&mut self) {
        self.status = JobStatus::Cancelled;
        self.completed_at_ms = Some(now_ms_or_zero());
    }

    /// Checks if the job has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at_ms {
            now_ms_or_zero() >= expires_at
        } else {
            false
        }
    }

    /// Checks if the job is in a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled | JobStatus::Closed
        )
    }
}

/// Gets the current Unix timestamp in milliseconds, logging a warning if the system
/// clock is before the Unix epoch and returning 0 as a fallback.
fn now_ms_or_zero() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_millis()).unwrap_or(u64::MAX),
        Err(e) => {
            // This should only happen if system time is before Unix epoch,
            // which indicates a misconfigured system clock
            tracing::warn!(error = %e, "System time is before Unix epoch, using 0 for timestamp");
            0
        }
    }
}
