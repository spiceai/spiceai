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

//! Traits that abstract the runtime-side `DataFusion` god-struct away from this
//! crate. The runtime side implements these; this crate only holds trait objects.

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use datafusion_expr::Expr;
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;

use crate::metadata::PartitionValue;

/// Parses a SQL partition expression against a specific table's schema.
///
/// Implemented in the runtime crate by `DataFusion::try_parse_expr`, which
/// looks up the table provider and uses its schema as the DF schema for
/// `SessionContext::parse_sql_expr`.
#[async_trait]
pub trait PartitionExprResolver: Send + Sync {
    async fn try_parse_expr(
        &self,
        tbl: &TableReference,
        expr: &str,
    ) -> Result<Expr, DataFusionError>;
}

/// Discovers the values a `table` can have for a given `partition_by` expression.
///
/// For >1 `partition_by` value, the cartesian product of individual options is returned.
#[async_trait]
pub trait PartitionDiscoverer: Send + Sync {
    async fn table_partition_values(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
    ) -> Result<Vec<PartitionValue>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Snafu)]
pub enum DiscoveryJobError {
    #[snafu(display("Discovery job executor is not initialized"))]
    JobExecutorNotInitialized,

    #[snafu(display("Failed to submit discovery job for table {table}: {source}"))]
    SubmitDiscoveryJob {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to poll discovery job {job_id}: {source}"))]
    PollDiscoveryJob {
        job_id: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Discovery job {job_id} completed without a result payload"))]
    MissingJobResult { job_id: String },

    #[snafu(display("Failed to decode discovery results for job {job_id}: {source}"))]
    DecodeDiscoveryResults {
        job_id: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Result of polling a partition discovery job.
#[derive(Debug)]
pub enum DiscoveryJobPollResult {
    /// The job is still executing.
    StillRunning,
    /// The job completed successfully with the discovered partition values.
    Completed(Vec<PartitionValue>),
    /// The job failed with the given error message.
    Failed(String),
}

/// Submits and polls partition discovery as Ballista jobs instead of
/// blocking on a synchronous `SELECT DISTINCT`.
#[async_trait]
pub trait PartitionDiscoverySubmitter: Send + Sync {
    /// Build and submit a discovery job for the given table. Returns the
    /// Ballista job ID on success.
    async fn submit_discovery_job(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
    ) -> Result<String, DiscoveryJobError>;

    /// Poll the status of a previously submitted discovery job.
    /// `partition_expressions` are the raw SQL expression strings so that
    /// result columns can be mapped back to partition keys.
    async fn poll_discovery_job(
        &self,
        job_id: &str,
        partition_expressions: &[String],
    ) -> Result<DiscoveryJobPollResult, DiscoveryJobError>;
}

/// Combined bound for partition-management operations that need expression
/// resolution (for serializing partition values), synchronous source discovery,
/// and async job-based discovery.
pub trait PartitionOperations:
    PartitionExprResolver + PartitionDiscoverer + PartitionDiscoverySubmitter
{
}

impl<T: PartitionExprResolver + PartitionDiscoverer + PartitionDiscoverySubmitter + ?Sized>
    PartitionOperations for T
{
}
