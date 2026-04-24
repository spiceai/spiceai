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

/// Result of polling a partition discovery job.
#[derive(Debug)]
pub enum DiscoveryJobPollResult {
    /// The job is still running (or queued).
    StillRunning,
    /// The job completed successfully; here are the discovered partition values.
    Completed(Vec<PartitionValue>),
    /// The job failed.
    Failed(String),
}

/// Submits and polls partition discovery jobs via the Ballista job machinery.
///
/// Partition discovery queries (`SELECT DISTINCT <partition_exprs> FROM <table>`)
/// are submitted as regular Ballista jobs through `JobExecutor`/`JobStore`. This
/// makes discovery non-blocking: the scheduler can submit a job and check for
/// completion on subsequent ticks.
#[async_trait]
pub trait PartitionDiscoverySubmitter: Send + Sync {
    /// Submit a partition discovery query as a Ballista job.
    ///
    /// Returns the `job_id` from the `JobStore` on success.
    async fn submit_discovery_job(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;

    /// Poll a previously submitted discovery job for results.
    ///
    /// `partition_expressions` are the SQL expression strings (e.g.
    /// `["bucket(8, customer_id)"]`) used to interpret the result columns.
    async fn poll_discovery_job(
        &self,
        job_id: &str,
        partition_expressions: &[String],
    ) -> Result<DiscoveryJobPollResult, Box<dyn std::error::Error + Send + Sync>>;
}

/// Combined bound for partition-management operations that need both expression
/// resolution (for serializing partition values) and discovery job
/// submission/polling.
pub trait PartitionOperations: PartitionExprResolver + PartitionDiscoverySubmitter {}

impl<T: PartitionExprResolver + PartitionDiscoverySubmitter + ?Sized> PartitionOperations for T {}
