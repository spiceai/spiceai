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

use clap::Parser;

use crate::commands::streaming::SourceType;
use crate::commands::streaming::querysets::QuerySetType;

use super::CommonArgs;

/// Arguments for DynamoDB streaming ingestion benchmarks.
///
/// Source and dataset configuration is done via environment variables:
///
/// ## DynamoDB Local (`--source dynamodb-streams-local`)
/// - `DYNAMODB_LOCAL_PORT`: Port for DynamoDB local (optional, default: 8000)
///
/// ## AWS DynamoDB (`--source dynamodb-streams`)
/// - `DYNAMODB_AWS_REGION`: AWS region (required)
/// - `DYNAMODB_AWS_ACCESS_KEY_ID`: AWS access key ID (required)
/// - `DYNAMODB_AWS_SECRET_ACCESS_KEY`: AWS secret access key (required)
/// - `DYNAMODB_AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for LocalStack)
///
/// ## Snapshot Storage (required for DynamoDB benchmarks)
/// - `SNAPSHOT_S3_LOCATION`: S3 location for snapshots (e.g., `s3://bucket/snapshots/`)
/// - `SNAPSHOT_S3_ACCESS_KEY_ID`: S3 access key ID (optional)
/// - `SNAPSHOT_S3_SECRET_ACCESS_KEY`: S3 secret access key (optional)
/// - `SNAPSHOT_S3_REGION`: S3 region (optional)
///
/// ## ClickBench data (`--queryset clickbench`)
/// - `CLICKBENCH_S3_URI`: S3 URI to hits.parquet (e.g., `s3://bucket/path/hits.parquet`)
/// - `CLICKBENCH_S3_ENDPOINT`: S3/MinIO endpoint (optional, for MinIO)
/// - `CLICKBENCH_S3_ACCESS_KEY_ID`: S3 access key ID (required when using S3)
/// - `CLICKBENCH_S3_SECRET_ACCESS_KEY`: S3 secret access key (required when using S3)
#[derive(Parser, Debug, Clone)]
#[expect(clippy::struct_excessive_bools)]
pub struct StreamingDynamodbTestArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Streaming source type (e.g., dynamodb-streams-local, dynamodb-streams)
    #[arg(long, value_enum)]
    pub source: SourceType,

    /// Query set type (e.g., tpch-lineitem). Determines which datasets to load.
    #[arg(long, value_enum)]
    pub queryset: QuerySetType,

    /// Scale factor for data generation (e.g., 0.01, 0.1, 1.0)
    #[arg(long, default_value = "0.01")]
    pub scale_factor: f64,

    /// Timeout in seconds to wait for ingestion to complete
    #[arg(long, default_value = "300")]
    pub ingestion_timeout: u64,

    /// Enable health monitoring during ingestion (tracks latency and failures)
    #[arg(long)]
    pub enable_liveness: bool,

    /// Run TPCH queries to verify data integrity after ingestion
    #[arg(long)]
    pub verify: bool,

    // Query liveness monitoring arguments
    /// Enable query liveness monitoring (runs COUNT(*) queries and tracks latency)
    #[arg(long)]
    pub enable_query_liveness: bool,

    /// Poll interval for query liveness checks in milliseconds
    #[arg(long, default_value = "500")]
    pub query_liveness_interval_ms: u64,

    // Mutation testing arguments (for CDC testing)
    /// Ratio of rows to mutate (0.0-1.0). When > 0, selected rows are split 50/50:
    /// - Half go through: INSERT (wrong) → UPDATE (correct)
    /// - Half go through: INSERT (wrong) → DELETE → INSERT (correct)
    /// Remaining rows are inserted directly with final values.
    #[arg(long, default_value = "0.0")]
    pub mutation_ratio: f64,

    /// Random seed for reproducible mutation row selection
    #[arg(long, default_value = "42")]
    pub mutation_seed: u64,

    // Multi-config benchmark arguments
    /// Additional spicepod paths for multi-config benchmarks.
    /// When multiple configs are provided, each is benchmarked against the same data.
    #[arg(long = "spicepod-path", value_name = "PATH")]
    pub additional_spicepod_paths: Vec<std::path::PathBuf>,

    /// Run benchmark configs in parallel (may cause resource contention)
    #[arg(long)]
    pub parallel: bool,
}

impl StreamingDynamodbTestArgs {
    /// Get all spicepod paths (common + additional).
    pub fn all_spicepod_paths(&self) -> Vec<std::path::PathBuf> {
        let mut paths = vec![self.common.spicepod_path.clone()];
        paths.extend(self.additional_spicepod_paths.clone());
        paths
    }
}
