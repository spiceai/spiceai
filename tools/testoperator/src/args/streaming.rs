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

use super::CommonArgs;
use crate::commands::streaming::querysets::QuerySetType;

/// Arguments for `DynamoDB` streaming ingestion performance benchmark.
///
/// Self-contained command that creates tables, inserts TPC-H data, starts Spice
/// with snapshot creation enabled, and measures ingestion throughput.
///
/// ## Flow
/// 1. Creates `DynamoDB` tables
/// 2. Generates and inserts TPC-H data
/// 3. Starts Spice with snapshot creation (`create_only`)
/// 4. Measures ingestion throughput and stream lag
/// 5. Optionally verifies data with TPC-H queries
///
/// ## Environment Variables
///
/// ### AWS `DynamoDB`
/// - `DYNAMODB_AWS_REGION`: AWS region (required)
/// - `DYNAMODB_AWS_ACCESS_KEY_ID`: AWS access key ID (required)
/// - `DYNAMODB_AWS_SECRET_ACCESS_KEY`: AWS secret access key (required)
/// - `DYNAMODB_AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for `LocalStack`)
///
/// ### Snapshot Storage (required)
/// - `SNAPSHOT_S3_LOCATION`: S3 location for snapshots (e.g., `s3://bucket/snapshots/`)
/// - `SNAPSHOT_S3_REGION`: S3 region (optional)
#[derive(Parser, Debug, Clone)]
pub struct StreamingDynamodbArgs {
    /// Common arguments (spicepod path, spiced path, metrics, etc.)
    #[command(flatten)]
    pub common: CommonArgs,

    /// Query set type (e.g., tpch). Determines which datasets to load.
    #[arg(long, value_enum)]
    pub queryset: QuerySetType,

    /// Scale factor for TPC-H data generation (e.g., 0.01, 0.1, 1.0)
    #[arg(long, default_value = "0.01")]
    pub scale_factor: f64,

    /// Run TPC-H queries to verify data integrity after ingestion
    #[arg(long)]
    pub verify: bool,

    /// Enable health monitoring during ingestion (tracks latency and failures)
    #[arg(long)]
    pub enable_liveness: bool,

    /// Enable query liveness monitoring (runs COUNT(*) queries and tracks latency)
    #[arg(long)]
    pub enable_query_liveness: bool,

    /// Poll interval for query liveness checks in milliseconds
    #[arg(long, default_value = "500")]
    pub query_liveness_interval_ms: u64,

    /// Number of records to insert per dataset before starting Spice
    /// (for schema inference). These records are consumed during startup.
    #[arg(long, default_value = "10")]
    pub initial_records: usize,
}

/// Arguments for `DynamoDB` streaming data correctness test (multi-round CDC verification).
///
/// Self-contained command that creates tables, starts Spice, then runs multiple
/// rounds of TPC-H data ingestion with CDC mutations, verifying correctness after
/// each round.
///
/// Each round inserts TPC-H data through mutation paths (INSERT wrong → UPDATE/DELETE
/// → INSERT correct) using a different mutation seed. `DynamoDB` upsert semantics means
/// each round overwrites previous data. The final state is always correct TPC-H data,
/// so the same verification queries work every round.
///
/// ## Environment Variables
///
/// ### AWS `DynamoDB`
/// - `DYNAMODB_AWS_REGION`: AWS region (required)
/// - `DYNAMODB_AWS_ACCESS_KEY_ID`: AWS access key ID (required)
/// - `DYNAMODB_AWS_SECRET_ACCESS_KEY`: AWS secret access key (required)
/// - `DYNAMODB_AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for `LocalStack`)
#[derive(Parser, Debug, Clone)]
pub struct StreamingDynamodbCorrectnessArgs {
    /// Common arguments (spicepod path, spiced path, metrics, etc.)
    #[command(flatten)]
    pub common: CommonArgs,

    /// Query set type (e.g., tpch). Determines which datasets to load.
    #[arg(long, value_enum)]
    pub queryset: QuerySetType,

    /// Scale factor for TPC-H data generation (e.g., 0.01, 0.1, 1.0)
    #[arg(long, default_value = "0.01")]
    pub scale_factor: f64,

    /// Number of correctness rounds to run. Each round uses a different mutation seed.
    #[arg(long, default_value = "3")]
    pub rounds: usize,

    /// Ratio of rows to mutate (0.0-1.0) in each round.
    /// Selected rows are split 50/50 between update path and delete path.
    #[arg(long, default_value = "0.3")]
    pub mutation_ratio: f64,

    /// Base mutation seed. Each round uses `base_seed + round_index`.
    #[arg(long, default_value = "42")]
    pub mutation_seed: u64,

    /// Number of records to insert per dataset before starting Spice
    /// (for schema inference). These records are consumed during startup.
    #[arg(long, default_value = "10")]
    pub initial_records: usize,
}
