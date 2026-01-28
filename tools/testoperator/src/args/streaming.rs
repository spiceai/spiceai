/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

/// Arguments for streaming ingestion benchmarks.
#[derive(Parser, Debug, Clone)]
#[expect(clippy::struct_excessive_bools)]
pub struct StreamingTestArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Streaming source type (e.g., dynamodb-streams, kafka)
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

    // AWS-specific arguments (for aws-dynamodb-streams source)
    /// AWS region (required for aws-dynamodb-streams source)
    #[arg(long, default_value = "us-east-1")]
    pub aws_region: String,

    /// AWS authentication method: `iam_role`, key, or env
    #[arg(long, value_enum, default_value = "iam-role")]
    pub aws_auth: AwsAuth,

    /// AWS access key ID (required when `aws_auth=key`)
    #[arg(long)]
    pub aws_access_key_id: Option<String>,

    /// AWS secret access key (required when `aws_auth=key`)
    #[arg(long)]
    pub aws_secret_access_key: Option<String>,

    /// AWS session token (optional, for temporary credentials)
    #[arg(long)]
    pub aws_session_token: Option<String>,

    /// Custom AWS endpoint URL (for `LocalStack`, testing, etc.)
    #[arg(long)]
    pub aws_endpoint_url: Option<String>,

    // Query liveness monitoring arguments
    /// Enable query liveness monitoring (runs COUNT(*) queries and tracks latency)
    #[arg(long)]
    pub enable_query_liveness: bool,

    /// Poll interval for query liveness checks in milliseconds
    #[arg(long, default_value = "500")]
    pub query_liveness_interval_ms: u64,

    // Mutation testing arguments (for CDC testing)
    /// Enable CDC mutation testing (each row goes through X mutations before final state)
    #[arg(long)]
    pub enable_mutations: bool,

    /// Random seed for reproducible mutations
    #[arg(long, default_value = "42")]
    pub mutation_seed: u64,

    /// Number of mutations per row (including initial insert and final update)
    #[arg(long, default_value = "3")]
    pub mutations_per_row: usize,

    /// Maximum number of rows to mutate per dataset (0 for all rows)
    #[arg(long, default_value = "100")]
    pub max_mutation_rows: usize,
}

/// AWS authentication methods for CLI.
#[derive(Debug, Clone, Copy, clap::ValueEnum, Default)]
pub enum AwsAuth {
    /// Use IAM role authentication (from environment, metadata service, etc.)
    #[default]
    IamRole,
    /// Use explicit access key credentials
    Key,
    /// Use environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
    Env,
}
