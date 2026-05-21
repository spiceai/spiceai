/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use clap::{Parser, Subcommand, ValueEnum};
use spice_cloud_client::types::UpdateChannel;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum BackendMode {
    Scp,
    Local,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum PgAccelerationEngine {
    Cayenne,
    Duckdb,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Single spiced process (no scheduler/executor split).
    SingleNode,
    /// Scheduler + N executor processes with mTLS.
    Distributed,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run spidapter as a newline-delimited JSON-RPC server over stdio
    Stdio(Box<StdioArgs>),
    /// Run spidapter backed by a local spiced cluster (scheduler + executors)
    LocalSpiced(Box<LocalSpicedArgs>),
    /// Run spidapter backed by a local cayenne-flightsql instance
    CayenneFlightsql(Box<CayenneFlightsqlArgs>),
}

#[derive(Parser, Debug, Clone)]
pub struct StdioArgs {
    /// Log received requests and child-command execution details to stderr
    #[arg(long)]
    pub verbose: bool,

    /// Base URL for Spice Cloud API calls.
    #[arg(
        long,
        env = "SPICE_CLOUD_API_URL",
        default_value = "https://api.spice.ai"
    )]
    pub spice_cloud_api_url: String,

    /// Timeout in seconds to wait for a Spice Cloud deployment to become ready.
    #[arg(long, default_value = "600")]
    pub ready_wait: u64,

    /// Release channel for the spice.ai runtime image (stable, preview, nightly, internal).
    #[arg(long)]
    pub channel: Option<UpdateChannel>,

    /// Custom container image tag (e.g. `spicebench-sf10`).
    /// When set, the app's image tag is updated before deploying.
    #[arg(long, env = "SPIDAPTER_IMAGE_TAG")]
    pub image_tag: Option<String>,

    /// Spice Cloud API key for authentication.
    /// When not provided, falls back to `SPICEAI_API_KEY`, `SPICE_API_KEY`, `SPICE_SPICEAI_API_KEY`, or `SPICE_SPICEAI_TOKEN`.
    #[arg(long, env = "SPICEAI_API_KEY")]
    pub api_key: Option<String>,

    /// Backend mode for provisioning: `scp` (Spice Cloud Platform, default) or `local`.
    #[arg(long, env = "SPIDAPTER_BACKEND", default_value = "scp")]
    pub backend: BackendMode,

    /// Override the Flight SQL endpoint URL instead of deriving it from the deployment cname.
    #[arg(long, env = "SPIDAPTER_FLIGHT_URL")]
    pub flight_url: Option<String>,

    /// Memory limit for the Spice Cloud app (scheduler) pod (e.g. `16Gi`).
    #[arg(long, env = "SPIDAPTER_APP_MEMORY_LIMIT")]
    pub app_memory_limit: Option<String>,

    /// CPU limit for the Spice Cloud app (scheduler) pod (e.g. `2`).
    #[arg(long, env = "SPIDAPTER_APP_CPU_LIMIT")]
    pub app_cpu_limit: Option<String>,

    /// CPU request for the Spice Cloud app (scheduler) pod (e.g. `0.1`).
    #[arg(long, env = "SPIDAPTER_APP_CPU_REQUEST")]
    pub app_cpu_request: Option<String>,

    /// Memory request for the Spice Cloud app (scheduler) pod (e.g. `256Mi`).
    #[arg(long, env = "SPIDAPTER_APP_MEMORY_REQUEST")]
    pub app_memory_request: Option<String>,

    /// Number of replicas for the Spice Cloud app (scheduler). Defaults to the platform default when not set.
    #[arg(long, env = "SPIDAPTER_APP_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    pub app_replicas: Option<i32>,

    /// Number of replicas for the Spice Cloud executor.
    #[arg(long, env = "SPIDAPTER_EXECUTOR_REPLICAS", default_value = "1", value_parser = clap::value_parser!(i32).range(0..))]
    pub executor_replicas: i32,

    /// Memory limit for the Spice Cloud executor pod (e.g. `16Gi`).
    #[arg(long, env = "SPIDAPTER_EXECUTOR_MEMORY_LIMIT")]
    pub executor_memory_limit: Option<String>,

    /// CPU limit for the Spice Cloud executor pod (e.g. `2`).
    #[arg(long, env = "SPIDAPTER_EXECUTOR_CPU_LIMIT")]
    pub executor_cpu_limit: Option<String>,

    /// CPU request for the Spice Cloud executor pod (e.g. `0.1`).
    #[arg(long, env = "SPIDAPTER_EXECUTOR_CPU_REQUEST")]
    pub executor_cpu_request: Option<String>,

    /// Memory request for the Spice Cloud executor pod (e.g. `256Mi`).
    #[arg(long, env = "SPIDAPTER_EXECUTOR_MEMORY_REQUEST")]
    pub executor_memory_request: Option<String>,

    /// PVC block storage size in GB for the Spice Cloud app (scheduler) pod (e.g. `10`).
    #[arg(long, env = "SPIDAPTER_APP_STORAGE_SIZE_GB")]
    pub app_storage_size_gb: Option<f64>,

    /// PVC block storage size in GB for the Spice Cloud executor pod (e.g. `5`).
    #[arg(long, env = "SPIDAPTER_EXECUTOR_STORAGE_SIZE_GB")]
    pub executor_storage_size_gb: Option<f64>,

    /// S3 URL prefix for the spiced scheduler state location (e.g. `s3://bucket/state`).
    #[arg(long, env = "SCHEDULER_STATE_LOCATION")]
    pub scheduler_state_location: Option<String>,

    /// AWS region for S3 data sources and scheduler state (e.g. `us-east-1`).
    /// Falls back to `AWS_DEFAULT_REGION` environment variable if not set.
    #[arg(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// Cayenne Catalog data directory
    #[arg(long, env = "SPIDAPTER_CAYENNE_DATA_DIR")]
    pub cayenne_data_dir: Option<String>,

    /// Cayenne Catalog metadata directory
    #[arg(long, env = "SPIDAPTER_CAYENNE_METADATA_DIR")]
    pub cayenne_metadata_dir: Option<String>,

    /// Ephemeral storage limit for pods (e.g. `50Gi`).
    #[arg(long, env = "SPIDAPTER_EPHEMERAL_STORAGE_LIMIT_GB")]
    pub ephemeral_storage_limit_gb: Option<String>,

    /// Spice Cloud organization tag to apply to created app
    #[arg(long, env = "SPIDAPTER_ORGANIZATION_TAG")]
    pub organization_tag: Option<String>,

    /// Query memory limit to apply to `runtime.query.memory_limit` spicepod configuration (e.g. `150Gi`).
    #[arg(long, env = "SPIDAPTER_QUERY_MEMORY_LIMIT")]
    pub query_memory_limit: Option<String>,

    /// Deployment mode: `single-node` or `distributed` (scheduler + executors).
    #[arg(long, env = "SPIDAPTER_DEPLOYMENT_MODE", default_value = "distributed")]
    pub deployment_mode: DeploymentMode,

    /// `PostgreSQL` host for WAL CDC mode. When set, spidapter writes via the `PostgreSQL`
    /// ADBC driver and configures Spice to read via WAL CDC.
    #[arg(long, env = "PG_HOST")]
    pub pg_host: Option<String>,

    /// `PostgreSQL` port for WAL CDC mode.
    #[arg(long, env = "PG_PORT", default_value = "5432")]
    pub pg_port: u16,

    /// `PostgreSQL` username for WAL CDC mode.
    #[arg(long, env = "PG_USER")]
    pub pg_user: Option<String>,

    /// `PostgreSQL` password for WAL CDC mode.
    #[arg(long, env = "PG_PASSWORD", default_value = "")]
    pub pg_password: String,

    /// `PostgreSQL` database name for WAL CDC mode.
    #[arg(long, env = "PG_DATABASE")]
    pub pg_database: Option<String>,

    /// Acceleration engine for `PostgreSQL` WAL CDC datasets (`cayenne` or `duckdb`).
    #[arg(long, env = "PG_ACCELERATION", default_value = "cayenne")]
    pub pg_acceleration: PgAccelerationEngine,

    /// EC2 subnet ID for provisioning a `PostgreSQL` instance. When set together with
    /// `EC2_SECURITY_GROUP_ID`, spidapter launches an EC2 instance instead of using
    /// an existing `PostgreSQL` host.
    #[arg(long, env = "EC2_SUBNET_ID")]
    pub ec2_subnet_id: Option<String>,

    /// EC2 security group ID for the provisioned `PostgreSQL` instance.
    #[arg(long, env = "EC2_SECURITY_GROUP_ID")]
    pub ec2_security_group_id: Option<String>,

    /// AMI ID for the EC2 `PostgreSQL` instance (Ubuntu 22.04 recommended).
    #[arg(long, env = "EC2_AMI_ID")]
    pub ec2_ami_id: Option<String>,

    /// EC2 instance type for the `PostgreSQL` instance.
    #[arg(long, env = "EC2_INSTANCE_TYPE", default_value = "m5.large")]
    pub ec2_instance_type: String,

    /// Assign a public IP to the provisioned EC2 `PostgreSQL` instance.
    /// Required when running spidapter outside the target VPC (e.g. local development).
    #[arg(long, env = "EC2_ASSOCIATE_PUBLIC_IP", default_value_t = false)]
    pub ec2_associate_public_ip: bool,

    /// IAM instance profile name or ARN to attach to the provisioned EC2 instance.
    /// Required for AWS Systems Manager Session Manager access (connect via AWS console).
    /// The profile must include the `AmazonSSMManagedInstanceCore` managed policy.
    #[arg(long, env = "EC2_IAM_INSTANCE_PROFILE")]
    pub ec2_iam_instance_profile: Option<String>,

    /// Name or path of the spiced binary to spawn (local backend only).
    #[arg(long, default_value = "spiced")]
    pub spiced_binary: String,
}

#[derive(Parser, Debug, Clone)]
pub struct LocalSpicedArgs {
    /// Log received requests and child-command execution details to stderr.
    #[arg(long)]
    pub verbose: bool,

    /// Timeout in seconds to wait for the local spiced cluster to become ready.
    #[arg(long, default_value = "600")]
    pub ready_wait: u64,

    /// AWS region for S3 data sources and scheduler state (e.g. `us-east-1`).
    /// Falls back to `AWS_DEFAULT_REGION` environment variable if not set.
    #[arg(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// Cayenne Catalog data directory.
    #[arg(long, env = "SPIDAPTER_CAYENNE_DATA_DIR")]
    pub cayenne_data_dir: Option<String>,

    /// Cayenne Catalog metadata directory.
    #[arg(long, env = "SPIDAPTER_CAYENNE_METADATA_DIR")]
    pub cayenne_metadata_dir: Option<String>,

    /// S3 URL prefix for the spiced scheduler state location (e.g. `s3://bucket/state`).
    #[arg(long, env = "SCHEDULER_STATE_LOCATION")]
    pub scheduler_state_location: Option<String>,

    /// Query memory limit to apply to `runtime.query.memory_limit` spicepod configuration (e.g. `150Gi`).
    #[arg(long, env = "SPIDAPTER_QUERY_MEMORY_LIMIT")]
    pub query_memory_limit: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct CayenneFlightsqlArgs {
    /// Log received requests to stderr.
    #[arg(long)]
    pub verbose: bool,

    /// Directory for Cayenne Vortex data files.
    #[arg(long, env = "CAYENNE_DATA_DIR")]
    pub cayenne_data_dir: Option<String>,

    /// Directory for Cayenne `SQLite` metadata files.
    #[arg(long, env = "CAYENNE_METADATA_DIR")]
    pub cayenne_metadata_dir: Option<String>,

    /// `DataFusion` catalog name.
    #[arg(long, env = "FLIGHTSQL_CATALOG", default_value = "cayenne")]
    pub catalog: String,

    /// Default schema for unqualified table references.
    #[arg(long, env = "FLIGHTSQL_DEFAULT_SCHEMA", default_value = "public")]
    pub default_schema: String,

    /// Vortex footer cache size in MB.
    #[arg(long, env = "CAYENNE_FOOTER_CACHE_MB")]
    pub cayenne_footer_cache_mb: Option<usize>,

    /// Vortex segment cache size in MB.
    #[arg(long, env = "CAYENNE_SEGMENT_CACHE_MB")]
    pub cayenne_segment_cache_mb: Option<usize>,

    /// Target Vortex file size in MB.
    #[arg(long, env = "CAYENNE_TARGET_FILE_SIZE_MB")]
    pub cayenne_target_file_size_mb: Option<usize>,

    /// Periodic catalog refresh interval (seconds). If omitted, refresh runs only at startup.
    #[arg(long, env = "CAYENNE_REFRESH_INTERVAL_SECS")]
    pub refresh_interval_secs: Option<u64>,

    /// Timeout in seconds to wait for the Flight SQL server to become ready.
    #[arg(long, default_value = "30")]
    pub ready_wait: u64,

    /// Name or path of the cayenne-flightsql binary to spawn.
    #[arg(long, default_value = "cayenne-flightsql")]
    pub cayenne_flightsql_binary: String,
}
