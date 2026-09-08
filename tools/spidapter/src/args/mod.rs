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

use clap::{Parser, Subcommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeploymentMode {
    /// Single spiced process (no scheduler/executor split).
    SingleNode,
    /// Scheduler + N executor processes with mTLS.
    Cluster,
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

    /// Enable debug mode on the Spice Cloud deployment (sets debug=true on the deployment request).
    #[arg(long, env = "SPIDAPTER_SPICE_DEBUG", default_value_t = false)]
    pub spice_debug: bool,

    /// Named scenario to load (e.g. `postgres-wal`). Defines the source and EC2/cloud config.
    /// Built-in: `direct-ingest`, `postgres-wal`, `postgres-debezium`, `dynamodb-streams`, `mongodb-streams`.
    #[arg(long, env = "SPIDAPTER_SCENARIO")]
    pub scenario: Option<String>,

    /// Directory to search for scenario YAML files before falling back to built-ins.
    /// When set, `--scenario foo` loads `<scenario_base_path>/foo.yaml` if it exists.
    #[arg(long, env = "SPIDAPTER_SCENARIO_BASE_PATH")]
    pub scenario_base_path: Option<String>,

    /// Timeout in seconds to wait for a Spice Cloud deployment to become ready.
    #[arg(long, default_value = "600")]
    pub ready_wait: u64,

    /// Spice Cloud API key for authentication.
    /// When not provided, falls back to `SPICEAI_API_KEY`, `SPICE_API_KEY`, `SPICE_SPICEAI_API_KEY`, or `SPICE_SPICEAI_TOKEN`.
    #[arg(long, env = "SPICEAI_API_KEY")]
    pub api_key: Option<String>,

    /// Base URL for Spice Cloud API calls.
    #[arg(
        long,
        env = "SPICE_CLOUD_API_URL",
        default_value = "https://api.spice.ai"
    )]
    pub spice_cloud_api_url: String,

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

    /// Named scenario to load (e.g. `direct-ingest`). Defines source and compute config.
    #[arg(long, env = "SPIDAPTER_SCENARIO")]
    pub scenario: Option<String>,

    /// Name or path of the spiced binary to spawn.
    #[arg(long, default_value = "spiced")]
    pub spiced_binary: String,
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
