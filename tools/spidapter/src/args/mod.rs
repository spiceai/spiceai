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

use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand, ValueEnum};

mod dataset;
pub use dataset::{DataConsistencyArgs, DatasetTestArgs, LoadTestArgs, QueryArgs, QuerySetLoader};

#[cfg(feature = "append")]
mod append;
#[cfg(feature = "append")]
pub use append::AppendTestArgs;

pub mod dispatch;
use dispatch::DispatchArgs;

mod evals;
pub use evals::EvalsTestArgs;

mod search;
pub use search::SearchTestArgs;

mod text_to_sql;
pub use text_to_sql::TextToSqlArgs;

mod streaming;
pub use streaming::{StreamingDynamodbDispatchArgs, StreamingDynamodbTestArgs};

#[derive(Subcommand)]
pub enum Commands {
    /// Run a test
    #[command(subcommand)]
    Run(TestCommands),
    /// Export the spicepod environment that would run for a test
    #[command(subcommand)]
    Export(TestCommands),
    /// Dispatch a number of tests in GitHub Actions
    Dispatch(DispatchArgs),
    /// Run spidapter as a newline-delimited JSON-RPC server over stdio
    Stdio(StdioArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct StdioArgs {
    /// Log received requests and child-command execution details to stderr
    #[arg(long)]
    pub verbose: bool,

    /// Base URL for Spice Cloud API calls.
    /// Falls back to `SPICE_CLOUD_API_URL` env var, then `https://api.spice.ai`.
    #[arg(long)]
    pub spice_cloud_api_url: Option<String>,

    /// Timeout in seconds to wait for a Spice Cloud deployment to become ready.
    #[arg(long, default_value = "600")]
    pub ready_wait: u64,
}

#[derive(Subcommand)]
pub enum TestCommands {
    /// Run a throughput test
    Throughput(DatasetTestArgs),
    /// Run an extended load test
    Load(LoadTestArgs),
    /// Run a single-run benchmark
    Bench(DatasetTestArgs),
    /// Run a data consistency test
    DataConsistency(DataConsistencyArgs),
    /// Run a models evaluations test
    Evals(EvalsTestArgs),
    #[cfg(feature = "append")]
    Append(AppendTestArgs),
    Search(SearchTestArgs),
    /// Execute benchmark queries against a pre-existing spiced instance
    Query(QueryArgs),
    /// Run a text-to-sql test
    TextToSql(TextToSqlArgs),
    /// Run multi-config `DynamoDB` streaming benchmarks (ingest once, benchmark many)
    StreamingDynamodbDispatch(StreamingDynamodbDispatchArgs),
    /// Run a streaming ingestion benchmark for `DynamoDB` Streams
    StreamingDynamodb(StreamingDynamodbTestArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SpicedStartMode {
    /// Start a local `spiced` process from `--spiced-path`.
    Local,
    /// Start a Spice Cloud instance through a start API.
    SpiceCloud,
}

/// Arguments Common to all [`TestCommands`].
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long, default_value = "spicepod.yaml")]
    pub(crate) spicepod_path: PathBuf,

    #[arg(short('z'), long)]
    pub(crate) spicepod_dependencies: Option<PathBuf>,

    /// The number of clients to run simultaneously. Each client will send a query, wait for a response, then send another query.
    #[arg(long, default_value = "1")]
    pub(crate) concurrency: usize,

    /// Path to the spiced binary, or URL to an already-running spiced instance's Flight endpoint
    /// (e.g., `http://localhost:50051` to connect to an external instance)
    #[arg(short, long, default_value = "spiced")]
    pub(crate) spiced_path: String,

    /// How to provision a `spiced` instance for the test.
    #[arg(long, value_enum, default_value_t = SpicedStartMode::Local)]
    pub(crate) spiced_start_mode: SpicedStartMode,

    /// Base URL for Spice Cloud start API calls (used only with `--spiced-start-mode spice-cloud`).
    #[arg(long, requires = "spiced_start_mode")]
    pub(crate) spiced_start_api_url: Option<String>,

    /// The number of seconds to wait for the spiced instance to become ready
    #[arg(long, default_value = "600")]
    pub(crate) ready_wait: u64,

    /// The duration of the test in seconds
    #[arg(long, default_value = "60")]
    pub(crate) duration: u64,

    /// Whether to disable progress bars, for CI or non-interactive environments
    #[arg(long)]
    pub(crate) disable_progress_bars: bool,

    /// An optional data directory, to symlink into the spiced instance
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Whether to enable metrics collection
    #[arg(long)]
    pub(crate) metrics: bool,

    /// Whether to enable scraping spiced metrics (automatically enables --metrics for spiced)
    #[arg(long)]
    pub(crate) scrape_spiced_metrics: bool,

    /// OTLP metrics collector endpoint (HTTP or gRPC). If unset, falls back to Arrow telemetry.
    #[arg(long)]
    pub(crate) otlp_endpoint: Option<String>,

    /// Additional OTLP headers in key=value form. Can be repeated.
    #[arg(long, value_parser = parse_key_val, action = ArgAction::Append, requires = "otlp_endpoint", value_name = "KEY=VALUE")]
    pub(crate) otlp_header: Vec<(String, String)>,
}

impl CommonArgs {
    /// Check if `spiced_path` is a URL to an external instance
    #[must_use]
    pub fn is_external_instance(&self) -> bool {
        self.spiced_path.starts_with("http://") || self.spiced_path.starts_with("https://")
    }

    /// Get the spiced path as a `PathBuf` (only valid when not an external instance)
    #[must_use]
    pub fn spiced_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.spiced_path)
    }
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| "expected KEY=VALUE formatted header".to_string())?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
