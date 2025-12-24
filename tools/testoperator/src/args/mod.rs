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

use clap::{ArgAction, Parser, Subcommand};

mod http;
pub use http::{HttpConsistencyTestArgs, HttpOverheadTestArgs, HttpTestArgs};

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
    /// Run an HTTP consistency test
    HttpConsistency(HttpConsistencyTestArgs),
    /// Run an HTTP overhead test
    HttpOverhead(HttpOverheadTestArgs),
    /// Run a models evaluations test
    Evals(EvalsTestArgs),
    #[cfg(feature = "append")]
    Append(AppendTestArgs),
    Search(SearchTestArgs),
    /// Execute benchmark queries against a pre-existing spiced instance
    Query(QueryArgs),
    /// Run a text-to-sql test
    TextToSql(TextToSqlArgs),
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

    /// The number of seconds to wait for the spiced instance to become ready
    #[arg(long, default_value = "30")]
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
