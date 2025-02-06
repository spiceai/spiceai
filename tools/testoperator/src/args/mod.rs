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

use clap::{Parser, Subcommand};

mod http;
pub use http::{HttpConsistencyTestArgs, HttpOverheadTestArgs, HttpTestArgs};

mod dataset;
pub use dataset::{DataConsistencyArgs, DatasetTestArgs};

pub mod dispatch;
use dispatch::DispatchArgs;

mod evals;
pub use evals::EvalsTestArgs;

#[derive(Subcommand)]
pub enum Commands {
    // Run a test
    #[command(subcommand)]
    Run(TestCommands),
    // Export the spicepod environment that would run for a test
    #[command(subcommand)]
    Export(TestCommands),
    // Dispatch a number of tests in GitHub Actions
    Dispatch(DispatchArgs),
}

#[derive(Subcommand)]
pub enum TestCommands {
    Throughput(DatasetTestArgs),
    Load(DatasetTestArgs),
    Bench(DatasetTestArgs),
    DataConsistency(DataConsistencyArgs),
    HttpConsistency(HttpConsistencyTestArgs),
    HttpOverhead(HttpOverheadTestArgs),
    Evals(EvalsTestArgs),
}

/// Arguments Common to all [`TestCommands`].
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long, default_value = "spicepod.yaml")]
    pub(crate) spicepod_path: PathBuf,

    /// The number of clients to run simultaneously. Each client will send a query, wait for a response, then send another query.
    #[arg(long, default_value = "1")]
    pub(crate) concurrency: usize,

    /// Path to the spiced binary
    #[arg(short, long, default_value = "spiced")]
    pub(crate) spiced_path: PathBuf,

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
}
