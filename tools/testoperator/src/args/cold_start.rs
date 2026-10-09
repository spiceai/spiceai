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

use std::path::PathBuf;

use clap::Parser;

use super::CommonArgs;

/// Arguments for the cold-start test: how long a fresh `spiced`, with an empty
/// data directory, takes to load the spicepod's accelerations and report ready.
#[derive(Parser, Debug, Clone)]
pub struct ColdStartArgs {
    #[command(flatten)]
    pub(crate) common: CommonArgs,

    /// A previous `spiced` release to run on the same host as the baseline for
    /// the `--spiced-path` binary. Baseline and candidate runs alternate.
    #[arg(long)]
    pub(crate) baseline_spiced_path: Option<PathBuf>,

    /// Cold starts per binary.
    #[arg(long, default_value = "3")]
    pub(crate) runs: usize,

    /// CPU budget passed to every `spiced` as `--cpu-cores`.
    #[arg(long, default_value = "2")]
    pub(crate) cpu_cores: String,

    /// Fail when the candidate's median or slowest time to ready exceeds this
    /// multiple of the baseline's median or slowest.
    #[arg(long, default_value = "2.0")]
    pub(crate) max_ready_ratio: f64,

    /// Fail when a candidate run commits more full Cayenne compactions than this
    /// before it reports ready.
    #[arg(long)]
    pub(crate) max_full_compactions: Option<u64>,

    /// Write the results as JSON to this path.
    #[arg(long)]
    pub(crate) results_out: Option<PathBuf>,

    /// Append a Markdown summary to this path, e.g. `$GITHUB_STEP_SUMMARY`.
    #[arg(long)]
    pub(crate) summary_out: Option<PathBuf>,
}
