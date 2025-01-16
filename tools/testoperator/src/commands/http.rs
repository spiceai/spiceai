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
use std::path::PathBuf;

use super::CommonArgs;

#[derive(Parser)]
pub struct HttpTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// The embedding model (named in spicepod) to test against. Cannot be used in conjunction with `model`.
    #[arg(long)]
    pub(crate) embedding: Option<String>,

    /// The language model (named in spicepod) to test against. Cannot be used in conjunction with `embedding`.
    #[arg(long)]
    pub(crate) model: Option<String>,

    /// The path to a file containing payloads to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload`.
    #[arg(long)]
    pub(crate) payload_file: Option<PathBuf>,

    /// The payload to use in testing. Either JSONL of compatible request bodies, or individual string payloads. Cannot not be used in conjunction with `payload_file`.
    #[arg(long)]
    pub(crate) payload: Option<Vec<String>>,
}

#[derive(Parser)]
pub struct HttpConsistencyTestArgs {
    #[command(flatten)]
    pub(crate) http: HttpTestArgs,

    /// The number of buckets to divide the test duration into.
    #[arg(long, default_value = "10")]
    pub(crate) buckets: usize,

    /// The threshold for the increase in percentile latency between the first and last bucket of the test.
    #[arg(long, default_value = "1.1")]
    pub(crate) increase_threshold: f64,
}

#[derive(Parser)]
pub struct HttpOverheadTestArgs {
    #[clap(flatten)]
    pub(crate) http: HttpTestArgs,

    /// The threshold for the increase in percentile latency between the spice component and the underlying HTTP connection.
    #[arg(long, default_value = "1.1")]
    pub(crate) increase_threshold: f64,

    /// The base URL of the underlying HTTP service to test against.
    #[arg(long)]
    pub(crate) http_base: String,

    /// If the component has a different name between the spicepod and the HTTP service, specify the name of the component in the HTTP service.
    #[arg(long)]
    pub(crate) component_override: Option<String>,
}
