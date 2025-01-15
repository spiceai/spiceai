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

#[derive(Parser)]
pub struct ConsistencyTestArgs {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long, default_value = "spicepod.yaml")]
    pub(crate) spicepod_path: PathBuf,

    /// Path to the spiced binary
    #[arg(short, long, default_value = "spiced")]
    pub(crate) spiced_path: PathBuf,

    /// The duration of the test in seconds
    #[arg(long)]
    pub(crate) duration: u64,

    /// The number of buckets to divide the test duration into.
    #[arg(long, default_value = "10")]
    pub(crate) buckets: usize,

    /// The embedding model (named in spicepod) to test against. Cannot be used in conjunction with `model`.
    #[arg(long)]
    pub(crate) embedding: Option<String>,

    /// The language model (named in spicepod) to test against. Cannot be used in conjunction with `embedding`.
    #[arg(long)]
    pub(crate) model: Option<String>,

    /// The threshold for the increase in percentile latency between the first and last bucket of the test.
    #[arg(long, default_value = "1.1")]
    pub(crate) increase_threshold: f64,

    /// The number of clients to run simultaneously. Each client will send a query, wait for a response, then send another query.
    #[arg(long, default_value = "1")]
    pub(crate) concurrency: usize,

    /// The number of seconds to wait for the spiced instance to become ready
    #[arg(long, default_value = "30")]
    pub(crate) ready_wait: u64,
}
