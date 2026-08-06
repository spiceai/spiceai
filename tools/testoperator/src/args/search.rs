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

use super::CommonArgs;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
pub struct SearchTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// Target test dataset to run the search test against.
    #[arg(long)]
    pub(crate) benchmark_dataset: SearchDatasetArg,
}

/// Search benchmark dataset selector. Used both as the `--benchmark-dataset` CLI value and as the
/// `benchmark_dataset` field in `testoperator dispatch` search test files.
#[derive(Clone, Copy, ValueEnum, Debug, Deserialize, Serialize)]
pub enum SearchDatasetArg {
    /// MTEB `QuoraRetrieval` (`https://huggingface.co/datasets/mteb/QuoraRetrieval_test_top_250_only_w_correct-v2/`).
    #[value(name = "quora_retrieval")]
    #[serde(rename = "quora_retrieval")]
    QuoraRetrieval,
}

impl std::fmt::Display for SearchDatasetArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchDatasetArg::QuoraRetrieval => write!(f, "quora_retrieval"),
        }
    }
}
