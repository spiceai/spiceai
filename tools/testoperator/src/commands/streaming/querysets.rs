/*
Copyright 2026 The Spice.ai OSS Authors

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

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::datasets::LineitemDataset;
use super::traits::StreamingDataset;

/// Available query set types for streaming benchmarks.
///
/// Each query set defines which datasets (tables) to load for the benchmark.
#[derive(Debug, Clone, Copy, ValueEnum, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum QuerySetType {
    Tpch,
}

impl QuerySetType {
    /// Create the dataset instances for this query set.
    #[must_use]
    pub fn create_datasets(&self) -> Vec<Box<dyn StreamingDataset>> {
        match self {
            QuerySetType::Tpch => vec![Box::new(LineitemDataset)],
        }
    }
}

impl std::fmt::Display for QuerySetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuerySetType::Tpch => write!(f, "tpch"),
        }
    }
}
