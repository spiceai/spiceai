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

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use test_framework::{queries::QuerySet, TestType};

use super::dataset::QueryOverridesArg;

#[derive(Parser, Debug, Clone)]
pub struct DispatchArgs {
    /// A positional argument for the directory to scan, or test file
    #[clap(index = 1)]
    pub(crate) path: PathBuf,

    /// The GitHub workflow to execute
    #[arg(long)]
    pub(crate) workflow: Workflow,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
pub enum Workflow {
    Bench,
    Throughput,
    Load,
    DataConsistency,
    HttpConsistency,
    HttpOverhead,
}

impl From<Workflow> for TestType {
    fn from(workflow: Workflow) -> Self {
        match workflow {
            Workflow::Bench => TestType::Benchmark,
            Workflow::Throughput => TestType::Throughput,
            Workflow::Load => TestType::Load,
            Workflow::DataConsistency => TestType::DataConsistency,
            Workflow::HttpConsistency => TestType::HttpConsistency,
            Workflow::HttpOverhead => TestType::HttpOverhead,
        }
    }
}

/// Represents a single test file payload
#[derive(Debug, Clone, Deserialize)]
pub struct DispatchTestFile {
    pub tests: DispatchTests,
}

/// Represents the tests that can be defined in a test file
/// The tests correspond to the different workflows that can be dispatched
/// If a test is not defined, it will be skipped for that workflow
#[derive(Debug, Clone, Deserialize)]
pub struct DispatchTests {
    pub bench: Option<BenchArgs>,
    pub throughput: Option<BenchArgs>,
    pub load: Option<LoadArgs>,
    // TODO: add data consistency, http consistency, http overhead
}

/// Benchmark and throughput workflow arguments, defined in the test files
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchArgs {
    pub spicepod_path: PathBuf,
    pub query_set: QuerySet,
    pub query_overrides: Option<QueryOverridesArg>,
    pub ready_wait: Option<u64>,
    pub runner_type: RunnerType,
}

/// Load workflow arguments, defined in the test files
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadArgs {
    #[serde(flatten)]
    pub bench_args: BenchArgs,
    pub duration: Option<u64>,
}

/// Represents the type of runner to use in the action
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum RunnerType {
    #[serde(rename = "spiceai-runners")]
    SelfHosted,
    #[serde(rename = "spiceai-large-runners")]
    LargeSelfHosted,
}

/// Payload sent to the GitHub Actions workflow request for Benchmark and Throughput tests
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the environment
#[derive(Debug, Clone, Serialize)]
pub struct BenchWorkflowArgs {
    #[serde(flatten)]
    pub bench_args: BenchArgs,
    pub spiced_commit: String,
}

/// Payload sent to the GitHub Actions workflow request for Load tests
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the environment
#[derive(Debug, Clone, Serialize)]
pub struct LoadWorkflowArgs {
    #[serde(flatten)]
    pub load_args: LoadArgs,
    pub spiced_commit: String,
}
