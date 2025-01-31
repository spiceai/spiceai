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

use super::HttpTestArgs;

#[derive(Parser, Debug, Clone)]
pub struct DispatchArgs {
    /// A positional argument for the directory to scan, or test file
    #[clap(index = 1)]
    pub(crate) path: PathBuf,

    /// The GitHub workflow to execute
    #[arg(long)]
    pub(crate) workflow: Workflow,

    #[arg(long, env = "GH_TOKEN")]
    pub(crate) github_token: String,

    #[arg(long, env = "SPICED_COMMIT", default_value = "")]
    pub(crate) spiced_commit: String,

    #[arg(long, env = "WORKFLOW_COMMIT", default_value = "trunk")]
    pub(crate) workflow_commit: String,
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
    pub http_consistency: Option<HttpConsistencyArgs>,
    pub http_overhead: Option<HttpOverheadArgs>,
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

/// Payload sent to the GitHub Actions workflow request for HTTP consistency tests
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the environment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConsistencyArgs {
    #[serde(flatten)]
    pub http_args: HttpTestArgs,

    pub buckets: usize,
    pub spicepod_path: PathBuf,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
}

/// Payload sent to the GitHub Actions workflow request for HTTP overhead tests
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the environment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOverheadArgs {
    #[serde(flatten)]
    pub http_args: HttpTestArgs,
    pub spicepod_path: PathBuf,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,

    pub base: OverheadBaseModel,
    pub base_component: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_payload_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OverheadBaseModel {
    #[serde(rename = "openai")]
    OpenAI,
    Anthropic,
    Xai,
}

/// A wrapper around input arguments, from a test file, to use in a GitHub Actions workflow, that also expects
/// a `spiced_commit` input.
///
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the
/// environment.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowArgs<T: Serialize> {
    #[serde(flatten)]
    pub specific_args: T,
    pub spiced_commit: String,
}
