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

use clap::{ArgAction, Parser, ValueEnum};
use serde::{Deserialize, Serialize, Serializer};
use std::path::PathBuf;
use test_framework::TestType;

use super::dataset::{QueryOverridesArg, QuerySetArg};

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

    #[arg(long, default_value = "false", action = ArgAction::Set)]
    pub(crate) update_snapshots: bool,
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
    pub query_set: QuerySetArg,
    pub query_overrides: Option<QueryOverridesArg>,
    pub ready_wait: Option<u64>,
    pub runner_type: RunnerType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_snapshots: Option<UpdateSnapshots>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_results: Option<bool>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_scale_factor"
    )]
    pub scale_factor: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrape_spiced_metrics: Option<bool>,
}

#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::ref_option)]
fn serialize_scale_factor<S>(x: &Option<f64>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        Some(v) => {
            if v.fract() == 0.0 {
                // no fractional part → serialize as integer
                s.serialize_i64(*v as i64)
            } else {
                s.serialize_f64(*v)
            }
        }
        None => s.serialize_none(),
    }
}

impl BenchArgs {
    #[must_use]
    pub fn with_update_snapshots(mut self, update_snapshots: UpdateSnapshots) -> Self {
        self.update_snapshots = Some(update_snapshots);
        self
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UpdateSnapshots {
    Always,
    No,
}

impl From<bool> for UpdateSnapshots {
    fn from(value: bool) -> Self {
        if value {
            UpdateSnapshots::Always
        } else {
            UpdateSnapshots::No
        }
    }
}

/// Load workflow arguments, defined in the test files
#[derive(Debug, Clone, Serialize)]
pub struct LoadArgs {
    #[serde(flatten)]
    pub bench_args: BenchArgs,
    pub duration: Option<u64>,
}

impl<'de> Deserialize<'de> for LoadArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct LoadArgsHelper {
            #[serde(flatten)]
            bench_args: BenchArgs,
            duration: Option<u64>,
        }

        let mut helper = LoadArgsHelper::deserialize(deserializer)?;

        // Default scrape_spiced_metrics to true for load tests if not specified
        if helper.bench_args.scrape_spiced_metrics.is_none() {
            helper.bench_args.scrape_spiced_metrics = Some(true);
        }

        Ok(LoadArgs {
            bench_args: helper.bench_args,
            duration: helper.duration,
        })
    }
}

/// Represents the type of runner to use in the action
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum RunnerType {
    #[serde(rename = "spiceai-runners")]
    SelfHosted,
    #[serde(rename = "spiceai-large-runners")]
    LargeSelfHosted,
    #[serde(rename = "spiceai-dev-runners")]
    Dev,
    #[serde(rename = "spiceai-dev-large-runners")]
    DevLarge,
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
