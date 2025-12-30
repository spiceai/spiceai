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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::path::PathBuf;
use test_framework::TestType;

use super::dataset::{QueryOverridesArg, QuerySetArg};

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

    #[arg(long, action = ArgAction::Set, default_value_t = false, default_missing_value = "true", num_args = 0..=1, require_equals = false)]
    pub(crate) validate: bool,

    /// Maximum number of concurrent workflow runs allowed
    #[arg(long)]
    pub(crate) max_concurrent: Option<usize>,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
pub enum Workflow {
    Bench,
    Throughput,
    Load,
    Append,
    DataConsistency,
}

impl From<Workflow> for TestType {
    fn from(workflow: Workflow) -> Self {
        match workflow {
            Workflow::Bench => TestType::Benchmark,
            Workflow::Throughput => TestType::Throughput,
            Workflow::Load => TestType::Load,
            Workflow::Append => TestType::Append,
            Workflow::DataConsistency => TestType::DataConsistency,
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
/// Each test type can be defined as a single section or as an array of sections
/// If a test is not defined, it will be skipped for that workflow
#[derive(Debug, Clone, Deserialize)]
pub struct DispatchTests {
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub bench: Vec<BenchArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub throughput: Vec<BenchArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub load: Vec<LoadArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub append: Vec<AppendArgs>,
}

/// Benchmark and throughput workflow arguments, defined in the test files
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchArgs {
    pub spicepod_path: PathBuf,
    pub query_set: QuerySetArg,
    pub query_overrides: Option<QueryOverridesArg>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

/// Custom deserializer that accepts either a single item or a vector of items
fn deserialize_single_or_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SingleOrVec<T> {
        Single(T),
        Vec(Vec<T>),
    }

    match SingleOrVec::deserialize(deserializer)? {
        SingleOrVec::Single(single) => Ok(vec![single]),
        SingleOrVec::Vec(vec) => Ok(vec),
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub random_param_set_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_clients: Option<bool>,
}

/// Append workflow arguments, defined in the test files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendArgs {
    pub spicepod_path: PathBuf,
    pub query_set: QuerySetArg,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_overrides: Option<QueryOverridesArg>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_steps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_conflict_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_retention_data: Option<bool>,
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
            concurrency: Option<u64>,
            random_param_set_count: Option<usize>,
            http_clients: Option<bool>,
        }

        let mut helper = LoadArgsHelper::deserialize(deserializer)?;

        // Default scrape_spiced_metrics to true for load tests if not specified
        if helper.bench_args.scrape_spiced_metrics.is_none() {
            helper.bench_args.scrape_spiced_metrics = Some(true);
        }

        // Remove ready_wait parameter as it's not supported by testoperator_run_load workflow
        if helper.bench_args.ready_wait.is_some() {
            eprintln!(
                "Warning: ready_wait parameter (spicepod_path = {}) is not supported by testoperator_run_load workflow and will be ignored",
                helper.bench_args.spicepod_path.display()
            );
            helper.bench_args.ready_wait = None;
        }

        Ok(LoadArgs {
            bench_args: helper.bench_args,
            duration: helper.duration,
            concurrency: helper.concurrency,
            random_param_set_count: helper.random_param_set_count,
            http_clients: helper.http_clients,
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_framework::queries::QuerySet;

    #[test]
    fn test_single_section_deserialization() {
        let yaml = "
tests:
  bench:
    spicepod_path: s3[parquet]-turso[file].yaml
    query_set: tpch
    ready_wait: 300
    runner_type: spiceai-dev-runners
  load:
    spicepod_path: s3[parquet]-turso[file].yaml
    query_set: tpch
    ready_wait: 300
    runner_type: spiceai-dev-runners
    concurrency: 128
    duration: 1800
    random_param_set_count: 1000
";

        let test_file: DispatchTestFile =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        // Verify bench section (single item becomes vec with one element)
        assert_eq!(test_file.tests.bench.len(), 1);
        assert_eq!(
            test_file.tests.bench[0].spicepod_path.to_string_lossy(),
            "s3[parquet]-turso[file].yaml"
        );
        assert_eq!(test_file.tests.bench[0].query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.bench[0].ready_wait, Some(300));

        // Verify load section (single item becomes vec with one element)
        assert_eq!(test_file.tests.load.len(), 1);
        assert_eq!(
            test_file.tests.load[0]
                .bench_args
                .spicepod_path
                .to_string_lossy(),
            "s3[parquet]-turso[file].yaml"
        );
        assert_eq!(test_file.tests.load[0].bench_args.query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.load[0].bench_args.ready_wait, Some(300));
        assert_eq!(test_file.tests.load[0].concurrency, Some(128));
        assert_eq!(test_file.tests.load[0].duration, Some(1800));
        assert_eq!(test_file.tests.load[0].random_param_set_count, Some(1000));

        // Verify empty sections default to empty vectors
        assert_eq!(test_file.tests.throughput.len(), 0);
    }

    #[test]
    fn test_multiple_sections_deserialization() {
        let yaml = "
tests:
  load:
    - spicepod_path: s3[parquet]-turso[file].yaml
      query_set: tpch
      ready_wait: 300
      runner_type: spiceai-dev-runners
      concurrency: 128
      duration: 1800
      random_param_set_count: 1000
    - spicepod_path: s3[parquet]-turso[file].yaml
      query_set: tpch
      ready_wait: 600
      runner_type: spicehq-dev-large-runners
      concurrency: 256
      duration: 3600
      random_param_set_count: 2000
    - spicepod_path: different-spicepod.yaml
      query_set: tpch
      ready_wait: 120
      runner_type: spiceai-dev-runners
      concurrency: 64
      duration: 900
      random_param_set_count: 500
";

        let test_file: DispatchTestFile =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        // Verify we have 3 load sections
        assert_eq!(test_file.tests.load.len(), 3);

        // Verify first load section
        assert_eq!(
            test_file.tests.load[0]
                .bench_args
                .spicepod_path
                .to_string_lossy(),
            "s3[parquet]-turso[file].yaml"
        );
        assert_eq!(test_file.tests.load[0].bench_args.query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.load[0].bench_args.ready_wait, Some(300));
        assert_eq!(test_file.tests.load[0].concurrency, Some(128));
        assert_eq!(test_file.tests.load[0].duration, Some(1800));
        assert_eq!(test_file.tests.load[0].random_param_set_count, Some(1000));

        // Verify second load section
        assert_eq!(
            test_file.tests.load[1]
                .bench_args
                .spicepod_path
                .to_string_lossy(),
            "s3[parquet]-turso[file].yaml"
        );
        assert_eq!(test_file.tests.load[1].bench_args.query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.load[1].bench_args.ready_wait, Some(600));
        assert_eq!(test_file.tests.load[1].concurrency, Some(256));
        assert_eq!(test_file.tests.load[1].duration, Some(3600));
        assert_eq!(test_file.tests.load[1].random_param_set_count, Some(2000));

        // Verify third load section
        assert_eq!(
            test_file.tests.load[2]
                .bench_args
                .spicepod_path
                .to_string_lossy(),
            "different-spicepod.yaml"
        );
        assert_eq!(test_file.tests.load[2].bench_args.query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.load[2].bench_args.ready_wait, Some(120));
        assert_eq!(test_file.tests.load[2].concurrency, Some(64));
        assert_eq!(test_file.tests.load[2].duration, Some(900));
        assert_eq!(test_file.tests.load[2].random_param_set_count, Some(500));

        // Verify other sections are empty
        assert_eq!(test_file.tests.bench.len(), 0);
        assert_eq!(test_file.tests.throughput.len(), 0);
    }

    #[test]
    fn test_empty_sections_default_to_empty_vec() {
        let yaml = "
tests: {}
";

        let test_file: DispatchTestFile =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");

        // All sections should default to empty vectors
        assert_eq!(test_file.tests.bench.len(), 0);
        assert_eq!(test_file.tests.throughput.len(), 0);
        assert_eq!(test_file.tests.load.len(), 0);
    }
}
