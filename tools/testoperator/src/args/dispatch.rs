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
use test_framework::{
    TestType, anyhow,
    layout::Layout,
    source_versions::{Source, source_versions},
};

use super::dataset::{QueryOverridesArg, QuerySetArg};
use super::search::SearchDatasetArg;

#[derive(Parser, Debug, Clone)]
pub struct DispatchArgs {
    /// A positional argument for the directory to scan, or test file
    #[clap(index = 1)]
    pub(crate) path: PathBuf,

    /// The GitHub workflow to execute
    #[arg(long)]
    pub(crate) workflow: Workflow,

    #[arg(long, env = "GH_TOKEN", required_if_eq("dry_run", "false"))]
    pub(crate) github_token: Option<String>,

    #[arg(long, env = "SPICED_COMMIT", default_value = "")]
    pub(crate) spiced_commit: String,

    #[arg(long, env = "WORKFLOW_COMMIT", default_value = "trunk")]
    pub(crate) workflow_commit: String,

    #[arg(long, default_value = "false", action = ArgAction::Set)]
    pub(crate) update_snapshots: bool,

    /// Maximum number of concurrent workflow runs allowed
    #[arg(long)]
    pub(crate) max_concurrent: Option<usize>,

    /// When `--max-concurrent` is set, how many minutes to wait for a free slot
    /// before dispatching anyway. Raise this above a single run's wall-clock for
    /// long workloads (e.g. SF-1000 HTAP) so the slot wait actually serializes them
    /// instead of giving up early. Ignored unless `--max-concurrent` is set.
    #[arg(long, default_value = "30")]
    pub(crate) max_concurrent_wait_timeout_mins: u64,

    /// Dry run mode - print the workflow dispatch request without sending it
    #[arg(long, default_value = "false")]
    pub(crate) dry_run: bool,

    /// Dispatch only the test files on this schedule (a file without a `schedule` key is
    /// `daily`). Without it, every test file is dispatched.
    #[arg(long, value_enum)]
    pub(crate) schedule: Option<Schedule>,
}

/// Which scheduled run dispatches a test file.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Schedule {
    #[default]
    Daily,
    /// For tests whose source is a hosted service, and for the runs of a
    /// benchmark on every supported release line of its source database.
    Weekly,
}

impl std::fmt::Display for Schedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Schedule::Daily => write!(f, "daily"),
            Schedule::Weekly => write!(f, "weekly"),
        }
    }
}

#[derive(Debug, Copy, Clone, ValueEnum)]
pub enum Workflow {
    Bench,
    Throughput,
    Load,
    Append,
    DataConsistency,
    Search,
    TextToSql,
    StreamingBench,
    StreamingCorrectness,
    Schema,
    Htap,
}

impl From<Workflow> for TestType {
    fn from(workflow: Workflow) -> Self {
        match workflow {
            Workflow::Bench => TestType::Benchmark,
            Workflow::Throughput => TestType::Throughput,
            Workflow::Load => TestType::Load,
            Workflow::Append => TestType::Append,
            Workflow::DataConsistency => TestType::DataConsistency,
            Workflow::Search => TestType::Search,
            Workflow::TextToSql => TestType::TextToSql,
            Workflow::StreamingBench => TestType::Streaming,
            Workflow::StreamingCorrectness => TestType::StreamingCorrectness,
            Workflow::Schema => TestType::Schema,
            Workflow::Htap => TestType::Htap,
        }
    }
}

/// Represents a single test file payload
#[derive(Debug, Clone, Deserialize)]
pub struct DispatchTestFile {
    /// Which scheduled run dispatches the file's tests.
    #[serde(default)]
    pub schedule: Schedule,
    pub tests: DispatchTests,
}

impl DispatchTestFile {
    /// Whether a dispatch restricted to `schedule` includes this file. An unrestricted
    /// dispatch (`None`) includes every file.
    #[must_use]
    pub fn is_on_schedule(&self, schedule: Option<Schedule>) -> bool {
        schedule.is_none_or(|schedule| schedule == self.schedule)
    }
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
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub search: Vec<SearchArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub text_to_sql: Vec<TextToSqlArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub streaming_bench: Vec<StreamingBenchDispatchArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub streaming_correctness: Vec<StreamingCorrectnessDispatchArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub schema: Vec<SchemaArgs>,
    #[serde(deserialize_with = "deserialize_single_or_vec", default)]
    pub htap: Vec<HtapDispatchArgs>,
}

/// Benchmark and throughput workflow arguments, defined in the test files
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchArgs {
    pub spicepod_path: PathBuf,
    pub query_set: QuerySetArg,
    pub query_overrides: Option<QueryOverridesArg>,
    /// Path to a scenario query set file (required when `query_set` is `Scenario`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_query_file: Option<PathBuf>,
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
    /// The `PostgreSQL` release line the bench workflow starts as its local
    /// `postgres_tpch` source: one of the `postgres` versions in
    /// `test/source_versions.json`, or `all` for one run on each of them. Unset
    /// runs the workflow's default line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_version: Option<String>,
    /// The acceleration layout the run configures (`testoperator --layout`):
    /// features from `primary_key`, `indexes`, `sort`, `cluster`,
    /// `time_column` and `partition`, joined by commas.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout: Option<String>,
    /// Several layouts: one run each, dispatched as its `layout`.
    #[serde(default, skip_serializing)]
    pub layouts: Vec<String>,
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

/// The `postgres_version` that expands to every listed `PostgreSQL` line.
pub const ALL_SOURCE_VERSIONS: &str = "all";

/// A test entry's runs per layout: its `layout`, or one per entry of `layouts`.
/// Each layout is parsed here, so a misspelled feature fails the dispatch
/// rather than the dispatched run.
fn expand_layouts(
    layout: Option<&String>,
    layouts: &[String],
) -> anyhow::Result<Vec<Option<String>>> {
    anyhow::ensure!(
        layout.is_none() || layouts.is_empty(),
        "a test sets either `layout` or `layouts`, not both"
    );
    let layouts: Vec<Option<String>> = if layouts.is_empty() {
        vec![layout.cloned()]
    } else {
        layouts.iter().cloned().map(Some).collect()
    };
    for layout in layouts.iter().flatten() {
        layout
            .parse::<Layout>()
            .map_err(|e| anyhow::anyhow!("layout '{layout}': {e}"))?;
    }
    Ok(layouts)
}

impl BenchArgs {
    #[must_use]
    pub fn with_update_snapshots(mut self, update_snapshots: UpdateSnapshots) -> Self {
        self.update_snapshots = Some(update_snapshots);
        self
    }

    /// The runs this entry dispatches: one per listed `PostgreSQL` line for
    /// `postgres_version: all`, times one per entry of `layouts`.
    ///
    /// # Errors
    ///
    /// When `postgres_version` names a version `test/source_versions.json` does
    /// not list, so a typo or a retired line fails the dispatch instead of
    /// starting an unsupported server; when a layout does not parse; or when
    /// the entry sets both `layout` and `layouts`.
    pub fn expand_runs(&self) -> anyhow::Result<Vec<Self>> {
        let layouts = expand_layouts(self.layout.as_ref(), &self.layouts)?;
        let versions = self.postgres_versions()?;
        Ok(versions
            .iter()
            .flat_map(|version| {
                layouts.iter().map(move |layout| Self {
                    postgres_version: version.clone(),
                    layout: layout.clone(),
                    layouts: Vec::new(),
                    ..self.clone()
                })
            })
            .collect())
    }

    /// Refuse the settings only the bench workflow declares inputs for, on an
    /// entry dispatched to `workflow`: GitHub refuses a dispatch carrying an
    /// input its workflow does not declare.
    ///
    /// # Errors
    ///
    /// When the entry sets `postgres_version`, `layout` or `layouts`.
    pub fn ensure_only_bench_settings_unset(&self, workflow: &str) -> anyhow::Result<()> {
        let set: Vec<&str> = [
            ("postgres_version", self.postgres_version.is_some()),
            ("layout", self.layout.is_some()),
            ("layouts", !self.layouts.is_empty()),
        ]
        .into_iter()
        .filter_map(|(setting, is_set)| is_set.then_some(setting))
        .collect();
        anyhow::ensure!(
            set.is_empty(),
            "a {workflow} test sets `{}`, which only the bench workflow takes; remove {}, or run the entry as a bench test",
            set.join("`, `"),
            if set.len() == 1 { "it" } else { "them" }
        );
        Ok(())
    }

    fn postgres_versions(&self) -> anyhow::Result<Vec<Option<String>>> {
        let Some(requested) = self.postgres_version.as_deref() else {
            return Ok(vec![None]);
        };
        let listed = source_versions(Source::Postgres)?;
        let versions: Vec<&str> = listed
            .versions
            .iter()
            .map(|listed| listed.version.as_str())
            .collect();
        if requested == ALL_SOURCE_VERSIONS {
            return Ok(versions
                .into_iter()
                .map(|version| Some(version.to_string()))
                .collect());
        }
        anyhow::ensure!(
            versions.contains(&requested),
            "postgres_version {requested} is not a supported PostgreSQL version; test/source_versions.json lists {}, or use `{ALL_SOURCE_VERSIONS}`",
            versions.join(", ")
        );
        Ok(vec![Some(requested.to_string())])
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distributed: Option<bool>,
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
    pub concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_steps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_conflict_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_retention_data: Option<bool>,
    /// Verify the query results against their expected answers once the loads
    /// finish. Defaults to on in the workflow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_results: Option<bool>,
}

/// Search benchmark workflow arguments, defined in the test files. Should match inputs in
/// `.github/workflows/testoperator_run_search.yml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    /// Built-in MTEB benchmark dataset. Omitted for a custom run against `spicepod_path` as-is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub benchmark_dataset: Option<SearchDatasetArg>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_wait: Option<u64>,
}

/// Schema test workflow arguments, defined in the test files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    /// Minimum number of tables expected in the catalog
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_tables: Option<usize>,
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
            distributed: Option<bool>,
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
            distributed: helper.distributed,
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
    #[serde(rename = "spiceai-dev-xlarge-runners")]
    DevXLarge,
}

/// Payload sent to the GitHub Actions workflow request. Should match inputs in `.github/workflows/testoperator_run_texttosql.yml`.
/// `spiced_commit` is not an eligible argument in the test files, as it is controlled by the environment.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TextToSqlArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    pub model_name: String,

    #[serde(flatten)]
    pub queryset_source: QuerysetSource,

    /// Limit the number of text-to-SQL operations to run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Unique name for the configured testoperator run. Used to identify/group runs in telemetry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration_name: Option<String>,

    /// Include evidence in the question for bird-bench querysets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_evidence: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum QuerysetSource {
    Benchmark {
        benchmark_queryset: BenchmarkQueryset,
    },
    File {
        queryset_file: PathBuf,
    },
    Payload {
        queryset: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum BenchmarkQueryset {
    #[serde(rename = "bird-bench-small[california_schools]")]
    BirdBenchSmallCaliforniaSchools,
    #[serde(rename = "bird-bench-small[card_games]")]
    BirdBenchSmallCardGames,
    #[serde(rename = "bird-bench-small[codebase_community]")]
    BirdBenchSmallCodebaseCommunity,
    #[serde(rename = "bird-bench-small[debit_card_specializing]")]
    BirdBenchSmallDebitCardSpecializing,
    #[serde(rename = "bird-bench-small[european_football_2]")]
    BirdBenchSmallEuropeanFootball2,
    #[serde(rename = "bird-bench-small[financial]")]
    BirdBenchSmallFinancial,
    #[serde(rename = "bird-bench-small[formula_1]")]
    BirdBenchSmallFormula1,
    #[serde(rename = "bird-bench-small[superhero]")]
    BirdBenchSmallSuperhero,
    #[serde(rename = "bird-bench-small[thrombosis_prediction]")]
    BirdBenchSmallThrombosisPrediction,
    #[serde(rename = "bird-bench-small[toxicology]")]
    BirdBenchSmallToxicology,
}

/// Streaming `DynamoDB` benchmark workflow arguments.
///
/// Mirrors the inputs of `testoperator_run_streaming_dynamodb.yml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamingBenchDispatchArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    #[serde(default = "default_queryset")]
    pub queryset: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_scale_factor"
    )]
    pub scale_factor: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_wait: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_liveness: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_query_liveness: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_liveness_interval_ms: Option<u64>,
}

/// Streaming `DynamoDB` correctness workflow arguments.
///
/// Mirrors the inputs of `testoperator_run_streaming_dynamodb_correctness.yml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamingCorrectnessDispatchArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    #[serde(default = "default_queryset")]
    pub queryset: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_scale_factor"
    )]
    pub scale_factor: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_wait: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rounds: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_seed: Option<u64>,
}

/// HTAP workflow arguments.
///
/// A subset of the inputs of `testoperator_run_htap.yml` — only the ones
/// scheduled/test-file dispatch needs to set (`spiced_commit` is handled by
/// the `WorkflowArgs` wrapper). Deliberately also omits `skip_analytic_gate`:
/// scheduled dispatch (`testoperator_dispatch_htap.yml`) builds its payload
/// from this struct, so leaving the field out means the dispatched workflow
/// run always falls back to the workflow's `false` default and the
/// analytical-correctness gate always runs on schedule.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HtapDispatchArgs {
    pub spicepod_path: PathBuf,
    pub runner_type: RunnerType,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_scale_factor"
    )]
    pub scale_factor: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_wait: Option<u64>,
    /// Override the number of OLTP terminals (default: `scale_factor` * 10).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminals: Option<usize>,
    /// Override default number of concurrent analytical query clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    /// Engine-specific query overrides
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_overrides: Option<QueryOverridesArg>,
    /// Optional target OLTP transaction rate for the OLTP workload (txn/s).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<u32>,
    /// The source database's release line: one of the versions
    /// `test/source_versions.json` lists for the spicepod's source (`MySQL` for
    /// a `mysql*` spicepod, otherwise `PostgreSQL`), or `all` for one run on each
    /// of them. Unset runs the workflow's default line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    /// The acceleration layout the run configures (`testoperator --layout`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout: Option<String>,
    /// Several layouts: one run each, dispatched as its `layout`.
    #[serde(default, skip_serializing)]
    pub layouts: Vec<String>,
}

impl HtapDispatchArgs {
    /// The source database the HTAP workflow starts for this spicepod: it reads
    /// the source from the file name, `mysql*` for `MySQL`, so this does too.
    fn source(&self) -> Source {
        let file_name = self
            .spicepod_path
            .file_name()
            .map(|name| name.to_string_lossy())
            .unwrap_or_default();
        if file_name.starts_with("mysql") {
            Source::MySql
        } else {
            Source::Postgres
        }
    }

    /// The runs this entry dispatches: one per listed release line of its
    /// source for `source_version: all`, times one per entry of `layouts`.
    ///
    /// # Errors
    ///
    /// When `source_version` names a version `test/source_versions.json` does
    /// not list for the spicepod's source, when a layout does not parse, or when
    /// the entry sets both `layout` and `layouts`.
    pub fn expand_runs(&self) -> anyhow::Result<Vec<Self>> {
        let layouts = expand_layouts(self.layout.as_ref(), &self.layouts)?;
        let versions = self.source_lines()?;
        Ok(versions
            .iter()
            .flat_map(|version| {
                layouts.iter().map(move |layout| Self {
                    source_version: version.clone(),
                    layout: layout.clone(),
                    layouts: Vec::new(),
                    ..self.clone()
                })
            })
            .collect())
    }

    fn source_lines(&self) -> anyhow::Result<Vec<Option<String>>> {
        let Some(requested) = self.source_version.as_deref() else {
            return Ok(vec![None]);
        };
        let source = self.source();
        let listed = source_versions(source)?;
        let versions: Vec<&str> = listed
            .versions
            .iter()
            .map(|listed| listed.version.as_str())
            .collect();
        if requested == ALL_SOURCE_VERSIONS {
            return Ok(versions
                .into_iter()
                .map(|version| Some(version.to_string()))
                .collect());
        }
        anyhow::ensure!(
            versions.contains(&requested),
            "source_version {requested} is not a supported {} version; test/source_versions.json lists {}, or use `{ALL_SOURCE_VERSIONS}`",
            source.key(),
            versions.join(", ")
        );
        Ok(vec![Some(requested.to_string())])
    }
}

fn default_queryset() -> String {
    "tpch".to_string()
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

    /// Only the bench workflow declares `postgres_version` and `layout` inputs,
    /// so a load or throughput entry that sets them is refused before dispatch
    /// rather than sending an input GitHub would reject.
    #[test]
    fn load_and_throughput_entries_refuse_the_bench_only_settings() {
        let yaml = "
tests:
  load:
    spicepod_path: accelerated/file[parquet]-arrow.yaml
    query_set: tpch
    runner_type: spiceai-dev-large-runners
    layout: primary_key
  throughput:
    - spicepod_path: accelerated/file[parquet]-arrow.yaml
      query_set: tpch
      runner_type: spiceai-dev-large-runners
      postgres_version: '17'
      layouts: [primary_key, indexes]
    - spicepod_path: accelerated/file[parquet]-arrow.yaml
      query_set: tpch
      runner_type: spiceai-dev-large-runners
";
        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");
        assert_eq!(
            test_file.tests.load[0]
                .bench_args
                .ensure_only_bench_settings_unset("load")
                .expect_err("a load test cannot take a layout")
                .to_string(),
            "a load test sets `layout`, which only the bench workflow takes; remove it, or run the entry as a bench test"
        );
        assert_eq!(
            test_file.tests.throughput[0]
                .ensure_only_bench_settings_unset("throughput")
                .expect_err("a throughput test cannot take a version or layouts")
                .to_string(),
            "a throughput test sets `postgres_version`, `layouts`, which only the bench workflow takes; remove them, or run the entry as a bench test"
        );
        test_file.tests.throughput[1]
            .ensure_only_bench_settings_unset("throughput")
            .expect("an entry without them dispatches");
    }

    /// An HTAP entry's versions are its spicepod's source's: `MySQL` for a
    /// `mysql*` spicepod, as the HTAP workflow reads it, `PostgreSQL` otherwise.
    #[test]
    fn htap_source_version_expands_to_the_spicepod_source_lines() {
        let yaml = "
tests:
  htap:
    - spicepod_path: accelerated/mysql-cayenne[file].yaml
      runner_type: spiceai-dev-large-runners
      source_version: all
    - spicepod_path: accelerated/postgres-cayenne[file].yaml
      runner_type: spiceai-dev-large-runners
      source_version: '8.4'
";
        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");
        let listed = |source| -> Vec<String> {
            source_versions(source)
                .expect("the version list parses")
                .versions
                .iter()
                .map(|listed| listed.version.clone())
                .collect()
        };

        let runs = test_file.tests.htap[0]
            .expand_runs()
            .expect("`all` expands");
        assert_eq!(
            runs.iter()
                .map(|run| run.source_version.clone().expect("each run names its line"))
                .collect::<Vec<_>>(),
            listed(Source::MySql)
        );
        assert_eq!(
            test_file.tests.htap[1]
                .expand_runs()
                .expect_err("8.4 is a MySQL line, not a PostgreSQL one")
                .to_string(),
            format!(
                "source_version 8.4 is not a supported postgres version; test/source_versions.json lists {}, or use `all`",
                listed(Source::Postgres).join(", ")
            )
        );
    }

    /// `postgres_version: all` is what keeps the weekly source-version runs in
    /// step with `test/source_versions.json`, and a refused version is what keeps
    /// a typo or a retired line from starting an unsupported server.
    #[test]
    fn bench_postgres_version_expands_to_the_listed_lines() {
        let yaml = "
tests:
  bench:
    - spicepod_path: federated/postgres[catalog].yaml
      query_set: tpch
      runner_type: spiceai-dev-runners
      postgres_version: all
    - spicepod_path: federated/postgres[catalog].yaml
      query_set: tpch
      runner_type: spiceai-dev-runners
      postgres_version: '13'
    - spicepod_path: federated/postgres[catalog].yaml
      query_set: tpch
      runner_type: spiceai-dev-runners
";
        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");
        let listed: Vec<String> = source_versions(Source::Postgres)
            .expect("the version list parses")
            .versions
            .iter()
            .map(|listed| listed.version.clone())
            .collect();

        let runs = test_file.tests.bench[0]
            .expand_runs()
            .expect("`all` expands");
        assert_eq!(
            runs.iter()
                .map(|run| run
                    .postgres_version
                    .clone()
                    .expect("each run names its line"))
                .collect::<Vec<_>>(),
            listed
        );
        let inputs = serde_json::to_value(&runs[0]).expect("Failed to serialize");
        assert_eq!(inputs["postgres_version"], listed[0]);

        assert_eq!(
            test_file.tests.bench[1]
                .expand_runs()
                .expect_err("13 is not a supported line")
                .to_string(),
            format!(
                "postgres_version 13 is not a supported PostgreSQL version; test/source_versions.json lists {}, or use `all`",
                listed.join(", ")
            )
        );

        let unset = test_file.tests.bench[2]
            .expand_runs()
            .expect("an unset version is one run");
        assert_eq!(unset.len(), 1);
        let inputs = serde_json::to_value(&unset[0]).expect("Failed to serialize");
        assert!(
            inputs.get("postgres_version").is_none(),
            "an unset version must not become a workflow input: {inputs}"
        );
    }

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
    append:
        spicepod_path: file[parquet]-cayenne[file]-append.yaml
        query_set: tpch
        duration: 720
        concurrency: 4
        load_interval: 30
        load_steps: 20
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

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
        assert_eq!(test_file.tests.load[0].bench_args.ready_wait, None); // stripped by load deserializer
        assert_eq!(test_file.tests.load[0].concurrency, Some(128));
        assert_eq!(test_file.tests.load[0].duration, Some(1800));
        assert_eq!(test_file.tests.load[0].random_param_set_count, Some(1000));

        // Verify append section (single item becomes vec with one element)
        assert_eq!(test_file.tests.append.len(), 1);
        assert_eq!(
            test_file.tests.append[0].spicepod_path.to_string_lossy(),
            "file[parquet]-cayenne[file]-append.yaml"
        );
        assert_eq!(test_file.tests.append[0].query_set, QuerySet::Tpch);
        assert_eq!(test_file.tests.append[0].duration, Some(720));
        assert_eq!(test_file.tests.append[0].concurrency, Some(4));
        assert_eq!(test_file.tests.append[0].load_interval, Some(30));
        assert_eq!(test_file.tests.append[0].load_steps, Some(20));

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
      runner_type: spiceai-dev-large-runners
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

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

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
        assert_eq!(test_file.tests.load[0].bench_args.ready_wait, None); // stripped by load deserializer
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
        assert_eq!(test_file.tests.load[1].bench_args.ready_wait, None); // stripped by load deserializer
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
        assert_eq!(test_file.tests.load[2].bench_args.ready_wait, None); // stripped by load deserializer
        assert_eq!(test_file.tests.load[2].concurrency, Some(64));
        assert_eq!(test_file.tests.load[2].duration, Some(900));
        assert_eq!(test_file.tests.load[2].random_param_set_count, Some(500));

        // Verify other sections are empty
        assert_eq!(test_file.tests.bench.len(), 0);
        assert_eq!(test_file.tests.throughput.len(), 0);
    }

    #[test]
    fn test_schedule_defaults_to_daily() {
        let yaml = "
tests:
  bench:
    spicepod_path: federated/file[parquet].yaml
    query_set: tpch
    runner_type: spiceai-dev-runners
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(test_file.schedule, Schedule::Daily);
        assert!(test_file.is_on_schedule(None));
        assert!(test_file.is_on_schedule(Some(Schedule::Daily)));
        assert!(!test_file.is_on_schedule(Some(Schedule::Weekly)));
    }

    #[test]
    fn test_weekly_schedule_is_skipped_by_the_daily_dispatch() {
        let yaml = "
schedule: weekly
tests:
  bench:
    spicepod_path: federated/oracle.yaml
    query_set: tpch
    runner_type: spiceai-dev-runners
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(test_file.schedule, Schedule::Weekly);
        assert!(test_file.is_on_schedule(None));
        assert!(!test_file.is_on_schedule(Some(Schedule::Daily)));
        assert!(test_file.is_on_schedule(Some(Schedule::Weekly)));
    }

    #[test]
    fn test_unknown_schedule_is_rejected() {
        let yaml = "
schedule: weeky
tests: {}
";

        yaml::from_str::<DispatchTestFile>(yaml).expect_err("an unknown schedule must not parse");
    }

    #[test]
    fn test_empty_sections_default_to_empty_vec() {
        let yaml = "
tests: {}
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        // All sections should default to empty vectors
        assert_eq!(test_file.tests.bench.len(), 0);
        assert_eq!(test_file.tests.throughput.len(), 0);
        assert_eq!(test_file.tests.load.len(), 0);
        assert_eq!(test_file.tests.htap.len(), 0);
    }

    #[test]
    fn test_htap_section_deserialization() {
        let yaml = "
tests:
  htap:
    spicepod_path: accelerated/postgres-cayenne[file].yaml
    runner_type: spiceai-dev-runners
    scale_factor: 1
    terminals: 100
    duration: 300
    ready_wait: 60
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(test_file.tests.htap.len(), 1);
        assert_eq!(
            test_file.tests.htap[0].spicepod_path.to_string_lossy(),
            "accelerated/postgres-cayenne[file].yaml"
        );
        assert!(matches!(
            test_file.tests.htap[0].runner_type,
            RunnerType::Dev
        ));
        assert_eq!(test_file.tests.htap[0].scale_factor, Some(1.0));
        assert_eq!(test_file.tests.htap[0].terminals, Some(100));
        assert_eq!(test_file.tests.htap[0].duration, Some(300));
        assert_eq!(test_file.tests.htap[0].ready_wait, Some(60));

        // Verify scale_factor serializes as integer 1 (not 1.0) for GitHub workflow inputs
        let serialized =
            serde_json::to_value(&test_file.tests.htap[0]).expect("Failed to serialize");
        assert_eq!(serialized["scale_factor"], 1);
        assert_eq!(serialized["terminals"], 100);
    }

    #[test]
    fn test_htap_section_deserialization_without_terminals() {
        let yaml = "
tests:
  htap:
    spicepod_path: accelerated/postgres-arrow.yaml
    runner_type: spiceai-dev-runners
    scale_factor: 10
    duration: 600
    ready_wait: 120
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(test_file.tests.htap[0].terminals, None);

        // Verify terminals is omitted from serialized output when None
        let serialized =
            serde_json::to_value(&test_file.tests.htap[0]).expect("Failed to serialize");
        assert!(
            serialized.get("terminals").is_none(),
            "terminals should be omitted when None"
        );
    }

    #[test]
    fn test_xlarge_runner_type_round_trip() {
        let yaml = "
tests:
  htap:
    spicepod_path: accelerated/postgres-cayenne[file].yaml
    runner_type: spiceai-dev-xlarge-runners
    scale_factor: 10
    duration: 600
    ready_wait: 300
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.htap[0].runner_type,
            RunnerType::DevXLarge
        ));

        // Verify it serializes back to the expected workflow input value
        let serialized =
            serde_json::to_value(&test_file.tests.htap[0]).expect("Failed to serialize");
        assert_eq!(serialized["runner_type"], "spiceai-dev-xlarge-runners");
    }

    #[test]
    fn test_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/quora/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: quora_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert_eq!(test_file.tests.search.len(), 1);
        assert_eq!(
            test_file.tests.search[0].spicepod_path.to_string_lossy(),
            "test/spicepods/search/mteb/quora/full_text_search-duckdb[file].yaml"
        );
        assert!(matches!(
            test_file.tests.search[0].runner_type,
            RunnerType::Dev
        ));
        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::QuoraRetrieval)
        ));
        assert_eq!(test_file.tests.search[0].ready_wait, Some(1800));

        // Verify benchmark_dataset serializes back to the exact string the workflow expects
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "quora_retrieval");
    }

    #[test]
    fn test_miracl_en_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/miracl_en/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: miracl_en_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::MiraclEnRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "miracl_en_retrieval");
    }

    #[test]
    fn test_fiqa_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/fiqa/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: fiqa_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::FiqaRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "fiqa_retrieval");
    }

    #[test]
    fn test_trec_covid_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/trec_covid/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: trec_covid_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::TrecCovidRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "trec_covid_retrieval");
    }

    #[test]
    fn test_arguana_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/arguana/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: arguana_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::ArguanaRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "arguana_retrieval");
    }

    #[test]
    fn test_scidocs_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/scidocs/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: scidocs_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::ScidocsRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "scidocs_retrieval");
    }

    #[test]
    fn test_scifact_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/scifact/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: scifact_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::ScifactRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "scifact_retrieval");
    }

    #[test]
    fn test_nfcorpus_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/nfcorpus/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: nfcorpus_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::NfcorpusRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "nfcorpus_retrieval");
    }

    #[test]
    fn test_touche2020_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/touche2020/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: touche2020_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::Touche2020Retrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "touche2020_retrieval");
    }

    #[test]
    fn test_msmarco_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/msmarco/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: msmarco_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::MsmarcoRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(serialized["benchmark_dataset"], "msmarco_retrieval");
    }

    #[test]
    fn test_stackoverflow_qa_search_section_deserialization() {
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/mteb/stackoverflow_qa/full_text_search-duckdb[file].yaml
    runner_type: spiceai-dev-runners
    benchmark_dataset: stackoverflow_qa_retrieval
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(matches!(
            test_file.tests.search[0].benchmark_dataset,
            Some(SearchDatasetArg::StackoverflowQaRetrieval)
        ));
        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert_eq!(
            serialized["benchmark_dataset"],
            "stackoverflow_qa_retrieval"
        );
    }

    #[test]
    fn test_custom_search_section_omits_benchmark_dataset() {
        // A custom run leaves `benchmark_dataset` unset. It must deserialize to `None` and, so the
        // dispatch workflow input stays absent (letting the workflow default to a custom run), it
        // must not serialize the field back out.
        let yaml = "
tests:
  search:
    spicepod_path: test/spicepods/search/custom/my-spicepod.yaml
    runner_type: spiceai-dev-runners
    ready_wait: 1800
";

        let test_file: DispatchTestFile = yaml::from_str(yaml).expect("Failed to deserialize");

        assert!(test_file.tests.search[0].benchmark_dataset.is_none());

        let serialized =
            serde_json::to_value(&test_file.tests.search[0]).expect("Failed to serialize");
        assert!(serialized.get("benchmark_dataset").is_none());
    }
}
