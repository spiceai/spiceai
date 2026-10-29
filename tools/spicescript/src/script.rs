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

//! The script format: a header naming sources, matrix and variants, a spicepod
//! template, and a list of steps.
//!
//! Steps are YAML mappings whose one *action key* names the step (`query:`,
//! `start:`, `stop-source:` …) and whose sibling keys are its options. They are
//! parsed by hand from a [`Value`] rather than with an externally tagged serde
//! enum, because an externally tagged enum cannot carry sibling keys.

use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::{Context, Result, anyhow, bail, ensure};
use indexmap::IndexMap;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Script {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub sources: IndexMap<String, SourceSpec>,
    /// Every combination of these values is a separate run, exposed to the
    /// templates as `${matrix.<key>}`.
    #[serde(default)]
    pub matrix: IndexMap<String, Vec<Value>>,
    /// Named spicepod patches. Every variant of a run must return identical
    /// rows for every query.
    #[serde(default)]
    pub variants: IndexMap<String, VariantSpec>,
    /// The spicepod, as YAML, with `${…}` placeholders.
    pub spicepod: Value,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(skip)]
    pub steps: Vec<Step>,
    /// How long the script takes to run, every matrix combination included,
    /// as recorded in `recordings/durations.json` beside it (see
    /// [`Script::load`]). `--jobs` starts the longest scripts first.
    #[serde(skip)]
    pub duration: Option<Duration>,
    /// The script file's name without `.yaml`: its key in the durations file.
    #[serde(skip)]
    pub file_stem: String,
    /// The directory the script is in; `corpus` and `slt` files are relative to it.
    #[serde(skip)]
    pub dir: std::path::PathBuf,
    /// The runs and variants `--matrix` and `--variant` left out (see
    /// [`Script::select`]).
    #[serde(skip)]
    pub left_out: LeftOut,
}

/// `--matrix` and `--variant`: the matrix values and variants a run keeps.
#[derive(Debug, Default)]
pub struct Selection {
    /// The values to keep, by matrix key, as `${matrix.<key>}` prints them.
    pub matrix: BTreeMap<String, Vec<String>>,
    pub variants: Vec<String>,
}

impl Selection {
    pub fn is_empty(&self) -> bool {
        self.matrix.is_empty() && self.variants.is_empty()
    }

    /// The selection as the command line spells it.
    pub fn describe(&self) -> String {
        self.matrix
            .iter()
            .flat_map(|(k, values)| values.iter().map(move |v| format!("--matrix {k}={v}")))
            .chain(self.variants.iter().map(|v| format!("--variant {v}")))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// What a [`Selection`] left out of a script: its ledger entries there are
/// not expected to fail this run, and recording keeps them.
#[derive(Debug, Default, Clone)]
pub struct LeftOut {
    /// Run labels (`name[key=value,…]`) of the combinations left out.
    pub runs: std::collections::BTreeSet<String>,
    pub variants: std::collections::BTreeSet<String>,
}

impl LeftOut {
    /// Whether a check of `run` by `who` (an instance, `<binary>/<variant>`
    /// with several binaries, `variants` for a comparison between them, or
    /// empty) could have been left out.
    pub fn covers(&self, run: &str, who: &str) -> bool {
        let variant = who.rsplit_once('/').map_or(who, |(_, v)| v);
        self.runs.contains(run)
            || self.variants.contains(variant)
            // A comparison between the variants that ran is not the one recorded.
            || (who == "variants" && !self.variants.is_empty())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SourceSpec {
    /// `postgres` or `mysql`; may be a `${matrix.…}` placeholder.
    pub kind: String,
    /// Image tag; defaults to the newest version in `test/source_versions.json`.
    #[serde(default)]
    pub version: Option<String>,
    /// For `kind: docker`: the container to run.
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub ports: Vec<u16>,
    /// Environment; `${port.N}` in a value (or in `command`) is the host
    /// port container port N is published on.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// A shell command inside the container that succeeds once it is ready.
    #[serde(default)]
    pub ready: Option<String>,
    /// `docker` sources: on Linux, share the host's network, so the
    /// container reaches what listens on the host's loopback (`spiced`, the
    /// other sources). Elsewhere Docker reaches the host through
    /// `host.docker.internal`, and on Linux that name is mapped to the host's
    /// loopback, so `${sources.<name>.host_from_container}` (that name) works
    /// either way.
    #[serde(default)]
    pub host_network: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct VariantSpec {
    /// A JSON merge patch (RFC 7386) applied to the whole spicepod.
    #[serde(default)]
    pub spicepod: Option<Value>,
    /// Merge patches applied to individual datasets, keyed by dataset name;
    /// `*` applies to every dataset. Exists because a merge patch replaces a
    /// list wholesale, and `datasets` is a list.
    #[serde(default)]
    pub datasets: IndexMap<String, Value>,
    /// The same, for `catalogs`.
    #[serde(default)]
    pub catalogs: IndexMap<String, Value>,
    /// Extra environment for this variant's `spiced`.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Run this variant only in matrix combinations with these values.
    #[serde(default)]
    pub when: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
#[expect(clippy::struct_excessive_bools, reason = "independent script switches")]
pub struct Defaults {
    #[serde(default)]
    pub expect: Option<Expect>,
    /// Default retry window for `query` steps that do not set a timing.
    #[serde(default, with = "opt_duration")]
    pub eventually: Option<Duration>,
    /// Default readiness timeout for `start`.
    #[serde(default, with = "opt_duration")]
    pub ready_timeout: Option<Duration>,
    /// How long one query may run before it counts as failed (default 120s),
    /// so a query that never returns fails its check instead of the run.
    #[serde(default, with = "opt_duration")]
    pub query_timeout: Option<Duration>,
    /// Whether every variant must give the same answers (default true). Off
    /// for scripts whose variants are expected to differ, each checked against
    /// its own expectation.
    #[serde(default = "yes")]
    pub compare_variants: bool,
    /// Also ask every query over HTTP (`/v1/sql`, JSON) and require the same
    /// answer as over Flight.
    #[serde(default)]
    pub also_http: bool,
    /// Also run every query through the asynchronous `/v1/queries` API (a
    /// distributed job on a cluster) and require the same answer as over
    /// Flight.
    #[serde(default)]
    pub also_async: bool,
    /// The variant every other variant's answers are compared with.
    #[serde(default)]
    pub compare_to: Option<String>,
    /// The API key the runtime requires: the runner presents it on its own
    /// requests, and steps present it unless they say otherwise.
    #[serde(default)]
    pub api_key: Option<String>,
    /// The runtime serves TLS: the runner connects with HTTPS and TLS Flight,
    /// trusting this root certificate (relative to the script).
    #[serde(default)]
    pub tls: Option<TlsDefaults>,
    /// Arguments passed to `spiced` after its endpoint flags; `${…}`
    /// placeholders resolve as in the spicepod (`${instance.port}` is a free
    /// port of the instance's own).
    #[serde(default)]
    pub spiced_args: Vec<String>,
    /// Starts a second `spiced` from the same spicepod alongside each
    /// instance's, with these arguments after its endpoint flags — a cluster
    /// executor beside a scheduler. `${instance.executor_port}` is a free port
    /// for it.
    #[serde(default)]
    pub executor_args: Option<Vec<String>>,
    /// Always run as a cluster: a scheduler with this many executors, over
    /// node mTLS with a CA and certificates generated for the run.
    #[serde(default)]
    pub cluster: Option<ClusterDefaults>,
    /// When the script runs as a cluster, the column each acceleration without
    /// `partition_by` is bucketed on, by dataset name (`*` for any other).
    #[serde(default)]
    pub cluster_partition_by: BTreeMap<String, String>,
    /// Run with no other script alongside under `--jobs`: for scripts whose
    /// timing or load would suffer from, or cause trouble for, neighbours.
    #[serde(default)]
    pub exclusive: bool,
    /// How the variants run: `side-by-side` runs them a step at a time, as
    /// one job, for scripts whose variants work together, such as a snapshot
    /// writer and its reader.
    #[serde(default)]
    pub run_variants: RunVariants,
}

/// How a script's variants run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunVariants {
    /// One after another.
    #[default]
    OneAfterAnother,
    /// Side by side, a step at a time.
    SideBySide,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ClusterDefaults {
    #[serde(default = "one")]
    pub executors: usize,
    /// Executors made ready but not started, for `add-executor` steps.
    #[serde(default)]
    pub spare: usize,
    /// Runs each executor in a container of its own (Linux only), writing
    /// shuffle output to its own `/tmp`, so executors read each other's
    /// shuffle output over the network, as they do on separate hosts.
    #[serde(default)]
    pub containers: bool,
}

fn one() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsDefaults {
    pub ca: std::path::PathBuf,
}

fn yes() -> bool {
    true
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            expect: None,
            eventually: None,
            ready_timeout: None,
            query_timeout: None,
            also_http: false,
            also_async: false,
            compare_to: None,
            api_key: None,
            tls: None,
            spiced_args: Vec::new(),
            executor_args: None,
            cluster: None,
            cluster_partition_by: BTreeMap::new(),
            exclusive: false,
            run_variants: RunVariants::OneAfterAnother,
            compare_variants: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Expect {
    /// Compare with the same SQL run directly on this source.
    #[serde(default)]
    pub oracle: Option<String>,
    /// A literal answer: each row as a list of cells, compared as rendered
    /// strings. `null` is SQL NULL.
    #[serde(default)]
    pub rows: Option<Vec<Vec<Value>>>,
    /// The same instance's answer to the earlier query with this `id` — for
    /// "the old data is still served" while the oracle itself is down.
    #[serde(default)]
    pub same_as: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Step {
    /// 1-based position in the script, for reports.
    pub index: usize,
    /// Identifies the check across runs, and across edits that move it: the
    /// action and its argument, with `#n` on the nth repeat. The
    /// expected-failure ledger keys entries by it.
    pub key: String,
    pub action: Action,
    /// Run the step only on these variants (all when empty).
    pub only: Vec<String>,
    /// Skip the step on these variants.
    pub except: Vec<String>,
    /// Run the step only in matrix combinations with these values.
    pub when: Map<String, Value>,
    /// For a query from a `corpus`, the position of that corpus in the
    /// script's steps: its queries share one `eventually` limit, counted from
    /// when the first of them starts, rather than taking one each.
    pub corpus: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum Action {
    /// Start `spiced` and wait for `/v1/ready`.
    Start {
        ready_timeout: Option<Duration>,
        /// `false` waits only for `spiced` to answer HTTP, for a start whose
        /// datasets are expected not to become ready.
        wait_ready: bool,
        /// A change to the spicepod, kept for every later start.
        patch: Option<VariantSpec>,
    },
    /// Stop `spiced` with SIGTERM, escalating to SIGKILL.
    Stop,
    /// Kill `spiced` without letting it shut down.
    Kill {
        signal: KillSignal,
    },
    /// Stop (or kill), then start again.
    Restart {
        kill: Option<KillSignal>,
        ready_timeout: Option<Duration>,
        wait_ready: bool,
        patch: Option<VariantSpec>,
        /// `upgrade`: start again on the binary under test (from the one
        /// `--from` started the instance on).
        upgrade: bool,
    },
    WaitReady {
        timeout: Duration,
    },
    /// Change the running instance's spicepod file in place, without a
    /// restart (for a runtime started with `--pods-watcher-enabled`).
    EditSpicepod(VariantSpec),
    /// Write rows to a dataset with a Flight `DoPut`.
    FlightPut {
        dataset: String,
        rows: Vec<Vec<Value>>,
        /// Rows a source query returns instead: (source, SQL).
        from_source: Option<(String, String)>,
        /// Send the same rows this many times; with `expect-error`, one of
        /// them must be refused.
        times: usize,
        key: crate::spiced::Key,
        expect_error: bool,
        error_contains: Option<String>,
    },
    /// Run a statement with Flight SQL `ExecuteUpdate`; `count` is the number
    /// of affected rows it must report.
    FlightExec {
        sql: String,
        count: Option<i64>,
        key: crate::spiced::Key,
        expect_error: bool,
        error_contains: Option<String>,
    },
    /// Kill one cluster executor (by its position, from 0).
    KillExecutor(usize),
    /// Start every cluster executor that is not running, and wait for the
    /// scheduler to see them.
    StartExecutors,
    /// Start the next spare executor (`defaults.cluster.spare`): the cluster
    /// grows by one, and the step waits for the scheduler to register it.
    AddExecutor,
    StartSource(String),
    StopSource(String),
    PauseSource(String),
    UnpauseSource(String),
    /// Run statements on a source, in this instance's database.
    SourceExec {
        source: String,
        sql: Vec<String>,
    },
    Write {
        source: String,
        sql: Vec<String>,
        mirror: String,
        key: String,
    },
    Refresh {
        dataset: String,
        expect_error: bool,
        /// `false` triggers the refresh and moves on without its outcome.
        wait: bool,
        /// Overrides the dataset's `refresh_sql` for this refresh.
        refresh_sql: Option<String>,
    },
    Query(QueryStep),
    /// Run a statement (DML) through `spiced`.
    SpiceExec {
        sql: String,
        expect_error: bool,
        /// Text the error must contain.
        error_contains: Option<String>,
    },
    /// `EXPLAIN` (or `EXPLAIN ANALYZE`) a query and check the plan's text.
    Explain(ExplainStep),
    /// Check a dataset's status from `/v1/datasets?status=true`, and
    /// optionally the runtime's `/v1/ready`.
    AssertStatus(StatusStep),
    AssertLog(LogStep),
    Http(HttpStep),
    Background(BackgroundStep),
    /// Ternary logic partitioning over random predicates.
    Tlp(TlpStep),
    RandomQueries(RandomQueriesStep),
    /// Loads TPC-H at a scale into each named source.
    Dbgen {
        scale: f64,
        sources: Vec<String>,
    },
    /// Asks a source directly and compares its text answer with literal rows.
    SourceQuery {
        source: String,
        sql: String,
        rows: Vec<Vec<Value>>,
        timing: Timing,
    },
    /// Checks the files under the instance's directory, by path relative to it.
    AssertFiles {
        contains: Vec<regex::Regex>,
        lacks: Vec<regex::Regex>,
    },
    StopBackground {
        id: String,
        /// Failures matching one of these do not fail the step.
        allow: Vec<regex::Regex>,
    },
    /// Runs the `spice` CLI against the instance's runtime.
    SpiceCli {
        args: Vec<String>,
        output_contains: Option<String>,
        expect_error: bool,
    },
    /// Run a sqllogictest file: its statements on the source (`setup`), or
    /// its queries through Spice, checked against the recorded answers.
    Slt(SltStep),
    /// Create a table on a source and fill it with seeded generated rows.
    Generate {
        source: String,
        /// The table spec as written; placeholders resolve per instance.
        spec: Value,
        /// Also write the same rows to this source, to answer for them.
        mirror: Option<String>,
    },
    /// Require at least `min` objects under a prefix of a files or object
    /// store source.
    AssertObjects {
        source: String,
        prefix: String,
        min: usize,
        within: Option<Duration>,
    },
    /// Wait until a `/metrics` series meets a condition.
    WaitMetric(MetricWait),
    /// Keep a `/metrics` scrape to measure later changes from.
    MarkMetrics(String),
    Sleep(Duration),
}

#[derive(Debug, Clone)]
pub struct ExplainStep {
    pub sql: String,
    pub analyze: bool,
    /// Substrings the plan must contain, such as `lookup_index=o_custkey`.
    pub contains: Vec<String>,
    /// Substrings the plan must not contain.
    pub omits: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MetricWait {
    /// Series name, as exported.
    pub name: String,
    /// Label values the series must have; the matching series are summed.
    pub labels: BTreeMap<String, String>,
    pub condition: MetricCondition,
    pub timeout: Duration,
    /// Keep writing new data while waiting, so a condition that needs new
    /// data (a compaction) is bounded by how much was written, not by time.
    pub drive: Option<Drive>,
}

#[derive(Debug, Clone)]
pub struct Drive {
    pub source: String,
    /// Run once per round; `${round}` is the round number, from 1.
    pub sql: String,
    pub every: Duration,
    /// Give up after this many rounds without the condition.
    pub rounds: u32,
}

#[derive(Debug, Clone)]
pub enum MetricCondition {
    Above(f64),
    Below(f64),
    /// Below the sum of another series (for "permits in use" as
    /// `available < total`).
    BelowMetric(String),
    /// Higher than when the step started.
    Increases,
    /// Exactly `amount` higher than at `mark-metrics: since` (or than when
    /// the step started).
    IncreasedBy {
        amount: f64,
        since: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct SltStep {
    pub file: String,
    pub source: String,
    /// `skipif`/`onlyif` labels; default: the source's (`postgresql`, `mysql`).
    pub labels: Option<Vec<String>>,
    pub setup: bool,
    /// Error messages (substrings) Spice is known to give for queries the
    /// source answers: counted and reported as `known`, not as failures.
    pub known_errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StatusStep {
    pub dataset: String,
    /// `Ready`, `Error`, `Refreshing`, …
    pub status: Option<String>,
    pub message_contains: Option<String>,
    /// Whether `/v1/ready` must answer ready (`true`) or not ready (`false`).
    pub ready: Option<bool>,
    /// Fields of the dataset's status that must be set, or must be absent.
    pub has: Vec<String>,
    pub lacks: Vec<String>,
    pub timing: Timing,
}

/// Random predicates whose three partitions must add up to the table.
#[derive(Debug, Clone)]
pub struct TlpStep {
    pub table: String,
    pub seed: u64,
    pub queries: u64,
    pub depth: u32,
    /// A numeric column whose count, sum, min and max must also add up.
    pub aggregate: Option<String>,
    /// A source answering the same SQL, to compare the partitions with.
    pub oracle: Option<String>,
    /// Columns left out of the predicates.
    pub skip_columns: Vec<String>,
    /// Known defects the predicates steer clear of (`avoid: [not, …]`).
    pub avoid: crate::tlp::Avoid,
}

/// Query transports available to random protocol comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryProtocol {
    Flight,
    FlightSql,
    Http,
}

/// Random queries compared with an oracle or across protocols.
#[derive(Debug, Clone)]
pub struct RandomQueriesStep {
    pub tables: Vec<String>,
    pub seed: u64,
    pub queries: u64,
    /// How many relations deep a query nests.
    pub depth: u32,
    pub oracle: Option<String>,
    /// Transports that must return the same rows for each generated query.
    pub protocols: Vec<QueryProtocol>,
    /// Equalities two of the tables may be joined on, `a.x = b.y`.
    pub joins: Vec<String>,
    pub skip_columns: Vec<String>,
    pub avoid: crate::tlp::Avoid,
}

/// Work repeated in the background while later steps run.
#[derive(Debug, Clone)]
pub struct BackgroundStep {
    pub id: String,
    pub work: BackgroundWork,
    /// Loops running the work side by side.
    pub concurrency: usize,
    /// How often each loop runs it; back to back when unset.
    pub every: Option<Duration>,
    /// For a query: the one row every answer must be, by column name, so an
    /// invariant that a concurrent change could break is checked on every
    /// iteration.
    pub expect: Option<BTreeMap<String, Option<String>>>,
}

#[derive(Debug, Clone)]
pub enum BackgroundWork {
    /// A statement through Spice's `/v1/sql`.
    Query(String),
    SourceExec {
        source: String,
        sql: String,
    },
}

impl BackgroundWork {
    pub fn describe(&self) -> String {
        match self {
            Self::Query(sql) => format!("query {sql}"),
            Self::SourceExec { source, sql } => format!("source-exec {source}: {sql}"),
        }
    }
}

/// One HTTP request to the runtime and what its response must carry.
#[derive(Debug, Clone)]
pub struct HttpStep {
    pub method: String,
    pub path: String,
    /// URL query parameters, encoded and appended to `path`.
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub status: Option<u16>,
    /// Response headers that must be present, each holding the value given
    /// (as a substring, so a list-valued header such as `vary` matches one
    /// entry).
    pub expect_headers: Vec<(String, String)>,
    /// Response headers that must be absent.
    pub lacks_headers: Vec<String>,
    pub body_contains: Option<String>,
    /// Patterns the body must match, and must not match.
    pub body_matches: Vec<regex::Regex>,
    pub body_lacks: Vec<regex::Regex>,
    /// Predicates over the body parsed as JSON.
    pub json: Vec<crate::json_assert::JsonAssertion>,
    pub timing: Timing,
    /// The API key the request presents (`api-key: none` for none).
    pub key: crate::spiced::Key,
}

/// Checks the instance's `spiced.log`, across every start.
#[derive(Debug, Clone)]
pub struct LogStep {
    /// No line reports an error or a failure (`error` or `failed`, in any
    /// case) unless an `allow` pattern matches it.
    pub clean: bool,
    pub allow: Vec<regex::Regex>,
    /// A line matches each of these.
    pub contains: Vec<regex::Regex>,
    /// No line matches any of these.
    pub lacks: Vec<regex::Regex>,
    /// The number of lines matching each pattern.
    pub count: Vec<(regex::Regex, LineCount)>,
    pub timing: Timing,
}

/// How many log lines a `count` pattern should match: `3`, `'>=3'` or `'<=3'`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineCount {
    Exactly(usize),
    AtLeast(usize),
    AtMost(usize),
}

impl LineCount {
    pub fn holds(self, n: usize) -> bool {
        match self {
            Self::Exactly(want) => n == want,
            Self::AtLeast(want) => n >= want,
            Self::AtMost(want) => n <= want,
        }
    }

    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let text = match value {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.trim().to_string(),
            _ => bail!("a `count` is a number or '>=N' / '<=N', not {value}"),
        };
        let number = |digits: &str| -> Result<usize> {
            digits
                .trim()
                .parse()
                .with_context(|| format!("a `count` is a number or '>=N' / '<=N', not '{text}'"))
        };
        Ok(if let Some(rest) = text.strip_prefix(">=") {
            Self::AtLeast(number(rest)?)
        } else if let Some(rest) = text.strip_prefix("<=") {
            Self::AtMost(number(rest)?)
        } else {
            Self::Exactly(number(&text)?)
        })
    }
}

impl std::fmt::Display for LineCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exactly(n) => write!(f, "{n}"),
            Self::AtLeast(n) => write!(f, ">= {n}"),
            Self::AtMost(n) => write!(f, "<= {n}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillSignal {
    Kill,
    /// SIGSTOP first, so the process is frozen at a stable point, then SIGKILL.
    StopThenKill,
}

#[derive(Debug, Clone)]
pub struct QueryStep {
    /// Compare this query over these transports; omitted keeps the existing path.
    pub protocols: Option<Vec<QueryProtocol>>,
    /// Names the answer, so a later query can expect `same-as` it.
    pub id: Option<String>,
    pub sql: String,
    /// The SQL to send to the oracle, when its dialect needs a different text.
    pub oracle_sql: Option<String>,
    pub expect: Option<Expect>,
    pub timing: Timing,
    /// Two empty answers agree without comparing anything, so they fail unless
    /// the script says an empty answer is the point.
    pub expect_empty: bool,
    pub expect_error: bool,
    /// Text the error must contain (`expect-error: "<text>"`).
    pub error_contains: Option<String>,
    /// Print the answer, for exploring what Spice returns.
    pub show: bool,
    /// What the SQL sent to the source must and must not contain, by place.
    pub sent: Vec<SentCheck>,
    /// The API key the query presents (`api-key: none` for none).
    pub key: crate::spiced::Key,
    /// Bind these values and run the query as a Flight SQL prepared
    /// statement (`$1`/`?` placeholders), instead of a plain query.
    pub parameters: Option<Vec<Value>>,
    /// Run the query as a Flight SQL prepared statement on this named
    /// session, kept for later queries naming it (`PREPARE` then `EXECUTE`).
    pub session: Option<String>,
}

/// At a place (variant and matrix conditions), what the query's plan, or the
/// SQL it sends to its source, must contain: a fragment (`sends`, `plan-has`),
/// no such fragment (`keeps`, `plan-lacks`), or the fragment exactly `count`
/// times (`plan-count`).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SentCheck {
    pub place: Vec<String>,
    pub fragment: String,
    /// Whether the fragment must appear (`count` takes precedence).
    pub sent: bool,
    /// Check the whole `EXPLAIN` output rather than the SQL sent.
    #[serde(default)]
    pub whole_plan: bool,
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub enum Timing {
    Once,
    /// Retry until it matches or the duration passes.
    Eventually(Duration),
    /// Must match on every attempt for the whole duration.
    Consistently(Duration),
}

impl Script {
    /// Loads a script file. Its run time must be recorded in
    /// `recordings/durations.json` beside it, unless `duration_optional` (a
    /// run that records durations, which is how a new script gets one).
    pub fn load(path: &Path, duration_optional: bool) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read script {}", path.display()))?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let shared = SharedVariants::find(base)?;
        let mut script = Self::parse_in(&text, base, &shared)
            .with_context(|| format!("Invalid script {}", path.display()))?;
        script.dir = base.to_path_buf();
        script.file_stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        script.duration = read_durations(base)?
            .get(&script.file_stem)
            .map(|s| Duration::from_secs(*s));
        ensure!(
            duration_optional || script.duration.is_some(),
            "Script {} has no recorded duration in {}. Run it with --record-durations to record one.",
            path.display(),
            base.join(DURATIONS_FILE).display()
        );
        Ok(script)
    }

    /// Records how long the scripts in `dir` took, by file name, in
    /// `dir/recordings/durations.json`, keeping the entries of the others.
    pub fn record_durations(dir: &Path, took: &[(String, Duration)]) -> Result<()> {
        let mut all = read_durations(dir)?;
        for (stem, duration) in took {
            all.insert(
                stem.clone(),
                duration.as_secs() + u64::from(duration.subsec_nanos() > 0),
            );
        }
        let path = dir.join(DURATIONS_FILE);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, serde_json::to_string_pretty(&all)? + "\n")
            .with_context(|| format!("Failed to write {}", path.display()))
    }

    /// Keeps only the matrix values and variants `selection` names, and
    /// records what it left out. `false` when the script has none of a
    /// requested key's values, or none of the variants: it is not run.
    pub fn select(&mut self, selection: &Selection) -> bool {
        if selection.is_empty() {
            return true;
        }
        let all_runs = self.run_labels();
        for (key, keep) in &selection.matrix {
            let Some(values) = self.matrix.get_mut(key) else {
                return false;
            };
            values.retain(|v| keep.contains(&matrix_value_text(v)));
            if values.is_empty() {
                return false;
            }
        }
        if !selection.variants.is_empty() {
            let dropped: Vec<String> = self
                .variants
                .keys()
                .filter(|n| !selection.variants.contains(n))
                .cloned()
                .collect();
            if dropped.len() == self.variants.len() {
                return false;
            }
            for name in dropped {
                self.variants.shift_remove(&name);
                self.left_out.variants.insert(name);
            }
        }
        let kept: std::collections::BTreeSet<String> = self.run_labels().into_iter().collect();
        self.left_out.runs = all_runs.into_iter().filter(|r| !kept.contains(r)).collect();
        true
    }

    /// The label of each matrix combination's run, `name[key=value,…]`.
    pub fn run_labels(&self) -> Vec<String> {
        matrix_combinations(&self.matrix)
            .iter()
            .map(|combo| self.run_label(combo))
            .collect()
    }

    /// A matrix combination's run, as failures and the ledger name it:
    /// `name[key=value,…]` with the keys in alphabetical order, or the name
    /// alone without a matrix. The keys are sorted here rather than taken in
    /// the map's order, which `serde_json`'s `preserve_order` feature changes
    /// whenever a build also links a crate that enables it.
    pub fn run_label(&self, combo: &Map<String, Value>) -> String {
        let mut pairs: Vec<(&String, &Value)> = combo.iter().collect();
        pairs.sort_unstable_by_key(|(k, _)| *k);
        let combo_label = pairs
            .iter()
            .map(|(k, v)| format!("{k}={}", matrix_value_text(v)))
            .collect::<Vec<_>>()
            .join(",");
        if combo_label.is_empty() {
            self.name.clone()
        } else {
            format!("{}[{combo_label}]", self.name)
        }
    }

    #[cfg(test)]
    pub fn parse(text: &str) -> Result<Self> {
        Self::parse_in(text, Path::new("."), &SharedVariants::default())
    }

    /// Parses a script whose `corpus` paths are relative to `base`, and
    /// whose variants may name one of `shared`.
    pub fn parse_in(text: &str, base: &Path, shared: &SharedVariants) -> Result<Self> {
        let mut root: Value = yaml::from_str(text)?;
        let steps = root
            .as_object_mut()
            .context("A script must be a YAML mapping")?
            .remove("steps")
            .context("A script needs `steps`")?;
        expand_shared_variants(&mut root, shared)?;
        let mut script: Script = serde_json::from_value(root)?;
        let Value::Array(steps) = steps else {
            bail!("`steps` must be a list");
        };
        let mut expanded = Vec::new();
        for (i, mut step) in steps.into_iter().enumerate() {
            let context = || format!("step {}", i + 1);
            read_sql_files(&mut step, base).with_context(context)?;
            if let Some(corpus) = step.get("corpus") {
                expanded.extend(
                    expand_corpus(corpus, &step, base)
                        .with_context(context)?
                        .into_iter()
                        .map(|query| Step {
                            corpus: Some(i),
                            ..query
                        }),
                );
            } else {
                expanded.push(parse_step(0, step).with_context(context)?);
            }
        }
        // Number after expanding, so a corpus's queries report and compare
        // as steps of their own.
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (i, step) in expanded.iter_mut().enumerate() {
            step.index = i + 1;
            let n = seen.entry(step.key.clone()).or_default();
            *n += 1;
            if *n > 1 {
                step.key = format!("{} #{n}", step.key);
            }
        }
        script.steps = expanded;
        let mut ids = std::collections::BTreeSet::new();
        for step in &script.steps {
            if let Action::Query(q) = &step.action {
                if let Some(target) = q.expect.as_ref().and_then(|e| e.same_as.as_ref())
                    && !ids.contains(target)
                {
                    bail!(
                        "step {} expects `same-as: {target}`, but no earlier query has that id",
                        step.index
                    );
                }
                if let Some(id) = &q.id
                    && !ids.insert(id.clone())
                {
                    bail!("step {} reuses query id '{id}'", step.index);
                }
            }
            for key in step.when.keys() {
                if !script.matrix.contains_key(key) {
                    bail!(
                        "step {} has `when: {key}`, which is not a matrix key",
                        step.index
                    );
                }
            }
            for (key, names) in [("only", &step.only), ("except", &step.except)] {
                for name in names {
                    if !script.variants.contains_key(name) {
                        bail!(
                            "step {} names unknown variant '{name}' in `{key}`",
                            step.index
                        );
                    }
                }
            }
        }
        Ok(script)
    }
}

/// `api-key: none` presents no key, any other value presents that key, and
/// no `api-key` presents the script's own.
fn api_key_choice(value: Option<String>) -> crate::spiced::Key {
    match value {
        None => crate::spiced::Key::Default,
        Some(key) if key == "none" => crate::spiced::Key::None,
        Some(key) => crate::spiced::Key::Given(key),
    }
}

impl Script {
    /// The defect names this script's generator steps avoid.
    pub fn avoided(&self) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = self
            .steps
            .iter()
            .flat_map(|step| match &step.action {
                Action::Tlp(t) => t.avoid.0.as_slice(),
                Action::RandomQueries(r) => r.avoid.0.as_slice(),
                _ => &[],
            })
            .map(|d| d.name())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }
}

/// Every combination of the matrix values, in declaration order.
pub fn matrix_combinations(matrix: &IndexMap<String, Vec<Value>>) -> Vec<Map<String, Value>> {
    matrix
        .iter()
        .fold(vec![Map::new()], |combos, (key, values)| {
            combos
                .into_iter()
                .flat_map(|combo| {
                    values.iter().map(move |v| {
                        let mut next = combo.clone();
                        next.insert(key.clone(), v.clone());
                        next
                    })
                })
                .collect()
        })
}

/// A matrix value as run labels and `--matrix` spell it: a string as is,
/// anything else as JSON.
pub fn matrix_value_text(v: &Value) -> String {
    v.as_str().map_or_else(|| v.to_string(), str::to_string)
}

/// The file defining the variants scripts name instead of writing the patch:
/// the one in a script's directory, or else the nearest directory above it.
pub const VARIANTS_FILE: &str = "variants.yaml";

/// Variant patches by name, from a [`VARIANTS_FILE`].
#[derive(Debug, Default)]
pub struct SharedVariants {
    /// The file they came from; `None` when no directory had one.
    path: Option<std::path::PathBuf>,
    patches: IndexMap<String, Value>,
}

impl SharedVariants {
    /// The variants of the [`VARIANTS_FILE`] in `dir` or the nearest
    /// directory above it; none when there is no such file.
    pub fn find(dir: &Path) -> Result<Self> {
        let dir = std::path::absolute(dir)?;
        let Some(path) = dir
            .ancestors()
            .map(|d| d.join(VARIANTS_FILE))
            .find(|p| p.is_file())
        else {
            return Ok(Self::default());
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        Self::parse(&text, path.clone()).with_context(|| format!("Invalid {}", path.display()))
    }

    fn parse(text: &str, path: std::path::PathBuf) -> Result<Self> {
        let patches: IndexMap<String, Value> = yaml::from_str(text)?;
        for (name, patch) in &patches {
            ensure!(
                patch.is_object(),
                "variant '{name}' must be a patch (a mapping), as a script writes one"
            );
            VariantSpec::deserialize(patch)
                .with_context(|| format!("variant '{name}' is not a valid patch"))?;
        }
        Ok(Self {
            path: Some(path),
            patches,
        })
    }
}

/// Replaces each variant written as a name instead of a patch with that
/// variant's patch from `shared`.
fn expand_shared_variants(root: &mut Value, shared: &SharedVariants) -> Result<()> {
    let Some(Value::Object(variants)) = root.get_mut("variants") else {
        return Ok(());
    };
    for (name, variant) in variants.iter_mut() {
        let Value::String(shared_name) = variant else {
            continue;
        };
        let patch = match (&shared.path, shared.patches.get(shared_name.as_str())) {
            (_, Some(patch)) => patch.clone(),
            (Some(path), None) => bail!(
                "variant '{name}': '{shared_name}' is not defined in {}; define it there, or write the patch (it defines {})",
                path.display(),
                shared
                    .patches
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            (None, None) => bail!(
                "variant '{name}': '{shared_name}' names a shared variant, but there is no {VARIANTS_FILE} in the script's directory or above it; write the patch"
            ),
        };
        *variant = patch;
    }
    Ok(())
}

/// The known defects an `avoid:` list names.
fn avoid_list(names: &[String]) -> Result<crate::tlp::Avoid> {
    use crate::tlp::Defect;
    names
        .iter()
        .map(|name| {
            Defect::NAMES
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, d)| *d)
                .with_context(|| {
                    let known: Vec<&str> = Defect::NAMES.iter().map(|(n, _)| *n).collect();
                    format!("`avoid` knows {}, not '{name}'", known.join(", "))
                })
        })
        .collect::<Result<Vec<_>>>()
        .map(crate::tlp::Avoid)
}

/// Expands `corpus: <file>` into one `query` step per statement in the file.
/// Statements end with `;` at the end of a line. A `-- expect-empty` or
/// `-- expect-error` line before a statement, `-- oracle-sql: <sql>`, or
/// `-- sends <place>: <fragment>` / `-- keeps <place>: <fragment>` (the SQL
/// sent to the source there contains / lacks the fragment), or
/// `-- plan-has`, `-- plan-lacks`, `-- plan-count <place>: <n> <fragment>` (the
/// same of the whole `EXPLAIN` output), applies to that statement.
/// The step's other keys (`eventually`, `expect`, `only`, `when`) apply to
/// every query.
fn expand_corpus(corpus: &Value, step: &Value, base: &Path) -> Result<Vec<Step>> {
    let file = corpus.as_str().context("`corpus` takes a file path")?;
    let mut path = base.join(file);
    // A directory, or `dir/prefix*suffix`, is a corpus of one statement per
    // matching `.sql` file, in file-name order (as testoperator's query sets
    // are kept).
    let pattern = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.split_once('*'))
        .map(|(a, b)| (a.to_string(), b.to_string()));
    if pattern.is_some() {
        path = path.parent().map(Path::to_path_buf).unwrap_or_default();
    }
    let text = if path.is_dir() {
        let mut files: Vec<_> = std::fs::read_dir(&path)
            .with_context(|| format!("Failed to list corpus {}", path.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
                pattern.as_ref().is_none_or(|(prefix, suffix)| {
                    name.starts_with(prefix.as_str()) && name.ends_with(suffix.as_str())
                })
            })
            .collect();
        files.sort();
        let mut text = String::new();
        for file in files {
            let statement = std::fs::read_to_string(&file)?;
            text.push_str(statement.trim().trim_end_matches(';'));
            text.push_str(";\n");
        }
        text
    } else {
        std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read corpus {}", path.display()))?
    };
    let Value::Object(template) = step else {
        bail!("a step must be a mapping");
    };
    let mut steps = Vec::new();
    let mut sql = String::new();
    let mut directives = Map::new();
    let template = template.clone();
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(comment) = trimmed.strip_prefix("--") {
            let comment = comment.trim();
            if comment == "expect-empty" || comment == "expect-error" {
                directives.insert(comment.into(), Value::Bool(true));
            } else if let Some(oracle) = comment.strip_prefix("oracle-sql:") {
                directives.insert(
                    "oracle-sql".into(),
                    Value::String(oracle.trim().to_string()),
                );
            } else if let Some(((sent, whole_plan, counted), rest)) = [
                ("sends ", (true, false, false)),
                ("keeps ", (false, false, false)),
                ("plan-has ", (true, true, false)),
                ("plan-lacks ", (false, true, false)),
                ("plan-count ", (true, true, true)),
            ]
            .iter()
            .find_map(|(verb, kind)| comment.strip_prefix(verb).map(|r| (*kind, r)))
                && let Some((place, fragment)) = rest.split_once(':')
            {
                let fragment = fragment.trim();
                let (count, fragment) = if counted {
                    let (n, f) = fragment
                        .split_once(' ')
                        .with_context(|| format!("`-- {comment}` needs `<count> <fragment>`"))?;
                    (
                        Some(
                            n.parse::<usize>()
                                .with_context(|| format!("`{n}` is not a count"))?,
                        ),
                        f.trim(),
                    )
                } else {
                    (None, fragment)
                };
                let check = SentCheck {
                    place: place
                        .split('&')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect(),
                    fragment: fragment.to_string(),
                    sent,
                    whole_plan,
                    count,
                };
                ensure!(
                    !check.fragment.is_empty(),
                    "`-- {comment}` names no SQL fragment"
                );
                let list = directives
                    .entry("sent")
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Value::Array(items) = list {
                    items.push(serde_json::to_value(check)?);
                }
            }
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        sql.push_str(line);
        sql.push('\n');
        if trimmed.ends_with(';') {
            let statement = sql.trim().trim_end_matches(';').trim().to_string();
            let mut query = template.clone();
            query.remove("corpus");
            query.insert("query".into(), Value::String(statement));
            query.extend(std::mem::take(&mut directives));
            steps.push(
                parse_step(0, Value::Object(query))
                    .with_context(|| format!("in corpus {}", path.display()))?,
            );
            sql.clear();
        }
    }
    ensure!(
        sql.trim().is_empty(),
        "corpus {} ends without `;`",
        path.display()
    );
    ensure!(
        !steps.is_empty(),
        "corpus {} has no statements",
        path.display()
    );
    Ok(steps)
}

const ACTIONS: &[&str] = &[
    "start",
    "stop",
    "kill-spiced",
    "restart",
    "upgrade",
    "wait-ready",
    "edit-spicepod",
    "flight-put",
    "flight-exec",
    "kill-executor",
    "start-executors",
    "add-executor",
    "start-source",
    "stop-source",
    "pause-source",
    "unpause-source",
    "source-exec",
    "write",
    "refresh",
    "query",
    "spice-exec",
    "explain",
    "assert-status",
    "assert-log",
    "http",
    "background",
    "tlp",
    "random-queries",
    "dbgen",
    "source-query",
    "assert-files",
    "stop-background",
    "spice",
    "slt-setup",
    "slt",
    "assert-objects",
    "wait-metric",
    "mark-metrics",
    "generate",
    "sleep",
];

/// Replaces `source-exec: { <source>: { file: <path> } }` with the file's
/// text, read relative to `base`, so a fixture shared with other harnesses
/// keeps one copy.
fn read_sql_files(step: &mut Value, base: &Path) -> Result<()> {
    let Some(Value::Object(sources)) = step.get_mut("source-exec") else {
        return Ok(());
    };
    for sql in sources.values_mut() {
        if let Some(file) = sql.get("file").and_then(Value::as_str) {
            let path = base.join(file);
            *sql = Value::String(
                std::fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?,
            );
        }
    }
    Ok(())
}

/// A step's ledger key: its action and argument, whitespace collapsed.
fn step_key(name: &str, arg: &Value) -> String {
    let text = match arg {
        Value::String(s) => s.clone(),
        other => {
            // Ledger identities must not depend on serde_json's preserve_order feature.
            let mut canonical = other.clone();
            canonical.sort_all_objects();
            canonical.to_string()
        }
    };
    format!(
        "{name}: {}",
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    )
}

fn parse_step(index: usize, step: Value) -> Result<Step> {
    let Value::Object(mut map) = step else {
        bail!("a step must be a mapping with one action key");
    };
    let actions: Vec<&str> = ACTIONS
        .iter()
        .copied()
        .filter(|a| map.contains_key(*a))
        .collect();
    let [name] = actions.as_slice() else {
        bail!(
            "a step needs exactly one action key, one of: {}",
            ACTIONS.join(", ")
        );
    };
    let name = *name;
    let arg = map.remove(name).unwrap_or(Value::Null);
    let key = step_key(name, &arg);
    let only = one_or_many(map.remove("only"), "only", "a variant name or a list")?;
    let except = one_or_many(map.remove("except"), "except", "a variant name or a list")?;
    ensure!(
        only.is_empty() || except.is_empty(),
        "a step takes `only` or `except`, not both"
    );
    let when = match map.remove("when") {
        None => Map::new(),
        Some(Value::Object(m)) => m,
        Some(_) => bail!("`when` must map matrix keys to values"),
    };
    let mut opts = Opts(map);
    let action = match name {
        "start" => Action::Start {
            ready_timeout: opts.duration_in(&arg, "ready-timeout")?,
            wait_ready: wait_ready(&arg)?,
            patch: patch(&arg)?,
        },
        "stop" => Action::Stop,
        "kill-spiced" => Action::Kill {
            signal: parse_signal(arg.get("signal"))?.unwrap_or(KillSignal::Kill),
        },
        "restart" | "upgrade" => Action::Restart {
            wait_ready: wait_ready(&arg)?,
            patch: patch(&arg)?,
            kill: parse_signal(arg.get("kill"))?,
            ready_timeout: opts.duration_in(&arg, "ready-timeout")?,
            upgrade: name == "upgrade",
        },
        "kill-executor" => Action::KillExecutor(
            serde_json::from_value(arg.clone())
                .context("`kill-executor` takes the executor's position, from 0")?,
        ),
        "start-executors" => Action::StartExecutors,
        "add-executor" => Action::AddExecutor,
        "flight-put" => {
            let mut inner = Opts::of_mapping(arg, "`flight-put` takes a mapping: dataset, rows")?;
            let dataset = inner
                .string("dataset")?
                .context("`flight-put` needs a `dataset`")?;
            let rows: Vec<Vec<Value>> = inner
                .parse("rows", "`rows` is a list of rows")?
                .unwrap_or_default();
            let from_source = match inner.take("from-source") {
                None => None,
                Some(Value::Object(m)) => {
                    let (source, sql) = single_entry(m, "`from-source` takes exactly one source")?;
                    let Value::String(sql) = sql else {
                        bail!("`from-source` SQL must be a string");
                    };
                    Some((source, sql))
                }
                Some(_) => bail!("`from-source` takes a mapping of source to SQL"),
            };
            ensure!(
                rows.is_empty() != from_source.is_none(),
                "`flight-put` takes one of `rows` or `from-source`"
            );
            let times = inner
                .parse("times", "`times` must be a number")?
                .unwrap_or(1);
            inner.finish()?;
            let (expect_error, error_contains) = opts.expected_error("expect-error")?;
            Action::FlightPut {
                dataset,
                rows,
                from_source,
                times,
                key: api_key_choice(opts.string("api-key")?),
                expect_error,
                error_contains,
            }
        }
        "flight-exec" => {
            let (expect_error, error_contains) = opts.expected_error("expect-error")?;
            Action::FlightExec {
                sql: string_arg(&arg, name)?,
                count: opts.parse("count", "`count` is a number")?,
                key: api_key_choice(opts.string("api-key")?),
                expect_error,
                error_contains,
            }
        }
        "edit-spicepod" => Action::EditSpicepod(
            serde_json::from_value(arg.clone())
                .context("`edit-spicepod` takes a patch, as a variant does")?,
        ),
        "wait-ready" => Action::WaitReady {
            timeout: opts
                .duration_in(&arg, "timeout")?
                .unwrap_or(Duration::from_mins(2)),
        },
        "start-source" => Action::StartSource(string_arg(&arg, name)?),
        "stop-source" => Action::StopSource(string_arg(&arg, name)?),
        "pause-source" => Action::PauseSource(string_arg(&arg, name)?),
        "unpause-source" => Action::UnpauseSource(string_arg(&arg, name)?),
        "write" => {
            let Value::Object(m) = arg else {
                bail!("`write` takes a mapping of source name to SQL");
            };
            let (source, sql) = single_entry(m, "`write` takes exactly one source")?;
            let sql = match sql {
                Value::String(s) => vec![s],
                v => serde_json::from_value(v).context("SQL must be a string or a list")?,
            };
            let mirror = opts
                .string("mirror")?
                .context("`write` requires `mirror`: a postgres source")?;
            ensure!(source != mirror, "a write target cannot be its own mirror");
            Action::Write {
                source,
                sql,
                mirror,
                key: opts.string("key")?.unwrap_or_else(|| "id".into()),
            }
        }
        "source-exec" => {
            let Value::Object(m) = arg else {
                bail!("`source-exec` takes a mapping of source name to SQL");
            };
            let (source, sql) = single_entry(m, "`source-exec` takes exactly one source")?;
            let sql = match sql {
                Value::String(s) => vec![s],
                v => serde_json::from_value(v).context("SQL must be a string or a list")?,
            };
            Action::SourceExec { source, sql }
        }
        "refresh" => match arg {
            Value::String(dataset) => Action::Refresh {
                dataset,
                expect_error: opts.bool("expect-error")?,
                wait: true,
                refresh_sql: None,
            },
            Value::Object(mut m) => {
                let dataset = m
                    .remove("dataset")
                    .and_then(|v| v.as_str().map(str::to_string))
                    .context("`refresh` needs a `dataset`")?;
                let mut inner = Opts(m);
                let expect_error = inner.bool("expect-error")?;
                let wait = match inner.take("wait") {
                    None => true,
                    Some(Value::Bool(b)) => b,
                    Some(_) => bail!("`wait` must be true or false"),
                };
                let refresh_sql = inner.string("refresh-sql")?;
                inner.finish()?;
                Action::Refresh {
                    dataset,
                    expect_error,
                    wait,
                    refresh_sql,
                }
            }
            _ => bail!("`refresh` takes a dataset name"),
        },
        "query" => {
            let sql = string_arg(&arg, name)?;
            let timing = opts.timing("a query")?;
            let (expect_error, error_contains) = opts.expected_error("expect-error")?;
            let protocols = opts.protocols()?;
            let parameters = opts.parse("parameters", "`parameters` is a list of values")?;
            let session = opts.string("flight-session")?;
            ensure!(
                (parameters.is_none() && session.is_none())
                    || protocols
                        .as_deref()
                        .is_none_or(|p| p == [QueryProtocol::FlightSql]),
                "prepared queries support only `protocols: [flightsql]`"
            );
            Action::Query(QueryStep {
                protocols,
                id: opts.string("id")?,
                sql,
                oracle_sql: opts.string("oracle-sql")?,
                expect: opts.parse("expect", "invalid `expect`")?,
                timing,
                expect_empty: opts.bool("expect-empty")?,
                expect_error,
                error_contains,
                show: opts.bool("show")?,
                sent: opts.parse("sent", "invalid `sent`")?.unwrap_or_default(),
                key: api_key_choice(opts.string("api-key")?),
                parameters,
                session,
            })
        }
        "spice-exec" => {
            let (expect_error, error_contains) = opts.expected_error("expect-error")?;
            Action::SpiceExec {
                sql: string_arg(&arg, name)?,
                expect_error,
                error_contains,
            }
        }
        "explain" => {
            let contains = one_or_many(opts.take("contains"), "contains", "a string or a list")?;
            let omits = one_or_many(opts.take("omits"), "omits", "a string or a list")?;
            if contains.is_empty() && omits.is_empty() {
                bail!("`explain` needs `contains` or `omits`");
            }
            Action::Explain(ExplainStep {
                sql: string_arg(&arg, name)?,
                analyze: opts.bool("analyze")?,
                contains,
                omits,
            })
        }
        "assert-status" => {
            let mut inner =
                Opts::of_mapping(arg, "`assert-status` takes a mapping with a `dataset`")?;
            let dataset = inner
                .string("dataset")?
                .context("`assert-status` needs a `dataset`")?;
            let status = inner.string("status")?;
            let message_contains = inner.string("message-contains")?;
            let ready = match inner.take("ready") {
                None => None,
                Some(Value::Bool(b)) => Some(b),
                Some(_) => bail!("`ready` must be true or false"),
            };
            let has = inner
                .parse("has", "`has` must be a list of field names")?
                .unwrap_or_default();
            let lacks = inner
                .parse("lacks", "`lacks` must be a list of field names")?
                .unwrap_or_default();
            inner.finish()?;
            let timing = opts.timing("a check")?;
            Action::AssertStatus(StatusStep {
                dataset,
                status,
                message_contains,
                ready,
                has,
                lacks,
                timing,
            })
        }
        "spice" => {
            let (args, output_contains, expect_error) = match arg {
                Value::Array(_) => (
                    serde_json::from_value(arg).context("`spice` arguments must be strings")?,
                    opts.string("output-contains")?,
                    opts.bool("expect-error")?,
                ),
                Value::Object(m) => {
                    let mut inner = Opts(m);
                    let args = inner
                        .parse("args", "`args` must be a list of strings")?
                        .context("`spice` needs `args`")?;
                    let contains = inner.string("output-contains")?;
                    let expect_error = inner.bool("expect-error")?;
                    inner.finish()?;
                    (args, contains, expect_error)
                }
                _ => bail!("`spice` takes a list of arguments, or a mapping with `args`"),
            };
            Action::SpiceCli {
                args,
                output_contains,
                expect_error,
            }
        }
        "assert-files" => {
            let mut inner = Opts::of_mapping(
                arg,
                "`assert-files` takes a mapping with `contains` or `lacks`",
            )?;
            let contains = inner.patterns("contains")?;
            let lacks = inner.patterns("lacks")?;
            inner.finish()?;
            ensure!(
                !contains.is_empty() || !lacks.is_empty(),
                "`assert-files` needs `contains` or `lacks`"
            );
            Action::AssertFiles { contains, lacks }
        }
        "source-query" => {
            let Value::Object(m) = arg else {
                bail!("`source-query` takes a mapping of source name to SQL");
            };
            let (source, sql) = single_entry(m, "`source-query` takes exactly one source")?;
            let Value::String(sql) = sql else {
                bail!("`source-query` SQL must be a string");
            };
            let rows = match opts.take("expect") {
                Some(Value::Object(mut e)) => {
                    let rows = e.remove("rows").context("`source-query` expects `rows`")?;
                    ensure!(
                        e.is_empty(),
                        "`source-query` takes only `expect: {{ rows }}`"
                    );
                    serde_json::from_value(rows).context("`rows` must be a list of rows")?
                }
                _ => bail!("`source-query` needs `expect: {{ rows: [...] }}`"),
            };
            let timing = opts.timing("a check")?;
            Action::SourceQuery {
                source,
                sql,
                rows,
                timing,
            }
        }
        "tlp" => {
            let mut inner = Opts::of_mapping(arg, "`tlp` takes a mapping with a `table`")?;
            let table = inner.string("table")?.context("`tlp` needs a `table`")?;
            let seed = inner.whole_number("seed", 1)?;
            let queries = inner.whole_number("queries", 100)?;
            let depth =
                u32::try_from(inner.whole_number("depth", 3)?).context("`depth` is too large")?;
            let aggregate = inner.string("aggregate")?;
            let oracle = inner.string("oracle")?;
            let skip_columns = inner.list("skip-columns")?;
            let avoid = avoid_list(&inner.list("avoid")?)?;
            inner.finish()?;
            Action::Tlp(TlpStep {
                table,
                seed,
                queries,
                depth,
                aggregate,
                oracle,
                skip_columns,
                avoid,
            })
        }
        "random-queries" => {
            let mut inner = Opts::of_mapping(
                arg,
                "`random-queries` takes a mapping with `tables` and `oracle` or `protocols`",
            )?;
            let tables = inner.list("tables")?;
            ensure!(!tables.is_empty(), "`random-queries` needs `tables`");
            let seed = inner.whole_number("seed", 1)?;
            let queries = inner.whole_number("queries", 100)?;
            let depth =
                u32::try_from(inner.whole_number("depth", 2)?).context("`depth` is too large")?;
            let oracle = inner.string("oracle")?;
            let protocols = inner
                .protocols()?
                .unwrap_or_else(|| vec![QueryProtocol::Flight]);
            ensure!(
                oracle.is_some() || protocols.len() >= 2,
                "`random-queries` needs an `oracle` or at least two `protocols`"
            );
            let joins = inner.list("joins")?;
            let skip_columns = inner.list("skip-columns")?;
            let avoid = avoid_list(&inner.list("avoid")?)?;
            inner.finish()?;
            Action::RandomQueries(RandomQueriesStep {
                tables,
                seed,
                queries,
                depth,
                oracle,
                protocols,
                joins,
                skip_columns,
                avoid,
            })
        }
        "dbgen" => {
            let mut inner =
                Opts::of_mapping(arg, "`dbgen` takes a mapping with `scale` and `into`")?;
            let scale = inner
                .parse("scale", "`scale` must be a number")?
                .context("`dbgen` needs a `scale`")?;
            let sources: Vec<String> = inner
                .parse("into", "`into` must be a list of source names")?
                .context("`dbgen` needs `into`")?;
            inner.finish()?;
            Action::Dbgen { scale, sources }
        }
        "background" => {
            let mut inner = Opts::of_mapping(arg, "`background` takes a mapping with an `id`")?;
            let id = inner.string("id")?.context("`background` needs an `id`")?;
            let query = inner.string("query")?;
            let source_exec = inner.take("source-exec");
            let work = match (query, source_exec) {
                (Some(sql), None) => BackgroundWork::Query(sql),
                (None, Some(Value::Object(m))) => {
                    let (source, sql) = single_entry(m, "`source-exec` takes exactly one source")?;
                    let Value::String(sql) = sql else {
                        bail!("background `source-exec` SQL must be a string");
                    };
                    BackgroundWork::SourceExec { source, sql }
                }
                _ => bail!("`background` needs one of `query` or `source-exec`"),
            };
            let concurrency = inner
                .parse("concurrency", "`concurrency` must be a number")?
                .unwrap_or(1);
            ensure!(concurrency > 0, "`concurrency` must be at least 1");
            let every = inner.duration("every")?;
            let expect = match inner.take("expect") {
                None => None,
                Some(Value::Object(m)) => Some(
                    m.into_iter()
                        .map(|(k, v)| {
                            let v = match v {
                                Value::Null => None,
                                Value::String(s) => Some(s),
                                other => Some(other.to_string()),
                            };
                            (k, v)
                        })
                        .collect(),
                ),
                Some(_) => bail!("background `expect` takes a mapping of column to value"),
            };
            ensure!(
                expect.is_none() || matches!(work, BackgroundWork::Query(_)),
                "background `expect` applies to a `query`"
            );
            inner.finish()?;
            Action::Background(BackgroundStep {
                id,
                work,
                concurrency,
                every,
                expect,
            })
        }
        "stop-background" => {
            let (id, allow) = match arg {
                Value::String(id) => (id, Vec::new()),
                Value::Object(m) => {
                    let mut inner = Opts(m);
                    let id = inner
                        .string("id")?
                        .context("`stop-background` needs an `id`")?;
                    let allow = one_or_many(inner.take("allow"), "allow", "a pattern or a list")?;
                    inner.finish()?;
                    (id, compile_patterns("allow", &allow)?)
                }
                _ => bail!("`stop-background` takes an id"),
            };
            Action::StopBackground { id, allow }
        }
        "http" => {
            let mut inner = Opts::of_mapping(arg, "`http` takes a mapping with a `path`")?;
            let path = inner.string("path")?.context("`http` needs a `path`")?;
            let body = inner.string("body")?;
            let method = inner
                .string("method")?
                .unwrap_or_else(|| if body.is_some() { "POST" } else { "GET" }.to_string())
                .to_ascii_uppercase();
            let pairs = |v: Option<Value>, key: &str| -> Result<Vec<(String, String)>> {
                let map: std::collections::BTreeMap<String, String> = v
                    .map(serde_json::from_value)
                    .transpose()
                    .with_context(|| format!("`{key}` must map header names to values"))?
                    .unwrap_or_default();
                Ok(map
                    .into_iter()
                    .map(|(k, v)| (k.to_ascii_lowercase(), v))
                    .collect())
            };
            let headers = pairs(inner.take("headers"), "headers")?;
            let query: Vec<(String, String)> = match inner.take("query") {
                None => Vec::new(),
                Some(Value::Object(m)) => m
                    .into_iter()
                    .map(|(k, v)| match v {
                        Value::String(s) => Ok((k, s)),
                        Value::Number(_) | Value::Bool(_) => Ok((k, v.to_string())),
                        _ => bail!("`query` values must be strings, numbers or booleans"),
                    })
                    .collect::<Result<_>>()?,
                Some(_) => bail!("`query` must map parameter names to values"),
            };
            let key = api_key_choice(inner.string("api-key")?);
            let mut body_matches = Vec::new();
            let mut body_lacks = Vec::new();
            let mut json = Vec::new();
            let (status, expect_headers, lacks_headers, body_contains) = match opts.take("expect") {
                None => (None, Vec::new(), Vec::new(), None),
                Some(Value::Object(e)) => {
                    let mut e = Opts(e);
                    let status = e.parse::<u16>("status", "`status` must be a number")?;
                    let expect_headers = pairs(e.take("headers"), "headers")?;
                    let lacks_headers: Vec<String> = e
                        .parse(
                            "lacks-headers",
                            "`lacks-headers` must be a list of header names",
                        )?
                        .unwrap_or_default();
                    let body_contains = e.string("body-contains")?;
                    for (key, into) in [
                        ("body-matches", &mut body_matches),
                        ("body-lacks", &mut body_lacks),
                    ] {
                        let list = one_or_many(e.take(key), key, "a pattern or a list")?;
                        *into = compile_patterns(key, &list)?;
                    }
                    json = match e.take("json") {
                        None => Vec::new(),
                        Some(Value::Array(list)) => list,
                        Some(one) => vec![one],
                    }
                    .into_iter()
                    .map(crate::json_assert::JsonAssertion::parse)
                    .collect::<Result<_>>()
                    .context("`json`")?;
                    e.finish()?;
                    (
                        status,
                        expect_headers,
                        lacks_headers
                            .iter()
                            .map(|h| h.to_ascii_lowercase())
                            .collect(),
                        body_contains,
                    )
                }
                Some(_) => bail!("`expect` must be a mapping"),
            };
            inner.finish()?;
            let timing = opts.timing("a check")?;
            Action::Http(HttpStep {
                method,
                path,
                query,
                headers,
                body,
                status,
                expect_headers,
                lacks_headers,
                body_contains,
                body_matches,
                body_lacks,
                json,
                timing,
                key,
            })
        }
        "assert-log" => {
            let mut inner = Opts::of_mapping(arg, "`assert-log` takes a mapping")?;
            let clean = inner.bool("clean")?;
            let allow = inner.patterns("allow")?;
            let contains = inner.patterns("contains")?;
            let lacks = inner.patterns("lacks")?;
            let count = match inner.take("count") {
                None => Vec::new(),
                Some(Value::Object(m)) => m
                    .iter()
                    .map(|(p, n)| {
                        Ok((
                            regex::Regex::new(p)
                                .with_context(|| format!("invalid `count` pattern '{p}'"))?,
                            LineCount::parse(n)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
                Some(v) => bail!("`count` maps a pattern to a number of lines, not {v}"),
            };
            inner.finish()?;
            ensure!(
                clean || !contains.is_empty() || !lacks.is_empty() || !count.is_empty(),
                "`assert-log` needs `clean: true`, `contains`, `lacks` or `count`"
            );
            let timing = opts.timing("a check")?;
            Action::AssertLog(LogStep {
                clean,
                allow,
                contains,
                lacks,
                count,
                timing,
            })
        }
        "slt" | "slt-setup" => {
            let mut inner = Opts::of_mapping(
                arg,
                &format!("`{name}` takes a mapping with `file` and `source`"),
            )?;
            let file = inner
                .string("file")?
                .with_context(|| format!("`{name}` needs a `file`"))?;
            let source = inner
                .string("source")?
                .with_context(|| format!("`{name}` needs a `source`"))?;
            let labels = inner.parse("labels", "`labels` must be a list of strings")?;
            let known_errors = inner
                .parse("known-errors", "`known-errors` must be a list of strings")?
                .unwrap_or_default();
            inner.finish()?;
            Action::Slt(SltStep {
                file,
                source,
                labels,
                setup: name == "slt-setup",
                known_errors,
            })
        }
        "assert-objects" => {
            let mut inner =
                Opts::of_mapping(arg, "`assert-objects` takes `source`, `prefix` and `min`")?;
            let source = inner
                .string("source")?
                .context("`assert-objects` needs a `source`")?;
            let prefix = inner.string("prefix")?.unwrap_or_default();
            let min = match inner.take("min") {
                None => 1,
                Some(v) => v
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .context("`min` must be a count")?,
            };
            inner.finish()?;
            Action::AssertObjects {
                source,
                prefix,
                min,
                within: opts.duration("eventually")?,
            }
        }
        "generate" => {
            let Value::Object(m) = arg else {
                bail!("`generate` takes a mapping of source name to table spec");
            };
            let (source, spec) = single_entry(m, "`generate` takes exactly one source")?;
            // Check the spec now when nothing in it waits on a placeholder.
            if !spec.to_string().contains("${") {
                serde_json::from_value::<crate::generate::TableSpec>(spec.clone())
                    .context("invalid table spec")?;
            }
            Action::Generate {
                source,
                spec,
                mirror: opts.string("mirror")?,
            }
        }
        "wait-metric" => {
            let mut inner = Opts::of_mapping(arg, "`wait-metric` takes a mapping with a `name`")?;
            let metric = inner
                .string("name")?
                .context("`wait-metric` needs a `name`")?;
            let labels = inner
                .parse("labels", "`labels` must map label names to values")?
                .unwrap_or_default();
            let number = |v: Value, key: &str| {
                v.as_f64()
                    .ok_or_else(|| anyhow!("`{key}` must be a number"))
            };
            let mut conditions = Vec::new();
            if let Some(v) = inner.take("above") {
                conditions.push(MetricCondition::Above(number(v, "above")?));
            }
            if let Some(v) = inner.take("below") {
                conditions.push(match v {
                    Value::Object(mut o) => MetricCondition::BelowMetric(
                        o.remove("metric")
                            .and_then(|m| m.as_str().map(str::to_string))
                            .ok_or_else(|| {
                                anyhow!("`below` takes a number or `{{ metric: name }}`")
                            })?,
                    ),
                    v => MetricCondition::Below(number(v, "below")?),
                });
            }
            if inner.bool("increases")? {
                conditions.push(MetricCondition::Increases);
            }
            if let Some(v) = inner.take("increased-by") {
                conditions.push(MetricCondition::IncreasedBy {
                    amount: number(v, "increased-by")?,
                    since: inner.string("since")?,
                });
            }
            let timeout = inner.duration("timeout")?.unwrap_or(Duration::from_mins(5));
            let drive = match inner.take("drive") {
                None => None,
                Some(Value::Object(d)) => {
                    let mut d = Opts(d);
                    let every = d.duration("every")?.unwrap_or(Duration::from_secs(2));
                    let rounds = match d.take("rounds") {
                        None => 60,
                        Some(v) => v
                            .as_u64()
                            .and_then(|n| u32::try_from(n).ok())
                            .context("`rounds` must be a positive integer")?,
                    };
                    let (source, sql) = single_entry(
                        std::mem::take(&mut d.0),
                        "`drive` takes one `<source>: <sql>` beside `every` and `rounds`",
                    )?;
                    let sql = sql
                        .as_str()
                        .context("`drive` SQL must be a string")?
                        .to_string();
                    Some(Drive {
                        source,
                        sql,
                        every,
                        rounds,
                    })
                }
                Some(_) => bail!("`drive` takes a mapping"),
            };
            inner.finish()?;
            let [condition] = conditions.try_into().map_err(|_| {
                anyhow!("`wait-metric` takes exactly one of `above`, `below`, `increases`, `increased-by`")
            })?;
            Action::WaitMetric(MetricWait {
                name: metric,
                labels,
                condition,
                timeout,
                drive,
            })
        }
        "mark-metrics" => Action::MarkMetrics(string_arg(&arg, name)?),
        "sleep" => Action::Sleep(parse_duration(&arg)?),
        _ => unreachable!("every name in ACTIONS is handled"),
    };
    opts.finish()?;
    Ok(Step {
        index,
        key,
        action,
        only,
        except,
        when,
        corpus: None,
    })
}

fn wait_ready(arg: &Value) -> Result<bool> {
    match arg.get("ready") {
        None => Ok(true),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => bail!("`ready` must be true or false"),
    }
}

/// A `patch:` on `start` or `restart`: the same shape as a variant.
fn patch(arg: &Value) -> Result<Option<VariantSpec>> {
    arg.get("patch")
        .map(|p| serde_json::from_value(p.clone()).context("invalid `patch`"))
        .transpose()
}

/// The one `key: value` entry of a mapping such as `<source>: <sql>`.
fn single_entry(map: Map<String, Value>, error: &str) -> Result<(String, Value)> {
    let [entry] = map
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| anyhow!("{error}"))?;
    Ok(entry)
}

/// A string or a list of strings, empty when absent; `expected` completes
/// "`<key>` must be …" for any other shape.
fn one_or_many(value: Option<Value>, key: &str, expected: &str) -> Result<Vec<String>> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(vec![s]),
        Some(v) => serde_json::from_value(v).with_context(|| format!("`{key}` must be {expected}")),
    }
}

fn compile_patterns(key: &str, patterns: &[String]) -> Result<Vec<regex::Regex>> {
    patterns
        .iter()
        .map(|p| regex::Regex::new(p).with_context(|| format!("invalid `{key}` pattern '{p}'")))
        .collect()
}

fn string_arg(arg: &Value, action: &str) -> Result<String> {
    arg.as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("`{action}` takes a string"))
}

fn parse_signal(v: Option<&Value>) -> Result<Option<KillSignal>> {
    match v.and_then(Value::as_str) {
        None => Ok(None),
        Some("KILL") => Ok(Some(KillSignal::Kill)),
        Some("STOP-then-KILL") => Ok(Some(KillSignal::StopThenKill)),
        Some(other) => bail!("unknown signal '{other}', expected KILL or STOP-then-KILL"),
    }
}

pub fn parse_duration(v: &Value) -> Result<Duration> {
    match v {
        Value::String(s) => {
            humantime::parse_duration(s).with_context(|| format!("bad duration '{s}'"))
        }
        Value::Number(n) => n
            .as_u64()
            .map(Duration::from_secs)
            .ok_or_else(|| anyhow!("bad duration {n}")),
        _ => bail!("a duration is a string like `30s`"),
    }
}

/// The sibling option keys of a step; [`Opts::finish`] rejects any left unread.
struct Opts(Map<String, Value>);

impl Opts {
    fn take(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }

    /// The `arg` of an action that takes a mapping of options.
    fn of_mapping(arg: Value, error: &str) -> Result<Self> {
        let Value::Object(m) = arg else {
            bail!("{error}");
        };
        Ok(Self(m))
    }

    /// An option deserialized as `T`; `error` describes the shape it must have.
    fn parse<T: DeserializeOwned>(&mut self, key: &str, error: &str) -> Result<Option<T>> {
        self.take(key)
            .map(serde_json::from_value)
            .transpose()
            .with_context(|| error.to_string())
    }

    fn protocols(&mut self) -> Result<Option<Vec<QueryProtocol>>> {
        let protocols: Option<Vec<QueryProtocol>> = self.parse(
            "protocols",
            "`protocols` is a list of flight, flightsql or http",
        )?;
        if let Some(protocols) = &protocols {
            ensure!(
                protocols.iter().any(|p| *p != QueryProtocol::Http),
                "`protocols` needs flight or flightsql to supply the result schema"
            );
            for (i, protocol) in protocols.iter().enumerate() {
                ensure!(
                    !protocols[..i].contains(protocol),
                    "`protocols` must not repeat {protocol:?}"
                );
            }
        }
        Ok(protocols)
    }

    /// A pattern or a list of patterns, compiled.
    fn patterns(&mut self, key: &str) -> Result<Vec<regex::Regex>> {
        let patterns = one_or_many(self.take(key), key, "a pattern or a list of patterns")?;
        compile_patterns(key, &patterns)
    }

    fn whole_number(&mut self, key: &str, default: u64) -> Result<u64> {
        self.take(key).map_or(Ok(default), |v| {
            serde_json::from_value(v).context("must be a whole number")
        })
    }

    fn list(&mut self, key: &str) -> Result<Vec<String>> {
        Ok(self
            .parse(key, &format!("`{key}` must be a list"))?
            .unwrap_or_default())
    }

    /// `eventually: <duration>` or `consistently: <duration>`, or neither;
    /// `what` names the step in the error for both.
    fn timing(&mut self, what: &str) -> Result<Timing> {
        match (self.duration("eventually")?, self.duration("consistently")?) {
            (None, None) => Ok(Timing::Once),
            (Some(d), None) => Ok(Timing::Eventually(d)),
            (None, Some(d)) => Ok(Timing::Consistently(d)),
            (Some(_), Some(_)) => bail!("{what} is either `eventually` or `consistently`"),
        }
    }

    /// `expect-error`: `true`, or the text the error must contain.
    fn expected_error(&mut self, key: &str) -> Result<(bool, Option<String>)> {
        match self.take(key) {
            None => Ok((false, None)),
            Some(Value::Bool(b)) => Ok((b, None)),
            Some(Value::String(text)) => Ok((true, Some(text))),
            Some(_) => bail!("`{key}` must be true, false, or the text the error must contain"),
        }
    }

    fn bool(&mut self, key: &str) -> Result<bool> {
        match self.take(key) {
            None => Ok(false),
            Some(Value::Bool(b)) => Ok(b),
            Some(_) => bail!("`{key}` must be true or false"),
        }
    }

    fn string(&mut self, key: &str) -> Result<Option<String>> {
        match self.take(key) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(_) => bail!("`{key}` must be a string"),
        }
    }

    fn duration(&mut self, key: &str) -> Result<Option<Duration>> {
        self.take(key).as_ref().map(parse_duration).transpose()
    }

    /// A duration given either inside the action's own mapping or as a sibling.
    fn duration_in(&mut self, arg: &Value, key: &str) -> Result<Option<Duration>> {
        match arg.get(key) {
            Some(v) => parse_duration(v).map(Some),
            None => self.duration(key),
        }
    }

    fn finish(self) -> Result<()> {
        if let Some(key) = self.0.keys().next() {
            bail!("unknown option `{key}`");
        }
        Ok(())
    }
}

mod opt_duration {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer};
    use serde_json::Value;

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Option::<Value>::deserialize(d)?
            .map(|v| super::parse_duration(&v).map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// Where a directory's scripts record their run times, in whole seconds by
/// file name.
pub const DURATIONS_FILE: &str = "recordings/durations.json";

/// The recorded run times of the scripts in `dir`; none if it has no file.
fn read_durations(dir: &Path) -> Result<std::collections::BTreeMap<String, u64>> {
    let path = dir.join(DURATIONS_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            serde_json::from_str(&text).with_context(|| format!("Invalid {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(std::collections::BTreeMap::new()),
        Err(e) => Err(e).with_context(|| format!("Failed to read {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_keys_sort_nested_objects() {
        let arg: Value =
            serde_json::from_str(r#"{"table":"t","seed":1,"options":{"z":2,"a":1},"queries":200}"#)
                .expect("parse generator arguments");
        assert_eq!(
            step_key("tlp", &arg),
            r#"tlp: {"options":{"a":1,"z":2},"queries":200,"seed":1,"table":"t"}"#,
        );
    }

    #[test]
    fn query_protocols_keep_prepared_statements_on_flightsql() {
        let script = |options: &str| {
            Script::parse(&format!(
                "name: protocols\nspicepod: {{}}\nsteps:\n  - query: SELECT 1\n    {options}"
            ))
        };
        let parsed = script("protocols: [flightsql, http]").expect("query protocols");
        let Action::Query(query) = &parsed.steps[0].action else {
            panic!("expected query");
        };
        assert_eq!(
            query.protocols.as_deref(),
            Some([QueryProtocol::FlightSql, QueryProtocol::Http].as_slice())
        );
        script("parameters: []\n    protocols: [flightsql]").expect("prepared Flight SQL");
        script("parameters: []\n    protocols: [flightsql, http]")
            .expect_err("prepared statements cannot compare HTTP");
        script("protocols: [http]").expect_err("HTTP alone supplies no Arrow schema");
    }

    #[test]
    fn random_query_protocols() {
        let script = |options: &str| {
            Script::parse(&format!(
                "name: protocols\nspicepod: {{}}\nsteps:\n  - random-queries: {{ tables: [t], {options} }}"
            ))
        };
        let legacy = script("oracle: ref").expect("legacy random query script");
        let Action::RandomQueries(step) = &legacy.steps[0].action else {
            panic!("expected random queries");
        };
        assert_eq!(step.protocols, [QueryProtocol::Flight]);
        assert_eq!(step.oracle.as_deref(), Some("ref"));
        let differential = script("protocols: [flightsql, http]")
            .expect("protocol comparison needs no external oracle");
        let Action::RandomQueries(step) = &differential.steps[0].action else {
            panic!("expected random queries");
        };
        assert_eq!(
            step.protocols,
            [QueryProtocol::FlightSql, QueryProtocol::Http]
        );
        for options in [
            "protocols: []",
            "protocols: [http]",
            "protocols: [flightsql]",
            "protocols: [flightsql, flightsql]",
            "protocols: [flightsql, adbc]",
        ] {
            assert!(script(options).is_err(), "must reject {options}");
        }
    }

    #[test]
    fn parses_steps_with_sibling_options() {
        let script = Script::parse(
            r"
name: t
sources: { src: { kind: postgres } }
variants: { a: {}, b: { datasets: { '*': { acceleration: { engine: cayenne } } } } }
spicepod: { version: v1, kind: Spicepod, name: t }
steps:
  - source-exec: { src: 'CREATE TABLE t (id int)' }
  - start: {}
  - query: SELECT count(*) FROM t
    eventually: 30s
    expect: { oracle: src }
  - refresh: { dataset: t, expect-error: true }
    only: b
  - kill-spiced: { signal: STOP-then-KILL }
",
        )
        .expect("script parses");
        assert_eq!(script.steps.len(), 5);
        let Action::Query(q) = &script.steps[2].action else {
            panic!("step 3 should be a query, got {:?}", script.steps[2].action);
        };
        assert!(matches!(q.timing, Timing::Eventually(d) if d == Duration::from_secs(30)));
        assert_eq!(
            q.expect.as_ref().and_then(|e| e.oracle.as_deref()),
            Some("src")
        );
        assert!(matches!(
            &script.steps[3].action,
            Action::Refresh { dataset, expect_error: true, .. } if dataset == "t"
        ));
        assert_eq!(script.steps[3].only, vec!["b".to_string()]);
        assert!(matches!(
            script.steps[4].action,
            Action::Kill {
                signal: KillSignal::StopThenKill
            }
        ));
    }

    #[test]
    fn a_corpus_marks_its_queries_with_its_position() {
        let suite = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test/spicescript");
        let script = Script::parse_in(
            "name: c\nspicepod: {}\nsteps:\n  - start: {}\n  - corpus: corpus/layout.sql\n    eventually: 30s\n  - query: SELECT 1\n  - corpus: corpus/layout.sql\n",
            &suite,
            &SharedVariants::default(),
        )
        .expect("script parses");
        let corpora: Vec<Option<usize>> = script.steps.iter().map(|s| s.corpus).collect();
        let first = corpora.iter().filter(|c| **c == Some(1)).count();
        let second = corpora.iter().filter(|c| **c == Some(3)).count();
        assert!(first > 1, "{corpora:?}");
        assert_eq!(first, second, "{corpora:?}");
        assert_eq!(corpora[0], None, "start");
        assert_eq!(corpora[1 + first], None, "the query between the corpora");
        assert_eq!(corpora.len(), 2 + 2 * first);
    }

    #[test]
    fn shared_variants_come_from_the_nearest_variants_file() {
        // A known-failing group's script uses the suite's file, two levels up.
        let group = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../test/spicescript/known-failing/cayenne");
        let shared = SharedVariants::find(&group).expect("the suite's variants file parses");
        let script = Script::parse_in(
            "name: p\nspicepod: {}\nvariants: { federated: federated, cayenne: cayenne-file, arrow: arrow, own: { env: { A: b } } }\nsteps: [ { start: {} } ]",
            &group,
            &shared,
        )
        .expect("shared variants parse");
        assert!(script.variants["federated"].datasets.is_empty());
        assert_eq!(
            script.variants["cayenne"].datasets["*"],
            serde_json::json!({ "acceleration": { "enabled": true, "engine": "cayenne", "mode": "file" } })
        );
        assert_eq!(
            script.variants["arrow"].datasets["*"],
            serde_json::json!({ "acceleration": { "enabled": true, "engine": "arrow" } })
        );
        assert_eq!(script.variants["own"].env["A"], "b");
        let err = Script::parse_in(
            "name: p\nspicepod: {}\nvariants: { x: cayenne-disk }\nsteps: [ { start: {} } ]",
            &group,
            &shared,
        )
        .expect_err("a name the file does not define is refused");
        assert!(
            format!("{err:#}").contains("'cayenne-disk' is not defined in")
                && format!("{err:#}").contains("test/spicescript/variants.yaml"),
            "{err:#}"
        );
        let err = Script::parse(
            "name: p\nspicepod: {}\nvariants: { x: arrow }\nsteps: [ { start: {} } ]",
        )
        .expect_err("a name without a variants file is refused");
        assert!(
            format!("{err:#}").contains("there is no variants.yaml"),
            "{err:#}"
        );
    }

    #[test]
    fn a_selection_keeps_matrix_values_and_variants_and_records_the_rest() {
        let text = "name: s\nmatrix: { src: [postgres, clickhouse], n: [1, 2] }\nvariants: { a: {}, b: {}, c: {} }\nspicepod: {}\nsteps: [ { start: {} } ]";
        let mut script = Script::parse(text).expect("parses");
        let selection = Selection {
            matrix: BTreeMap::from([("src".to_string(), vec!["clickhouse".to_string()])]),
            variants: vec!["b".into(), "c".into()],
        };
        assert!(script.select(&selection));
        assert_eq!(
            script.run_labels(),
            ["s[n=1,src=clickhouse]", "s[n=2,src=clickhouse]"]
        );
        assert_eq!(script.variants.keys().collect::<Vec<_>>(), ["b", "c"]);
        assert_eq!(
            script.left_out.runs.iter().collect::<Vec<_>>(),
            ["s[n=1,src=postgres]", "s[n=2,src=postgres]"]
        );
        assert!(script.left_out.covers("s[n=1,src=clickhouse]", "a"));
        assert!(script.left_out.covers("s[n=1,src=clickhouse]", "trunk/a"));
        assert!(script.left_out.covers("s[n=1,src=clickhouse]", "variants"));
        assert!(script.left_out.covers("s[n=1,src=postgres]", "b"));
        assert!(!script.left_out.covers("s[n=1,src=clickhouse]", "b"));
        assert!(!script.left_out.covers("s[n=1,src=clickhouse]", ""));
        // A number is selected as it prints. Labels list keys alphabetically.
        let mut script = Script::parse(text).expect("parses");
        let by_number = Selection {
            matrix: BTreeMap::from([("n".to_string(), vec!["2".to_string()])]),
            variants: Vec::new(),
        };
        assert!(script.select(&by_number));
        assert_eq!(
            script.run_labels(),
            ["s[n=2,src=postgres]", "s[n=2,src=clickhouse]"]
        );
        assert!(!script.left_out.covers("s[n=2,src=postgres]", "variants"));
        // A script without the key, value or any variant is not run.
        for selection in [
            Selection {
                matrix: BTreeMap::from([("mode".to_string(), vec!["file".to_string()])]),
                variants: Vec::new(),
            },
            Selection {
                matrix: BTreeMap::from([("src".to_string(), vec!["mysql".to_string()])]),
                variants: Vec::new(),
            },
            Selection {
                matrix: BTreeMap::new(),
                variants: vec!["d".into()],
            },
        ] {
            let mut script = Script::parse(text).expect("parses");
            assert!(!script.select(&selection), "{}", selection.describe());
        }
    }

    #[test]
    fn a_shared_variant_must_be_a_valid_patch() {
        for (text, message) in [
            ("a: cayenne-file\n", "variant 'a' must be a patch"),
            ("a: { datasets: [] }\n", "variant 'a' is not a valid patch"),
            ("a: { dataset: {} }\n", "variant 'a' is not a valid patch"),
        ] {
            let err = SharedVariants::parse(text, "variants.yaml".into())
                .expect_err("an invalid entry is refused");
            assert!(format!("{err:#}").contains(message), "{text}: {err:#}");
        }
    }

    #[test]
    fn rejects_unknown_options_and_ambiguous_steps() {
        let base = "name: t\nspicepod: {}\nsteps:\n";
        let err = Script::parse(&format!("{base}  - query: SELECT 1\n    eventualy: 3s\n"))
            .expect_err("typo must be rejected");
        assert!(
            format!("{err:#}").contains("unknown option `eventualy`"),
            "{err:#}"
        );
        let err = Script::parse(&format!("{base}  - start: {{}}\n    stop: {{}}\n"))
            .expect_err("two actions must be rejected");
        assert!(
            format!("{err:#}").contains("exactly one action key"),
            "{err:#}"
        );
    }
}
