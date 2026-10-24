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

//! Runs a script: once per matrix combination, on every *instance* — a
//! `spiced` binary (a target) crossed with a variant.
//!
//! Instances run either in lockstep (every instance finishes a step before
//! any starts the next) or one after another. Either way each query's final
//! answer is kept, and after the run every instance's answer is compared with
//! the first instance's, so variants and targets must agree even where the
//! script names no oracle.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};

use arrow::array::RecordBatch;

use indexmap::IndexMap;

use serde_json::{Map, Value, json};

use crate::{
    compare,
    script::{Action, RunVariants, Script, Step, Timing, VariantSpec},
    source::{self, Source},
    spiced::Spiced,
    template,
};

tokio::task_local! {
    /// Under `--jobs`, a script's output, printed whole when it finishes so
    /// that scripts running side by side do not interleave their lines.
    static OUTPUT: std::cell::RefCell<String>;
}

/// Writes a line of a script's output: to its buffer under `--jobs`, else
/// straight out.
pub(crate) fn emit(line: &str) {
    let buffered = OUTPUT.try_with(|out| {
        let mut out = out.borrow_mut();
        out.push_str(line);
        out.push('\n');
    });
    if buffered.is_err() {
        println!("{line}");
    }
}

macro_rules! say {
    ($($arg:tt)*) => { $crate::runner::emit(&format!($($arg)*)) };
}

mod api;
mod background;
mod checks;
mod cluster;
mod generators;
mod query;
mod steps;

use api::{http_step, refresh, source_query, spice_cli};
use background::{start_background, stop_background};
use checks::{assert_files, assert_log, assert_status, metric_value, wait_metric};
use cluster::{
    cluster_args, make_cluster_ready, start_executors, undistributed_engine,
    unpartitionable_acceleration,
};
use generators::{random_queries, run_slt, tlp};
use query::{check_error_text, explain, run_query};
use steps::{describe, one_line, run_instance_step, run_source_step};

/// Runs `work` with its output collected instead of printed, and returns
/// both.
async fn buffered<T>(work: impl std::future::Future<Output = T>) -> (T, String) {
    OUTPUT
        .scope(std::cell::RefCell::new(String::new()), async {
            let value = work.await;
            let out = OUTPUT.with(std::cell::RefCell::take);
            (value, out)
        })
        .await
}

/// Runs a script with its output collected instead of printed, and returns
/// both.
pub async fn run_script_buffered(
    script: &Script,
    options: &Options,
    slots: &Slots,
) -> (Result<Report>, String) {
    buffered(run_script(script, options, slots)).await
}

/// The `--jobs` budget: one slot per `spiced` process running at once.
/// Waiters are served in the order they asked, so the longest scripts, which
/// ask first, start first.
pub struct Slots {
    semaphore: Arc<tokio::sync::Semaphore>,
    size: u32,
}

impl Slots {
    pub fn new(size: usize) -> Self {
        let size = u32::try_from(size.max(1)).unwrap_or(u32::MAX);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(size as usize)),
            size,
        }
    }

    /// Waits for `n` slots (all of them when `n` is more than there are).
    async fn take(&self, n: u32) -> Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.semaphore)
            .acquire_many_owned(n.clamp(1, self.size))
            .await
            .context("the --jobs budget was closed")
    }
}

/// The `spiced` processes one instance of `script` runs at once: itself, and
/// its executors on a cluster.
fn instance_weight(script: &Script, options: &Options) -> u32 {
    let cluster = (options.cluster || script.defaults.cluster.is_some())
        && script.defaults.executor_args.is_none();
    let processes = if cluster {
        1 + script.defaults.cluster.as_ref().map_or(1, |c| c.executors)
    } else {
        1 + usize::from(script.defaults.executor_args.is_some())
    };
    u32::try_from(processes).unwrap_or(u32::MAX)
}

/// How a combination's instances run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Schedule {
    /// Each instance is a job of its own, holding its slots while it runs.
    Independent,
    /// Every instance a step at a time, for scripts whose variants work
    /// together or that stop and start a source all instances share.
    Lockstep,
    /// Alone, one instance after another, holding every slot.
    Exclusive,
}

fn schedule(script: &Script, steps: &[Step]) -> Schedule {
    if script.defaults.exclusive {
        Schedule::Exclusive
    } else if script.defaults.run_variants == RunVariants::SideBySide
        || steps.iter().any(|s| {
            matches!(
                s.action,
                Action::StartSource(_)
                    | Action::StopSource(_)
                    | Action::PauseSource(_)
                    | Action::UnpauseSource(_)
            )
        })
    {
        Schedule::Lockstep
    } else {
        Schedule::Independent
    }
}

const DEFAULT_READY_TIMEOUT: Duration = Duration::from_mins(3);

/// `--max-wait`: the longest any wait for a condition may take, whatever the
/// script asks for. Scripts that are expected to fail pay every wait in full,
/// so a run of them is bounded by this instead.
static MAX_WAIT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();

/// Sets `--max-wait` for this run.
pub fn set_max_wait(limit: Duration) {
    let _ = MAX_WAIT.set(limit);
}

/// A wait's limit, capped by `--max-wait`.
fn max_wait(limit: Duration) -> Duration {
    MAX_WAIT.get().map_or(limit, |cap| limit.min(*cap))
}

/// Whether a check's attempt under `timing`, retried since `started`, is its
/// outcome: `None` to try again.
fn settled<T>(timing: Timing, started: Instant, attempt: Result<T>) -> Option<Result<T>> {
    match (timing, attempt) {
        (Timing::Once, result) => Some(result),
        (Timing::Eventually(_), Ok(seen)) => Some(Ok(seen)),
        (Timing::Eventually(limit), Err(e)) if started.elapsed() >= max_wait(limit) => Some(Err(e)),
        (Timing::Consistently(_), Err(e)) => Some(Err(e)),
        (Timing::Consistently(limit), Ok(seen)) if started.elapsed() >= limit => Some(Ok(seen)),
        _ => None,
    }
}

const POLL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone)]
pub struct Target {
    pub name: String,
    pub binary: PathBuf,
}

pub struct Options {
    pub targets: Vec<Target>,
    /// The binary every instance starts on; an `upgrade` step moves it to
    /// its target's.
    pub from: Option<PathBuf>,
    /// Run every instance as a cluster scheduler with one executor.
    pub cluster: bool,
    pub work_dir: PathBuf,
    pub keep: bool,
    pub spice_cli: PathBuf,
}

/// A failed check: where it ran, which step, and what went wrong.
#[derive(Debug, Clone)]
pub struct Failure {
    /// The script and matrix combination, as `name[key=value,…]`.
    pub run: String,
    /// The instance, `variants` for a comparison between them, or empty for
    /// the combination as a whole.
    pub who: String,
    pub step: usize,
    /// The step's ledger key; for a failure outside any step, its kind.
    pub key: String,
    pub what: String,
    /// Within a generator step, the generated query's number.
    pub query: Option<u64>,
    /// Within a generator step, the known-defect shapes the query contains.
    pub labels: Vec<String>,
    pub message: String,
}

impl Failure {
    /// A failure outside any step: starting sources or instances.
    fn outside_steps(run: &str, who: &str, message: &str) -> Self {
        Self {
            run: run.to_string(),
            who: who.to_string(),
            step: 0,
            key: "setup".into(),
            what: "setup".into(),
            query: None,
            labels: Vec::new(),
            message: message.to_string(),
        }
    }

    /// Whether this failure is outside any step; steps are numbered from 1.
    pub fn is_outside_steps(&self) -> bool {
        self.step == 0 && self.what == "setup"
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let who = if self.who.is_empty() {
            String::new()
        } else {
            format!(" {}", self.who)
        };
        let query = self
            .query
            .map(|q| format!(" query {q}"))
            .unwrap_or_default();
        write!(
            f,
            "{}{who} step {} {}{query}\n{}",
            self.run, self.step, self.what, self.message
        )
    }
}

/// A generator step's failed queries, each a failure of its own.
#[derive(Debug)]
pub(super) struct QueryFailures {
    pub summary: String,
    pub failed: Vec<QueryFailure>,
}

#[derive(Debug)]
pub(super) struct QueryFailure {
    pub number: u64,
    pub text: String,
    pub problem: String,
    pub labels: Vec<String>,
}

impl std::fmt::Display for QueryFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.summary)
    }
}

impl std::error::Error for QueryFailures {}

#[derive(Default)]
pub struct Report {
    pub passed: usize,
    pub failures: Vec<Failure>,
    /// When the script first held slots: its run time starts here, not
    /// while it waited for them.
    pub started: Option<Instant>,
}

struct Instance {
    label: String,
    variant: String,
    spiced: Spiced,
    /// The binary under test, which an `upgrade` step starts.
    target_binary: PathBuf,
    /// The cluster executors started beside `spiced` (by `--cluster`,
    /// `defaults.cluster` or `defaults.executor-args`), spares last.
    executors: Vec<Spiced>,
    /// How many of `executors` the cluster runs; the rest are spares that
    /// `add-executor` starts one at a time.
    active_executors: usize,
    spicepod_yaml: String,
    /// The rendered spicepod, which `patch:` on a start changes.
    spicepod: Value,
    /// The namespace this instance owns in each source.
    namespaces: BTreeMap<String, String>,
    /// Set when a lifecycle step failed; later steps are skipped.
    broken: Option<String>,
    /// The final answer of each query step, by step index.
    answers: BTreeMap<usize, Vec<RecordBatch>>,
    /// Answers of queries that carry an `id`.
    named: BTreeMap<String, Vec<RecordBatch>>,
    /// What `${…}` placeholders resolve to, in the spicepod and in step SQL.
    context: Value,
    /// The `spice` CLI that `spice` steps run.
    spice_cli: PathBuf,
    /// Generated inputs shared across runs (TPC-H files).
    cache_dir: PathBuf,
    /// Work running in the background, by id, until a `stop-background`.
    background: BTreeMap<String, Background>,
    /// `/metrics` scrapes taken by `mark-metrics`, by id.
    metric_marks: BTreeMap<String, String>,
}

/// How long `spiced` gets to shut down after SIGTERM before `stop` and
/// `restart` count the shutdown as failed.
/// How long `stop` and `restart` steps give `spiced` to exit on SIGTERM: more
/// than its own 30 s for draining connections, which clients still sending
/// requests (a background reader) use in full.
const STOP_TIMEOUT: Duration = Duration::from_secs(45);

/// How long a finished script's instances get to stop on SIGTERM before they
/// are killed. Shutdown itself is checked by `stop` and `restart` steps, at
/// `STOP_TIMEOUT`; at the end of a script it only costs time.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The partition assignment interval a cluster runs with (see
/// `make_cluster_ready`).
const CLUSTER_ASSIGNMENT_INTERVAL: &str = "2s";

/// A background task and what it has seen so far.
struct Background {
    stop: Arc<std::sync::atomic::AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    stats: Arc<parking_lot::Mutex<BackgroundStats>>,
}

#[derive(Default)]
struct BackgroundStats {
    iterations: u64,
    latencies: Vec<Duration>,
    /// Every failure's message, in order.
    errors: Vec<String>,
}

impl Instance {
    fn sql(&self, text: &str) -> Result<String> {
        template::substitute_text(text, &self.context)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourceState {
    Running,
    Stopped,
    Paused,
}

struct Sources {
    by_name: IndexMap<String, Arc<dyn Source>>,
    state: BTreeMap<String, SourceState>,
    /// Each source's resolved kind (`postgres`, `mysql`).
    kinds: BTreeMap<String, String>,
}

impl Sources {
    fn get(&self, name: &str) -> Result<&dyn Source> {
        self.by_name
            .get(name)
            .map(AsRef::as_ref)
            .ok_or_else(|| anyhow!("unknown source '{name}'"))
    }

    async fn set(&mut self, name: &str, target: SourceState) -> Result<()> {
        let current = *self
            .state
            .get(name)
            .ok_or_else(|| anyhow!("unknown source '{name}'"))?;
        if current == target {
            return Ok(());
        }
        let container = self
            .get(name)?
            .container()
            .ok_or_else(|| anyhow!("source '{name}' has no server to stop, start or pause"))?;
        match (current, target) {
            (a, b) if a == b => {}
            (SourceState::Running, SourceState::Stopped) => container.stop().await?,
            (SourceState::Running, SourceState::Paused) => container.pause().await?,
            (SourceState::Stopped, SourceState::Running) => container.resume().await?,
            (SourceState::Paused, SourceState::Running) => container.unpause().await?,
            (SourceState::Paused, SourceState::Stopped) => {
                container.unpause().await?;
                container.stop().await?;
            }
            (SourceState::Stopped, SourceState::Paused) => {
                bail!("source '{name}' is stopped; start it before pausing it")
            }
            _ => unreachable!("every pair of states is handled"),
        }
        self.state.insert(name.to_string(), target);
        Ok(())
    }
}

pub async fn run_script(script: &Script, options: &Options, slots: &Slots) -> Result<Report> {
    let runs = matrix_combinations(&script.matrix)
        .into_iter()
        .enumerate()
        .map(|(combo_index, combo)| async move {
            let combo_label = combo
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{k}={}",
                        v.as_str().map_or_else(|| v.to_string(), str::to_string)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let run_label = if combo_label.is_empty() {
                script.name.clone()
            } else {
                format!("{}[{combo_label}]", script.name)
            };
            let run_id = format!(
                "{}-{combo_index}-{}",
                sanitize(&script.name),
                std::process::id()
            );
            buffered(async {
                match &script.description {
                    Some(d) => say!("== {run_label}: {}", one_line(d, 120)),
                    None => say!("== {run_label}"),
                }
                let started = Instant::now();
                let mut report = Report::default();
                if let Err(e) = run_combination(
                    script,
                    options,
                    slots,
                    &combo,
                    &run_id,
                    &run_label,
                    &mut report,
                )
                .await
                {
                    report
                        .failures
                        .push(Failure::outside_steps(&run_label, "", &format!("{e:#}")));
                }
                say!(
                    "== {run_label} finished in {:.1}s",
                    started.elapsed().as_secs_f64()
                );
                report
            })
            .await
        });
    // Combinations run side by side as slots allow; their output keeps
    // their order.
    let mut report = Report::default();
    for (part, out) in futures::future::join_all(runs).await {
        say!("{}", out.trim_end_matches('\n'));
        report.passed += part.passed;
        report.failures.extend(part.failures);
        report.started = match (report.started, part.started) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    Ok(report)
}

async fn run_combination(
    script: &Script,
    options: &Options,
    slots: &Slots,
    combo: &Map<String, Value>,
    run_id: &str,
    run_label: &str,
    report: &mut Report,
) -> Result<()> {
    // The steps of this matrix combination.
    let steps: Vec<Step> = script
        .steps
        .iter()
        .filter(|s| when_matches(&s.when, combo))
        .cloned()
        .collect();
    let schedule = schedule(script, &steps);
    let weight = instance_weight(script, options);
    let instance_count =
        u32::try_from(options.targets.len() * script.variants.len().max(1)).unwrap_or(u32::MAX);
    // Sources start only once the first instance has its slots, so waiting
    // combinations hold no containers.
    let first_permit = slots
        .take(match schedule {
            Schedule::Independent => weight,
            Schedule::Lockstep => weight.saturating_mul(instance_count),
            Schedule::Exclusive => u32::MAX,
        })
        .await?;
    report.started = Some(
        report
            .started
            .map_or_else(Instant::now, |t| t.min(Instant::now())),
    );
    let mut sources = Sources {
        by_name: IndexMap::new(),
        state: BTreeMap::new(),
        kinds: BTreeMap::new(),
    };
    let matrix_ctx = json!({ "matrix": combo });
    for (name, spec) in &script.sources {
        let kind = template::substitute_text(&spec.kind, &matrix_ctx)?;
        let version = spec
            .version
            .as_deref()
            .map(|v| template::substitute_text(v, &matrix_ctx))
            .transpose()?;
        let src = source::start(
            name,
            run_id,
            &kind,
            version.as_deref(),
            &options.work_dir.join(run_id).join("sources"),
            spec,
        )
        .await
        .with_context(|| format!("Failed to start source '{name}' ({kind})"))?;
        sources.by_name.insert(name.clone(), Arc::from(src));
        sources.state.insert(name.clone(), SourceState::Running);
        sources.kinds.insert(name.clone(), kind);
    }

    let result = async {
        let mut instances = create_instances(script, options, combo, run_id, &sources).await?;
        match schedule {
            Schedule::Lockstep => {
                run_lockstep(
                    script,
                    &steps,
                    &mut instances,
                    &mut sources,
                    report,
                    run_label,
                )
                .await;
            }
            Schedule::Exclusive => {
                for instance in &mut instances {
                    for name in script.sources.keys() {
                        sources.set(name, SourceState::Running).await?;
                    }
                    run_sequential(script, &steps, instance, &mut sources, report, run_label).await;
                }
            }
            Schedule::Independent => {
                run_independent(
                    script,
                    &steps,
                    &mut instances,
                    &sources,
                    slots,
                    weight,
                    first_permit,
                    report,
                    run_label,
                )
                .await;
            }
        }
        if script.defaults.compare_variants {
            let all: Vec<&Instance> = instances.iter().collect();
            compare_instances(
                &steps,
                &all,
                script.defaults.compare_to.as_deref(),
                report,
                run_label,
            )?;
        } else if options.targets.len() > 1 {
            // Variants may differ from one another, but each answers the same
            // on every binary.
            let mut by_variant: IndexMap<&str, Vec<&Instance>> = IndexMap::new();
            for instance in &instances {
                by_variant
                    .entry(instance.variant.as_str())
                    .or_default()
                    .push(instance);
            }
            for group in by_variant.values() {
                compare_instances(&steps, group, None, report, run_label)?;
            }
        }
        // Every process of every instance at once.
        let mut stopping = Vec::new();
        for instance in &mut instances {
            stopping.push(instance.spiced.stop(TEARDOWN_TIMEOUT));
            for executor in &mut instance.executors {
                stopping.push(executor.stop(TEARDOWN_TIMEOUT));
            }
        }
        let _ = futures::future::join_all(stopping).await;
        for instance in &mut instances {
            if !instance.answers.is_empty() || instance.broken.is_some() {
                say!(
                    "   {} log: {}",
                    instance.label,
                    instance.spiced.log_path().display()
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;

    if !options.keep {
        for src in sources.by_name.values() {
            if let Err(e) = src.remove().await {
                say!("warning: {e:#}");
            }
        }
    }
    result
}

async fn create_instances(
    script: &Script,
    options: &Options,
    combo: &Map<String, Value>,
    run_id: &str,
    sources: &Sources,
) -> Result<Vec<Instance>> {
    let default_variant = IndexMap::from([("default".to_string(), VariantSpec::default())]);
    let variants = if script.variants.is_empty() {
        &default_variant
    } else {
        &script.variants
    };
    let mut instances = Vec::new();
    for target in &options.targets {
        for (variant_name, variant) in variants {
            if !when_matches(&variant.when, combo) {
                continue;
            }
            let index = instances.len();
            let label = if options.targets.len() > 1 {
                format!("{}/{variant_name}", target.name)
            } else {
                variant_name.clone()
            };
            let dir = options.work_dir.join(run_id).join(sanitize(&label));
            let mut namespaces = BTreeMap::new();
            let mut source_ctx = Map::new();
            for (name, src) in &sources.by_name {
                let namespace = format!("i{index}_{}", sanitize(variant_name));
                let created = src.create_namespace(&namespace).await?;
                let mut entry = created.extra;
                entry.insert("namespace".into(), json!(namespace));
                let kind = sources.kinds.get(name).map_or("", String::as_str);
                let (from_prefix, from_suffix, file_based) = table_from(kind, &entry);
                let mut table_params = created.params.clone();
                if file_based && let Value::Object(map) = &mut table_params {
                    map.entry("file_format").or_insert_with(|| json!("parquet"));
                }
                entry.insert("params".into(), created.params);
                entry.insert("table_params".into(), table_params);
                entry.insert("from_prefix".into(), json!(from_prefix));
                entry.insert("from_suffix".into(), json!(from_suffix));
                source_ctx.insert(name.clone(), Value::Object(entry));
                namespaces.insert(name.clone(), namespace);
            }
            let context = json!({
                "sources": source_ctx,
                "matrix": combo,
                "variant": { "name": variant_name },
                "instance": { "name": label, "dir": dir.display().to_string(), "port": crate::docker::free_port()?,
                    "executor_port": crate::docker::free_port()?, "http_port": crate::docker::free_port()? },
                "script": { "dir": std::path::absolute(&script.dir)?.display().to_string() },
            });
            let mut spicepod = script.spicepod.clone();
            template::apply_variant(&mut spicepod, variant)
                .with_context(|| format!("variant '{variant_name}'"))?;
            let mut spicepod = template::substitute(&spicepod, &context)?;
            disable_results_cache_unless_configured(&mut spicepod);
            let cluster = (options.cluster || script.defaults.cluster.is_some())
                && script.defaults.executor_args.is_none();
            if cluster {
                if let Some(engine) = undistributed_engine(&spicepod) {
                    // A cluster accelerates only into Cayenne and Arrow.
                    say!("   skip {label:<20} the '{engine}' engine does not run in a cluster");
                    continue;
                }
                if let Some(reason) = unpartitionable_acceleration(&spicepod) {
                    say!("   skip {label:<20} {reason}");
                    continue;
                }
                make_cluster_ready(&mut spicepod, &dir, &script.defaults.cluster_partition_by);
            }
            let context_for_steps = context.clone();
            let spicepod_yaml = yaml::to_string(&spicepod)?;
            let spicepod = spicepod.clone();
            let mut env = variant.env.clone();
            // Spice derives replication slot names from this, so instances
            // sharing a source do not take each other's slots.
            env.insert("SPICE_INSTANCE_ID".into(), format!("{run_id}-{index}"));
            let first_binary = options
                .from
                .clone()
                .unwrap_or_else(|| target.binary.clone());
            let executor_count = script.defaults.cluster.as_ref().map_or(1, |c| c.executors);
            let spare_count = script.defaults.cluster.as_ref().map_or(0, |c| c.spare);
            let cluster_args = if cluster {
                Some(cluster_args(
                    script,
                    &context,
                    executor_count + spare_count,
                )?)
            } else {
                None
            };
            let executor_args: Vec<Value> = match &cluster_args {
                Some((_, executors)) => executors.iter().map(|a| json!(a)).collect(),
                None => script
                    .defaults
                    .executor_args
                    .iter()
                    .map(|a| json!(a))
                    .collect(),
            };
            let mut executors = Vec::new();
            for (n, args) in executor_args.iter().enumerate() {
                let name = if executor_args.len() == 1 {
                    "executor".to_string()
                } else {
                    format!("executor-{n}")
                };
                // Each process its own identity: two live instances sharing one
                // at a state location are refused.
                let mut executor_env = env.clone();
                executor_env.insert(
                    "SPICE_INSTANCE_ID".into(),
                    format!("{run_id}-{index}-{name}"),
                );
                let mut executor =
                    Spiced::new(first_binary.clone(), dir.join(&name), executor_env)?;
                let args = template::substitute(args, &context)?;
                executor.set_extra_args(serde_json::from_value(args)?);
                executor.set_api_key(script.defaults.api_key.clone())?;
                executors.push(executor);
            }
            let mut spiced = Spiced::new(first_binary, dir, env)?;
            if let Some(port) = context["instance"]["http_port"]
                .as_u64()
                .and_then(|p| u16::try_from(p).ok())
            {
                spiced.set_http_port(port);
            }
            spiced.set_api_key(script.defaults.api_key.clone())?;
            spiced.set_tls(script.defaults.tls.as_ref().map(|t| script.dir.join(&t.ca)))?;
            let args = template::substitute(&json!(script.defaults.spiced_args), &context)?;
            let mut args: Vec<String> = serde_json::from_value(args)?;
            if let Some((scheduler, _)) = &cluster_args {
                args.extend(scheduler.iter().cloned());
            }
            spiced.set_extra_args(args);
            instances.push(Instance {
                label,
                variant: variant_name.clone(),
                spiced,
                target_binary: target.binary.clone(),
                active_executors: executors.len().saturating_sub(spare_count),
                executors,
                spicepod_yaml,
                spicepod,
                namespaces,
                broken: None,
                answers: BTreeMap::new(),
                metric_marks: BTreeMap::new(),
                named: BTreeMap::new(),
                context: context_for_steps,
                spice_cli: options.spice_cli.clone(),
                background: BTreeMap::new(),
                cache_dir: options.work_dir.join("cache"),
            });
        }
    }
    Ok(instances)
}

fn is_source_step(action: &Action) -> bool {
    matches!(
        action,
        Action::StartSource(_)
            | Action::StopSource(_)
            | Action::PauseSource(_)
            | Action::UnpauseSource(_)
            | Action::Sleep(_)
    )
}

async fn run_lockstep(
    script: &Script,
    steps: &[Step],
    instances: &mut [Instance],
    sources: &mut Sources,
    report: &mut Report,
    run_label: &str,
) {
    for step in steps {
        if is_source_step(&step.action) {
            let outcome = run_source_step(&step.action, sources).await;
            record(report, run_label, "all", step, outcome);
            continue;
        }
        let shared: &Sources = sources;
        let outcomes = futures::future::join_all(
            instances
                .iter_mut()
                .filter(|i| i.broken.is_none() && applies(step, i))
                .map(|instance| async move {
                    let outcome = run_instance_step(script, instance, step, shared).await;
                    (instance, outcome)
                }),
        )
        .await;
        for (instance, outcome) in outcomes {
            note_broken(instance, step, &outcome);
            record(report, run_label, &instance.label, step, outcome);
        }
    }
}

async fn run_sequential(
    script: &Script,
    steps: &[Step],
    instance: &mut Instance,
    sources: &mut Sources,
    report: &mut Report,
    run_label: &str,
) {
    for step in steps {
        if instance.broken.is_some() || !applies(step, instance) {
            continue;
        }
        let outcome = if is_source_step(&step.action) {
            run_source_step(&step.action, sources).await
        } else {
            run_instance_step(script, instance, step, sources).await
        };
        note_broken(instance, step, &outcome);
        record(report, run_label, &instance.label, step, outcome);
    }
}

/// Runs each instance as a job of its own: the first on the slots the
/// combination took, the others as slots free up. Each stops its processes
/// as soon as it finishes, releasing its slots; its output and results join
/// the combination's in instance order.
#[expect(
    clippy::too_many_arguments,
    reason = "one call site, each argument distinct"
)]
async fn run_independent(
    script: &Script,
    steps: &[Step],
    instances: &mut [Instance],
    sources: &Sources,
    slots: &Slots,
    weight: u32,
    first_permit: tokio::sync::OwnedSemaphorePermit,
    report: &mut Report,
    run_label: &str,
) {
    let mut first_permit = Some(first_permit);
    let jobs = instances.iter_mut().map(|instance| {
        let held = first_permit.take();
        buffered(async move {
            let mut part = Report::default();
            let _permit = match held {
                Some(permit) => permit,
                None => match slots.take(weight).await {
                    Ok(permit) => permit,
                    Err(e) => {
                        part.failures.push(Failure::outside_steps(
                            run_label,
                            &instance.label,
                            &format!("{e:#}"),
                        ));
                        return part;
                    }
                },
            };
            for step in steps {
                if instance.broken.is_some() || !applies(step, instance) {
                    continue;
                }
                let outcome = match &step.action {
                    Action::Sleep(d) => {
                        tokio::time::sleep(*d).await;
                        Ok(String::new())
                    }
                    _ => run_instance_step(script, instance, step, sources).await,
                };
                note_broken(instance, step, &outcome);
                record(&mut part, run_label, &instance.label, step, outcome);
            }
            stop_processes(instance).await;
            part
        })
    });
    for (part, out) in futures::future::join_all(jobs).await {
        if !out.is_empty() {
            say!("{}", out.trim_end_matches('\n'));
        }
        report.passed += part.passed;
        report.failures.extend(part.failures);
    }
}

/// Stops an instance's `spiced` and executors together.
async fn stop_processes(instance: &mut Instance) {
    let mut stopping = vec![instance.spiced.stop(TEARDOWN_TIMEOUT)];
    for executor in &mut instance.executors {
        stopping.push(executor.stop(TEARDOWN_TIMEOUT));
    }
    let _ = futures::future::join_all(stopping).await;
}

/// Whether every condition of a place (a variant, or `key=value` on the
/// matrix) holds for the instance.
fn at_place(place: &[String], instance: &Instance) -> bool {
    place
        .iter()
        .all(|condition| match condition.split_once('=') {
            Some((key, value)) => instance.context["matrix"]
                .get(key)
                .is_some_and(|v| match v {
                    Value::String(s) => s == value,
                    other => serde_json::from_str::<Value>(value).ok().as_ref() == Some(other),
                }),
            None => condition == instance.variant.as_str(),
        })
}

fn applies(step: &Step, instance: &Instance) -> bool {
    let variant = instance.variant.as_str();
    (step.only.is_empty() || step.only.iter().any(|v| v == variant))
        && !step.except.iter().any(|v| v == variant)
}

/// A failed lifecycle step leaves `spiced` in an unknown state, so the
/// instance's remaining steps are skipped. Any other failed step is a failed
/// check, and the script goes on.
fn note_broken(instance: &mut Instance, step: &Step, outcome: &Result<String>) {
    if let Err(e) = outcome
        && matches!(
            step.action,
            Action::Start { .. }
                | Action::Stop
                | Action::Kill { .. }
                | Action::Restart { .. }
                | Action::WaitReady { .. }
        )
    {
        instance.broken = Some(format!("step {}: {e:#}", step.index));
    }
}

fn record(report: &mut Report, run_label: &str, who: &str, step: &Step, outcome: Result<String>) {
    let what = describe(&step.action);
    match outcome {
        Ok(detail) => {
            report.passed += 1;
            say!("   ok   {who:<20} {:>3} {what}{detail}", step.index);
        }
        Err(e) => {
            say!("   FAIL {who:<20} {:>3} {what}", step.index);
            let failure = |query: Option<u64>, labels: Vec<String>, message: String| Failure {
                run: run_label.to_string(),
                who: who.to_string(),
                step: step.index,
                key: step.key.clone(),
                what: what.clone(),
                query,
                labels,
                message,
            };
            match e.downcast_ref::<QueryFailures>() {
                Some(queries) => {
                    for q in &queries.failed {
                        report.failures.push(failure(
                            Some(q.number),
                            q.labels.clone(),
                            format!("{}\n      {}", q.problem, q.text),
                        ));
                    }
                }
                None => report
                    .failures
                    .push(failure(None, Vec::new(), format!("{e:#}"))),
            }
        }
    }
}

/// How a dataset names table `t` of a source of this kind: `from:
/// <prefix>t<suffix>`, plus whether it is a directory of files (which then
/// needs a `file_format`), so one spicepod line serves a matrix over sources.
fn table_from(kind: &str, entry: &Map<String, Value>) -> (String, String, bool) {
    let field = |key: &str| entry.get(key).and_then(Value::as_str).unwrap_or_default();
    match kind {
        "postgres" => ("postgres:public.".into(), String::new(), false),
        "mysql" | "clickhouse" => (format!("{kind}:"), String::new(), false),
        "ducklake" => ("ducklake:main.".into(), String::new(), false),
        "iceberg" => (
            format!(
                "iceberg:{}/v1/namespaces/{}/tables/",
                field("catalog"),
                field("namespace")
            ),
            String::new(),
            false,
        ),
        "rustfs" => (format!("s3://{}/", field("bucket")), "/".into(), true),
        "files" => (format!("file://{}/", field("path")), "/".into(), true),
        _ => (String::new(), String::new(), false),
    }
}

/// Whether a matrix combination satisfies a `when:`: each key's value equals
/// the combination's, or, given as a list, contains it.
fn when_matches(when: &Map<String, Value>, combo: &Map<String, Value>) -> bool {
    when.iter()
        .all(|(key, wanted)| match (wanted, combo.get(key)) {
            (Value::Array(any), Some(value)) => any.contains(value),
            (wanted, value) => value == Some(wanted),
        })
}

fn disable_results_cache_unless_configured(spicepod: &mut Value) {
    let Some(root) = spicepod.as_object_mut() else {
        return;
    };
    let runtime = root.entry("runtime").or_insert_with(|| json!({}));
    let Some(runtime) = runtime.as_object_mut() else {
        return;
    };
    let caching = runtime.entry("caching").or_insert_with(|| json!({}));
    let Some(caching) = caching.as_object_mut() else {
        return;
    };
    caching
        .entry("sql_results")
        .or_insert_with(|| json!({ "enabled": false }));
}

/// Applies a start's `patch:` to the instance's spicepod, for this and every
/// later start.
fn apply_patch(instance: &mut Instance, patch: Option<&VariantSpec>) -> Result<()> {
    let Some(patch) = patch else {
        return Ok(());
    };
    ensure!(
        patch.env.is_empty(),
        "a start's `patch` cannot change `env`; an instance's environment is fixed when it is created"
    );
    let resolved = VariantSpec {
        spicepod: patch
            .spicepod
            .as_ref()
            .map(|p| template::substitute(p, &instance.context))
            .transpose()?,
        datasets: patch
            .datasets
            .iter()
            .map(|(k, v)| Ok((k.clone(), template::substitute(v, &instance.context)?)))
            .collect::<Result<_>>()?,
        catalogs: patch
            .catalogs
            .iter()
            .map(|(k, v)| Ok((k.clone(), template::substitute(v, &instance.context)?)))
            .collect::<Result<_>>()?,
        env: patch.env.clone(),
        when: Map::new(),
    };
    template::apply_variant(&mut instance.spicepod, &resolved)?;
    instance.spicepod_yaml = yaml::to_string(&instance.spicepod)?;
    Ok(())
}

fn namespace(instance: &Instance, source: &str) -> Result<String> {
    instance
        .namespaces
        .get(source)
        .cloned()
        .ok_or_else(|| anyhow!("unknown source '{source}'"))
}

/// Every instance must have given the same final answer to every query.
fn compare_instances(
    steps: &[Step],
    instances: &[&Instance],
    compare_to: Option<&str>,
    report: &mut Report,
    run_label: &str,
) -> Result<()> {
    if instances.len() < 2 {
        return Ok(());
    }
    for step in steps {
        let Action::Query(q) = &step.action else {
            continue;
        };
        let mut answers: Vec<(&str, &Vec<RecordBatch>)> = instances
            .iter()
            .filter_map(|i| i.answers.get(&step.index).map(|a| (i.label.as_str(), a)))
            .collect();
        // The reference variant goes first, so every other is compared with it.
        if let Some(reference) = compare_to
            && let Some(at) = instances
                .iter()
                .filter(|i| i.answers.contains_key(&step.index))
                .position(|i| i.variant == reference)
        {
            answers.swap(0, at);
        }
        let Some(((base_label, base), rest)) = answers.split_first() else {
            continue;
        };
        let mut mismatches = String::new();
        for (label, answer) in rest {
            if let Some(diff) = compare::compare(label, &q.sql, answer, base, q.expect_empty)? {
                let _ = writeln!(mismatches, "{label} vs {base_label}: {diff}");
            }
        }
        if mismatches.is_empty() {
            report.passed += 1;
        } else {
            say!(
                "   FAIL {:<20} {:>3} instances disagree",
                "variants",
                step.index
            );
            report.failures.push(Failure {
                run: run_label.to_string(),
                who: "variants".into(),
                step: step.index,
                key: step.key.clone(),
                what: format!("instances disagree on: {}", one_line(&q.sql, 120)),
                query: None,
                labels: Vec::new(),
                message: mismatches,
            });
        }
    }
    Ok(())
}

fn matrix_combinations(matrix: &IndexMap<String, Vec<Value>>) -> Vec<Map<String, Value>> {
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

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn matrix_is_the_cartesian_product_in_declaration_order() {
        let matrix = IndexMap::from([
            (
                "source".to_string(),
                vec![json!("postgres"), json!("mysql")],
            ),
            ("mode".to_string(), vec![json!("file"), json!("memory")]),
        ]);
        let combos: Vec<Value> = matrix_combinations(&matrix)
            .into_iter()
            .map(Value::Object)
            .collect();
        assert_eq!(
            combos,
            vec![
                json!({ "source": "postgres", "mode": "file" }),
                json!({ "source": "postgres", "mode": "memory" }),
                json!({ "source": "mysql", "mode": "file" }),
                json!({ "source": "mysql", "mode": "memory" }),
            ]
        );
        assert_eq!(matrix_combinations(&IndexMap::new()), vec![Map::new()]);
    }
}

#[cfg(test)]
mod table_from_tests {
    use super::*;

    #[test]
    fn each_source_kind_names_a_table_the_way_spice_reads_it() {
        let entry = json!({ "catalog": "http://127.0.0.1:1", "namespace": "i0_x", "bucket": "b", "path": "/d" });
        let entry = entry.as_object().expect("an object");
        let named = |kind: &str| {
            let (prefix, suffix, files) = table_from(kind, entry);
            (format!("{prefix}t{suffix}"), files)
        };
        assert_eq!(named("postgres"), ("postgres:public.t".into(), false));
        assert_eq!(named("mysql"), ("mysql:t".into(), false));
        assert_eq!(named("clickhouse"), ("clickhouse:t".into(), false));
        assert_eq!(named("ducklake"), ("ducklake:main.t".into(), false));
        assert_eq!(
            named("iceberg"),
            (
                "iceberg:http://127.0.0.1:1/v1/namespaces/i0_x/tables/t".into(),
                false
            )
        );
        assert_eq!(named("rustfs"), ("s3://b/t/".into(), true));
        assert_eq!(named("files"), ("file:///d/t/".into(), true));
    }
}
