// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! `standby` traces from the protocol model, replayed through a real writer
//! lease ([`object_store_occ::lease::Lease`]) and a real head log
//! ([`object_store_occ::headlog::Writer`]) on conditional storage.
//!
//! Each model instance is a process with its own `Lease` (a new one at each
//! restart) and its own head-log `Writer`. A takeover gets its generation from
//! the lease and takes the head over at exactly that number; a retake first
//! advances the holder's own lease. After every action the replay checks the
//! implementation against the model step (who holds the lease and at what
//! generation, whether a takeover or commit was accepted) and against
//! invariants that share no code with either:
//!
//! - every head takeover carries the generation of the lease its taker holds,
//!   read back from storage;
//! - the committed records, read by a fresh reader, are exactly the accepted
//!   commits, and the source changes 1..=n each once.
//!
//! The `split` control's counterexample replays twice: with the implementation
//! (it refuses to take a head over at any generation but its lease's, so the
//! invariant holds), and with a takeover at the head's next generation, the
//! rule removed (the invariant fails).
#![expect(clippy::expect_used, reason = "integration setup and trace assertions")]

use bytes::Bytes;
use cayenne_metastore_model::{StandbyCommitOutcome, StandbyStep, StandbyStepKind};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store_occ::{
    ConflictRetry, LocalConditionalPut,
    headlog::{HeadLog, HeadLogConfig, Outcome, PreparedAttempt, Reader, Writer},
    lease::{Acquired, Holder, Lease, LeaseRecord, Role},
};
use serde::Deserialize;
use std::{convert::Infallible, sync::Arc, time::Duration};

type TestResult<T = ()> = Result<T, String>;

/// One model tick of real time; a lease lasts the model's three ticks.
const TICK: Duration = Duration::from_secs(10);
const LEASE: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct Trace {
    run: String,
    family: String,
    actions: Vec<String>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    property: String,
}

fn traces(text: &str) -> Vec<Trace> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("driver JSON"))
        .collect()
}

fn require(condition: bool, message: impl FnOnce() -> String) -> TestResult {
    if condition { Ok(()) } else { Err(message()) }
}

fn log() -> Path {
    Path::from("standby/log")
}

fn lease_path() -> Path {
    Path::from("standby/leases/writer.json")
}

fn retry() -> ConflictRetry {
    ConflictRetry {
        max_attempts: 4,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    }
}

/// How the replay takes a head over.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// At the generation of the lease the taker holds; a retake first
    /// advances the taker's own lease.
    LeaseGeneration,
    /// At the head's next generation, whatever the lease holds: the control's
    /// two generations.
    HeadNext,
}

/// One model instance: its current process.
#[derive(Default)]
struct Process {
    run: u32,
    lease: Option<Lease>,
    /// The lease generation it holds, as the lease told it.
    epoch: u64,
    writer: Option<Writer>,
    /// A prepared commit, with the writer that prepared it when the instance
    /// has taken its head over again since.
    pending: Option<(Option<Writer>, PreparedAttempt, u64)>,
    stopped: bool,
}

struct Replay {
    store: Arc<dyn ObjectStore>,
    rule: Rule,
    processes: Vec<Process>,
    /// The source changes of the commits the head accepted, in order.
    accepted: Vec<u64>,
    /// Extra lapse waits: a process that started after the lease was last
    /// written must watch it for a whole lease before it may take it over.
    waits: usize,
}

impl Replay {
    async fn new(store: Arc<dyn ObjectStore>, instances: usize, rule: Rule) -> TestResult<Self> {
        HeadLog::create(Arc::clone(&store), log(), HeadLogConfig::default())
            .await
            .map_err(|e| e.to_string())?;
        let mut replay = Self {
            store,
            rule,
            processes: (0..instances).map(|_| Process::default()).collect(),
            accepted: Vec::new(),
            waits: 0,
        };
        for i in 0..instances {
            replay.start(i);
        }
        // The model starts with instance 0 holding generation 1 and writing
        // the head at it.
        let first = replay.acquire(0).await?;
        require(
            matches!(
                first,
                Role::Holder {
                    generation: 1,
                    acquired: Acquired::Created
                }
            ),
            || format!("initial lease: {first:?}"),
        )?;
        replay.processes[0].epoch = 1;
        replay.take_over(0).await?;
        replay.observe(0).await?;
        Ok(replay)
    }

    fn start(&mut self, i: usize) {
        let process = &mut self.processes[i];
        process.run += 1;
        process.lease = Some(Lease::new(
            Holder::new(format!("i{i}"), format!("i{i}-run{}", process.run)),
            LEASE,
        ));
        process.epoch = 0;
        process.writer = None;
        process.pending = None;
        process.stopped = false;
    }

    fn lease(&self, i: usize) -> TestResult<Lease> {
        self.processes[i]
            .lease
            .clone()
            .ok_or_else(|| format!("instance {i} has no process"))
    }

    /// The generation after `previous` and after the head's.
    async fn next_generation(
        store: Arc<dyn ObjectStore>,
        previous: u64,
    ) -> Result<u64, Infallible> {
        let head = object_store_occ::headlog::head_generation(store.as_ref(), &log())
            .await
            .expect("read the head")
            .unwrap_or(0);
        Ok(previous.max(head) + 1)
    }

    async fn acquire(&self, i: usize) -> TestResult<Role> {
        let store = Arc::clone(&self.store);
        self.lease(i)?
            .acquire(self.store.as_ref(), &lease_path(), &retry(), |previous| {
                Self::next_generation(Arc::clone(&store), previous)
            })
            .await
            .map_err(|e| e.to_string())
    }

    async fn advance(&self, i: usize) -> TestResult<Option<u64>> {
        let store = Arc::clone(&self.store);
        self.lease(i)?
            .advance(self.store.as_ref(), &lease_path(), |previous| {
                Self::next_generation(store, previous)
            })
            .await
            .map_err(|e| e.to_string())
    }

    /// Every running process that does not hold the lease reads it, so its
    /// lapse clock starts at this version.
    async fn observe(&mut self, writer: usize) -> TestResult {
        // A process whose identity the lease names renews it on reading it
        // (a restarted instance resumes its previous run's lease), which the
        // model does at its next acquisition: it does not observe.
        let holder = self.stored_lease().await?.holder_identity;
        for i in 0..self.processes.len() {
            if i == writer
                || self.processes[i].stopped
                || self.processes[i].epoch != 0
                || holder == format!("i{i}")
            {
                continue;
            }
            let role = self.acquire(i).await?;
            require(matches!(role, Role::Standby { .. }), || {
                format!("instance {i} observing the lease got {role:?}")
            })?;
        }
        Ok(())
    }

    async fn stored_lease(&self) -> TestResult<LeaseRecord> {
        let bytes = self
            .store
            .get(&lease_path())
            .await
            .map_err(|e| e.to_string())?
            .bytes()
            .await
            .map_err(|e| e.to_string())?;
        LeaseRecord::parse(&bytes).ok_or_else(|| "unreadable lease".to_string())
    }

    async fn head_generation(&self) -> TestResult<u64> {
        object_store_occ::headlog::head_generation(self.store.as_ref(), &log())
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no head".to_string())
    }

    /// Take instance `i`'s head over under the replay's rule. `Ok(false)` when
    /// the head refused it.
    async fn take_over(&mut self, i: usize) -> TestResult<bool> {
        let generation = match self.rule {
            Rule::LeaseGeneration => self.processes[i].epoch,
            Rule::HeadNext => self.head_generation().await? + 1,
        };
        let Ok(writer) = Writer::take_over(
            Arc::clone(&self.store),
            log(),
            HeadLogConfig::default(),
            generation,
        )
        .await
        else {
            self.processes[i].epoch = 0;
            self.processes[i].writer = None;
            return Ok(false);
        };
        self.processes[i].writer = Some(writer);
        // The invariant: the head's generation is the generation of the lease
        // its taker holds, both read back from storage.
        let head = self.head_generation().await?;
        let lease = self.stored_lease().await?;
        require(
            lease.holder_identity == format!("i{i}")
                && head == lease.generation
                && head == self.processes[i].epoch,
            || {
                format!(
                    "head taken over at generation {head} by instance {i}, which holds lease generation {}; the stored lease is {}'s at generation {}",
                    self.processes[i].epoch, lease.holder_identity, lease.generation
                )
            },
        )?;
        Ok(true)
    }

    /// The committed source changes, read by a fresh reader.
    async fn committed(&self) -> TestResult<Vec<u64>> {
        let (_, opened) = Reader::open(Arc::clone(&self.store), log())
            .await
            .map_err(|e| e.to_string())?;
        opened
            .records
            .into_iter()
            .map(|record| {
                std::str::from_utf8(&record.record.payload)
                    .ok()
                    .and_then(|text| text.strip_prefix("change "))
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| "unreadable record".to_string())
            })
            .collect()
    }

    async fn step(&mut self, step: &StandbyStep) -> TestResult {
        match step.kind {
            StandbyStepKind::Tick => tokio::time::advance(TICK).await,
            StandbyStepKind::Acquire { instance: i, epoch } => {
                let mut role = self.acquire(i).await?;
                if matches!(role, Role::Standby { .. }) {
                    // This process has not watched the lease for a whole
                    // lease yet (it started after the lease was written).
                    self.waits += 1;
                    tokio::time::advance(LEASE).await;
                    role = self.acquire(i).await?;
                }
                let generation = match role {
                    Role::Holder {
                        generation,
                        acquired: Acquired::Created | Acquired::TookOver { .. },
                    } => generation,
                    // Its previous run held the lease: it moves to a
                    // generation of its own before taking the head over.
                    Role::Holder {
                        acquired: Acquired::Renewed { .. },
                        ..
                    } => self
                        .advance(i)
                        .await?
                        .ok_or_else(|| format!("instance {i} lost the lease it renewed"))?,
                    other => return Err(format!("Acquire({i}) got {other:?}")),
                };
                require(generation == epoch as u64, || {
                    format!("Acquire({i}) at generation {generation}, the model's {epoch}")
                })?;
                self.processes[i].epoch = generation;
                self.processes[i].writer = None;
                self.observe(i).await?;
            }
            StandbyStepKind::Renew {
                instance: i,
                renewed,
            } => {
                let role = self.acquire(i).await?;
                match (role, renewed) {
                    (
                        Role::Holder {
                            generation,
                            acquired:
                                Acquired::Renewed {
                                    by_other_process: false,
                                },
                        },
                        true,
                    ) if generation == self.processes[i].epoch => self.observe(i).await?,
                    (Role::Standby { .. }, false) => {
                        let process = &mut self.processes[i];
                        process.epoch = 0;
                        process.writer = None;
                    }
                    (role, _) => {
                        return Err(format!(
                            "Renew({i}) got {role:?}, the model renewed: {renewed}"
                        ));
                    }
                }
            }
            StandbyStepKind::Takeover {
                instance: i,
                accepted,
                ..
            } => {
                let took = self.take_over(i).await?;
                require(took == accepted, || {
                    format!("Takeover({i}) accepted: {took}, the model's: {accepted}")
                })?;
            }
            StandbyStepKind::Retake { instance: i, epoch } => {
                let process = &mut self.processes[i];
                let writer = process.writer.take();
                if let Some((owner, _, _)) = &mut process.pending
                    && owner.is_none()
                {
                    *owner = writer;
                }
                if self.rule == Rule::LeaseGeneration {
                    let advanced = self
                        .advance(i)
                        .await?
                        .ok_or_else(|| format!("Retake({i}) lost the lease"))?;
                    require(
                        epoch.and_then(|e| u64::try_from(e).ok()) == Some(advanced),
                        || format!("Retake({i}) advanced to {advanced}, the model to {epoch:?}"),
                    )?;
                    self.processes[i].epoch = advanced;
                    self.observe(i).await?;
                }
            }
            StandbyStepKind::Read {
                instance: i,
                change,
            } => {
                let process = &mut self.processes[i];
                let writer = process
                    .writer
                    .as_mut()
                    .ok_or_else(|| format!("Read({i}) with no writer"))?;
                let attempt = writer
                    .prepare(Bytes::from(format!("change {change}")))
                    .await
                    .map_err(|e| format!("Read({i}): {e}"))?;
                process.pending = Some((None, attempt, change as u64));
            }
            StandbyStepKind::Commit {
                instance: i,
                outcome,
            } => {
                let process = &mut self.processes[i];
                let (owner, attempt, change) = process
                    .pending
                    .take()
                    .ok_or_else(|| format!("Commit({i}) with nothing prepared"))?;
                let (real, rebased) = if let Some(mut writer) = owner {
                    (writer.commit(&attempt).await, false)
                } else {
                    let writer = process
                        .writer
                        .as_mut()
                        .ok_or_else(|| format!("Commit({i}) with no writer"))?;
                    (writer.commit(&attempt).await, true)
                };
                let real = real.map_err(|e| format!("Commit({i}): {e}"))?;
                match (outcome, &real) {
                    (StandbyCommitOutcome::Accepted, Outcome::Accepted { .. }) => {
                        self.accepted.push(change);
                    }
                    (StandbyCommitOutcome::Rebase, Outcome::Rejected) if rebased => {
                        // The writer reads the head again under its generation.
                        let generation = process.epoch;
                        process.writer = Some(
                            Writer::resume(
                                Arc::clone(&self.store),
                                log(),
                                HeadLogConfig::default(),
                                generation,
                            )
                            .await
                            .map_err(|e| format!("resume {i}: {e}"))?,
                        );
                    }
                    (StandbyCommitOutcome::Fenced, Outcome::Fenced { .. }) => {
                        process.epoch = 0;
                        process.writer = None;
                    }
                    (StandbyCommitOutcome::Stale, Outcome::Rejected | Outcome::Fenced { .. }) => {}
                    (outcome, real) => {
                        return Err(format!("Commit({i}): {real:?}, the model's {outcome:?}"));
                    }
                }
            }
            StandbyStepKind::Ack { .. }
            | StandbyStepKind::Pause { .. }
            | StandbyStepKind::Resume { .. }
            | StandbyStepKind::Serve { .. } => {}
            StandbyStepKind::Crash { instance: i } => {
                let process = &mut self.processes[i];
                process.lease = None;
                process.writer = None;
                process.pending = None;
                process.epoch = 0;
                process.stopped = true;
            }
            StandbyStepKind::Restart { instance: i } => {
                self.start(i);
                self.observe(usize::MAX).await?;
            }
        }
        // What a fresh reader sees: exactly the accepted commits, which are
        // the source changes 1..=n, each once.
        let committed = self.committed().await?;
        require(committed == self.accepted, || {
            format!("committed {committed:?}, accepted {:?}", self.accepted)
        })?;
        require(
            committed.iter().copied().eq(1..=committed.len() as u64),
            || format!("committed changes {committed:?} are not the source prefix"),
        )?;
        require(
            committed
                .iter()
                .map(|&c| usize::try_from(c).unwrap_or(usize::MAX))
                .eq(step.rows.iter().copied()),
            || format!("committed {committed:?}, the model's rows {:?}", step.rows),
        )?;
        Ok(())
    }
}

fn backends(directory: &std::path::Path) -> Vec<(&'static str, Arc<dyn ObjectStore>)> {
    vec![
        ("memory", Arc::new(object_store::memory::InMemory::new())),
        (
            "local",
            Arc::new(LocalConditionalPut::new_durable(directory).expect("local")),
        ),
    ]
}

fn instances(run: &str) -> usize {
    if run.contains("three") { 3 } else { 2 }
}

async fn replay(trace: &Trace, rule: Rule) -> TestResult<usize> {
    let steps = cayenne_metastore_model::standby_steps(&trace.run, &trace.actions)
        .ok_or_else(|| format!("{}: trace does not step the model", trace.run))?;
    let mut waits = 0;
    let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
    for (name, store) in backends(directory.path()) {
        let mut replay = Replay::new(store, instances(&trace.run), rule).await?;
        for (index, step) in steps.iter().enumerate() {
            replay.step(step).await.map_err(|error| {
                format!(
                    "{name} {} at {index} {:?} of {:?}: {error}",
                    trace.run, trace.actions[index], trace.actions
                )
            })?;
        }
        waits += replay.waits;
    }
    Ok(waits)
}

#[tokio::test(start_paused = true)]
async fn standby_traces_replay_through_the_lease_and_head_log() {
    let mut all = traces(include_str!("traces/standby-retake.jsonl"));
    all.extend(traces(include_str!("traces/standby-retake-walk.jsonl")));
    let mut runs = std::collections::BTreeMap::<String, usize>::new();
    let mut waits = 0;
    for trace in all.iter().filter(|t| t.family == "standby") {
        waits += replay(trace, Rule::LeaseGeneration)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        *runs.entry(trace.run.clone()).or_default() += 1;
    }
    println!(
        "REPLAYED {runs:?}; lapse waits for a process started after the lease was written: {waits}"
    );
    assert!(runs.values().sum::<usize>() >= 20, "{runs:?}");
}

#[tokio::test(start_paused = true)]
async fn two_generations_drift_and_one_generation_does_not() {
    let traces = traces(include_str!("traces/standby-split-generations.jsonl"));
    let counterexamples: Vec<&Trace> = traces
        .iter()
        .filter(|t| {
            t.status == "FAIL" && t.property == "a head takeover carries its lease's generation"
        })
        .collect();
    assert!(
        !counterexamples.is_empty(),
        "no counterexample in the control traces"
    );
    for trace in counterexamples {
        // The control trace names the split model's actions; under the
        // implementation's rule each retake advances the lease first.
        let safe_run = trace.run.replace("-split-generations", "");
        let safe = Trace {
            run: safe_run,
            family: trace.family.clone(),
            actions: trace.actions.clone(),
            status: String::new(),
            property: String::new(),
        };
        replay(&safe, Rule::LeaseGeneration)
            .await
            .unwrap_or_else(|error| panic!("the implementation's rule fails: {error}"));
        let error = replay(trace, Rule::HeadNext)
            .await
            .expect_err("a takeover at the head's next generation must drift from the lease");
        assert!(
            error.contains("head taken over at generation"),
            "unexpected failure: {error}"
        );
        println!("CONTROL {:?}: {error}", trace.actions);
    }
}
