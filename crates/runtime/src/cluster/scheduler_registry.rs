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

//! Scheduler-side registration loop, discovery, and reaper wiring.
//!
//! All non-job distributed state now lives in a single document
//! (`cluster.json`) and per-scheduler heartbeats live in
//! `heartbeats/{id}.json`. See
//! `plans/consolidate-cluster-state-into-cluster-json.md`.

use arc_swap::ArcSwapOption;
use object_store::UpdateVersion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use app::spicepod::component::runtime::Scheduler as SchedulerConfig;
use object_store::ObjectStore;
use runtime_secrets::Secrets;
use snafu::prelude::*;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Runtime;
use crate::cluster::cluster_state::{
    self, ClusterStateStore, MutateError, MutateOk, MutationOutcome, SchedulerEntry,
};
use crate::cluster::heartbeat::{self, CLOCK_SKEW_TOLERANCE_MS, SchedulerHeartbeatStore};
use crate::cluster::reaper::Reaper;
use runtime_metrics::cluster as cluster_metrics;

const DEFAULT_TTL_MS: u64 = 30_000;

/// Upper bound on the deadline for the membership read that gates a heartbeat.
/// The effective deadline is derived from the scheduler's own heartbeat
/// interval by [`membership_check_timeout`] and capped here, because `ttl_ms`
/// is carried per entry: a fixed deadline could exceed the interval for a short
/// TTL and delay a beat far enough for peers to reap a healthy scheduler.
const MEMBERSHIP_CHECK_TIMEOUT_CAP: Duration = Duration::from_secs(2);

/// Deadline for the membership read that gates a heartbeat, as a fraction of
/// that scheduler's heartbeat interval so it always fits inside one beat.
fn membership_check_timeout(ttl_ms: u64) -> Duration {
    let interval = Duration::from_millis(ttl_ms.saturating_div(HEARTBEAT_DIVISOR).max(1));
    (interval / 2).min(MEMBERSHIP_CHECK_TIMEOUT_CAP)
}

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);
const JOB_RECOVERY_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_DIVISOR: u64 = 3;

/// Compare-and-set attempts for one heartbeat before skipping this beat.
/// Contention means another incarnation is writing the same key, which is
/// resolved by re-reading membership rather than by writing harder.
const MAX_HEARTBEAT_CAS_ATTEMPTS: usize = 2;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to initialize scheduler state object store: {source}"))]
    ObjectStoreState {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Scheduler registration record already exists for {scheduler_id} and is still active"
    ))]
    SchedulerIdConflict { scheduler_id: String },

    #[snafu(display("Missing scheduler advertise address for registration"))]
    MissingAdvertiseAddress,

    #[snafu(display("Failed to access cluster state: {source}"))]
    ClusterState { source: MutateError },

    #[snafu(display("Failed to access scheduler heartbeats: {source}"))]
    Heartbeat { source: heartbeat::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type SchedulerPeers = HashMap<String, SchedulerEntry>;

struct SchedulerRegistryRunner {
    cluster: Arc<ClusterStateStore>,
    heartbeats: Arc<SchedulerHeartbeatStore>,
    reaper: Reaper,
    scheduler_id: String,
    instance_id: Uuid,
    entry: SchedulerEntry,
    peers: Arc<RwLock<SchedulerPeers>>,
    job_executor: Arc<crate::jobs::JobExecutor>,
    /// Set once this incarnation has been *definitively* observed as
    /// superseded. Sticky, so a later failed read cannot resume claiming the
    /// heartbeat key.
    superseded: Arc<AtomicBool>,
    /// Version of this incarnation's last successful heartbeat write. A read
    /// failure therefore does not force a choice between writing blind and
    /// skipping: the write stays conditional on what this process last wrote, so
    /// a successor that has written since is protected by the predicate rather
    /// than by a liveness guess.
    last_written_version: Arc<ArcSwapOption<UpdateVersion>>,
}

pub async fn start_scheduler_registry(
    rt: Arc<Runtime>,
    config: &SchedulerConfig,
    cancel: CancellationToken,
    peers: Arc<RwLock<SchedulerPeers>>,
    cluster: Arc<ClusterStateStore>,
    heartbeats: Arc<SchedulerHeartbeatStore>,
) -> Result<()> {
    let datafusion = rt.datafusion();
    let advertise_host = datafusion
        .cluster_config
        .node_advertise_address()
        .ok_or(Error::MissingAdvertiseAddress)?
        .to_string();

    let scheduler_id = format!(
        "{advertise_host}:{}",
        rt.datafusion().cluster_config.node_bind_address().port()
    );

    let instance_id = Uuid::new_v4();
    let now = now_ms()?;
    let entry = SchedulerEntry {
        scheduler_id: scheduler_id.clone(),
        instance_id,
        advertise_address: scheduler_id.clone(),
        grpc_address: format!(
            "{advertise_host}:{}",
            rt.config().flight_bind_address.port()
        ),
        http_address: format!("{advertise_host}:{}", rt.config().http_bind_address.port()),
        started_at_ms: now,
        ttl_ms: DEFAULT_TTL_MS,
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        labels: HashMap::new(),
    };

    // Initialize job executor for async SQL queries. Reuses the raw
    // (store, base_prefix) pair behind ClusterStateStore.
    let (store, base_prefix) =
        build_object_store(rt.as_ref(), &config.state_location, config).await?;
    let job_store = crate::jobs::JobStore::new(
        Arc::clone(&store),
        base_prefix.clone(),
        instance_id.to_string(),
    );
    let job_executor = Arc::new(crate::jobs::JobExecutor::new(
        Arc::new(job_store),
        rt.datafusion(),
    ));
    rt.set_job_executor(Arc::clone(&job_executor)).await;
    tracing::info!(
        "Initialized async SQL jobs API with state location: {}",
        config.state_location
    );

    let reaper = Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats));

    // The job-recovery sweep runs as another arm of the runner's select loop (see
    // `run`), so it shares this already-tracked registry task's lifecycle and
    // graceful shutdown rather than a separate untracked tokio::spawn.
    let runner = SchedulerRegistryRunner {
        cluster,
        heartbeats,
        reaper,
        scheduler_id,
        instance_id,
        entry,
        peers,
        job_executor,
        superseded: Arc::new(AtomicBool::new(false)),
        last_written_version: Arc::new(ArcSwapOption::empty()),
    };

    runner.run(cancel).await
}

/// Resumes any running job whose recorded scheduler instance is not currently
/// live. Each driving scheduler records its own `instance_id` on the job, and
/// this scheduler's id is always treated as live so it never reclaims its own
/// in-flight jobs.
async fn recover_orphaned_jobs(
    job_executor: &crate::jobs::JobExecutor,
    peers: &RwLock<SchedulerPeers>,
    instance_id: Uuid,
) {
    let live: HashSet<String> = {
        let guard = peers.read().await;
        let mut live: HashSet<String> = guard.values().map(|e| e.instance_id.to_string()).collect();
        live.insert(instance_id.to_string());
        live
    };

    let jobs = match job_executor
        .list_jobs(Some(crate::jobs::JobStatus::Running))
        .await
    {
        Ok(jobs) => jobs,
        Err(err) => {
            tracing::warn!("Failed to list jobs during recovery sweep: {err}");
            return;
        }
    };

    for job in jobs {
        let driven = job
            .scheduler_node
            .as_ref()
            .is_some_and(|node| live.contains(node));
        if !driven {
            tracing::info!(
                job_id = %job.job_id,
                scheduler_node = ?job.scheduler_node,
                "Recovering job orphaned by a lost scheduler"
            );
            job_executor.resume(&job.job_id).await;
        }
    }
}

impl SchedulerRegistryRunner {
    async fn run(self, cancel: CancellationToken) -> Result<()> {
        self.cluster.bootstrap().await.context(ClusterStateSnafu)?;
        self.register_self().await?;

        let heartbeat_interval =
            Duration::from_millis(self.entry.ttl_ms.saturating_div(HEARTBEAT_DIVISOR).max(1));
        let reaper_interval = Duration::from_millis(self.entry.ttl_ms);
        let reaper_jittered = heartbeat::jitter(reaper_interval, 0.2);
        let mut heartbeat_tick = tokio::time::interval(heartbeat_interval);
        let mut discovery_tick = tokio::time::interval(DISCOVERY_INTERVAL);
        let mut reaper_tick = tokio::time::interval(reaper_jittered);
        // `interval_at` starts the first recovery sweep one interval out, so the
        // initial discovery below has populated `peers` before we judge any job
        // orphaned — otherwise live schedulers look lost at startup.
        let mut recovery_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + JOB_RECOVERY_INTERVAL,
            JOB_RECOVERY_INTERVAL,
        );

        // Run an initial discovery so peers are populated promptly.
        if let Err(err) = self.refresh_peers().await {
            tracing::warn!("Initial scheduler discovery failed: {err}");
        }

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    self.shutdown().await;
                    break;
                }
                _ = heartbeat_tick.tick() => {
                    if let Err(err) = self.send_heartbeat().await {
                        tracing::warn!("Scheduler heartbeat failed: {err}");
                    }
                }
                _ = discovery_tick.tick() => {
                    if let Err(err) = self.refresh_peers().await {
                        tracing::warn!("Scheduler discovery failed: {err}");
                    }
                }
                _ = reaper_tick.tick() => {
                    match now_ms() {
                        Ok(now) => match self.reaper.tick(now).await {
                            Ok(out) if !out.evicted.is_empty() => {
                                tracing::info!(
                                    evicted = ?out.evicted,
                                    skipped = ?out.skipped,
                                    "Reaper tick"
                                );
                            }
                            Ok(_) => {}
                            Err(err) => tracing::warn!("Reaper tick failed: {err}"),
                        },
                        Err(err) => tracing::warn!(
                            "Skipping reaper tick because current time is unavailable: {err}"
                        ),
                    }
                }
                _ = recovery_tick.tick() => {
                    recover_orphaned_jobs(&self.job_executor, &self.peers, self.instance_id).await;
                }
            }
        }

        Ok(())
    }

    async fn register_self(&self) -> Result<()> {
        let observed = self
            .heartbeats
            .read_versioned(&self.scheduler_id)
            .await
            .context(HeartbeatSnafu)?;
        let observed_version = observed.as_ref().map(|(_, version)| version.clone());
        let observed_beat = observed.and_then(|(beat, _)| beat);

        let entry = self.entry.clone();
        let scheduler_id = self.scheduler_id.clone();
        let instance_id = self.instance_id;

        let now = now_ms()?;

        let res = self
            .cluster
            .mutate(|state| {
                if let Some(existing) = state.schedulers.get(&scheduler_id) {
                    // Judge the registered entry by *its own* heartbeat. The
                    // key is shared, so a beat left by another incarnation says
                    // nothing about this entry: if a successor commits its
                    // membership and dies before its first beat, the
                    // predecessor's beat is what remains, and treating that as
                    // a disqualifying mismatch would lock the id forever
                    // (registration refuses, and the reaper skips the same
                    // mismatch).
                    //
                    // A foreign beat is therefore not proof this entry is
                    // alive — but a *fresh* one is proof that something is
                    // actively writing the key, so it still blocks takeover.
                    // Otherwise a legacy writer that briefly overwrites a live
                    // successor's beat would let a third incarnation evict that
                    // live successor. Recovery is then delayed by at most the
                    // TTL it takes the foreign beat to go stale.
                    let stale_or_missing = match observed_beat.as_ref() {
                        Some(beat) if beat.instance_id == existing.instance_id => {
                            beat.is_stale(now)
                        }
                        Some(foreign) => {
                            let grace = existing.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS);
                            foreign.is_stale(now)
                                && now.saturating_sub(existing.started_at_ms) > grace
                        }
                        None => {
                            let grace = existing.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS);
                            now.saturating_sub(existing.started_at_ms) > grace
                        }
                    };
                    if stale_or_missing {
                        state.schedulers.insert(scheduler_id.clone(), entry.clone());
                        MutationOutcome::Apply
                    } else if existing.instance_id == instance_id {
                        // Re-register on top of our own (e.g. retry).
                        MutationOutcome::NoChange
                    } else {
                        MutationOutcome::Abort(MutateError::Conflict {
                            message: format!("scheduler id {scheduler_id} is already registered"),
                        })
                    }
                } else {
                    state.schedulers.insert(scheduler_id.clone(), entry.clone());
                    MutationOutcome::Apply
                }
            })
            .await;

        match res {
            Ok(_) => {}
            Err(MutateError::Conflict { .. }) => {
                return Err(Error::SchedulerIdConflict {
                    scheduler_id: self.scheduler_id.clone(),
                });
            }
            Err(other) => return Err(Error::ClusterState { source: other }),
        }

        // Write our first heartbeat so peers see liveness on next discovery. This
        // write is conditional on the beat observed above, like every other
        // heartbeat write: registration commits membership first, and if this
        // task stalls in between, a successor can register and publish its own
        // beat — an unconditional seed would replace it and recreate exactly the
        // false-orphan window this guards against.
        //
        // Its version is retained, because without one an incarnation whose reads
        // fail from birth would have nothing to condition on and would skip every
        // beat until it was reaped.
        match self
            .heartbeats
            .heartbeat_if_unchanged(
                &self.scheduler_id,
                self.instance_id,
                now,
                self.entry.ttl_ms,
                observed_version.as_ref(),
            )
            .await
        {
            Ok(version) => {
                self.last_written_version.store(version.map(Arc::new));
                Ok(())
            }
            // Someone wrote the key between the observation and this seed. Leave
            // it alone: membership already names this incarnation, so the first
            // heartbeat tick reclaims the key legitimately once it can read the
            // beat and re-confirm ownership. Liveness is delayed by at most one
            // interval, where overwriting could silence a live successor.
            Err(heartbeat::Error::HeartbeatSupersededDuringWrite { .. }) => {
                self.last_written_version.store(None);
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    instance_id = %self.instance_id,
                    "Heartbeat changed while registering; leaving it for the first beat to reconcile"
                );
                Ok(())
            }
            Err(source) => Err(Error::Heartbeat { source }),
        }
    }

    async fn send_heartbeat(&self) -> Result<()> {
        // `scheduler_id` is `{advertise_host}:{port}` and so is stable across
        // restarts, while `instance_id` is per-process. Two incarnations can
        // therefore share the heartbeat key, and registration deliberately
        // lets a new one take over once the old looks stale. An incarnation
        // that has been superseded must stop claiming the id: otherwise it
        // keeps overwriting the key with its own `instance_id`, `list_alive`
        // filters the *registered* incarnation out as an orphan, and peers
        // re-drive jobs it is still running.
        if self.superseded.load(Ordering::Relaxed) {
            return Ok(());
        }

        match self.read_membership().await {
            Some(false) => {
                // A successful read proved someone else owns the id. Retire.
                self.superseded.store(true, Ordering::Relaxed);
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    instance_id = %self.instance_id,
                    "This scheduler incarnation is no longer registered; it will stop heartbeating"
                );
                Ok(())
            }
            // Registered, or membership unreadable. Both proceed, and for the
            // same reason: heartbeats deliberately live outside `cluster.json`
            // so emission does not depend on that document, and suppressing
            // beats when it cannot be read would self-evict a healthy
            // scheduler. Proceeding is safe because the write path re-confirms
            // ownership against a fresh read before it will touch a beat
            // belonging to another incarnation — this observation is not
            // carried forward as proof of anything.
            Some(true) | None => self.write_heartbeat().await,
        }
    }

    /// `Some(true)` when a successful, bounded read shows this incarnation
    /// registered, `Some(false)` when it shows someone else (or nobody), and
    /// `None` when membership could not be read at all.
    async fn read_membership(&self) -> Option<bool> {
        let read_timeout = membership_check_timeout(self.entry.ttl_ms);
        match tokio::time::timeout(read_timeout, self.cluster.read()).await {
            Ok(Ok(state)) => Some(
                state
                    .schedulers
                    .get(&self.scheduler_id)
                    .map(|entry| entry.instance_id)
                    == Some(self.instance_id),
            ),
            Ok(Err(err)) => {
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    error = %err,
                    "Could not confirm cluster membership; heartbeating anyway"
                );
                None
            }
            Err(_elapsed) => {
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    timeout_ms = read_timeout.as_millis(),
                    "Timed out confirming cluster membership; heartbeating anyway"
                );
                None
            }
        }
    }
    /// Writes the heartbeat without ever overwriting another incarnation's.
    ///
    /// A conditional write proves only "the object did not change since I read
    /// it" — it cannot prove "I am the registered incarnation". The two are
    /// therefore combined: the version predicate closes the window between
    /// checking membership and writing, while `membership` decides whether
    /// writing over a *foreign* beat is permitted at all.
    ///
    /// - Our own beat, or none yet: refresh it. Safe even when membership is
    ///   unreadable, and refusing would self-evict a healthy scheduler.
    /// - A foreign beat: overwrite only against a membership read taken *after*
    ///   that beat was observed, proving we are still registered. Without that
    ///   proof, skip the beat — a superseded incarnation must not refresh over
    ///   its successor merely because `cluster.json` was slow, and an
    ///   observation from earlier in the tick may predate a successor's
    ///   registration.
    async fn write_heartbeat(&self) -> Result<()> {
        for _ in 0..MAX_HEARTBEAT_CAS_ATTEMPTS {
            let read_timeout = membership_check_timeout(self.entry.ttl_ms);
            let (observed, from_cache) = match tokio::time::timeout(
                read_timeout,
                self.heartbeats.read_versioned(&self.scheduler_id),
            )
            .await
            {
                Ok(Ok(result)) => (result, false),
                // A read error and a read timeout are the same situation: the
                // current beat is unknown. Aborting the tick here would let
                // repeated transient errors suppress every beat until the TTL
                // lapses and this healthy scheduler is reaped.
                Ok(Err(err)) => match self.cached_predicate(&format!("{err}")) {
                    Some(version) => (Some((None, version)), true),
                    None => return Ok(()),
                },
                Err(_elapsed) => match self.cached_predicate("read timed out") {
                    Some(version) => (Some((None, version)), true),
                    None => return Ok(()),
                },
            };

            // An unparsable payload reads back as `None`: the holder is unknown,
            // which is not evidence that it is ours, so it is treated like a
            // foreign beat.
            // A beat belonging to someone else — or one that cannot be parsed, so
            // the holder is unknown — may only be reclaimed against a membership
            // read taken *after* it was observed. The read this tick started with
            // may predate a successor's registration, and a version predicate
            // does not help: it proves the object has not changed since the beat
            // was read, not that this incarnation still owns the id.
            if !from_cache
                && let Some((beat, _)) = observed.as_ref()
                && beat
                    .as_ref()
                    .is_none_or(|beat| beat.instance_id != self.instance_id)
            {
                let holder = beat.as_ref().map(|b| b.instance_id);
                match self.read_membership().await {
                    // Freshly proven to still own the id, so reclaiming the key
                    // is legitimate: fall through to the conditional write.
                    Some(true) => {}
                    Some(false) => {
                        self.superseded.store(true, Ordering::Relaxed);
                        tracing::warn!(
                            scheduler_id = %self.scheduler_id,
                            instance_id = %self.instance_id,
                            holder = ?holder,
                            "Another incarnation holds the heartbeat and is the registered owner; retiring"
                        );
                        return Ok(());
                    }
                    None => {
                        tracing::warn!(
                            scheduler_id = %self.scheduler_id,
                            instance_id = %self.instance_id,
                            holder = ?holder,
                            "Another incarnation holds the heartbeat and membership is unreadable; skipping this beat"
                        );
                        return Ok(());
                    }
                }
            }

            let expected = observed.as_ref().map(|(_, version)| version.clone());
            let now = now_ms()?;
            match self
                .heartbeats
                .heartbeat_if_unchanged(
                    &self.scheduler_id,
                    self.instance_id,
                    now,
                    self.entry.ttl_ms,
                    expected.as_ref(),
                )
                .await
            {
                Ok(version) => {
                    self.last_written_version.store(version.map(Arc::new));
                    return Ok(());
                }
                Err(heartbeat::Error::HeartbeatSupersededDuringWrite { .. }) if from_cache => {
                    // The predicate came from this incarnation's own last write,
                    // not from a read of the current object. A failed precondition
                    // therefore does not prove someone else took the key: an
                    // earlier write of ours may have landed while its
                    // acknowledgement was lost, moving the version. Treat the
                    // retained version as unusable and wait for a readable beat
                    // rather than retrying against a predicate known to be wrong,
                    // or retiring on evidence that does not exist.
                    self.last_written_version.store(None);
                    tracing::warn!(
                        scheduler_id = %self.scheduler_id,
                        instance_id = %self.instance_id,
                        "Heartbeat version is stale and the beat is unreadable; skipping until it can be read"
                    );
                    return Ok(());
                }
                Err(heartbeat::Error::HeartbeatSupersededDuringWrite { .. }) => {
                    // Someone else wrote between the read and the write. Only a
                    // fresh membership read decides retry versus retire.
                    match self.read_membership().await {
                        // Still ours, so retry: the next attempt re-reads the
                        // beat and re-confirms ownership before writing.
                        Some(true) => {}
                        Some(false) => {
                            self.superseded.store(true, Ordering::Relaxed);
                            tracing::warn!(
                                scheduler_id = %self.scheduler_id,
                                instance_id = %self.instance_id,
                                "Lost the heartbeat to the registered incarnation; retiring"
                            );
                            return Ok(());
                        }
                        None => return Ok(()),
                    }
                }
                Err(source) => return Err(Error::Heartbeat { source }),
            }
        }
        tracing::warn!(
            scheduler_id = %self.scheduler_id,
            "Heartbeat contended by another incarnation; skipping this beat"
        );
        Ok(())
    }

    /// The current beat could not be read. Rather than choose between writing
    /// without a predicate (which cannot prove it is not overwriting a
    /// successor) and skipping until the TTL lapses (which gets a healthy
    /// scheduler reaped), fall back to the version this incarnation last wrote.
    ///
    /// The write therefore stays conditional through a read outage: if a
    /// successor has written since, the predicate fails and the caller
    /// reconciles against membership instead of clobbering it. Read-only
    /// outages, where writes still succeed, no longer force either bad choice.
    ///
    /// Returns `None` only when no write by this incarnation has ever reported a
    /// version — registration seeds one, so in practice this means the store does
    /// not report versions at all — in which case the beat is skipped.
    fn cached_predicate(&self, reason: &str) -> Option<UpdateVersion> {
        let Some(version) = self.last_written_version.load_full() else {
            tracing::warn!(
                scheduler_id = %self.scheduler_id,
                reason,
                "Cannot read the current heartbeat and no prior write to condition on; skipping this beat"
            );
            return None;
        };
        tracing::warn!(
            scheduler_id = %self.scheduler_id,
            reason,
            "Cannot read the current heartbeat; writing conditionally against the last version this process wrote"
        );
        Some(UpdateVersion::clone(&version))
    }

    async fn refresh_peers(&self) -> Result<()> {
        let snap = self.cluster.read().await.context(ClusterStateSnafu)?;
        let now = now_ms()?;
        let alive = self
            .heartbeats
            .list_alive(now, &snap)
            .await
            .context(HeartbeatSnafu)?;

        let mut next: HashMap<String, SchedulerEntry> = HashMap::new();
        for (id, entry) in &snap.schedulers {
            if alive.contains_key(id) || id == &self.scheduler_id {
                next.insert(id.clone(), entry.clone());
            }
        }

        let mut peers = self.peers.write().await;
        let previous: HashSet<String> = peers.keys().cloned().collect();
        let next_keys: HashSet<String> = next.keys().cloned().collect();
        let added: Vec<_> = next_keys.difference(&previous).cloned().collect();
        let removed: Vec<_> = previous.difference(&next_keys).cloned().collect();
        if !added.is_empty() || !removed.is_empty() {
            tracing::info!(
                "Scheduler membership updated; added={}, removed={}",
                added.len(),
                removed.len()
            );
        }
        *peers = next;
        cluster_metrics::set_scheduler_count(&self.scheduler_id, peers.len() as u64);
        Ok(())
    }

    async fn shutdown(&self) {
        let scheduler_id = self.scheduler_id.clone();
        let instance_id = self.instance_id;
        let owned_the_entry = match self
            .cluster
            .mutate(|state| match state.schedulers.get(&scheduler_id) {
                Some(entry) if entry.instance_id == instance_id => {
                    state.schedulers.remove(&scheduler_id);
                    MutationOutcome::Apply
                }
                _ => MutationOutcome::NoChange,
            })
            .await
        {
            Ok(MutateOk::Committed) => true,
            // We were not the registered incarnation, so the id — and the
            // heartbeat object under it — belongs to someone else now.
            Ok(MutateOk::AlreadySatisfied) => false,
            Err(err) => {
                tracing::warn!("Failed to remove scheduler entry on shutdown: {err}");
                false
            }
        };

        // Deliberately do not delete the heartbeat object. It is keyed by
        // scheduler id alone, and there is no conditional delete, so any
        // delete races a successor: between the mutation above committing and
        // the delete landing, a successor can register and publish its
        // heartbeat, and this process would then erase the new incarnation's
        // liveness — the false-orphan window this change exists to close.
        //
        // Removing the membership entry is sufficient. `list_alive` only
        // reports a heartbeat that matches a registered entry, so with the
        // entry gone the object is inert, and whoever takes the id next
        // overwrites it.
        let _ = owned_the_entry;
    }
}

pub(super) async fn build_object_store(
    rt: &Runtime,
    state_location: &str,
    config: &SchedulerConfig,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    build_object_store_internal(
        Arc::clone(&rt.secrets()),
        rt.tokio_io_runtime().clone(),
        state_location,
        config,
    )
    .await
}

pub async fn build_object_store_internal(
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
    state_location: &str,
    config: &SchedulerConfig,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    crate::object_store_state::build_object_store(
        secrets,
        io_runtime,
        state_location,
        config.params.as_ref(),
        "scheduler state",
    )
    .await
    .map_err(|source| Error::ObjectStoreState {
        source: Box::new(source),
    })
}

fn now_ms() -> Result<u64> {
    cluster_state::now_ms().map_err(|source| Error::ClusterState { source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::datafusion::builder::DataFusionBuilder;
    use crate::status;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn registry_runner_basic_register_and_shutdown() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };
        runner.register_self().await.expect("register");

        let snap = cluster.read().await.expect("read");
        assert!(snap.schedulers.contains_key("test:50051"));
        let beat = heartbeats.read("test:50051").await.expect("read hb");
        assert!(beat.is_some());

        runner.shutdown().await;
        let snap = cluster.read().await.expect("read");
        assert!(!snap.schedulers.contains_key("test:50051"));
        // Shutdown removes membership and deliberately leaves the heartbeat
        // object alone: deleting a key shared with a possible successor races
        // that successor. With no registered entry the object is inert.
        let alive = heartbeats
            .list_alive(1_000, &snap)
            .await
            .expect("list alive");
        assert!(!alive.contains_key("test:50051"));
    }

    /// A superseded incarnation must stop claiming the shared heartbeat key.
    /// `scheduler_id` is stable across restarts, so without this an old
    /// process keeps overwriting the key with its own `instance_id`,
    /// `list_alive` filters the registered incarnation out as an orphan, and
    /// peers re-drive jobs it is still running.
    #[tokio::test]
    async fn a_superseded_incarnation_stops_heartbeating() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let successor = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry: entry.clone(),
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };
        runner.register_self().await.expect("register");

        // A successor takes the id over, as registration allows once the old
        // incarnation looks stale.
        let mut successor_entry = entry;
        successor_entry.instance_id = successor;
        cluster
            .mutate(|state| {
                state
                    .schedulers
                    .insert("test:50051".to_string(), successor_entry.clone());
                MutationOutcome::Apply
            })
            .await
            .expect("successor takes over");
        heartbeats
            .heartbeat("test:50051", successor, 10_000, 30_000)
            .await
            .expect("successor heartbeat");

        // The superseded runner must now decline to write.
        runner.send_heartbeat().await.expect("send_heartbeat");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat present");
        assert_eq!(
            beat.instance_id, successor,
            "a superseded incarnation must not overwrite the registered one's heartbeat"
        );
        assert_eq!(beat.last_heartbeat_ms, 10_000);

        // The decision is sticky. Make the next cluster read fail outright and
        // heartbeat again: fail-open would otherwise write, so this is what
        // actually proves the guard — asserting the flag alone would pass even
        // with the guard removed.
        store
            .delete(&object_store::path::Path::from("cluster.json"))
            .await
            .expect("remove cluster state so the next read fails");
        runner
            .send_heartbeat()
            .await
            .expect("send_heartbeat after supersession");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat present");
        assert_eq!(
            beat.instance_id, successor,
            "a later failed read must not let a superseded incarnation resume claiming the key"
        );
        assert_eq!(beat.last_heartbeat_ms, 10_000);
        assert!(runner.superseded.load(Ordering::Relaxed));
    }

    /// The membership read sits on the heartbeat path, so its deadline must
    /// stay inside one heartbeat interval for *any* `ttl_ms`, not just the
    /// default: `ttl_ms` is carried per entry, and a short one would otherwise
    /// leave a slow store able to delay beats until peers reap a healthy
    /// scheduler.
    #[test]
    fn membership_check_timeout_stays_within_the_heartbeat_interval() {
        for ttl_ms in [1, 10, 100, 1_000, DEFAULT_TTL_MS, 300_000] {
            let interval = Duration::from_millis(ttl_ms.saturating_div(HEARTBEAT_DIVISOR).max(1));
            let timeout = membership_check_timeout(ttl_ms);
            assert!(
                timeout < interval,
                "ttl_ms={ttl_ms}: deadline {timeout:?} does not fit inside a {interval:?} heartbeat interval"
            );
            assert!(timeout <= MEMBERSHIP_CHECK_TIMEOUT_CAP);
        }
    }

    /// Store that lets a successor complete its takeover *between* the membership
    /// read and the heartbeat read. That window is invisible to a version
    /// predicate: the predicate proves the beat object has not changed since it
    /// was read, while what went stale is the membership observation that
    /// authorised overwriting a foreign beat at all.
    #[derive(Debug)]
    struct TakeOverBetweenMembershipAndBeatRead {
        inner: Arc<dyn ObjectStore>,
        cluster: Arc<ClusterStateStore>,
        heartbeats: Arc<SchedulerHeartbeatStore>,
        successor: Uuid,
        armed: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for TakeOverBetweenMembershipAndBeatRead {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TakeOverBetweenMembershipAndBeatRead")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for TakeOverBetweenMembershipAndBeatRead {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let result = self.inner.get_opts(location, options).await;
            // Fire once, after the membership read has been answered: the caller
            // now holds a `ConfirmedSelf` observation that is already obsolete.
            if location.as_ref().contains("cluster.json")
                && self.armed.swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                self.cluster
                    .mutate(|state| {
                        if let Some(entry) = state.schedulers.get_mut("test:50051") {
                            entry.instance_id = self.successor;
                            entry.started_at_ms = 50_000;
                        }
                        MutationOutcome::Apply
                    })
                    .await
                    .expect("successor claims membership");
                self.heartbeats
                    .heartbeat("test:50051", self.successor, 60_000, 30_000)
                    .await
                    .expect("successor beat");
            }
            result
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A membership observation authorising the overwrite of a foreign beat can be
    /// obsolete by the time the beat is read. A version predicate does not cover
    /// that window, so the overwrite must be re-authorised against a fresh read.
    #[tokio::test]
    async fn a_successor_registering_before_the_beat_read_is_not_clobbered() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // The takeover the wrapper performs must not re-enter the wrapper, so it
        // acts through stores bound directly to the un-wrapped object store.
        let inner_cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let inner_heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));
        let successor = Uuid::new_v4();
        let wrapper = Arc::new(TakeOverBetweenMembershipAndBeatRead {
            inner: Arc::clone(&inner),
            cluster: Arc::clone(&inner_cluster),
            heartbeats: Arc::clone(&inner_heartbeats),
            successor,
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        // The runner reads membership *through* the wrapper, which is what lets
        // the successor land between its membership read and its beat read.
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        inner_cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");

        wrapper
            .armed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        runner
            .send_heartbeat()
            .await
            .expect("losing the id mid-tick must not fail the loop");

        let beat = inner_heartbeats
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            beat.instance_id, successor,
            "a successor that registered before the beat was read must not be clobbered"
        );
        assert!(
            runner.superseded.load(Ordering::Relaxed),
            "the fresh membership read proves supersession, which must be sticky"
        );
    }

    /// Store whose heartbeat *reads* fail on demand while writes keep working —
    /// the asymmetric outage that decides between clobbering a successor and
    /// letting a healthy scheduler's TTL lapse.
    #[derive(Debug)]
    struct FailingBeatReads {
        inner: Arc<dyn ObjectStore>,
        failing: std::sync::atomic::AtomicBool,
        beat_writes: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for FailingBeatReads {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingBeatReads")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingBeatReads {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location.as_ref().contains("heartbeats/")
                && self.failing.load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(object_store::Error::Generic {
                    store: "FailingBeatReads",
                    source: "injected heartbeat read failure".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let result = self.inner.put_opts(location, payload, opts).await;
            if result.is_ok() && location.as_ref().contains("heartbeats/") {
                self.beat_writes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            result
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Builds a runner over the given stores. `inner` is the un-wrapped store, so
    /// a test can observe and seed state the runner sees through its wrapper.
    fn runner_over(
        cluster: &Arc<ClusterStateStore>,
        heartbeats: &Arc<SchedulerHeartbeatStore>,
        inner: &Arc<dyn ObjectStore>,
        instance_id: Uuid,
    ) -> SchedulerRegistryRunner {
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(inner), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        SchedulerRegistryRunner {
            cluster: Arc::clone(cluster),
            heartbeats: Arc::clone(heartbeats),
            reaper: Reaper::new(Arc::clone(cluster), Arc::clone(heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        }
    }

    /// A read-only outage must not cost this scheduler its liveness. Writes still
    /// work, so the beat is written conditionally against the version this
    /// process last wrote rather than skipped until the TTL lapses.
    #[tokio::test]
    async fn a_heartbeat_read_outage_still_refreshes_the_heartbeat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(FailingBeatReads {
            inner: Arc::clone(&inner),
            failing: std::sync::atomic::AtomicBool::new(false),
            beat_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");
        let observer = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));

        let instance_id = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");
        // One good tick, so there is a version to condition on.
        runner.send_heartbeat().await.expect("first beat");
        let before = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");

        wrapper
            .failing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let writes_before = wrapper
            .beat_writes
            .load(std::sync::atomic::Ordering::Relaxed);
        runner
            .send_heartbeat()
            .await
            .expect("a read outage must not fail the loop");
        assert_eq!(
            wrapper
                .beat_writes
                .load(std::sync::atomic::Ordering::Relaxed),
            writes_before + 1,
            "the beat must actually be written during a read outage, not merely left alone"
        );

        let after = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            after.instance_id, instance_id,
            "the beat must still belong to this incarnation"
        );
        assert!(
            after.last_heartbeat_ms >= before.last_heartbeat_ms,
            "the refreshed beat must not move backwards: {} < {}",
            after.last_heartbeat_ms,
            before.last_heartbeat_ms
        );
        assert!(
            !runner.superseded.load(Ordering::Relaxed),
            "a read outage is not evidence of supersession"
        );
    }

    /// When membership has already moved to the successor, a read outage must not
    /// stop this incarnation from noticing: it retires on the membership read
    /// alone, without attempting any write.
    #[tokio::test]
    async fn a_read_outage_retires_when_membership_has_moved_on() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(FailingBeatReads {
            inner: Arc::clone(&inner),
            failing: std::sync::atomic::AtomicBool::new(false),
            beat_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");
        let observer = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));

        let instance_id = Uuid::new_v4();
        let successor = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");
        runner.send_heartbeat().await.expect("first beat");

        // A real takeover: membership *and* the heartbeat move to the successor.
        cluster
            .mutate(|state| {
                if let Some(entry) = state.schedulers.get_mut("test:50051") {
                    entry.instance_id = successor;
                    entry.started_at_ms = 50_000;
                }
                MutationOutcome::Apply
            })
            .await
            .expect("successor claims membership");
        heartbeats
            .heartbeat("test:50051", successor, 60_000, 30_000)
            .await
            .expect("successor beat");

        wrapper
            .failing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        runner
            .send_heartbeat()
            .await
            .expect("losing the key must not fail the loop");

        let beat = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            beat.instance_id, successor,
            "a successor must not be clobbered by a write made during a read outage"
        );
        assert!(
            runner.superseded.load(Ordering::Relaxed),
            "losing the conditional write to the registered incarnation retires this one"
        );
    }

    /// The clobber this change exists to prevent, in the form that actually
    /// reaches the conditional write: the beat has been written by someone else,
    /// but membership still names this incarnation, so it does not retire. Its
    /// beat cannot be read, so ownership of the *object* cannot be re-observed —
    /// only the version it last wrote is available. That predicate no longer
    /// matches, so the write must fail rather than overwrite the other beat.
    #[tokio::test]
    async fn a_read_outage_does_not_clobber_a_beat_written_since_the_last_write() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(FailingBeatReads {
            inner: Arc::clone(&inner),
            failing: std::sync::atomic::AtomicBool::new(false),
            beat_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");
        let observer = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));

        let instance_id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");
        // A good tick, so there is a version to condition on.
        runner.send_heartbeat().await.expect("first beat");

        // Someone else writes the key. Membership is untouched, so this
        // incarnation still believes — correctly — that it is registered.
        heartbeats
            .heartbeat("test:50051", other, 60_000, 30_000)
            .await
            .expect("other beat");

        wrapper
            .failing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        runner
            .send_heartbeat()
            .await
            .expect("a contended beat during a read outage must not fail the loop");

        let beat = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            beat.instance_id, other,
            "a write during a read outage must stay conditional and lose to the newer beat"
        );
        assert_eq!(
            beat.last_heartbeat_ms, 60_000,
            "the other beat must be left exactly as written"
        );
    }

    /// Reads failing from the moment the process registers must not cost it its
    /// liveness. The unconditional first write seeds a predicate, so the beat can
    /// still be refreshed conditionally without ever having read the object.
    #[tokio::test]
    async fn a_read_outage_from_birth_still_refreshes_the_heartbeat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(FailingBeatReads {
            inner: Arc::clone(&inner),
            failing: std::sync::atomic::AtomicBool::new(false),
            beat_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");
        let observer = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));

        let instance_id = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");

        // Reads fail from here on, and this incarnation has never completed a
        // conditional write — only the unconditional registration one.
        wrapper
            .failing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let writes_before = wrapper
            .beat_writes
            .load(std::sync::atomic::Ordering::Relaxed);
        runner
            .send_heartbeat()
            .await
            .expect("a read outage from birth must not fail the loop");

        assert_eq!(
            wrapper
                .beat_writes
                .load(std::sync::atomic::Ordering::Relaxed),
            writes_before + 1,
            "the beat must be refreshed from the version the registration write reported"
        );
        let beat = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            beat.instance_id, instance_id,
            "the refreshed beat must belong to this incarnation"
        );
    }

    /// A retained predicate that no longer matches must not be escalated into an
    /// unconditional write: the beat has moved on, this incarnation cannot read
    /// it, and so it skips rather than displacing whatever is there.
    #[tokio::test]
    async fn a_stale_predicate_does_not_displace_a_newer_beat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wrapper = Arc::new(FailingBeatReads {
            inner: Arc::clone(&inner),
            failing: std::sync::atomic::AtomicBool::new(false),
            beat_writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");
        let observer = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&inner), ""));

        let instance_id = Uuid::new_v4();
        let foreign = Uuid::new_v4();
        let runner = runner_over(&cluster, &heartbeats, &inner, instance_id);
        runner.register_self().await.expect("register");
        // Someone else's beat replaces the one registration wrote, so the version
        // this incarnation retained is now stale.
        heartbeats
            .heartbeat("test:50051", foreign, 40_000, 30_000)
            .await
            .expect("foreign beat");

        wrapper
            .failing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        runner.send_heartbeat().await.expect("skip must not fail");

        let beat = observer
            .read("test:50051")
            .await
            .expect("read")
            .expect("present");
        assert_eq!(
            beat.instance_id, foreign,
            "a predicate that no longer matches must not be escalated to a blind write"
        );
    }

    /// Store that lets a successor take over *between* the heartbeat read and
    /// the conditional write, which is the only way to reach the CAS-conflict
    /// branch: installing the successor beforehand makes the initial membership
    /// check retire first, so the conditional write is never attempted.
    ///
    /// The successor claims membership as well as the heartbeat, because that is
    /// what a real takeover does — `register_self` mutates `cluster.json` before
    /// it beats. A heartbeat-only writer has no ownership claim, and reclaiming
    /// the key from one is correct rather than a clobber.
    #[derive(Debug)]
    struct PublishBetweenReadAndWrite {
        inner: Arc<dyn ObjectStore>,
        cluster: Arc<ClusterStateStore>,
        successor: Uuid,
        armed: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for PublishBetweenReadAndWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PublishBetweenReadAndWrite")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for PublishBetweenReadAndWrite {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let result = self.inner.get_opts(location, options).await;
            // After the heartbeat has been read once, let the successor take the
            // key. The pending conditional write then finds a changed object.
            if location.as_ref().contains("heartbeats/")
                && self.armed.swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                self.cluster
                    .mutate(|state| {
                        if let Some(entry) = state.schedulers.get_mut("test:50051") {
                            entry.instance_id = self.successor;
                            entry.started_at_ms = 50_000;
                        }
                        MutationOutcome::Apply
                    })
                    .await
                    .expect("successor claims membership");
                let beat = crate::cluster::heartbeat::SchedulerHeartbeat {
                    scheduler_id: "test:50051".to_string(),
                    instance_id: self.successor,
                    last_heartbeat_ms: 50_000,
                    ttl_ms: 30_000,
                };
                self.inner
                    .put_opts(
                        location,
                        serde_json::to_vec(&beat).expect("serialize").into(),
                        object_store::PutOptions::from(object_store::PutMode::Overwrite),
                    )
                    .await?;
            }
            result
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The interleaving the conditional write exists for: membership still names
    /// this incarnation when it checks, and the successor publishes after the
    /// heartbeat read but before the write lands. The write must lose its
    /// precondition and leave the successor's beat intact.
    #[tokio::test]
    async fn a_successor_publishing_mid_write_is_not_clobbered() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let successor = Uuid::new_v4();
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let wrapper = Arc::new(PublishBetweenReadAndWrite {
            inner: Arc::clone(&inner),
            cluster: Arc::clone(&cluster),
            successor,
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let store: Arc<dyn ObjectStore> = Arc::clone(&wrapper) as Arc<dyn ObjectStore>;
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&inner), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };
        // Registered and heartbeating normally, so the membership check passes
        // and the conditional write is actually attempted.
        runner.register_self().await.expect("register");

        // Arm the interleaving for the next heartbeat read.
        wrapper
            .armed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        runner
            .send_heartbeat()
            .await
            .expect("a contended heartbeat must not fail the loop");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat present");
        assert_eq!(
            beat.instance_id, successor,
            "a successor that took over mid-write must not be clobbered"
        );
    }

    /// Object store whose `get` never returns, standing in for a hung backend.
    /// Everything else delegates to an in-memory store so the heartbeat write
    /// under test still works.
    #[derive(Debug)]
    struct HangingReads(Arc<dyn ObjectStore>);

    impl std::fmt::Display for HangingReads {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HangingReads")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for HangingReads {
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            // Hang only on the cluster document. The heartbeat path reads its
            // own object to write conditionally, and hanging that too would
            // test a different failure than the one under examination.
            if location.as_ref().contains("cluster.json") {
                return std::future::pending().await;
            }
            self.0.get_opts(location, options).await
        }
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.0.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    /// A hung membership read must not suppress the heartbeat. This is the
    /// branch that keeps a healthy scheduler from being reaped when the store
    /// stalls rather than errors, so it is exercised directly.
    #[tokio::test(start_paused = true)]
    async fn a_hung_cluster_read_does_not_suppress_the_heartbeat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = Arc::new(HangingReads(Arc::clone(&inner)));
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));

        let instance_id = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster,
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(
                Arc::new(ClusterStateStore::new(Arc::clone(&store), "")),
                Arc::clone(&heartbeats),
            ),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };

        // Paused time auto-advances once the read is the only pending work, so
        // the deadline fires without a real wait.
        runner
            .send_heartbeat()
            .await
            .expect("a hung membership read must not fail the heartbeat");

        let beat = inner
            .get(&heartbeats.path_for("test:50051"))
            .await
            .expect("the heartbeat must still have been written");
        let bytes = beat.bytes().await.expect("bytes");
        let beat: crate::cluster::heartbeat::SchedulerHeartbeat =
            serde_json::from_slice(&bytes).expect("parse");
        assert_eq!(beat.instance_id, instance_id);
    }

    /// A conditional write proves only that the object did not change — not
    /// that the writer owns the id. So when membership is unreadable and the
    /// heartbeat belongs to a *successor*, the beat must be skipped: otherwise a
    /// superseded incarnation refreshes over the live one every tick for as
    /// long as `cluster.json` stays slow.
    #[tokio::test]
    async fn an_unconfirmed_incarnation_does_not_overwrite_a_successors_heartbeat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = Arc::new(HangingReads(Arc::clone(&inner)));
        // Seed membership and the successor's heartbeat through the inner store,
        // so the wrapper only affects later cluster reads.
        let seed_cluster = Arc::new(ClusterStateStore::new(Arc::clone(&inner), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        seed_cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let successor = Uuid::new_v4();
        heartbeats
            .heartbeat("test:50051", successor, 20_000, 30_000)
            .await
            .expect("successor heartbeat");

        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&inner), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            // Cluster reads go through the hanging wrapper: membership unknown.
            cluster: Arc::new(ClusterStateStore::new(Arc::clone(&store), "")),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(
                Arc::new(ClusterStateStore::new(Arc::clone(&store), "")),
                Arc::clone(&heartbeats),
            ),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };

        runner
            .send_heartbeat()
            .await
            .expect("an unconfirmed beat must not fail the loop");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat present");
        assert_eq!(
            beat.instance_id, successor,
            "an incarnation that cannot prove membership must not overwrite a successor's heartbeat"
        );
        assert_eq!(beat.last_heartbeat_ms, 20_000);
    }

    /// The mirror case: our *own* beat with membership unreadable must still be
    /// refreshed. Skipping here would let the TTL lapse and get a healthy
    /// scheduler reaped — the failure this whole change exists to prevent.
    #[tokio::test]
    async fn an_unconfirmed_incarnation_still_refreshes_its_own_heartbeat() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = Arc::new(HangingReads(Arc::clone(&inner)));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));

        let instance_id = Uuid::new_v4();
        heartbeats
            .heartbeat("test:50051", instance_id, 1_000, 30_000)
            .await
            .expect("own heartbeat");

        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&inner), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::new(ClusterStateStore::new(Arc::clone(&store), "")),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(
                Arc::new(ClusterStateStore::new(Arc::clone(&store), "")),
                Arc::clone(&heartbeats),
            ),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };

        runner
            .send_heartbeat()
            .await
            .expect("refreshing our own beat must succeed");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat present");
        assert_eq!(beat.instance_id, instance_id);
        assert!(
            beat.last_heartbeat_ms > 1_000,
            "our own heartbeat must be refreshed even when membership is unreadable"
        );
    }

    /// A successor that commits its membership entry and dies before its first
    /// heartbeat leaves the *predecessor's* beat under a shared key. That must
    /// not lock the id: registration has to judge the entry by its own beat,
    /// treat an unrelated one as absent, and take over after the grace window.
    #[tokio::test]
    async fn a_successor_that_died_before_its_first_heartbeat_does_not_lock_the_id() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");

        let predecessor = Uuid::new_v4();
        let dead_successor = Uuid::new_v4();
        let newcomer = Uuid::new_v4();

        // Membership names a successor that never published a beat, while the
        // shared key still holds the predecessor's.
        let mut dead_entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id: dead_successor,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        cluster
            .mutate(|state| {
                state
                    .schedulers
                    .insert("test:50051".to_string(), dead_entry.clone());
                MutationOutcome::Apply
            })
            .await
            .expect("seed the crashed successor");
        // Long stale: a *fresh* foreign beat deliberately still blocks
        // takeover, because something is actively writing the key.
        heartbeats
            .heartbeat("test:50051", predecessor, 1, 1)
            .await
            .expect("predecessor heartbeat");

        // A fresh process starts long after the grace window.
        dead_entry.instance_id = newcomer;
        let entry = dead_entry;
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", newcomer.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id: newcomer,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };

        runner
            .register_self()
            .await
            .expect("an unrelated heartbeat must not block takeover");

        let snap = cluster.read().await.expect("read");
        assert_eq!(
            snap.schedulers
                .get("test:50051")
                .map(|entry| entry.instance_id),
            Some(newcomer),
            "the id must be recoverable, not locked by a stale unrelated heartbeat"
        );
    }

    /// A superseded incarnation must not delete the heartbeat key on the way
    /// out: the key is shared, so deleting it removes the *successor's*
    /// liveness and makes it look orphaned until its next tick.
    #[tokio::test]
    async fn a_superseded_incarnation_does_not_delete_the_successors_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        cluster.bootstrap().await.expect("bootstrap");

        let instance_id = Uuid::new_v4();
        let successor = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry: entry.clone(),
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };
        runner.register_self().await.expect("register");

        // A successor takes the id over and publishes its own liveness.
        let mut successor_entry = entry;
        successor_entry.instance_id = successor;
        cluster
            .mutate(|state| {
                state
                    .schedulers
                    .insert("test:50051".to_string(), successor_entry.clone());
                MutationOutcome::Apply
            })
            .await
            .expect("successor takes over");
        heartbeats
            .heartbeat("test:50051", successor, 10_000, 30_000)
            .await
            .expect("successor heartbeat");

        runner.shutdown().await;

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("the successor's heartbeat must survive the old incarnation's shutdown");
        assert_eq!(beat.instance_id, successor);
        let snap = cluster.read().await.expect("read");
        assert_eq!(
            snap.schedulers.get("test:50051").map(|e| e.instance_id),
            Some(successor),
            "shutdown must not remove the successor's membership entry"
        );
    }

    /// Heartbeats deliberately live outside `cluster.json`. Gating them on a
    /// read of that document must fail OPEN: a slow or failing GET is not
    /// evidence of supersession, and suppressing the beat would let peers reap
    /// a healthy scheduler and re-drive its jobs.
    #[tokio::test]
    async fn a_failed_cluster_read_does_not_suppress_the_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
        // Deliberately NOT bootstrapped: `cluster.read()` fails, standing in
        // for a timed-out or erroring GET of the cluster document.

        let instance_id = Uuid::new_v4();
        let entry = SchedulerEntry {
            scheduler_id: "test:50051".to_string(),
            instance_id,
            advertise_address: "test:50051".to_string(),
            grpc_address: "test:50051".to_string(),
            http_address: "test:8090".to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        };
        let job_store = crate::jobs::JobStore::new(Arc::clone(&store), "", instance_id.to_string());
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .build();
        let job_executor = Arc::new(crate::jobs::JobExecutor::new(
            Arc::new(job_store),
            Arc::new(df),
        ));
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
            job_executor,
            superseded: Arc::new(AtomicBool::new(false)),
            last_written_version: Arc::new(ArcSwapOption::empty()),
        };

        runner
            .send_heartbeat()
            .await
            .expect("a failed cluster read must not fail the heartbeat");

        let beat = heartbeats
            .read("test:50051")
            .await
            .expect("read hb")
            .expect("heartbeat must still have been written");
        assert_eq!(beat.instance_id, instance_id);
        assert!(
            !runner.superseded.load(Ordering::Relaxed),
            "a read failure is not evidence of supersession"
        );
    }
}
