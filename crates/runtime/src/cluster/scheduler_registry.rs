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
            .read(&self.scheduler_id)
            .await
            .context(HeartbeatSnafu)?;
        let observed_instance = observed.as_ref().map(|b| b.instance_id);

        let entry = self.entry.clone();
        let scheduler_id = self.scheduler_id.clone();
        let instance_id = self.instance_id;

        let now = now_ms()?;

        let res = self
            .cluster
            .mutate(|state| {
                if let Some(existing) = state.schedulers.get(&scheduler_id) {
                    // Allow takeover only if (a) the heartbeat we
                    // observed matches the existing entry's
                    // instance_id (so we are taking over the
                    // incarnation we judged stale), AND (b) that
                    // observation is in fact stale, OR there is no
                    // heartbeat at all and the existing entry has
                    // had time to publish one.
                    let observed_matches =
                        observed_instance.is_none_or(|i| i == existing.instance_id);
                    let stale_or_missing = if let Some(beat) = observed.as_ref() {
                        beat.is_stale(now)
                    } else {
                        let grace = existing.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS);
                        now.saturating_sub(existing.started_at_ms) > grace
                    };
                    if observed_matches && stale_or_missing {
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

        // Write our first heartbeat so peers see liveness on next discovery.
        self.heartbeats
            .heartbeat(&self.scheduler_id, self.instance_id, now, self.entry.ttl_ms)
            .await
            .context(HeartbeatSnafu)?;
        Ok(())
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

        // Only a *successful* read is evidence of supersession. Heartbeats
        // deliberately live outside `cluster.json` so the high-frequency write
        // path does not depend on that document; making emission conditional
        // on reading it would mean a slow or failing GET silently suppresses
        // beats and self-evicts a healthy scheduler — producing exactly the
        // false-dead job re-drive this change exists to prevent. So fail open
        // on a read error and only decline on a definitive observation.
        // Bound the read. Failing open covers an *erroring* store, but a hung
        // or very slow GET would block this loop before the heartbeat write,
        // and once that exceeds the TTL peers reap this healthy scheduler and
        // re-drive its jobs — the exact failure the fail-open behaviour exists
        // to prevent. A timeout is treated exactly like an error: heartbeat
        // anyway.
        let read_timeout = membership_check_timeout(self.entry.ttl_ms);
        let membership = match tokio::time::timeout(read_timeout, self.cluster.read()).await {
            Ok(result) => result,
            Err(_elapsed) => {
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    timeout_ms = read_timeout.as_millis(),
                    "Timed out confirming cluster membership; heartbeating anyway"
                );
                return self.write_heartbeat().await;
            }
        };
        match membership {
            Ok(state) => {
                let registered = state
                    .schedulers
                    .get(&self.scheduler_id)
                    .map(|entry| entry.instance_id);
                if registered != Some(self.instance_id) {
                    self.superseded.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        scheduler_id = %self.scheduler_id,
                        instance_id = %self.instance_id,
                        registered = ?registered,
                        "This scheduler incarnation is no longer registered; it will stop heartbeating"
                    );
                    return Ok(());
                }
            }
            Err(err) => {
                tracing::warn!(
                    scheduler_id = %self.scheduler_id,
                    error = %err,
                    "Could not confirm cluster membership; heartbeating anyway"
                );
            }
        }

        self.write_heartbeat().await
    }

    async fn write_heartbeat(&self) -> Result<()> {
        let now = now_ms()?;
        self.heartbeats
            .heartbeat(&self.scheduler_id, self.instance_id, now, self.entry.ttl_ms)
            .await
            .context(HeartbeatSnafu)
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
