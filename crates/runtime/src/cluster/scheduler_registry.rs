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
    self, ClusterStateStore, MutateError, MutationOutcome, SchedulerEntry,
};
use crate::cluster::heartbeat::{self, CLOCK_SKEW_TOLERANCE_MS, SchedulerHeartbeatStore};
use crate::cluster::reaper::Reaper;
use crate::metrics::cluster as cluster_metrics;

const DEFAULT_TTL_MS: u64 = 30_000;
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);
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
        scheduler_id.clone(),
    );
    let job_executor = Arc::new(crate::jobs::JobExecutor::new(
        Arc::new(job_store),
        rt.datafusion(),
    ));
    rt.datafusion().set_job_executor(Arc::clone(&job_executor));
    rt.set_job_executor(job_executor).await;
    tracing::info!(
        "Initialized async SQL jobs API with state location: {}",
        config.state_location
    );

    let reaper = Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats));

    let runner = SchedulerRegistryRunner {
        cluster,
        heartbeats,
        reaper,
        scheduler_id,
        instance_id,
        entry,
        peers,
    };

    runner.run(cancel).await
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
        if let Err(err) = self
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
            tracing::warn!("Failed to remove scheduler entry on shutdown: {err}");
        }
        if let Err(err) = self.heartbeats.delete(&self.scheduler_id).await {
            tracing::warn!("Failed to delete heartbeat on shutdown: {err}");
        }
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
        let runner = SchedulerRegistryRunner {
            cluster: Arc::clone(&cluster),
            heartbeats: Arc::clone(&heartbeats),
            reaper: Reaper::new(Arc::clone(&cluster), Arc::clone(&heartbeats)),
            scheduler_id: "test:50051".to_string(),
            instance_id,
            entry,
            peers: Arc::new(RwLock::new(HashMap::new())),
        };
        runner.register_self().await.expect("register");

        let snap = cluster.read().await.expect("read");
        assert!(snap.schedulers.contains_key("test:50051"));
        let beat = heartbeats.read("test:50051").await.expect("read hb");
        assert!(beat.is_some());

        runner.shutdown().await;
        let snap = cluster.read().await.expect("read");
        assert!(!snap.schedulers.contains_key("test:50051"));
        let beat = heartbeats.read("test:50051").await.expect("read hb");
        assert!(beat.is_none());
    }
}
