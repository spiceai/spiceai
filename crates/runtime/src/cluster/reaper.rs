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

//! Reaper that evicts dead schedulers from `cluster.json`.
//!
//! Heartbeat files are deliberately *not* deleted by the reaper. Object
//! stores have no conditional-delete primitive, so deleting after a
//! takeover would race the new process's freshly written heartbeat.
//! Orphan heartbeats (those whose `instance_id` no longer matches any
//! entry in `cluster.json`) are filtered out at read time by
//! [`crate::cluster::heartbeat::SchedulerHeartbeatStore::list_alive`],
//! so leaving them in place is harmless.

use std::sync::Arc;

use snafu::Snafu;
use uuid::Uuid;

use crate::cluster::cluster_state::{
    ClusterStateStore, MutateError, MutateOk, MutationOutcome, SchedulerId,
};
use crate::cluster::heartbeat::{self, CLOCK_SKEW_TOLERANCE_MS, SchedulerHeartbeatStore};

/// Errors that can be raised during a single reaper tick.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read cluster state in reaper: {source}"))]
    ClusterRead { source: MutateError },

    #[snafu(display("Failed to mutate cluster state in reaper: {source}"))]
    ClusterMutate { source: MutateError },

    #[snafu(display("Failed to list scheduler heartbeats in reaper: {source}"))]
    ListHeartbeats { source: heartbeat::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Outcome of one reaper tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReaperOutcome {
    pub evicted: Vec<SchedulerId>,
    pub skipped: Vec<SchedulerId>,
}

#[derive(Debug)]
pub struct Reaper {
    cluster: Arc<ClusterStateStore>,
    heartbeats: Arc<SchedulerHeartbeatStore>,
}

impl Reaper {
    #[must_use]
    pub fn new(cluster: Arc<ClusterStateStore>, heartbeats: Arc<SchedulerHeartbeatStore>) -> Self {
        Self {
            cluster,
            heartbeats,
        }
    }

    /// Run a single reap pass. Returns the set of scheduler ids that
    /// were evicted from `cluster.json` and any candidates that were
    /// skipped because of an `instance_id` mismatch (likely takeover).
    pub async fn tick(&self, now_ms: u64) -> Result<ReaperOutcome> {
        let snapshot = self
            .cluster
            .read()
            .await
            .map_err(|source| Error::ClusterRead { source })?;
        let heartbeats = self
            .heartbeats
            .list_all()
            .await
            .map_err(|source| Error::ListHeartbeats { source })?;

        // Build the (id -> instance_id we observed as stale-or-missing) map.
        let mut stale: Vec<(SchedulerId, Uuid)> = Vec::new();
        for (id, entry) in &snapshot.schedulers {
            match heartbeats.get(id) {
                Some(beat) if beat.instance_id == entry.instance_id => {
                    if beat.is_stale(now_ms) {
                        stale.push((id.clone(), entry.instance_id));
                    }
                }
                Some(_) => {
                    // Heartbeat carries a different instance_id (orphan or
                    // mid-takeover). Don't reap.
                }
                None => {
                    // No heartbeat at all. Only reap if the entry has
                    // had time to publish its first heartbeat.
                    let grace = entry.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS);
                    if now_ms.saturating_sub(entry.started_at_ms) > grace {
                        stale.push((id.clone(), entry.instance_id));
                    }
                }
            }
        }

        let mut outcome = ReaperOutcome::default();
        for (id, observed_instance) in stale {
            let id_for_closure = id.clone();
            let res = self
                .cluster
                .mutate(|state| match state.schedulers.get(&id_for_closure) {
                    Some(entry) if entry.instance_id == observed_instance => {
                        state.schedulers.remove(&id_for_closure);
                        MutationOutcome::Apply
                    }
                    _ => MutationOutcome::NoChange,
                })
                .await
                .map_err(|source| Error::ClusterMutate { source })?;
            match res {
                MutateOk::Committed => {
                    tracing::info!(
                        scheduler_id = %id,
                        instance_id = %observed_instance,
                        "Reaped stale scheduler"
                    );
                    outcome.evicted.push(id);
                }
                MutateOk::AlreadySatisfied => {
                    outcome.skipped.push(id);
                }
            }
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::cluster_state::{MutationOutcome, SchedulerEntry};
    use crate::cluster::heartbeat::SchedulerHeartbeatStore;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use std::collections::HashMap;

    fn entry(id: &str, instance: Uuid, started_at_ms: u64, ttl_ms: u64) -> SchedulerEntry {
        SchedulerEntry {
            scheduler_id: id.to_string(),
            instance_id: instance,
            advertise_address: id.to_string(),
            grpc_address: id.to_string(),
            http_address: id.to_string(),
            started_at_ms,
            ttl_ms,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        }
    }

    async fn setup() -> (Arc<ClusterStateStore>, Arc<SchedulerHeartbeatStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
        cs.bootstrap().await.expect("bootstrap");
        let hb = Arc::new(SchedulerHeartbeatStore::new(store, ""));
        (cs, hb)
    }

    async fn add_entry(cs: &ClusterStateStore, e: SchedulerEntry) {
        cs.mutate(|s| {
            s.schedulers.insert(e.scheduler_id.clone(), e.clone());
            MutationOutcome::Apply
        })
        .await
        .expect("mutate");
    }

    #[tokio::test]
    async fn reaps_scheduler_with_stale_heartbeat() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");

        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick(1_000_000).await.expect("tick");
        assert_eq!(out.evicted, vec!["a".to_string()]);
        let snap = cs.read().await.expect("read");
        assert!(!snap.schedulers.contains_key("a"));
    }

    #[tokio::test]
    async fn reaps_scheduler_with_missing_heartbeat_after_grace_period() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        // No heartbeat written.
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick(1_000_000).await.expect("tick");
        assert_eq!(out.evicted, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn does_not_reap_scheduler_with_fresh_heartbeat() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 999_000, 30_000).await.expect("hb");
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick(1_000_000).await.expect("tick");
        assert!(out.evicted.is_empty());
    }

    #[tokio::test]
    async fn does_not_reap_recently_registered_scheduler_without_heartbeat() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 990_000, 30_000)).await;
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick(1_000_000).await.expect("tick");
        assert!(out.evicted.is_empty());
    }

    #[tokio::test]
    async fn reaper_skips_when_instance_id_does_not_match() {
        let (cs, hb) = setup().await;
        let old = Uuid::new_v4();
        let new = Uuid::new_v4();
        // cluster.json carries the new (post-takeover) instance id.
        add_entry(&cs, entry("a", new, 999_000, 30_000)).await;
        // Heartbeat file still carries the old instance id (orphan
        // about to be overwritten by the new process).
        hb.heartbeat("a", old, 1_000, 30_000).await.expect("hb");
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick(1_000_000).await.expect("tick");
        assert!(out.evicted.is_empty());
        let snap = cs.read().await.expect("read");
        assert!(snap.schedulers.contains_key("a"));
    }

    #[tokio::test]
    async fn concurrent_reapers_idempotent() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");
        let r1 = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let r2 = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let (o1, o2) = tokio::join!(r1.tick(1_000_000), r2.tick(1_000_000));
        let o1 = o1.expect("o1");
        let o2 = o2.expect("o2");
        let total: Vec<_> = o1
            .evicted
            .iter()
            .chain(o2.evicted.iter())
            .cloned()
            .collect();
        // Eviction should happen exactly once across the two reapers.
        assert_eq!(total, vec!["a".to_string()]);
        // The other reaper either skipped (saw entry, mutator returned
        // NoChange against fresh state) or saw nothing-to-do because the
        // first reaper had already committed by the time it read the
        // cluster snapshot.
        assert!(o1.skipped.len() + o2.skipped.len() <= 1);
    }

    #[tokio::test]
    async fn reaper_does_not_delete_heartbeat_files() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        r.tick(1_000_000).await.expect("tick");
        // Heartbeat file is still present (orphan now, but not deleted
        // by the reaper).
        let beat = hb.read("a").await.expect("read");
        assert!(beat.is_some());
    }
}
