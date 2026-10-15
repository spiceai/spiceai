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
//! A scheduler is dead once its heartbeat has lapsed in this process's
//! observation (see [`crate::cluster::heartbeat`]), or, when it has no
//! heartbeat object, once this process has seen it without one for its TTL. A
//! reaper that has just started therefore evicts nothing for its first TTL.
//!
//! Heartbeat files are deliberately *not* deleted by the reaper. Object
//! stores have no conditional-delete primitive, so deleting after a
//! takeover would race the new process's freshly written heartbeat.
//! Orphan heartbeats (those whose `instance_id` no longer matches any
//! entry in `cluster.json`) are filtered out at read time by
//! [`crate::cluster::heartbeat::SchedulerHeartbeatStore::list_alive`],
//! so leaving them in place is harmless.

use std::sync::Arc;
use std::time::Duration;

use snafu::Snafu;
use uuid::Uuid;

use crate::cluster::cluster_state::{
    ClusterStateStore, MutateError, MutateOk, MutationOutcome, SchedulerId,
};
use crate::cluster::heartbeat::{self, SchedulerHeartbeatStore};

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
    pub async fn tick(&self) -> Result<ReaperOutcome> {
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
                Some(observed) if observed.beat.instance_id == entry.instance_id => {
                    if observed.lapsed() {
                        stale.push((id.clone(), entry.instance_id));
                    }
                }
                Some(_) => {
                    // Heartbeat carries a different instance_id (orphan or
                    // mid-takeover). Don't reap.
                }
                None => {
                    // No heartbeat at all. Only reap once the entry has had
                    // its TTL, in this process's observation, to publish one.
                    if self.heartbeats.observe(id, None) >= Duration::from_millis(entry.ttl_ms) {
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

    const TTL: Duration = Duration::from_secs(30);

    fn wall_clock_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("fits")
    }

    #[tokio::test(start_paused = true)]
    async fn reaps_scheduler_whose_heartbeat_stays_unchanged_for_its_ttl() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");

        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        // A reaper that has just started has seen no version yet.
        assert!(r.tick().await.expect("tick").evicted.is_empty());
        tokio::time::advance(TTL - Duration::from_secs(1)).await;
        assert!(r.tick().await.expect("tick").evicted.is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        let out = r.tick().await.expect("tick");
        assert_eq!(out.evicted, vec!["a".to_string()]);
        let snap = cs.read().await.expect("read");
        assert!(!snap.schedulers.contains_key("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn reaps_scheduler_seen_without_a_heartbeat_for_its_ttl() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        // No heartbeat written.
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        assert!(r.tick().await.expect("tick").evicted.is_empty());
        tokio::time::advance(TTL).await;
        let out = r.tick().await.expect("tick");
        assert_eq!(out.evicted, vec!["a".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_reap_scheduler_whose_heartbeat_keeps_changing() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        for beat in 0..10 {
            hb.heartbeat("a", id, 1_000 + beat * 10_000, 30_000)
                .await
                .expect("hb");
            assert!(r.tick().await.expect("tick").evicted.is_empty());
            tokio::time::advance(Duration::from_secs(10)).await;
        }
    }

    /// Clock skew between schedulers cannot get a live one reaped: its
    /// heartbeat timestamps a minute behind or ahead of the reaper's clock are
    /// never compared with that clock.
    #[tokio::test(start_paused = true)]
    async fn does_not_reap_a_live_scheduler_whose_clock_is_a_minute_off() {
        for skew_ms in [-60_000_i64, 60_000] {
            let (cs, hb) = setup().await;
            let id = Uuid::new_v4();
            add_entry(&cs, entry("a", id, 0, 30_000)).await;
            let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
            for beat in 0..10_u64 {
                let written = wall_clock_ms()
                    .saturating_add_signed(skew_ms)
                    .saturating_add(beat * 10_000);
                hb.heartbeat("a", id, written, 30_000).await.expect("hb");
                assert!(
                    r.tick().await.expect("tick").evicted.is_empty(),
                    "a scheduler with a clock {skew_ms}ms off was reaped while live"
                );
                tokio::time::advance(Duration::from_secs(10)).await;
            }
        }
    }

    #[tokio::test]
    async fn does_not_reap_recently_registered_scheduler_without_heartbeat() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 990_000, 30_000)).await;
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let out = r.tick().await.expect("tick");
        assert!(out.evicted.is_empty());
    }

    #[tokio::test(start_paused = true)]
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
        assert!(r.tick().await.expect("tick").evicted.is_empty());
        tokio::time::advance(TTL * 2).await;
        let out = r.tick().await.expect("tick");
        assert!(out.evicted.is_empty());
        let snap = cs.read().await.expect("read");
        assert!(snap.schedulers.contains_key("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_reapers_idempotent() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");
        let r1 = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        let r2 = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        r1.tick().await.expect("first observation");
        tokio::time::advance(TTL).await;
        let (o1, o2) = tokio::join!(r1.tick(), r2.tick());
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

    #[tokio::test(start_paused = true)]
    async fn reaper_does_not_delete_heartbeat_files() {
        let (cs, hb) = setup().await;
        let id = Uuid::new_v4();
        add_entry(&cs, entry("a", id, 0, 30_000)).await;
        hb.heartbeat("a", id, 1_000, 30_000).await.expect("hb");
        let r = Reaper::new(Arc::clone(&cs), Arc::clone(&hb));
        r.tick().await.expect("first observation");
        tokio::time::advance(TTL).await;
        let out = r.tick().await.expect("tick");
        // The reaper ran and evicted the stale scheduler...
        assert_eq!(out.evicted, vec!["a".to_string()]);
        // ...but its heartbeat file is still present and unchanged (an orphan
        // now, but not deleted by the reaper).
        let beat = hb.read("a").await.expect("read");
        assert_eq!(
            beat,
            Some(heartbeat::SchedulerHeartbeat {
                scheduler_id: "a".to_string(),
                instance_id: id,
                last_heartbeat_ms: 1_000,
                ttl_ms: 30_000,
            })
        );
    }
}
