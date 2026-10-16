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

//! Per-scheduler heartbeat files at `heartbeats/{scheduler_id}.json`.
//!
//! Heartbeats live outside [`crate::cluster::cluster_state::ClusterState`]
//! so that the high-frequency write path does not contend with partition
//! and membership writes against the single cluster document.
//!
//! Each heartbeat file carries the writer's `instance_id`. A heartbeat
//! whose `instance_id` does not match the corresponding entry in
//! `cluster.json` is treated as orphan and ignored — this lets a fresh
//! process safely take over a crashed predecessor's `scheduler_id` even
//! though the underlying object store has no conditional-delete primitive.
//!
//! A scheduler is gone once its heartbeat has lapsed: the reading process has
//! seen the same version of the heartbeat object for the TTL the heartbeat
//! carries, on its own monotonic clock ([`object_store_occ::lease::LapseClock`]).
//! `last_heartbeat_ms` is the writer's wall clock and is never compared with
//! the reader's, so clock skew between schedulers cannot make a live one look
//! gone. A process that has just started has seen no version yet, so it judges
//! no scheduler gone for its first TTL.
//!
//! A registered scheduler whose heartbeat file is missing counts as alive until
//! the reader has seen the file missing for its TTL: the file of a scheduler
//! that restarts under its name can be deleted by another scheduler's cleanup,
//! and the scheduler writes it again at its next beat.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::UpdateVersion;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt};
use object_store_occ::lease::{LapseClock, RenewClock};
use object_store_occ::{ConditionalWriteError, Expected, conditional_put};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio::time::Instant;
use uuid::Uuid;

use crate::cluster::cluster_state::{ClusterState, SchedulerId};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read heartbeat at {path}: {source}"))]
    Read {
        path: String,
        source: ObjectStoreError,
    },
    #[snafu(display("Failed to write heartbeat: {source}"))]
    Write { source: ConditionalWriteError },
    #[snafu(display("Failed to delete heartbeat at {path}: {source}"))]
    Delete {
        path: String,
        source: ObjectStoreError,
    },
    #[snafu(display("Failed to list heartbeats at {prefix}: {source}"))]
    List {
        prefix: String,
        source: ObjectStoreError,
    },
    #[snafu(display("Failed to (de)serialize heartbeat: {source}"))]
    Serde { source: serde_json::Error },
    #[snafu(display(
        "Heartbeat for {scheduler_id} was written by another incarnation during this write"
    ))]
    HeartbeatSupersededDuringWrite { scheduler_id: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// On-disk heartbeat record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerHeartbeat {
    pub scheduler_id: SchedulerId,
    /// Must match the `instance_id` of the corresponding entry in
    /// `cluster.json`. Mismatched heartbeats are filtered out as
    /// orphans by [`SchedulerHeartbeatStore::list_alive`].
    pub instance_id: Uuid,
    /// The writer's wall clock when it wrote the beat, later than its previous
    /// beat. Readers judge liveness by the object's version, not by this.
    pub last_heartbeat_ms: u64,
    pub ttl_ms: u64,
}

/// A heartbeat as read, and how long the reading process has seen this
/// version of it unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedHeartbeat {
    pub beat: SchedulerHeartbeat,
    pub unchanged_for: Duration,
}

impl ObservedHeartbeat {
    /// Whether the heartbeat has lapsed: unchanged for its TTL.
    #[must_use]
    pub fn lapsed(&self) -> bool {
        self.unchanged_for >= Duration::from_millis(self.beat.ttl_ms)
    }
}

/// Object-store-backed heartbeat store.
///
/// No internal cache: the discovery tick and the reaper both want a
/// fresh view, the file count is bounded by the number of schedulers,
/// and skipping the cache eliminates the "ghost key after delete" edge
/// case that exists in [`object_store_occ::ObjectState::refresh`].
#[derive(Debug)]
pub struct SchedulerHeartbeatStore {
    store: Arc<dyn ObjectStore>,
    /// e.g. `prefix/heartbeats/` or `heartbeats/`. Always ends with `/`.
    prefix: String,
    /// How long this process has seen each scheduler's heartbeat object, or
    /// its absence under one registration, unchanged. Keyed by scheduler id.
    lapse: LapseClock,
    /// Renewal times of this process's heartbeats, so each beat it writes
    /// changes the object's version.
    renew: RenewClock,
}

impl SchedulerHeartbeatStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base_prefix: &str) -> Self {
        let trimmed = base_prefix.trim_end_matches('/');
        let prefix = if trimmed.is_empty() {
            "heartbeats/".to_string()
        } else {
            format!("{trimmed}/heartbeats/")
        };
        Self {
            store,
            prefix,
            lapse: LapseClock::default(),
            renew: RenewClock::default(),
        }
    }

    /// Records a read of `scheduler_id`'s heartbeat object at `version`
    /// (`None`: no object), started at `read_at`, and returns how long this
    /// process has seen that version unchanged.
    pub fn observe(
        &self,
        scheduler_id: &str,
        version: Option<&UpdateVersion>,
        read_at: Instant,
    ) -> Duration {
        self.lapse.observe(scheduler_id, version, read_at)
    }

    /// Records that `scheduler_id`, registered by `instance_id`, has no
    /// heartbeat object in a listing started at `listed_at`, and returns how
    /// long this process has seen it so. Absence under another registration is
    /// another state, so each registration has its own TTL to publish its
    /// first beat.
    pub fn observe_missing(
        &self,
        scheduler_id: &str,
        instance_id: Uuid,
        listed_at: Instant,
    ) -> Duration {
        let absent = UpdateVersion {
            e_tag: None,
            version: Some(format!("absent; registered by {instance_id}")),
        };
        self.lapse.observe(scheduler_id, Some(&absent), listed_at)
    }

    /// Returns the full path for a given scheduler id.
    #[must_use]
    pub fn path_for(&self, scheduler_id: &str) -> Path {
        Path::from(format!("{}{}.json", self.prefix, scheduler_id))
    }

    /// Writes the heartbeat for a scheduler without any condition, standing in
    /// for another incarnation's write in tests. Production code writes through
    /// [`Self::heartbeat_if_unchanged`].
    #[cfg(test)]
    pub async fn heartbeat(
        &self,
        scheduler_id: &str,
        instance_id: Uuid,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Option<UpdateVersion>> {
        let beat = SchedulerHeartbeat {
            scheduler_id: scheduler_id.to_string(),
            instance_id,
            last_heartbeat_ms: now_ms,
            ttl_ms,
        };
        let payload = serde_json::to_vec(&beat).context(SerdeSnafu)?;
        let result = self
            .store
            .put(&self.path_for(scheduler_id), payload.into())
            .await
            .map_err(|source| Error::Write {
                source: ConditionalWriteError::Store {
                    path: self.path_for(scheduler_id).to_string(),
                    source,
                },
            })?;
        Ok(Self::usable_version_of(result.e_tag, result.version))
    }

    /// Reads the heartbeat together with the object version needed to write it
    /// back conditionally. `None` means the object does not exist yet.
    /// The version is what a conditional write needs; the payload is only used
    /// to see *whose* beat it is. An unparsable payload therefore yields
    /// `Some((None, version))` rather than an error: failing here would stop a
    /// healthy scheduler from ever repairing its own key, where the previous
    /// unconditional write self-healed.
    pub async fn read_versioned(
        &self,
        scheduler_id: &str,
    ) -> Result<Option<(Option<SchedulerHeartbeat>, UpdateVersion)>> {
        let path = self.path_for(scheduler_id);
        match self.store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await.map_err(|source| Error::Read {
                    path: path.to_string(),
                    source,
                })?;
                let beat = match serde_json::from_slice::<SchedulerHeartbeat>(&bytes) {
                    Ok(beat) => Some(beat),
                    Err(err) => {
                        tracing::warn!(
                            path = %path,
                            error = %err,
                            "Unparsable heartbeat payload; treating the holder as unknown"
                        );
                        None
                    }
                };
                Ok(Some((beat, version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(source) => Err(Error::Read {
                path: path.to_string(),
                source,
            }),
        }
    }

    /// A version is only usable as a write predicate when it actually carries an
    /// identifier. Some stores return neither an `ETag` nor a version, and passing
    /// an empty predicate would assert a condition the store cannot check.
    fn usable_version_of(e_tag: Option<String>, version: Option<String>) -> Option<UpdateVersion> {
        if e_tag.is_none() && version.is_none() {
            return None;
        }
        Some(UpdateVersion { e_tag, version })
    }

    /// Writes the heartbeat only if the stored object is still the one the
    /// caller observed (`expected`), or is still absent when `expected` is
    /// `None`.
    ///
    /// The key is shared by every incarnation of a `scheduler_id`, so an
    /// unconditional write lets a superseded process clobber the incarnation
    /// that replaced it. A conditional write means that if anyone else wrote in
    /// between, this fails with [`Error::HeartbeatSupersededDuringWrite`]
    /// instead of overwriting them.
    ///
    /// This is **not** an ownership fence. It proves only that the object has
    /// not changed since `expected` was observed; it says nothing about whether
    /// the caller is still the registered incarnation. Callers that need
    /// ownership must check membership separately, as `write_heartbeat` does.
    ///
    /// The beat carries a renewal time later than this process's previous
    /// beat, so every write changes the object's version.
    ///
    /// Returns the version of the object just written, so a caller can keep
    /// writing conditionally even while reads are failing.
    pub async fn heartbeat_if_unchanged(
        &self,
        scheduler_id: &str,
        instance_id: Uuid,
        ttl_ms: u64,
        expected: Option<&UpdateVersion>,
    ) -> Result<Option<UpdateVersion>> {
        let beat = SchedulerHeartbeat {
            scheduler_id: scheduler_id.to_string(),
            instance_id,
            last_heartbeat_ms: u64::try_from(self.renew.next_ms()).unwrap_or_default(),
            ttl_ms,
        };
        let payload = serde_json::to_vec(&beat).context(SerdeSnafu)?;
        let path = self.path_for(scheduler_id);
        let expected = match expected {
            Some(version) => Expected::at_version(version.clone()).ok_or_else(|| Error::Write {
                source: ConditionalWriteError::Unsupported {
                    path: path.to_string(),
                    source: ObjectStoreError::NotSupported {
                        source: "the store reported neither an ETag nor a version".into(),
                    },
                },
            })?,
            None => Expected::Absent,
        };

        // Deliberately a single attempt. Retrying a *conditional* write after an
        // ambiguous failure is what makes a lost acknowledgement dangerous: the
        // retry's precondition fails against a write that may have been our own,
        // which cannot be distinguished from a successor's. The caller treats an
        // unknown outcome as "the retained version can no longer be trusted"
        // instead, and the store's own retry policy still covers transport
        // errors.
        //
        // A conditional update whose target was deleted since it was read has
        // lost the same race, and counts as a conflict too: the caller re-reads
        // and creates the key instead of aborting registration after membership
        // was committed.
        match conditional_put(self.store.as_ref(), &path, payload.into(), &expected).await {
            Ok(result) => Ok(Self::usable_version_of(result.e_tag, result.version)),
            Err(err) if err.is_conflict() => Err(Error::HeartbeatSupersededDuringWrite {
                scheduler_id: scheduler_id.to_string(),
            }),
            Err(source) => Err(Error::Write { source }),
        }
    }

    /// Reads the heartbeat for a scheduler, if any.
    pub async fn read(&self, scheduler_id: &str) -> Result<Option<SchedulerHeartbeat>> {
        let path = self.path_for(scheduler_id);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|source| Error::Read {
                    path: path.to_string(),
                    source,
                })?;
                let beat: SchedulerHeartbeat =
                    serde_json::from_slice(&bytes).context(SerdeSnafu)?;
                Ok(Some(beat))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(source) => Err(Error::Read {
                path: path.to_string(),
                source,
            }),
        }
    }

    /// Best-effort delete; missing files are tolerated.
    pub async fn delete(&self, scheduler_id: &str) -> Result<()> {
        let path = self.path_for(scheduler_id);
        match self.store.delete(&path).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(source) => Err(Error::Delete {
                path: path.to_string(),
                source,
            }),
        }
    }

    /// Lists the heartbeat files in the store, with the time the store last
    /// wrote each.
    pub async fn list_files(&self) -> Result<Vec<(SchedulerId, DateTime<Utc>)>> {
        let prefix_path = Path::from(self.prefix.trim_end_matches('/'));
        let mut stream = self.store.list(Some(&prefix_path));
        let mut files = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|source| Error::List {
                prefix: self.prefix.clone(),
                source,
            })?;
            let path = meta.location.to_string();
            if let Some(id) = path
                .strip_prefix(&self.prefix)
                .and_then(|rest| rest.strip_suffix(".json"))
            {
                files.push((id.to_string(), meta.last_modified));
            }
        }
        Ok(files)
    }

    /// Lists every heartbeat present in the store, with how long this process
    /// has seen each one unchanged. Always performs a fresh `list` + `get`;
    /// never reads from a cache.
    pub async fn list_all(&self) -> Result<HashMap<SchedulerId, ObservedHeartbeat>> {
        let prefix_path = Path::from(self.prefix.trim_end_matches('/'));
        let mut stream = self.store.list(Some(&prefix_path));
        let mut paths = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|source| Error::List {
                prefix: self.prefix.clone(),
                source,
            })?;
            paths.push(meta.location);
        }

        let mut out = HashMap::with_capacity(paths.len());
        for path in paths {
            let path_str = path.to_string();
            let Some(rest) = path_str.strip_prefix(&self.prefix) else {
                continue;
            };
            let Some(id) = rest.strip_suffix(".json") else {
                continue;
            };
            let read_at = Instant::now();
            match self.store.get(&path).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let bytes = result.bytes().await.map_err(|source| Error::Read {
                        path: path_str.clone(),
                        source,
                    })?;
                    match serde_json::from_slice::<SchedulerHeartbeat>(&bytes) {
                        Ok(beat) => {
                            let unchanged_for = self.observe(id, Some(&version), read_at);
                            out.insert(
                                id.to_string(),
                                ObservedHeartbeat {
                                    beat,
                                    unchanged_for,
                                },
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                path = %path_str,
                                error = %err,
                                "Skipping unparseable heartbeat file"
                            );
                        }
                    }
                }
                Err(ObjectStoreError::NotFound { .. }) => {
                    // Concurrent delete during list; skip.
                }
                Err(source) => {
                    return Err(Error::Read {
                        path: path_str,
                        source,
                    });
                }
            }
        }

        Ok(out)
    }

    /// Returns the schedulers registered in `cluster_state` that are alive:
    /// their heartbeat carries the matching `instance_id` and has not lapsed,
    /// or they have no heartbeat file and this process has not yet seen it
    /// missing for their TTL.
    pub async fn list_alive(&self, cluster_state: &ClusterState) -> Result<HashSet<SchedulerId>> {
        let listed_at = Instant::now();
        let all = self.list_all().await?;
        let mut alive = HashSet::with_capacity(cluster_state.schedulers.len());
        for (id, entry) in &cluster_state.schedulers {
            let live = match all.get(id) {
                Some(observed) => {
                    entry.instance_id == observed.beat.instance_id && !observed.lapsed()
                }
                None => {
                    self.observe_missing(id, entry.instance_id, listed_at)
                        < Duration::from_millis(entry.ttl_ms)
                }
            };
            if live {
                alive.insert(id.clone());
            }
        }
        Ok(alive)
    }
}

/// Convenience: small jittered sleep helper used by callers that schedule
/// the reaper task with random offset.
#[must_use]
pub fn jitter(base: Duration, randomization_factor: f64) -> Duration {
    let nanos = u128_to_f64(base.as_nanos());
    let factor = 1.0 + (rand::random::<f64>() - 0.5) * 2.0 * randomization_factor;
    let jittered = (nanos * factor).max(0.0);
    Duration::from_nanos(f64_to_u64(jittered))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "jitter only needs approximate nanosecond precision"
)]
fn u128_to_f64(value: u128) -> f64 {
    value as f64
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "jittered value is clamped to >= 0 and bounded by base duration"
)]
fn f64_to_u64(value: f64) -> u64 {
    value as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::cluster_state::{
        ClusterState, ClusterStateStore, MutationOutcome, SchedulerEntry,
    };
    use object_store::memory::InMemory;

    fn entry(id: &str, instance: Uuid) -> SchedulerEntry {
        SchedulerEntry {
            scheduler_id: id.to_string(),
            instance_id: instance,
            advertise_address: id.to_string(),
            grpc_address: id.to_string(),
            http_address: id.to_string(),
            started_at_ms: 0,
            ttl_ms: 30_000,
            build_version: "test".to_string(),
            labels: HashMap::new(),
        }
    }

    async fn cluster_with(entries: &[SchedulerEntry]) -> ClusterState {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = ClusterStateStore::new(store, "");
        cs.bootstrap().await.expect("bootstrap");
        cs.mutate(|s| {
            for e in entries {
                s.schedulers.insert(e.scheduler_id.clone(), e.clone());
            }
            MutationOutcome::Apply
        })
        .await
        .expect("mutate");
        (*cs.read().await.expect("read")).clone()
    }

    #[tokio::test]
    async fn heartbeat_writes_then_reads_back() {
        let s = SchedulerHeartbeatStore::new(Arc::new(InMemory::new()), "");
        let id = Uuid::new_v4();
        s.heartbeat("a", id, 1000, 30_000).await.expect("write");
        let read = s.read("a").await.expect("read").expect("present");
        assert_eq!(read.scheduler_id, "a");
        assert_eq!(read.instance_id, id);
        assert_eq!(read.last_heartbeat_ms, 1000);
    }

    /// A heartbeat that keeps changing stays alive; one that stops changing
    /// lapses once the reader has seen it unchanged for its TTL.
    #[tokio::test(start_paused = true)]
    async fn list_alive_drops_a_heartbeat_unchanged_for_its_ttl() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        s.heartbeat("a", id_a, 1_000, 30_000).await.expect("a");
        s.heartbeat("b", id_b, 1_000, 30_000).await.expect("b");
        let cluster = cluster_with(&[entry("a", id_a), entry("b", id_b)]).await;

        // A first read judges neither gone, however old their timestamps.
        let alive = s.list_alive(&cluster).await.expect("list");
        assert!(alive.contains("a") && alive.contains("b"));

        // a renews every 10s; b stops.
        for renewal in 1..=3_u64 {
            tokio::time::advance(Duration::from_secs(10)).await;
            s.heartbeat("a", id_a, 1_000 + renewal * 10_000, 30_000)
                .await
                .expect("a renews");
            let alive = s.list_alive(&cluster).await.expect("list");
            assert!(alive.contains("a"));
            assert_eq!(alive.contains("b"), renewal < 3, "after {renewal}0s");
        }
    }

    /// A scheduler registered without a heartbeat lapses after its TTL. A new
    /// registration under the same id gets a TTL of its own.
    #[tokio::test(start_paused = true)]
    async fn list_alive_gives_each_registration_its_own_ttl_to_publish_a_beat() {
        let s = SchedulerHeartbeatStore::new(Arc::new(InMemory::new()), "");
        let first = cluster_with(&[entry("a", Uuid::new_v4())]).await;
        assert!(s.list_alive(&first).await.expect("list").contains("a"));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(!s.list_alive(&first).await.expect("list").contains("a"));

        let second = cluster_with(&[entry("a", Uuid::new_v4())]).await;
        assert!(s.list_alive(&second).await.expect("list").contains("a"));
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(s.list_alive(&second).await.expect("list").contains("a"));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!s.list_alive(&second).await.expect("list").contains("a"));
    }

    /// Liveness reads no wall clock: a scheduler whose clock is a minute behind
    /// or ahead of the reader's stays alive while it renews.
    #[tokio::test(start_paused = true)]
    async fn list_alive_ignores_the_writers_clock() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let behind = Uuid::new_v4();
        let ahead = Uuid::new_v4();
        let cluster = cluster_with(&[entry("behind", behind), entry("ahead", ahead)]).await;
        let reader_now = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("fits");
        for renewal in 0..10_u64 {
            s.heartbeat(
                "behind",
                behind,
                reader_now - 60_000 + renewal * 10_000,
                30_000,
            )
            .await
            .expect("behind");
            s.heartbeat(
                "ahead",
                ahead,
                reader_now + 60_000 + renewal * 10_000,
                30_000,
            )
            .await
            .expect("ahead");
            let alive = s.list_alive(&cluster).await.expect("list");
            assert!(
                alive.contains("behind") && alive.contains("ahead"),
                "renewal {renewal}"
            );
            tokio::time::advance(Duration::from_secs(10)).await;
        }
    }

    #[tokio::test]
    async fn list_alive_filters_orphan_instance_id() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let cluster_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        s.heartbeat("a", other_id, 100_000, 30_000)
            .await
            .expect("a");

        let cluster = cluster_with(&[entry("a", cluster_id)]).await;
        let alive = s.list_alive(&cluster).await.expect("list");
        assert!(!alive.contains("a"));
    }

    #[tokio::test]
    async fn list_all_evicts_deleted_keys() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let id = Uuid::new_v4();
        s.heartbeat("a", id, 1, 1).await.expect("a");
        s.heartbeat("b", id, 1, 1).await.expect("b");
        s.delete("a").await.expect("delete");
        let all = s.list_all().await.expect("list");
        assert!(!all.contains_key("a"));
        assert!(all.contains_key("b"));
    }

    #[tokio::test]
    async fn delete_then_read_returns_none() {
        let s = SchedulerHeartbeatStore::new(Arc::new(InMemory::new()), "");
        let id = Uuid::new_v4();
        s.heartbeat("a", id, 1, 1).await.expect("write");
        s.delete("a").await.expect("delete");
        assert!(s.read("a").await.expect("read").is_none());
    }

    #[tokio::test]
    async fn concurrent_heartbeats_for_same_id_converge() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s1 = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let s2 = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let f1 = s1.heartbeat("a", id1, 100, 30_000);
        let f2 = s2.heartbeat("a", id2, 200, 30_000);
        let (r1, r2) = tokio::join!(f1, f2);
        r1.expect("h1");
        r2.expect("h2");
        let read = s1.read("a").await.expect("read").expect("present");
        // Last writer wins; either id is acceptable.
        assert!(read.instance_id == id1 || read.instance_id == id2);
    }
}
