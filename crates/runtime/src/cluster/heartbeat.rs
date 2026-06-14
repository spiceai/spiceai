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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use uuid::Uuid;

use crate::cluster::cluster_state::{ClusterState, SchedulerId};

/// Tolerated clock skew between schedulers when judging heartbeat
/// freshness, in milliseconds.
pub const CLOCK_SKEW_TOLERANCE_MS: u64 = 5_000;

/// Maximum write retries for a single heartbeat write. Heartbeats live in
/// per-scheduler files so contention is essentially impossible in steady
/// state; retries handle the bootstrap race only.
const MAX_HEARTBEAT_ATTEMPTS: usize = 5;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read heartbeat at {path}: {source}"))]
    Read {
        path: String,
        source: ObjectStoreError,
    },
    #[snafu(display("Failed to write heartbeat at {path}: {source}"))]
    Write {
        path: String,
        source: ObjectStoreError,
    },
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
    #[snafu(display("Heartbeat write for {scheduler_id} exhausted retries: {source}"))]
    RetryExhausted {
        scheduler_id: String,
        source: ObjectStoreError,
    },
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
    pub last_heartbeat_ms: u64,
    pub ttl_ms: u64,
}

impl SchedulerHeartbeat {
    /// Returns true if this heartbeat is older than its TTL plus the
    /// configured clock-skew tolerance.
    #[must_use]
    pub fn is_stale(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_heartbeat_ms)
            > self.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS)
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
        Self { store, prefix }
    }

    /// Returns the full path for a given scheduler id.
    #[must_use]
    pub fn path_for(&self, scheduler_id: &str) -> Path {
        Path::from(format!("{}{}.json", self.prefix, scheduler_id))
    }

    /// Writes (or overwrites) the heartbeat for a scheduler.
    pub async fn heartbeat(
        &self,
        scheduler_id: &str,
        instance_id: Uuid,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<()> {
        let beat = SchedulerHeartbeat {
            scheduler_id: scheduler_id.to_string(),
            instance_id,
            last_heartbeat_ms: now_ms,
            ttl_ms,
        };
        let payload = serde_json::to_vec(&beat).context(SerdeSnafu)?;
        let path = self.path_for(scheduler_id);

        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_HEARTBEAT_ATTEMPTS))
            .build();

        let mut last_err: Option<ObjectStoreError> = None;
        loop {
            match self
                .store
                .put_opts(
                    &path,
                    payload.clone().into(),
                    PutOptions::from(PutMode::Overwrite),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(source) => {
                    let Some(delay) = backoff.next_duration() else {
                        return Err(Error::RetryExhausted {
                            scheduler_id: scheduler_id.to_string(),
                            source: last_err.unwrap_or(source),
                        });
                    };
                    tracing::debug!(
                        scheduler_id,
                        path = %path,
                        error = %source,
                        retry_in_ms = delay.as_millis(),
                        "Heartbeat write failed, retrying"
                    );
                    last_err = Some(source);
                    tokio::time::sleep(delay).await;
                }
            }
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

    /// Lists every heartbeat present in the store. Always performs a
    /// fresh `list` + `get`; never reads from a cache.
    pub async fn list_all(&self) -> Result<HashMap<SchedulerId, SchedulerHeartbeat>> {
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
            match self.store.get(&path).await {
                Ok(result) => {
                    let bytes = result.bytes().await.map_err(|source| Error::Read {
                        path: path_str.clone(),
                        source,
                    })?;
                    match serde_json::from_slice::<SchedulerHeartbeat>(&bytes) {
                        Ok(beat) => {
                            out.insert(id.to_string(), beat);
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

    /// Returns the heartbeats that are (a) registered in `cluster_state`,
    /// (b) carry the matching `instance_id`, and (c) are not stale.
    pub async fn list_alive(
        &self,
        now_ms: u64,
        cluster_state: &ClusterState,
    ) -> Result<HashMap<SchedulerId, SchedulerHeartbeat>> {
        let all = self.list_all().await?;
        let mut alive = HashMap::with_capacity(all.len());
        for (id, beat) in all {
            let Some(entry) = cluster_state.schedulers.get(&id) else {
                continue;
            };
            if entry.instance_id != beat.instance_id {
                continue;
            }
            if beat.is_stale(now_ms) {
                continue;
            }
            alive.insert(id, beat);
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

    #[tokio::test]
    async fn list_alive_filters_stale() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = SchedulerHeartbeatStore::new(Arc::clone(&store), "");
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        s.heartbeat("a", id_a, 100_000, 30_000).await.expect("a");
        s.heartbeat("b", id_b, 1_000, 30_000).await.expect("b"); // stale

        let cluster = cluster_with(&[entry("a", id_a), entry("b", id_b)]).await;
        let alive = s.list_alive(100_000, &cluster).await.expect("list");
        assert!(alive.contains_key("a"));
        assert!(!alive.contains_key("b"));
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
        let alive = s.list_alive(100_000, &cluster).await.expect("list");
        assert!(!alive.contains_key("a"));
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
