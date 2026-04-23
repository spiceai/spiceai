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

//! Single-document cluster state stored at `cluster.json`.
//!
//! This module holds [`ClusterState`] (the schema for the document), the
//! [`ClusterStateStore`] wrapper that performs OCC reads/writes, and the
//! [`mutate`](ClusterStateStore::mutate) abstraction that all writers go
//! through.
//!
//! Layout reminder: every non-job piece of distributed state lives in this
//! one document — registered schedulers (without per-tick heartbeat data),
//! accelerated table partition metadata, and catalog/federated table
//! partition metadata. Heartbeats live in `heartbeats/{id}.json` and are
//! managed by the scheduler heartbeat store.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, UpdateVersion};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use uuid::Uuid;

use crate::metadata::TablePartitionMetadata;

/// Current schema version for `cluster.json`. Bump if the on-disk shape
/// changes; readers reject unknown versions.
pub const CLUSTER_STATE_SCHEMA_VERSION: u32 = 1;

/// Maximum number of conditional-write attempts for [`ClusterStateStore::mutate`].
/// Bumped from the 5 used per-table in the old layout because the document is
/// now shared across all schedulers and partition operations.
const MAX_MUTATE_ATTEMPTS: usize = 8;

/// Logical scheduler identifier (`host:port` style, stable across restarts).
pub type SchedulerId = String;
/// Normalized table name as produced by the partition metadata module.
pub type NormalizedTableName = String;

/// The full distributed cluster state, persisted as a single OCC document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterState {
    pub schema_version: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,

    /// All currently registered schedulers (heartbeats live in a separate
    /// per-scheduler file).
    #[serde(default)]
    pub schedulers: HashMap<SchedulerId, SchedulerEntry>,

    /// Partition metadata for accelerated tables.
    #[serde(default)]
    pub accelerations: HashMap<NormalizedTableName, TablePartitionMetadata>,

    /// Partition metadata for catalog/federated tables.
    #[serde(default)]
    pub catalog: HashMap<NormalizedTableName, TablePartitionMetadata>,
}

impl ClusterState {
    fn new(now_ms: u64) -> Self {
        Self {
            schema_version: CLUSTER_STATE_SCHEMA_VERSION,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            schedulers: HashMap::new(),
            accelerations: HashMap::new(),
            catalog: HashMap::new(),
        }
    }
}

/// One scheduler's registration. The `instance_id` is regenerated on every
/// process start so that takeover and heartbeat ownership stay safe even
/// though heartbeats live in a separate file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerEntry {
    pub scheduler_id: SchedulerId,
    pub instance_id: Uuid,
    pub advertise_address: String,
    pub grpc_address: String,
    pub http_address: String,
    pub started_at_ms: u64,
    pub ttl_ms: u64,
    pub build_version: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Which partition submap a [`ClusterStateStore`] mutation should operate on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionScope {
    Acceleration,
    Catalog,
}

impl PartitionScope {
    pub(crate) fn map_mut(
        self,
        state: &mut ClusterState,
    ) -> &mut HashMap<NormalizedTableName, TablePartitionMetadata> {
        match self {
            PartitionScope::Acceleration => &mut state.accelerations,
            PartitionScope::Catalog => &mut state.catalog,
        }
    }

    pub(crate) fn map(
        self,
        state: &ClusterState,
    ) -> &HashMap<NormalizedTableName, TablePartitionMetadata> {
        match self {
            PartitionScope::Acceleration => &state.accelerations,
            PartitionScope::Catalog => &state.catalog,
        }
    }

    #[expect(dead_code, reason = "useful for diagnostics; kept for symmetry")]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            PartitionScope::Acceleration => "acceleration",
            PartitionScope::Catalog => "catalog",
        }
    }
}

/// Outcome a mutator returns to [`ClusterStateStore::mutate`].
#[derive(Debug)]
pub enum MutationOutcome {
    /// The mutator detected its desired change is already present. The
    /// store will only return [`MutateOk::AlreadySatisfied`] after
    /// confirming this against a freshly-read state — `NoChange` against a
    /// stale cached snapshot will trigger a fresh read and a re-run.
    NoChange,
    /// Commit the mutated state.
    Apply,
    /// Mutator detected an unrecoverable condition; surface this error to
    /// the caller without writing.
    Abort(MutateError),
}

/// Result of a successful [`ClusterStateStore::mutate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutateOk {
    /// A new revision was committed.
    Committed,
    /// The mutator returned [`MutationOutcome::NoChange`] against fresh
    /// state — the change was already present, no write was issued.
    AlreadySatisfied,
}

#[derive(Debug, Snafu)]
pub enum MutateError {
    #[snafu(display("Cluster document is missing at {path}"))]
    ClusterDocMissing { path: String },

    #[snafu(display(
        "Concurrent modification on cluster document at {path}; gave up after {attempts} attempts"
    ))]
    ConcurrentModification { path: String, attempts: usize },

    #[snafu(display("Failed to read cluster document at {path}: {source}"))]
    Read {
        path: String,
        source: ObjectStoreError,
    },

    #[snafu(display("Failed to write cluster document at {path}: {source}"))]
    Write {
        path: String,
        source: ObjectStoreError,
    },

    #[snafu(display("Failed to (de)serialize cluster document at {path}: {source}"))]
    Serde {
        path: String,
        source: serde_json::Error,
    },

    #[snafu(display("Cluster document at {path} has unsupported schema version {found}"))]
    UnsupportedSchemaVersion { path: String, found: u32 },

    #[snafu(display("Failed to read system clock: {source}"))]
    Clock { source: std::time::SystemTimeError },

    /// Caller-supplied condition (e.g. scheduler-id conflict).
    #[snafu(display("{message}"))]
    Conflict { message: String },
}

pub type Result<T, E = MutateError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
struct CachedState {
    value: Arc<ClusterState>,
    version: UpdateVersion,
}

/// OCC-protected store for the single `cluster.json` document.
///
/// All writes go through [`Self::mutate`]. Readers can use [`Self::read`]
/// for a fresh fetch or [`Self::read_cached`] for the in-memory snapshot
/// (returned as an `Arc` so query routing doesn't pay a deep clone).
#[derive(Debug)]
pub struct ClusterStateStore {
    store: Arc<dyn ObjectStore>,
    /// Full path of the cluster document, e.g. `prefix/cluster.json`.
    path: Path,
    cache: RwLock<Option<CachedState>>,
}

impl ClusterStateStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base_prefix: &str) -> Self {
        let path = if base_prefix.is_empty() {
            Path::from("cluster.json")
        } else {
            Path::from(format!(
                "{}/cluster.json",
                base_prefix.trim_end_matches('/')
            ))
        };
        Self {
            store,
            path,
            cache: RwLock::new(None),
        }
    }

    /// Returns the full object-store path of the cluster document.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Idempotent first-writer-wins create. Should be called exactly once
    /// per scheduler at startup; subsequent calls are no-ops.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the object-store write fails.
    pub async fn bootstrap(&self) -> Result<()> {
        let now_ms = now_ms()?;
        let state = ClusterState::new(now_ms);
        let payload = serde_json::to_vec(&state).context(SerdeSnafu {
            path: self.path.to_string(),
        })?;

        match self
            .store
            .put_opts(
                &self.path,
                payload.into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(result) => {
                let version = UpdateVersion::from(result);
                self.update_cache(state, version);
                Ok(())
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => Ok(()),
            Err(source) => Err(MutateError::Write {
                path: self.path.to_string(),
                source,
            }),
        }
    }

    /// Forces a fresh read from the object store. Updates the cached `Arc`.
    ///
    /// # Errors
    ///
    /// Returns an error if the object-store read or deserialization fails.
    pub async fn read(&self) -> Result<Arc<ClusterState>> {
        let (state, version) = self.fetch().await?;
        let arc = Arc::new(state);
        *self.cache.write() = Some(CachedState {
            value: Arc::clone(&arc),
            version,
        });
        Ok(arc)
    }

    /// Returns the in-memory snapshot if any has been observed so far.
    /// Cheap (Arc clone) and IO-free.
    #[must_use]
    pub fn read_cached(&self) -> Option<Arc<ClusterState>> {
        self.cache.read().as_ref().map(|c| Arc::clone(&c.value))
    }

    /// Returns the cached `Arc` if present, otherwise fetches.
    ///
    /// # Errors
    ///
    /// Returns an error if the fallback fetch fails (see [`Self::read`]).
    pub async fn read_or_cached(&self) -> Result<Arc<ClusterState>> {
        if let Some(arc) = self.read_cached() {
            return Ok(arc);
        }
        self.read().await
    }

    /// Centralised OCC mutation. See module docs and the project plan
    /// (`plans/consolidate-cluster-state-into-cluster-json.md`) for the
    /// full semantics. Key invariants:
    ///
    /// - The mutator must be pure and IO-free; it will be re-run on
    ///   conflicts and on stale-cache verification.
    /// - [`MutationOutcome::NoChange`] returned against a cached
    ///   snapshot is *not* trusted: the store forces one fresh read and
    ///   re-invokes the mutator before declaring `AlreadySatisfied`.
    /// - [`MutationOutcome::Apply`] starts from cache (fast path), then
    ///   on `Conflict` re-reads and re-runs against the fresh state.
    /// - `NotFound` mid-flight is *not* auto-bootstrapped; it is
    ///   surfaced as [`MutateError::ClusterDocMissing`].
    ///
    /// # Errors
    ///
    /// Returns an error if the mutator aborts, if the document is missing,
    /// if serialization fails, or if the maximum number of OCC retries is exceeded.
    pub async fn mutate<F>(&self, mut mutator: F) -> Result<MutateOk>
    where
        F: FnMut(&mut ClusterState) -> MutationOutcome,
    {
        // Track whether the next mutator invocation will run against a
        // cached (potentially stale) snapshot. If so, a `NoChange` result
        // must be re-confirmed against a fresh read before being trusted.
        let mut state_arc: Arc<ClusterState>;
        let mut current_version: UpdateVersion;
        let mut used_cache: bool;
        if let Some(cached) = self.cached_pair() {
            state_arc = cached.0;
            current_version = cached.1;
            used_cache = true;
        } else {
            let (state, version) = self.fetch().await?;
            current_version = version.clone();
            state_arc = Arc::new(state);
            *self.cache.write() = Some(CachedState {
                value: Arc::clone(&state_arc),
                version,
            });
            used_cache = false;
        }

        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_MUTATE_ATTEMPTS))
            .build();
        let mut attempts = 0usize;

        loop {
            attempts += 1;
            let mut draft = (*state_arc).clone();
            match mutator(&mut draft) {
                MutationOutcome::Abort(err) => return Err(err),
                MutationOutcome::NoChange => {
                    if used_cache {
                        // Cached state may be stale; verify against fresh read.
                        let (fresh, fresh_version) = self.fetch().await?;
                        current_version = fresh_version.clone();
                        state_arc = Arc::new(fresh);
                        *self.cache.write() = Some(CachedState {
                            value: Arc::clone(&state_arc),
                            version: fresh_version,
                        });
                        used_cache = false;
                        continue;
                    }
                    return Ok(MutateOk::AlreadySatisfied);
                }
                MutationOutcome::Apply => {
                    draft.updated_at_ms = now_ms()?;
                    let payload = serde_json::to_vec(&draft).context(SerdeSnafu {
                        path: self.path.to_string(),
                    })?;
                    match self
                        .store
                        .put_opts(
                            &self.path,
                            payload.into(),
                            PutOptions::from(PutMode::Update(current_version.clone())),
                        )
                        .await
                    {
                        Ok(result) => {
                            let new_version = UpdateVersion::from(result);
                            let new_arc = Arc::new(draft);
                            *self.cache.write() = Some(CachedState {
                                value: Arc::clone(&new_arc),
                                version: new_version,
                            });
                            return Ok(MutateOk::Committed);
                        }
                        Err(ObjectStoreError::NotFound { .. }) => {
                            return Err(MutateError::ClusterDocMissing {
                                path: self.path.to_string(),
                            });
                        }
                        Err(ObjectStoreError::Precondition { .. }) => {
                            // Conflict: refresh and retry.
                            let Some(delay) = backoff.next_duration() else {
                                return Err(MutateError::ConcurrentModification {
                                    path: self.path.to_string(),
                                    attempts,
                                });
                            };
                            tokio::time::sleep(delay).await;
                            let (fresh, fresh_version) = self.fetch().await?;
                            current_version = fresh_version.clone();
                            state_arc = Arc::new(fresh);
                            *self.cache.write() = Some(CachedState {
                                value: Arc::clone(&state_arc),
                                version: fresh_version,
                            });
                            used_cache = false;
                        }
                        Err(source) => {
                            return Err(MutateError::Write {
                                path: self.path.to_string(),
                                source,
                            });
                        }
                    }
                }
            }
        }
    }

    fn cached_pair(&self) -> Option<(Arc<ClusterState>, UpdateVersion)> {
        self.cache
            .read()
            .as_ref()
            .map(|c| (Arc::clone(&c.value), c.version.clone()))
    }

    fn update_cache(&self, value: ClusterState, version: UpdateVersion) {
        *self.cache.write() = Some(CachedState {
            value: Arc::new(value),
            version,
        });
    }

    async fn fetch(&self) -> Result<(ClusterState, UpdateVersion)> {
        let result = match self.store.get(&self.path).await {
            Ok(r) => r,
            Err(ObjectStoreError::NotFound { .. }) => {
                return Err(MutateError::ClusterDocMissing {
                    path: self.path.to_string(),
                });
            }
            Err(source) => {
                return Err(MutateError::Read {
                    path: self.path.to_string(),
                    source,
                });
            }
        };

        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result.bytes().await.map_err(|source| MutateError::Read {
            path: self.path.to_string(),
            source,
        })?;
        let state: ClusterState = serde_json::from_slice(&bytes).context(SerdeSnafu {
            path: self.path.to_string(),
        })?;

        if state.schema_version != CLUSTER_STATE_SCHEMA_VERSION {
            return Err(MutateError::UnsupportedSchemaVersion {
                path: self.path.to_string(),
                found: state.schema_version,
            });
        }

        Ok((state, version))
    }
}

/// Returns the current time as milliseconds since the Unix epoch.
///
/// # Errors
///
/// Returns an error if the system clock is set before the Unix epoch.
pub fn now_ms() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(ClockSnafu)?;
    Ok(u64::try_from(now.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

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

    #[tokio::test]
    async fn bootstrap_is_idempotent() {
        let store = store();
        let s = ClusterStateStore::new(Arc::clone(&store), "");
        s.bootstrap().await.expect("first bootstrap");
        // Mutate to bump revision so we can detect overwrites.
        let id = Uuid::new_v4();
        s.mutate(|cs| {
            cs.schedulers.insert("a".to_string(), entry("a", id));
            MutationOutcome::Apply
        })
        .await
        .expect("insert ok");

        // Second bootstrap should not overwrite.
        let s2 = ClusterStateStore::new(Arc::clone(&store), "");
        s2.bootstrap().await.expect("second bootstrap");
        let read = s2.read().await.expect("read");
        assert!(read.schedulers.contains_key("a"));
    }

    #[tokio::test]
    async fn mutate_apply_writes_new_revision() {
        let s = ClusterStateStore::new(store(), "");
        s.bootstrap().await.expect("bootstrap");
        let id = Uuid::new_v4();
        let result = s
            .mutate(|cs| {
                cs.schedulers.insert("a".to_string(), entry("a", id));
                MutationOutcome::Apply
            })
            .await
            .expect("mutate");
        assert_eq!(result, MutateOk::Committed);
        let read = s.read().await.expect("read");
        assert!(read.schedulers.contains_key("a"));
    }

    #[tokio::test]
    async fn mutate_no_change_against_fresh_state_skips_write() {
        let s = ClusterStateStore::new(store(), "");
        s.bootstrap().await.expect("bootstrap");
        // Force a fresh fetch first so the cache is current.
        s.read().await.expect("read");
        let result = s
            .mutate(|_cs| MutationOutcome::NoChange)
            .await
            .expect("mutate");
        assert_eq!(result, MutateOk::AlreadySatisfied);
    }

    #[tokio::test]
    async fn mutate_no_change_from_stale_cache_forces_fresh_read() {
        let store = store();
        let a = ClusterStateStore::new(Arc::clone(&store), "");
        let b = ClusterStateStore::new(Arc::clone(&store), "");
        a.bootstrap().await.expect("bootstrap");
        // Prime A's cache.
        a.read().await.expect("read");
        // B mutates the doc behind A's back.
        let id = Uuid::new_v4();
        b.mutate(|cs| {
            cs.schedulers.insert("a".to_string(), entry("a", id));
            MutationOutcome::Apply
        })
        .await
        .expect("b mutate");

        // A's mutator decides "already inserted" based on stale cache —
        // verify the store re-runs the mutator against fresh state and
        // the second pass actually applies the change.
        let calls = AtomicUsize::new(0);
        let new_id = Uuid::new_v4();
        let result = a
            .mutate(|cs| {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // Pretend "already done" using stale view.
                    MutationOutcome::NoChange
                } else {
                    cs.schedulers.insert("b".to_string(), entry("b", new_id));
                    MutationOutcome::Apply
                }
            })
            .await
            .expect("mutate");
        assert_eq!(result, MutateOk::Committed);
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn mutate_conflict_then_effective_no_op() {
        let store = store();
        let a = ClusterStateStore::new(Arc::clone(&store), "");
        let b = ClusterStateStore::new(Arc::clone(&store), "");
        a.bootstrap().await.expect("bootstrap");
        a.read().await.expect("read a");
        b.read().await.expect("read b");

        let id = Uuid::new_v4();
        // B applies a change A is also about to make.
        b.mutate(|cs| {
            cs.schedulers.insert("a".to_string(), entry("a", id));
            MutationOutcome::Apply
        })
        .await
        .expect("b mutate");

        // A tries to add the same scheduler. After the first attempt
        // conflicts, the re-run sees the entry already present and
        // returns NoChange against fresh state -> AlreadySatisfied.
        let result = a
            .mutate(|cs| {
                if cs.schedulers.contains_key("a") {
                    MutationOutcome::NoChange
                } else {
                    cs.schedulers.insert("a".to_string(), entry("a", id));
                    MutationOutcome::Apply
                }
            })
            .await
            .expect("a mutate");
        assert_eq!(result, MutateOk::AlreadySatisfied);
    }

    #[tokio::test]
    async fn mutate_returns_error_on_mid_flight_doc_missing() {
        let store = store();
        let s = ClusterStateStore::new(Arc::clone(&store), "");
        // No bootstrap -> first mutate sees missing doc.
        let err = s
            .mutate(|_| MutationOutcome::Apply)
            .await
            .expect_err("should fail");
        assert!(matches!(err, MutateError::ClusterDocMissing { .. }));
    }

    #[tokio::test]
    async fn read_cached_returns_arc_pointer_clone() {
        let s = ClusterStateStore::new(store(), "");
        s.bootstrap().await.expect("bootstrap");
        s.read().await.expect("read");
        let a = s.read_cached().expect("cached");
        let b = s.read_cached().expect("cached");
        assert!(Arc::ptr_eq(&a, &b));
    }
}
