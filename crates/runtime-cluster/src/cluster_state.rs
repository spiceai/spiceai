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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, UpdateVersion};
use object_store_occ::{
    Attempt, ConditionalWriteError, ConflictRetry, Expected, RetryOnConflictError, conditional_put,
    retry_on_conflict,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use uuid::Uuid;

use crate::metadata::TablePartitionMetadata;

/// Current schema version for `cluster.json`. Bump if the on-disk shape
/// changes; readers reject unknown versions.
pub const CLUSTER_STATE_SCHEMA_VERSION: u32 = 1;

/// How long the entry of a table no scheduler has registered stays in the cluster
/// document, counted from when a scheduler first found the table unregistered.
pub const UNREGISTERED_TABLE_RETENTION: Duration = Duration::from_hours(24);

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

    /// Applies one scheduler's view of which tables it has registered to the table
    /// entries: an entry whose table is registered loses its unregistered time, an
    /// entry whose table is not registered gets one, and an entry whose
    /// unregistered time is at least `retention` before `now_ms` is removed.
    /// Entries `registered` does not name are left as they are.
    pub fn reconcile_table_entries(
        &mut self,
        registered: &HashMap<NormalizedTableName, bool>,
        now_ms: u64,
        retention: Duration,
    ) -> TableEntryChanges {
        let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        let mut changes = TableEntryChanges::default();
        for map in [&mut self.accelerations, &mut self.catalog] {
            map.retain(|table, entry| {
                match (registered.get(table), entry.unregistered_at_ms) {
                    (None, _) => {}
                    (Some(true), Some(_)) => {
                        entry.unregistered_at_ms = None;
                        changes.cleared += 1;
                    }
                    (Some(true), None) => {}
                    (Some(false), None) => {
                        entry.unregistered_at_ms = Some(now_ms);
                        changes.marked += 1;
                    }
                    (Some(false), Some(unregistered_at))
                        if now_ms >= unregistered_at.saturating_add(retention_ms) =>
                    {
                        changes.removed.push(table.clone());
                        return false;
                    }
                    (Some(false), Some(_)) => {}
                }
                true
            });
        }
        changes.removed.sort_unstable();
        changes
    }
}

/// What [`ClusterState::reconcile_table_entries`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableEntryChanges {
    /// Entries that got an unregistered time.
    pub marked: usize,
    /// Entries whose unregistered time was cleared.
    pub cleared: usize,
    /// The tables whose entries were removed, sorted.
    pub removed: Vec<NormalizedTableName>,
}

impl TableEntryChanges {
    fn is_empty(&self) -> bool {
        self.marked == 0 && self.cleared == 0 && self.removed.is_empty()
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

    #[snafu(display("{source}"))]
    Write { source: ConditionalWriteError },

    #[snafu(display(
        "The object store reported neither an ETag nor a version for the cluster document at {path}, so it cannot be updated conditionally"
    ))]
    Unversioned { path: String },

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
    conflict_retry: ConflictRetry,
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
            conflict_retry: ConflictRetry::default(),
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

        match conditional_put(
            self.store.as_ref(),
            &self.path,
            payload.into(),
            &Expected::Absent,
        )
        .await
        {
            Ok(result) => {
                let version = UpdateVersion::from(result);
                self.update_cache(state, version);
                Ok(())
            }
            Err(err) if err.is_conflict() => Ok(()),
            Err(source) => Err(MutateError::Write { source }),
        }
    }

    /// Forces a fresh read from the object store. Updates the cached `Arc`.
    ///
    /// # Errors
    ///
    /// Returns an error if the object-store read or deserialization fails.
    pub async fn read(&self) -> Result<Arc<ClusterState>> {
        self.fetch_and_cache().await.map(|(state, _)| state)
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

    /// Centralised OCC mutation. Key invariants:
    ///
    /// - The mutator must be pure and IO-free; it will be re-run on
    ///   conflicts and on stale-cache verification.
    /// - [`MutationOutcome::NoChange`] returned against a cached
    ///   snapshot is *not* trusted: the store forces one fresh read and
    ///   re-invokes the mutator before declaring `AlreadySatisfied`.
    /// - [`MutationOutcome::Apply`] starts from cache (fast path). The write
    ///   is conditional on the version the mutator ran against; when another
    ///   writer changed the document first, the mutator runs again against a
    ///   fresh read, under the shared conflict-retry policy.
    /// - `NotFound` mid-flight is *not* auto-bootstrapped; it is
    ///   surfaced as [`MutateError::ClusterDocMissing`].
    ///
    /// # Errors
    ///
    /// Returns an error if the mutator aborts, if the document is missing,
    /// if serialization fails, or if every attempt lost to another writer.
    pub async fn mutate<F>(&self, mutator: F) -> Result<MutateOk>
    where
        F: FnMut(&mut ClusterState) -> MutationOutcome,
    {
        let mutator = Mutex::new(mutator);
        // Only the first attempt may start from the cached snapshot.
        let from_cache = AtomicBool::new(true);
        retry_on_conflict(&self.conflict_retry, || async {
            let cached = if from_cache.swap(false, Ordering::Relaxed) {
                self.cached_pair()
            } else {
                None
            };
            let (mut state, mut version, mut used_cache) = match cached {
                Some((state, version)) => (state, version, true),
                None => {
                    let (state, version) = self.fetch_and_cache().await?;
                    (state, version, false)
                }
            };
            loop {
                let mut draft = (*state).clone();
                let outcome = (&mut *mutator.lock())(&mut draft);
                match outcome {
                    MutationOutcome::Abort(err) => return Err(err),
                    MutationOutcome::NoChange if used_cache => {
                        // Cached state may be stale; verify against a fresh read.
                        (state, version) = self.fetch_and_cache().await?;
                        used_cache = false;
                    }
                    MutationOutcome::NoChange => {
                        return Ok(Attempt::Done(MutateOk::AlreadySatisfied));
                    }
                    MutationOutcome::Apply => {
                        draft.updated_at_ms = now_ms()?;
                        return self.write(draft, version).await;
                    }
                }
            }
        })
        .await
        .map_err(|err| match err {
            RetryOnConflictError::ConflictsExhausted { attempts } => {
                MutateError::ConcurrentModification {
                    path: self.path.to_string(),
                    attempts,
                }
            }
            RetryOnConflictError::Failed(err) => err,
        })
    }

    /// Applies [`ClusterState::reconcile_table_entries`] to the stored document,
    /// with [`UNREGISTERED_TABLE_RETENTION`], and returns what it changed.
    ///
    /// # Errors
    ///
    /// Returns an error if the mutation fails (see [`Self::mutate`]).
    pub async fn reconcile_table_entries(
        &self,
        registered: &HashMap<NormalizedTableName, bool>,
    ) -> Result<TableEntryChanges> {
        let retention = object_store_occ::cleanup::age(UNREGISTERED_TABLE_RETENTION);
        let mut changes = TableEntryChanges::default();
        self.mutate(|state| {
            let now_ms = match now_ms() {
                Ok(now_ms) => now_ms,
                Err(err) => return MutationOutcome::Abort(err),
            };
            changes = state.reconcile_table_entries(registered, now_ms, retention);
            if changes.is_empty() {
                MutationOutcome::NoChange
            } else {
                MutationOutcome::Apply
            }
        })
        .await?;
        Ok(changes)
    }

    /// Writes `state` if the document is still at `version`, and caches it.
    async fn write(
        &self,
        state: ClusterState,
        version: UpdateVersion,
    ) -> Result<Attempt<MutateOk>> {
        let expected = Expected::at_version(version).context(UnversionedSnafu {
            path: self.path.to_string(),
        })?;
        let payload = serde_json::to_vec(&state).context(SerdeSnafu {
            path: self.path.to_string(),
        })?;
        match conditional_put(self.store.as_ref(), &self.path, payload.into(), &expected).await {
            Ok(result) => {
                self.update_cache(state, UpdateVersion::from(result));
                Ok(Attempt::Done(MutateOk::Committed))
            }
            Err(err) if err.is_conflict() => Ok(Attempt::Conflict),
            Err(source) => Err(MutateError::Write { source }),
        }
    }

    async fn fetch_and_cache(&self) -> Result<(Arc<ClusterState>, UpdateVersion)> {
        let (state, version) = self.fetch().await?;
        let state = Arc::new(state);
        *self.cache.write() = Some(CachedState {
            value: Arc::clone(&state),
            version: version.clone(),
        });
        Ok((state, version))
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
    use crate::metadata::TablePartitionMetadata;
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

    /// Schedulers mutating `cluster.json` concurrently on one local directory
    /// lose no update.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_mutations_on_a_local_directory_all_land() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(object_store_occ::LocalConditionalPut::new(dir.path()).expect("local store"));
        ClusterStateStore::new(Arc::clone(&store), "")
            .bootstrap()
            .await
            .expect("bootstrap");

        let writers = 4;
        let per_writer = 10;
        let mut tasks = Vec::new();
        for writer in 0..writers {
            let s = ClusterStateStore::new(Arc::clone(&store), "");
            tasks.push(tokio::spawn(async move {
                for i in 0..per_writer {
                    let id = format!("w{writer}-{i}");
                    s.mutate(|cs| {
                        cs.schedulers.insert(id.clone(), entry(&id, Uuid::new_v4()));
                        MutationOutcome::Apply
                    })
                    .await
                    .expect("mutation lands within the retry budget");
                }
            }));
        }
        for task in tasks {
            task.await.expect("writer task");
        }

        let read = ClusterStateStore::new(store, "")
            .read()
            .await
            .expect("read");
        assert_eq!(read.schedulers.len(), writers * per_writer);
    }

    fn table_entry(table: &str, unregistered_at_ms: Option<u64>) -> TablePartitionMetadata {
        TablePartitionMetadata {
            table_name: table.to_string(),
            partitions: Vec::new(),
            schema_version: 1,
            updated_at: 1,
            partition_expressions: vec!["date".to_string()],
            unregistered_at_ms,
        }
    }

    fn entries(
        map: &HashMap<NormalizedTableName, TablePartitionMetadata>,
    ) -> Vec<(String, Option<u64>)> {
        let mut entries: Vec<_> = map
            .iter()
            .map(|(table, entry)| (table.clone(), entry.unregistered_at_ms))
            .collect();
        entries.sort();
        entries
    }

    /// An entry goes once its table has been unregistered for 24 hours, counted
    /// from when a scheduler first found it unregistered; a scheduler that has the
    /// table registered resets that count.
    #[test]
    fn a_table_entry_is_removed_24_hours_after_its_table_was_found_unregistered() {
        let day_ms = 24 * 60 * 60 * 1000;
        let now = 10 * day_ms;
        let mut state = ClusterState::new(0);
        for (table, unregistered_at) in [
            ("spice.public.registered", None),
            ("spice.public.reregistered", Some(now - day_ms - 1)),
            ("spice.public.newly_unregistered", None),
            ("spice.public.just_under_a_day", Some(now - day_ms + 1)),
            ("spice.public.exactly_a_day", Some(now - day_ms)),
            ("spice.public.unknown_to_this_scheduler", Some(now - 2 * day_ms)),
        ] {
            state
                .accelerations
                .insert(table.to_string(), table_entry(table, unregistered_at));
        }
        state.catalog.insert(
            "cayenne.sales.orders".to_string(),
            table_entry("cayenne.sales.orders", Some(now - 2 * day_ms)),
        );
        let registered: HashMap<String, bool> = [
            ("spice.public.registered", true),
            ("spice.public.reregistered", true),
            ("spice.public.newly_unregistered", false),
            ("spice.public.just_under_a_day", false),
            ("spice.public.exactly_a_day", false),
            ("cayenne.sales.orders", false),
        ]
        .into_iter()
        .map(|(table, registered)| (table.to_string(), registered))
        .collect();

        let changes = state.reconcile_table_entries(
            &registered,
            now,
            UNREGISTERED_TABLE_RETENTION,
        );

        assert_eq!(
            changes,
            TableEntryChanges {
                marked: 1,
                cleared: 1,
                removed: vec![
                    "cayenne.sales.orders".to_string(),
                    "spice.public.exactly_a_day".to_string()
                ],
            }
        );
        assert_eq!(
            entries(&state.accelerations),
            vec![
                ("spice.public.just_under_a_day".to_string(), Some(now - day_ms + 1)),
                ("spice.public.newly_unregistered".to_string(), Some(now)),
                ("spice.public.registered".to_string(), None),
                ("spice.public.reregistered".to_string(), None),
                (
                    "spice.public.unknown_to_this_scheduler".to_string(),
                    Some(now - 2 * day_ms)
                ),
            ]
        );
        assert!(state.catalog.is_empty());
    }

    /// Two schedulers that reconcile the same document at once both succeed. A
    /// table one of them still has registered is never removed, and an expired
    /// entry is removed whichever scheduler gets there first.
    #[tokio::test]
    async fn two_schedulers_reconciling_table_entries_at_once_both_succeed() {
        let store = store();
        let first = ClusterStateStore::new(Arc::clone(&store), "");
        let second = ClusterStateStore::new(Arc::clone(&store), "");
        first.bootstrap().await.expect("bootstrap");
        let long_ago = now_ms().expect("clock") - 2 * 24 * 60 * 60 * 1000;
        first
            .mutate(|state| {
                for (table, unregistered_at) in [
                    ("spice.public.shared", None),
                    ("spice.public.dropped", Some(long_ago)),
                ] {
                    state
                        .accelerations
                        .insert(table.to_string(), table_entry(table, unregistered_at));
                }
                MutationOutcome::Apply
            })
            .await
            .expect("seed the table entries");

        let has_shared: HashMap<String, bool> = [
            ("spice.public.shared".to_string(), true),
            ("spice.public.dropped".to_string(), false),
        ]
        .into();
        let lacks_shared: HashMap<String, bool> = [
            ("spice.public.shared".to_string(), false),
            ("spice.public.dropped".to_string(), false),
        ]
        .into();
        let (first_pass, second_pass) = tokio::join!(
            first.reconcile_table_entries(&has_shared),
            second.reconcile_table_entries(&lacks_shared)
        );
        first_pass.expect("the first scheduler's pass succeeds");
        second_pass.expect("the second scheduler's pass succeeds");

        let state = ClusterStateStore::new(store, "").read().await.expect("read");
        assert_eq!(
            state.accelerations.keys().collect::<Vec<_>>(),
            vec!["spice.public.shared"],
            "the entry a scheduler still has registered stays, the expired one goes"
        );
    }
}
