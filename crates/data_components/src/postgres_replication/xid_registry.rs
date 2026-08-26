/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Outstanding-transaction registry for CDC echo suppression on `PostgreSQL`
//! durable write-back.
//!
//! When Spice delivers a durable write-back to a `PostgreSQL` source, that write
//! is itself replicated back to us as a change (an *echo*). Re-applying the echo
//! would double-count maintained aggregates and resurrect just-deleted rows. The
//! connector suppresses echoes by transaction identity: it records the transaction
//! id (`xid8`) of every write-back it issues, and the CDC pump drops the arbitrated
//! table's changes from any replicated transaction carrying one of those ids before
//! they become Arrow.
//!
//! This module owns that recorded set — the [`XidRegistry`] — together with its
//! durable persistence and garbage collection. It is deliberately **connection
//! free**: it never talks to `PostgreSQL`. The connector supplies everything that
//! needs a server round trip (transaction statuses, the current server `xid8`) as
//! plain inputs to [`XidRegistry::gc`], so this crate keeps no driver dependency.
//!
//! # Why identity, and why persistence and GC are required
//!
//! The stream's `xid` is 32-bit and `PostgreSQL` xids wrap at 2^32, so an entry
//! that lingers un-pruned past a full xid epoch would collide with an unrelated
//! future transaction and wrongly drop it. Entry lifetime is therefore bounded by
//! real garbage collection ([`XidRegistry::gc`]), not treated as harmless hygiene.
//!
//! An in-memory-only set loses outstanding xids on restart; an echo whose commit
//! the replication slot had not yet consumed would then leak and double-apply. The
//! registry persists its outstanding entries in the dataset's own accelerator
//! (a sibling of the applied-LSN watermark), so a restart resumes with the same
//! suppression set.
//!
//! # Lifecycle
//!
//! - [`register`](XidRegistry::register) — persist the entry **before** the delivery
//!   `COMMIT` is issued. The instant `COMMIT` succeeds the echo can arrive, so a
//!   crash between `COMMIT` and a late registration would leak an unregistered echo.
//!   Register-before-`COMMIT` instead leaves a *stale* entry when the `COMMIT`
//!   subsequently fails — safe, and cleaned by GC.
//! - [`set_upper_bound`](XidRegistry::set_upper_bound) — record the post-`COMMIT`
//!   `pg_current_wal_insert_lsn()` as an upper bound on the echo's commit LSN
//!   (best-effort), which is what makes GC of a lost unregister possible.
//! - [`contains`](XidRegistry::contains) — the pump hot-path membership test, once
//!   per source transaction, on the low 32 bits.
//! - [`mark_commit_observed`](XidRegistry::mark_commit_observed) — record the echo's
//!   actual commit LSN when the pump sees its `Commit`. **Never** removes the entry:
//!   the slot replays everything after `confirmed_flush` on any reconnect, so an
//!   entry removed at commit-observation would re-admit the replayed echo.
//! - [`prune_acked`](XidRegistry::prune_acked) — remove an entry only once the
//!   durable applied position has advanced past the echo's observed commit LSN.
//! - [`gc`](XidRegistry::gc) — safety net for entries that will never be pruned
//!   normally (aborted delivery, lost unregister, slot far behind, rewound
//!   source). Runs at startup and periodically thereafter, driven by the
//!   connector.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock as SyncRwLock;
use runtime_checkpoint_api::BlobCheckpointStore;
use rustc_hash::{FxHashMap, FxHashSet};
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;
use tracing::warn;

/// Payload schema version for the persisted registry blob. Bump only on an
/// incompatible layout change; a blob whose version this build does not recognize
/// is discarded (with a warning) rather than mis-parsed.
const REGISTRY_VERSION: u32 = 1;

/// Distance, in transaction ids, at which the safety valve discards an entry: half
/// the 32-bit xid space. An entry this far behind the server's current `xid8` sits
/// beyond the point where its 32-bit stream projection could still be told apart
/// from a fresh transaction, so keeping it risks suppressing an unrelated change.
const XID_WRAPAROUND_SAFETY_DISTANCE: u64 = 1 << 31;

/// Project a 64-bit epoch-qualified `xid8` onto the 32-bit `xid` the pgoutput
/// stream carries. The stream's `Begin.xid` is exactly the low 32 bits of the
/// value `pg_current_xact_id()` returned for the same transaction.
#[expect(
    clippy::cast_possible_truncation,
    reason = "intentional low-32-bit projection: the pgoutput stream carries only a 32-bit xid, which is by definition the low 32 bits of the 64-bit xid8"
)]
const fn low32(xid8: u64) -> u32 {
    xid8 as u32
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to load the durable write-back transaction registry for dataset '{dataset}': {source}"
    ))]
    Load {
        dataset: String,
        source: runtime_checkpoint_api::CheckpointError,
    },

    #[snafu(display(
        "Failed to persist the durable write-back transaction registry for dataset '{dataset}': {source}"
    ))]
    Persist {
        dataset: String,
        source: runtime_checkpoint_api::CheckpointError,
    },

    #[snafu(display(
        "Failed to serialize the durable write-back transaction registry for dataset '{dataset}': {source}"
    ))]
    Serialize {
        dataset: String,
        source: serde_json::Error,
    },

    #[snafu(display(
        "Failed to parse the persisted durable write-back transaction registry for dataset '{dataset}': {source}"
    ))]
    Deserialize {
        dataset: String,
        source: serde_json::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// One outstanding write-back transaction whose echo has not yet been durably
/// consumed. See the module docs for the lifecycle these three fields drive.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct XidEntry {
    /// Full epoch-qualified id from `pg_current_xact_id()` / `txid_current()`. The
    /// pump matches against its low 32 bits (see [`low32`]); the full value drives
    /// the GC epoch-distance safety valve and `pg_xact_status` lookups.
    pub xid8: u64,
    /// `pg_current_wal_insert_lsn()` read *after* the delivery `COMMIT`
    /// (best-effort): an upper bound on the echo's commit LSN, used to GC an entry
    /// whose unregister was lost.
    pub commit_lsn_upper_bound: Option<u64>,
    /// The echo's actual commit LSN, learned when the pump observes its `Commit`.
    /// The entry becomes prunable once the durable applied position reaches this.
    pub observed_commit_lsn: Option<u64>,
}

/// Result of `pg_xact_status($1::xid8)` / `txid_status($1)` for one entry, supplied
/// to [`XidRegistry::gc`]. The registry never queries `PostgreSQL` itself; the
/// connector resolves these before calling `gc`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XactStatus {
    /// The transaction committed. Its echo either has arrived or still will.
    Committed,
    /// The transaction aborted — its delivery `COMMIT` failed, so no echo will ever
    /// arrive. The entry is safe to drop (a retry registers a fresh xid).
    Aborted,
    /// The transaction is still in progress on the server.
    InProgress,
    /// The status could not be determined (the transaction is older than the
    /// server's tracked range, or the query returned NULL). Handled by the
    /// epoch-distance safety valve rather than the aborted rule.
    Unknown,
}

/// The mutable, durably-persisted state, guarded by the async read-write lock so
/// every mutation and its persistence serialize as one critical section, while a
/// pure membership/duplicate check (the `register` fast path, `outstanding_xid8s`)
/// can take a shared read lock and run concurrently with other readers instead of
/// blocking behind every writer.
struct XidState {
    /// Keyed by the full `xid8` so a re-registration of the same transaction is a
    /// no-op and GC / `set_upper_bound` can look up by the exact id.
    entries: FxHashMap<u64, XidEntry>,
}

/// The single owner of the durable outstanding-write-back-xid set for one dataset.
///
/// Every mutation flows through [`state`](Self::state) (an async read-write lock)
/// and persists the whole blob *inside* that lock, so the delivery path, the pump,
/// and the applied-position writer never race on the persisted JSON. The lock is a
/// read-write lock rather than a plain mutex so a pure membership check (the
/// [`register`](Self::register) duplicate-xid fast path, [`outstanding_xid8s`](Self::outstanding_xid8s))
/// can take a shared read lock and run concurrently with other readers instead of
/// queueing behind every writer. The pump's hot-path [`contains`](Self::contains)
/// reads a separate lock-free-ish [`mirror`](Self::mirror) of the low-32-bit keys,
/// so it never blocks on a persistence round trip.
pub struct XidRegistry {
    store: Arc<dyn BlobCheckpointStore>,
    /// Identity of the source these xids belong to (endpoint/database/table), as in
    /// the applied-LSN sidecar. Persisted so a blob carried onto a different source
    /// is discarded rather than matched against the wrong stream.
    source: String,
    /// Dataset name, purely for user-facing log and error messages.
    dataset: String,
    state: RwLock<XidState>,
    /// Read-optimized mirror of the low-32-bit keys for the pump hot path. Rebuilt
    /// from `state.entries` after every successful mutation, so it only ever
    /// reflects persisted (or in-memory-authoritative) entries.
    mirror: SyncRwLock<FxHashSet<u32>>,
}

/// Versioned, source-qualified persisted form of the registry, mirroring
/// `StoredAppliedLsn`'s shape in the connector's applied-LSN sidecar.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredXidRegistry {
    version: u32,
    /// The source this set was recorded against. Absent (or mismatched) means the
    /// accelerator was repointed; the entries describe a different source's xids and
    /// are discarded on load.
    #[serde(default)]
    source: Option<String>,
    entries: Vec<XidEntry>,
}

impl XidRegistry {
    /// Load the persisted registry for a dataset, or start empty when nothing (or a
    /// foreign / unrecognized-version blob) is stored.
    ///
    /// `source_identity` is the same endpoint/database/table string the applied-LSN
    /// watermark uses; `dataset_name` is used only in user-facing messages. A blob
    /// that exists but cannot be parsed is surfaced as an error rather than silently
    /// dropped — losing outstanding entries risks a double-apply, so the caller
    /// decides how to proceed.
    ///
    /// # Errors
    ///
    /// Returns an error if the persisted blob cannot be read from the accelerator's
    /// checkpoint store, or if a stored blob cannot be parsed.
    pub async fn load(
        store: Arc<dyn BlobCheckpointStore>,
        source_identity: String,
        dataset_name: String,
    ) -> Result<Arc<Self>> {
        let loaded = store.get().await.context(LoadSnafu {
            dataset: dataset_name.clone(),
        })?;

        let entries: FxHashMap<u64, XidEntry> = match loaded {
            None => FxHashMap::default(),
            Some(blob) => {
                let stored: StoredXidRegistry =
                    serde_json::from_str(&blob.data).context(DeserializeSnafu {
                        dataset: dataset_name.clone(),
                    })?;
                if stored.version != REGISTRY_VERSION {
                    warn!(
                        dataset = %dataset_name,
                        stored_version = stored.version,
                        expected_version = REGISTRY_VERSION,
                        "The persisted durable write-back transaction registry for dataset '{dataset_name}' was written in an unrecognized format, so its outstanding change-echo suppression entries are discarded; a change already delivered to the source but not yet re-observed may be applied once more."
                    );
                    FxHashMap::default()
                } else if stored.source.as_deref() != Some(source_identity.as_str()) {
                    warn!(
                        dataset = %dataset_name,
                        recorded_for = stored.source.as_deref().unwrap_or("an unrecorded source"),
                        streaming_from = %source_identity,
                        "The persisted durable write-back transaction registry for dataset '{dataset_name}' was recorded against a different source, so its outstanding change-echo suppression entries are discarded; this is expected after repointing the dataset to a new source."
                    );
                    FxHashMap::default()
                } else {
                    stored
                        .entries
                        .into_iter()
                        .map(|entry| (entry.xid8, entry))
                        .collect()
                }
            }
        };

        let mirror = entries.keys().map(|&xid8| low32(xid8)).collect();

        Ok(Arc::new(Self {
            store,
            source: source_identity,
            dataset: dataset_name,
            state: RwLock::new(XidState { entries }),
            mirror: SyncRwLock::new(mirror),
        }))
    }

    /// Register a write-back transaction's `xid8`, persisting **before returning**.
    ///
    /// Callers MUST await this successfully before issuing the delivery `COMMIT`: the
    /// entry has to be durable before the echo can arrive. On a persistence failure
    /// the in-memory insertion is rolled back and the error is returned so the caller
    /// aborts delivery rather than committing an unsuppressible write.
    ///
    /// Takes a shared read lock first for the (common, cheap) case of an
    /// already-registered xid — for example a caller retrying a delivery after a
    /// transient failure — so concurrent duplicate checks don't queue behind each
    /// other or pay the cost of the write path below.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry cannot be persisted to the checkpoint store. The
    /// caller must then abort the delivery instead of issuing its `COMMIT`.
    pub async fn register(&self, xid8: u64) -> Result<()> {
        if self.state.read().await.entries.contains_key(&xid8) {
            // Already registered and previously persisted — nothing to write.
            return Ok(());
        }

        let mut guard = self.state.write().await;
        if guard.entries.contains_key(&xid8) {
            // A racing caller registered this xid between the read check above and
            // this write lock — nothing to write.
            return Ok(());
        }

        // Mutate a candidate map, not `guard.entries`, and only publish it (map and
        // mirror) after `upsert` succeeds. If this call is cancelled while the await
        // below is pending, dropping the future releases the lock without touching
        // `guard.entries` or the mirror, so a retry starts from the untouched state
        // instead of skipping persistence on a `contains_key` false positive.
        let mut candidate = guard.entries.clone();
        candidate.insert(
            xid8,
            XidEntry {
                xid8,
                commit_lsn_upper_bound: None,
                observed_commit_lsn: None,
            },
        );
        let payload = self.serialize(&candidate)?;

        self.store.upsert(&payload).await.context(PersistSnafu {
            dataset: self.dataset.clone(),
        })?;

        self.rebuild_mirror(&candidate);
        guard.entries = candidate;
        Ok(())
    }

    /// Record the post-`COMMIT` WAL upper bound for an entry (best-effort).
    ///
    /// Persistence failure here is tolerated — the entry is still bounded by the
    /// epoch-distance safety valve in [`gc`](Self::gc) — so it is logged, not
    /// returned. A missing entry (already pruned) is ignored.
    pub async fn set_upper_bound(&self, xid8: u64, lsn: u64) {
        let mut guard = self.state.write().await;
        let updated = if let Some(entry) = guard.entries.get_mut(&xid8) {
            entry.commit_lsn_upper_bound = Some(lsn);
            true
        } else {
            false
        };
        if updated {
            self.persist_or_warn(&guard.entries).await;
        }
    }

    /// Remove a single entry eagerly — the unregister path for a delivery whose
    /// `COMMIT` the source *unambiguously* aborted, so no echo will ever arrive
    /// (a retry registers a fresh xid). A missing entry is ignored.
    ///
    /// Reserved for a definite server-side rollback: an ambiguous failure (a
    /// connection drop mid-`COMMIT`) must **not** call this, because the
    /// transaction may have committed and its echo is still coming — startup
    /// [`gc`](Self::gc) resolves that case instead. Best-effort persistence: on
    /// failure the entry stays on disk and GC re-resolves it from
    /// `pg_xact_status`.
    pub async fn unregister(&self, xid8: u64) {
        let mut guard = self.state.write().await;
        if guard.entries.remove(&xid8).is_some() {
            self.rebuild_mirror(&guard.entries);
            self.persist_or_warn(&guard.entries).await;
        }
    }

    /// Hot-path membership test for the pump, once per source transaction.
    ///
    /// Reads only the low-32-bit mirror, never the async state lock or the store, so
    /// it never blocks on a persistence round trip.
    #[must_use]
    pub fn contains(&self, stream_xid: u32) -> bool {
        self.mirror.read().contains(&stream_xid)
    }

    /// The full `xid8` of every outstanding entry, for the connector to resolve
    /// per-entry `pg_xact_status` before calling [`gc`](Self::gc). The registry
    /// stays connection-free, so it hands out the ids and takes back the resolved
    /// statuses rather than querying the server itself. Returns an empty vector
    /// when nothing is outstanding, letting the caller skip the server round trip.
    #[must_use]
    pub async fn outstanding_xid8s(&self) -> Vec<u64> {
        self.state.read().await.entries.keys().copied().collect()
    }

    /// Record the echo's observed commit LSN when the pump sees its `Commit`.
    ///
    /// Matches on the low 32 bits (the stream's `xid` width). The entry is **not**
    /// removed here: unregistration is gated on the durable ack floor
    /// ([`prune_acked`](Self::prune_acked)), because the slot replays everything after
    /// `confirmed_flush` on any reconnect. Best-effort persistence — on failure the
    /// commit is simply re-observed after a restart.
    pub async fn mark_commit_observed(&self, stream_xid: u32, commit_lsn: u64) {
        let mut guard = self.state.write().await;
        let mut changed = false;
        for entry in guard.entries.values_mut() {
            if low32(entry.xid8) == stream_xid && entry.observed_commit_lsn.is_none() {
                entry.observed_commit_lsn = Some(commit_lsn);
                changed = true;
            }
        }
        if changed {
            self.persist_or_warn(&guard.entries).await;
        }
    }

    /// Drop every entry the durable applied position has now provably consumed.
    /// This is the only steady-state removal path, so it applies both rules
    /// [`gc`](Self::gc) uses to recognize a consumed echo:
    ///
    /// 1. An **observed** commit LSN at or below the floor — the pump saw the
    ///    echo's `Commit` and the applied position has since passed it.
    /// 2. A `commit_lsn_upper_bound` at or below the floor, even with no
    ///    observed commit — a delivery whose echo affects zero rows (for
    ///    example an absent-key `DELETE` that matches nothing at the source)
    ///    never appears in the change stream, so `observed_commit_lsn` never
    ///    gets set; without this rule such an entry survives every steady-state
    ///    prune and waits on the once-per-process startup [`gc`](Self::gc),
    ///    risking a 32-bit xid-wraparound collision on a long-lived process.
    ///
    /// An entry with neither field set is left untouched — nothing here says
    /// whether its echo is still pending. Best-effort persistence: a
    /// persistence failure leaves the entry on disk to be re-pruned (it is
    /// already consumed, so no echo re-arrives).
    pub async fn prune_acked(&self, durably_applied_lsn: u64) {
        let mut guard = self.state.write().await;
        let before = guard.entries.len();
        guard.entries.retain(|_, entry| {
            let observed_consumed = entry
                .observed_commit_lsn
                .is_some_and(|commit_lsn| commit_lsn <= durably_applied_lsn);
            let upper_bound_consumed = entry
                .commit_lsn_upper_bound
                .is_some_and(|upper_bound| upper_bound <= durably_applied_lsn);
            !(observed_consumed || upper_bound_consumed)
        });
        if guard.entries.len() != before {
            self.rebuild_mirror(&guard.entries);
            self.persist_or_warn(&guard.entries).await;
        }
    }

    /// Reconciliation garbage collection — the safety net for entries that will
    /// never be pruned normally. The connector runs it at startup and periodically
    /// thereafter. Applies all four design rules; any one is sufficient to drop
    /// an entry:
    ///
    /// 1. `pg_xact_status(xid8) == aborted` — the delivery `COMMIT` failed, so no echo
    ///    will ever arrive.
    /// 2. The echo's commit is provably consumed: either its `observed_commit_lsn`
    ///    or its `commit_lsn_upper_bound` is at or below `applied_lsn` — the same
    ///    two rules as [`prune_acked`](Self::prune_acked). The observed-LSN half
    ///    covers a crash after the applied watermark was saved but before the
    ///    steady-state prune persisted; the upper-bound half covers a lost
    ///    unregister.
    /// 3. The entry is at or ahead of `current_xid8` — the server's first
    ///    as-yet-unassigned transaction id, so an id at or above it was never
    ///    assigned in the source's current history. It can only come from a source
    ///    restored or rewound (for example point-in-time recovery) since the entry
    ///    was recorded, and once the restored server's ids catch up, its low 32
    ///    bits would suppress a genuine, unrelated change. It emits a user-facing
    ///    `warn!`.
    /// 4. The entry is more than ~2^31 transactions behind `current_xid8` — a slot
    ///    that far behind would long since have been invalidated; keeping the entry
    ///    risks a 32-bit xid-wraparound collision that would suppress a genuine,
    ///    unrelated source change. This rule alone bounds entries whose upper bound
    ///    was never persisted and whose echo was suppressed server-side. It emits a
    ///    user-facing `warn!`.
    ///
    /// `statuses` maps each entry's full `xid8` to its resolved [`XactStatus`], and
    /// its key set is exactly the `xid8`s [`outstanding_xid8s`](Self::outstanding_xid8s)
    /// returned when the connector took its reconciliation snapshot — an entry with a
    /// resolved-but-unknown status (older than the server's tracked range, or a failed
    /// lookup) is still a key, mapped to [`XactStatus::Unknown`]. Rules 3 and 4 infer
    /// staleness purely from comparing `xid8` against the snapshot's `current_xid8`,
    /// with no per-entry signal of their own, so they only fire for a `xid8` that was
    /// actually a member of that snapshot: a `xid8` **absent** from `statuses` was
    /// registered after the snapshot was taken (concurrently with this reconciliation
    /// pass), was never resolved against `current_xid8`, and is left untouched this
    /// round — without this restriction, a delivery registered between the snapshot
    /// and this call is at or above the (now-stale) `current_xid8` purely because it
    /// is new, and rule 3 would wrongly discard it as a rewound-source phantom before
    /// its `COMMIT` even runs, permanently unsuppressing its echo. It is picked up
    /// correctly on a later pass, once it either commits and reaches
    /// [`prune_acked`](Self::prune_acked) or is itself part of a subsequent snapshot.
    /// Rules 1 and 2 are keyed on per-entry data actually observed for that `xid8`
    /// (its resolved status, or its own recorded LSNs), not on snapshot membership, so
    /// they apply regardless of whether `xid8` is a key in `statuses`. The registry
    /// stays connection-free: the connector resolves `statuses` and `current_xid8`
    /// (the snapshot xmax) from a live connection and passes them in.
    pub async fn gc(
        &self,
        statuses: &HashMap<u64, XactStatus>,
        current_xid8: u64,
        applied_lsn: u64,
    ) {
        let mut guard = self.state.write().await;

        let mut to_remove: Vec<u64> = Vec::new();
        let mut safety_valve: Vec<u64> = Vec::new();
        let mut rewound: Vec<u64> = Vec::new();
        for (&xid8, entry) in &guard.entries {
            // Rules 3 and 4 below infer staleness purely from comparing `xid8`
            // against this pass's `current_xid8` snapshot, with no per-entry signal
            // of their own, so they must only fire for a `xid8` that was actually a
            // member of the reconciliation snapshot `statuses` was resolved from.
            let in_snapshot = statuses.contains_key(&xid8);
            let aborted = matches!(statuses.get(&xid8), Some(XactStatus::Aborted));
            let consumed = entry
                .observed_commit_lsn
                .is_some_and(|observed| applied_lsn >= observed)
                || entry
                    .commit_lsn_upper_bound
                    .is_some_and(|upper_bound| applied_lsn >= upper_bound);
            // `current_xid8` is the first as-yet-unassigned id (snapshot xmax), so
            // an id at or above it was never assigned in this server history.
            let ahead_of_current = in_snapshot && xid8 >= current_xid8;
            let too_far_behind =
                in_snapshot && current_xid8.saturating_sub(xid8) > XID_WRAPAROUND_SAFETY_DISTANCE;

            if aborted || consumed {
                to_remove.push(xid8);
            } else if ahead_of_current {
                to_remove.push(xid8);
                rewound.push(xid8);
            } else if too_far_behind {
                to_remove.push(xid8);
                safety_valve.push(xid8);
            }
        }

        if to_remove.is_empty() {
            return;
        }

        for &xid8 in &rewound {
            let dataset = self.dataset.as_str();
            warn!(
                dataset,
                xid8,
                current_xid8,
                "Durable write-back for dataset '{dataset}' is discarding an outstanding change-echo suppression entry (transaction {xid8}) that is ahead of the source's current transaction id {current_xid8}; a transaction id from the future means the source was restored or rewound (for example from a backup or point-in-time recovery), so the entry cannot describe its current history and is dropped to prevent it from suppressing an unrelated future change. Verify the dataset's replication slot exists on the restored source and re-synchronize if needed. See: https://spiceai.org/docs/components/data-connectors/postgres",
            );
        }

        for &xid8 in &safety_valve {
            let dataset = self.dataset.as_str();
            warn!(
                dataset,
                xid8,
                current_xid8,
                "Durable write-back for dataset '{dataset}' is discarding an outstanding change-echo suppression entry (transaction {xid8}) that is more than 2^31 source transactions behind the source's current transaction id {current_xid8}; the replication slot is too far behind for this echo to still be pending, so the entry is dropped to prevent a 32-bit transaction-id wraparound from suppressing an unrelated source change. If this dataset's replication was stalled, verify its slot health. See: https://spiceai.org/docs/components/data-connectors/postgres",
            );
        }

        for xid8 in to_remove {
            guard.entries.remove(&xid8);
        }
        self.rebuild_mirror(&guard.entries);
        self.persist_or_warn(&guard.entries).await;
    }

    /// Serialize the current entry set into its versioned, source-qualified blob.
    /// Entries are sorted by `xid8` for a deterministic payload.
    fn serialize(&self, entries: &FxHashMap<u64, XidEntry>) -> Result<String> {
        let mut list: Vec<XidEntry> = entries.values().cloned().collect();
        list.sort_by_key(|entry| entry.xid8);
        serde_json::to_string(&StoredXidRegistry {
            version: REGISTRY_VERSION,
            source: Some(self.source.clone()),
            entries: list,
        })
        .context(SerializeSnafu {
            dataset: self.dataset.clone(),
        })
    }

    /// Persist the entry set, logging (not returning) any failure. For the
    /// best-effort mutation paths whose durability is a correctness *convenience*,
    /// backed by GC, rather than a correctness *precondition* (which only
    /// [`register`](Self::register) is).
    async fn persist_or_warn(&self, entries: &FxHashMap<u64, XidEntry>) {
        let dataset = self.dataset.as_str();
        let payload = match self.serialize(entries) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(
                    dataset,
                    error = %e,
                    "Could not serialize the durable write-back transaction registry for dataset '{dataset}', so its outstanding change-echo suppression set was not persisted; startup garbage collection still bounds any stale entry.",
                );
                return;
            }
        };
        if let Err(e) = self.store.upsert(&payload).await {
            warn!(
                dataset,
                error = %e,
                "Could not persist the durable write-back transaction registry for dataset '{dataset}', so its outstanding change-echo suppression set may be stale after a restart; startup garbage collection still bounds any stale entry.",
            );
        }
    }

    /// Replace the hot-path mirror with the low-32-bit projection of the current
    /// entry set. Cheap: the outstanding set is bounded by the number of
    /// delivered-but-not-yet-durably-acked transactions.
    fn rebuild_mirror(&self, entries: &FxHashMap<u64, XidEntry>) {
        let mirror: FxHashSet<u32> = entries.keys().map(|&xid8| low32(xid8)).collect();
        *self.mirror.write() = mirror;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex as SyncMutex;
    use runtime_checkpoint_api::{BlobCheckpoint, BlobCheckpointStore, CheckpointError};

    use super::{XID_WRAPAROUND_SAFETY_DISTANCE, XactStatus, XidRegistry, low32};

    /// In-memory [`BlobCheckpointStore`] for the registry state-machine tests. Holds
    /// the single persisted blob and can be armed to fail the next `upsert`, so the
    /// register-must-persist path is exercised. `gate` can also be held by a test to
    /// suspend `upsert` indefinitely (signalling `reached_gate` right before
    /// blocking), so a caller can cancel `register` mid-persist.
    #[derive(Default)]
    struct FakeBlobStore {
        data: SyncMutex<Option<String>>,
        fail_next_upsert: SyncMutex<bool>,
        gate: tokio::sync::Mutex<()>,
        reached_gate: tokio::sync::Notify,
        /// Counts every call to `upsert`, so tests can assert that a batch of
        /// best-effort mutations (`prune_acked`, `mark_commit_observed`) collapses
        /// into far fewer whole-blob persists than calls, and that `register`'s
        /// read-lock fast path for an already-registered xid never persists at all.
        upsert_count: SyncMutex<u32>,
    }

    impl FakeBlobStore {
        fn arc() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn arm_upsert_failure(&self) {
            *self.fail_next_upsert.lock() = true;
        }

        fn stored(&self) -> Option<String> {
            self.data.lock().clone()
        }

        fn upsert_count(&self) -> u32 {
            *self.upsert_count.lock()
        }
    }

    #[async_trait::async_trait]
    impl BlobCheckpointStore for FakeBlobStore {
        async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError> {
            Ok(self.data.lock().clone().map(|data| BlobCheckpoint {
                data,
                updated_at: None,
            }))
        }

        async fn upsert(&self, data: &str) -> Result<(), CheckpointError> {
            // Scoped so the sync guard's lifetime unambiguously ends here, well
            // before the `.await` below — a guard merely `drop`ped mid-function
            // can still widen the async block's Send-ness analysis for some
            // control-flow shapes.
            let should_fail = {
                let mut fail = self.fail_next_upsert.lock();
                if *fail {
                    *fail = false;
                    true
                } else {
                    false
                }
            };
            if should_fail {
                return Err(CheckpointError::Store {
                    source: "injected upsert failure".into(),
                });
            }
            self.reached_gate.notify_one();
            let _permit = self.gate.lock().await;
            *self.data.lock() = Some(data.to_string());
            *self.upsert_count.lock() += 1;
            Ok(())
        }
    }

    const SOURCE: &str = "localhost:5432/db/public.orders";
    const DATASET: &str = "orders";

    async fn empty_registry(store: Arc<FakeBlobStore>) -> Arc<XidRegistry> {
        XidRegistry::load(store, SOURCE.to_string(), DATASET.to_string())
            .await
            .expect("fresh registry loads from an empty store")
    }

    /// The connector resolves each outstanding entry's `pg_xact_status` from this
    /// listing before calling `gc` — it must name every registered xid and none
    /// that were never registered or already pruned.
    #[tokio::test]
    async fn outstanding_xid8s_lists_every_registered_entry() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;
        assert!(
            registry.outstanding_xid8s().await.is_empty(),
            "a fresh registry has no outstanding entries"
        );

        registry.register(10).await.expect("register 10");
        registry.register(20).await.expect("register 20");
        let mut outstanding = registry.outstanding_xid8s().await;
        outstanding.sort_unstable();
        assert_eq!(outstanding, vec![10, 20]);

        registry.mark_commit_observed(low32(10), 100).await;
        registry.prune_acked(100).await;
        assert_eq!(
            registry.outstanding_xid8s().await,
            vec![20],
            "a pruned entry is no longer outstanding"
        );
    }

    /// The full happy path: register a delivery xid, observe its echo's commit, then
    /// prune it once the durable floor passes that commit LSN.
    #[tokio::test]
    async fn register_observe_prune_round_trip() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        let xid8: u64 = 4242;
        registry
            .register(xid8)
            .await
            .expect("register persists and returns");
        assert!(registry.contains(low32(xid8)), "registered xid is a member");

        registry.mark_commit_observed(low32(xid8), 900).await;
        {
            let state = registry.state.read().await;
            assert_eq!(
                state.entries[&xid8].observed_commit_lsn,
                Some(900),
                "commit LSN recorded on observation"
            );
        }

        // Floor below the commit LSN: entry survives.
        registry.prune_acked(899).await;
        assert!(
            registry.contains(low32(xid8)),
            "entry survives while the durable floor is below its commit LSN"
        );

        // Floor reaches the commit LSN: entry pruned.
        registry.prune_acked(900).await;
        assert!(
            !registry.contains(low32(xid8)),
            "entry pruned once the durable floor reaches its commit LSN"
        );
        let state = registry.state.read().await;
        assert!(state.entries.is_empty(), "no entries remain after prune");
    }

    /// `register` must be durable before it returns — callers persist before issuing
    /// the delivery COMMIT.
    #[tokio::test]
    async fn register_persists_before_returning() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        registry.register(7).await.expect("register succeeds");

        let persisted = store.stored().expect("register wrote the blob");
        assert!(
            persisted.contains("\"xid8\":7"),
            "the persisted blob names the registered xid: {persisted}"
        );
    }

    /// A persistence failure in `register` rolls the entry back and surfaces the
    /// error, so the caller aborts delivery instead of committing an unsuppressible
    /// write.
    #[tokio::test]
    async fn register_rolls_back_on_persist_failure() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        store.arm_upsert_failure();
        registry
            .register(11)
            .await
            .expect_err("register surfaces the persistence failure");
        assert!(
            !registry.contains(low32(11)),
            "the entry is rolled back after a failed persist"
        );
        let state = registry.state.read().await;
        assert!(state.entries.is_empty(), "no in-memory entry lingers");
    }

    /// Cancelling `register` while its `upsert` is in flight must leave the
    /// in-memory map and mirror exactly as they were: a candidate is only published
    /// after persistence succeeds, so dropping the future mid-`upsert` touches
    /// neither. A retry must then actually persist, rather than short-circuiting on
    /// a stale `contains_key`.
    #[tokio::test]
    async fn register_cancelled_mid_persist_leaves_state_untouched() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        let gate_guard = store.gate.lock().await;
        let task_registry = Arc::clone(&registry);
        let handle = tokio::spawn(async move { task_registry.register(99).await });

        // Wait until `upsert` is blocked on the gate, then cancel `register` while
        // it is suspended mid-persist.
        store.reached_gate.notified().await;
        handle.abort();
        drop(handle.await);
        drop(gate_guard);

        assert!(
            !registry.contains(low32(99)),
            "a cancelled register must not leave the mirror as if it had succeeded"
        );
        {
            let state = registry.state.read().await;
            assert!(
                !state.entries.contains_key(&99),
                "a cancelled register must not leave a half-registered in-memory entry"
            );
        }
        assert!(
            store.stored().is_none(),
            "a cancelled register must not have persisted anything"
        );

        registry
            .register(99)
            .await
            .expect("a retry after cancellation persists successfully");
        assert!(registry.contains(low32(99)), "the retry is a member");
        let persisted = store.stored().expect("the retry wrote the blob");
        assert!(
            persisted.contains("\"xid8\":99"),
            "the retry's persisted blob names the registered xid: {persisted}"
        );
    }

    /// The whole point of persistence: a reloaded registry sees exactly what was
    /// written, including the upper bound and observed commit LSN.
    #[tokio::test]
    async fn persistence_survives_reload() {
        let store = FakeBlobStore::arc();
        {
            let registry = empty_registry(Arc::clone(&store)).await;
            registry.register(100).await.expect("register 100");
            registry.register(200).await.expect("register 200");
            registry.set_upper_bound(100, 5000).await;
            registry.mark_commit_observed(low32(200), 6000).await;
        }

        let reloaded = empty_registry(Arc::clone(&store)).await;
        assert!(reloaded.contains(low32(100)), "100 survives reload");
        assert!(reloaded.contains(low32(200)), "200 survives reload");

        let state = reloaded.state.read().await;
        assert_eq!(state.entries[&100].commit_lsn_upper_bound, Some(5000));
        assert_eq!(state.entries[&100].observed_commit_lsn, None);
        assert_eq!(state.entries[&200].commit_lsn_upper_bound, None);
        assert_eq!(state.entries[&200].observed_commit_lsn, Some(6000));
    }

    /// A blob recorded against a different source is discarded on load — its xids
    /// name a different server's transactions and must not suppress this stream.
    #[tokio::test]
    async fn load_discards_foreign_source_blob() {
        let store = FakeBlobStore::arc();
        {
            let registry = empty_registry(Arc::clone(&store)).await;
            registry.register(555).await.expect("register 555");
        }
        // Reload under a different source identity.
        let store_dyn: Arc<dyn BlobCheckpointStore> = Arc::<FakeBlobStore>::clone(&store);
        let reloaded = XidRegistry::load(
            store_dyn,
            "otherhost:5432/db/public.orders".to_string(),
            DATASET.to_string(),
        )
        .await
        .expect("load succeeds even with a foreign blob");
        assert!(
            !reloaded.contains(low32(555)),
            "a foreign-source entry is not matched against this stream"
        );
        let state = reloaded.state.read().await;
        assert!(state.entries.is_empty());
    }

    /// GC rule 1: an aborted delivery's entry is dropped (no echo will ever arrive).
    #[tokio::test]
    async fn gc_drops_aborted_entries() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;
        registry.register(10).await.expect("register 10");
        registry.register(20).await.expect("register 20");

        let mut statuses = std::collections::HashMap::new();
        statuses.insert(10u64, XactStatus::Aborted);
        statuses.insert(20u64, XactStatus::Committed);

        // current_xid8 close by, applied_lsn 0: only the aborted rule can fire.
        registry.gc(&statuses, 30, 0).await;

        assert!(
            !registry.contains(low32(10)),
            "aborted delivery entry dropped"
        );
        assert!(
            registry.contains(low32(20)),
            "committed delivery entry retained"
        );
    }

    /// GC rule 2: an entry whose upper bound the durable applied position has reached
    /// is dropped (a lost unregister); one still ahead of the floor is kept.
    #[tokio::test]
    async fn gc_drops_entries_with_consumed_upper_bound() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;
        registry.register(10).await.expect("register 10");
        registry.register(20).await.expect("register 20");
        registry.set_upper_bound(10, 1000).await;
        registry.set_upper_bound(20, 3000).await;

        let statuses = std::collections::HashMap::new();
        // applied_lsn 2000: entry 10 (ub 1000) consumed, entry 20 (ub 3000) not.
        registry.gc(&statuses, 30, 2000).await;

        assert!(
            !registry.contains(low32(10)),
            "entry with consumed upper bound dropped"
        );
        assert!(
            registry.contains(low32(20)),
            "entry whose upper bound is still ahead of the floor retained"
        );
    }

    /// GC rule 2, observed-LSN half: an entry whose observed commit LSN the durable
    /// applied position has reached is dropped even with no upper bound recorded —
    /// the crash-between-watermark-save-and-prune case `prune_acked` would
    /// otherwise leave to the next steady-state prune.
    #[tokio::test]
    async fn gc_drops_entries_with_consumed_observed_commit() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;
        registry.register(10).await.expect("register 10");
        registry.register(20).await.expect("register 20");
        registry.mark_commit_observed(low32(10), 1000).await;
        registry.mark_commit_observed(low32(20), 3000).await;

        let statuses = std::collections::HashMap::new();
        // applied_lsn 2000: entry 10 (observed 1000) consumed, entry 20 (observed
        // 3000) not.
        registry.gc(&statuses, 30, 2000).await;

        assert!(
            !registry.contains(low32(10)),
            "entry with consumed observed commit LSN dropped"
        );
        assert!(
            registry.contains(low32(20)),
            "entry whose observed commit is still ahead of the floor retained"
        );
    }

    /// GC rule 3: an entry at or ahead of the server's current xid8 (the first
    /// as-yet-unassigned id) cannot belong to the current server history — it
    /// survives only a restore/rewind — and is dropped; an entry just behind the
    /// current id is a legitimately outstanding delivery and is retained.
    #[tokio::test]
    async fn gc_drops_entries_ahead_of_current_xid() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        let current_xid8: u64 = 1000;
        registry.register(1000).await.expect("register at current");
        registry.register(1500).await.expect("register ahead");
        registry.register(999).await.expect("register just behind");

        // All three entries were part of the reconciliation snapshot rules 3 and 4
        // are restricted to (see `gc`'s doc comment); their statuses are otherwise
        // irrelevant to rule 3, so `Unknown` (an unresolvable lookup) is enough.
        let mut statuses = std::collections::HashMap::new();
        statuses.insert(1000u64, XactStatus::Unknown);
        statuses.insert(1500u64, XactStatus::Unknown);
        statuses.insert(999u64, XactStatus::Unknown);
        registry.gc(&statuses, current_xid8, 0).await;

        assert!(
            !registry.contains(low32(1000)),
            "an entry at the first unassigned id was never assigned here and is dropped"
        );
        assert!(
            !registry.contains(low32(1500)),
            "an entry ahead of the current id is dropped"
        );
        assert!(
            registry.contains(low32(999)),
            "an entry just behind the current id is retained"
        );
    }

    /// GC rule 4: an entry strictly more than 2^31 transactions behind the server's
    /// current `xid8` is dropped by the wraparound safety valve; an entry exactly at
    /// the 2^31 boundary is kept (the rule is `>`, not `>=`).
    #[tokio::test]
    async fn gc_safety_valve_drops_far_behind_entries() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        // A current xid comfortably larger than the safety distance, so both test
        // entries are positive and their low-32 projections differ.
        let current_xid8: u64 = 1 << 33;
        let boundary_xid: u64 = current_xid8 - XID_WRAPAROUND_SAFETY_DISTANCE; // distance == 2^31
        let far_behind_xid: u64 = boundary_xid - 1; // distance == 2^31 + 1
        assert_ne!(
            low32(boundary_xid),
            low32(far_behind_xid),
            "the two entries must be distinguishable by their low-32 projection"
        );
        registry
            .register(far_behind_xid)
            .await
            .expect("register far-behind");
        registry
            .register(boundary_xid)
            .await
            .expect("register boundary");

        // Both entries were part of the reconciliation snapshot rule 4 is
        // restricted to (see `gc`'s doc comment).
        let mut statuses = std::collections::HashMap::new();
        statuses.insert(far_behind_xid, XactStatus::Unknown);
        statuses.insert(boundary_xid, XactStatus::Unknown);
        registry.gc(&statuses, current_xid8, 0).await;

        assert!(
            !registry.contains(low32(far_behind_xid)),
            "an entry more than 2^31 transactions behind is dropped"
        );
        assert!(
            registry.contains(low32(boundary_xid)),
            "an entry exactly at the 2^31 boundary distance is retained"
        );
    }

    /// Regression test for the race between periodic reconciliation and a live
    /// `register`: the connector snapshots `outstanding_xid8s()` and resolves each
    /// of those xids into `statuses` *before* calling `gc`, so a delivery
    /// registered afterwards — but before this `gc` call actually runs — is a live
    /// entry that is not a key in `statuses`. The old code inferred rule 3 purely
    /// from `xid8 >= current_xid8`, which is true of any brand-new transaction, so
    /// it wrongly swept the entry as a "rewound source" phantom before its
    /// `COMMIT` even ran — permanently unsuppressing its echo. The entry must
    /// survive this pass because it was never part of the reconciled snapshot.
    #[tokio::test]
    async fn gc_leaves_entry_registered_after_the_snapshot_untouched() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        // Simulate `reconcile_registry`: take the snapshot (empty, nothing
        // outstanding yet) and resolve it into `statuses` — still empty.
        let outstanding_at_snapshot_time = registry.outstanding_xid8s().await;
        assert!(outstanding_at_snapshot_time.is_empty());
        let statuses = std::collections::HashMap::new();

        // A new write-back registers concurrently, after the snapshot was taken
        // but before `gc` runs below.
        let new_xid8: u64 = 5000;
        registry
            .register(new_xid8)
            .await
            .expect("concurrent register succeeds");

        // `current_xid8` resolved at snapshot time is stale relative to the new
        // entry: it is at or below the new xid, which would have tripped the old
        // "ahead of current" rule.
        let current_xid8_at_snapshot_time = new_xid8;
        registry
            .gc(&statuses, current_xid8_at_snapshot_time, 0)
            .await;

        assert!(
            registry.contains(low32(new_xid8)),
            "an entry registered after the reconciliation snapshot was taken must \
             not be swept by a rule inferred from that stale snapshot"
        );
    }

    /// GC persists its result: a reloaded registry does not resurrect dropped
    /// entries.
    #[tokio::test]
    async fn gc_persists_removals() {
        let store = FakeBlobStore::arc();
        {
            let registry = empty_registry(Arc::clone(&store)).await;
            registry.register(10).await.expect("register 10");
            let mut statuses = std::collections::HashMap::new();
            statuses.insert(10u64, XactStatus::Aborted);
            registry.gc(&statuses, 30, 0).await;
        }
        let reloaded = empty_registry(Arc::clone(&store)).await;
        assert!(
            !reloaded.contains(low32(10)),
            "a GC'd entry stays gone across reload"
        );
    }

    /// `prune_acked` must never touch an entry whose echo has not been observed — the
    /// durable floor passing an LSN says nothing about an unobserved commit.
    #[tokio::test]
    async fn prune_ignores_entries_without_observed_commit() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;
        registry.register(77).await.expect("register 77");

        // No mark_commit_observed: pruning at any floor must leave it in place.
        registry.prune_acked(u64::MAX).await;
        assert!(
            registry.contains(low32(77)),
            "an entry without an observed commit LSN is never pruned"
        );
    }

    /// Membership matches on the low 32 bits of the 64-bit `xid8`, which is exactly the
    /// stream's xid width.
    #[tokio::test]
    async fn contains_matches_low_32_bits() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        // A high epoch bit set plus a distinctive low 32 bits.
        let xid8: u64 = (1u64 << 32) | 0x00AB_CDEF;
        registry.register(xid8).await.expect("register");

        assert!(
            registry.contains(0x00AB_CDEF),
            "matches on the low 32 bits regardless of the epoch bits"
        );
        assert!(
            !registry.contains(0x00AB_CDEE),
            "a different low-32 xid is not a member"
        );
    }

    /// Concurrent `register` and `contains` must not race or lose entries: every
    /// concurrently-registered xid is present afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_register_and_contains_are_safe() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        let mut handles = Vec::new();
        for xid8 in 1u64..=64 {
            let registry = Arc::clone(&registry);
            handles.push(tokio::spawn(async move {
                registry.register(xid8).await.expect("concurrent register");
                // Interleave reads with the writes.
                let _ = registry.contains(low32(xid8));
            }));
        }
        for handle in handles {
            handle.await.expect("task joins");
        }

        for xid8 in 1u64..=64 {
            assert!(
                registry.contains(low32(xid8)),
                "every concurrently-registered xid is present"
            );
        }
        let state = registry.state.read().await;
        assert_eq!(state.entries.len(), 64, "no registration was lost");
    }

    /// `register`'s read-lock fast path for an already-registered xid must not take
    /// the write lock or persist anything — the whole point of checking under a
    /// shared read lock first is to let concurrent duplicate checks avoid queueing
    /// behind, or paying the cost of, a write.
    #[tokio::test]
    async fn register_duplicate_takes_read_lock_fast_path_without_persisting() {
        let store = FakeBlobStore::arc();
        let registry = empty_registry(Arc::clone(&store)).await;

        registry
            .register(42)
            .await
            .expect("first register persists");
        let after_first = store.upsert_count();
        assert_eq!(after_first, 1, "the first registration persists once");

        for _ in 0..5 {
            registry
                .register(42)
                .await
                .expect("re-registering an already-registered xid is a no-op");
        }

        assert_eq!(
            store.upsert_count(),
            after_first,
            "re-registering an already-present xid must never persist"
        );
    }
}
