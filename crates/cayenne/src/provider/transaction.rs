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

//! Per-request transaction, threaded from the HTTP executor
//! to the write path via the [`RequestContext`] extensions.
//!
//! A transaction (`BEGIN; SELECT assert(<gate>); UPDATE …;
//! COMMIT;`) must run **every** statement through the query builder (so authz,
//! masking, logging, and tracing apply uniformly) while still being atomic. The
//! executor cannot intercept the write plan without bypassing those checks, so
//! instead it installs this object on the request context and the write path
//! reads it back:
//!
//! 1. The executor captures the target table's optimistic-concurrency token
//!    ([`TransactionWriteToken`]) **before** the gate read and installs a
//!    [`Self::armed`] txn. Staging runs lock-free; the token is re-checked at
//!    commit (see the staged-upsert module).
//! 2. [`super::sink::CayenneDataSink`] (and the UPDATE insert leg) detects the
//!    active txn for its table, takes the token, and **stages** the write via
//!    [`super::table::CayenneTableProvider::begin_staged_upsert_occ`] instead of
//!    publishing, registering the [`CayenneStagedUpsert`] handle.
//! 3. At `COMMIT` the executor takes the staged handle and publishes it
//!    atomically (aborting with a retryable conflict if the token no longer
//!    holds); on a gate abort or any error it rolls it back.
//!
//! The object rides on the [`RequestContext`] typed extension (source 1 of
//! [`runtime_datafusion::extension::request_context::resolve_request_context`]),
//! so the write path reads the exact same context installed by the query
//! builder — never the task-local `RequestContext::current`, whose silent
//! internal-context fallback would make a missed installation publish
//! immediately (an undetectable atomicity break).

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::pk_index::PkDigestSet;
use super::staged_upsert::{CayenneStagedUpsert, PreparedTxnCommit, TransactionWriteToken};
use super::table::CayenneTableProvider;
use super::{Error, Result};
use crate::CayenneCatalog;

/// One participant table in a transaction (read and/or written): its per-table
/// begin token, provider, accumulated read footprint, and — once a write stages
/// — its staged handle. Written tables have `stage.is_some()`; read-only tables
/// are validated by version only.
pub struct TxnTable {
    /// Per-table begin sequence high-water (allocators are per-table, so the
    /// token is meaningless across tables).
    pub token: TransactionWriteToken,
    /// The table's provider (locks, catalog, per-key index) for validation and,
    /// for written tables, the fenced publish.
    pub provider: CayenneTableProvider,
    /// Digests of primary keys this transaction read from the table.
    pub footprint: HashSet<u128>,
    /// A read without a bounded PK predicate — forces per-table OCC fallback.
    pub footprint_incomplete: bool,
    /// The staged write handle, present iff the transaction wrote this table.
    pub stage: Option<CayenneStagedUpsert>,
}

struct TxnInner {
    /// One entry per participating table (read or written), keyed by `table_id`.
    /// A `BTreeMap` gives the canonical (sorted) commit order for the N-table
    /// lock/fence protocol.
    tables: Mutex<BTreeMap<String, TxnTable>>,
    /// Set when a statement read a Cayenne table that is NOT a registered
    /// participant — the transaction is fail-closed and aborts at commit.
    unregistered_read: AtomicBool,
}

/// A transaction handle. Cloning is a cheap `Arc` clone; the executor and the
/// write path share one inner object.
#[derive(Clone)]
pub struct CayenneTransaction(Arc<TxnInner>);

impl Default for CayenneTransaction {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CayenneTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let participants = self
            .0
            .tables
            .lock()
            .map(|t| t.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        f.debug_struct("CayenneTransaction")
            .field("participants", &participants)
            .finish_non_exhaustive()
    }
}

impl CayenneTransaction {
    /// Create an empty transaction; the executor registers each participant
    /// table (with its begin token) before running the body.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(TxnInner {
            tables: Mutex::new(BTreeMap::new()),
            unregistered_read: AtomicBool::new(false),
        }))
    }

    /// Register a participant table (read or written) with its begin token and
    /// provider. Captured up front by the executor, before the gate read.
    pub fn register(
        &self,
        table_id: String,
        token: TransactionWriteToken,
        provider: CayenneTableProvider,
    ) {
        if let Ok(mut tables) = self.0.tables.lock() {
            tables.entry(table_id).or_insert(TxnTable {
                token,
                provider,
                footprint: HashSet::new(),
                footprint_incomplete: false,
                stage: None,
            });
        }
    }

    /// Whether `table_id` is a registered participant of this transaction.
    #[must_use]
    pub fn is_participant(&self, table_id: &str) -> bool {
        self.0.tables.lock().is_ok_and(|t| t.contains_key(table_id))
    }

    /// Record primary-key digests a statement read from `table_id` (from the
    /// scan's pushed-down PK predicate) into that table's footprint.
    pub fn record_read_keys(&self, table_id: &str, digests: impl IntoIterator<Item = u128>) {
        if let Ok(mut tables) = self.0.tables.lock()
            && let Some(t) = tables.get_mut(table_id)
        {
            t.footprint.extend(digests);
        }
    }

    /// Mark `table_id`'s read footprint incomplete (an unbounded read) — commit
    /// falls back to the conservative per-table OCC check for that table.
    pub fn mark_footprint_incomplete(&self, table_id: &str) {
        if let Ok(mut tables) = self.0.tables.lock()
            && let Some(t) = tables.get_mut(table_id)
        {
            t.footprint_incomplete = true;
        }
    }

    /// Flag that a Cayenne table outside the participant set was read — the
    /// transaction is fail-closed and aborts at commit.
    pub fn mark_unregistered_read(&self) {
        self.0.unregistered_read.store(true, Ordering::Relaxed);
    }

    /// Whether a non-participant Cayenne table was read (fail-closed abort).
    #[must_use]
    pub fn has_unregistered_read(&self) -> bool {
        self.0.unregistered_read.load(Ordering::Relaxed)
    }

    /// Take the begin token for `table_id` so the write path can stage. Returns
    /// `None` if the table is not a participant or already staged (v1 allows one
    /// write per table); the write path treats `None` as fail-closed.
    #[must_use]
    pub fn take_token(&self, table_id: &str) -> Option<TransactionWriteToken> {
        let tables = self.0.tables.lock().ok()?;
        let t = tables.get(table_id)?;
        if t.stage.is_some() {
            return None;
        }
        Some(t.token)
    }

    /// Register the staged write for `table_id`, after the write path staged it.
    pub fn set_staged(&self, table_id: &str, upsert: CayenneStagedUpsert) {
        if let Ok(mut tables) = self.0.tables.lock()
            && let Some(t) = tables.get_mut(table_id)
        {
            t.stage = Some(upsert);
        }
    }

    /// Take all participants (consuming the map) in canonical `table_id` order
    /// for the commit protocol.
    #[must_use]
    pub fn take_all(&self) -> Vec<TxnTable> {
        self.0
            .tables
            .lock()
            .map(|mut t| std::mem::take(&mut *t).into_values().collect())
            .unwrap_or_default()
    }

    /// Discard all transaction state (abort path when nothing was published).
    pub fn release(&self) {
        if let Ok(mut tables) = self.0.tables.lock() {
            tables.clear();
        }
    }

    /// Abort the transaction before commit: roll back every staged write
    /// (removing its staged snapshot directory) and discard all state. Used when
    /// a gate fails or a statement errors.
    pub async fn abort(&self) {
        rollback_staged(self.take_all()).await;
    }

    /// Commit the transaction atomically across every written table.
    ///
    /// Consumes the registered participants (sorted by `table_id`) and runs the
    /// N-table protocol:
    ///
    /// 1. Fail closed if any statement read a Cayenne table outside the
    ///    participant set (`unregistered_read`) — the snapshot is not fully
    ///    validated, so serializability cannot be guaranteed.
    /// 2. Take every participant's `write_lock` in canonical `table_id` order
    ///    (deadlock-free), heal any interrupted write on the written tables, then
    ///    per-key OCC re-check **every** participant — read-only tables by their
    ///    read footprint, written tables by footprint ∪ write-set. Any table
    ///    committed to since the transaction began aborts with a retryable
    ///    [`Error::WriteConflict`].
    /// 3. Take each written table's commit prerequisites (canonical order):
    ///    the mem-tier checkpoint lock, and the inline move / materialization,
    ///    which take the listing fence themselves and so run before any fence is
    ///    held here.
    /// 4. Take every written table's visibility lock, then its listing fence
    ///    (canonical order), owning each fence guard (`HeldListingFence`).
    ///    `write_lock` alone does NOT exclude every publisher: a CDC Stage-B
    ///    publish, a staged position-vector publish and a compaction flip run
    ///    without it and are excluded only by the visibility lock / listing
    ///    fence. With all of them held, re-run the per-key OCC (activity that
    ///    registered before the waits) — the one post-wait validation point.
    /// 5. Take ONE `now()` for the whole publication, after every wait, and
    ///    `prepare_commit_under_guards` each written table (reserve sequences,
    ///    write deletion-vector files; retention scans lease the table's own
    ///    held fence and complete inside the prepare) **before** opening the
    ///    shared metastore transaction — the sequence allocator autocommits,
    ///    which would deadlock against an open `BEGIN IMMEDIATE`.
    /// 6. Before the durable point, revoke every fence lease and require that
    ///    no leased scan still runs (else an ordinary abort, nothing committed).
    /// 7. Fuse every table's durable publish into one `MetastoreTransaction` on
    ///    the shared catalog and commit it (bounded retry on a busy backend).
    ///    All tables become durable together or none do.
    /// 8. Flip each written table's in-memory visibility under its held fence;
    ///    after the durable commit nothing returns an ordinary error.
    ///
    /// On any conflict or error before the shared transaction commits, every
    /// staged/prepared write is rolled back (staged snapshot directories removed)
    /// and nothing is published.
    ///
    /// Cancellation (the caller drops this future): before the shared COMMIT
    /// starts, the guards unwind, every leased scan was awaited inside its
    /// prepare (its pin dropped with it), each prepared publish's `Drop`
    /// removes only its own uncommitted deletion-vector files, and the staged
    /// snapshot directories, never registered in the catalog, are left to the
    /// orphan sweep (no `Drop` removes them: there is no synchronous removal,
    /// and none is needed for correctness). While the COMMIT's outcome is
    /// unresolved, `OutcomeUnknownOnDrop` keeps every file and fails the
    /// tables closed. After it, `mark_committed` runs before any await and the
    /// in-memory switch is synchronous, so no cancellation point separates the
    /// durable commit from its publication, and no cleanup can remove a file
    /// the catalog references.
    ///
    /// # Errors
    ///
    /// [`Error::WriteConflict`] on a lost OCC race (retryable → HTTP 409); an
    /// [`Error::Internal`] fail-closed abort for an unregistered read or a
    /// cross-database participant set; or the underlying catalog / object-store
    /// error if preparing or committing the fused write fails.
    pub async fn commit(&self) -> Result<TransactionCommit> {
        if self.has_unregistered_read() {
            rollback_staged(self.take_all()).await;
            return Err(Error::Internal {
                table: "<transaction>".to_string(),
                message: "transaction read a Cayenne table outside its registered participant \
                          set; cannot guarantee serializable isolation"
                    .to_string(),
            });
        }

        // Canonical (sorted) participant order from the `BTreeMap`.
        let mut participants = self.take_all();
        if !participants.iter().any(|p| p.stage.is_some()) {
            // Read-only transaction: nothing to publish.
            return Ok(TransactionCommit::empty());
        }

        // 1. write_lock every participant in canonical order.
        let write_guards = {
            let mut guards = Vec::with_capacity(participants.len());
            for p in &participants {
                guards.push(p.provider.write_lock_arc().lock_owned().await);
            }
            guards
        };

        // 2. Heal interrupted writes on written tables, then OCC-validate all.
        let mut hard_err: Option<Error> = None;
        for p in &participants {
            if p.stage.is_some()
                && let Err(e) = p.provider.ensure_no_incomplete_write().await
            {
                hard_err = Some(e);
                break;
            }
        }
        let mut conflict: Option<String> = None;
        if hard_err.is_none() {
            let empty = PkDigestSet::with_capacity(0);
            for p in &participants {
                let current = p.provider.sequence_high_water().await;
                let write_set = p
                    .stage
                    .as_ref()
                    .map_or(&empty, CayenneStagedUpsert::validated_keys);
                if !p.token.staging_clean()
                    || p.provider.transaction_has_conflict(
                        p.token.stage_seq(),
                        &p.footprint,
                        !p.footprint_incomplete,
                        write_set,
                        current,
                    )
                {
                    conflict = Some(p.provider.table_name().to_string());
                    break;
                }
            }
        }
        if let Some(e) = hard_err {
            drop(write_guards);
            rollback_staged(participants).await;
            return Err(e);
        }
        if let Some(table) = conflict {
            drop(write_guards);
            rollback_staged(participants).await;
            return Err(Error::WriteConflict { table });
        }

        // 3. Every written table with rows, in the canonical order.
        let staged: Vec<CayenneStagedUpsert> = participants
            .iter_mut()
            .filter_map(|p| p.stage.take())
            .collect();
        // A statement that staged no rows has nothing to publish: no snapshot,
        // and no deletions, which only incoming rows make.
        let mut staged_with_rows = Vec::with_capacity(staged.len());
        for stage in staged {
            if stage.publishes_nothing() {
                if let Err(e) = stage.rollback().await {
                    tracing::warn!("transaction: staged rollback cleanup failed: {e}");
                }
            } else {
                staged_with_rows.push(stage);
            }
        }
        if staged_with_rows.is_empty() {
            drop(write_guards);
            return Ok(TransactionCommit::empty());
        }

        // 4. All written tables must share one metastore database (checked
        //    before any further lock is taken).
        let shared_catalog = Arc::clone(staged_with_rows[0].table().catalog());
        if staged_with_rows[1..]
            .iter()
            .any(|stage| !Arc::ptr_eq(stage.table().catalog(), &shared_catalog))
        {
            drop(write_guards);
            rollback_stages(staged_with_rows).await;
            return Err(Error::Internal {
                table: "<transaction>".to_string(),
                message: "multi-table transaction spans more than one metastore database"
                    .to_string(),
            });
        }
        let Some(catalog) = shared_catalog.as_any().downcast_ref::<CayenneCatalog>() else {
            drop(write_guards);
            rollback_stages(staged_with_rows).await;
            return Err(Error::Internal {
                table: "<transaction>".to_string(),
                message: "transaction participant is not backed by a Cayenne catalog".to_string(),
            });
        };

        // 5. Prerequisites, canonical order, before any publication guard:
        //    every written table's mem-tier checkpoint lock (always; held in its
        //    `PreparedTxnCommit` through `finish`), and the inline move, which
        //    takes that table's listing fence itself (never `write_lock` or the
        //    checkpoint lock, both already held here) and so must never run
        //    under the fences held below. Cross-table order: all `write_lock`s,
        //    then each table's checkpoint lock in `table_id` order, then all
        //    visibility locks, then all fences; no holder of a checkpoint lock
        //    blocks on `write_lock` (`acquire_capture_locks_blocking` drops it
        //    first), and no holder of a visibility lock or fence waits on a
        //    checkpoint lock.
        let mut prerequisites = Vec::with_capacity(staged_with_rows.len());
        for stage in &staged_with_rows {
            match stage.acquire_commit_prerequisites().await {
                Ok(taken) => prerequisites.push(taken),
                Err(e) => {
                    drop(prerequisites);
                    drop(write_guards);
                    rollback_stages(staged_with_rows).await;
                    return Err(e);
                }
            }
        }

        // 6. Every publication guard, canonical order: visibility, then listing
        //    fence (the order every other holder of both takes). Held across the
        //    prepare, the fused durable commit and the in-memory publish, so no
        //    publisher that bypasses `write_lock` (a CDC Stage-B publish, a
        //    staged position-vector publish, a compaction flip) lands between
        //    this commit's retention scan and its publication.
        let mut visibility_guards = Vec::with_capacity(staged_with_rows.len());
        for stage in &staged_with_rows {
            visibility_guards.push(stage.table().visibility_lock_arc().lock_owned().await);
        }
        let mut fence_guards: Vec<super::delete::HeldListingFence> =
            Vec::with_capacity(staged_with_rows.len());
        for stage in &staged_with_rows {
            fence_guards.push(super::delete::HeldListingFence::new(
                stage.table().lock_listing_fence_write_owned().await,
            ));
        }

        // 7. Re-validate after every wait. `write_lock` (held since step 1)
        //    does not exclude a CDC Stage-B publish, which runs under the
        //    visibility lock and listing fence only; its keys were recorded at
        //    its reserved sequence under `write_lock` before that was released,
        //    so this check sees any such activity that registered before the
        //    waits, and nothing further can publish while these guards are held.
        let mut conflict: Option<String> = None;
        {
            let empty = PkDigestSet::with_capacity(0);
            for p in &participants {
                let current = p.provider.sequence_high_water().await;
                let write_set = staged_with_rows
                    .iter()
                    .find(|stage| stage.table().table_id() == p.provider.table_id())
                    .map_or(&empty, CayenneStagedUpsert::validated_keys);
                if !p.token.staging_clean()
                    || p.provider.transaction_has_conflict(
                        p.token.stage_seq(),
                        &p.footprint,
                        !p.footprint_incomplete,
                        write_set,
                        current,
                    )
                {
                    conflict = Some(p.provider.table_name().to_string());
                    break;
                }
            }
        }
        if let Some(table) = conflict {
            drop(fence_guards);
            drop(visibility_guards);
            drop(prerequisites);
            drop(write_guards);
            rollback_stages(staged_with_rows).await;
            return Err(Error::WriteConflict { table });
        }
        #[cfg(test)]
        for stage in &staged_with_rows {
            if let Some(pause) =
                super::append_commit::test_seams::take_pause_under_publication_guards(
                    stage.table().table_id(),
                )
            {
                pause().await;
            }
        }
        // The read-only participants are validated; only their held write_locks
        // matter now.
        drop(participants);

        // 8. ONE instant for the whole publication, after every wait above;
        //    every participant's `retention_sql` context folds `now()` at it.
        let commit_now = chrono::Utc::now();
        let mut contexts = Vec::with_capacity(staged_with_rows.len());
        for stage in &staged_with_rows {
            match stage.commit_retention_context_at(commit_now) {
                Ok(context) => contexts.push(context),
                Err(e) => {
                    drop(fence_guards);
                    drop(visibility_guards);
                    drop(prerequisites);
                    drop(write_guards);
                    rollback_stages(staged_with_rows).await;
                    return Err(e);
                }
            }
        }

        // 9. Prepare each table under the held guards (retention scans lease
        //    the table's own fence and complete before the prepare returns).
        let mut prepared: Vec<PreparedTxnCommit> = Vec::with_capacity(staged_with_rows.len());
        {
            let mut inputs = staged_with_rows
                .into_iter()
                .zip(prerequisites)
                .zip(contexts)
                .zip(fence_guards.iter());
            while let Some((((stage, taken), context), fence)) = inputs.next() {
                match stage
                    .prepare_commit_under_guards(taken, fence, context)
                    .await
                {
                    Ok(pc) => prepared.push(pc),
                    Err(e) => {
                        let remaining: Vec<CayenneStagedUpsert> =
                            inputs.map(|(((stage, _), _), _)| stage).collect();
                        drop(fence_guards);
                        drop(visibility_guards);
                        drop(write_guards);
                        rollback_prepared(prepared).await;
                        rollback_stages(remaining).await;
                        return Err(e);
                    }
                }
            }
        }

        // 10. Before the durable point: revoke every lease and require that no
        //     scan the prepare started still runs. A violation here is an
        //     ordinary abort (nothing committed); after the revoke no lease can be
        //     pinned again, so nothing leased runs past this point.
        if let Some(busy) = fence_guards
            .iter()
            .zip(&prepared)
            .find(|(fence, _)| !fence.revoke_leases_and_check_idle())
            .map(|(_, pc)| pc.table_id().to_string())
        {
            drop(fence_guards);
            drop(visibility_guards);
            drop(write_guards);
            rollback_prepared(prepared).await;
            return Err(Error::Internal {
                table: busy,
                message: "a retention scan was still running on the transaction's listing fence \
                          before its commit; nothing was committed"
                    .to_string(),
            });
        }

        // 11. Fuse every table's durable publish into one transaction; commit or none.
        match commit_fused(catalog, &mut prepared).await {
            Ok(()) => {}
            Err(FusedFailure::Uncommitted(e)) => {
                drop(fence_guards);
                drop(visibility_guards);
                drop(write_guards);
                rollback_prepared(prepared).await;
                return Err(e);
            }
            // The catalog may reference the staged files: keep them.
            Err(FusedFailure::Unknown(e)) => return Err(e),
        }

        // 12. Flip in-memory visibility under the held fences. The durable write
        //     already committed, so a finish failure is logged, not fatal (reopen
        //     reconstructs the same state from the catalog), and nothing below can
        //     turn the committed outcome into an error: releasing the guards has
        //     no error path, and the leases were revoked (and found idle) before
        //     the durable point, so no capture can still run on a fence.
        let mut summary = TransactionCommit::empty();
        for pc in prepared {
            let table_id = pc.table_id().to_string();
            let rows = pc.row_count();
            if let Err(e) = pc.finish() {
                tracing::warn!(
                    "transaction: in-memory publish for table '{table_id}' failed after durable \
                     commit (reopen will reconcile): {e}"
                );
            }
            summary.row_count += rows;
            summary.written_tables.push(table_id);
        }
        drop(fence_guards);
        drop(visibility_guards);
        drop(write_guards);
        Ok(summary)
    }
}

/// A fused commit that failed: never committed, so its staged files may go, or
/// of unknown outcome, so they stay.
enum FusedFailure {
    Uncommitted(Error),
    Unknown(Error),
}

/// Fuse every prepared table's durable publish into one `MetastoreTransaction`.
/// Bounded retry on a busy backend; an `apply` failure is terminal (the whole
/// multi-table commit aborts). On success every table is durable together.
/// The prepared commits of a shared COMMIT whose outcome the caller has not
/// yet observed. The COMMIT can land durably while its future is cancelled (a
/// dropped caller, an aborted task); dropping this guard before
/// [`Self::observed`] treats that as an unknown outcome — every table keeps
/// its staged and deletion-vector files and refuses writes until it is
/// reloaded — rather than letting each publish's abort cleanup delete files a
/// committed catalog references, or letting later writes build on an
/// in-memory view the catalog has moved past.
///
/// This is fail-closed, not a completed finish: the in-memory switch and the
/// post-publish steps do not run, and the outcome (committed or not) is
/// resolved only by reloading the table. Cancellation before the shared
/// COMMIT begins is not handled here: the catalog transaction is dropped
/// uncommitted and each publish's abort cleanup runs.
struct OutcomeUnknownOnDrop<'a>(Option<&'a mut [PreparedTxnCommit]>);

impl<'a> OutcomeUnknownOnDrop<'a> {
    fn prepared(&self) -> &[PreparedTxnCommit] {
        self.0.as_deref().unwrap_or_default()
    }

    /// The outcome is now known to the caller; hand the commits back.
    fn observed(mut self) -> &'a mut [PreparedTxnCommit] {
        self.0.take().unwrap_or_default()
    }
}

impl Drop for OutcomeUnknownOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(prepared) = self.0.take() {
            for pc in prepared.iter_mut() {
                pc.retain_after_unknown_outcome();
            }
        }
    }
}

async fn commit_fused(
    catalog: &CayenneCatalog,
    prepared: &mut [PreparedTxnCommit],
) -> std::result::Result<(), FusedFailure> {
    use write_retry::{DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, retry_backoff_delay};

    let first_table = prepared
        .first()
        .map(|pc| pc.table_id().to_string())
        .unwrap_or_default();
    for attempt in 1..=DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS {
        let mut txn = catalog
            .begin_transaction()
            .await
            .map_err(|e| FusedFailure::Uncommitted(e.into()))?;
        let mut apply_err = None;
        for pc in prepared.iter_mut() {
            if let Err(e) = pc.apply_in_txn(catalog, txn.as_mut()).await {
                apply_err = Some(e);
                break;
            }
        }
        if let Some(e) = apply_err {
            let _ = txn.rollback().await;
            return Err(FusedFailure::Uncommitted(Error::from(e)));
        }
        let pending = OutcomeUnknownOnDrop(Some(&mut *prepared));
        match super::append_commit::commit_transaction(txn, &first_table).await {
            Ok(()) => {
                // Durably committed — disarm each publish's abort cleanup so its
                // `Drop` does not delete the now-live deletion-vector files.
                for pc in pending.observed().iter_mut() {
                    pc.mark_committed();
                }
                return Ok(());
            }
            Err(e)
                if attempt < DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS
                    && crate::is_retryable_write_conflict(&e) =>
            {
                // A conflict is a definite rollback.
                pending.observed();
                tracing::debug!(
                    attempt,
                    "retrying fused multi-table transaction after commit conflict"
                );
                tokio::time::sleep(retry_backoff_delay(attempt)).await;
            }
            Err(e) => return resolve_failed_fused_commit(catalog, pending, Error::from(e)).await,
        }
    }
    Err(FusedFailure::Uncommitted(Error::WriteConflict {
        table: "<transaction>".to_string(),
    }))
}

/// A COMMIT that reports a failure may still have committed. Every table's
/// staged snapshot sequence commits with the rest of the shared transaction,
/// so they are read back before anything is discarded: all present, the
/// transaction stands; none, it never happened; otherwise the staged files are
/// kept and each table refuses writes until it is reloaded.
async fn resolve_failed_fused_commit(
    catalog: &CayenneCatalog,
    pending: OutcomeUnknownOnDrop<'_>,
    error: Error,
) -> std::result::Result<(), FusedFailure> {
    let mut committed = 0;
    let mut unreadable = None;
    for pc in pending.prepared() {
        match pc.read_back_committed(catalog).await {
            Ok(true) => committed += 1,
            Ok(false) => {}
            Err(read_error) => {
                unreadable = Some(read_error);
                break;
            }
        }
    }
    let prepared = pending.observed();
    match unreadable {
        None if committed == prepared.len() => {
            tracing::warn!(
                "The metastore reported that a transaction failed to commit, but it did commit, so it is published. Cause: {error}"
            );
            for pc in prepared.iter_mut() {
                pc.mark_committed();
            }
            Ok(())
        }
        None if committed == 0 => Err(FusedFailure::Uncommitted(error)),
        _ => {
            for pc in prepared.iter_mut() {
                pc.retain_after_unknown_outcome();
            }
            let read_back = unreadable.map_or_else(
                || {
                    format!(
                        "{committed} of {} tables read back as committed",
                        prepared.len()
                    )
                },
                |read_error| format!("reading back whether it committed failed too ({read_error})"),
            );
            Err(FusedFailure::Unknown(Error::IncompleteWrite {
                table: "<transaction>".to_string(),
                message: format!(
                    "a transaction's commit failed ({error}), and {read_back}. Its files are kept, and writes to its tables are refused until they are reloaded from their catalog. Restart Spice to reload them"
                ),
            }))
        }
    }
}

/// Roll back staged-but-unprepared writes: remove each staged snapshot directory.
async fn rollback_staged(participants: Vec<TxnTable>) {
    for mut p in participants {
        if let Some(stage) = p.stage.take()
            && let Err(e) = stage.rollback().await
        {
            tracing::warn!("transaction: staged rollback cleanup failed: {e}");
        }
    }
}

/// Discard staged upserts that never reached a prepare.
async fn rollback_stages(stages: Vec<CayenneStagedUpsert>) {
    for stage in stages {
        if let Err(e) = stage.rollback().await {
            tracing::warn!("transaction: staged rollback cleanup failed: {e}");
        }
    }
}

/// Roll back prepared-but-uncommitted writes: remove each staged snapshot
/// directory (the shared transaction was never committed).
async fn rollback_prepared(prepared: Vec<PreparedTxnCommit>) {
    for pc in prepared {
        pc.rollback().await;
    }
}

/// Outcome of a committed multi-table transaction: the total rows published and
/// the `table_id`s written (for the executor's post-commit cache invalidation).
#[derive(Debug, Default)]
pub struct TransactionCommit {
    /// Total rows published across all written tables.
    pub row_count: u64,
    /// The `table_id`s whose staged writes were published.
    pub written_tables: Vec<String>,
}

impl TransactionCommit {
    fn empty() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl runtime_request_context::Extension for CayenneTransaction {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
