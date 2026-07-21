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
    /// 3. `prepare_commit` each written table (reserve sequences + write
    ///    deletion-vector files) **before** opening the shared metastore
    ///    transaction — the sequence allocator autocommits, which would deadlock
    ///    against an open `BEGIN IMMEDIATE`.
    /// 4. Hold each written table's visibility + listing-fence locks (canonical
    ///    order), then fuse every table's durable publish into one
    ///    `MetastoreTransaction` on the shared catalog and commit it (bounded
    ///    retry on a busy backend). All tables become durable together or none do.
    /// 5. Flip each written table's in-memory visibility under its held fence.
    ///
    /// On any conflict or error before the shared transaction commits, every
    /// staged/prepared write is rolled back (staged snapshot directories removed)
    /// and nothing is published.
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

        // 3. Prepare each written table (reserve sequences + write DV files)
        //    before opening the shared transaction.
        let staged: Vec<CayenneStagedUpsert> = participants
            .iter_mut()
            .filter_map(|p| p.stage.take())
            .collect();
        // The read-only participants are validated; only their held write_locks
        // matter now, so drop the `TxnTable`s and keep the guards.
        drop(participants);
        let mut staged_iter = staged.into_iter();
        let mut prepared: Vec<PreparedTxnCommit> = Vec::new();
        while let Some(stage) = staged_iter.next() {
            match stage.prepare_commit().await {
                Ok(pc) => prepared.push(pc),
                Err(e) => {
                    drop(write_guards);
                    rollback_prepared(prepared).await;
                    for remaining in staged_iter {
                        let _ = remaining.rollback().await;
                    }
                    return Err(e);
                }
            }
        }

        // 4. All written tables must share one metastore database.
        let shared_catalog = Arc::clone(prepared[0].provider().catalog());
        if prepared[1..]
            .iter()
            .any(|pc| !Arc::ptr_eq(pc.provider().catalog(), &shared_catalog))
        {
            drop(write_guards);
            rollback_prepared(prepared).await;
            return Err(Error::Internal {
                table: "<transaction>".to_string(),
                message: "multi-table transaction spans more than one metastore database"
                    .to_string(),
            });
        }
        let Some(catalog) = shared_catalog.as_any().downcast_ref::<CayenneCatalog>() else {
            drop(write_guards);
            rollback_prepared(prepared).await;
            return Err(Error::Internal {
                table: "<transaction>".to_string(),
                message: "transaction participant is not backed by a Cayenne catalog".to_string(),
            });
        };

        // Hold visibility + listing-fence locks (canonical order) across the fused
        // durable commit and the in-memory publish.
        let mut visibility_guards = Vec::with_capacity(prepared.len());
        for pc in &prepared {
            visibility_guards.push(pc.provider().visibility_lock_arc().lock_owned().await);
        }
        let mut fence_guards = Vec::with_capacity(prepared.len());
        for pc in &prepared {
            fence_guards.push(pc.provider().lock_listing_fence_write_owned().await);
        }

        // Fuse every table's durable publish into one transaction; commit or none.
        if let Err(e) = commit_fused(catalog, &mut prepared).await {
            drop(fence_guards);
            drop(visibility_guards);
            drop(write_guards);
            rollback_prepared(prepared).await;
            return Err(e);
        }

        // 5. Flip in-memory visibility under the held fences. The durable write
        //    already committed, so a finish failure is logged, not fatal (reopen
        //    reconstructs the same state from the catalog).
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

/// Fuse every prepared table's durable publish into one `MetastoreTransaction`.
/// Bounded retry on a busy backend; an `apply` failure is terminal (the whole
/// multi-table commit aborts). On success every table is durable together.
async fn commit_fused(catalog: &CayenneCatalog, prepared: &mut [PreparedTxnCommit]) -> Result<()> {
    use turso_shared::{DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, retry_backoff_delay};

    for attempt in 1..=DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS {
        let mut txn = catalog.begin_transaction().await?;
        let mut apply_err = None;
        for pc in prepared.iter_mut() {
            if let Err(e) = pc.apply_in_txn(catalog, txn.as_mut()).await {
                apply_err = Some(e);
                break;
            }
        }
        if let Some(e) = apply_err {
            let _ = txn.rollback().await;
            return Err(Error::from(e));
        }
        match txn.commit().await {
            Ok(()) => {
                // Durably committed — disarm each publish's abort cleanup so its
                // `Drop` does not delete the now-live deletion-vector files.
                for pc in prepared.iter_mut() {
                    pc.mark_committed();
                }
                return Ok(());
            }
            Err(e)
                if attempt < DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS
                    && crate::is_retryable_write_conflict(&e) =>
            {
                tracing::debug!(
                    attempt,
                    "retrying fused multi-table transaction after commit conflict"
                );
                tokio::time::sleep(retry_backoff_delay(attempt)).await;
            }
            Err(e) => return Err(Error::from(e)),
        }
    }
    Err(Error::WriteConflict {
        table: "<transaction>".to_string(),
    })
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
