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
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::staged_upsert::{CayenneStagedUpsert, TransactionWriteToken};

/// The mutable stage of a transaction.
///
/// Only ever holds one of the two: the optimistic-concurrency token captured at
/// transaction begin (before the write stages), or the staged handle after.
enum TxnStage {
    /// Token captured by the executor; no write staged yet.
    Armed(TransactionWriteToken),
    /// Write staged (not yet published).
    Staged(CayenneStagedUpsert),
}

struct TxnInner {
    /// Identifies the single table this transaction may write to.
    table_id: String,
    /// `None` only transiently, while the sink moves the token into a staged
    /// handle. Never held across an await.
    stage: Mutex<Option<TxnStage>>,
    /// Read footprint: digests of the primary keys the transaction's statements
    /// read (extracted from pushed-down PK equality/IN predicates). Re-checked
    /// per-key at commit against each key's stored commit sequence.
    footprint: Mutex<HashSet<u128>>,
    /// Set when a statement read this table with a predicate the footprint
    /// capture could not resolve to a bounded key set (an unbounded-scan gate).
    /// Forces the commit to fall back to the conservative per-table OCC check.
    footprint_incomplete: AtomicBool,
}

/// A transaction handle. Cloning is a cheap `Arc` clone; the
/// executor and the write path share one inner object.
#[derive(Clone)]
pub struct CayenneTransaction(Arc<TxnInner>);

impl std::fmt::Debug for CayenneTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneTransaction")
            .field("table_id", &self.0.table_id)
            .finish_non_exhaustive()
    }
}

impl CayenneTransaction {
    /// Create a transaction armed with the target table's optimistic-concurrency
    /// token (captured before the gate read).
    #[must_use]
    pub fn armed(table_id: String, token: TransactionWriteToken) -> Self {
        Self(Arc::new(TxnInner {
            table_id,
            stage: Mutex::new(Some(TxnStage::Armed(token))),
            footprint: Mutex::new(HashSet::new()),
            footprint_incomplete: AtomicBool::new(false),
        }))
    }

    /// The single table this transaction may write to.
    #[must_use]
    pub fn table_id(&self) -> &str {
        &self.0.table_id
    }

    /// Record primary-key digests read by a statement into the transaction's
    /// read footprint (called from the scan when a bounded PK predicate is
    /// pushed down for this table).
    pub fn record_read_keys(&self, digests: impl IntoIterator<Item = u128>) {
        if let Ok(mut fp) = self.0.footprint.lock() {
            fp.extend(digests);
        }
    }

    /// Mark the read footprint incomplete — a statement read this table without
    /// a bounded PK predicate, so commit must fall back to per-table OCC.
    pub fn mark_footprint_incomplete(&self) {
        self.0.footprint_incomplete.store(true, Ordering::Relaxed);
    }

    /// Take the accumulated read footprint and whether it is complete. Returns
    /// `(digests, complete)`; `complete == false` forces per-table fallback.
    #[must_use]
    pub fn take_footprint(&self) -> (HashSet<u128>, bool) {
        let complete = !self.0.footprint_incomplete.load(Ordering::Relaxed);
        let digests = self
            .0
            .footprint
            .lock()
            .map(|mut fp| std::mem::take(&mut *fp))
            .unwrap_or_default();
        (digests, complete)
    }

    /// Take the armed OCC token so the caller can hand it to
    /// [`super::table::CayenneTableProvider::begin_staged_upsert_occ`].
    ///
    /// Returns `None` if the transaction is not in the armed state — i.e. a
    /// second write in the same transaction (already staged) or a poisoned
    /// lock. The write path treats `None` as fail-closed (rejects the write)
    /// rather than publishing.
    #[must_use]
    pub fn take_token(&self) -> Option<TransactionWriteToken> {
        let mut stage = self.0.stage.lock().ok()?;
        match stage.take() {
            Some(TxnStage::Armed(token)) => Some(token),
            other => {
                *stage = other;
                None
            }
        }
    }

    /// Register the staged write. Called by the write path after
    /// [`Self::take_token`] and a successful stage.
    pub fn set_staged(&self, upsert: CayenneStagedUpsert) {
        if let Ok(mut stage) = self.0.stage.lock() {
            *stage = Some(TxnStage::Staged(upsert));
        }
    }

    /// Take the staged handle for commit or rollback. Returns `None` if no
    /// write was staged (the transaction is still armed, or already consumed).
    #[must_use]
    pub fn take_staged(&self) -> Option<CayenneStagedUpsert> {
        let mut stage = self.0.stage.lock().ok()?;
        match stage.take() {
            Some(TxnStage::Staged(upsert)) => Some(upsert),
            other => {
                *stage = other;
                None
            }
        }
    }

    /// Discard any remaining stage. Used on the abort path when the write never
    /// reached the sink (e.g. a gate assert failed before the write statement
    /// ran). Off-lock staging holds no write guard, so there is nothing to
    /// release beyond dropping the token / staged handle.
    pub fn release(&self) {
        if let Ok(mut stage) = self.0.stage.lock() {
            let _ = stage.take();
        }
    }
}

#[async_trait::async_trait]
impl runtime_request_context::Extension for CayenneTransaction {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
