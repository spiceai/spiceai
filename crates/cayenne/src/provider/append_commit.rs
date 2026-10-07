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

//! Atomic publication of a fully validated append snapshot.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::sync::OwnedMutexGuard;
use turso_shared::{DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, retry_backoff_delay};

use super::column_stats::ColumnStatsAccumulator;
use super::context::CayenneContext;
use super::on_conflict::{OnConflictDeletions, PreparedOnConflictDeletionPublish};
use super::pk_index::PkDigestSet;
use super::table::{CayenneTableProvider, record_cayenne_write_phase};
use super::{Error, Result};
use crate::CayenneCatalog;
use crate::catalog::{CatalogResult, MetadataCatalog};
use crate::metastore::MetastoreTransaction;

/// Owns the write lock until durable publication, visibility, and maintenance
/// bookkeeping are complete, including when the requesting task is cancelled.
pub(super) struct PreparedResolvedAppend {
    pub table: CayenneTableProvider,
    pub context: Arc<CayenneContext>,
    pub _write_guard: OwnedMutexGuard<()>,
    pub snapshot_id: String,
    pub rows: u64,
    pub stats: Arc<ColumnStatsAccumulator>,
    pub deletions: OnConflictDeletions,
    pub validated_keys: PkDigestSet,
    pub superseded: usize,
    pub into_empty_table: bool,
}

/// A failed publication, and whether its snapshot's files must be kept because
/// the catalog may reference them.
struct Failure {
    error: Error,
    keep_files: bool,
}

impl Failure {
    fn discard(error: impl Into<Error>) -> Self {
        Self {
            error: error.into(),
            keep_files: false,
        }
    }
}

impl PreparedResolvedAppend {
    pub async fn commit(mut self) -> Result<u64> {
        let table_name = self.table.table_name().to_string();
        tokio::spawn(async move {
            match self.commit_inner().await {
                Ok(rows) => Ok(rows),
                Err(Failure { error, keep_files }) => {
                    self.table.clear_cached_pk_keyset();
                    if !keep_files {
                        super::staged_upsert::cleanup_orphan_snapshot_dir(
                            &self.table,
                            &self.snapshot_id,
                        )
                        .await;
                    }
                    Err(error)
                }
            }
        })
        .await
        .map_err(|source| Error::TaskPanicked {
            table: table_name,
            source,
        })?
    }

    async fn commit_inner(&mut self) -> std::result::Result<u64, Failure> {
        if self.rows == 0 {
            super::staged_upsert::cleanup_orphan_snapshot_dir(&self.table, &self.snapshot_id).await;
            return Ok(0);
        }
        let catalog = self
            .table
            .catalog()
            .as_any()
            .downcast_ref::<CayenneCatalog>()
            .ok_or(Failure::discard(Error::Unsupported {
                operation: "atomic append with a non-Cayenne metadata catalog",
            }))?;
        self.table
            .sync_local_snapshot_dir(&self.snapshot_id)
            .await
            .map_err(Failure::discard)?;
        #[cfg(test)]
        if let Some(pause) = test_seams::take_pause(self.table.table_id()) {
            pause().await;
        }
        let lock_start = Instant::now();
        let _visibility = self.table.visibility_lock_arc().lock_owned().await;
        let _fence = self.table.lock_listing_fence_write_owned().await;
        record_cayenne_write_phase(self.table.table_name(), "publish_lock_wait", lock_start);

        let publish_start = Instant::now();
        let mut prepared = self
            .table
            .prepare_on_conflict_deletions_for_staged_snapshot(
                std::mem::take(&mut self.deletions),
                self.snapshot_id.clone(),
                true,
            )
            .await
            .map_err(Failure::discard)?;
        prepared.publish_as_protected_snapshot = true;
        // The replacement snapshot and its inline tombstone become durable in
        // the same transaction. Restart must not depend on a later flag update.
        if let Some(payload) = prepared.durable_payload.as_mut()
            && let Some(tombstone) = payload.inline_tombstone.as_mut()
        {
            tombstone.published = true;
        }
        let sequence = prepared.snapshot_sequence;
        let reserved_delta = self.table.reserve_live_rows_delta();
        commit_metadata(catalog, &mut prepared).await?;

        let cas_start = Instant::now();
        self.table.publish_prepared_on_conflict_deletions(prepared);
        self.table.feed_staged_ivm_under_fence(None);
        let published_delta = reserved_delta.published();
        let retention = self.table.has_retention_delete_filters();
        let delta = i64::try_from(self.rows)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(self.superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(Arc::clone(&self.stats)),
            true,
            retention,
            delta,
            published_delta,
        );
        if retention || self.into_empty_table {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table
                .record_file_pk_keys(&self.validated_keys, sequence);
        }
        record_cayenne_write_phase(self.table.table_name(), "publish_cas", cas_start);
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);
        self.context.record_publish_latency(publish_start.elapsed());
        Ok(self.rows)
    }
}

async fn commit_metadata(
    catalog: &CayenneCatalog,
    prepared: &mut PreparedOnConflictDeletionPublish,
) -> std::result::Result<(), Failure> {
    for attempt in 1..=DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS {
        let mut txn = catalog
            .begin_transaction()
            .await
            .map_err(Failure::discard)?;
        if let Err(error) = catalog
            .apply_prepared_on_conflict_in_txn(txn.as_mut(), prepared)
            .await
        {
            let _ = txn.rollback().await;
            return Err(Failure::discard(error));
        }
        match commit_transaction(txn, prepared.table.table_id()).await {
            Ok(()) => {
                prepared.mark_catalog_committed();
                return Ok(());
            }
            Err(error)
                if attempt < DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS
                    && crate::is_retryable_write_conflict(&error) =>
            {
                tokio::time::sleep(retry_backoff_delay(attempt)).await;
            }
            Err(error) => return resolve_failed_commit(catalog, prepared, error.into()).await,
        }
    }
    Err(Failure::discard(Error::WriteConflict {
        table: prepared.table.table_name().to_string(),
    }))
}

/// A COMMIT that reports a failure may still have committed. The snapshot's
/// sequence row commits with the rest of the publication, so it is read back
/// before anything is discarded: present, the publication stands; absent, it
/// never happened; unreadable, the files are kept and the table refuses writes
/// until it is reloaded.
async fn resolve_failed_commit(
    catalog: &CayenneCatalog,
    prepared: &mut PreparedOnConflictDeletionPublish,
    error: Error,
) -> std::result::Result<(), Failure> {
    let table = prepared.table.table_name().to_string();
    match snapshot_sequence(
        catalog,
        prepared.table.table_id(),
        &prepared.target_snapshot_id,
    )
    .await
    {
        Ok(Some(sequence)) if sequence == prepared.snapshot_sequence => {
            tracing::warn!(
                "Dataset '{table}': the metastore reported that a write failed to commit, but it did commit, so it is published. Cause: {error}"
            );
            prepared.mark_catalog_committed();
            Ok(())
        }
        Ok(_) => Err(Failure::discard(error)),
        Err(read_error) => {
            prepared.retain_files_for_wal_recovery();
            prepared
                .table
                .publication_outcome_unknown()
                .store(true, Ordering::Release);
            Err(Failure {
                error: Error::IncompleteWrite {
                    table,
                    message: format!(
                        "a write's commit failed ({error}), and reading back whether it committed failed too ({read_error}). Its files are kept, and writes are refused until the table is reloaded from its catalog. Restart Spice to reload it"
                    ),
                },
                keep_files: true,
            })
        }
    }
}

#[cfg(not(test))]
pub(super) async fn commit_transaction(
    txn: Box<dyn MetastoreTransaction>,
    _table_id: &str,
) -> CatalogResult<()> {
    txn.commit().await
}

#[cfg(not(test))]
pub(super) async fn snapshot_sequence(
    catalog: &CayenneCatalog,
    table_id: &str,
    snapshot_id: &str,
) -> CatalogResult<Option<i64>> {
    catalog.get_snapshot_sequence(table_id, snapshot_id).await
}

#[cfg(test)]
pub(super) async fn commit_transaction(
    txn: Box<dyn MetastoreTransaction>,
    table_id: &str,
) -> CatalogResult<()> {
    use test_seams::CommitFault;
    let fault = test_seams::commit_fault(table_id);
    let committed = if fault == Some(CommitFault::RolledBack) {
        txn.rollback().await
    } else {
        txn.commit().await
    };
    match (committed, fault) {
        (Ok(()), Some(_)) => Err(crate::catalog::CatalogError::InvalidOperationNoSource {
            message: "injected commit failure".to_string(),
        }),
        (committed, _) => committed,
    }
}

#[cfg(test)]
pub(super) async fn snapshot_sequence(
    catalog: &CayenneCatalog,
    table_id: &str,
    snapshot_id: &str,
) -> CatalogResult<Option<i64>> {
    if test_seams::read_back_fails(table_id) {
        return Err(crate::catalog::CatalogError::InvalidOperationNoSource {
            message: "injected read-back failure".to_string(),
        });
    }
    catalog.get_snapshot_sequence(table_id, snapshot_id).await
}

/// Faults and pauses a test sets on one table's next resolved-append commit.
#[cfg(test)]
pub(crate) mod test_seams {
    use std::collections::HashMap;
    use std::sync::LazyLock;

    use parking_lot::Mutex;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum CommitFault {
        /// The catalog transaction commits, then reports a failure.
        CommittedButReported,
        /// As [`Self::CommittedButReported`], and reading the outcome back fails.
        CommittedUnreadable,
        /// The catalog transaction rolls back, then reports a failure.
        RolledBack,
    }

    pub(crate) type Pause = Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send>;

    static FAULTS: LazyLock<Mutex<HashMap<String, CommitFault>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    static PAUSES: LazyLock<Mutex<HashMap<String, Pause>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    pub(crate) fn inject(table_id: &str, fault: CommitFault) {
        FAULTS.lock().insert(table_id.to_string(), fault);
    }

    /// Run `pause` in the next commit, before it takes the visibility locks.
    pub(crate) fn pause_before_commit(table_id: &str, pause: Pause) {
        PAUSES.lock().insert(table_id.to_string(), pause);
    }

    pub(super) fn commit_fault(table_id: &str) -> Option<CommitFault> {
        FAULTS.lock().get(table_id).copied()
    }

    pub(super) fn read_back_fails(table_id: &str) -> bool {
        FAULTS.lock().remove(table_id) == Some(CommitFault::CommittedUnreadable)
    }

    pub(super) fn take_pause(table_id: &str) -> Option<Pause> {
        PAUSES.lock().remove(table_id)
    }
}
