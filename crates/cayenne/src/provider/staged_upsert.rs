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

//! Optimistic primary-key writes staged in unpublished snapshots.
//!
//! Validation uses a private keyset without holding the table write lock. Each
//! statement resolves repeated keys against its complete input. Commit holds
//! the write lock while checking the transaction's read footprint and write-set
//! against a token captured before its gate read. Conflicting writes fail with
//! [`Error::WriteConflict`].
//!
//! Snapshot visibility and deletion metadata commit in one catalog transaction.
//! The protected snapshot's sequence exceeds its deletion sequence so replacement
//! rows survive their own tombstones. The listing fence covers publication of
//! the matching in-memory views. Rollback discards unpublished files.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;

use super::Error;
use super::Result;
use super::append_stage::{StagedAppend, ValidationScope};
use super::column_stats::ColumnStatsAccumulator;
use super::delta_encoding::WritePolicy;
use super::on_conflict::{OnConflictDeletions, PostValidationState};
use super::pk_index::PkDigestSet;
use super::table::CayenneTableProvider;

/// What [`CayenneStagedUpsert::acquire_commit_prerequisites`] took for one
/// table, handed to [`CayenneStagedUpsert::prepare_commit_under_guards`].
pub(crate) struct CommitPrerequisites {
    /// The mem-tier checkpoint lock, held to the end of the commit.
    checkpoint_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    /// The staged keys, read before any fold.
    superseding: Option<Vec<arrow::record_batch::RecordBatch>>,
}

/// Optimistic-concurrency token for an off-lock transaction write.
///
/// Captured at transaction **begin** (before the gate read) under a brief
/// `write_lock` hold, and re-checked at commit under `write_lock`. It is the
/// per-table sequence high-water plus a "no staging append in flight" bit —
/// together, everything that changes key liveness on the table either advances
/// the sequence or sets the staging bit, so an unchanged token at commit proves
/// no intervening commit touched the table (see the module docs).
#[derive(Debug, Clone, Copy)]
pub struct TransactionWriteToken {
    /// The table's sequence high-water at capture (`allocator.next - 1`).
    stage_seq: i64,
    /// Whether no pipelined staging append was in flight at capture. A staging
    /// append's Stage-B finalize publishes without drawing a sequence, so it is
    /// covered by this bit rather than by `stage_seq`.
    staging_clean: bool,
}

impl TransactionWriteToken {
    /// The table's begin sequence high-water (per-table; each table's allocator
    /// is independent).
    #[must_use]
    pub fn stage_seq(&self) -> i64 {
        self.stage_seq
    }

    /// Whether no staging append was in flight at capture.
    #[must_use]
    pub fn staging_clean(&self) -> bool {
        self.staging_clean
    }
}

/// A staged upsert: the replacement rows have been written to a fresh snapshot
/// directory, but no catalog visibility change has been made yet. The rows are
/// published (or discarded) at [`Self::commit`] / [`Self::rollback`].
pub struct CayenneStagedUpsert {
    table: CayenneTableProvider,
    /// Optimistic-concurrency token captured at transaction begin, re-checked at
    /// commit against the table's live sequence high-water.
    token: TransactionWriteToken,
    new_snapshot_id: String,
    /// The prior versions this upsert supersedes, captured at validation time
    /// and applied under the fence at commit. Taken (`mem::take`) on commit.
    on_conflict_deletions: OnConflictDeletions,
    validated_keys: PkDigestSet,
    stats: Arc<ColumnStatsAccumulator>,
    row_count: u64,
    superseded: usize,
    /// The table has `retention_sql`: staging evaluated nothing, and the commit
    /// evaluates the staged candidates and the resident rows it keeps in ONE
    /// retention context created after its lock waits and optimistic check.
    retention_deferred: bool,
    /// The superseded copies a folding table has not yet folded out of the
    /// staged files (see [`super::append_stage::StagedAppend::fold`]).
    fold: Option<super::append_stage::PendingFold>,
    /// Staged rows the commit's `retention_sql` folded out of the staged files:
    /// not stored, but still reported accepted, as the insert count always was.
    retention_folded: u64,
}

impl std::fmt::Debug for CayenneStagedUpsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedUpsert")
            .field("table", &self.table.table_name())
            .field("new_snapshot_id", &self.new_snapshot_id)
            .field("row_count", &self.row_count)
            .field("superseded", &self.superseded)
            .finish_non_exhaustive()
    }
}

impl CayenneStagedUpsert {
    fn new(
        table: CayenneTableProvider,
        token: TransactionWriteToken,
        staged: StagedAppend,
        retention_deferred: bool,
    ) -> Self {
        Self {
            table,
            token,
            new_snapshot_id: staged.new_snapshot_id,
            on_conflict_deletions: staged.on_conflict_deletions,
            validated_keys: staged.validated_keys,
            stats: staged.stats,
            row_count: staged.row_count,
            superseded: staged.superseded,
            retention_deferred,
            fold: staged.fold,
            retention_folded: 0,
        }
    }

    /// Number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The write-set (kept keys) of this staged upsert — union'd into the
    /// commit-time per-key OCC re-check.
    pub(crate) fn validated_keys(&self) -> &PkDigestSet {
        &self.validated_keys
    }

    /// The table this upsert was staged for.
    pub(crate) fn table(&self) -> &CayenneTableProvider {
        &self.table
    }

    /// The staged snapshot's id (its unregistered directory until committed).
    #[cfg(test)]
    pub(crate) fn staged_snapshot_id(&self) -> &str {
        &self.new_snapshot_id
    }

    /// Whether committing publishes nothing: no row was staged, so no stored
    /// row is superseded either.
    pub(crate) fn publishes_nothing(&self) -> bool {
        self.row_count == 0
            && self.on_conflict_deletions.delete_specs.is_empty()
            && self.on_conflict_deletions.total_superseded() == 0
            && self.on_conflict_deletions.reinserted_over_tombstone == 0
    }

    /// Under every held publication guard of the commit (its `write_lock`,
    /// mem-tier checkpoint lock, visibility lock and `fence`, this table's own
    /// listing-fence write guard), in the commit's one retention context
    /// `filters` (folded at an instant taken after every lock wait): evaluate
    /// the staged candidates (each winning copy once; superseded copies never)
    /// and prepare the delete over the resident rows this publication keeps.
    /// Every scan leases `fence` and finishes before this returns. Prepared
    /// before the snapshot sequence is reserved, so resident key deletes
    /// precede the staged rows. `None` without `retention_sql`. An error
    /// publishes nothing.
    async fn prepare_retention_under_guards(
        &mut self,
        superseding: Option<Vec<arrow::record_batch::RecordBatch>>,
        fence: &super::delete::HeldListingFence,
        filters: Option<Arc<Vec<datafusion::logical_expr::Expr>>>,
    ) -> Result<Option<super::delete::PreparedDeletionPublish>> {
        let Some(filters) = filters else {
            return Ok(None);
        };
        super::overwrite_retention::with_publication_retention(
            Arc::clone(&filters),
            self.retain_staged_candidates(),
        )
        .await?;
        let prepared = super::overwrite_retention::with_publication_retention(
            Arc::clone(&filters),
            self.table.prepare_transaction_resident_retention(
                &self.new_snapshot_id,
                &filters,
                superseding,
                Some(fence),
            ),
        )
        .await?;
        #[cfg(test)]
        if let Some(pause) =
            super::append_commit::test_seams::take_pause_after_commit_time_retention(
                self.table.table_id(),
            )
        {
            pause().await;
        }
        Ok(prepared)
    }

    /// Evaluate `retention_sql` (the caller's context) over the staged rows the
    /// statement's own repeated keys left visible, and hide the matches: a
    /// folding table folds them out with the pending superseded copies in one
    /// rewrite (counted out of `row_count`, as a fold at staging was); any other
    /// hides them by position in the staged files (counted as superseded).
    async fn retain_staged_candidates(&mut self) -> Result<()> {
        // Only a folding table or one hiding by position can hide a staged row;
        // any other shape never evaluated its staged rows (unchanged).
        if self.fold.is_none() && !self.table.hides_repeated_keys_with_position_deletes() {
            return Ok(());
        }
        let marker = format!("/{}/", self.new_snapshot_id);
        let already_hidden: std::collections::HashMap<String, Vec<u32>> = match &self.fold {
            Some(fold) => fold.superseded.clone(),
            None => self
                .on_conflict_deletions
                .delete_specs
                .iter()
                .filter(|(path, _)| path.contains(&marker))
                .map(|(path, rows)| {
                    let mut rows: Vec<u32> = rows
                        .iter()
                        .filter_map(|&row| u32::try_from(row).ok())
                        .collect();
                    rows.sort_unstable();
                    rows.dedup();
                    (path.to_string(), rows)
                })
                .collect(),
        };
        let hidden = self
            .table
            .hide_retention_matches_in_snapshot(&self.new_snapshot_id, already_hidden.clone())
            .await?;
        match self.fold.take() {
            Some(fold) => {
                if hidden.is_empty() {
                    return Ok(());
                }
                let dropped: u64 = hidden.values().map(|rows| rows.len() as u64).sum();
                let repeats: u64 = fold.superseded.values().map(|rows| rows.len() as u64).sum();
                self.retention_folded = dropped.saturating_sub(repeats);
                self.stats = self
                    .table
                    .fold_superseded_copies(
                        &self.new_snapshot_id,
                        &hidden,
                        fold.write,
                        &fold.file_stats,
                        &self.stats,
                    )
                    .await?;
                self.row_count = self.row_count.saturating_sub(dropped);
            }
            None => {
                for (path, rows) in hidden {
                    let before: std::collections::HashSet<u32> = already_hidden
                        .get(&path)
                        .map(|rows| rows.iter().copied().collect())
                        .unwrap_or_default();
                    let new: Vec<u64> = rows
                        .into_iter()
                        .filter(|row| !before.contains(row))
                        .map(u64::from)
                        .collect();
                    if new.is_empty() {
                        continue;
                    }
                    self.superseded = self.superseded.saturating_add(new.len());
                    self.on_conflict_deletions
                        .delete_specs
                        .entry(Arc::from(path.as_str()))
                        .or_default()
                        .extend(new);
                }
            }
        }
        Ok(())
    }

    /// Step 1 of a commit for this table (transactional or single-table),
    /// under the commit's held `write_lock` (after its optimistic check) and
    /// BEFORE any visibility lock or listing fence: wait for the mem-tier
    /// checkpoint lock, always (held to the end of the commit, as a durable
    /// write holds it, so every commit publishes under all four guards), then,
    /// under `retention_sql`, move the inline corpus into files and capture the
    /// staged keys before any fold. The inline move takes the listing fence
    /// itself (`checkpoint_inlined_data`), and neither `write_lock` nor the
    /// checkpoint lock, which is why it runs here, under the two locks this
    /// commit already holds, and never under the commit's held fence.
    ///
    /// # Errors
    ///
    /// Returns an error if the inline corpus cannot be moved or the staged keys
    /// cannot be read. Nothing is published.
    pub(crate) async fn acquire_commit_prerequisites(&self) -> Result<CommitPrerequisites> {
        let generation = self.publishes_carried_generation()?;
        #[cfg(test)]
        if let Some(pause) =
            super::append_commit::test_seams::take_pause_at_checkpoint_wait(self.table.table_id())
        {
            pause().await;
        }
        let checkpoint_guard = self
            .table
            .lock_mem_tier_checkpoint_for_durable_write()
            .await;
        if generation {
            self.table.materialize_inline_rows_for_carry().await?;
            return Ok(CommitPrerequisites {
                checkpoint_guard: Some(checkpoint_guard),
                superseding: None,
            });
        }
        if !self.retention_deferred {
            return Ok(CommitPrerequisites {
                checkpoint_guard: Some(checkpoint_guard),
                superseding: None,
            });
        }
        // Every staged key supersedes its resident version, a replacement the
        // predicate is about to fold out of the staged files included: capture
        // them before the fold, so resident retention still skips (never
        // evaluates) that superseded version.
        let superseding = self.table.staged_key_columns(&self.new_snapshot_id).await?;
        self.table
            .move_inline_corpus_to_files_for_retention()
            .await?;
        Ok(CommitPrerequisites {
            checkpoint_guard: Some(checkpoint_guard),
            superseding,
        })
    }

    /// This table's `retention_sql` context for a commit, with `now()` folded
    /// at `at`: the one instant the transaction takes after every
    /// participant's lock waits. `None` without deferred retention.
    pub(crate) fn commit_retention_context_at(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Arc<Vec<datafusion::logical_expr::Expr>>>> {
        if !self.retention_deferred {
            return Ok(None);
        }
        Ok(Some(Arc::new(
            self.table
                .coerced_retention_filters_at(at)
                .map_err(|source| Error::DataFusion { source })?,
        )))
    }

    /// Step 2 of a transaction commit for this table: prepare the durable
    /// payload (reserve sequences, write deletion-vector files, no metastore
    /// commit) under every held publication guard of the transaction. `fence`
    /// is this table's own listing-fence write guard; every scan here leases it
    /// and finishes before this returns. `filters` is this table's context
    /// from the transaction's one post-wait instant. The caller applies the
    /// payload via [`PreparedTxnCommit::apply_in_txn`] inside the shared
    /// transaction and publishes with [`PreparedTxnCommit::finish`].
    ///
    /// # Errors
    ///
    /// Returns an error if retention cannot be evaluated, the staged snapshot
    /// directory cannot be synced, or reserving sequences / writing deletion
    /// vectors fails. Nothing is published.
    pub(crate) async fn prepare_commit_under_guards(
        self,
        prerequisites: CommitPrerequisites,
        fence: &super::delete::HeldListingFence,
        filters: Option<Arc<Vec<datafusion::logical_expr::Expr>>>,
    ) -> Result<PreparedTxnCommit> {
        // Nothing is durable before the shared commit, so a failed prepare
        // removes its own staged directory (the existing rollback step) rather
        // than leave it to the orphan sweep; the stage is consumed here, so the
        // transaction cannot roll it back itself.
        let table = self.table.clone_for_write();
        let snapshot_id = self.new_snapshot_id.clone();
        let prepared = self
            .prepare_commit_under_guards_inner(prerequisites, fence, filters)
            .await;
        if prepared.is_err() {
            cleanup_orphan_snapshot_dir(&table, &snapshot_id).await;
        }
        prepared
    }

    async fn prepare_commit_under_guards_inner(
        mut self,
        prerequisites: CommitPrerequisites,
        fence: &super::delete::HeldListingFence,
        filters: Option<Arc<Vec<datafusion::logical_expr::Expr>>>,
    ) -> Result<PreparedTxnCommit> {
        if !fence.locks(self.table.listing_fence_arc()) {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: "transaction prepare given another table's listing fence".to_string(),
            });
        }
        let CommitPrerequisites {
            checkpoint_guard,
            superseding,
        } = prerequisites;
        if self.publishes_carried_generation()? {
            self.table
                .sync_local_snapshot_dir(&self.new_snapshot_id)
                .await?;
            return self
                .prepare_generation_commit(checkpoint_guard, fence, filters)
                .await;
        }
        // The commit's retention context first: a fold rewrites staged files,
        // so the directory barrier below follows it.
        let resident_retention = self
            .prepare_retention_under_guards(superseding, fence, filters)
            .await?;
        // Directory barrier for the staged rows, before the shared transaction
        // makes them visible. The non-transactional publish takes this barrier
        // in `record_written_snapshot_sequence`; the fused path commits the
        // snapshot-sequence row inside the caller's multi-table transaction and
        // so never reaches that helper — without the barrier here a power loss
        // can leave the catalog referencing a snapshot whose directory entries
        // were never flushed. Nothing after `stage_upsert_data` adds files to
        // this directory (the on-conflict deletion vectors are written under the
        // *current* snapshot's `_deletes/`), so the barrier is complete here.
        self.table
            .sync_local_snapshot_dir(&self.new_snapshot_id)
            .await?;

        let on_conflict_deletions = std::mem::take(&mut self.on_conflict_deletions);
        // Reserve sequences + write deletion-vector files + build the inline
        // tombstone, WITHOUT committing the catalog metadata — `defer_catalog_commit`
        // carries them in the returned publish's `durable_payload` for application
        // in the shared multi-table transaction. This composes inline-mode
        // deletions (unlike the earlier bespoke path, which rejected them).
        let mut publish = self
            .table
            .prepare_on_conflict_deletions_for_staged_snapshot(
                on_conflict_deletions,
                self.new_snapshot_id.clone(),
                true,
            )
            .await?;
        // `defer_catalog_commit` couples two concerns for the append path: it
        // both defers the durable catalog write AND suppresses the in-memory
        // protected-snapshot promotion (appends publish into the current
        // snapshot, not a protected one). A staged UPSERT still needs the
        // protected-snapshot promotion — the durable side is recorded by
        // `apply_prepared_on_conflict_in_txn` — so re-enable just the in-memory
        // promotion, which `finish` performs via `publish_prepared_on_conflict_deletions`.
        publish.publish_as_protected_snapshot = true;
        // The resident retention delete commits in the same shared transaction
        // as the snapshot; until then its files are cleaned up with the publish.
        if let Some(resident) = resident_retention.as_ref() {
            let Some(payload) = publish.durable_payload.as_mut() else {
                return Err(Error::Internal {
                    table: self.table.table_name().to_string(),
                    message: "a retention delete reached a transaction without a deferred catalog payload"
                        .to_string(),
                });
            };
            payload.retention_delete_files = resident.delete_files().to_vec();
        }
        Ok(PreparedTxnCommit {
            table: self.table,
            new_snapshot_id: self.new_snapshot_id,
            validated_keys: self.validated_keys,
            stats: self.stats,
            row_count: self.row_count,
            superseded: self.superseded,
            publication: TxnPublication::Protected {
                publish,
                resident_retention,
            },
            retention_folded: self.retention_folded,
            _checkpoint_guard: checkpoint_guard,
        })
    }

    /// TEST-ONLY: the guarded prepare for ONE table outside a
    /// [`super::transaction::CayenneTransaction`], in the production order:
    /// `write_lock`, prerequisites (checkpoint lock, inline move), visibility
    /// lock, listing fence, one post-wait instant, the guarded prepare, the
    /// lease revoke. It takes `write_lock` itself (it does not bypass the
    /// writer) but performs NO optimistic check, and every guard is released on
    /// return, so the returned commit must not be applied: it is for asserting
    /// what a prepare produces. A real commit is
    /// [`super::transaction::CayenneTransaction::commit`] or [`Self::commit`].
    ///
    /// # Errors
    ///
    /// See [`Self::acquire_commit_prerequisites`] and
    /// [`Self::prepare_commit_under_guards`].
    #[cfg(test)]
    pub(crate) async fn prepare_commit_for_test(self) -> Result<PreparedTxnCommit> {
        let _write = self.table.write_lock_arc().lock_owned().await;
        let prerequisites = self.acquire_commit_prerequisites().await?;
        let _visibility = self.table.visibility_lock_arc().lock_owned().await;
        let fence =
            super::delete::HeldListingFence::new(self.table.lock_listing_fence_write_owned().await);
        let filters = self.commit_retention_context_at(chrono::Utc::now())?;
        let table = self.table.table_name().to_string();
        let prepared = self
            .prepare_commit_under_guards(prerequisites, &fence, filters)
            .await?;
        if !fence.revoke_leases_and_check_idle() {
            prepared.rollback().await;
            return Err(Error::Internal {
                table,
                message: "a retention scan was still running on the listing fence; nothing \
                          was committed"
                    .to_string(),
            });
        }
        Ok(prepared)
    }

    /// Whether this table's transactional write publishes a carried generation
    /// rather than a protected snapshot: a table without a primary key whose
    /// deletes are per file. Its protected snapshots are not reloaded on open
    /// (`load_protected_snapshots` loads none for that shape), so a committed
    /// transaction's rows published as one would be lost on reopen.
    fn publishes_carried_generation(&self) -> Result<bool> {
        Ok(self.table.key_resolver()?.is_none()
            && self.table.pk_deletion_strategy.is_position_based())
    }

    /// The carried-generation half of [`Self::prepare_commit_under_guards`]:
    /// carry every committed published run into the staged snapshot, with the
    /// run's deletes and, under `retention_sql`, the resident rows the
    /// publication's retention matches. The inline rows were materialized in
    /// the prerequisites. The shared commit flips `current_snapshot_id` to the
    /// staged snapshot, so the complete current generation holds the
    /// transaction's rows; no recovery or load path changes. An error publishes
    /// nothing and removes the staged snapshot.
    async fn prepare_generation_commit(
        self,
        checkpoint_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
        fence: &super::delete::HeldListingFence,
        filters: Option<Arc<Vec<datafusion::logical_expr::Expr>>>,
    ) -> Result<PreparedTxnCommit> {
        let prepare = self.table.prepare_transaction_generation(
            self.new_snapshot_id.clone(),
            self.row_count,
            Arc::clone(&self.stats),
            fence,
        );
        // The staged candidates and every carried resident row are evaluated
        // in this one context.
        let (generation, staged_matches) = match filters {
            Some(filters) => {
                super::overwrite_retention::with_publication_retention(filters, prepare).await?
            }
            None => prepare.await?,
        };
        // The accepted insert count includes the staged rows retention hides.
        let _ = staged_matches;
        let row_count = self.row_count;
        Ok(PreparedTxnCommit {
            table: self.table,
            new_snapshot_id: self.new_snapshot_id,
            validated_keys: self.validated_keys,
            stats: self.stats,
            superseded: self.superseded,
            publication: TxnPublication::Generation {
                generation: Some(generation),
                committed: false,
            },
            retention_folded: 0,
            row_count,
            _checkpoint_guard: checkpoint_guard,
        })
    }

    /// Publish the staged rows, making the upsert visible to readers.
    ///
    /// Acquires `write_lock` now (staging ran off-lock) and re-checks the
    /// transaction's per-key read footprint + write-set first, aborting with
    /// [`Error::WriteConflict`] (staged dir cleaned up) if any of those keys was
    /// committed after the transaction began (or, when per-key state is
    /// unavailable, if the table's high-water moved at all). It then takes the
    /// mem-tier checkpoint lock, the visibility lock and the listing fence, in
    /// that order, binds the `retention_sql` context at one instant after every
    /// wait, and prepares retention under all four; an owned commit task
    /// retains all four through atomic catalog publication and cache updates.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriteConflict`] on a lost OCC race (retryable), or an
    /// error if applying the deletions, reserving the sequence, or the durable
    /// snapshot-sequence write fails. Uncommitted staged files are removed on error.
    pub async fn commit(
        mut self,
        footprint: std::collections::HashSet<u128>,
        footprint_complete: bool,
    ) -> Result<u64> {
        // A table without a primary key publishes a carried generation
        // (`prepare_transaction_generation` + `commit_overwrite_in_txn`), which
        // only the fused `CayenneTransaction::commit` composes; this
        // single-table path publishes a protected snapshot, which a reopen of
        // such a table does not load, so it refuses.
        if self.publishes_carried_generation()? {
            cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
            return Err(Error::Unsupported {
                operation: "committing a staged write to a table without a primary key outside a \
                            CayenneTransaction",
            });
        }
        // Acquire the write lock now (staging ran off-lock), then re-check the
        // transaction's read footprint + write-set per-key before touching
        // anything visible: if any of those keys was committed after this
        // transaction began, it must abort and retry. Keys not in the footprint
        // are unaffected, so disjoint-key transactions commit concurrently.
        let write_guard = self.table.write_lock_arc().lock_owned().await;
        self.table.ensure_no_incomplete_write().await?;
        let current_high_water = self.table.sequence_high_water().await;
        if !self.token.staging_clean
            || self.table.transaction_has_conflict(
                self.token.stage_seq,
                &footprint,
                footprint_complete,
                &self.validated_keys,
                current_high_water,
            )
        {
            drop(write_guard);
            cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
            return Err(Error::WriteConflict {
                table: self.table.table_name().to_string(),
            });
        }

        // The same contract as a transaction commit (`CayenneTransaction::commit`),
        // for one table: prerequisites (checkpoint lock, inline move) before any
        // publication guard, then the visibility lock and listing fence, ONE
        // `now()` after every wait, the retention prepare under all four guards
        // (its scans lease the held fence), and the publication under the same
        // guards, which the publish task owns until it has published.
        let prerequisites = match self.acquire_commit_prerequisites().await {
            Ok(taken) => taken,
            Err(error) => {
                drop(write_guard);
                cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
                return Err(error);
            }
        };
        let CommitPrerequisites {
            checkpoint_guard,
            superseding,
        } = prerequisites;
        let visibility = self.table.visibility_lock_arc().lock_owned().await;
        let fence =
            super::delete::HeldListingFence::new(self.table.lock_listing_fence_write_owned().await);
        let resident_retention = match self.commit_retention_context_at(chrono::Utc::now()) {
            Ok(filters) => {
                self.prepare_retention_under_guards(superseding, &fence, filters)
                    .await
            }
            Err(error) => Err(error),
        };
        let resident_retention = match resident_retention {
            Ok(prepared) => prepared,
            Err(error) => {
                drop(fence);
                drop(visibility);
                drop(checkpoint_guard);
                drop(write_guard);
                cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
                return Err(error);
            }
        };
        let retention_folded = self.retention_folded;
        super::append_commit::PreparedResolvedAppend {
            context: Arc::clone(self.table.context()),
            table: self.table,
            _write_guard: write_guard,
            _held_write_lock: super::write_lock_held::held_guard(),
            _checkpoint_guard: checkpoint_guard,
            publication_guards: Some(super::append_commit::PublicationGuards { visibility, fence }),
            snapshot_id: self.new_snapshot_id,
            rows: self.row_count,
            stats: self.stats,
            deletions: self.on_conflict_deletions,
            validated_keys: self.validated_keys,
            superseded: self.superseded,
            into_empty_table: false,
            first_load_pk_bloom: None,
            resident_retention,
        }
        .commit()
        .await
        .map(|rows| rows.saturating_add(retention_folded))
    }

    /// Discard the staged upsert and remove its staged snapshot directory.
    ///
    /// The catalog was never touched, so this only cleans the orphan directory
    /// (best-effort; object-store orphans are pruned by the next successful
    /// snapshot cleanup cycle).
    ///
    /// # Errors
    ///
    /// Infallible today; returns `Result` for symmetry with the other staged
    /// lifecycles and to allow future durable-cleanup steps.
    pub async fn rollback(self) -> Result<()> {
        // Staging held no lock and the catalog was never touched, so this just
        // removes the orphan directory.
        cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
        Ok(())
    }
}

/// A staged upsert prepared for atomic commit inside a caller-owned
/// `MetastoreTransaction` (multi-table fusion). Sequences are reserved and
/// deletion-vector files written at [`CayenneStagedUpsert::prepare_commit_under_guards`]; the
/// durable catalog write happens in [`Self::apply_in_txn`] and the in-memory
/// visibility flip in [`Self::finish`], after the shared transaction commits.
pub struct PreparedTxnCommit {
    table: CayenneTableProvider,
    new_snapshot_id: String,
    validated_keys: PkDigestSet,
    stats: Arc<ColumnStatsAccumulator>,
    row_count: u64,
    superseded: usize,
    /// What the shared commit publishes for this table.
    publication: TxnPublication,
    /// See [`CayenneStagedUpsert::retention_folded`].
    retention_folded: u64,
    /// Held with `write_lock` from preparation through `finish`, as a durable
    /// write holds it, so a checkpoint cannot move rows the delete was decided on.
    _checkpoint_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

/// How a transaction publishes one table's staged snapshot.
enum TxnPublication {
    /// A keyed table, or one whose deletes are by key: the staged snapshot
    /// becomes a protected snapshot.
    Protected {
        /// The deferred on-conflict publish (trunk's `durable_payload` model):
        /// its `durable_payload` carries the delete-vector files,
        /// protected-snapshot sequence, inline tombstone, and deferred flips,
        /// applied in the shared transaction by [`PreparedTxnCommit::apply_in_txn`].
        /// Its `Drop` cleans up the staged deletion-vector files unless
        /// [`PreparedTxnCommit::mark_committed`] disarms it.
        publish: super::on_conflict::PreparedOnConflictDeletionPublish,
        /// The `retention_sql` delete over the resident rows this publication
        /// keeps; its files are in the publish's durable payload. Its `Drop`
        /// removes them unless the shared commit may reference them.
        resident_retention: Option<super::delete::PreparedDeletionPublish>,
    },
    /// A table without a primary key whose deletes are per file: the staged
    /// snapshot, carrying every committed run, becomes the current snapshot.
    /// `None` once published or rolled back, so each runs exactly once.
    Generation {
        generation: Option<super::overwrite::PreparedOverwrite>,
        /// The shared commit is (or may be) durable: rollback keeps the files.
        committed: bool,
    },
}

impl PreparedTxnCommit {
    /// The written table's id (participants are committed in canonical id order).
    #[must_use]
    pub fn table_id(&self) -> &str {
        self.table.table_id()
    }

    /// The written table's provider — the multi-table orchestrator holds its
    /// visibility + listing-fence locks and reads its catalog for the shared txn.
    pub(crate) fn provider(&self) -> &CayenneTableProvider {
        &self.table
    }

    /// Rows accepted on commit: the rows published plus the staged rows the
    /// commit's `retention_sql` folded out (an insert count, as before).
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count.saturating_add(self.retention_folded)
    }

    /// Append this table's deferred on-conflict payload (delete files + snapshot
    /// sequence + inline tombstone + deferred flips) to the caller-owned
    /// transaction, plus the durable write-back markers. No `begin`/`commit`.
    ///
    /// # Errors
    ///
    /// Returns the first statement failure; the caller rolls back the shared
    /// transaction and aborts the whole multi-table commit.
    pub async fn apply_in_txn(
        &mut self,
        catalog: &crate::CayenneCatalog,
        txn: &mut dyn crate::metastore::MetastoreTransaction,
    ) -> crate::catalog::CatalogResult<()> {
        let sequence = match &mut self.publication {
            TxnPublication::Protected { publish, .. } => {
                catalog
                    .apply_prepared_on_conflict_in_txn(txn, publish)
                    .await?;
                publish.snapshot_sequence
            }
            TxnPublication::Generation { generation, .. } => {
                let Some(generation) = generation.as_ref() else {
                    return Err(crate::catalog::CatalogError::InvalidOperationNoSource {
                        message: "a transaction generation was applied after it was released"
                            .to_string(),
                    });
                };
                generation.apply_in_txn(catalog, txn).await?;
                self.table.sequence_high_water().await
            }
        };
        // Durable federated write-back (#11838): on a durable-write-back table,
        // durably mark the written PKs (their `OwnedRow` encodings) in the SAME
        // commit transaction so the delivery worker reconciles them to the
        // source. A non-write-back table never marks.
        if self.table.is_durable_write_back() {
            let dirty_pk_bytes: Vec<Vec<u8>> = self
                .validated_keys
                .iter()
                .map(|row| row.as_ref().to_vec())
                .collect();
            if !dirty_pk_bytes.is_empty() {
                catalog
                    .mark_dirty_keys_in_txn(txn, self.table.table_id(), &dirty_pk_bytes, sequence)
                    .await?;
            }
        }
        Ok(())
    }

    /// Read back whether the shared commit published this table, after a
    /// COMMIT that reported a failure: a protected snapshot by its staged
    /// snapshot-sequence row, a carried generation by `current_snapshot_id`
    /// (its commit removes every snapshot-sequence row instead).
    pub(crate) async fn read_back_committed(
        &self,
        catalog: &crate::CayenneCatalog,
    ) -> crate::catalog::CatalogResult<bool> {
        match &self.publication {
            TxnPublication::Protected { publish, .. } => {
                Ok(super::append_commit::snapshot_sequence(
                    catalog,
                    self.table.table_id(),
                    &self.new_snapshot_id,
                )
                .await?
                    == Some(publish.snapshot_sequence))
            }
            TxnPublication::Generation { .. } => Ok(super::append_commit::current_snapshot_id(
                catalog,
                self.table.table_id(),
            )
            .await?
                == self.new_snapshot_id),
        }
    }

    /// Keep this table's staged files and refuse its writes until it is
    /// reloaded, after a shared commit whose outcome could not be read back.
    pub(crate) fn retain_after_unknown_outcome(&mut self) {
        match &mut self.publication {
            TxnPublication::Protected {
                publish,
                resident_retention,
            } => {
                publish.retain_files_for_wal_recovery();
                if let Some(resident) = resident_retention.as_mut() {
                    resident.mark_catalog_committed();
                }
            }
            TxnPublication::Generation { committed, .. } => *committed = true,
        }
        self.table
            .publication_outcome_unknown()
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Disarm the publish's destructive abort cleanup after the shared
    /// transaction has durably committed (mirrors the sync path's
    /// post-commit `mark_catalog_committed`).
    pub(crate) fn mark_committed(&mut self) {
        match &mut self.publication {
            TxnPublication::Protected {
                publish,
                resident_retention,
            } => {
                publish.mark_catalog_committed();
                if let Some(resident) = resident_retention.as_mut() {
                    resident.mark_catalog_committed();
                }
            }
            TxnPublication::Generation { committed, .. } => *committed = true,
        }
    }

    /// Flip in-memory visibility after the shared transaction committed. Must run
    /// under the table's held listing fence; best-effort per the same crash
    /// contract as [`super::overwrite::PreparedOverwrite::finish`] (a crash before
    /// finish reconstructs the same state from the catalog on reopen).
    ///
    /// # Errors
    ///
    /// Returns an error only if swapping the in-memory deletion caches fails.
    pub fn finish(mut self) -> Result<u64> {
        let (publish, resident_retention) = match std::mem::replace(
            &mut self.publication,
            TxnPublication::Generation {
                generation: None,
                committed: true,
            },
        ) {
            TxnPublication::Protected {
                publish,
                resident_retention,
            } => (publish, resident_retention),
            TxnPublication::Generation { generation, .. } => {
                return self.finish_generation(generation);
            }
        };
        let sequence = publish.snapshot_sequence;
        // Taken before the publish below, which makes this transaction's staged
        // rows visible while the `num_rows` delta describing them only reaches
        // the maintenance queue afterwards.
        let reserved_live_rows_delta = self.table.reserve_live_rows_delta();
        self.table.publish_prepared_on_conflict_deletions(publish);
        // The resident rows this publication's retention delete removes become
        // invisible in the same held-fence flip as the staged rows appear.
        if let Some(resident) = resident_retention {
            // Maintained aggregates are not fed the removed resident rows.
            self.table.mark_maintained_aggregates_stale();
            if let Err(error) = resident.publish() {
                // The catalog already holds the delete; reopening rebuilds it.
                tracing::error!(
                    table = %self.table.table_name(),
                    %error,
                    "failed to publish the committed retention delete to the in-memory caches"
                );
            }
        }
        // The fused transaction publish just made this transaction's staged rows
        // visible; mark maintained aggregates stale under the same held listing
        // fence so an IVM query cannot serve a Fresh aggregate that omits them.
        // The non-fused `commit_on_conflict_publish` path already does this; the
        // fused path (the only live transaction commit path) previously did not,
        // so aggregate queries served pre-transaction state as Fresh until an
        // unrelated CDC event re-fed the registry. `None` marks stale
        // (conservative base-scan fallback); feeding the staged batches as
        // retraction+insert deltas for incremental maintenance is a follow-up.
        self.table.feed_staged_ivm_under_fence(None);
        // The rows are visible now, so the claim survives from here.
        let published_live_rows_delta = reserved_live_rows_delta.published();
        let retention_requested = self.table.has_retention_delete_filters();
        let live_rows_delta = i64::try_from(self.row_count)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(self.superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(Arc::clone(&self.stats)),
            false,
            retention_requested,
            live_rows_delta,
            published_live_rows_delta,
        );
        if retention_requested {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table
                .record_file_pk_keys(&self.validated_keys, sequence);
        }
        Ok(self.row_count)
    }

    /// [`Self::finish`] for a carried generation. Lock ownership and order: the
    /// transaction holds this table's `write_lock`, then its visibility lock,
    /// then its `listing_fence` write guard (canonical participant order), and
    /// this flips visibility synchronously under them without re-acquiring
    /// any; the steps that await (statistics, cleanup scheduling, the manifest
    /// prune) start on their own task, which takes what it needs after the
    /// transaction releases them and runs once even if the caller is cancelled.
    fn finish_generation(
        self,
        generation: Option<super::overwrite::PreparedOverwrite>,
    ) -> Result<u64> {
        let Some(generation) = generation else {
            return Err(Error::Internal {
                table: self.table.table_name().to_string(),
                message: "a transaction generation was published twice".to_string(),
            });
        };
        generation.publish_under_held_fence()?;
        // The flip replaced the visible rows: maintained aggregates and the
        // cached keyset describe the previous generation.
        self.table.mark_maintained_aggregates_stale();
        self.table.feed_staged_ivm_under_fence(None);
        Ok(self.row_count)
    }

    /// Discard a prepared-but-uncommitted table (the shared transaction was not
    /// committed): the publish's `Drop` removes its staged deletion-vector files
    /// (still armed — `mark_committed` was never called); this also removes the
    /// unreferenced staged replacement-snapshot directory. A carried generation
    /// whose commit is or may be durable keeps its files.
    pub async fn rollback(mut self) {
        if let TxnPublication::Generation {
            generation,
            committed,
        } = &mut self.publication
        {
            if let Some(generation) = generation.take()
                && !*committed
                && let Err(error) = generation.rollback().await
            {
                tracing::warn!(
                    table = %self.table.table_name(),
                    %error,
                    "failed to remove an uncommitted transaction generation"
                );
            }
            return;
        }
        cleanup_orphan_snapshot_dir(&self.table, &self.new_snapshot_id).await;
    }
}

impl CayenneTableProvider {
    /// Capture the optimistic-concurrency token for a transaction write.
    ///
    /// Must be called at transaction **begin**, before the gate read — a commit
    /// landing between the gate read and staging would otherwise be invisible to
    /// the staged validation while making the gate verdict stale (a lost update).
    /// Holds `write_lock` only long enough to read the sequence high-water and
    /// the staging-append flag (the documented soundness condition on
    /// `sequence_high_water`).
    pub async fn transaction_write_token(&self) -> TransactionWriteToken {
        let _guard = self.write_lock_arc().lock_owned().await;
        let stage_seq = self.sequence_high_water().await;
        let staging_clean = !self.has_inflight_staging_appends();
        TransactionWriteToken {
            stage_seq,
            staging_clean,
        }
    }

    /// Stage a primary-key upsert **off-lock** for a transaction: validation +
    /// encode run without `write_lock`; the guard is acquired and `token`
    /// re-checked at [`CayenneStagedUpsert::commit`]. The caller must have
    /// captured `token` via [`Self::transaction_write_token`] before the
    /// transaction's gate read.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is partitioned (unsupported for the MVP) or
    /// if writing the staged data fails.
    pub async fn begin_staged_upsert_occ(
        &self,
        token: TransactionWriteToken,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<CayenneStagedUpsert> {
        // `retention_sql` is not evaluated while staging: the commit evaluates
        // the staged candidates in its own context (see `retention_deferred`).
        let retention_deferred = self.has_retention_delete_filters();
        let stage = self.stage_upsert_data(data, target_partitions);
        let staged = if retention_deferred {
            super::overwrite_retention::with_staged_retention_deferred(stage).await?
        } else {
            stage.await?
        };
        Ok(CayenneStagedUpsert::new(
            self.clone_for_write(),
            token,
            staged,
            retention_deferred,
        ))
    }

    /// Validate the incoming rows for PK conflicts (off-lock: private keyset, no
    /// shared-cache take/store) and encode them into a fresh, unreferenced
    /// snapshot directory.
    async fn stage_upsert_data(
        &self,
        data: SendableRecordBatchStream,
        target_partitions: usize,
    ) -> Result<StagedAppend> {
        // Partitioned tables publish across partitions; their visibility flip
        // cannot be a single protected-snapshot publish. Out of scope for the MVP.
        if self.metadata().partition_column.is_some() {
            return Err(Error::Unsupported {
                operation: "staged upsert for partitioned Cayenne tables",
            });
        }

        if let Some(resolver) = self.key_resolver()? {
            return self
                .stage_resolved_append(
                    data,
                    resolver,
                    super::overwrite::WriteShape {
                        target_size_bytes: self.target_file_size_bytes(),
                        target_partitions,
                        write_policy: WritePolicy::DELTA,
                    },
                    ValidationScope::Optimistic,
                )
                .await;
        }

        // A table without a primary key supersedes nothing; its staged rows are
        // all candidates, evaluated with the carried generation at commit.
        let prepared = self.prepare_stream_for_insert_offlock(data).await?;
        let post_validation = prepared.post_validation();

        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let target_size_bytes = self.target_file_size_bytes();
        // A keyed table that deletes by key cannot hide a staged row by
        // position: with `retention_sql` deferred to the commit, record per-file
        // statistics so the commit folds its matches out of the staged files,
        // as a resolved upsert does (`StagedAppend::fold`).
        let write = super::overwrite::WriteShape {
            target_size_bytes,
            target_partitions,
            write_policy: WritePolicy::DELTA,
        };
        let file_stats = if super::overwrite_retention::staged_retention_deferred()
            && self.primary_key_indices()?.is_some()
            && !self.hides_repeated_keys_with_position_deletes()
        {
            Some(Arc::new(super::overwrite::FileStatsObserver::new(
                self.table_name(),
                &self.table_schema(),
                None,
            )?))
        } else {
            None
        };

        let (row_count, _writer_ops, stats) = match self
            .write_to_snapshot_with_schema(
                prepared.stream,
                target_size_bytes,
                &new_snapshot_id,
                target_partitions,
                // Unknown size (the validation stream is consumed lazily); shard
                // across the full write concurrency, matching `begin_staged_append`.
                None,
                WritePolicy::DELTA,
                None,
                file_stats.as_ref().map(|observer| {
                    Arc::clone(observer) as Arc<dyn vortex_datafusion::VortexWriteObserver>
                }),
                self.table_schema(),
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                cleanup_orphan_snapshot_dir(self, &new_snapshot_id).await;
                return Err(e);
            }
        };

        // On-conflict deletions are computed by the validation stream as it is
        // consumed by `write_to_snapshot` above, so take them only now. A table
        // without a primary key (or with conflict detection disabled) yields the
        // default (empty) state — a plain insert published as a protected snapshot.
        let PostValidationState {
            on_conflict_deletions,
            validated_keys,
        } = post_validation.lock().take().unwrap_or_default();
        let superseded = on_conflict_deletions.total_superseded();

        Ok(StagedAppend {
            new_snapshot_id,
            on_conflict_deletions,
            validated_keys,
            stats,
            row_count,
            superseded,
            first_load_pk_bloom: None,
            retention_folded: 0,
            superseding_keys: None,
            fold: file_stats.map(|file_stats| super::append_stage::PendingFold {
                write,
                file_stats,
                superseded: std::collections::HashMap::new(),
            }),
        })
    }
}

/// Best-effort removal of an unreferenced staged snapshot directory.
///
/// The catalog never referenced the directory (no `cayenne_snapshot_sequence`
/// row, `current_snapshot_id` unchanged), so leaving it is safe; object stores
/// (S3) have no atomic "remove dir" and are left to the next successful
/// snapshot-cleanup cycle, mirroring [`super::overwrite::PreparedOverwrite::rollback`].
pub(super) async fn cleanup_orphan_snapshot_dir(table: &CayenneTableProvider, snapshot_id: &str) {
    if table.table_path().starts_with("s3://") {
        return;
    }
    let snapshot_dir = table.snapshot_dir_path_for(snapshot_id);
    match tokio::fs::remove_dir_all(&snapshot_dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(
                "Failed to clean up staged snapshot dir {} for table {}: {e}",
                snapshot_dir.display(),
                table.table_name()
            );
        }
    }
}
