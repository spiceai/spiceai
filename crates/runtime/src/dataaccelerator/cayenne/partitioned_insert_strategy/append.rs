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

// ============================================================================
// Append-mode coordinator (issue #10125 §6.3 + step 6).
//
// Append cannot use the overwrite path's MetastoreTransaction batching
// because append commits don't mutate the catalog at all — visibility is
// filesystem state + the in-memory ListingTable. The cross-partition
// guarantee is delivered by a *barrier*: every participating partition's
// listing fence is held for write while file moves + ListingTable swaps
// happen, so any reader going through `CayenneTableProvider::scan()`
// resolves either before or after the whole barrier, never in the middle.
//
// Crash safety: the top-level `PartitionedWal`
// written at `<table_root>/_partitioned_wal/<commit_id>.json` records every
// partition participating in this barrier. If the writer crashes, provider
// startup converges each participant from the catalog pointer and then this
// coordinator validates and removes the stale set anchor. Local filesystems
// use an atomic file; S3-compatible stores use object-store put/delete.
// ============================================================================

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne::{CayenneCatalog, PartitionedWal, PartitionedWalEntry, PreparedStagedAppend};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::sink::DataSink;
use futures::StreamExt;
use runtime_table_partition::Partition;
use runtime_table_partition::insert::partition_batch_composite;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use super::{
    CompositePartitionKey, PARTITION_WRITER_CHANNEL_DEPTH, WriteFanoutFailure, downcast_to_cayenne,
    get_or_create_partition_provider, join_writer_handles, poison_open_writer_channels,
    resolve_write_error,
};

/// `DataSink` that fans the input stream out by partition key, stages every
/// partition's append, and commits all of them under one shared
/// cross-partition barrier.
///
/// Streaming + backpressure work the same way as the overwrite sink (see
/// [`super::insert::CayennePartitionedOverwriteSink`]); only the commit-side
/// differs.
///
/// ## Coordination flow
///
/// Mirrors the overwrite flow up to the commit boundary. After every
/// partition's writer task returns a [`PreparedStagedAppend`]:
///
/// 1. Sort the receipts by `table_id` for deterministic fence-acquisition
///    order across concurrent coordinators.
/// 2. Acquire every partition's `listing_fence.write()` (held until the
///    barrier closes).
/// 3. Write a top-level [`cayenne::PartitionedWal`] anchor at
///    `<table_root>/_partitioned_wal/<commit_id>.json` before any file move.
/// 4. For each receipt, call `apply_under_held_barrier`: move staged files
///    into the snapshot directory, remove the per-partition WAL, swap the
///    in-memory `ListingTable`.
/// 5. Remove the top-level WAL.
/// 6. Release fences (drop guards together).
/// 7. Run `PreparedStagedAppend::finish` on each receipt.
pub(super) struct CayennePartitionedAppendSink {
    catalog: Arc<CayenneCatalog>,
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    table_root: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
enum AppendCommitState {
    AllCommitted,
    AllUncommitted,
    Mixed {
        committed: usize,
        uncommitted: usize,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum AppendCommitFailureDisposition<E> {
    RecoverCommitted,
    Rollback,
    RetainMixed {
        committed: usize,
        uncommitted: usize,
    },
    RetainUnknown(E),
}

fn append_commit_failure_disposition<E>(
    classification: Result<AppendCommitState, E>,
) -> AppendCommitFailureDisposition<E> {
    match classification {
        Ok(AppendCommitState::AllCommitted) => AppendCommitFailureDisposition::RecoverCommitted,
        Ok(AppendCommitState::AllUncommitted) => AppendCommitFailureDisposition::Rollback,
        Ok(AppendCommitState::Mixed {
            committed,
            uncommitted,
        }) => AppendCommitFailureDisposition::RetainMixed {
            committed,
            uncommitted,
        },
        Err(error) => AppendCommitFailureDisposition::RetainUnknown(error),
    }
}

trait AmbiguousCommitReceipt {
    type OnConflict;

    fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>);
    fn retain_for_wal_recovery(&mut self);
}

impl AmbiguousCommitReceipt for PreparedStagedAppend {
    type OnConflict = cayenne::provider::PreparedOnConflictDeletionPublish;

    fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>) {
        self.restore_prepared_on_conflict(prepared);
    }

    fn retain_for_wal_recovery(&mut self) {
        self.retain_files_for_wal_recovery();
    }
}

fn retain_ambiguous_commit_receipts<R: AmbiguousCommitReceipt>(
    receipts: &mut [R],
    prepared_on_conflicts: Vec<Option<R::OnConflict>>,
) {
    for (receipt, on_conflict) in receipts.iter_mut().zip(prepared_on_conflicts) {
        receipt.restore_on_conflict(on_conflict);
        receipt.retain_for_wal_recovery();
    }
}

fn classify_pointer_matches(pointer_matches: impl IntoIterator<Item = bool>) -> AppendCommitState {
    let mut committed = 0usize;
    let mut uncommitted = 0usize;
    for matches_target in pointer_matches {
        if matches_target {
            committed += 1;
        } else {
            uncommitted += 1;
        }
    }
    if uncommitted == 0 {
        AppendCommitState::AllCommitted
    } else if committed == 0 {
        AppendCommitState::AllUncommitted
    } else {
        AppendCommitState::Mixed {
            committed,
            uncommitted,
        }
    }
}

fn same_partitioned_wal_backend(
    first: Option<&cayenne::provider::PartitionedWalObjectStore>,
    participant: Option<&cayenne::provider::PartitionedWalObjectStore>,
) -> bool {
    match (first, participant) {
        (None, None) => true,
        (Some((_, first_prefix, first_backend)), Some((_, prefix, backend))) => {
            first_prefix == prefix && first_backend == backend
        }
        _ => false,
    }
}

impl std::fmt::Debug for CayennePartitionedAppendSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedAppendSink")
            .field("partition_exprs", &self.physical_exprs.len())
            .field("table_root", &self.table_root)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayennePartitionedAppendSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayennePartitionedAppendSink")
    }
}

#[async_trait]
impl DataSink for CayennePartitionedAppendSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let _coordinator_guard = Arc::clone(&self.coordinator_lock).lock_owned().await;

        let target_partitions = context.session_config().target_partitions();

        // Phase 1: fan input out to per-partition writer tasks that each call
        // begin_staged_append → prepare (writes the per-partition staging
        // WAL). Same streaming + backpressure shape as the overwrite sink.
        let mut senders: HashMap<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>> =
            HashMap::new();
        let mut handles: Vec<JoinHandle<cayenne::provider::Result<PreparedStagedAppend>>> =
            Vec::new();
        let mut fanout_failure: Option<WriteFanoutFailure> = None;

        'outer: while let Some(batch_result) = data.next().await {
            let batch = match batch_result {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => batch,
                Err(e) => {
                    fanout_failure = Some(WriteFanoutFailure::Upstream(e));
                    break 'outer;
                }
            };
            for (key, (values, batch_part)) in
                partition_batch_composite(&batch, &self.physical_exprs)?
            {
                let sender = if let Some(s) = senders.get(&key).cloned() {
                    s
                } else {
                    let (handle, tx) = self
                        .prepare_new_provider_for_partition(values, target_partitions)
                        .await?;

                    senders.insert(key.clone(), tx.clone());
                    handles.push(handle);
                    tx
                };

                if sender.send(Ok(batch_part)).await.is_err() {
                    // This partition's writer task already returned before
                    // the coordinator finished feeding it — its own
                    // `Err`/panic (surfaced when we join below) is the real
                    // cause, not a placeholder.
                    fanout_failure = Some(WriteFanoutFailure::WriterChannelClosed);
                    break 'outer;
                }
            }
        }

        // If the fan-out stopped early, propagate a poison pill down every
        // still-open channel so each writer task observes the failure and
        // returns Err (rather than committing a truncated append).
        if let Some(ref failure) = fanout_failure {
            poison_open_writer_channels(&senders, failure).await;
        }
        drop(senders);

        // Join all writer tasks.
        let (mut prepared, genuine_task_err, poisoned_task_err) =
            join_writer_handles(handles).await;

        if let Some(err) = resolve_write_error(fanout_failure, genuine_task_err, poisoned_task_err)
        {
            for prep in prepared {
                let table_id = prep.table_id().to_string();
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        table_id,
                        %rollback_err,
                        "Failed to roll back a partition's write after a write error elsewhere in the batch"
                    );
                }
            }
            return Err(err);
        }

        if prepared.is_empty() {
            return Ok(0);
        }

        // Phase 2: cross-partition barrier.
        //
        // (a) Sort prepared by table_id so all coordinators acquire fences in
        //     the same total order. (We already serialize coordinators via
        //     coordinator_lock, but sorting keeps the invariant defensible if
        //     the outer lock is ever relaxed.)
        // (b) Acquire every participating partition's listing fence for write.
        //     Held for the duration of the file-move + listing-swap loop.
        // (c) Write the top-level partitioned WAL before any file move.
        // (d) For each partition: move staged files into the snapshot dir,
        //     remove the per-partition WAL, swap the in-memory ListingTable.
        // (e) Remove the top-level WAL.
        // (f) Drop fence guards.
        prepared.sort_by(|a, b| a.table_id().cmp(b.table_id()));

        let mut fence_guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>> =
            Vec::with_capacity(prepared.len());
        for p in &prepared {
            fence_guards.push(p.lock_listing_fence_write_owned().await);
        }

        let commit_id = uuid::Uuid::now_v7().to_string();
        let table_root_str = self.table_root.to_string_lossy();
        let wal_entries: Vec<PartitionedWalEntry> = prepared
            .iter()
            .map(|p| PartitionedWalEntry {
                table_id: p.table_id().to_string(),
                target_snapshot_id: Some(p.target_snapshot_id().to_string()),
                staging_wal_path: Some(p.staging_wal_path().to_string_lossy().to_string()),
            })
            .collect();
        let top_level_wal =
            PartitionedWal::new(commit_id.clone(), table_root_str.to_string(), wal_entries);
        let object_store_wal = prepared[0]
            .partitioned_wal_object_store()
            .map_err(DataFusionError::from)?;
        for participant in prepared.iter().skip(1) {
            let participant_wal = participant
                .partitioned_wal_object_store()
                .map_err(DataFusionError::from)?;
            let same_backend =
                same_partitioned_wal_backend(object_store_wal.as_ref(), participant_wal.as_ref());
            if !same_backend {
                drop(fence_guards);
                for receipt in prepared {
                    let table_id = receipt.table_id().to_string();
                    if let Err(error) = receipt.rollback().await {
                        tracing::warn!(
                            table_id,
                            %error,
                            "Failed to roll back a partition's write after detecting that this \
                             table's partitions use different write-ahead log storage locations"
                        );
                    }
                }
                return Err(DataFusionError::Execution(
                    "Cannot write to this table because its partitions use different \
                     write-ahead log storage locations. Every partition of a table must share \
                     the same storage location for atomic multi-partition writes to work."
                        .to_string(),
                ));
            }
        }
        let wal_write_result = if let Some((store, prefix, _)) = &object_store_wal {
            top_level_wal
                .write_to_object_store(store.as_ref(), prefix)
                .await
                .map(|_| ())
        } else {
            top_level_wal.write_to(&self.table_root).await.map(|_| ())
        };
        if let Err(e) = wal_write_result {
            drop(fence_guards);
            for prep in prepared {
                let table_id = prep.table_id().to_string();
                if let Err(rollback_error) = prep.rollback().await {
                    tracing::warn!(
                        table_id,
                        commit_id,
                        %rollback_error,
                        "Failed to roll back a partition's write after failing to write this \
                         table's multi-partition write-ahead log"
                    );
                }
            }
            return Err(DataFusionError::from(e));
        }

        // Build every fallible in-memory publication object before moving a
        // staged file. Once the barrier move starts, rollback is no longer a
        // generally safe option; after the catalog commit publication itself
        // must be infallible and await-free.
        let publish_states = match prepared
            .iter()
            .map(PreparedStagedAppend::prepare_deferred_snapshot_publish)
            .collect::<cayenne::provider::Result<Vec<_>>>()
        {
            Ok(states) => states,
            Err(error) => {
                if let Err(cleanup_error) = if let Some((store, prefix, _)) = &object_store_wal {
                    PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id)
                        .await
                } else {
                    PartitionedWal::remove(&self.table_root, &commit_id).await
                } {
                    tracing::warn!(
                        commit_id,
                        %cleanup_error,
                        "Failed to remove this table's multi-partition write-ahead log after a \
                         write preparation failure"
                    );
                }
                drop(fence_guards);
                for receipt in prepared {
                    let table_id = receipt.table_id().to_string();
                    if let Err(rollback_error) = receipt.rollback().await {
                        tracing::warn!(
                            table_id,
                            %rollback_error,
                            "Failed to roll back a partition's write after a write preparation failure"
                        );
                    }
                }
                return Err(DataFusionError::from(error));
            }
        };

        // Apply the barrier on every partition. If any fails partway, the
        // top-level WAL stays on disk so the next process restart can
        // recover the set; we surface the error and stop. We do NOT attempt
        // automated mid-barrier rollback because already-applied partitions
        // have moved their files and removed their per-partition WALs —
        // reverting that without coordination is unsafe.
        for p in &prepared {
            if let Err(e) = p.apply_under_held_barrier().await {
                drop(fence_guards);
                return Err(DataFusionError::from(e));
            }
        }
        drop(fence_guards);
        for receipt in &mut prepared {
            if let Err(error) = receipt.prepare_deferred_manifest().await {
                return Err(DataFusionError::from(error));
            }
        }

        // Take the on-conflict payload out only after the manifest is prepared.
        // An on-conflict append (on_conflict: upsert) reserves its append
        // sequence inside `prepared_on_conflict` and leaves `append_sequence`
        // None, and `prepare_deferred_manifest` reads that sequence from
        // `prepared_on_conflict`; moving the payload out first leaves the
        // manifest with no reserved sequence. regression test for #12779
        let mut prepared_on_conflicts = prepared
            .iter_mut()
            .map(PreparedStagedAppend::take_prepared_on_conflict)
            .collect::<Vec<_>>();

        let mut fence_guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>> =
            Vec::with_capacity(prepared.len());
        for receipt in &prepared {
            fence_guards.push(receipt.lock_listing_fence_write_owned().await);
        }

        // Own the durable commit, every participant publication, and cleanup in
        // one detached-safe task. Dropping the request future while COMMIT is in
        // flight only drops this JoinHandle; the task retains the receipts,
        // deletion-file guards, and listing fences and runs through publication
        // before releasing them. This closes the ambiguous-COMMIT cancellation
        // window where a database connection could commit after its caller was
        // dropped while abort guards unlinked newly-live deletion vectors.
        let catalog = Arc::clone(&self.catalog);
        let table_root = self.table_root.clone();
        let completion = tokio::spawn(async move {
            let _coordinator_guard = _coordinator_guard;
            // The complete post-append contents now exist in one private snapshot
            // per partition. Flip every pointer in a single metastore transaction;
            // readers cannot observe a subset through a fresh directory listing.
            if let Err(error) = Self::commit_append_snapshots_in_one_txn(
                catalog.as_ref(),
                &prepared,
                &mut prepared_on_conflicts,
            )
            .await
            {
                let commit_state =
                    Self::classify_append_snapshot_pointers(catalog.as_ref(), &prepared).await;
                match append_commit_failure_disposition(commit_state) {
                    AppendCommitFailureDisposition::RecoverCommitted => {
                        // `COMMIT` can complete but report an ambiguous transport
                        // failure. Durable pointers are the decision: restore each
                        // payload, prove its generated DV paths are in committed
                        // metadata, then let per-partition WAL recovery reload and
                        // publish the complete durable state.
                        for (receipt, on_conflict) in prepared.iter_mut().zip(prepared_on_conflicts)
                        {
                            receipt.restore_prepared_on_conflict(on_conflict);
                            receipt
                                .reconcile_committed_on_conflict_cleanup()
                                .await
                                .map_err(DataFusionError::from)?;
                        }
                        drop(fence_guards);
                        for receipt in &prepared {
                            receipt
                                .recover_committed_snapshot()
                                .await
                                .map_err(DataFusionError::from)?;
                        }
                        return Err(error);
                    }
                    AppendCommitFailureDisposition::Rollback => {}
                    AppendCommitFailureDisposition::RetainMixed {
                        committed,
                        uncommitted,
                    } => {
                        retain_ambiguous_commit_receipts(&mut prepared, prepared_on_conflicts);
                        drop(fence_guards);
                        return Err(DataFusionError::Execution(format!(
                            "This write to a multi-partition table failed partway through: \
                             {committed} partition(s) committed and {uncommitted} did not. \
                             Spice will not automatically roll back; this table needs manual recovery."
                        )));
                    }
                    AppendCommitFailureDisposition::RetainUnknown(classification_error) => {
                        retain_ambiguous_commit_receipts(&mut prepared, prepared_on_conflicts);
                        drop(fence_guards);
                        return Err(DataFusionError::Execution(format!(
                            "This write to a multi-partition table failed, and Spice could not \
                             determine whether any partition committed ({classification_error}). \
                             Spice will not automatically roll back; this table needs manual \
                             recovery. Original error: {error}"
                        )));
                    }
                }
                drop(fence_guards);
                for (receipt, on_conflict) in prepared.iter_mut().zip(prepared_on_conflicts) {
                    receipt.restore_prepared_on_conflict(on_conflict);
                }
                let mut rollback_failed = false;
                for receipt in prepared {
                    let table_id = receipt.table_id().to_string();
                    if let Err(rollback_error) = receipt.rollback().await {
                        rollback_failed = true;
                        tracing::warn!(
                            table_id,
                            %rollback_error,
                            "Failed to roll back a partition's write after the multi-partition commit failed"
                        );
                    }
                }
                if !rollback_failed
                    && let Err(cleanup_error) = if let Some((store, prefix, _)) = &object_store_wal
                    {
                        PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id)
                            .await
                    } else {
                        PartitionedWal::remove(&table_root, &commit_id).await
                    }
                {
                    tracing::warn!(
                        commit_id,
                        %cleanup_error,
                        "Failed to remove this table's multi-partition write-ahead log after \
                         rolling back a failed write"
                    );
                }
                return Err(error);
            }

            // No await is permitted between these publications. The catalog commit
            // above is the durable global decision; cancellation must not leave a
            // proper subset of the participating providers on the new snapshots.
            for ((receipt, publish_state), prepared_on_conflict) in prepared
                .iter()
                .zip(publish_states)
                .zip(prepared_on_conflicts)
            {
                receipt.publish_deferred_snapshot_under_held_fence(publish_state);
                // Capture the on-conflict publish sequence before the value is
                // consumed below; an on-conflict append has no `append_sequence`,
                // so this is the sequence its validated keys must be stamped with.
                let on_conflict_sequence = prepared_on_conflict
                    .as_ref()
                    .map(cayenne::provider::PreparedOnConflictDeletionPublish::snapshot_sequence);
                if let Some(on_conflict) = prepared_on_conflict {
                    receipt.publish_on_conflict_under_held_fence(on_conflict);
                }
                receipt.publish_validated_file_keys(on_conflict_sequence);
            }

            // Publication is complete for every participant. Release all listing
            // fences before WAL deletion and other maintenance so readers are not
            // blocked by recoverable post-commit I/O.
            drop(fence_guards);

            // WAL cleanup is post-commit maintenance. Recovery consults the
            // catalog pointers and safely recognizes these as committed if cleanup
            // is interrupted.
            for receipt in &prepared {
                if let Err(error) = receipt.remove_committed_staging_wal().await {
                    tracing::warn!(
                        table_id = receipt.table_id(),
                        target_snapshot = receipt.target_snapshot_id(),
                        staging_wal_path = %receipt.staging_wal_path().display(),
                        %error,
                        "Failed to remove this partition's write-ahead log after its write was \
                         committed; the write itself succeeded and is not affected"
                    );
                }
            }

            let wal_remove_result = if let Some((store, prefix, _)) = &object_store_wal {
                PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id).await
            } else {
                PartitionedWal::remove(&table_root, &commit_id).await
            };
            if let Err(error) = wal_remove_result {
                tracing::warn!(
                    commit_id,
                    %error,
                    "Failed to remove this table's multi-partition write-ahead log after the \
                     write was committed; the write itself succeeded and is not affected"
                );
            }

            // Phase 3: per-partition finish (drops the per-partition write guard,
            // returns row count).
            let mut total_rows: u64 = 0;
            for prep in prepared {
                let table_id = prep.table_id().to_string();
                prep.finish_deferred_snapshot_maintenance().await;
                match prep.finish().await {
                    Ok(rows) => total_rows = total_rows.saturating_add(rows),
                    Err(error) => {
                        tracing::warn!(
                            table_id,
                            %error,
                            "Failed to update a partition's in-memory state after its write was \
                             committed; it will catch up automatically the next time this table \
                             is queried"
                        );
                    }
                }
            }

            Ok(total_rows)
        });

        match completion.await {
            Ok(result) => result,
            Err(error) => Err(DataFusionError::Execution(format!(
                "Cayenne cross-partition commit task failed: {error}"
            ))),
        }
    }
}

impl CayennePartitionedAppendSink {
    pub(super) fn new(
        catalog: Arc<CayenneCatalog>,
        coordinator_lock: Arc<tokio::sync::Mutex<()>>,
        creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
        partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
        schema: SchemaRef,
        physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
        table_root: PathBuf,
    ) -> Self {
        Self {
            catalog,
            coordinator_lock,
            creator,
            partitions,
            schema,
            physical_exprs,
            table_root,
        }
    }

    async fn classify_append_snapshot_pointers(
        catalog: &CayenneCatalog,
        prepared: &[PreparedStagedAppend],
    ) -> datafusion::common::Result<AppendCommitState> {
        let mut pointer_matches = Vec::with_capacity(prepared.len());
        for receipt in prepared {
            let current = catalog
                .current_snapshot_id_for_table(receipt.table_id())
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            pointer_matches.push(current == receipt.target_snapshot_id());
        }
        Ok(classify_pointer_matches(pointer_matches))
    }

    async fn commit_append_snapshots_in_one_txn(
        catalog: &CayenneCatalog,
        prepared: &[PreparedStagedAppend],
        prepared_on_conflicts: &mut [Option<
            cayenne::provider::PreparedOnConflictDeletionPublish,
        >],
    ) -> datafusion::common::Result<()> {
        let max_attempts = turso_shared::DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        let snapshots: Vec<(&str, &str)> = prepared
            .iter()
            .map(|receipt| (receipt.table_id(), receipt.target_snapshot_id()))
            .collect();

        'attempts: for attempt in 1..=max_attempts {
            let mut txn = catalog
                .begin_transaction()
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if let Err(error) = catalog
                .set_current_snapshots_in_txn(&mut *txn, &snapshots)
                .await
            {
                // Roll back explicitly (not via the transaction's best-effort,
                // possibly-detached Drop) so the metastore writer lock is released
                // deterministically before this attempt backs off and retries.
                if let Err(rollback_error) = txn.rollback().await {
                    tracing::warn!(
                        attempt,
                        max_attempts,
                        %rollback_error,
                        "Failed to roll back the multi-partition commit before retrying"
                    );
                }
                if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                    tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                    continue;
                }
                return Err(DataFusionError::External(Box::new(error)));
            }
            for on_conflict in prepared_on_conflicts.iter_mut().flatten() {
                if let Err(error) = catalog
                    .apply_prepared_on_conflict_in_txn(&mut *txn, on_conflict)
                    .await
                {
                    // Roll back explicitly (not via the transaction's best-effort,
                    // possibly-detached Drop) so the metastore writer lock is released
                    // deterministically before this attempt backs off and retries.
                    if let Err(rollback_error) = txn.rollback().await {
                        tracing::warn!(
                            attempt,
                            max_attempts,
                            %rollback_error,
                            "Failed to roll back the multi-partition commit before retrying"
                        );
                    }
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                        tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                        continue 'attempts;
                    }
                    return Err(DataFusionError::External(Box::new(error)));
                }
            }
            for receipt in prepared {
                if let Some(manifest) = receipt.deferred_manifest()
                    && let Err(error) = catalog
                        .replace_snapshot_files_in_txn(
                            &mut *txn,
                            receipt.table_id(),
                            receipt.target_snapshot_id(),
                            manifest,
                        )
                        .await
                {
                    // Roll back explicitly (not via the transaction's best-effort,
                    // possibly-detached Drop) so the metastore writer lock is released
                    // deterministically before this attempt backs off and retries.
                    if let Err(rollback_error) = txn.rollback().await {
                        tracing::warn!(
                            attempt,
                            max_attempts,
                            %rollback_error,
                            "Failed to roll back the multi-partition commit before retrying"
                        );
                    }
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                        tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                        continue 'attempts;
                    }
                    return Err(DataFusionError::External(Box::new(error)));
                }
            }
            match txn.commit().await {
                Ok(()) => {
                    for on_conflict in prepared_on_conflicts.iter_mut().flatten() {
                        on_conflict.mark_catalog_committed();
                    }
                    return Ok(());
                }
                Err(error)
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) =>
                {
                    tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                }
                Err(error) => return Err(DataFusionError::External(Box::new(error))),
            }
        }

        Err(DataFusionError::Execution(format!(
            "Failed to commit this write across all its partitions after {max_attempts} attempts \
             due to repeated conflicting writes; retry the write"
        )))
    }

    async fn prepare_new_provider_for_partition(
        &self,
        partition_values: Vec<ScalarValue>,
        target_partitions: usize,
    ) -> Result<
        (
            JoinHandle<cayenne::provider::Result<PreparedStagedAppend>>,
            mpsc::Sender<datafusion::common::Result<RecordBatch>>,
        ),
        DataFusionError,
    > {
        let provider =
            get_or_create_partition_provider(&self.creator, &self.partitions, partition_values)
                .await?;
        let cayenne = downcast_to_cayenne(&provider).ok_or_else(|| {
            DataFusionError::Execution(
                "CayennePartitionedInsertStrategy expects every partition's table \
             provider to be a CayenneTableProvider"
                    .to_string(),
            )
        })?;
        if !cayenne.supports_deferred_partition_append() {
            return Err(DataFusionError::NotImplemented(
                "This Cayenne partition does not support atomic deferred append".to_string(),
            ));
        }
        let cayenne_owned = cayenne.clone_for_write_operations();
        let (tx, rx) = mpsc::channel::<datafusion::common::Result<RecordBatch>>(
            PARTITION_WRITER_CHANNEL_DEPTH,
        );
        let schema_clone = Arc::clone(&self.schema);
        let handle: JoinHandle<cayenne::provider::Result<PreparedStagedAppend>> =
            tokio::spawn(async move {
                let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    schema_clone,
                    ReceiverStream::new(rx),
                ));
                let staged = cayenne_owned
                    .begin_deferred_snapshot_append(stream, target_partitions)
                    .await?;
                Ok(staged)
            });
        Ok((handle, tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectStorePath;

    fn memory_store() -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        store
    }

    #[derive(Debug, Default)]
    struct FakeAmbiguousReceipt {
        restored: Option<u8>,
        retained: bool,
    }

    impl AmbiguousCommitReceipt for FakeAmbiguousReceipt {
        type OnConflict = u8;

        fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>) {
            self.restored = prepared;
        }

        fn retain_for_wal_recovery(&mut self) {
            self.retained = true;
        }
    }

    fn object_store_wal(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        backend: &str,
    ) -> cayenne::provider::PartitionedWalObjectStore {
        (store, ObjectStorePath::from(prefix), backend.to_string())
    }

    #[test]
    fn classifies_every_durable_pointer_outcome() {
        assert_eq!(
            classify_pointer_matches([true, true]),
            AppendCommitState::AllCommitted
        );
        assert_eq!(
            classify_pointer_matches([false, false]),
            AppendCommitState::AllUncommitted
        );
        assert_eq!(
            classify_pointer_matches([true, false, true]),
            AppendCommitState::Mixed {
                committed: 2,
                uncommitted: 1,
            }
        );
    }

    #[test]
    fn maps_mixed_and_unknown_outcomes_to_non_destructive_retention() {
        assert_eq!(
            append_commit_failure_disposition::<&str>(Ok(AppendCommitState::Mixed {
                committed: 1,
                uncommitted: 2,
            })),
            AppendCommitFailureDisposition::RetainMixed {
                committed: 1,
                uncommitted: 2,
            }
        );
        assert_eq!(
            append_commit_failure_disposition::<&str>(Err("catalog unavailable")),
            AppendCommitFailureDisposition::RetainUnknown("catalog unavailable")
        );
    }

    #[test]
    fn ambiguous_outcomes_restore_payloads_before_retaining_files() {
        let mut receipts = vec![
            FakeAmbiguousReceipt::default(),
            FakeAmbiguousReceipt::default(),
        ];
        retain_ambiguous_commit_receipts(&mut receipts, vec![Some(7), None]);

        assert_eq!(receipts[0].restored, Some(7));
        assert!(receipts[0].retained);
        assert_eq!(receipts[1].restored, None);
        assert!(receipts[1].retained);
    }

    #[test]
    fn accepts_distinct_handles_for_the_same_wal_backend() {
        let first = memory_store();
        let second = memory_store();
        assert!(!Arc::ptr_eq(&first, &second));

        assert!(same_partitioned_wal_backend(
            Some(&object_store_wal(first, "shared/table", "s3://bucket")),
            Some(&object_store_wal(second, "shared/table", "s3://bucket")),
        ));
    }

    #[test]
    fn rejects_same_prefix_on_heterogeneous_wal_backends() {
        let first = memory_store();
        let second = memory_store();

        assert!(!same_partitioned_wal_backend(
            Some(&object_store_wal(first, "shared/table", "s3://bucket-a")),
            Some(&object_store_wal(second, "shared/table", "s3://bucket-b")),
        ));
    }

    #[test]
    fn rejects_different_prefixes_on_the_same_wal_backend() {
        let first = memory_store();
        let second = memory_store();

        assert!(!same_partitioned_wal_backend(
            Some(&object_store_wal(first, "table-a", "s3://bucket")),
            Some(&object_store_wal(second, "table-b", "s3://bucket")),
        ));
    }
}
