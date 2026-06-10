//! CDC write path, sequence allocation, and durable inlined-commit types.
//!
//! Entry point: [`CayenneTableProvider::write_cdc_append_stream`] takes
//! `write_lock` and runs the pipelined Stage-A write; the returned
//! [`CayenneCdcWrite::finish`] is Stage B, which publishes the staged file move
//! and the deletion/protected-snapshot caches under `visibility_lock` then
//! `listing_fence.write()` (one atomic visibility boundary for scans).
//! Also home to the in-memory sequence allocator ([`SeqAllocator`], lever B2):
//! every sequence this table hands out flows through [`reserve_sequences_in`].
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use crate::catalog::MetadataCatalog;
use crate::provider::mutation_writer::AppendMutationWriter;

use super::{
    Arc, AtomicU64, CatalogError, CatalogResult, CayenneTableProvider, ColumnStatsAccumulator,
    Duration, HashMap, HashSet, Instant, Ordering, OwnedRow, PreparedStagedAppend,
    RecordBatchStreamAdapter, Result, SendableRecordBatchStream, StreamExt, TryStreamExt,
    record_cayenne_write_phase,
};

/// Outcome of a durable inlined-data commit that has not yet been published to the in-memory caches.
///
/// Returned by [`CayenneTableProvider::commit_inlined_data_durable`] and
/// consumed by [`CayenneTableProvider::publish_inlined_mutation`] under
/// `scan_state_lock.write()`.
pub(super) struct InlinedDurableCommit {
    /// Number of rows removed by the rewrite (superseded inlined copies).
    pub(super) removed_rows: i64,
    /// Sequence assigned to newly appended inlined rows, or `None` when the
    /// commit only rewrote/removed existing entries. When `Some`, publishing
    /// advances `published_inlined_seq` to this value to make the appended rows
    /// visible.
    pub(super) published_seq: Option<i64>,
}

/// Result of a Cayenne CDC append write.
///
/// A write can be fully complete when this value is returned, or it can have a
/// staged append whose WAL is durable but whose file publish still needs to be
/// finalized. CDC catch-up mode can safely commit the source offset once this
/// value is returned; callers must still drive [`Self::finish`] to make the
/// rows visible and release the table write guard.
#[must_use]
pub struct CayenneCdcWrite {
    pub(super) table: CayenneTableProvider,
    pub(super) rows: u64,
    pub(super) prepared_append: Option<PreparedStagedAppend>,
    pub(super) prepared_on_conflict: Option<PreparedOnConflictDeletionPublish>,
    pub(super) stats: Option<Arc<ColumnStatsAccumulator>>,
    pub(super) validated_file_keys: HashSet<OwnedRow>,
    /// Set when this write appended to the in-memory CDC tier
    /// (`cdc_durability: memory`): the mem-tier epoch the batch landed in. The
    /// runtime reads this (via [`Self::in_memory_epoch`]) to DEFER the source
    /// slot ack — instead of advancing the slot per-batch, it queues the
    /// batch's committer tagged with this epoch and runs it only after a
    /// checkpoint reports the epoch durable (the slot-deferral correctness
    /// seam). `None` for every durable-path write (file mode, fallback, the
    /// non-pipelined path), which keep the normal per-batch committer.
    pub(super) in_memory_epoch: Option<u64>,
}

pub(crate) struct PreparedOnConflictDeletionPublish {
    pub(super) target_snapshot_id: String,
    pub(super) snapshot_sequence: i64,
    pub(super) delete_sequence: Option<i64>,
    pub(super) insert_sequence: Option<i64>,
    pub(super) deleted_pk_i64: Vec<i64>,
    pub(super) deleted_row_keys: Vec<Box<[u8]>>,
    /// PKs whose prior copy was INLINE (the ones the inline tombstone hides),
    /// kept separate from the file-deletion `deleted_pk_i64`/`deleted_row_keys`
    /// above (cycle-5 TASK 1). At finalize these — at `delete_sequence` — are the
    /// removal applied to the inline-cache base via `pending_tombstone_deltas`, so
    /// they MUST be the inline keys (the tombstone's keys), NOT the file keys: a
    /// file-conflict deletion never matches a cached inline row, so using the file
    /// keys would fail to hide the old inline copy (a transient duplicate). Empty
    /// when the batch replaced no inline rows (then `inlined_delete_id` is `None`
    /// and no removal is enqueued). One of the two is always empty per PK strategy.
    pub(super) deleted_inlined_pk_i64: Vec<i64>,
    pub(super) deleted_inlined_row_keys: Vec<Box<[u8]>>,
    pub(super) position_deletions: HashMap<String, Vec<u32>>,
    /// `inlined_id` of the inline tombstone this staged upsert wrote with
    /// `published = false` (Option D), or `None` when the batch replaced no
    /// inlined rows. At finalize (`publish_prepared_on_conflict_deletions`, under
    /// the listing fence, after the replacement files are moved into the snapshot)
    /// the tombstone is activated IN MEMORY (recorded in
    /// `inlined_locally_published` so the read filter applies it immediately) and
    /// its durable `published = 1` flip is DEFERRED into
    /// `pending_durable_tombstone_flips` — the cycle-4 b1★ Stage-B-writer-free
    /// path. `pending_inline_tombstones` is decremented there. Carrying the exact
    /// id (rather than re-deriving from keys) makes both the in-memory activation
    /// and the later durable flip target precisely THIS tombstone — never a
    /// later-staged tombstone for the same PK.
    pub(super) inlined_delete_id: Option<String>,
    /// Count of existing rows superseded by this upsert, captured from
    /// [`OnConflictDeletions::total_superseded`] at validation time. This is the
    /// authoritative live-row-delta input: it must NOT be recomputed from the
    /// fields above, because `deleted_pk_i64` and `deleted_row_keys` carry the
    /// SAME `Int64Pk` deletions in two encodings (i64 + committed byte keys), so
    /// summing their lengths double-counts, and neither captures `position_deletions`.
    pub(super) superseded: usize,
}

impl CayenneCdcWrite {
    pub(crate) fn completed(table: CayenneTableProvider, rows: u64) -> Self {
        Self {
            table,
            rows,
            prepared_append: None,
            prepared_on_conflict: None,
            stats: None,
            validated_file_keys: HashSet::new(),
            in_memory_epoch: None,
        }
    }

    /// A write that appended to the in-memory CDC tier at `epoch`
    /// (`cdc_durability: memory`). Nothing is staged or pending — the visibility
    /// swap already happened synchronously under the listing fence in the write
    /// path — so `finish()` is a no-op and `has_pending_finalize()` is `false`.
    /// `epoch` is carried so the runtime can DEFER the source slot ack until a
    /// checkpoint reports it durable (it is NOT used to advance the slot here).
    pub(crate) fn in_memory_staged(table: CayenneTableProvider, rows: u64, epoch: u64) -> Self {
        Self {
            table,
            rows,
            prepared_append: None,
            prepared_on_conflict: None,
            stats: None,
            validated_file_keys: HashSet::new(),
            in_memory_epoch: Some(epoch),
        }
    }

    pub(crate) fn prepared_append(
        table: CayenneTableProvider,
        rows: u64,
        prepared_append: PreparedStagedAppend,
        stats: Arc<ColumnStatsAccumulator>,
        validated_file_keys: HashSet<OwnedRow>,
    ) -> Self {
        Self {
            table,
            rows,
            prepared_append: Some(prepared_append),
            prepared_on_conflict: None,
            stats: Some(stats),
            validated_file_keys,
            in_memory_epoch: None,
        }
    }

    pub(crate) fn prepared_upsert_append(
        table: CayenneTableProvider,
        rows: u64,
        prepared_append: PreparedStagedAppend,
        prepared_on_conflict: PreparedOnConflictDeletionPublish,
        stats: Arc<ColumnStatsAccumulator>,
        validated_file_keys: HashSet<OwnedRow>,
    ) -> Self {
        Self {
            table,
            rows,
            prepared_append: Some(prepared_append),
            prepared_on_conflict: Some(prepared_on_conflict),
            stats: Some(stats),
            validated_file_keys,
            in_memory_epoch: None,
        }
    }

    /// Returns the number of rows written or staged by this CDC write.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The in-memory CDC tier epoch this write landed in, when the table is in
    /// `cdc_durability: memory` mode and the batch took the RAM-append path.
    /// `None` for every durable-path write. The runtime uses this to defer the
    /// source slot ack until the epoch is checkpointed durably.
    #[must_use]
    pub fn in_memory_epoch(&self) -> Option<u64> {
        self.in_memory_epoch
    }

    /// Returns true when the staged append still needs to be made visible.
    ///
    /// Always `false` for an in-memory-staged write (`in_memory_epoch.is_some()`):
    /// the RAM visibility swap already happened synchronously under the listing
    /// fence in `write_cdc_pipelined`, so there is no Stage-B publish to run.
    #[must_use]
    pub fn has_pending_finalize(&self) -> bool {
        self.prepared_append.is_some()
    }

    /// Finalize the staged append, if any, and schedule post-write maintenance.
    ///
    /// # Errors
    ///
    /// Returns an error if the staged append cannot be published.
    pub async fn finish(self) -> Result<u64> {
        if let Some(prepared_append) = self.prepared_append {
            let publish_start = Instant::now();
            let superseded_rows = if let Some(prepared_on_conflict) = self.prepared_on_conflict {
                // Authoritative superseded-row count, captured at validation time.
                // Do NOT recompute from `deleted_pk_i64`/`deleted_row_keys`: for an
                // `Int64Pk` table those hold the SAME deletions in two encodings, so
                // summing their lengths double-counts, and neither sees
                // `position_deletions`. See `PreparedOnConflictDeletionPublish::superseded`.
                let superseded = prepared_on_conflict.superseded;

                // Publish the staged file move AND the deletion / protected-snapshot
                // caches under a SINGLE listing-fence write. A concurrent `scan()`
                // captures its deletion snapshot and `protected_snapshots` under
                // `listing_fence.read()`, so holding the write fence across both
                // makes the upsert atomic to readers: a scan observes either the
                // pre-publish state (old rows, no new snapshot) or the full
                // post-publish state (new snapshot + its deletes), never the new
                // snapshot's rows without the deletes that hide the old versions.
                // (visibility lock then fence: same order as `apply_under_barrier`.)
                //
                // cycle-5 TASK 3: decompose the Stage-B `publish` phase so bench #5
                // attributes the ~312 ms residual. `publish_lock_wait` is the time
                // blocked acquiring the visibility lock + listing fence — it blocks
                // on a concurrent `scan()` holding the read fence and on the prior
                // finalize, so a large value here means scan/finalize contention,
                // not work this batch does.
                let lock_wait_start = Instant::now();
                let _visibility = self.table.visibility_lock_arc().lock_owned().await;
                let _fence = self.table.lock_listing_fence_write_owned().await;
                record_cayenne_write_phase(
                    self.table.table_name(),
                    "publish_lock_wait",
                    lock_wait_start,
                );

                // b1★ (cycle-4): Stage-B is now WRITER-FREE. It performs NO durable
                // metastore write — the snapshot sequence was written in Stage A's
                // folded transaction, the inline tombstone's DURABLE `published = 1`
                // flip is DEFERRED (folded into the next batch's Stage-A txn, or the
                // idle-table maintenance drain), and the protected-snapshot RCU +
                // structural-epoch bump + in-memory tombstone activation below are
                // all in-memory. This eliminates the pre-b1★ `publish_tombstone_flip`
                // autocommit `UPDATE … SET published = 1` (88 ms under the fence)
                // whose single-statement WAL-writer grab ALTERNATED with the next
                // batch's Stage-A `BEGIN IMMEDIATE` (the 463 ms `stage_tombstone_prepare`
                // lock-wait). Stage-B no longer competes for stock's WAL writer.
                //
                // Crash-window analysis (the part a reviewer scrutinizes):
                //  * The tombstone is durable `published = 0` from Stage A; the
                //    replacement files' staging WAL is durable BEFORE it. Nothing
                //    here changes that.
                //  * `apply_under_held_barrier` (below) makes the replacement files
                //    durable AND removes the staging WAL. If it fails, the tombstone
                //    is STILL durable `published = 0`, the in-memory activation has
                //    NOT run, and the staging WAL is still present — so there is NO
                //    durable state to compensate (unlike the pre-b1★ flip-before-move
                //    ordering, which had to revert a premature `published = 1`). We
                //    simply propagate the error; reopen re-drives the move via
                //    `ensure_no_incomplete_write` then activates the orphan tombstone
                //    via `publish_orphan_inlined_deletes`. Applied exactly once.
                //  * A crash AFTER the move (WAL gone, files durable) but BEFORE the
                //    deferred durable flip lands: the tombstone is durable
                //    `published = 0` with a durable replacement and NO WAL, so reopen's
                //    unconditional orphan sweep flips it `published = 1`. Old inline
                //    copy hidden, replacement visible, applied once. The in-memory
                //    `inlined_locally_published` entry is lost on crash, which is
                //    correct: it only ever advanced visibility PAST the durable flag,
                //    so losing it cannot resurface a row (the orphan sweep republishes).
                //  This is the SAME recovery mechanism that already makes "crash
                //  before finish()" safe (correctness_audit.md §5 case 1); deferring
                //  the flip merely widens that already-healed window by one batch.
                // `publish_apply_move`: the staged-file MOVE into the snapshot dir
                // (object-store / fs rename + parent-dir fsync) + staging-WAL
                // removal + the list-files-cache delta-apply, all under the held
                // fence. This is the real I/O of the publish — on EBS the dir
                // fsync is a device flush.
                let apply_move_start = Instant::now();
                prepared_append.apply_under_held_barrier().await?;
                record_cayenne_write_phase(
                    self.table.table_name(),
                    "publish_apply_move",
                    apply_move_start,
                );
                // `publish_deletion_apply`: the in-memory deletion-cache merge
                // (`extend_max_conflicts` builds a new deletion index), the
                // protected-snapshot RCU, the tombstone-delta enqueue (cycle-5
                // TASK 1) + generation bump, and the `pending_inline_tombstones`
                // decrement. O(conflicts) CPU, NO metastore round-trip (Stage-B is
                // writer-free). Held under the fence for atomicity with the move.
                let deletion_apply_start = Instant::now();
                self.table
                    .publish_prepared_on_conflict_deletions(prepared_on_conflict)?;
                record_cayenne_write_phase(
                    self.table.table_name(),
                    "publish_deletion_apply",
                    deletion_apply_start,
                );
                superseded
            } else {
                prepared_append.apply_under_barrier().await?;
                0
            };
            let rows = prepared_append.finish().await?;
            record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);
            let retention_requested = self.table.has_retention_delete_filters();
            if retention_requested {
                // Match the non-pipelined path: retention's delete outcome is
                // not yet known, so clear conservatively. See the comment in
                // `AppendMutationWriter::write_prepared_stream`.
                self.table.clear_cached_pk_keyset();
            } else {
                self.table.record_file_pk_keys(&self.validated_file_keys);
            }
            // Live `num_rows` delta is inserted rows minus upsert replacements.
            // The staged path has no standalone deletes, so this remains a pure
            // append delta when `superseded_rows` is 0.
            let live_rows_delta = i64::try_from(rows)
                .unwrap_or(i64::MAX)
                .saturating_sub(i64::try_from(superseded_rows).unwrap_or(i64::MAX));
            self.table.schedule_post_write_maintenance(
                self.stats,
                false,
                retention_requested,
                live_rows_delta,
            );
            Ok(rows)
        } else {
            Ok(self.rows)
        }
    }
}

/// Block size for the in-memory sequence allocator (lever B2). Each metastore
/// `UPDATE … += BLOCK … RETURNING` refill durably reserves this many sequence
/// numbers in one writer acquisition; they are then served from memory until
/// exhausted, amortizing the writer cost to ~1/BLOCK reservations. A crash
/// wastes at most `BLOCK - 1` sequences (the unused tail of the current block)
/// but can NEVER reissue one (the DB high-water is always >= every handed-out
/// value — see `SeqAllocator` and `reserve_sequences_local`). There is no
/// correctness ceiling on this value; it only trades waste-on-crash against
/// refill frequency.
pub(crate) const SEQ_RESERVE_BLOCK: i64 = 1024;

/// In-memory sequence allocator (lever B2). Hands out monotonic per-table
/// sequence numbers WITHOUT acquiring the metastore writer on the hot path.
///
/// `next` is the lowest UNUSED sequence; `persisted_hi` is the highest value
/// durably reserved in `cayenne_table.current_sequence_number`.
///
/// INVARIANT (held whenever the allocator lock is not inside a refill):
/// `next - 1 <= persisted_hi` — every value already handed out is `<=
/// persisted_hi`, i.e. the DB row is always at-or-ahead of what we have issued.
/// The refill durably bumps `persisted_hi` (an fsynced `UPDATE … RETURNING`)
/// BEFORE advancing `next` past the newly reserved range, so a crash/restart
/// reseeds from a DB high-water that is `>=` every value ever handed out and
/// therefore never reissues one. See `reserve_sequences_local`.
pub(crate) struct SeqAllocator {
    /// Next sequence number to hand out.
    pub(super) next: i64,
    /// Highest value durably written to `cayenne_table.current_sequence_number`.
    pub(super) persisted_hi: i64,
}

/// Reserve `count` (>= 1) consecutive sequence numbers from a shared in-memory
/// allocator (lever B2), refilling from the metastore only when the in-memory
/// block is exhausted. Returns the FIRST of the contiguous reserved block (the
/// half-open range `first ..= first + count - 1`). This is the single
/// implementation shared by [`CayenneTableProvider::reserve_sequences_local`]
/// and the DML delete sink, so every sequence handout for a table goes through
/// one monotone source.
///
/// See [`CayenneTableProvider::reserve_sequences_local`] for the
/// monotonicity-on-reopen correctness argument.
pub(crate) async fn reserve_sequences_in(
    allocator: &tokio::sync::Mutex<SeqAllocator>,
    catalog: &Arc<dyn MetadataCatalog>,
    table_id: &str,
    table_name: &str,
    count: u32,
) -> CatalogResult<i64> {
    let count = i64::from(count.max(1));
    let mut allocator = allocator.lock().await;

    if allocator.next + count - 1 > allocator.persisted_hi {
        let bump = std::cmp::max(SEQ_RESERVE_BLOCK, count);
        let bump_u32 = u32::try_from(bump).unwrap_or(u32::MAX);
        let block_first = catalog.reserve_sequence_numbers(table_id, bump_u32).await?;
        let reserved = i64::from(bump_u32);
        let new_hi = block_first.checked_add(reserved - 1).ok_or_else(|| {
            CatalogError::InvalidOperationNoSource {
                message: format!(
                    "sequence-number high-water overflow reserving {reserved} for table {table_name}"
                ),
            }
        })?;
        allocator.next = std::cmp::max(allocator.next, block_first);
        allocator.persisted_hi = new_hi;
    }

    let first = allocator.next;
    allocator.next += count;
    debug_assert!(
        allocator.next - 1 <= allocator.persisted_hi,
        "B2 invariant violated: next-1 ({}) > persisted_hi ({})",
        allocator.next - 1,
        allocator.persisted_hi,
    );
    Ok(first)
}

impl CayenneTableProvider {
    /// Append a CDC upsert stream using Cayenne's native writer path.
    ///
    /// This bypasses `TableProvider::insert_into`/`DataSinkExec` construction
    /// for high-frequency CDC bursts. For simple staged appends, the returned
    /// [`CayenneCdcWrite`] is ready as soon as the staging WAL is durable; the
    /// caller can commit the source offset before awaiting its final publish.
    ///
    /// # Errors
    ///
    /// Returns an error if the CDC append cannot be staged or written.
    pub async fn write_cdc_append_stream(
        &self,
        data: SendableRecordBatchStream,
        task_context: &Arc<datafusion_execution::TaskContext>,
    ) -> Result<CayenneCdcWrite> {
        let target_schema = Arc::clone(&self.table_metadata.schema);
        // Tally the in-memory Arrow size of every batch as it streams through, so
        // the auto-tuner sees the real ingest *volume* (bytes/s), not just rows/s.
        // Costs one relaxed add per batch on a path that already touches each batch.
        let ingest_bytes = Arc::new(AtomicU64::new(0));
        let ingest_bytes_tally = Arc::clone(&ingest_bytes);
        let normalized = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&target_schema),
            data.map(move |batch_result| {
                batch_result.and_then(|batch| {
                    arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema))
                        .inspect(|cast| {
                            ingest_bytes_tally
                                .fetch_add(cast.get_array_memory_size() as u64, Ordering::Relaxed);
                        })
                        .map_err(Into::into)
                })
            }),
        ));

        let lock_wait_start = Instant::now();
        let write_guard = self.write_lock_arc().lock_owned().await;
        let lock_wait_elapsed = lock_wait_start.elapsed();
        if lock_wait_elapsed > Duration::from_millis(10) {
            tracing::debug!(
                table = self.table_name(),
                duration_ms = lock_wait_elapsed.as_millis(),
                "Cayenne write lock acquisition exceeded threshold in write_cdc_append_stream"
            );
        }

        let result = AppendMutationWriter::new(self, &self.context, task_context)
            .write_cdc_pipelined(normalized, write_guard)
            .await;
        // Feed the dynamic auto-tuner's rolling ingest accounting: the batch's row
        // count, the real ingested bytes (tallied above), and the full per-batch
        // apply wall (lock-wait + write) — the "am I keeping up with the offered
        // load?" response signal. Cheap and recorded regardless of whether dynamic
        // tuning is enabled (it also backs the always-on observability gauges).
        if let Ok(cdc_write) = &result {
            self.context.record_ingest(
                cdc_write.rows,
                ingest_bytes.load(Ordering::Relaxed),
                lock_wait_start.elapsed(),
            );
        }
        result
    }
}
