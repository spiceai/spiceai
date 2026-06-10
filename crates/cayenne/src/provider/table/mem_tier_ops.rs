//! Memory-tier (mem-table) append, checkpoint, and scan-plan operations
//! for `cdc_durability: memory`.
//!
//! [`CayenneTableProvider::append_to_mem_tier`] swaps the RAM tier and reserves
//! its (delete, data) sequence pair under `listing_fence.write()`;
//! [`CayenneTableProvider::checkpoint_mem_tier`] is the two-phase durable flush
//! (encode + metastore commit OFF the fence, in-memory visibility swap UNDER
//! it) that fires the `SlotAdvancer` only after durability. Checkpoints are
//! serialized by `mem_checkpoint_lock` (held by the caller).
//! `build_mem_tier_scan_plan` builds the scan-side merge-on-read branch.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;

use super::{
    Arc, CayenneTableProvider, Error, InlinedDeletionMaps, InlinedViewEntry, OnConflictDeletions,
    Ordering, RecordBatch, Result, RowCountUpdate, TryStreamExt,
};

impl CayenneTableProvider {
    pub(super) fn mem_tier_deletion_maps(
        snapshot: &crate::provider::mem_tier::MemTier,
    ) -> InlinedDeletionMaps {
        // The tier holds tombstones in persistent (`im::HashMap`) maps; the
        // merge-on-read filter consumes the std-`HashMap`-backed
        // `InlinedDeletionMaps`, so materialize a std map by iteration here. This
        // runs only on the SCAN / CHECKPOINT path (building the removal map), not
        // per CDC append, so it does not reintroduce the per-append O(tier) tax
        // the HAMT removed — and the resulting map is identical to the pre-HAMT
        // representation the filter already expected.
        InlinedDeletionMaps {
            int64_pk: snapshot
                .tombstones
                .int64_pk
                .iter()
                .map(|(&pk, &seq)| (pk, seq))
                .collect(),
            row_keys: snapshot
                .tombstones
                .row_keys
                .iter()
                .map(|(key, &seq)| (key.clone(), seq))
                .collect(),
        }
    }

    pub(super) fn mem_tier_has_tombstones(snapshot: &crate::provider::mem_tier::MemTier) -> bool {
        !snapshot.tombstones.int64_pk.is_empty() || !snapshot.tombstones.row_keys.is_empty()
    }

    pub(super) fn pruned_inlined_batches(
        &self,
        view: &[InlinedViewEntry],
        mem_tier: &crate::provider::mem_tier::MemTier,
        pruning_predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Vec<RecordBatch>> {
        if view.is_empty() {
            return Ok(Vec::new());
        }

        let schema = Arc::clone(&self.table_metadata.schema);
        let removal = if Self::mem_tier_has_tombstones(mem_tier) {
            Some(Self::mem_tier_deletion_maps(mem_tier))
        } else {
            None
        };

        let mut batches = Vec::new();
        for entry in view {
            let visible = if let Some(ref removal) = removal {
                self.apply_tombstone_removal_to_entry(entry, removal)?
            } else if entry.batches.is_empty() {
                continue;
            } else {
                entry.clone()
            };

            if visible.batches.is_empty() {
                continue;
            }

            if let Some(predicate) = pruning_predicate
                && super::file_pruning::should_prune_statistics(
                    visible.statistics.as_ref(),
                    &schema,
                    predicate,
                )
            {
                continue;
            }

            batches.extend(visible.batches);
        }
        Ok(batches)
    }

    pub(super) fn visible_mem_tier_batches(
        &self,
        snapshot: &crate::provider::mem_tier::MemTier,
        pruning_predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Vec<RecordBatch>> {
        if snapshot.segments.is_empty() {
            return Ok(Vec::new());
        }

        let schema = Arc::clone(&self.table_metadata.schema);
        let inlined_deletions = Self::mem_tier_deletion_maps(snapshot);
        let mut visible_batches: Vec<RecordBatch> = Vec::new();
        for segment in snapshot.segments.iter() {
            if let Some(predicate) = pruning_predicate
                && super::file_pruning::should_prune_statistics(
                    segment.statistics.as_ref(),
                    &schema,
                    predicate,
                )
            {
                continue;
            }

            for batch in segment.batches.iter() {
                let Some(visible) = self.filter_inlined_batch_for_deletions(
                    batch.clone(),
                    segment.data_sequence,
                    &inlined_deletions,
                )?
                else {
                    continue;
                };
                visible_batches.push(visible);
            }
        }

        Ok(visible_batches)
    }

    /// Whether appending `incoming_bytes`/`incoming_rows` to the live mem tier
    /// would breach this table's per-table byte cap or age cap. (The global byte
    /// budget is checked separately at reservation time.)
    pub(crate) fn mem_tier_per_table_cap_breached(&self, incoming_bytes: u64) -> bool {
        let cur = self.mem_tier.load();
        let would_be = cur.bytes.saturating_add(incoming_bytes);
        let byte_breach = would_be >= self.mem_tier_max_bytes;
        let age_breach = self.mem_tier_max_age_ms > 0 && cur.age_ms() >= self.mem_tier_max_age_ms;
        byte_breach || age_breach
    }

    /// Build the in-RAM tombstone map for a mem-tier append from the upsert's
    /// computed on-conflict deletions, at `delete_sequence`. In memory mode the
    /// prior copy of a superseded PK lives either in the RAM tier or in an
    /// earlier durable checkpoint; the merge-on-read filter applies the same
    /// tombstone to both, so the file-backed AND inlined deleted-key encodings
    /// are unioned into one RAM map keyed by the strategy. `superseded` is the
    /// authoritative live-row delta (carried separately, NOT recomputed).
    pub(super) fn build_mem_tombstones(
        &self,
        deletions: &OnConflictDeletions,
        delete_sequence: i64,
    ) -> crate::provider::mem_tier::InMemTombstones {
        let mut tombstones = crate::provider::mem_tier::InMemTombstones::default();
        if self.pk_deletion_strategy.is_int64_pk() {
            for &pk in deletions
                .deleted_pk_i64
                .iter()
                .chain(deletions.deleted_inlined_pk_i64.iter())
            {
                tombstones
                    .int64_pk
                    .entry(pk)
                    .and_modify(|s| *s = (*s).max(delete_sequence))
                    .or_insert(delete_sequence);
            }
        } else {
            for key in deletions
                .deleted_row_keys
                .iter()
                .chain(deletions.deleted_inlined_row_keys.iter())
            {
                tombstones
                    .row_keys
                    .entry(key.clone())
                    .and_modify(|s| *s = (*s).max(delete_sequence))
                    .or_insert(delete_sequence);
            }
        }
        tombstones
    }

    /// Append a validated CDC batch to the in-memory tier (`cdc_durability:
    /// memory`) and return the mem-tier epoch it landed in.
    ///
    /// Allocates two monotone sequences from the shared durable allocator:
    /// `delete_sequence = base` for the tombstones (hiding prior copies of
    /// superseded PKs) and `data_sequence = base + 1` for the appended rows (so
    /// the fresh rows are visible above their own tombstones). The visibility
    /// swap happens under the listing fence and is published by an
    /// `inlined_generation` bump, exactly like the durable inline path — so a
    /// concurrent scan observes the append atomically.
    ///
    /// Does NOT persist a durable BLOB and does NOT advance the source slot; the
    /// slot ack is deferred to [`Self::checkpoint_mem_tier`]. The caller has
    /// already reserved `incoming_bytes` against the global budget.
    pub(crate) async fn append_to_mem_tier(
        &self,
        batches: Vec<RecordBatch>,
        deletions: &OnConflictDeletions,
        incoming_bytes: u64,
        superseded: u64,
    ) -> Result<u64> {
        let incoming_rows: u64 = batches
            .iter()
            .map(|b| b.num_rows() as u64)
            .fold(0, u64::saturating_add);

        let arc_batches = Arc::new(batches);
        let epoch = {
            // Publish the RAM swap under the same listing fence the durable
            // inline publish uses, so readers capture the new tier atomically.
            let _fence = self.listing_fence.write().await;

            // Reserve the (delete, data) sequence pair INSIDE the fence so append
            // sequence assignment is mutually exclusive with a checkpoint's
            // snapshot-sequence reservation (which also runs under this fence).
            // This guarantees the ordering invariant the off-fence checkpoint
            // relies on: a checkpoint's `snapshot_sequence` is strictly below
            // every sequence an append reserves AFTER the checkpoint captured its
            // flush set — so a later upsert that supersedes a just-flushed key
            // always outranks the durable copy on merge-on-read, and its tombstone
            // (delete_sequence > snapshot_sequence) correctly hides the file. Were
            // the reserve OUTSIDE the fence, an append could reserve a sequence
            // BELOW a concurrent checkpoint's snapshot_sequence yet append AFTER
            // it, inverting the order and orphaning the stale durable copy (a
            // permanent over-count). delete below data so new rows survive their
            // own tombstones; from the shared durable allocator for monotonicity.
            let base_sequence = self.reserve_sequences_local(2).await?;
            let delete_sequence = base_sequence;
            let data_sequence = base_sequence + 1;
            let tombstones = self.build_mem_tombstones(deletions, delete_sequence);

            let cur = self.mem_tier.load();
            let next = cur.append_segment(
                arc_batches,
                data_sequence,
                &tombstones,
                incoming_bytes,
                incoming_rows,
                superseded,
            );
            let epoch = next.epoch;
            self.mem_tier.store(Arc::new(next));
            // A new tombstone can retroactively hide rows already materialized in
            // a cached inline/mem view, so this is a STRUCTURAL change (full
            // re-read on the next scan), matching the durable tombstone path.
            self.bump_inlined_structural_epoch();
            epoch
        };

        // Net the live row count by the superseded rows, exactly like the durable
        // upsert path (`inserted - superseded`).
        let net = i64::try_from(incoming_rows).unwrap_or(i64::MAX)
            - i64::try_from(superseded).unwrap_or(i64::MAX);
        self.inlined_row_count.fetch_add(net, Ordering::Relaxed);

        Ok(epoch)
    }

    /// Checkpoint the in-memory CDC tier: encode the retained corpus to a durable
    /// Vortex snapshot, clear the tier, then — and ONLY then — fire the
    /// [`SlotAdvancer`] callback so the runtime advances the source slot to cover
    /// every batch in the flushed epoch (the slot-deferral correctness contract).
    ///
    /// For the key/Int64 strategies this runs the durable encode + metastore
    /// commit OUTSIDE the listing fence and takes the fence only for the cheap
    /// in-memory visibility swap (two-phase; see the body), so concurrent CDC
    /// appends do not stall on the checkpoint. The position-based strategy keeps
    /// encode+swap under one held fence (it appends to the current snapshot). The
    /// corpus comes from the RAM tier and the post-fence tail advances the slot
    /// rather than clearing inline metastore rows.
    ///
    /// On ANY error before the durable fence commits, the slot is NOT advanced
    /// (the deferred committers stay queued) and the error propagates up so the
    /// refresh status flips to Error and the stream replays from the slot
    /// (correctness item #4 — a failed checkpoint never advances).
    #[doc(hidden)]
    pub async fn checkpoint_mem_tier(&self) -> Result<u64> {
        // Capture the corpus to flush AND reserve this checkpoint's
        // snapshot_sequence ATOMICALLY under a brief listing-fence write, so the
        // capture+reservation is mutually exclusive with append sequence
        // assignment (appends reserve under the same fence). This pins the
        // ordering invariant: `snapshot_sequence` is strictly above every flushed
        // row's sequence and strictly below every sequence an append reserves
        // after this point — so the off-fence encode/commit below cannot be
        // overtaken by a concurrent upsert (which would orphan the stale durable
        // copy as a permanent over-count). The fence is held only for the cheap
        // load + reserve, NOT the encode/commit. Checkpoints are serialized by
        // `mem_checkpoint_lock` (caller-held) so no concurrent checkpoint races.
        let (snapshot, reserved_snapshot_sequence) = {
            let _fence = self.listing_fence.write().await;
            let snapshot = self.mem_tier.load_full();
            let seq = if snapshot.is_empty() || self.pk_deletion_strategy.is_position_based() {
                None
            } else {
                Some(self.reserve_sequences_local(1).await?)
            };
            (snapshot, seq)
        };
        if snapshot.is_empty() {
            return Ok(0);
        }
        let flushed_epoch = snapshot.epoch;
        let inlined_view = self.cached_inlined_view().await?;
        let mut batches = self.pruned_inlined_batches(&inlined_view, &snapshot, None)?;
        let mem_batches = self.visible_mem_tier_batches(&snapshot, None)?;
        let flushed_mem_rows: usize = mem_batches.iter().map(RecordBatch::num_rows).sum();
        batches.extend(mem_batches);

        if batches.is_empty() {
            // Tombstones-only tier (every appended row already superseded): clear
            // and still advance the slot for the flushed epoch.
            self.clear_mem_tier_up_to_epoch(flushed_epoch, snapshot.segments.len());
            if !inlined_view.is_empty() {
                self.clear_inlined_metadata_after_checkpoint().await?;
                self.flip_inlined_keyset_entries_to_file_unlocated();
            }
            let remaining_mem_rows = self.mem_tier.load().rows;
            self.inlined_row_count.store(
                i64::try_from(remaining_mem_rows).unwrap_or(i64::MAX),
                Ordering::Relaxed,
            );
            self.fire_slot_advancer(flushed_epoch).await;
            return Ok(0);
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let estimated_bytes = Some(
            batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .fold(0u64, u64::saturating_add),
        );
        tracing::info!(
            table = %self.table_metadata.table_name,
            rows = flushed_mem_rows,
            inlined_rows = total_rows.saturating_sub(flushed_mem_rows),
            batches = batches.len(),
            epoch = flushed_epoch,
            "Checkpointing in-memory CDC tier to a durable Vortex snapshot"
        );

        let schema = Arc::clone(&self.table_metadata.schema);
        let mem_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[batches],
            Arc::clone(&schema),
            None,
        )?;
        let ctx = self.create_session_context();
        let stream = datafusion_physical_plan::execute_stream(mem_exec, ctx.task_ctx())?;

        // Two-phase checkpoint (moonshot lever 1+2): for the key/Int64 strategies
        // the expensive durable work — the Vortex ENCODE (`write_to_snapshot`) and
        // the `BEGIN IMMEDIATE` metastore COMMIT (`commit_mem_tier_checkpoint_metadata`,
        // ~98% of publish cost) — runs OUTSIDE the listing fence, then a tiny
        // fence-held section performs only the in-memory visibility swap (publish
        // the snapshot pointer + deletion cache, clear the flushed epoch, refresh
        // the listing table). This is safe because the encoded file lands under a
        // FRESH `new_snapshot_id` that no reader references until the under-fence
        // `refresh_listing_table_under_held_fence`, and the source slot is advanced
        // only AFTER the fence (`fire_slot_advancer` below) — so on a crash in the
        // gap the durable file + metastore pointer exist and the un-acked tail
        // replays PK-idempotently. The win: concurrent CDC appends (which also take
        // `listing_fence.write()`) no longer stall on the encode/commit, so the
        // background checkpointer can keep the RAM tier drained without throttling
        // ingest. The position-based strategy appends to the CURRENT snapshot and
        // therefore must keep encode+swap atomic under one held fence (unchanged).
        let stats = if self.pk_deletion_strategy.is_position_based() {
            let _fence = self.listing_fence.write().await;
            let target_size_bytes = self.context.target_file_size_bytes();
            let (_rows, _ops, stats) = self
                .write_to_snapshot(
                    stream,
                    target_size_bytes,
                    &self.get_current_snapshot_id(),
                    ctx.state().config().target_partitions(),
                    estimated_bytes,
                    super::delta_encoding::WriteClass::Delta,
                )
                .await?;
            self.clear_mem_tier_up_to_epoch(flushed_epoch, snapshot.segments.len());
            if !inlined_view.is_empty() {
                self.clear_inlined_metadata_after_checkpoint().await?;
                self.flip_inlined_keyset_entries_to_file_unlocated();
            }
            let remaining_mem_rows = self.mem_tier.load().rows;
            self.inlined_row_count.store(
                i64::try_from(remaining_mem_rows).unwrap_or(i64::MAX),
                Ordering::Relaxed,
            );
            self.refresh_listing_table_under_held_fence().await?;
            stats
        } else {
            // PHASE 1 — durable, OUTSIDE the fence (does not block appends). The
            // new file is unreferenced and the RAM tier still holds the rows, so a
            // concurrent scan sees the RAM rows (correct) and never the file yet.
            // The snapshot_sequence was reserved UNDER the fence at capture time
            // (serialized with appends) — it is strictly below every later append's
            // sequence, so the durable file never outranks a concurrent supersede.
            let Some(sequence_number) = reserved_snapshot_sequence else {
                return Err(Error::DataValidation {
                    table: self.table_name().to_string(),
                    message: "non-position checkpoint reserved its snapshot_sequence at capture"
                        .to_string(),
                });
            };
            let new_snapshot_id = uuid::Uuid::now_v7().to_string();
            let target_size_bytes = self.context.target_file_size_bytes();
            let (_rows, _ops, stats) = self
                .write_to_snapshot(
                    stream,
                    target_size_bytes,
                    &new_snapshot_id,
                    ctx.state().config().target_partitions(),
                    estimated_bytes,
                    super::delta_encoding::WriteClass::Delta,
                )
                .await?;

            if !self.table_metadata.path.starts_with("s3://") {
                let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
                Self::sync_snapshot_dir(&snapshot_dir).await?;
            }

            let update = self
                .commit_mem_tier_checkpoint_metadata(&snapshot, &new_snapshot_id, sequence_number)
                .await?;

            // PHASE 2 — in-memory visibility swap, UNDER the fence. Cheap: an
            // ArcSwap publish, a tier clear, and a listing-table refresh — no
            // encode, no metastore round-trip. Indivisible w.r.t. scans, so the
            // file becoming visible and the RAM rows clearing happen atomically.
            {
                let _fence = self.listing_fence.write().await;
                self.commit_on_conflict_publish(update, Some((&new_snapshot_id, sequence_number)))
                    .await;
                self.clear_mem_tier_up_to_epoch(flushed_epoch, snapshot.segments.len());
                if !inlined_view.is_empty() {
                    self.clear_inlined_metadata_after_checkpoint().await?;
                    self.flip_inlined_keyset_entries_to_file_unlocated();
                }
                let remaining_mem_rows = self.mem_tier.load().rows;
                self.inlined_row_count.store(
                    i64::try_from(remaining_mem_rows).unwrap_or(i64::MAX),
                    Ordering::Relaxed,
                );
                self.refresh_listing_table_under_held_fence().await?;
            }
            stats
        };

        // The rows moved from RAM to a file; they were already counted live on
        // append, so the live count is unchanged (only the stats blob re-merges).
        self.persist_table_stats(&stats, RowCountUpdate::Unchanged)
            .await;

        // ONLY NOW — after the Vortex file + metastore pointer are durable — tell
        // the runtime it may advance the source slot to cover this epoch.
        self.fire_slot_advancer(flushed_epoch).await;

        Ok(u64::try_from(flushed_mem_rows).unwrap_or(u64::MAX))
    }

    /// Swap the mem tier to its remainder after a checkpoint flushed every
    /// segment in the flushed snapshot (its first `flushed_segment_count`
    /// segments — an append-ordered prefix). A concurrent append that grew the
    /// tier ABOVE the snapshot (which the two-phase checkpoint now allows, since
    /// its encode/commit run OUTSIDE the listing fence) is preserved: we drop the
    /// flushed prefix and KEEP ONLY the survivor segments, re-folding their
    /// tombstones/bytes/rows via [`MemTier::retain_after`]. Keeping the whole tier
    /// instead would re-flush the already-durable prefix into a second file on the
    /// next checkpoint — a double-count (the bug the off-fence move exposed).
    /// Checkpoints are serialized by `mem_checkpoint_lock`, so the only interleaving
    /// writer is an append (push-only); this store runs under the listing fence the
    /// caller holds (appends also take it to swap), so the prefix is stable.
    pub(super) fn clear_mem_tier_up_to_epoch(
        &self,
        flushed_epoch: u64,
        flushed_segment_count: usize,
    ) {
        let cur = self.mem_tier.load_full();
        let survivors = cur.retain_after(flushed_segment_count);
        // Release exactly the flushed segments' bytes (cur − survivors) back to
        // the process-global budget; survivor bytes stay resident.
        let released = cur.bytes.saturating_sub(survivors.bytes);
        debug_assert!(
            survivors.epoch >= flushed_epoch,
            "survivor tier must preserve the monotone epoch at/above the flushed one"
        );
        self.mem_tier.store(Arc::new(survivors));
        crate::provider::mem_tier_budget::release_bytes(released);
        self.bump_inlined_structural_epoch();
    }

    /// Fire the installed [`SlotAdvancer`] for `durable_epoch`, if one is wired
    /// up (memory mode). A no-op in file mode / when the runtime did not install
    /// a handle.
    pub(super) async fn fire_slot_advancer(&self, durable_epoch: u64) {
        let advancer = self.slot_advancer.lock().clone();
        if let Some(advancer) = advancer {
            advancer.on_checkpoint_durable(durable_epoch).await;
        }
    }

    /// Build the scan-side `MemorySourceConfig` exec for the in-memory CDC tier
    /// snapshot, applying each segment's merge-on-read tombstones and the
    /// requested projection. Returns `None` when the tier is empty (file mode, or
    /// a freshly-checkpointed memory-mode table) so the scan plan is unchanged.
    ///
    /// Each retained segment is filtered against the tier's accumulated
    /// tombstones at the segment's own `data_sequence` via
    /// [`Self::filter_inlined_batch_for_deletions`] — the SAME merge-on-read keep
    /// rule (and the same disjoint-skip fast path) as the durable inline corpus,
    /// with the tombstones supplied from the in-RAM map instead of the metastore.
    pub(super) fn build_mem_tier_scan_plan(
        &self,
        snapshot: &crate::provider::mem_tier::MemTier,
        effective_projection: Option<&Vec<usize>>,
        pruning_predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if snapshot.is_empty() || snapshot.segments.is_empty() {
            return Ok(None);
        }

        let visible_batches = self
            .visible_mem_tier_batches(snapshot, pruning_predicate)
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to apply in-memory CDC tier deletion visibility for table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;

        if visible_batches.is_empty() {
            return Ok(None);
        }

        // Project to the effective projection (reusing the inlined-plan logic).
        let proj_schema = if let Some(proj) = effective_projection {
            let schema_fields = self.table_metadata.schema.fields();
            let fields: Vec<arrow_schema::FieldRef> = proj
                .iter()
                .map(|&i| Arc::clone(&schema_fields[i]))
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            Arc::clone(&self.table_metadata.schema)
        };

        let projected_batches: Vec<RecordBatch> = visible_batches
            .into_iter()
            .map(|batch| {
                if let Some(proj) = effective_projection {
                    batch.project(proj).map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to project in-memory CDC tier batch for table {}: {e}",
                            self.table_metadata.table_name
                        ))
                    })
                } else {
                    Ok(batch)
                }
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        if projected_batches.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                &[projected_batches],
                proj_schema,
                None,
            )?,
        ))
    }
}
