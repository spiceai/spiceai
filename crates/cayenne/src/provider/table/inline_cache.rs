//! Inline memtable (metastore-inlined data) cache, checkpointing, and inline deletes.
//!
//! The decoded inline corpus is cached in [`InlinedCache`], keyed by the
//! `inlined_generation` counter with a `structural_epoch` deciding between the
//! append-only delta path (`extend_inlined_cache_delta`) and a full rebuild
//! (`rebuild_inlined_cache_full`) on a miss — see the type-level contract.
//! Entry points: `read_inlined_batches` / `cached_inlined_view` (scan side),
//! `try_inline_batches_with_inlined_deletions` (inline write, publishes under
//! `visibility_lock` → `listing_fence.write()` → `scan_state_lock.write()`),
//! and `checkpoint_inlined_data` (flush to Vortex under `listing_fence.write()`;
//! callers hold `write_lock`).
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use arrow::array::Array;
use datafusion_physical_expr::PhysicalExpr;

use super::{
    Arc, CatalogError, CayenneTableProvider, DFSchema, Error, ExecutionProps, Expr,
    ExtractedPrimaryKeys, HashMap, HashSet, InlinedData, InlinedDataRewrite, InlinedDataStats,
    InlinedDeletionMaps, Int64Array, ObjectStoreExt, Ordering, PkDeletionStrategyWithCache,
    RecordBatch, Result, RowCountUpdate, Statistics, TryStreamExt, TypeCoercionRewriter,
    create_physical_expr, deserialize_delete_keys_from_ipc, deserialize_ipc_to_batch,
    serialize_batches_to_ipc,
};

/// Per-entry decoded view of one metastore inline-data row.
///
/// Pairs the original [`InlinedData`] envelope (needed to build rewrites
/// without a second metastore round-trip) with the pre-decoded,
/// deletion-filtered `RecordBatch`es for that entry.
///
/// `Clone` is cheap: the envelope is small metadata and each `RecordBatch`
/// shares its Arrow buffers via `Arc`. The append-only inline-cache delta path
/// clones the base view's entries (structural sharing of the buffers) before
/// appending the newly decoded entries.
#[derive(Clone)]
pub(super) struct InlinedViewEntry {
    /// Original metastore envelope; provides `inlined_id`, `sequence_number`,
    /// and other fields required to reconstruct a rewrite.
    pub(super) envelope: InlinedData,
    /// Batches already decoded from IPC and filtered through the deletion map.
    /// Empty when all rows in this entry were removed by the deletion filter.
    pub(super) batches: Vec<RecordBatch>,
    /// Conservative min/max over the decoded IPC batches (pre-tombstone filter).
    pub(super) statistics: Arc<Statistics>,
}

/// Cached result of [`CayenneTableProvider::read_inlined_batches`] and
/// [`CayenneTableProvider::cached_inlined_view`].
///
/// The cache is keyed by an `inlined_generation` counter that is incremented
/// (with `Release` ordering) by every `commit_inlined_data_mutation` and
/// `clear_inlined_metadata_after_checkpoint` call. A cache entry is valid only
/// when its stored `generation` equals the live counter — guaranteeing that any
/// write or checkpoint immediately invalidates the cache without a lock.
///
/// # Incremental maintenance contract
///
/// On a miss, the cache is **not** always rebuilt from the whole corpus. The
/// `structural_epoch` records the value of `inlined_structural_epoch` this view
/// was built at. That epoch is bumped ONLY by mutations that can retroactively
/// change an already-materialized entry — an inline rewrite/removal
/// (`removed_rows > 0`), a newly published tombstone, a checkpoint clear, an
/// overwrite, or open-time recovery. A pure append (new rows at a sequence above
/// every existing entry, with no rewrite and no new tombstone) bumps only the
/// generation. So when a miss observes the SAME structural epoch as the cached
/// view, the only changes since were appends, and
/// `CayenneTableProvider::populate_inlined_cache` takes the cheap delta path:
/// it fetches just the entries with `sequence_number >
/// materialized_through_sequence`, decodes+filters those, and merges them onto
/// the structurally-shared existing `view` — never re-reading or re-decoding the
/// corpus. Any other miss
/// (structural-epoch mismatch, sentinel/first touch) falls back to a full
/// rebuild. See [`CayenneTableProvider::populate_inlined_cache`].
pub(super) struct InlinedCache {
    /// Generation at the time this entry was built.
    pub(super) generation: u64,
    /// `inlined_structural_epoch` at the time this entry was built. A miss whose
    /// live structural epoch still matches this value proves every change since
    /// was append-only and the entry can be extended with the delta instead of
    /// rebuilt. See the type-level "Incremental maintenance contract".
    pub(super) structural_epoch: u64,
    /// The visibility watermark (`published_inlined_seq`) at the time this view
    /// was built: the view materialized exactly the entries with
    /// `sequence_number <= materialized_through_sequence`. The append-only delta
    /// path queries `sequence_number > materialized_through_sequence` to fetch
    /// precisely the entries that have become eligible since — both rows appended
    /// above the old watermark AND rows that were durably committed but held back
    /// by the old watermark and are now published. This boundary (not the corpus
    /// max) is what makes the delta both gap-free (a watermark advance re-fetches
    /// the now-visible held-back rows) and duplicate-free (already-materialized
    /// rows have `seq <= this` and are excluded). `i64::MIN` for the empty
    /// sentinel so the first real read fetches everything.
    pub(super) materialized_through_sequence: i64,
    /// Highest `PendingTombstoneDeltas::seq` whose removal this view has applied
    /// (cycle-5 TASK 1). A published tombstone now enqueues a removal delta and
    /// bumps ONLY the generation (not the structural epoch), so the delta path
    /// applies exactly the deltas with `seq > this` to the structurally-shared
    /// base entries — re-filtering them against just the newly-deleted keys
    /// instead of full-rebuilding from the corpus. A full rebuild stamps this
    /// with the queue's current seq (it captured every tombstone via
    /// `load_inlined_deletion_maps`). `0` for the empty sentinel.
    pub(super) tombstone_delta_seq: u64,
    /// Flattened `RecordBatch`es across all entries. Each batch shares Arrow
    /// buffer ownership via `Arc`, so cloning the `Vec` is cheap.
    pub(super) batches: Arc<Vec<RecordBatch>>,
    /// Per-entry view used by the upsert-rewrite path to avoid a second
    /// metastore round-trip and re-decode.
    pub(super) view: Arc<Vec<InlinedViewEntry>>,
}

// Inlining caps are intentionally conservative: inlined data is reread on every
// scan, lives as BLOBs in the metastore, and gets no zone-map pruning. Raising
// these limits trades a slightly cheaper write path for read amplification on
// every subsequent query — the wrong tradeoff for large-dataset workloads,
// which are the dominant use case for Cayenne. The right lever for large
// datasets is `target_vortex_file_size_mb` plus the tiered small-files
// compaction in `provider::compaction`, not bigger inline flush caps.

/// Maximum number of rows to inline in the metastore instead of writing a Vortex file.
#[cfg(test)]
pub(crate) const INLINE_MAX_ROWS: usize = crate::metadata::DEFAULT_INLINE_MAX_ROWS;

/// Maximum rows to keep inline before flushing to Vortex.
#[cfg(test)]
pub(crate) const INLINE_FLUSH_MAX_ROWS: i64 = crate::metadata::DEFAULT_INLINE_FLUSH_MAX_ROWS;

/// Maximum inline entries before flushing to Vortex.
#[cfg(test)]
pub(crate) const INLINE_FLUSH_MAX_SEGMENTS: i64 =
    crate::metadata::DEFAULT_INLINE_FLUSH_MAX_SEGMENTS;

/// Maximum serialized IPC bytes to keep inline before flushing to Vortex.
#[cfg(test)]
pub(crate) const INLINE_FLUSH_MAX_BYTES: i64 = crate::metadata::DEFAULT_INLINE_FLUSH_MAX_BYTES;

/// Maximum in-memory byte budget while buffering the inline fast-path stream.
///
/// `DEFAULT_INLINE_MAX_ROWS` alone does not bound memory usage — a pathological batch
/// with few rows but very large string / binary values can still consume a lot
/// of RAM. Once the cumulative array memory size of buffered batches exceeds
/// this budget the fast-path bails out and falls through to the normal Vortex
/// write path, where the stream is consumed incrementally. Held slightly above
/// the default serialized IPC cap to account for in-memory Arrow overhead vs.
/// the compact IPC representation.
#[cfg(test)]
pub(crate) const INLINE_MAX_BUFFER_BYTES: usize = crate::metadata::DEFAULT_INLINE_MAX_BUFFER_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlineMemtablePressure {
    Rows,
    Segments,
    IpcBytes,
}

impl InlineMemtablePressure {
    #[must_use]
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::Segments => "segments",
            Self::IpcBytes => "ipc_bytes",
        }
    }
}

#[must_use]
#[cfg(test)]
pub(crate) fn inline_memtable_pressure(stats: InlinedDataStats) -> Option<InlineMemtablePressure> {
    inline_memtable_pressure_with_thresholds(
        stats,
        INLINE_FLUSH_MAX_ROWS,
        INLINE_FLUSH_MAX_SEGMENTS,
        INLINE_FLUSH_MAX_BYTES,
    )
}

#[must_use]
pub(super) fn inline_memtable_pressure_with_thresholds(
    stats: InlinedDataStats,
    max_rows: i64,
    max_segments: i64,
    max_bytes: i64,
) -> Option<InlineMemtablePressure> {
    if stats.record_count >= max_rows {
        return Some(InlineMemtablePressure::Rows);
    }
    if stats.entry_count > max_segments {
        return Some(InlineMemtablePressure::Segments);
    }
    if stats.ipc_bytes >= max_bytes {
        return Some(InlineMemtablePressure::IpcBytes);
    }
    None
}

impl CayenneTableProvider {
    /// Write small batches directly to the metastore, optionally atomically
    /// rewriting inline rows they replace.
    pub(crate) async fn try_inline_batches_with_inlined_deletions(
        &self,
        batches: &[RecordBatch],
        deleted_inlined_pk_i64: &[i64],
        deleted_inlined_row_keys: &[Box<[u8]>],
        file_deleted_pk_i64: &[i64],
        file_deleted_row_keys: &[Box<[u8]>],
    ) -> Result<bool> {
        let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if total_rows == 0 {
            return Ok(true); // nothing to write
        }
        let inline_max_rows = self.context.inline_max_rows();
        let inline_max_bytes = self.context.inline_max_bytes();
        if inline_max_rows == 0 || inline_max_bytes == 0 || total_rows > inline_max_rows {
            return Ok(false);
        }
        let ipc_bytes =
            serialize_batches_to_ipc(batches).map_err(|e| Error::Arrow { source: e })?;
        if ipc_bytes.len() > inline_max_bytes {
            return Ok(false);
        }

        // --- Past this point, inlining WILL proceed (all size checks passed). ---

        let has_file_deletions =
            !file_deleted_pk_i64.is_empty() || !file_deleted_row_keys.is_empty();

        // Lever B2 + I5: allocate the file-deletion sequence and the inline-row
        // sequence from the SAME in-memory allocator as ONE contiguous block, so
        // the inline entry's sequence is strictly higher than the `delete_seq`
        // (`inline_seq = delete_seq + 1`). Previously the `delete_seq` came from
        // a `reserve_sequence_numbers` call and the inline-row seq was derived
        // INSIDE `commit_inlined_mutation` by bumping the DB counter again — with
        // the allocator owning allocation, the DB counter no longer moves inside
        // that txn, so the inline-row seq must be reserved here and passed in.
        //   - file deletions present: block of 2 -> [delete_seq, inline_seq]
        //   - inline-only:            block of 1 -> [inline_seq]
        let block_count = if has_file_deletions { 2 } else { 1 };
        let block_first = self
            .reserve_sequences_local(block_count)
            .await
            .map_err(|err| Error::Catalog {
                source: CatalogError::InvalidOperationNoSource {
                    message: format!("Failed to reserve sequences for inline insert: {err}"),
                },
            })?;
        let (delete_seq, inline_seq) = if has_file_deletions {
            (Some(block_first), block_first + 1)
        } else {
            (None, block_first)
        };

        let rewrite = self
            .build_inlined_data_rewrite_for_pk_keys(
                deleted_inlined_pk_i64,
                deleted_inlined_row_keys,
            )
            .await?;
        let removed_rows = rewrite.removed_rows;

        // Durably commit the inlined data WITHOUT publishing visibility yet, so
        // making the new inlined row visible and hiding the file-backed row it
        // supersedes can be published together under one
        // `scan_state_lock.write()`. The inline-row sequence (`inline_seq`,
        // reserved above from the same allocator and strictly above
        // `delete_seq`) is passed in; `commit_inlined_mutation` no longer mutates
        // the DB counter.
        let inlined_commit = self
            .commit_inlined_data_durable(
                rewrite,
                vec![InlinedData::pending_catalog_insert(
                    self.table_metadata.table_id.clone(),
                    None,
                    ipc_bytes,
                    i64::try_from(total_rows).unwrap_or(i64::MAX),
                )],
                Some(inline_seq),
            )
            .await?;

        // Persist file-backed deletion vectors durably BEFORE the in-memory
        // publish, so all durable I/O is complete by the time the write guard is
        // taken and the guard is held only for the synchronous cache flips.
        if let Some(delete_seq) = delete_seq {
            self.persist_file_deletions_after_inlined_insert(
                file_deleted_pk_i64,
                file_deleted_row_keys,
                delete_seq,
            )
            .await
            .map_err(|err| Error::Catalog { source: err })?;
        }

        // Atomically publish both in-memory visibility changes: make the new inlined row visible (generation bump) and hide the superseded
        // file-backed row (deletion cache). Scans capture the (inlined view, deletion view) pair under `scan_state_lock.read()`,
        // so committing both halves here under one `.write()` closes the duplicate-PK window.
        // Only synchronous in-memory work runs under the guard; all durable I/O above is already complete.
        {
            let _visibility = self.visibility_lock_arc().lock_owned().await;
            let _fence = self.lock_listing_fence_write_owned().await;
            let _view_guard = self.scan_state_lock.write().await;
            if let Some(commit) = inlined_commit {
                // `inlined_row_count` must reflect the actual number of rows
                // living in the inline memtable — scan paths use it as a
                // visibility signal (`> 0` → read inline data). Only subtract
                // inlined supersedes (prior inline entries rewritten), NOT
                // file-backed supersedes. The file-backed supersede netting is
                // handled by `live_rows_delta` in `try_inline_or_restream`.
                self.publish_inlined_mutation(
                    total_rows,
                    commit.removed_rows,
                    commit.published_seq,
                );
            }
            if let Some(delete_seq) = delete_seq {
                self.update_file_deletion_cache(
                    file_deleted_pk_i64,
                    file_deleted_row_keys,
                    delete_seq,
                );
            }
        }

        tracing::debug!(
            table = self.table_metadata.table_name,
            rows = total_rows,
            inlined_rows_removed = removed_rows,
            file_pk_deletions = file_deleted_pk_i64.len() + file_deleted_row_keys.len(),
            "Inlined write"
        );

        Ok(true)
    }

    #[must_use]
    pub(crate) fn cached_inlined_row_count(&self) -> i64 {
        self.inlined_row_count.load(Ordering::Relaxed)
    }

    /// Returns the current inline-memtable cache generation counter.
    ///
    /// Monotonically increasing: bumped after every `commit_inlined_data_mutation`
    /// (write path) and `clear_inlined_metadata_after_checkpoint` (flush path).
    /// Exposed for testing cache-invalidation invariants.
    #[must_use]
    pub fn inlined_generation(&self) -> u64 {
        self.inlined_generation.load(Ordering::Relaxed)
    }

    /// Returns the current inline-memtable cache STRUCTURAL epoch counter.
    ///
    /// A subset of the generation bumps: advanced only by mutations that can
    /// retroactively change an already-materialized inline view (rewrite/removal,
    /// tombstone publish, checkpoint, overwrite, recovery). A cache miss whose
    /// cached view shares this epoch takes the append-only delta path. Exposed for
    /// testing the delta-vs-full provenance invariant.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn inlined_structural_epoch(&self) -> u64 {
        self.inlined_structural_epoch.load(Ordering::Relaxed)
    }

    /// Returns the `materialized_through_sequence` boundary of the currently
    /// cached inline view. Exposed for testing the incremental delta boundary.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn cached_inlined_materialized_through_sequence(&self) -> i64 {
        self.inlined_cache.load().materialized_through_sequence
    }

    pub(super) fn try_read_inlined_batches_cached(&self) -> Option<Vec<RecordBatch>> {
        let current_gen = self.inlined_generation.load(Ordering::Acquire);
        let cached = self.inlined_cache.load();
        (cached.generation == current_gen).then(|| (*cached.batches).clone())
    }

    pub(super) fn try_read_inlined_view_cached(&self) -> Option<Arc<Vec<InlinedViewEntry>>> {
        let current_gen = self.inlined_generation.load(Ordering::Acquire);
        let cached = self.inlined_cache.load();
        (cached.generation == current_gen).then(|| Arc::clone(&cached.view))
    }

    pub(super) fn try_read_inlined_view_for_scan(&self) -> Option<Arc<Vec<InlinedViewEntry>>> {
        if self.cached_inlined_row_count() <= 0 {
            return Some(Arc::new(Vec::new()));
        }
        self.try_read_inlined_view_cached()
    }

    /// Read visible inlined data for this table and return as `RecordBatch`es.
    ///
    /// Used at scan time to union inlined data with the file-based data. For
    /// primary-key tables this still honors legacy metastore-inlined delete
    /// markers, while new inline mutations rewrite `cayenne_inlined_data` rows
    /// directly.
    ///
    /// # Caching
    ///
    /// The result is cached keyed by `inlined_generation`. Writers bump the
    /// generation (with `Release` ordering) after every successful catalog
    /// commit, so a cache hit requires no metastore I/O and no Arrow IPC
    /// decode — it is one atomic load and one `Arc::clone`.
    ///
    /// On a cache miss the function rebuilds from the metastore and stores the
    /// decoded batches in `inlined_cache`. Concurrent misses are safe: each
    /// produces identical results for the same generation, and the last
    /// `ArcSwap::store` wins without corrupting data.
    pub(crate) async fn read_inlined_batches(&self) -> Result<Vec<RecordBatch>> {
        if let Some(batches) = self.try_read_inlined_batches_cached() {
            return Ok(batches);
        }

        let current_gen = self.inlined_generation.load(Ordering::Acquire);
        // Cache miss: populate both `batches` and `view` together.
        self.populate_inlined_cache(current_gen).await?;
        Ok((*self.inlined_cache.load().batches).clone())
    }

    /// Return the per-entry inline view, building and caching it on first access
    /// for the current `inlined_generation`.
    ///
    /// Unlike [`Self::read_inlined_batches`], which flattens all entries into a
    /// single `Vec<RecordBatch>`, this returns the full per-entry structure
    /// including the original [`InlinedData`] envelope — enabling the upsert-
    /// rewrite path to reconstruct updated entries without a second metastore
    /// round-trip or IPC re-decode.
    pub(super) async fn cached_inlined_view(&self) -> Result<Arc<Vec<InlinedViewEntry>>> {
        let current_gen = self.inlined_generation.load(Ordering::Acquire);
        {
            let cached = self.inlined_cache.load();
            if cached.generation == current_gen {
                return Ok(Arc::clone(&cached.view));
            }
        }
        self.populate_inlined_cache(current_gen).await?;
        Ok(Arc::clone(&self.inlined_cache.load().view))
    }

    /// Materialize the inline view for `generation` into `inlined_cache`,
    /// choosing the incremental delta path when it is provably correct and the
    /// full rebuild otherwise.
    ///
    /// # Incremental contract (see [`InlinedCache`])
    ///
    /// The cache miss is satisfied one of two ways:
    /// - **append-only delta** — taken iff the currently-cached entry is a real
    ///   (non-sentinel) view whose `structural_epoch` still equals the live
    ///   `inlined_structural_epoch`. That equality proves every change since the
    ///   cached view was built was a pure append (no rewrite, no removal, no
    ///   newly published tombstone, no checkpoint/overwrite/recovery), so the
    ///   already-decoded+filtered entries remain valid and only the entries with
    ///   `sequence_number > materialized_through_sequence` need to be fetched,
    ///   decoded, filtered, and merged on top. This avoids the O(corpus) metastore read +
    ///   IPC re-decode + O(rows × tombstones) re-filter on every scan under
    ///   sustained CDC — the dominant inline read-tax.
    /// - **full rebuild** — the fallback for every other miss: the sentinel/first
    ///   touch, or any structural-epoch mismatch (rewrite, tombstone publish,
    ///   checkpoint clear, overwrite, retention/manual delete, recovery). This is
    ///   the original from-scratch path and is the safety net for anything that
    ///   cannot prove delta-consistency.
    ///
    /// Both paths capture the structural epoch with `Acquire` BEFORE reading the
    /// corpus and stamp the resulting `InlinedCache` with `generation`, so the
    /// generation gate, the `Release`/`Acquire` ordering, and the
    /// `scan_state_lock` publish fence are all unchanged — only HOW the payload
    /// is computed differs. Concurrent misses stay idempotent: two threads for
    /// the same generation read the same corpus and produce identical views; the
    /// last `ArcSwap::store` wins. If a writer bumps the generation between the
    /// caller's load and this store, the stored entry simply misses next read and
    /// is recomputed — no data is lost or corrupted.
    pub(super) async fn populate_inlined_cache(&self, generation: u64) -> Result<()> {
        // cycle-5 TASK 1: if the pending tombstone-delta queue has outgrown its
        // cap, take the bounded over-cap path (full rebuild + queue release). The
        // common per-batch single-tombstone case stays well under the cap and
        // takes the delta path.
        if self.pending_tombstone_deltas.lock().over_cap() {
            return self.rebuild_inlined_cache_over_cap().await;
        }

        // Snapshot the structural epoch first; both the delta-eligibility check
        // and the stamp on the new cache entry use this same value so a racing
        // structural bump can only cause a (safe) miss-and-recompute, never a
        // stale delta accepted under a structural change.
        let structural_epoch = self.inlined_structural_epoch.load(Ordering::Acquire);

        // Delta eligibility: a real cached base at the same structural epoch.
        let cached = self.inlined_cache.load_full();
        let delta_base = (cached.structural_epoch == structural_epoch
            && cached.generation != u64::MAX)
            .then(|| Arc::clone(&cached));

        let new_cache = if let Some(base) = delta_base {
            self.extend_inlined_cache_delta(generation, structural_epoch, &base)
                .await?
        } else {
            self.rebuild_inlined_cache_full(generation, structural_epoch)
                .await?
        };

        // Store the materialized entry. Concurrent misses are safe — last store
        // wins, and a stale store at an older generation only causes a correct
        // miss-and-recompute on the next read (the generation gate). The pending
        // tombstone-delta queue is NOT drained here: re-applying a removal to an
        // already-filtered base entry is an idempotent no-op, so a delta cache is
        // correct regardless of how far the queue has been pruned, and the
        // delta-path removal scan walks only `seq > base.tombstone_delta_seq` (from
        // the ordered queue's back) — O(new deltas), not O(queue). The queue is
        // bounded by `rebuild_inlined_cache_over_cap` and fully cleared at
        // checkpoint/overwrite/recovery.
        self.inlined_cache.store(Arc::new(new_cache));
        Ok(())
    }

    /// Over-cap fallback for [`Self::populate_inlined_cache`] (cycle-5 TASK 1):
    /// the pending tombstone-delta queue has grown past its bound, so full-rebuild
    /// and release the queue.
    ///
    /// # Why this BUMPS the structural epoch (the race-safety crux)
    ///
    /// Draining the queue concurrently with a delta-populate that started while
    /// the queue was still under-cap is unsafe on its own: that delta cache could
    /// store last carrying a `tombstone_delta_seq` BELOW the seq we drain through,
    /// and a future miss extending it would have lost the drained removals. Bumping
    /// the structural epoch closes this: every concurrent delta cache carries the
    /// OLD epoch, so even if one stores last, the next read sees
    /// `cached.structural_epoch != live` and full-rebuilds — never reusing a delta
    /// cache whose base predates the drain. The drain then removes only deltas
    /// at/below the seq captured BEFORE the corpus read (deltas pushed afterward
    /// have higher seqs and are retained), and the corpus read reflects every
    /// tombstone, so the released queue cannot drop a still-needed removal.
    pub(super) async fn rebuild_inlined_cache_over_cap(&self) -> Result<()> {
        // Capture the queue seq to drain through BEFORE the corpus read so a
        // tombstone published during the rebuild (higher seq) is retained.
        let drain_through = self.pending_tombstone_deltas.lock().seq;

        // Bump the structural epoch to invalidate any concurrent delta cache, then
        // read the post-bump generation/epoch to stamp this rebuild — so the
        // stored cache is current (not immediately stale from our own bump).
        self.bump_inlined_structural_epoch();
        let structural_epoch = self.inlined_structural_epoch.load(Ordering::Acquire);
        let generation = self.inlined_generation.load(Ordering::Acquire);

        let new_cache = self
            .rebuild_inlined_cache_full(generation, structural_epoch)
            .await?;

        // Release the baked-in deltas now that the structural bump has fenced off
        // any concurrent delta base.
        self.pending_tombstone_deltas
            .lock()
            .drain_through(drain_through);

        self.inlined_cache.store(Arc::new(new_cache));
        Ok(())
    }

    /// Full rebuild: read the entire `cayenne_inlined_data` corpus, decode each
    /// entry, apply the deletion map, and return the materialized [`InlinedCache`].
    /// This is the fallback path of [`Self::populate_inlined_cache`].
    pub(super) async fn rebuild_inlined_cache_full(
        &self,
        generation: u64,
        structural_epoch: u64,
    ) -> Result<InlinedCache> {
        telemetry::track_cayenne_inline_cache_populate(
            false,
            &[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )],
        );

        // Capture the watermark BEFORE the corpus read so the materialized
        // boundary is the value entries were filtered against. A publish that
        // advances the watermark after this capture only bumps the generation, so
        // its newly visible entry misses on the next read and is delta-fetched
        // (its `sequence_number > materialized_through_sequence`). The view
        // materializes exactly the entries with `seq <= watermark`; the boundary
        // stored on the cache is this same watermark, NOT the corpus max — see the
        // `InlinedCache::materialized_through_sequence` doc for why this keeps the
        // delta gap-free and duplicate-free across a watermark advance.
        let materialized_through_sequence = self.published_inlined_seq.load(Ordering::Acquire);

        // cycle-5 TASK 1: capture the tombstone-delta queue seq BEFORE the corpus
        // read. `load_inlined_deletion_maps` (used below) applies EVERY published +
        // locally-published tombstone, so this rebuild bakes in every removal up to
        // here; stamping with the seq captured before the read is conservative — a
        // tombstone published during the read has a higher seq and is re-applied by
        // the next delta miss (an idempotent no-op if already reflected). This is
        // exactly analogous to `materialized_through_sequence` above.
        let tombstone_delta_seq = self.pending_tombstone_deltas.lock().seq;

        let inlined = self
            .catalog
            .get_inlined_data(&self.table_metadata.table_id)
            .await?;

        let view: Vec<InlinedViewEntry> = if inlined.is_empty() {
            Vec::new()
        } else {
            let inlined_deletions = self.load_inlined_deletion_maps().await?;
            let mut view = Vec::with_capacity(inlined.len());
            for entry in inlined {
                // Entries durably committed but not yet published (sequence
                // strictly greater than the captured watermark) are skipped so an
                // in-flight inline write's freshly committed row stays hidden
                // until its writer publishes the paired file deletion-cache update
                // under `scan_state_lock`.
                if entry.sequence_number > materialized_through_sequence {
                    continue;
                }
                view.push(self.decode_and_filter_inlined_entry(entry, &inlined_deletions)?);
            }
            view
        };

        Ok(Self::assemble_inlined_cache(
            generation,
            structural_epoch,
            materialized_through_sequence,
            tombstone_delta_seq,
            view,
        ))
    }

    /// Incremental delta: reuse the structurally-shared entries of `base`, apply
    /// any tombstone REMOVALS published since it was built (cycle-5 TASK 1), and
    /// append the entries that became eligible since (`sequence_number >
    /// base.materialized_through_sequence`), returning the extended
    /// [`InlinedCache`]. The fast path of [`Self::populate_inlined_cache`].
    ///
    /// # Why reusing the base entries is sound
    ///
    /// `filter_inlined_batch_for_deletions` filters an entry against the inline
    /// tombstone maps AND the file-backed deletion snapshot. Two append-class
    /// mutations bump only the generation (so they reach this path):
    ///
    /// 1. **Pure inline append** (`removed_rows == 0`). Adds new entries above the
    ///    corpus max; cannot retroactively hide a row in a cached entry, so the
    ///    base entries stay correctly filtered. The new entries are fetched +
    ///    filtered here against the CURRENT maps.
    /// 2. **Published inline tombstone** (cycle-5 TASK 1). A tombstone ONLY removes
    ///    rows: it hides the prior inline copy of an upserted PK whose entry
    ///    `sequence_number <= delete_sequence`. Removal can never invalidate a
    ///    *retained* base entry (the same soundness as pruning-under-deletes), so
    ///    rather than full-rebuilding, the publish enqueues the removal in
    ///    `pending_tombstone_deltas` and this path RE-FILTERS the reused base
    ///    entries against just the deltas with `seq > base.tombstone_delta_seq`
    ///    (`removal_above`). The semantics match `filter_inlined_batch_for_deletions`
    ///    exactly (keep iff `entry.sequence_number > delete_sequence`), so the
    ///    delta produces the identical view a full rebuild would — only without the
    ///    O(corpus) re-read. A file deletion added by a file-conflict upsert
    ///    (`removed_rows == 0`, no inline copy ⇒ never matches a cached inline row)
    ///    is the case (1) above and likewise cannot hide a cached row.
    ///
    /// A newly appended entry carries the highest sequence (above any deletion of
    /// the rows it replaces) and is correctly kept.
    pub(super) async fn extend_inlined_cache_delta(
        &self,
        generation: u64,
        structural_epoch: u64,
        base: &InlinedCache,
    ) -> Result<InlinedCache> {
        telemetry::track_cayenne_inline_cache_populate(
            true,
            &[telemetry::KeyValue::new(
                "table",
                self.table_metadata.table_name.clone(),
            )],
        );

        // cycle-5 TASK 1: snapshot the tombstone removals published since `base`
        // was built (those with `seq > base.tombstone_delta_seq`) plus the queue's
        // current seq, under one lock. `removal_map` is the merged removal to apply
        // to the base entries; `new_tombstone_seq` becomes the stored cache's
        // `tombstone_delta_seq`. Walking the ordered queue from the back stops at
        // the base seq, so this is O(new deltas), not O(queue).
        let (removal_map, new_tombstone_seq) = self
            .pending_tombstone_deltas
            .lock()
            .removal_above(base.tombstone_delta_seq);
        let has_tombstone_delta =
            !removal_map.int64_pk.is_empty() || !removal_map.row_keys.is_empty();

        // Capture the new watermark first; the new boundary stored on the cache
        // is exactly this value (the entries materialized below). Capturing
        // before the query keeps the boundary conservative: an entry the query
        // returns but whose sequence is above this watermark stays held and is
        // re-fetched on the next miss.
        let new_watermark = self.published_inlined_seq.load(Ordering::Acquire);

        // Fetch precisely the entries that became eligible since `base` was built:
        // those with `sequence_number > base.materialized_through_sequence`. The
        // base view materialized exactly `seq <= base.materialized_through_sequence`,
        // so this is gap-free (a watermark advance re-fetches a previously held
        // row, whose `seq` is above the old boundary) and duplicate-free
        // (already-materialized rows are excluded by `>`).
        let new_entries = self
            .catalog
            .get_inlined_data_above_sequence(
                &self.table_metadata.table_id,
                base.materialized_through_sequence,
            )
            .await?;

        // Nothing appended AND no tombstone removal ⇒ the visible set is unchanged.
        // Restamp the existing payload under the new generation/epoch/seq without
        // re-deriving the flat batch list or re-reading the deletion maps. (An
        // empty append delta means the watermark cannot have advanced — a watermark
        // advance is always paired with a freshly appended entry above the old
        // boundary.)
        if new_entries.is_empty() && !has_tombstone_delta {
            return Ok(InlinedCache {
                generation,
                structural_epoch,
                materialized_through_sequence: base.materialized_through_sequence,
                tombstone_delta_seq: new_tombstone_seq,
                batches: Arc::clone(&base.batches),
                view: Arc::clone(&base.view),
            });
        }

        // Reuse the base entries, applying the tombstone removal in-place when
        // present. The base batches are already filtered against everything up to
        // `base`; re-filtering against just the new removal map removes exactly the
        // rows the newly published tombstones hide (entries with `sequence_number
        // <= delete_sequence` whose PK is in the removal).
        let mut view: Vec<InlinedViewEntry> = if has_tombstone_delta {
            let mut filtered = Vec::with_capacity(base.view.len());
            for entry in base.view.iter() {
                filtered.push(self.apply_tombstone_removal_to_entry(entry, &removal_map)?);
            }
            filtered
        } else {
            (*base.view).clone()
        };

        if !new_entries.is_empty() {
            // New entries are filtered against the FULL deletion maps (the loaded
            // maps already include the just-published tombstones, so a new entry
            // hidden by one is dropped here too).
            let inlined_deletions = self.load_inlined_deletion_maps().await?;
            for entry in new_entries {
                // A row still above the (possibly just-advanced) watermark stays
                // hidden; it is re-fetched on the next miss once published.
                if entry.sequence_number > new_watermark {
                    continue;
                }
                view.push(self.decode_and_filter_inlined_entry(entry, &inlined_deletions)?);
            }
        }

        Ok(Self::assemble_inlined_cache(
            generation,
            structural_epoch,
            new_watermark,
            new_tombstone_seq,
            view,
        ))
    }

    /// Re-filter an already-materialized base entry's batches against a tombstone
    /// REMOVAL map (cycle-5 TASK 1). The entry's batches were already filtered when
    /// the base view was built; this removes only the rows hidden by tombstones
    /// published since (those with `entry.sequence_number <= delete_sequence` whose
    /// PK is in `removal`). Reuses `filter_inlined_batch_for_deletions` so the keep
    /// predicate is byte-identical to the full-rebuild path.
    pub(super) fn apply_tombstone_removal_to_entry(
        &self,
        entry: &InlinedViewEntry,
        removal: &InlinedDeletionMaps,
    ) -> Result<InlinedViewEntry> {
        let mut filtered_batches = Vec::with_capacity(entry.batches.len());
        for batch in &entry.batches {
            if let Some(filtered) = self.filter_inlined_batch_for_deletions(
                batch.clone(),
                entry.envelope.sequence_number,
                removal,
            )? {
                filtered_batches.push(filtered);
            }
        }
        Ok(InlinedViewEntry {
            batches: filtered_batches,
            envelope: entry.envelope.clone(),
            statistics: Arc::clone(&entry.statistics),
        })
    }

    /// Decode one inline-data entry's IPC blob and apply the deletion-map filter,
    /// returning its per-entry view. Shared by the full-rebuild and delta paths so
    /// the two never diverge in how an entry is materialized.
    pub(super) fn decode_and_filter_inlined_entry(
        &self,
        entry: InlinedData,
        inlined_deletions: &InlinedDeletionMaps,
    ) -> Result<InlinedViewEntry> {
        let entry_batches = deserialize_ipc_to_batch(&entry.data_ipc)
            .map_err(|e| super::Error::Arrow { source: e })?;
        // Pre-filter stats are a conservative superset: tombstone removal can only
        // shrink row ranges, never widen min/max.
        let statistics = Arc::new(super::file_pruning::statistics_from_record_batches(
            &self.table_metadata.schema,
            &entry_batches,
        ));
        let mut filtered_batches = Vec::with_capacity(entry_batches.len());
        for batch in entry_batches {
            if let Some(filtered) = self.filter_inlined_batch_for_deletions(
                batch,
                entry.sequence_number,
                inlined_deletions,
            )? {
                filtered_batches.push(filtered);
            }
        }
        Ok(InlinedViewEntry {
            batches: filtered_batches,
            envelope: entry,
            statistics,
        })
    }

    /// Flatten a per-entry view into the cached [`InlinedCache`] (shared tail of
    /// both the full and delta paths).
    pub(super) fn assemble_inlined_cache(
        generation: u64,
        structural_epoch: u64,
        materialized_through_sequence: i64,
        tombstone_delta_seq: u64,
        view: Vec<InlinedViewEntry>,
    ) -> InlinedCache {
        let batches: Vec<RecordBatch> = view
            .iter()
            .flat_map(|e| e.batches.iter().cloned())
            .collect();
        InlinedCache {
            generation,
            structural_epoch,
            materialized_through_sequence,
            tombstone_delta_seq,
            batches: Arc::new(batches),
            view: Arc::new(view),
        }
    }

    pub(super) async fn load_inlined_deletion_maps(&self) -> Result<InlinedDeletionMaps> {
        if self.pk_deletion_strategy.is_position_based() {
            return Ok(InlinedDeletionMaps::default());
        }

        // Per-tombstone activation gate (Option D). A tombstone is applied ONLY
        // when its own durable `published` flag is true — NEVER from a global
        // watermark or protected-snapshot membership. A staged inline-conflict
        // upsert writes its tombstone with `published = false`; an inline-cache
        // rebuild (which a concurrent same-table inline INSERT can trigger)
        // running before the owning snapshot finalizes therefore does NOT hide
        // the old inline row, so the PK cannot transiently vanish. The owning
        // snapshot's finalize flips this to true under the listing fence, before
        // its replacement rows become discoverable; only the inline checkpoint
        // clears it.
        //
        // The gate is pushed into SQL (`get_published_inlined_deletes` filters
        // `published = 1`), which is exactly equivalent to the previous in-memory
        // `if !delete.published { continue; }` skip, and avoids materialising and
        // shipping the expensive `delete_ipc` blobs of in-flight tombstones only
        // to discard them here.
        //
        // b1★ (cycle-4): a tombstone whose DURABLE `published = 1` flip has been
        // deferred (folded into a later Stage-A) is recorded in
        // `inlined_locally_published` and MUST be applied here too — otherwise the
        // old inline row and its file replacement would both be visible in the
        // inter-batch gap (a transient duplicate). When that set is empty (the
        // common steady state, and always after the deferred flips drain) we keep
        // the fast `published = 1`-only SQL path with ZERO extra cost. When it is
        // non-empty we fetch the full tombstone set once and apply a tombstone iff
        // it is durably published OR locally published. The set holds only the few
        // in-flight deferred ids, so this widening is bounded and transient.
        let locally_published = {
            let guard = self.inlined_locally_published.lock();
            if guard.is_empty() {
                None
            } else {
                Some(guard.clone())
            }
        };

        let inlined_deletes = if let Some(locally_published) = locally_published.as_ref() {
            // Fetch all tombstones (published + the deferred-flip ones) and filter
            // in memory against the durable flag OR the locally-published set.
            let all = self
                .catalog
                .get_inlined_deletes(&self.table_metadata.table_id)
                .await?;
            all.into_iter()
                .filter(|delete| delete.published || locally_published.contains(&delete.inlined_id))
                .collect()
        } else {
            self.catalog
                .get_published_inlined_deletes(&self.table_metadata.table_id)
                .await?
        };

        let mut maps = InlinedDeletionMaps::default();
        for delete in inlined_deletes {
            debug_assert!(
                delete.published
                    || locally_published
                        .as_ref()
                        .is_some_and(|set| set.contains(&delete.inlined_id)),
                "load_inlined_deletion_maps must only apply durably- or locally-published tombstones"
            );
            let row_keys = deserialize_delete_keys_from_ipc(&delete.delete_ipc)
                .map_err(|e| super::Error::Arrow { source: e })?;
            for row_key in row_keys {
                if self.pk_deletion_strategy.is_int64_pk() {
                    let pk = Self::row_key_to_i64(&row_key, &self.table_metadata.table_name)?;
                    maps.int64_pk
                        .entry(pk)
                        .and_modify(|sequence| {
                            *sequence = (*sequence).max(delete.sequence_number);
                        })
                        .or_insert(delete.sequence_number);
                } else {
                    maps.row_keys
                        .entry(row_key)
                        .and_modify(|sequence| *sequence = (*sequence).max(delete.sequence_number))
                        .or_insert(delete.sequence_number);
                }
            }
        }

        Ok(maps)
    }

    /// Closed `[min,max]` over the keys of an in-RAM `Int64Pk` deletion map, or
    /// `None` when empty. The disjoint-skip superset for the in-memory
    /// (inline/mem-tier) tombstones, mirroring `DeletionIndex::deleted_key_range`
    /// for the file-backed snapshot.
    pub(super) fn int64_map_key_range(map: &HashMap<i64, i64>) -> Option<(i64, i64)> {
        let mut iter = map.keys().copied();
        let first = iter.next()?;
        let mut lo = first;
        let mut hi = first;
        for k in iter {
            lo = lo.min(k);
            hi = hi.max(k);
        }
        Some((lo, hi))
    }

    pub(super) fn row_key_to_i64(row_key: &[u8], table_name: &str) -> Result<i64> {
        if row_key.len() != 8 {
            return Err(Error::DataValidation {
                table: table_name.to_string(),
                message: format!(
                    "Invalid inlined Int64 delete key length {}; expected 8 bytes",
                    row_key.len()
                ),
            });
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(row_key);
        Ok(i64::from_be_bytes(bytes))
    }

    pub(super) fn filter_inlined_batch_for_deletions(
        &self,
        batch: RecordBatch,
        data_sequence: i64,
        inlined_deletions: &InlinedDeletionMaps,
    ) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 || self.pk_deletion_strategy.is_position_based() {
            return Ok((batch.num_rows() > 0).then_some(batch));
        }

        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok(Some(batch));
        };

        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => {
                let pk_index = *pk_indices.first().ok_or_else(|| Error::Internal {
                    table: self.table_metadata.table_name.clone(),
                    message: "Int64 PK strategy requires a primary key column".to_string(),
                })?;
                let pk_array = batch
                    .column(pk_index)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| Error::Internal {
                        table: self.table_metadata.table_name.clone(),
                        message: format!(
                            "Expected Int64Array for PK column at index {pk_index}, got {:?}",
                            batch.column(pk_index).data_type()
                        ),
                    })?;
                let deleted_pk = Arc::clone(&deletion_snapshot.load_full().tombstones);

                // [compose-trap / b3 disjoint-skip] Before the O(rows) per-row
                // HashMap/tombstone probe, prove the batch's PK `[min,max]` window
                // is disjoint from EVERY deletion's key range — both the
                // file-backed `deletion_snapshot` range AND the in-RAM
                // `inlined_deletions` range (the in-memory CDC tier feeds its
                // tombstones through here, so its keys must be in the union or a
                // PK-disjoint fresh batch would re-incur the per-row read-tax that
                // b3 sheds one layer up in the exec builders). The min/max is a
                // single vectorized pass (cheaper than the per-row probe it
                // replaces); on a disjoint verdict no scanned PK is deletable, so
                // the batch passes through unfiltered. Only taken when the PK
                // column has no nulls — a null PK must still hit the per-row loop
                // below to surface the existing validation error.
                if pk_array.null_count() == 0
                    && let Some((batch_lo, batch_hi)) =
                        arrow::compute::min(pk_array).zip(arrow::compute::max(pk_array))
                {
                    let file_disjoint = deleted_pk
                        .deleted_key_range()
                        .is_none_or(|(del_lo, del_hi)| batch_hi < del_lo || batch_lo > del_hi);
                    let inline_disjoint = Self::int64_map_key_range(&inlined_deletions.int64_pk)
                        .is_none_or(|(del_lo, del_hi)| batch_hi < del_lo || batch_lo > del_hi);
                    if file_disjoint && inline_disjoint {
                        return Ok(Some(batch));
                    }
                }

                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let pk = pk_array.value(row_index);
                    let max_delete_sequence = deleted_pk
                        .get(pk)
                        .map(|tombstone| tombstone.delete_sequence)
                        .into_iter()
                        .chain(inlined_deletions.int64_pk.get(&pk).copied())
                        .max();
                    keep_mask.push(
                        max_delete_sequence
                            .is_none_or(|delete_sequence| data_sequence > delete_sequence),
                    );
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => {
                let converter = self.build_pk_converter(&pk_indices)?;
                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;
                let deleted_row_keys = Arc::clone(&deletion_snapshot.load_full().tombstones);

                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let row_key = rows.row(row_index);
                    let max_delete_sequence = deleted_row_keys
                        .get(row_key.as_ref())
                        .map(|tombstone| tombstone.delete_sequence)
                        .into_iter()
                        .chain(inlined_deletions.row_keys.get(row_key.as_ref()).copied())
                        .max();
                    keep_mask.push(
                        max_delete_sequence
                            .is_none_or(|delete_sequence| data_sequence > delete_sequence),
                    );
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => unreachable!(
                "Position-based inlined deletion filtering returned before PK handling"
            ),
        }

        if keep_mask.iter().all(|keep| *keep) {
            return Ok(Some(batch));
        }
        if keep_mask.iter().all(|keep| !*keep) {
            return Ok(None);
        }

        let filter = arrow::array::BooleanArray::from(keep_mask);
        Ok(Some(arrow::compute::filter_record_batch(&batch, &filter)?))
    }
}

impl CayenneTableProvider {
    /// Checkpoint: flush all inlined data to a Vortex file and clear from metastore.
    ///
    /// Reads all inlined data entries, concatenates them into a single stream,
    /// writes to Vortex, and clears the inlined data in the metastore.
    ///
    /// Exposed as `#[doc(hidden)] pub` for integration tests that need to
    /// directly trigger a checkpoint and observe the generation bump.
    #[doc(hidden)]
    pub async fn checkpoint_inlined_data(&self) -> Result<u64> {
        let batches = self.read_inlined_batches().await?;
        if batches.is_empty() {
            let stats = self
                .catalog
                .get_inlined_data_stats(&self.table_metadata.table_id)
                .await?;
            self.inlined_row_count
                .store(stats.record_count, Ordering::Relaxed);

            if stats.entry_count > 0 {
                tracing::info!(
                    table = %self.table_metadata.table_name,
                    rows = stats.record_count,
                    segments = stats.entry_count,
                    ipc_bytes = stats.ipc_bytes,
                    "Clearing fully-deleted inline memtable"
                );
                self.clear_inlined_metadata_after_checkpoint().await?;
            }

            return Ok(0);
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // The inline memtable is fully materialized here, so we can size the
        // checkpoint write exactly (sum of in-memory Arrow bytes). This lets the
        // write shard count scale with the actual flush size instead of always
        // fanning out — a small inline flush stays a single file. Computed before
        // `batches` is moved into the MemorySource below.
        let estimated_bytes = Some(
            batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .fold(0u64, u64::saturating_add),
        );

        // Extract Int64 PKs from the batches before they're moved into the
        // `MemorySource` below. After the flush, these PKs live in the new
        // Vortex file at `sequence_number` (assigned later inside the fence).
        // Any pending tombstone with `insert_seq=None` for one of these PKs
        // would otherwise cause `vortex_key_delete_pushdown_filter` to add the
        // PK to its `pk NOT IN (...)` filter and prune the brand-new file.
        let flushed_int64_pks: Vec<i64> = if matches!(
            self.pk_deletion_strategy,
            PkDeletionStrategyWithCache::Int64Pk { .. }
        ) && self.pk_column_indices.len() == 1
        {
            let pk_idx = self.pk_column_indices[0];
            let mut pks: Vec<i64> = Vec::new();
            for batch in &batches {
                if let Some(arr) = batch.column(pk_idx).as_any().downcast_ref::<Int64Array>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            pks.push(arr.value(i));
                        }
                    }
                }
            }
            pks
        } else {
            Vec::new()
        };
        tracing::info!(
            "Checkpointing {} inlined rows ({} batches) for table {}",
            total_rows,
            batches.len(),
            self.table_metadata.table_name,
        );

        // Write inlined data through the normal staging path
        let schema = Arc::clone(&self.table_metadata.schema);
        let mem_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[batches],
            Arc::clone(&schema),
            None,
        )?;

        let ctx = self.create_session_context();
        let stream = datafusion_physical_plan::execute_stream(mem_exec, ctx.task_ctx())?;

        // Hold the listing fence across the visibility flip: for position-based
        // tables the checkpoint writes directly into the current snapshot
        // directory, and for PK tables it publishes a protected snapshot. In
        // both cases, clearing the inline metastore rows must be indivisible
        // with making the Vortex files visible to scans, or a reader can see
        // both copies of the same rows.
        let stats = {
            let _fence = self.listing_fence.write().await;

            let stats = if self.pk_deletion_strategy.is_position_based() {
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
                stats
            } else {
                // Lever B2: the checkpoint flush's new snapshot sequence comes
                // from the same allocator as every other handout for this table.
                let sequence_number = self.reserve_sequences_local(1).await?;
                let (_rows, stats) = self
                    .insert_to_new_snapshot_with_sequence(
                        stream,
                        sequence_number,
                        ctx.state().config().target_partitions(),
                        estimated_bytes,
                    )
                    .await?;
                // Pair every PK now in the new file with `sequence_number` on
                // any pre-existing delete-only tombstone, so listing-time
                // pruning and the runtime deletion filter stop hiding rows
                // from the just-written checkpoint file.
                self.upgrade_tombstones_for_flushed_pks(&flushed_int64_pks, sequence_number)
                    .await?;
                stats
            };

            self.clear_inlined_metadata_after_checkpoint().await?;
            self.refresh_listing_table_under_held_fence().await?;
            // The flush moved every inline row to a file, but the keyset still tags
            // those rows `Inlined`. Without this flip a later upsert will lead to duplicate record.
            self.flip_inlined_keyset_entries_to_file_unlocated();
            stats
        };

        // Persist table stats from the checkpoint write (best-effort; logs on
        // error). The flushed rows were already counted on insert (inline-data
        // commit) — this only moves them from the metastore to Vortex files — so
        // the live count is `Unchanged`; only the min/max/NDV blob re-merges
        // (idempotently).
        self.persist_table_stats(&stats, RowCountUpdate::Unchanged)
            .await;

        Ok(u64::try_from(total_rows).unwrap_or(u64::MAX))
    }

    pub(super) async fn clear_inlined_metadata_after_checkpoint(&self) -> Result<()> {
        self.catalog
            .clear_inlined_data_and_deletes(&self.table_metadata.table_id)
            .await?;
        self.inlined_row_count.store(0, Ordering::Relaxed);
        // b1★ (cycle-4): the catalog `DELETE FROM cayenne_inlined_delete` above
        // removed EVERY tombstone for this table, including any whose durable
        // `published = 1` flip was still deferred (recorded in
        // `inlined_locally_published` / `pending_durable_tombstone_flips`). Their
        // old inline rows were already excluded from the flush above (the flush
        // reads visible rows via `load_inlined_deletion_maps`, which consults the
        // in-memory override under THIS held listing fence), so the tombstones have
        // served their purpose and their rows no longer exist. Clear the in-memory
        // bookkeeping so a future deferred flip cannot target a deleted id and the
        // read filter drops back to its fast `published = 1`-only SQL path.
        self.inlined_locally_published.lock().clear();
        self.pending_durable_tombstone_flips.lock().clear();
        // cycle-5 TASK 1: the corpus is gone, so every pending tombstone removal
        // is moot — drop them all (the `seq` stays monotonic so future deltas
        // stay globally ordered). Runs under the same held listing fence as the
        // structural bump below, so no scan can be mid-delta against the
        // about-to-be-cleared queue.
        self.pending_tombstone_deltas.lock().drain_through(u64::MAX);
        // Invalidate the inlined-batch cache so subsequent scans see the now-empty
        // metastore immediately rather than serving the pre-checkpoint batches.
        // STRUCTURAL: the corpus was cleared (flushed to a Vortex file), so the
        // cached entries are no longer a valid base — the next miss must
        // full-rebuild (which reads the now-empty corpus), never delta-extend.
        self.bump_inlined_structural_epoch();
        Ok(())
    }

    /// Flush the inline level-0 memtable when accumulated entries would make reads or
    /// rewrites too expensive.
    pub(crate) async fn checkpoint_inlined_data_if_memtable_pressure_exceeded(&self) -> Result<()> {
        // Defer while any staged inline-conflict tombstone is unpublished
        // (Option D). A checkpoint flushes inline data to a file WITHOUT applying
        // an inert (`published = false`) tombstone — the read filter skips it —
        // and then clears every tombstone, so running it inside the staged window
        // would write the old inline row to a file AND drop the tombstone,
        // resurfacing the old version once the replacement publishes. Deferring is
        // safe and self-healing: the memtable pressure stays high, the in-flight
        // finalize publishes and decrements the counter within ms, and the next
        // inline insert reschedules this checkpoint. (The user-DELETE checkpoint
        // path — `checkpoint_inlined_data_if_present_for_delete` — needs no such
        // defer not because of locking, but by MUTUAL EXCLUSIVITY: the only
        // deletes that reach it are file-based retention (which requires a
        // `time_retention_filter_builder`, i.e. `has_retention_delete_filters()`,
        // which BLOCKS inline upserts at `mutation_writer.rs`) and position-based
        // deletes (whose tables don't support upserts) — so a staged inline-
        // conflict tombstone can never coexist with that path.)
        if self.pending_inline_tombstones.load(Ordering::Acquire) > 0 {
            tracing::debug!(
                table = %self.table_metadata.table_name,
                "Deferring inline checkpoint: staged inline-conflict tombstone(s) not yet published"
            );
            return Ok(());
        }

        // Fast path: skip the catalog round trip when the cached row count
        // is provably below every memtable-pressure threshold. The pre-fix
        // implementation issued a `get_inlined_data_stats` SQL query on
        // every inline-write commit just to read three integer counters
        // that we already maintain in-process. On network catalogs (Turso,
        // PostgreSQL metastore) each round trip costs 10-50 ms — orders of
        // magnitude more than the rest of the per-row write — and
        // dominated throughput on small-batch CDC ingestion. This is the
        // same shape of fast path the parallel agents added for
        // `clear_staging_dir`, `ensure_no_incomplete_write`, and the
        // compaction trigger.
        //
        // Why the threshold is `inline_flush_max_bytes / inline_max_bytes`
        // (runtime values derived from `DEFAULT_INLINE_FLUSH_MAX_BYTES`,
        // `DEFAULT_INLINE_FLUSH_MAX_SEGMENTS`, `DEFAULT_INLINE_FLUSH_MAX_ROWS`,
        // and `DEFAULT_INLINE_MAX_BYTES`): every `commit_inlined_data_mutation`
        // call from the inline-write path adds at most 1 inline entry, with
        // at most `inline_max_bytes` of IPC payload and at most
        // `inline_max_rows` rows.
        // Cached `inlined_row_count` ≥ number of commits (each commit
        // contributes ≥ 1 row). So:
        //   - commits ≤ cached_rows
        //   - entries  ≤ commits          ≤ cached_rows < INLINE_FLUSH_MAX_SEGMENTS
        //   - bytes    ≤ commits·max_ipc  ≤ cached_rows·max_ipc < INLINE_FLUSH_MAX_BYTES
        // when `cached_rows < INLINE_FLUSH_MAX_BYTES / INLINE_MAX_BYTES`.
        // The bytes bound usually dominates the safe-skip region.
        //
        // For workloads with many small rows per commit (typical CDC: a
        // single row per envelope) this skips the catalog for the entire
        // first few commits. For larger commits (each near `inline_max_bytes`)
        // the safe-skip ends sooner — correctly — because they are closer to
        // the bytes threshold. After the fast path stops, we fall through
        // to the catalog for accurate stats including bytes.
        let cached_rows = self.inlined_row_count.load(Ordering::Relaxed);
        let inline_max_bytes_i64 = i64::try_from(self.context.inline_max_bytes())
            .unwrap_or(i64::MAX)
            .max(1);
        let safe_skip_threshold: i64 =
            (self.context.inline_flush_max_bytes() / inline_max_bytes_i64).max(1);
        if cached_rows < safe_skip_threshold {
            return Ok(());
        }

        let stats = self
            .catalog
            .get_inlined_data_stats(&self.table_metadata.table_id)
            .await?;
        self.inlined_row_count
            .store(stats.record_count, Ordering::Relaxed);

        let Some(pressure) = inline_memtable_pressure_with_thresholds(
            stats,
            self.context.inline_flush_max_rows(),
            self.context.inline_flush_max_segments(),
            self.context.inline_flush_max_bytes(),
        ) else {
            return Ok(());
        };

        tracing::info!(
            table = %self.table_metadata.table_name,
            rows = stats.record_count,
            segments = stats.entry_count,
            ipc_bytes = stats.ipc_bytes,
            reason = pressure.as_str(),
            "Checkpointing inline memtable to Vortex"
        );
        self.checkpoint_inlined_data().await?;
        Ok(())
    }

    /// Flush inlined rows to Vortex files when pending inline data exists.
    ///
    /// Callers must hold `write_lock` while calling this helper.
    ///
    /// Unlike `checkpoint_inlined_data_if_memtable_pressure_exceeded`, this does
    /// NOT defer on `pending_inline_tombstones`, and is safe to do so by MUTUAL
    /// EXCLUSIVITY rather than locking (the staged-tombstone finalize runs
    /// WITHOUT `write_lock`, so the lock is not what protects it): both callers
    /// — the file-based retention delete (gated by `file_based_deletes_preferred`,
    /// which requires a `time_retention_filter_builder`, i.e.
    /// `has_retention_delete_filters()`, which BLOCKS inline upserts in
    /// `mutation_writer::write_all_append`) and the position-based delete (whose
    /// tables don't support upserts) — cannot coexist with a staged inline-
    /// conflict tombstone on the same table.
    pub(super) async fn checkpoint_inlined_data_if_present_for_delete(
        &self,
    ) -> datafusion_common::Result<()> {
        let inlined_count = self.cached_inlined_row_count();

        if inlined_count > 0 {
            self.checkpoint_inlined_data().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to checkpoint inlined data before delete: {e}"
                ))
            })?;
        }

        Ok(())
    }

    pub(super) async fn delete_inlined_rows_matching_filters(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<u64> {
        if self.pk_deletion_strategy.is_position_based() {
            return Ok(0);
        }

        let inlined_data = self
            .catalog
            .get_inlined_data(&self.table_metadata.table_id)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read inlined data for delete on table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;
        if inlined_data.is_empty() {
            return Ok(0);
        }

        let legacy_inlined_deletions = self.load_inlined_deletion_maps().await.map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read inlined delete metadata for delete on table {}: {e}",
                self.table_metadata.table_name
            ))
        })?;

        let coerced_filters = self.coerce_filters_for_inlined_delete(filters)?;
        let physical_filters = self.build_physical_filters_for_inlined_delete(&coerced_filters)?;
        let mut rewrite = InlinedDataRewrite::default();
        let mut matched_deleted_rows = 0_usize;

        for entry in inlined_data {
            let batches = deserialize_ipc_to_batch(&entry.data_ipc)?;
            let mut rewritten_batches = Vec::with_capacity(batches.len());
            let mut original_rows = 0_usize;
            let mut remaining_rows = 0_usize;
            let mut entry_matched_rows = 0_usize;

            for batch in batches {
                original_rows += batch.num_rows();
                let Some(visible_batch) = self
                    .filter_inlined_batch_for_deletions(
                        batch,
                        entry.sequence_number,
                        &legacy_inlined_deletions,
                    )
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to apply inlined delete visibility for table {}: {e}",
                            self.table_metadata.table_name
                        ))
                    })?
                else {
                    continue;
                };

                let filtered_batch =
                    self.apply_inlined_delete_filters(visible_batch.clone(), &physical_filters)?;
                if filtered_batch.num_rows() == 0 {
                    remaining_rows += visible_batch.num_rows();
                    rewritten_batches.push(visible_batch);
                    continue;
                }

                let keys = self.extract_primary_keys_from_batch(&filtered_batch)?;
                let deleted_pk_i64: HashSet<i64> = keys.int64_pk.into_iter().collect();
                let deleted_row_keys: HashSet<Box<[u8]>> = keys.row_keys.into_iter().collect();
                let (filtered_batch, removed_rows) = self
                    .filter_inlined_batch_for_pk_deletions(
                        visible_batch,
                        &deleted_pk_i64,
                        &deleted_row_keys,
                    )
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to rewrite inlined data for delete on table {}: {e}",
                            self.table_metadata.table_name
                        ))
                    })?;
                entry_matched_rows += removed_rows;
                if let Some(batch) = filtered_batch {
                    remaining_rows += batch.num_rows();
                    rewritten_batches.push(batch);
                }
            }

            if entry_matched_rows == 0 {
                continue;
            }

            matched_deleted_rows += entry_matched_rows;
            rewrite.removed_rows += original_rows.saturating_sub(remaining_rows);
            if remaining_rows == 0 {
                rewrite.deleted_inlined_ids.push(entry.inlined_id);
            } else {
                rewrite.updated_data.push(
                    Self::rewritten_inlined_data_entry(&entry, &rewritten_batches, remaining_rows)
                        .map_err(|e| {
                            datafusion_common::DataFusionError::Execution(format!(
                                "Failed to serialize rewritten inlined data for table {}: {e}",
                                self.table_metadata.table_name
                            ))
                        })?,
                );
            }
        }

        if rewrite.is_empty() {
            return Ok(0);
        }

        let deleted_rows = u64::try_from(matched_deleted_rows).map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Inlined delete row count exceeds u64::MAX".to_string(),
            )
        })?;

        // Rewrite-/delete-only: no rows appended, so no sequence is consumed.
        self.commit_inlined_data_mutation(rewrite, vec![], 0, None)
            .await
            .map_err(|err| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to rewrite inlined data for table {}: {err}",
                    self.table_metadata.table_name
                ))
            })?;

        Ok(deleted_rows)
    }

    pub(super) fn coerce_filters_for_inlined_delete(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Vec<Expr>> {
        let df_schema = DFSchema::try_from(self.table_metadata.schema.as_ref().clone())?;
        let mut coerced_filters = Vec::with_capacity(filters.len());

        for filter in filters {
            let mut rewriter = TypeCoercionRewriter::new(&df_schema);
            coerced_filters.push(filter.clone().rewrite(&mut rewriter)?.data);
        }

        Ok(coerced_filters)
    }

    pub(super) fn build_physical_filters_for_inlined_delete(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = DFSchema::try_from(self.table_metadata.schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, &execution_props))
            .collect()
    }

    pub(super) fn apply_inlined_delete_filters(
        &self,
        mut batch: RecordBatch,
        physical_filters: &[Arc<dyn PhysicalExpr>],
    ) -> datafusion_common::Result<RecordBatch> {
        for filter in physical_filters {
            if batch.num_rows() == 0 {
                break;
            }

            let filter_value = filter.evaluate(&batch)?;
            let filter_array = filter_value.into_array(batch.num_rows())?;
            let filter_array = filter_array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Delete filter for table {} did not evaluate to BooleanArray, got {:?}",
                        self.table_metadata.table_name,
                        filter_array.data_type()
                    ))
                })?;

            batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
        }

        Ok(batch)
    }

    pub(super) fn extract_primary_keys_from_batch(
        &self,
        batch: &RecordBatch,
    ) -> datafusion_common::Result<ExtractedPrimaryKeys> {
        let Some(pk_indices) = self
            .primary_key_indices()
            .map_err(datafusion_common::DataFusionError::from)?
        else {
            return Ok(ExtractedPrimaryKeys::default());
        };

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                let pk_index = *pk_indices.first().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Int64 PK strategy requires a primary key column".to_string(),
                    )
                })?;
                let pk_array = batch
                    .column(pk_index)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Expected Int64Array for PK column at index {pk_index}, got {:?}",
                            batch.column(pk_index).data_type()
                        ))
                    })?;
                let mut values = Vec::with_capacity(batch.num_rows());
                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(datafusion_common::DataFusionError::Execution(format!(
                            "Primary key values must be non-null for table {}",
                            self.table_metadata.table_name
                        )));
                    }
                    values.push(pk_array.value(row_index));
                }
                Ok(ExtractedPrimaryKeys {
                    int64_pk: values,
                    row_keys: Vec::new(),
                })
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let converter = self
                    .pk_row_converter
                    .as_ref()
                    .map_or_else(
                        || self.build_pk_converter(&pk_indices).map(Arc::new),
                        |converter| Ok(Arc::clone(converter)),
                    )
                    .map_err(datafusion_common::DataFusionError::from)?;
                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;
                let mut row_keys = Vec::with_capacity(batch.num_rows());
                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(datafusion_common::DataFusionError::Execution(format!(
                            "Primary key values must be non-null for table {}",
                            self.table_metadata.table_name
                        )));
                    }
                    row_keys.push(rows.row(row_index).as_ref().to_vec().into_boxed_slice());
                }
                Ok(ExtractedPrimaryKeys {
                    int64_pk: Vec::new(),
                    row_keys,
                })
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                Ok(ExtractedPrimaryKeys::default())
            }
        }
    }
}
