/*
Copyright 2025-2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Decoded-state types for the in-memory inlined-data cache.
//!
//! These are the cached representations of metastore inline-data rows that the
//! provider materializes, filters through the deletion map, and serves from the
//! scan path. The cache-maintenance logic that builds and invalidates them lives
//! on `CayenneTableProvider`.

use crate::metadata::InlinedData;
use arrow::record_batch::RecordBatch;
use datafusion_common::Statistics;
use std::sync::Arc;

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
pub(crate) struct InlinedViewEntry {
    /// Original metastore envelope; provides `inlined_id`, `sequence_number`,
    /// and other fields required to reconstruct a rewrite.
    pub(crate) envelope: InlinedData,
    /// Batches already decoded from IPC and filtered through the deletion map.
    /// Empty when all rows in this entry were removed by the deletion filter.
    pub(crate) batches: Vec<RecordBatch>,
    /// Conservative min/max over the decoded IPC batches (pre-tombstone filter).
    pub(crate) statistics: Arc<Statistics>,
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
pub(crate) struct InlinedCache {
    /// Generation at the time this entry was built.
    pub(crate) generation: u64,
    /// `inlined_structural_epoch` at the time this entry was built. A miss whose
    /// live structural epoch still matches this value proves every change since
    /// was append-only and the entry can be extended with the delta instead of
    /// rebuilt. See the type-level "Incremental maintenance contract".
    pub(crate) structural_epoch: u64,
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
    pub(crate) materialized_through_sequence: i64,
    /// Highest `PendingTombstoneDeltas::seq` whose removal this view has applied
    /// (cycle-5 TASK 1). A published tombstone now enqueues a removal delta and
    /// bumps ONLY the generation (not the structural epoch), so the delta path
    /// applies exactly the deltas with `seq > this` to the structurally-shared
    /// base entries — re-filtering them against just the newly-deleted keys
    /// instead of full-rebuilding from the corpus. A full rebuild stamps this
    /// with the queue's current seq (it captured every tombstone via
    /// `load_inlined_deletion_maps`). `0` for the empty sentinel.
    pub(crate) tombstone_delta_seq: u64,
    /// Flattened `RecordBatch`es across all entries. Each batch shares Arrow
    /// buffer ownership via `Arc`, so cloning the `Vec` is cheap.
    pub(crate) batches: Arc<Vec<RecordBatch>>,
    /// Per-entry view used by the upsert-rewrite path to avoid a second
    /// metastore round-trip and re-decode.
    pub(crate) view: Arc<Vec<InlinedViewEntry>>,
}

/// Outcome of a durable inlined-data commit that has not yet been published to the in-memory caches.
///
/// Returned by [`CayenneTableProvider::commit_inlined_data_durable`] and
/// consumed by [`CayenneTableProvider::publish_inlined_mutation`] under
/// `scan_state_lock.write()`.
pub(crate) struct InlinedDurableCommit {
    /// Number of rows removed by the rewrite (superseded inlined copies).
    pub(crate) removed_rows: i64,
    /// Sequence assigned to newly appended inlined rows, or `None` when the
    /// commit only rewrote/removed existing entries. When `Some`, publishing
    /// advances `published_inlined_seq` to this value to make the appended rows
    /// visible.
    pub(crate) published_seq: Option<i64>,
}
