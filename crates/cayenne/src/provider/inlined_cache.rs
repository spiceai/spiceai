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
use arrow::array::{Array, ArrayData};
use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use datafusion_common::Statistics;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Per-entry decoded view of one metastore inline-data row.
///
/// Pairs the original [`InlinedData`] envelope (needed to build rewrites
/// without a second metastore round-trip) with the pre-decoded,
/// deletion-filtered `RecordBatch`es for that entry.
///
/// Stored in [`InlinedCache::view`] as `Arc<InlinedViewEntry>`, not by value:
/// `batches: Vec<RecordBatch>` is a real per-entry allocation (one `Vec` plus
/// one `RecordBatch` clone per element), so cloning an *entry* is not free the
/// way cloning one already-Arc'd `RecordBatch` is. The append-only inline-cache
/// delta path (`CayenneTableProvider::extend_inlined_cache_delta`) extends the
/// base view on every scan that observes a new write, so entry-level `Clone`
/// cost is paid once per entry per scan — sharing entries via `Arc` turns that
/// into a refcount bump instead.
#[derive(Clone)]
pub(crate) struct InlinedViewEntry {
    /// Original metastore envelope; provides `inlined_id`, `sequence_number`,
    /// and other fields required to reconstruct a rewrite.
    ///
    /// `Arc<InlinedData>`, not by value: the envelope carries the entry's
    /// serialized IPC payload, and the scan path clones an entry per visible
    /// entry per scan (`CayenneTableProvider::pruned_inlined_batches_with_removal`
    /// and `apply_tombstone_removal_to_entry`), so an envelope clone is a
    /// refcount bump instead of a copy of that payload. The metastore keeps its
    /// owned `Vec<u8>` representation; the `Arc` is added here, on cache entry.
    pub(crate) envelope: Arc<InlinedData>,
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
/// (with `Release` ordering) by every inline mutation publication and
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
    ///
    /// `Arc<InlinedViewEntry>` per element, not by value: extending this cache
    /// with new entries (`CayenneTableProvider::extend_inlined_cache_delta`)
    /// clones the outer `Vec`, and an element clone that is itself a refcount
    /// bump is what keeps that an O(entries)-pointers operation instead of
    /// O(entries)-allocations.
    pub(crate) view: Arc<Vec<Arc<InlinedViewEntry>>>,
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

/// Resident Arrow bytes of `batches`, counting each physical allocation once.
///
/// [`RecordBatch::get_array_memory_size`] cannot be used for this. It sums
/// `Buffer::capacity()` once per buffer *reference*, and `capacity()` reports
/// the whole parent allocation however narrow a slice the reference covers. The
/// batches in this cache come out of the IPC reader, which decodes a message
/// body into one allocation and points every column and child buffer at a slice
/// of it — so that sum bills a single allocation once per buffer in the batch
/// and over-reports by roughly the buffer count per row-group. On a table whose
/// entries carry 19 buffers it reported 554 MB for a cache holding 28.7 MB.
///
/// Sliced batches reach the budget and chunking paths too
/// ([`super::streaming::ChunkCap::Bytes`], the mem-tier cap), where the same
/// over-report errs toward smaller chunks and tighter budgets and is therefore
/// left alone. A gauge has no conservative direction: an operator attributing
/// resident memory reads the number as the memory, so it has to be the memory.
///
/// # Dedupe key
///
/// [`arrow::buffer::Buffer::data_ptr`] — the parent allocation's base address —
/// and never `as_ptr()`, which has the slice offset already applied and so hands
/// every slice of one allocation a distinct key, dedupes nothing, and silently
/// reproduces the buggy sum. An address identifies an allocation only while it
/// is alive, which holds here: the caller owns `batches` across the walk.
///
/// Counts buffer capacity only. `get_array_memory_size` also adds a fixed-size
/// term per array and per buffer reference for the Arrow bookkeeping structs,
/// which is a real cost — on a cache of one-row batches it rivals the values
/// themselves — but it is an estimate rather than a measurement, and a gauge
/// that was disbelieved for over-reporting does not get to carry one.
/// `cayenne_inline_cache_batches` beside it is what that cost tracks: bytes far
/// below what the batch count implies is a cache paying per batch rather than
/// per row.
pub(crate) fn resident_bytes(batches: &[RecordBatch]) -> usize {
    // The inline corpus is bounded by the checkpoint thresholds
    // (`INLINE_FLUSH_MAX_ROWS`/`_SEGMENTS`), so this set stays small enough that
    // the default hasher costs nothing worth a dependency.
    let mut seen = HashSet::new();
    let mut total = 0usize;
    for batch in batches {
        for column in batch.columns() {
            add_distinct_allocations(&column.to_data(), &mut seen, &mut total);
        }
    }
    total
}

/// Add every allocation reachable from `data` that `seen` has not already
/// counted, recursing through child arrays.
fn add_distinct_allocations(data: &ArrayData, seen: &mut HashSet<NonZeroUsize>, total: &mut usize) {
    for buffer in data.buffers() {
        add_buffer_once(buffer, seen, total);
    }
    if let Some(nulls) = data.nulls() {
        add_buffer_once(nulls.buffer(), seen, total);
    }
    for child in data.child_data() {
        add_distinct_allocations(child, seen, total);
    }
}

fn add_buffer_once(buffer: &Buffer, seen: &mut HashSet<NonZeroUsize>, total: &mut usize) {
    if seen.insert(buffer.data_ptr().addr()) {
        *total = total.saturating_add(buffer.capacity());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Two columns and eight rows: enough buffers that a per-reference sum and a
    /// per-allocation sum are far apart, small enough to reason about by hand.
    fn fixture() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..8).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("name-{id}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("the fixture batch should build")
    }

    fn per_reference_sum(batches: &[RecordBatch]) -> usize {
        batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(0, usize::saturating_add)
    }

    /// The cache's own shape: one row group per metastore entry, decoded from
    /// IPC, so every column and child buffer is a slice of the one allocation
    /// the message body was read into.
    ///
    /// The bound is the serialized stream itself — the body allocation cannot
    /// exceed the bytes it was read from — which is external to how the size is
    /// computed. `get_array_memory_size` is ~3x past it on this two-column
    /// fixture and ~19x on a real table's schema.
    #[test]
    fn an_ipc_decoded_batch_cannot_exceed_the_bytes_it_was_decoded_from() {
        let batch = fixture();
        let mut ipc = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &batch.schema())
                .expect("the IPC writer should build");
            writer.write(&batch).expect("the batch should serialize");
            writer.finish().expect("the stream should finish");
        }

        let decoded: Vec<RecordBatch> =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&ipc), None)
                .expect("the IPC reader should build")
                .collect::<Result<_, _>>()
                .expect("the stream should decode");

        let reported = resident_bytes(&decoded);
        assert!(
            reported <= ipc.len(),
            "the decoded batch is reported at {reported} B, more than the {} B of IPC it was \
             decoded from — the reader gave every buffer a slice of one body allocation, so the \
             sum is billing that allocation once per buffer",
            ipc.len()
        );
        assert!(
            per_reference_sum(&decoded) > reported,
            "the fixture no longer shares an allocation across its buffers, so this test has \
             stopped covering the over-report it exists for"
        );
    }

    /// Many one-row batches sliced from one parent — what the scan path leaves
    /// in the cache — hold exactly the parent's allocations and nothing more.
    #[test]
    fn slices_of_one_batch_are_counted_once_across_all_of_them() {
        let parent = fixture();
        let slices: Vec<RecordBatch> = (0..parent.num_rows())
            .map(|row| parent.slice(row, 1))
            .collect();

        let parent_bytes = resident_bytes(std::slice::from_ref(&parent));
        let slice_bytes = resident_bytes(&slices);
        assert_eq!(
            slice_bytes,
            parent_bytes,
            "{} slices of one batch are reported at {slice_bytes} B against the parent's \
             {parent_bytes} B; they share the parent's allocations and add none of their own",
            slices.len()
        );
        assert!(
            per_reference_sum(&slices) >= parent_bytes * slices.len(),
            "the per-reference sum should bill the parent once per slice, which is the \
             over-report this counts each allocation once to avoid"
        );
    }

    /// Nothing is shared here, so the count must not fall below the payload the
    /// batch demonstrably holds — a dedupe that over-matched would under-report
    /// and be just as wrong in the other direction.
    #[test]
    fn independently_allocated_columns_are_all_counted() {
        let batch = fixture();
        let reported = resident_bytes(std::slice::from_ref(&batch));

        // 8 i64 values, plus 9 i32 offsets, plus the "name-N" bytes.
        let payload = 8 * 8 + 9 * 4 + 8 * "name-0".len();
        assert!(
            reported >= payload,
            "an unshared batch holding at least {payload} B of values is reported at \
             {reported} B"
        );
        assert!(
            reported <= per_reference_sum(std::slice::from_ref(&batch)),
            "counting each allocation once must never exceed the per-reference sum"
        );
    }

    #[test]
    fn an_empty_cache_is_zero() {
        assert_eq!(resident_bytes(&[]), 0);
    }
}
