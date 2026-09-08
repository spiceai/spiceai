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

//! Snapshot-manifest planning types for the seq-prefix bake.
//!
//! [`SeqPrefixPlan`] partitions a snapshot's manifest by a seq-prefix cutoff
//! into bake-eligible and reference-in-place files; [`ManifestSequenceTag`]
//! describes how each listed file's `[min_sequence, max_sequence]` range is
//! tagged per snapshot kind. The manifest authoring/partitioning logic lives on
//! `CayenneTableProvider`.

use crate::metadata::SnapshotFile;

/// Partition of a snapshot's manifest by a seq-prefix cutoff `T`, the planning
/// core of an incremental seq-prefix compaction.
///
/// A file is **bake-eligible** when `min_sequence <= T`: it holds at least one
/// row committed at or before `T`, so it may carry a deletion with
/// `delete_seq <= T` that the rewrite physically applies — these files are
/// consolidated into one new file with their dead rows removed. A file is
/// **reference-in-place** when `min_sequence > T`: EVERY row it holds was
/// committed strictly after `T`, hence after every applicable `<= T` tombstone
/// (which deletes a row that existed at seq `<= T`; a `> T` row was written
/// after, so under upsert semantics the newer row wins). Such a file can be
/// referenced unchanged in the new snapshot without rewriting its bytes — the
/// shrink the manifest model exists to enable.
///
/// The predicate is `min_sequence`, NOT `max_sequence`: a merged file that
/// STRADDLES the cutoff (`min_sequence <= T < max_sequence`) holds rows at or
/// below `T`, so it MUST be baked. Splitting on `max_sequence` would put a
/// straddling file in `reference`, and the subsequent
/// [`CayenneTableProvider::prune_deletion_index_at_or_below`] would then drop a
/// `<= T` tombstone that still applied to that file's `<= T` rows — resurrecting
/// the deleted rows. For single-commit files `min == max`, so this is a no-op;
/// it only matters for merged/straddling files, which is exactly where the bug
/// would bite.
#[derive(Debug, Default)]
pub(crate) struct SeqPrefixPlan {
    /// Manifest rows whose `min_sequence <= T` — rewritten into one new file.
    pub(crate) bake: Vec<SnapshotFile>,
    /// Manifest rows whose `min_sequence > T` — referenced in place unchanged.
    pub(crate) reference: Vec<SnapshotFile>,
}

/// How `upsert_snapshot_manifest_from_listing` tags each listed file's
/// `[min_sequence, max_sequence]` — the TRUE per-file commit-seq range the
/// seq-prefix bake needs, in place of the prior single per-snapshot watermark
/// (which degenerated `partition_manifest_by_sequence` to all-bake/all-reference
/// and delivered zero shrink).
///
/// The variant is chosen per snapshot kind by the write path, which knows how
/// the snapshot's files were produced:
/// - [`Self::Uniform`] — every file shares one `[min, max]`, overwriting any
///   prior row. Used by a compaction write-site that AUTHORS the snapshot's
///   manifest with its merged `[min, max]` over the inputs (full-rewrite,
///   subset merge).
/// - [`Self::PreserveOrUniform`] — keep any range already recorded for a file
///   (so a re-list never clobbers a compaction-authored merged range — the
///   catalog upsert is `INSERT OR REPLACE`), and tag a brand-new (untagged)
///   file `[min, max]`. Used by `rebuild_live_snapshot_manifests`:
///   - the CURRENT snapshot uses `[0, current_seq]` — a brand-new file there
///     has an unknown true min, so `0` keeps it always bake-eligible (never
///     wrongly referenced-in-place, never resurrecting a row);
///   - a PROTECTED snapshot uses `[S, S]` where `S` is its single reserved
///     sequence (the protected-set value) — every row in a fresh
///     CDC-staged-append / checkpoint snapshot was committed at `S`, so `[S, S]`
///     is exact and lets the newest, highest-sequence files be referenced in
///     place (the shrink the lever exists for). A merged subset-compaction
///     output is a protected snapshot too, but it AUTHORED its merged range at
///     commit time, so the preserve arm keeps that range rather than the
///     (delete-seq) protected-set value.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ManifestSequenceTag {
    Uniform { min: i64, max: i64 },
    PreserveOrUniform { min: i64, max: i64 },
}
