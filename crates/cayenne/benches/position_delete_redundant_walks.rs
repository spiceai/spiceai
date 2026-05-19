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

//! Regression bench: redundant data movement in the position-based deletion
//! commit hot path.
//!
//! For each touched data file in `FileBasedDeletionSink::commit_deletions`
//! (`src/provider/delete/sink/position_based.rs:671-697`), the current code
//! does four full passes over the position set:
//!
//! 1. `let mut combined_ids: Vec<u64> = existing_deletion.iter().map(u64::from).collect()`
//!    (`position_based.rs:671-673`) — first walk: existing bitmap → `Vec<u64>`.
//! 2. `combined_ids.sort_unstable(); combined_ids.dedup();`
//!    (`position_based.rs:675-676`) — sort + dedup. Already-monotone bitmap
//!    iteration means dedup is a no-op walk; the sort is `O((K+N) log (K+N))`
//!    where K is the existing bitmap size and N is the new ids.
//! 3. `DeletionVectorWriter::write` (`src/provider/delete/vector_io.rs:227-228`)
//!    re-runs `row_ids.sort_unstable(); row_ids.dedup();` on the same vec —
//!    pure redundant work because step 2 already left it sorted+deduped.
//! 4. `build_position_based_batch` (`vector_io.rs:467-480`) does
//!    `UInt64Array::from(row_ids.to_vec())` — a third full copy of the same
//!    vec to materialise the Arrow column.
//!
//! Separately, `position_based.rs:685-693` rebuilds a fresh `RoaringBitmap`
//! by walking the existing bitmap again (`deletion_vector.to_bitmap()`) and
//! then extending with the new ids — a fifth O(K) walk that is logically
//! equivalent to taking the union of the existing and new sets.
//!
//! For a single touched file with K = 1 M previously-deleted positions and
//! N = 16 K newly-deleted positions:
//!
//!   - Walk 1 (`map(u64::from).collect()`):    ~1 M `i64`s touched.
//!   - Sort 1 (sort_unstable on 1.016 M):       N log N ≈ ~20 ms.
//!   - Sort 2 (the redundant re-sort):          another ~20 ms.
//!   - to_vec (UInt64Array::from):              full copy.
//!   - Bitmap rebuild walk:                     full O(K) clone.
//!
//! ~40-60 ms wasted CPU per commit per touched file at 1 M existing
//! deletions, dominated by the two redundant sorts.
//!
//! The natural fix is two-track:
//!
//! 1. Mark the `DeletionVectorWriteSpec::PositionBased` payload as
//!    pre-sorted (or accept the `RoaringBitmap` directly), letting the
//!    writer skip the re-sort/re-dedup and stream `UInt64Array` directly
//!    from the bitmap iterator.
//! 2. Build `combined_ids` and `updated_bitmap` from a single pass over
//!    `(existing ∪ new)` using `RoaringBitmap::union`-style fusion, not two
//!    `to_bitmap()` / `to_vec()` clones in sequence.
//!
//! This bench measures the two-sort overhead in isolation against the
//! single-sort baseline so the wall-time savings of the fix are visible
//! before any disk IO is involved.
//!
//! `cargo bench --bench position_delete_redundant_walks -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use roaring::RoaringBitmap;

/// Existing-deletion sizes. 1 M is the "wide compactable file" upper bound;
/// 100 K is a typical mid-tier file; 10 K is a small file under early
/// compaction. The 1 M case is where the two redundant sorts dominate.
const EXISTING_DELETIONS: &[usize] = &[10_000, 100_000, 1_000_000];
/// Newly-deleted positions in one commit. 16 K is one DataFusion batch.
const NEW_DELETIONS: usize = 16_000;

fn build_existing_bitmap(size: usize) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    for i in 0..size {
        bitmap.insert(i as u32);
    }
    bitmap
}

/// Builds the new-row-ids vector the same way the production caller does
/// (a contiguous `Vec<u64>`), with new ids that don't overlap with the
/// existing bitmap so the dedup pass cannot accidentally short-circuit.
fn build_new_row_ids(existing_size: usize, new_count: usize) -> Vec<u64> {
    (existing_size..existing_size + new_count)
        .map(|i| i as u64)
        .collect()
}

/// Mirrors `position_based.rs:671-697` AND `vector_io.rs:227-228` — the
/// two-sort hot path on a single touched file.
fn current_two_sort_path(existing: &RoaringBitmap, new_row_ids: &[u64]) -> (Vec<u64>, RoaringBitmap)
{
    // position_based.rs:671-676 — first walk + first sort/dedup.
    let mut combined_ids: Vec<u64> = existing.iter().map(u64::from).collect();
    combined_ids.extend(new_row_ids.iter().copied());
    combined_ids.sort_unstable();
    combined_ids.dedup();

    // position_based.rs:685-693 — second walk to rebuild the cache bitmap.
    let mut updated_bitmap = existing.clone();
    updated_bitmap.extend(
        new_row_ids
            .iter()
            .filter_map(|&id| u32::try_from(id).ok()),
    );

    // vector_io.rs:227-228 — second sort/dedup on the already-sorted vec.
    let mut spec_ids = combined_ids;
    spec_ids.sort_unstable();
    spec_ids.dedup();

    (spec_ids, updated_bitmap)
}

/// Mirrors the proposed fix: build `updated_bitmap` first (the union of
/// the existing bitmap and the new positions), emit the writer-bound
/// `Vec<u64>` directly from the bitmap's monotone iterator, and skip the
/// second sort/dedup entirely.
fn proposed_single_walk_path(
    existing: &RoaringBitmap,
    new_row_ids: &[u64],
) -> (Vec<u64>, RoaringBitmap) {
    let mut updated_bitmap = existing.clone();
    updated_bitmap.extend(
        new_row_ids
            .iter()
            .filter_map(|&id| u32::try_from(id).ok()),
    );
    let spec_ids: Vec<u64> = updated_bitmap.iter().map(u64::from).collect();
    (spec_ids, updated_bitmap)
}

fn deletion_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::UInt64, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
}

/// Mirrors `vector_io.rs:467-480` — the redundant `to_vec()` plus
/// timestamp-column fan-out. Both lanes pay this; isolating it would
/// muddle the redundant-sort signal, so the bench includes it on both
/// sides for parity.
fn build_position_batch(row_ids: &[u64]) -> RecordBatch {
    let schema = deletion_schema();
    let now = 1_700_000_000_000_000_i64;
    let row_id_array = UInt64Array::from(row_ids.to_vec());
    let deleted_at_array = Int64Array::from(vec![now; row_ids.len()]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(row_id_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
        ],
    )
    .expect("batch")
}

fn bench_commit_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("position_delete_redundant_walks");
    for &existing_size in EXISTING_DELETIONS {
        let existing = build_existing_bitmap(existing_size);
        let new_row_ids = build_new_row_ids(existing_size, NEW_DELETIONS);
        let throughput_elems = existing_size + NEW_DELETIONS;
        group.throughput(Throughput::Elements(throughput_elems as u64));

        group.bench_with_input(
            BenchmarkId::new("current_two_sort", existing_size),
            &existing_size,
            |b, _| {
                b.iter(|| {
                    let (ids, bitmap) =
                        current_two_sort_path(black_box(&existing), black_box(&new_row_ids));
                    let batch = build_position_batch(&ids);
                    black_box((bitmap, batch));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("proposed_single_walk", existing_size),
            &existing_size,
            |b, _| {
                b.iter(|| {
                    let (ids, bitmap) =
                        proposed_single_walk_path(black_box(&existing), black_box(&new_row_ids));
                    let batch = build_position_batch(&ids);
                    black_box((bitmap, batch));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_commit_paths);
criterion_main!(benches);
