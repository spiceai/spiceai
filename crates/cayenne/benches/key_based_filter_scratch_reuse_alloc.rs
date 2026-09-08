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

//! Before/after bench for per-batch scratch-allocation reuse in
//! `KeyBasedDeletionFilterStream` (`crates/cayenne/src/provider/delete/filter_exec.rs`).
//!
//! CPU profiling of a local CH-benCHmark HTAP run (sf10, directional) showed
//! this stream as the single biggest query-path hotspot (~10.7% of
//! query-stage spiced CPU). Most of that cost is the algorithmically
//! necessary per-row `RowConverter` encode + XXH3-128 hash, but two things
//! were allocated fresh on every single `poll_next` call instead of being
//! reused across the stream's lifetime:
//!
//!   1. `KeyBasedDeletionFilterStream::poll_next`'s `pk_columns`/`deleted`
//!      `Vec`s; `deleted` in particular started at zero capacity, so a batch
//!      with many deletions re-grew it geometrically from scratch every poll.
//!   2. `KeyDeletionIndex::get_batch`'s internal chunk-sweep buffers — two
//!      `BATCH_SWEEP_CHUNK`-sized (~40 KB total) `Vec`s per call.
//!
//! Two lanes, each processing the *same* fixed batch repeatedly (criterion's
//! own iteration repetition stands in for "many `poll_next` calls over a
//! stream's lifetime" — scratch buffers declared outside `b.iter` persist
//! across those repeated calls exactly as stream fields would):
//!
//!   - `fresh_alloc`   — mirrors the pre-fix code: every simulated batch
//!     allocates `pk_columns`/`deleted` from scratch and probes via the
//!     allocating `KeyDeletionIndex::get_batch`.
//!   - `scratch_reuse` — mirrors the fix: `pk_columns`/`deleted`/`hashes`/
//!     `candidates` are declared once outside the timed loop and `clear()`ed
//!     (capacity retained) per simulated batch, probing via
//!     `get_batch_with_scratch`.
//!
//! Both lanes produce a bit-identical filtered `RecordBatch`; the only
//! difference is allocation churn. Four visibility shapes exercise the
//! realistic-steady-state / sparse / mixed-keep / no-keep range, matching
//! `int64_pk_filter_keep_mask_alloc`'s convention (extended with `sparse`).
//! The index is seeded with a realistic ~100K-entry background of unrelated
//! tombstones before each shape's own deletions (see `seed_background_churn`),
//! fed in via chained `extend_max_deletes` calls so it actually freezes into
//! several `RunData` tiers — matching what a table accumulates under
//! sustained merge-on-read churn, not a single small, freshly-fused map. This
//! also keeps `delete_count > 0` so the full bloom-sweep + tier-walk runs for
//! every shape, including `all_visible`, rather than short-circuiting on
//! `get_batch`'s own `delete_count == 0` early return.
//!
//! This bench models a composite Int64 PK (2 columns); string/UUID composite
//! keys cost more to row-encode, which would dilute the allocation-reuse
//! share somewhat. `sparse`/`half_visible`/`none_visible` are reasoned-about
//! brackets for plausible per-batch deletion ratios (steady churn / a
//! churny queue-like table / a stale uncompacted partition), not a ratio
//! read directly off a production trace.
//!
//! Measured result (aarch64, Apple Silicon, two consecutive clean runs after
//! letting the machine settle — a run taken right after a 14-minute release
//! compile was contaminated by that background load per the "one cargo
//! invocation at a time" guidance and is not reported): `scratch_reuse` is
//! **consistently faster across all four shapes**, by roughly **9-10 %** on
//! `all_visible`/`sparse` (where the per-row hash+bloom+tier-walk cost
//! dominates, since little or nothing is filtered out) and **1-4 %** on
//! `half_visible`/`none_visible` (where `arrow::compute::filter_record_batch`,
//! unchanged in both lanes, or the cost of the tier walk actually finding a
//! hit for every row, eats a larger share of the total).
//!
//! `cargo bench --bench key_based_filter_scratch_reuse_alloc -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::provider::deletion_index::KeyDeletionIndex;
use cayenne::row_converter::{RowConverter, SortField};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const ROWS_PER_BATCH: usize = 8_192;
/// Visibility shapes — the second value is the deletion ratio applied to
/// *this probed batch's* PKs. `sparse` (1 %) is the realistic steady-state
/// shape for a table under continuous CDC churn (most rows in most batches
/// are untouched); `half_visible`/`none_visible` bracket a churny queue-like
/// table (e.g. TPC-C `new_order`, inserted and deleted at similar rates) and
/// a stale, not-yet-compacted partition under a bulk retention delete,
/// respectively — extremes, not the median case.
const SHAPES: &[(&str, f64)] = &[
    ("all_visible", 0.0),
    ("sparse", 0.01),
    ("half_visible", 0.5),
    ("none_visible", 1.0),
];

/// Background tombstone volume seeded into the index *before* each shape's
/// batch-specific deletions, so probes exercise the same frozen-run layering
/// a table accumulates under sustained merge-on-read churn — not a single
/// small, freshly-fused map. `KeyDeletionIndex` freezes its hot `active` delta
/// into a new frozen `run` once `active.len()` crosses `DELTA_MERGE_MIN`
/// (16,384 outside `cfg(test)`, which this bench binary is); seeding in
/// `BACKGROUND_CHUNK`-sized calls means each call's `active` fills and
/// freezes independently, so `BACKGROUND_TOMBSTONES` ends up spread across
/// several `RunData` tiers plus a partial active delta — the same shape
/// `KeyDeletionIndex::get_batch`'s tier-walk has to probe in production,
/// with a correspondingly realistically-sized bloom filter.
const BACKGROUND_TOMBSTONES: usize = 100_000;
const BACKGROUND_CHUNK: usize = 20_000;

/// A composite two-column (`shop_id`, `line_id`) PK, matching the shape that
/// routes to `KeyBasedDeletionFilterExec` (composite/non-Int64 PKs).
fn make_batch(rows: usize) -> (RecordBatch, RowConverter) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("shop_id", DataType::Int64, false),
        Field::new("line_id", DataType::Int64, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    let shop_ids: Vec<i64> = (0..rows as i64).map(|i| i % 64).collect();
    let line_ids: Vec<i64> = (0..rows as i64).collect();
    let qty: Vec<i64> = (0..rows as i64).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(shop_ids)),
            Arc::new(Int64Array::from(line_ids)),
            Arc::new(Int64Array::from(qty)),
        ],
    )
    .expect("batch");
    let row_converter = RowConverter::new(vec![
        SortField::new(DataType::Int64),
        SortField::new(DataType::Int64),
    ])
    .expect("row converter");
    (batch, row_converter)
}

/// Seed a realistic, multi-run background of unrelated tombstones (disjoint
/// key space from any probed batch: `shop_id`/`line_id` offset well above the
/// batch's own range) by chaining `extend_max_deletes` in `BACKGROUND_CHUNK`
/// pieces, so `KeyDeletionIndex`'s freeze threshold triggers repeatedly and
/// the result has several frozen `RunData` tiers, not one flat map.
fn seed_background_churn(row_converter: &RowConverter) -> KeyDeletionIndex {
    let mut index = KeyDeletionIndex::empty();
    let mut start = 0_i64;
    let mut seq = 1_i64;
    while (start as usize) < BACKGROUND_TOMBSTONES {
        let end = ((start as usize + BACKGROUND_CHUNK).min(BACKGROUND_TOMBSTONES)) as i64;
        let shop_ids: Vec<i64> = (start..end).map(|i| 10_000_000 + i % 64).collect();
        let line_ids: Vec<i64> = (start..end).map(|i| 10_000_000 + i).collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(shop_ids)),
            Arc::new(Int64Array::from(line_ids)),
        ];
        let rows = row_converter.convert_columns(&columns).expect("rows");
        let additions: Vec<(Box<[u8]>, i64)> = rows
            .iter()
            .map(|row| (Box::from(row.data()), seq))
            .collect();
        index = index.extend_max_deletes(additions);
        seq += 1;
        start = end;
    }
    index
}

/// Extend `background` with the first `(rows * ratio)` PK tuples of the
/// *probed* batch, row-encoded the same way, so this shape's probes hit real
/// hits/misses against realistic encoding on top of the realistic background.
fn build_index(
    background: &KeyDeletionIndex,
    batch: &RecordBatch,
    row_converter: &RowConverter,
    ratio: f64,
) -> KeyDeletionIndex {
    let count = ((ROWS_PER_BATCH as f64) * ratio) as usize;
    let pk_columns = vec![Arc::clone(batch.column(0)), Arc::clone(batch.column(1))];
    let rows = row_converter.convert_columns(&pk_columns).expect("rows");
    let additions: Vec<(Box<[u8]>, i64)> = rows
        .iter()
        .take(count)
        .map(|row| (Box::from(row.data()), i64::MAX))
        .collect();
    background.extend_max_deletes(additions)
}

/// Mirror of the pre-fix `KeyBasedDeletionFilterStream::poll_next`: fresh
/// `pk_columns`/`deleted` `Vec`s and the allocating `get_batch` every call.
fn probe_fresh_alloc(
    batch: &RecordBatch,
    row_converter: &RowConverter,
    tombstones: &KeyDeletionIndex,
) -> RecordBatch {
    let batch_size = batch.num_rows();
    let pk_columns: Vec<ArrayRef> = vec![Arc::clone(batch.column(0)), Arc::clone(batch.column(1))];
    let rows = row_converter.convert_columns(&pk_columns).expect("rows");

    let mut deleted: Vec<usize> = Vec::new();
    tombstones.get_batch(rows.iter(), |i, tombstone| {
        let visible = tombstone
            .insert_sequence
            .is_some_and(|insert_seq| insert_seq > tombstone.delete_sequence);
        if !visible {
            deleted.push(i);
        }
    });

    if deleted.is_empty() {
        return batch.clone();
    }
    if deleted.len() == batch_size {
        return RecordBatch::new_empty(batch.schema());
    }
    let mut keep_mask = BooleanBufferBuilder::new(batch_size);
    keep_mask.append_n(batch_size, true);
    for &i in &deleted {
        keep_mask.set_bit(i, false);
    }
    let filter_array = BooleanArray::new(keep_mask.finish(), None);
    filter_record_batch(batch, &filter_array).expect("filter")
}

/// Per-stream scratch, reused across every simulated `poll_next` call —
/// mirrors the `KeyBasedDeletionFilterStream` fields added by this change.
struct ScratchReuseState {
    pk_columns: Vec<ArrayRef>,
    deleted: Vec<usize>,
    hashes: Vec<u128>,
    candidates: Vec<u32>,
}

impl ScratchReuseState {
    fn new() -> Self {
        Self {
            pk_columns: Vec::new(),
            deleted: Vec::new(),
            hashes: Vec::new(),
            candidates: Vec::new(),
        }
    }
}

/// Mirror of the post-fix `KeyBasedDeletionFilterStream::poll_next`: scratch
/// buffers are cleared (capacity retained), not reallocated, and probing
/// goes through `get_batch_with_scratch`.
fn probe_scratch_reuse(
    state: &mut ScratchReuseState,
    batch: &RecordBatch,
    row_converter: &RowConverter,
    tombstones: &KeyDeletionIndex,
) -> RecordBatch {
    let batch_size = batch.num_rows();
    state.pk_columns.clear();
    state.pk_columns.push(Arc::clone(batch.column(0)));
    state.pk_columns.push(Arc::clone(batch.column(1)));
    let rows = row_converter
        .convert_columns(&state.pk_columns)
        .expect("rows");

    state.deleted.clear();
    state.deleted.reserve(batch_size);
    tombstones.get_batch_with_scratch(
        rows.iter(),
        &mut state.hashes,
        &mut state.candidates,
        |i, tombstone| {
            let visible = tombstone
                .insert_sequence
                .is_some_and(|insert_seq| insert_seq > tombstone.delete_sequence);
            if !visible {
                state.deleted.push(i);
            }
        },
    );

    if state.deleted.is_empty() {
        return batch.clone();
    }
    if state.deleted.len() == batch_size {
        return RecordBatch::new_empty(batch.schema());
    }
    let mut keep_mask = BooleanBufferBuilder::new(batch_size);
    keep_mask.append_n(batch_size, true);
    for &i in &state.deleted {
        keep_mask.set_bit(i, false);
    }
    let filter_array = BooleanArray::new(keep_mask.finish(), None);
    filter_record_batch(batch, &filter_array).expect("filter")
}

fn bench_key_based_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_based_filter_scratch_reuse_alloc");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    let (batch, row_converter) = make_batch(ROWS_PER_BATCH);
    let background = seed_background_churn(&row_converter);

    for (shape_name, ratio) in SHAPES {
        let index = build_index(&background, &batch, &row_converter, *ratio);

        group.bench_with_input(
            BenchmarkId::new("fresh_alloc", shape_name),
            shape_name,
            |b, _| {
                b.iter(|| {
                    let out = probe_fresh_alloc(
                        black_box(&batch),
                        black_box(&row_converter),
                        black_box(&index),
                    );
                    black_box(out);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("scratch_reuse", shape_name),
            shape_name,
            |b, _| {
                // Declared OUTSIDE the timed closure: persists across every
                // repeated `b.iter` call, exactly as `KeyBasedDeletionFilterStream`
                // fields persist across `poll_next` calls over a stream's lifetime.
                let mut state = ScratchReuseState::new();
                b.iter(|| {
                    let out = probe_scratch_reuse(
                        &mut state,
                        black_box(&batch),
                        black_box(&row_converter),
                        black_box(&index),
                    );
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_key_based_filter);
criterion_main!(benches);
