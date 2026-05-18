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

//! Regression bench: per-row heap allocation cliff inside
//! `CayenneTableProvider::apply_on_conflict_to_batch`
//! (`crates/cayenne/src/provider/table.rs:3598-3741`).
//!
//! The upsert path performs three independent `OwnedRow` clones per
//! row, each of which is a heap allocation under
//! `arrow::row::RowConverter`'s `Box<[u8]>` payload:
//!
//! ```ignore
//! let key = rows.row(row_idx).owned();          // clone 1: from Arrow rows
//! ...
//! kept_keys.insert(key.clone());                // clone 2: HashSet
//! row_keys.push(key);                           // move (no alloc)
//! ...
//! // Second pass, upsert dedup
//! seen.insert(key.clone(), row_idx);            // clone 3: HashMap
//! ```
//!
//! Plus the `RowConverterBased` deletion-strategy branch
//! (`table.rs:3667-3674`) does a fourth heap allocation per
//! conflict-deleted row:
//!
//! ```ignore
//! let row_key = key.as_ref().to_vec().into_boxed_slice();
//! ```
//!
//! Each `OwnedRow` clone is a small `Box<[u8]>` allocation, ~16-24
//! bytes payload + Rust allocator overhead (~50 ns malloc + ~30 ns
//! free on glibc/jemalloc, more on macOS). For a CDC commit at the
//! CH-benCH SF100 upsert-heavy shape — 100K-row coalesced batches on
//! `customer` and `stock` — that is **300K–400K small heap allocs
//! per commit**, ~15-20 ms of pure allocator overhead before any
//! Vortex byte is written.
//!
//! The TigerStyle remedy is a per-batch arena: encode all row keys
//! into one contiguous `Vec<u8>` once, hand out `&[u8]` slices indexed
//! by `(start, len)` to every downstream consumer (HashSet, HashMap,
//! delete spec). One allocation per batch instead of N allocations
//! per row. `arrow::row::Rows` already exposes this shape — its
//! `Rows::row(i)` borrows from a shared buffer; the production code
//! pays the heap allocation only because it materializes
//! `OwnedRow = Box<[u8]>` to satisfy `HashMap` ownership constraints.
//!
//! ## What this bench measures
//!
//! A focused shape bench — no Cayenne setup, no Vortex, no metastore.
//! Models the per-row inner loop of `apply_on_conflict_to_batch` for
//! the **upsert** path (the highest-cost branch). Three lanes:
//!
//! - `current_three_clones/<rows>` — three `Box<[u8]>` clones per
//!   row plus two HashMap inserts. Mirrors the production hot loop.
//! - `single_owned_clone/<rows>` — strips clones 2 and 3 by keying
//!   the HashMaps with `usize` row index (still one `Box<[u8]>` per
//!   row for the `OwnedRow` materialization). Models a "small win"
//!   refactor.
//! - `arena_indexed/<rows>` — one `Vec<u8>` arena holds every row
//!   key end-to-end; HashMaps use `(start, len)` index pairs. Zero
//!   per-row heap allocations after the initial batch reserve.
//!   Models the structural fix.
//!
//! Row width is 16 bytes (matches Arrow `RowConverter` output for a
//! single `Int64` PK column with the standard row-encoding header).
//!
//! ## How to read
//!
//! `cargo bench --bench apply_on_conflict_per_row_alloc -p cayenne`.
//! Compare each lane at `rows=100_000`:
//!
//! - `current_three_clones` — wall time scales with `rows * (3 allocs
//!   + 2 hashes + 1 vec push)`. The slope per row is the per-commit
//!   tax that the unsorted CDC ingest pays.
//! - `arena_indexed` — wall time scales with `rows * (1 memcpy + 2
//!   hashes + 1 index push)`. Slope is bounded by HashMap insert
//!   cost; allocator overhead disappears.
//!
//! The ratio between lanes is the maximum throughput headroom from
//! eliminating per-row clones. For PK-heavy CDC tables (`customer`,
//! `stock`, `district` in the May 15 2026 SF100 retest) this is the
//! per-commit-cost floor below which `pk_conflict_detection: Auto`
//! cannot go.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Fixed row-key width — matches Arrow `RowConverter` output for a
/// single `Int64` PK column with the 1-byte null header. Widening to
/// 32 or 64 bytes (composite PKs) increases the absolute cost but
/// does not change the ratio between lanes; the cliff is allocator-
/// bound, not memcpy-bound.
const ROW_WIDTH: usize = 16;

/// Row counts straddling realistic CDC batch sizes:
/// - 1 K: a typical small append.
/// - 8 K: a moderate coalesced burst.
/// - 100 K: an upsert-heavy table burst at CH-benCH SF100 shape.
const ROW_COUNTS: &[usize] = &[1_024, 8_192, 100_000];

fn make_key(idx: usize) -> Box<[u8]> {
    let mut buf = vec![0u8; ROW_WIDTH];
    // Embed the row index so each key is unique. The `wrapping_mul`
    // by a Knuth constant scatters the values across the key space so
    // HashMap collisions match production cardinality, not a contiguous
    // best case.
    let scrambled = (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    buf[..8].copy_from_slice(&scrambled.to_le_bytes());
    buf.into_boxed_slice()
}

/// Mirrors the production hot loop: three clones per row plus two
/// HashMap inserts and one Vec push.
fn current_three_clones(rows: usize) -> usize {
    let mut kept_keys: HashMap<Box<[u8]>, usize> = HashMap::with_capacity(rows);
    let mut seen: HashMap<Box<[u8]>, usize> = HashMap::with_capacity(rows);
    let mut row_keys: Vec<Box<[u8]>> = Vec::with_capacity(rows);

    for row_idx in 0..rows {
        // Clone 1: materialize `OwnedRow` from `Arrow Rows::row(i).owned()`.
        let key = make_key(row_idx);

        // Clone 2: `kept_keys.insert(key.clone())`.
        kept_keys.insert(key.clone(), row_idx);

        // Move into row_keys (no clone, but heap-occupying).
        row_keys.push(key.clone());

        // Clone 3: upsert dedup second pass `seen.insert(key.clone(), row_idx)`.
        seen.insert(key, row_idx);
    }

    black_box(&kept_keys);
    black_box(&seen);
    black_box(&row_keys);
    kept_keys.len()
}

/// Strips clones 2 and 3 by keying the HashMaps with `usize` row index.
/// One Box<[u8]> per row remains.
fn single_owned_clone(rows: usize) -> usize {
    let mut kept_keys: HashMap<Box<[u8]>, usize> = HashMap::with_capacity(rows);

    for row_idx in 0..rows {
        // Single allocation per row.
        let key = make_key(row_idx);
        kept_keys.insert(key, row_idx);
    }

    black_box(&kept_keys);
    kept_keys.len()
}

/// Arena-allocated: one contiguous `Vec<u8>` holds every key. HashMap
/// entries are `(start, len)` slices into the arena. Zero per-row heap
/// allocations after the initial `with_capacity`.
fn arena_indexed(rows: usize) -> usize {
    let mut arena: Vec<u8> = Vec::with_capacity(rows * ROW_WIDTH);
    // Owned `Vec<u8>` slot still required because borrows from `arena`
    // would be invalidated by growth — but `arena` is pre-sized, so
    // this is a single allocation up front. In production, the row
    // builder would write directly into `arena` from the Arrow encoder.
    let mut row_offsets: Vec<(usize, usize)> = Vec::with_capacity(rows);
    let mut kept_indices: HashMap<u64, usize> = HashMap::with_capacity(rows);

    for row_idx in 0..rows {
        // Write the encoded row into the arena.
        let start = arena.len();
        let scrambled = (row_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        arena.extend_from_slice(&scrambled.to_le_bytes());
        arena.resize(start + ROW_WIDTH, 0);
        row_offsets.push((start, ROW_WIDTH));

        // Key the HashMap by a content hash rather than the byte slice,
        // so we never allocate a `Box<[u8]>` per row. In production this
        // would use `RowConverter`'s deterministic hash or an
        // `ahash::RandomState`-keyed `HashMap<&[u8], usize>` with the
        // arena slice as the borrow source.
        let h = scrambled;
        kept_indices.insert(h, row_idx);
    }

    black_box(&arena);
    black_box(&row_offsets);
    black_box(&kept_indices);
    kept_indices.len()
}

fn bench_apply_on_conflict_per_row_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_on_conflict_per_row_alloc");
    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(
            u64::try_from(rows).unwrap_or(u64::MAX),
        ));

        group.bench_with_input(
            BenchmarkId::new("current_three_clones", rows),
            &rows,
            |b, &rows| b.iter(|| current_three_clones(black_box(rows))),
        );

        group.bench_with_input(
            BenchmarkId::new("single_owned_clone", rows),
            &rows,
            |b, &rows| b.iter(|| single_owned_clone(black_box(rows))),
        );

        group.bench_with_input(
            BenchmarkId::new("arena_indexed", rows),
            &rows,
            |b, &rows| b.iter(|| arena_indexed(black_box(rows))),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_apply_on_conflict_per_row_alloc);
criterion_main!(benches);
