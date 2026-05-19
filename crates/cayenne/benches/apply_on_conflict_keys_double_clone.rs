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

//! Regression bench: redundant key clone in
//! `CayenneTableProvider::apply_on_conflict_deletions`
//! (`src/provider/table.rs:4642-4657`, `RowConverterBased` arm).
//!
//! For composite-PK upserts, the on-conflict commit path builds two owned
//! representations of the conflict-row keys back-to-back:
//!
//! ```ignore
//! // table.rs:4647 — clones the entire Vec<Box<[u8]>>.
//! PkDeletionStrategyWithCache::RowConverterBased { .. } => deleted_row_keys.clone(),
//!
//! // table.rs:4654-4657 — walks the just-cloned Vec and copies each
//! // key into a fresh Vec<u8> for the insert-records side of the commit.
//! let pk_bytes_list_for_insert_records: Vec<Vec<u8>> = row_keys_for_deletion
//!     .iter()
//!     .map(|key| key.as_ref().to_vec())
//!     .collect();
//! ```
//!
//! `deleted_row_keys` is **already owned** at this point — destructured
//! from `OnConflictDeletions` at `table.rs:4568`. The `.clone()` at line
//! 4647 is unnecessary: the original could simply be moved into
//! `row_keys_for_deletion`, paying zero per-element allocation.
//!
//! For a burst of N conflicts that's N redundant `Box<[u8]>` allocations
//! (the cloned vec) before we even start building the
//! `Vec<Vec<u8>>` for the catalog round-trip. With ~8 K conflicts in a
//! batch and ~32-byte composite PKs, this is ~8 K wasted heap allocs +
//! ~256 KB redundantly copied bytes per upsert.
//!
//! ## What this bench measures
//!
//! Pure CPU shape. Two lanes per conflict-count:
//!
//! - `current_clone_then_build` — mirrors today's code path:
//!   `deleted.clone()` → walk to build `Vec<Vec<u8>>`.
//! - `proposed_move_then_build` — mirrors the proposed fix:
//!   build `Vec<Vec<u8>>` from `deleted.iter()` first, then **move**
//!   `deleted` into the spec (no clone).
//!
//! Both end up with the identical pair of owned vectors. The difference
//! is one fewer `Vec<Box<[u8]>>` clone per call.
//!
//! `cargo bench --bench apply_on_conflict_keys_double_clone -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Conflict counts. 100 is a typical small burst; 1 K is a moderate
/// upsert; 10 K is the high-fan-out scenario that
/// `large_upsert_test.rs:23` documents (`add_insert_records_batch`
/// chunks at 4 params/row → 10 K rows = 40 K SQL params).
const CONFLICT_COUNTS: &[usize] = &[100, 1_000, 10_000];

/// Realistic composite-PK length: e.g. `(i64, Utf8="abcdefghij")` row
/// converter-encoded is ~32 bytes.
const PK_BYTES: usize = 32;

fn build_deleted_row_keys(n: usize) -> Vec<Box<[u8]>> {
    (0..n)
        .map(|i| {
            // Mix the index into the bytes so the allocator can't dedup
            // and the touched cachelines differ per element.
            let mut buf = vec![0u8; PK_BYTES];
            buf[0..8].copy_from_slice(&(i as u64).to_be_bytes());
            buf.into_boxed_slice()
        })
        .collect()
}

/// Mirror of `apply_on_conflict_deletions` current code path
/// (`table.rs:4642-4657`, RowConverterBased arm).
fn current_clone_then_build(deleted: &Vec<Box<[u8]>>) -> (Vec<Box<[u8]>>, Vec<Vec<u8>>) {
    let row_keys_for_deletion: Vec<Box<[u8]>> = deleted.clone();
    let pk_bytes_list: Vec<Vec<u8>> = row_keys_for_deletion
        .iter()
        .map(|key| key.as_ref().to_vec())
        .collect();
    (row_keys_for_deletion, pk_bytes_list)
}

/// Proposed: build `pk_bytes_list` from the borrowed slice, then move
/// the original `deleted` into the deletion-spec position. Saves one
/// `Vec<Box<[u8]>>` clone (N `Box<[u8]>` allocations).
fn proposed_move_then_build(deleted: Vec<Box<[u8]>>) -> (Vec<Box<[u8]>>, Vec<Vec<u8>>) {
    let pk_bytes_list: Vec<Vec<u8>> = deleted
        .iter()
        .map(|key| key.as_ref().to_vec())
        .collect();
    // No clone — `deleted` is the caller's owned vector. After this point
    // the borrow above is released so the move is legal.
    (deleted, pk_bytes_list)
}

fn bench_double_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_on_conflict_keys_double_clone");
    for &n in CONFLICT_COUNTS {
        let deleted = build_deleted_row_keys(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("current_clone_then_build", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let pair = current_clone_then_build(black_box(&deleted));
                    black_box(pair);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("proposed_move_then_build", n),
            &n,
            |b, _| {
                // `iter_batched` lets us hand the function an OWNED clone
                // per iteration so the move is real but the input setup
                // is not measured.
                b.iter_batched(
                    || deleted.clone(),
                    |owned| {
                        let pair = proposed_move_then_build(owned);
                        black_box(pair);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_double_clone);
criterion_main!(benches);
