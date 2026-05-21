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

//! Regression bench for the composite-PK on-conflict cache update in
//! `CayenneTableProvider::apply_on_conflict_deletions`.
//!
//! The production path writes key-based deletion records, takes the owned
//! `Vec<Box<[u8]>>` back from the catalog write result, and extends two
//! immutable cache indexes:
//!
//! - `deleted_row_keys`: clones each key because the same key set is still
//!   needed by the second cache update.
//! - `insert_records`: consumes the owned key vector with `into_iter()`, so
//!   the second update moves keys instead of cloning them again.
//!
//! A future edit that changes the second update back to `.iter().cloned()`
//! pays one extra `Box<[u8]>` allocation per conflict. With ~8 K conflicts
//! and ~32-byte composite keys, that is ~8 K avoidable heap allocations and
//! ~256 KB of redundant key copies per upsert.
//!
//! ## What this bench measures
//!
//! Pure CPU shape. Two lanes per conflict-count:
//!
//! - `regression_clone_both_indices`: clones keys into both cache indexes.
//! - `current_move_second_index`: mirrors the current code by cloning into
//!   the first index and moving the owned keys into the second.
//!
//! Both end up with equivalent cache indexes. The difference is one fewer
//! `Box<[u8]>` clone per conflict in the current path.
//!
//! `cargo bench --bench apply_on_conflict_keys_double_clone -p cayenne`.

use std::hint::black_box;

use cayenne::provider::deletion_index::KeyDeletionIndex;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Conflict counts. 100 is a typical small burst; 1 K is a moderate
/// upsert; 10 K is the high-fan-out scenario that
/// `large_upsert_test.rs:23` documents (`add_insert_records_batch`
/// chunks at 4 params/row → 10 K rows = 40 K SQL params).
const CONFLICT_COUNTS: &[usize] = &[100, 1_000, 10_000];

/// Realistic composite-PK length: e.g. `(i64, Utf8="abcdefghij")` row
/// converter-encoded is ~32 bytes.
const PK_BYTES: usize = 32;
const DELETE_SEQUENCE: i64 = 10;
const INSERT_SEQUENCE: i64 = 11;

fn throughput_elements(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn build_deleted_row_keys(n: usize) -> Vec<Box<[u8]>> {
    (0..n)
        .map(|i| {
            // Mix the index into the bytes so the allocator can't dedup
            // and the touched cachelines differ per element.
            let mut buf = vec![0u8; PK_BYTES];
            let index_bytes = i.to_be_bytes();
            let bytes_to_copy = index_bytes.len().min(PK_BYTES);
            buf[..bytes_to_copy].copy_from_slice(&index_bytes[..bytes_to_copy]);
            buf.into_boxed_slice()
        })
        .collect()
}

fn regression_clone_both_indices(
    written_keys: &[Box<[u8]>],
) -> (KeyDeletionIndex, KeyDeletionIndex) {
    let empty_deleted = KeyDeletionIndex::empty();
    let deleted_row_keys = empty_deleted.extend_max(
        written_keys
            .iter()
            .map(|key| (key.clone(), DELETE_SEQUENCE)),
    );

    let empty_inserts = KeyDeletionIndex::empty();
    let insert_records = empty_inserts.extend_max(
        written_keys
            .iter()
            .map(|key| (key.clone(), INSERT_SEQUENCE)),
    );

    (deleted_row_keys, insert_records)
}

fn current_move_second_index(written_keys: Vec<Box<[u8]>>) -> (KeyDeletionIndex, KeyDeletionIndex) {
    let empty_deleted = KeyDeletionIndex::empty();
    let deleted_row_keys = empty_deleted.extend_max(
        written_keys
            .iter()
            .map(|key| (key.clone(), DELETE_SEQUENCE)),
    );

    let empty_inserts = KeyDeletionIndex::empty();
    let insert_records =
        empty_inserts.extend_max(written_keys.into_iter().map(|key| (key, INSERT_SEQUENCE)));

    (deleted_row_keys, insert_records)
}

fn bench_double_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_on_conflict_keys_double_clone");
    for &n in CONFLICT_COUNTS {
        let written_keys = build_deleted_row_keys(n);
        group.throughput(Throughput::Elements(throughput_elements(n)));

        group.bench_with_input(
            BenchmarkId::new("regression_clone_both_indices", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let pair = regression_clone_both_indices(black_box(&written_keys));
                    black_box(pair);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("current_move_second_index", n),
            &n,
            |b, _| {
                // `iter_batched` hands the measured function an owned vector
                // each iteration so the in-tree move is real while input setup
                // remains outside the measured body.
                b.iter_batched(
                    || written_keys.clone(),
                    |owned| {
                        let pair = current_move_second_index(owned);
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
