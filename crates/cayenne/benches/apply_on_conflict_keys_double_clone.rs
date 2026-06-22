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
//! The production path historically extended TWO immutable cache indexes per
//! conflict batch — `deleted_row_keys` (cloning each key) and
//! `insert_records` (moving the keys) — i.e. two full passes and two maps.
//! The fused tombstone index records the deletion and the re-insertion in a
//! single `extend_max_conflicts` pass over borrowed keys (the index stores
//! XXH3-128 identities, never the key bytes), so the second pass and every
//! per-key `Box<[u8]>` clone disappear.
//!
//! ## What this bench measures
//!
//! Pure CPU shape. Two lanes per conflict-count:
//!
//! - `historical_two_indexes`: the pre-fusion shape — two extend passes
//!   building two indexes from the same key set.
//! - `current_fused_single_pass`: one `extend_max_conflicts` pass building
//!   the fused index.
//!
//! A future edit that re-splits the indexes (or re-introduces per-key key
//! cloning) shows up as the `current` lane regressing toward `historical`.
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

fn historical_two_indexes(written_keys: &[Box<[u8]>]) -> (KeyDeletionIndex, KeyDeletionIndex) {
    let empty_deleted = KeyDeletionIndex::empty();
    let deleted_row_keys =
        empty_deleted.extend_max_deletes(written_keys.iter().map(|key| (key, DELETE_SEQUENCE)));

    let empty_inserts = KeyDeletionIndex::empty();
    let insert_records =
        empty_inserts.extend_max_deletes(written_keys.iter().map(|key| (key, INSERT_SEQUENCE)));

    (deleted_row_keys, insert_records)
}

fn current_fused_single_pass(written_keys: &[Box<[u8]>]) -> KeyDeletionIndex {
    let empty = KeyDeletionIndex::empty();
    empty.extend_max_conflicts(written_keys.iter(), DELETE_SEQUENCE, INSERT_SEQUENCE)
}

fn bench_double_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_on_conflict_keys_double_clone");
    for &n in CONFLICT_COUNTS {
        let written_keys = build_deleted_row_keys(n);
        group.throughput(Throughput::Elements(throughput_elements(n)));

        group.bench_with_input(BenchmarkId::new("historical_two_indexes", n), &n, |b, _| {
            b.iter(|| {
                let pair = historical_two_indexes(black_box(&written_keys));
                black_box(pair);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("current_fused_single_pass", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let index = current_fused_single_pass(black_box(&written_keys));
                    black_box(index);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_double_clone);
criterion_main!(benches);
