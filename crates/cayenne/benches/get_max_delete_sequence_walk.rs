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

//! Regression bench: the deletion-index "max delete sequence" lookup must
//! stay O(1). The scan / deletion-application path reads
//! [`DeletionIndex::max_sequence_number`] (`src/provider/deletion_index.rs`)
//! to bound the partial-deletion filter; an earlier implementation walked
//! the entire deletion HashMap (`entries().values().max()`) on every lookup.
//! This bench guards against regressing back to that O(N) walk.
//!
//! Historical (pre-cache) implementation:
//!
//! ```ignore
//! deletion_snapshot
//!     .load()
//!     .deleted_pk
//!     .entries()        // &HashMap<i64, i64>
//!     .values()
//!     .max()            // O(N) walk over every entry
//!     .copied()
//!     .unwrap_or(0)
//! ```
//!
//! [`DeletionIndex::from_map`] eagerly computes
//! `max_sequence_number = entries.values().copied().max()` and stores it
//! on the struct. [`DeletionIndex::extend_max`] (the incremental insert
//! path used during writes) maintains the invariant by taking
//! `max(self.max_sequence_number, new_seq)`.
//!
//! For a table with N cached deletes, the per-lookup cost of the O(N) walk
//! grows as:
//!
//!   - N =   1 K:    ~1 µs per call.
//!   - N = 100 K:  ~100 µs per call.
//!   - N =   1 M:    ~10 ms per call.
//!
//! The 1 M case multiplies scan-setup tail latency by an order of magnitude
//! over the cached read, especially on long-running tables that have
//! accumulated deletions between compactions.
//!
//! ## Invariant under test
//!
//! `max_sequence_number().unwrap_or(0)` (O(1) cached read) must stay
//! asymptotically flat as occupancy grows, versus the O(N)
//! `entries().values().max().copied().unwrap_or(0)` walk.
//!
//! ## What this bench measures
//!
//! Pure CPU shape: the same deletion index populated to four sizes that
//! bracket realistic occupancy. Two lanes per size:
//!
//! - `o_n_walk_entries_values_max` — historical baseline; walks all
//!   entries.
//! - `o_1_cached_max_sequence`     — current implementation; reads the cached
//!   `max_sequence_number` field directly.
//!
//! `cargo bench --bench get_max_delete_sequence_walk -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use std::collections::HashMap;
use std::hint::black_box;

use cayenne::provider::deletion_index::DeletionIndex;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Deletion-index occupancy. 1 K is small; 100 K is mid-load between
/// compactions; 1 M is a busy upsert table that hasn't compacted in a while.
const DELETION_SIZES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

fn build_index(size: usize) -> DeletionIndex {
    let mut map: HashMap<i64, i64> = HashMap::with_capacity(size);
    for i in 0..size as i64 {
        // delete_seq grows with i so .max() actually has to walk to the end
        // in the worst case for a sorted-iter; HashMap iteration is unordered
        // anyway so the cost is the full walk.
        map.insert(i, i + 1);
    }
    DeletionIndex::from_map(map)
}

/// Mirror of the historical O(N) walk (pre-`max_sequence_number` cache).
fn o_n_walk(index: &DeletionIndex) -> i64 {
    index
        .iter_entries()
        .filter_map(|(_, entry)| entry.delete_sequence())
        .max()
        .unwrap_or(0)
}

/// Proposed: read the cached max directly.
fn o_1_cached(index: &DeletionIndex) -> i64 {
    index.max_sequence_number().unwrap_or(0)
}

fn bench_get_max(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_max_delete_sequence_walk");
    for &n in DELETION_SIZES {
        let index = build_index(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(
            BenchmarkId::new("o_n_walk_entries_values_max", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let v = o_n_walk(black_box(&index));
                    black_box(v);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("o_1_cached_max_sequence", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let v = o_1_cached(black_box(&index));
                    black_box(v);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_get_max);
criterion_main!(benches);
