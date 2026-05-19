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

//! Regression bench: per-write cost of the unconditional `HashMap` clone in
//! `DeletionIndex::extend_max` and `KeyDeletionIndex::extend_max`
//! (`crates/cayenne/src/provider/deletion_index.rs:159-208` and `:306-358`).
//!
//! Re-validated during scheduled task 019e3cbde0ee (2026-05-18) as the top
//! remaining CDC ingestion performance concern for Cayenne (linear cost on
//! growing deletion caches under PK delete/upsert churn). No other critical
//! perf or correctness issues found in full audit of ingestion/query paths,
//! index/filter builds, optimizer rules, locks, and disk flushes.
//!
//! Every PK-aware CDC write (delete or upsert with a non-empty deletion
//! set) calls `extend_max` to publish a new immutable deletion snapshot.
//! The bloom filter side is amortized to O(K) per call by the doubling
//! capacity heuristic (commit history), but the entry map itself is still
//! cloned in full on every call:
//!
//! ```ignore
//! pub fn extend_max(&self, additions: impl IntoIterator<Item = (i64, i64)>) -> Self {
//!     let mut entries = self.entries.clone();   // <-- O(N) on every call
//!     ...
//! }
//! ```
//!
//! `HashMap::clone()` for a `HashMap<i64, i64>` of N entries:
//! - allocates a fresh bucket vector (~2.5N slots at default load factor)
//! - memcpy-copies every occupied slot (16 bytes of payload + the hash)
//! - rehashes nothing (the clone keeps the same hash seed)
//!
//! At 100K entries that is ~2 MB of allocator traffic per CDC commit. The
//! existing `bench_extend_max_at_growing_cache_sizes`
//! (`deletion_index_probe.rs:218`) measures `extend_max` as a whole — bloom
//! + map clone bundled — so the map-clone slice of the budget is not
//! directly visible. This bench isolates it.
//!
//! The TigerStyle remedy is to store the entry map as
//! `Arc<HashMap<…>>` and use `Arc::make_mut` to copy-on-write only when
//! the writer actually mutates; in practice all `extend_max` calls
//! mutate, but readers (`DeletionIndex::probe`) need only an `Arc::clone`.
//! Combined with persistent / structurally-shared maps (`im::HashMap` or
//! `imbl::HashMap`), the per-write cost drops to O(K log N) instead of
//! O(N), and steady-state CDC writes against a 1 M-entry deletion cache
//! stop scaling with cache size.
//!
//! ## What this bench measures
//!
//! Pure shape — no metastore, no Cayenne setup. Models the **map-clone
//! slice** of `extend_max` at four cache sizes that bracket realistic
//! deletion-cache shapes:
//!
//! - 1 K     entries — a fresh table after the first few deletes.
//! - 10 K    entries — typical operational state.
//! - 100 K   entries — long-lived table that has absorbed many deletes
//!   without a compaction.
//! - 1 M     entries — the upper end before compaction absorbs deletions
//!   into the data files.
//!
//! Two lanes per size:
//!
//! - `int64_map_clone_then_insert/<entries>` — `HashMap<i64, i64>::clone()`
//!   followed by inserting one fresh entry. Mirrors the body of
//!   `DeletionIndex::extend_max`.
//! - `binary_map_clone_then_insert/<entries>` — `HashMap<Box<[u8]>, i64>::clone()`
//!   with 16-byte keys, plus one insert. Mirrors `KeyDeletionIndex::extend_max`,
//!   which also has to clone every `Box<[u8]>` key (an additional heap
//!   allocation per entry, not just memcpy).
//!
//! ## How to read
//!
//! `cargo bench --bench deletion_index_extend_map_clone -p cayenne`.
//!
//! - `int64_map_clone_then_insert/100000` is the per-CDC-commit tax for
//!   the dominant integer-PK case. Multiply by your write rate to get
//!   the allocator-bound floor on PK-deletion throughput.
//! - The ratio `int64_map_clone_then_insert/1000000` divided by
//!   `int64_map_clone_then_insert/1000` shows linear scaling. The fix
//!   should make this ratio approach 1 (i.e. constant time on the
//!   common path).
//! - `binary_map_clone_then_insert` should be ~2-3 × `int64_map_clone_then_insert`
//!   at the same N, because each entry pays one extra `Box<[u8]>` allocation
//!   on top of the memcpy. Composite-PK tables (Utf8 PKs, multi-column PKs)
//!   land on this lane.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Entry counts spanning fresh-table to long-lived-cache shapes.
const ENTRY_COUNTS: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

fn build_int64_map(n: usize) -> HashMap<i64, i64> {
    let mut map = HashMap::with_capacity(n);
    for i in 0..n {
        // Knuth-multiplicative scrambling so HashMap bucket distribution
        // matches realistic collision profiles instead of a contiguous-key
        // best case.
        let scrambled = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        map.insert(scrambled as i64, i as i64);
    }
    map
}

fn build_binary_map(n: usize) -> HashMap<Box<[u8]>, i64> {
    let mut map = HashMap::with_capacity(n);
    for i in 0..n {
        let scrambled = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut buf = vec![0u8; 16];
        buf[..8].copy_from_slice(&scrambled.to_le_bytes());
        buf[8..].copy_from_slice(&(i as u64).to_le_bytes());
        map.insert(buf.into_boxed_slice(), i as i64);
    }
    map
}

fn bench_int64_map_clone_then_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_extend_map_clone_int64");
    for &n in ENTRY_COUNTS {
        let base = build_int64_map(n);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                // Exactly the body of `DeletionIndex::extend_max` for one
                // fresh-key addition: clone the entire entry map, then
                // insert one new entry past the populated range.
                let mut cloned = base.clone();
                cloned.insert((n as i64) + 1, 1);
                black_box(cloned);
            });
        });
    }
    group.finish();
}

fn bench_binary_map_clone_then_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_extend_map_clone_binary");
    for &n in ENTRY_COUNTS {
        let base = build_binary_map(n);
        let fresh_key_template = {
            let mut buf = vec![0u8; 16];
            buf[..8].copy_from_slice(&((n as u64) + 1).to_le_bytes());
            buf.into_boxed_slice()
        };
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Mirrors `KeyDeletionIndex::extend_max` for one fresh-key
                // addition. The clone has to copy every `Box<[u8]>` key
                // — an additional heap allocation per entry on top of the
                // bucket memcpy.
                let mut cloned = base.clone();
                cloned.insert(fresh_key_template.clone(), 1);
                black_box(cloned);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_int64_map_clone_then_insert,
    bench_binary_map_clone_then_insert,
    bench_cdc_deletion_churn,
);
criterion_main!(benches);

/// Real-work churn benchmark: measures the cost of a batch of extend_max-style
/// operations (the core of every PK-aware CDC delete or upsert-tombstone write)
/// when the live deletion cache has already grown to various sizes.
///
/// This is the primary "before" measurement for the recurring Cayenne CDC
/// validation task. It directly executes the expensive `HashMap::clone()` +
/// insert that `DeletionIndex::extend_max` (and the Key variant) perform on
/// every such write. As the starting cache size grows (10 K → 1 M entries),
/// the per-batch time increases linearly — this is the visible "poor
/// performance with existing code" for any long-lived table under sustained
/// delete/upsert CDC load.
///
/// A follow-up replacing the owned HashMap with a persistent or COW structure
/// (imbl, rpds, or Arc<HashMap> + make_mut with private writer copy) would
/// make the cost O(K log N) or constant and flatten these lines. The existing
/// `vs_duckdb_delete` / `vs_duckdb_upsert` infrastructure can then be extended
/// with a true end-to-end "churn under compaction" variant that drives many
/// small PK deletes + appends on both engines while the deletion set grows,
/// giving a head-to-head wall-time comparison (Cayenne vector + index probe vs
/// DuckDB block rewrite).
fn bench_cdc_deletion_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_deletion_index_churn");
    // We perform BATCH_SIZE real extend-style clones per iteration.
    // Throughput is reported in "extends" so the plot shows cost per logical
    // CDC delete operation as the cache grows.
    const BATCH_SIZE: u64 = 256;

    group.throughput(Throughput::Elements(BATCH_SIZE));

    for &starting_n in &[10_000usize, 100_000, 1_000_000] {
        let base = build_int64_map(starting_n);
        let fresh_base = starting_n as i64;

        group.bench_with_input(
            BenchmarkId::new("int64", starting_n),
            &starting_n,
            |b, _| {
                b.iter(|| {
                    let mut map = base.clone();
                    for i in 0..BATCH_SIZE {
                        // Mirrors the Occupied/Vacant + insert path in extend_max
                        map.insert(fresh_base + i as i64, 1);
                    }
                    black_box(map)
                });
            },
        );
    }
    group.finish();
}
