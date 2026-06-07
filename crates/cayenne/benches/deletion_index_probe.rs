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

//! Micro-benchmarks for the bloom-prefiltered deletion index used by
//! `Int64PkDeletionFilterExec` and `KeyBasedDeletionFilterExec`.
//!
//! These benchmarks isolate the hot loop that runs once per row inside the deletion
//! filter execs and the upsert keyset builder. They cover:
//!
//! 1. **`probe_int64_at_ratios`** — vectorised probe of an `Int64Array` PK column
//!    against a [`DeletionIndex`] at a range of deletion ratios. Demonstrates the
//!    bloom-prefilter win on the common low-deletion-ratio case.
//!
//! 2. **`probe_row_keys_at_ratios`** — same shape for the composite-PK path with a
//!    [`KeyDeletionIndex`] of `RowConverter`-encoded keys.
//!
//! 3. **`concurrent_load_under_publish`** — N reader tasks doing wait-free
//!    [`ArcSwap::load_full`] while a writer publishes new index snapshots in a loop.
//!    Demonstrates that read throughput is not blocked by writer activity.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use arc_swap::ArcSwap;
use arrow::array::Int64Array;
use cayenne::provider::deletion_index::{DeletionIndex, KeyDeletionIndex};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const ROWS_PER_BATCH: usize = 8_192;
const DELETION_RATIOS: [f64; 5] = [0.0, 0.001, 0.01, 0.1, 0.5];

/// Build a `DeletionIndex` of size `total * ratio` covering deterministic PK values.
fn build_int64_index(total: usize, ratio: f64) -> DeletionIndex {
    let count = ((total as f64) * ratio) as usize;
    let mut map = HashMap::with_capacity(count);
    for i in 0..count {
        // Use even PKs for deletions; the probe uses both even and odd, exercising
        // bloom rejection of the odd half.
        map.insert((i as i64) * 2, i as i64);
    }
    DeletionIndex::from_map(map)
}

fn build_int64_pk_column(total: usize) -> Int64Array {
    Int64Array::from((0..total as i64).collect::<Vec<_>>())
}

fn bench_int64_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_int64_probe");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    let pk_array = build_int64_pk_column(ROWS_PER_BATCH);
    let pk_slice = pk_array.values();

    for ratio in DELETION_RATIOS {
        let index = build_int64_index(ROWS_PER_BATCH, ratio);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("ratio={ratio}")),
            &ratio,
            |b, _| {
                b.iter(|| {
                    // Mirror the hot loop in Int64PkDeletionFilterStream::poll_next.
                    let mut keep = 0_usize;
                    for &pk in pk_slice.iter() {
                        let visible = match index.get(pk) {
                            None => true,
                            Some(t) => t
                                .insert_sequence
                                .is_some_and(|ins_seq| ins_seq > t.delete_sequence),
                        };
                        keep += usize::from(visible);
                    }
                    black_box(keep);
                });
            },
        );
    }
    group.finish();
}

fn build_row_key(row_index: usize) -> Box<[u8]> {
    let mut buf = [0_u8; 16];
    buf[..8].copy_from_slice(&((row_index as i64) * 2).to_be_bytes());
    buf[8..].copy_from_slice(&(row_index as i64).to_be_bytes());
    Box::from(buf)
}

/// Build a `KeyDeletionIndex` of `total * ratio` 16-byte keys from the probe keyspace.
fn build_key_index(total: usize, ratio: f64) -> KeyDeletionIndex {
    let count = ((total as f64) * ratio) as usize;
    let mut map: HashMap<Box<[u8]>, i64> = HashMap::with_capacity(count);
    for row_index in 0..count {
        map.insert(build_row_key(row_index), row_index as i64);
    }
    KeyDeletionIndex::from_map(map)
}

fn build_row_keys(total: usize) -> Vec<Box<[u8]>> {
    (0..total).map(build_row_key).collect()
}

fn bench_row_keys_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_row_keys_probe");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    let row_keys = build_row_keys(ROWS_PER_BATCH);

    for ratio in DELETION_RATIOS {
        let index = build_key_index(ROWS_PER_BATCH, ratio);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("ratio={ratio}")),
            &ratio,
            |b, _| {
                b.iter(|| {
                    let mut keep = 0_usize;
                    for key in &row_keys {
                        let visible = match index.get(key.as_ref()) {
                            None => true,
                            Some(t) => t
                                .insert_sequence
                                .is_some_and(|ins_seq| ins_seq > t.delete_sequence),
                        };
                        keep += usize::from(visible);
                    }
                    black_box(keep);
                });
            },
        );
    }
    group.finish();
}

fn bench_concurrent_load_under_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_concurrent_load_under_publish");
    group.throughput(Throughput::Elements(1));

    // Pre-populated index of 100k entries — realistic medium deletion set.
    let initial = build_int64_index(100_000, 1.0);
    let cell = Arc::new(ArcSwap::from_pointee(initial));

    let stop = Arc::new(AtomicBool::new(false));
    // Background publisher: rebuilds and stores a fresh snapshot continuously. This
    // would have blocked readers under the previous Arc<RwLock<Arc<HashMap>>> shape;
    // with ArcSwap it does not.
    let publisher_handle = {
        let cell = Arc::clone(&cell);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut counter = 0_i64;
            while !stop.load(Ordering::Relaxed) {
                let mut map = HashMap::with_capacity(100_000);
                for i in 0..100_000_i64 {
                    map.insert(i * 2, counter);
                }
                cell.store(Arc::new(DeletionIndex::from_map(map)));
                counter = counter.wrapping_add(1);
            }
        })
    };

    group.bench_function("load_full", |b| {
        b.iter(|| {
            let snapshot = cell.load_full();
            black_box(snapshot.len());
        });
    });

    stop.store(true, Ordering::Relaxed);
    publisher_handle.join().expect("publisher thread");
    group.finish();
}

/// Micro-bench that quantifies the per-call cost of `DeletionIndex::extend_max`
/// as the cumulative deletion-cache size grows. This is the exact hot path
/// hit by every PK-aware upsert / delete on a table that accumulates
/// deletion entries.
///
/// A previous revision rebuilt the bloom filter from scratch on every call
/// (iterating ALL existing entries to re-hash). That made per-call work
/// O(N) where N is the cumulative cache size. Across M writes the cost was
/// O(M·N) — quadratic in the cache size, the root cause of the
/// user-reported ~200% ingestion regression.
///
/// The current implementation keeps amortized cost at O(K) per call by:
///   - Tracking `bloom_capacity` and only rebuilding the bloom when entry
///     count crosses `2 * bloom_capacity` (geometric amortization).
///   - Inserting only newly-added keys into a clone of the existing bloom
///     in the common path.
///
/// This bench runs `extend_max` at several pre-populated cache sizes and
/// reports per-call latency. Watch for these signals on regression:
///   - The 10K/100K/1M curves diverging from constant time (returning to
///     O(N)) is the regression returning.
///   - Sudden jumps at `2^k`-boundaries are the (intentional) amortized
///     full-rebuild cost; they should still be much cheaper than the
///     pre-fix worst case.
fn bench_extend_max_at_growing_cache_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_extend_max_growth");
    group.throughput(Throughput::Elements(1));

    // For each pre-populated size, time one extend_max call that adds K=1
    // new key (the common per-row upsert pattern). Cache sizes are picked
    // to span small (typical CDC), medium, and large (long-lived table)
    // workloads.
    for n in [100_usize, 1_000, 10_000, 100_000] {
        let mut seed_map = HashMap::with_capacity(n);
        for i in 0..n {
            seed_map.insert(i as i64, 1_i64);
        }
        let base = DeletionIndex::from_map(seed_map);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                // Always extend with one fresh key past the seeded range, so
                // every iteration takes the Vacant branch. (If we extended
                // with an existing key, the Occupied branch would short-
                // circuit and obscure the new-key bloom-insert work.)
                let next = base.extend_max_deletes([((n as i64) + 1, 2)]);
                black_box(next);
            });
        });
    }

    group.finish();
}

/// Companion bench that quantifies the *opposite* end of the workload:
/// many small extend_max calls in a row from an empty start. This is the
/// "high-rate CDC into a fresh table" pattern that catches the O(N²)
/// cumulative regression — naive iteration time grows quadratically with N
/// if the bloom is rebuilt from scratch on every call, but stays linear
/// (one bloom rebuild per doubling) with the current amortized
/// implementation.
fn bench_extend_max_cumulative_from_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_extend_max_cumulative");

    for total in [128_usize, 1_024, 8_192] {
        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(BenchmarkId::from_parameter(total), &total, |b, &total| {
            b.iter(|| {
                // Re-build from empty on every iteration so the cumulative
                // work is observable; the benchmark reports total time
                // divided by Throughput=total, giving "per-row insert"
                // latency. With the regression (O(N²) cumulative) the per-
                // row number grows linearly with `total`; with the fix it
                // stays roughly flat.
                let mut idx = DeletionIndex::empty();
                for i in 0..total as i64 {
                    idx = idx.extend_max_deletes([(i, 1)]);
                }
                black_box(idx);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_int64_probe,
    bench_row_keys_probe,
    bench_concurrent_load_under_publish,
    bench_extend_max_at_growing_cache_sizes,
    bench_extend_max_cumulative_from_empty,
);
criterion_main!(benches);
