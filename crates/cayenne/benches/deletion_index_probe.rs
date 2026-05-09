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
    let empty_inserts = DeletionIndex::empty();

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
                            Some(del_seq) => empty_inserts
                                .get(pk)
                                .is_some_and(|ins_seq| ins_seq > del_seq),
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

/// Build a `KeyDeletionIndex` of `total * ratio` 16-byte keys.
fn build_key_index(total: usize, ratio: f64) -> KeyDeletionIndex {
    let count = ((total as f64) * ratio) as usize;
    let mut map: HashMap<Box<[u8]>, i64> = HashMap::with_capacity(count);
    for i in 0..count {
        let mut buf = [0_u8; 16];
        buf[..8].copy_from_slice(&((i as i64) * 2).to_be_bytes());
        buf[8..].copy_from_slice(&(i as i64).to_be_bytes());
        map.insert(buf.into(), i as i64);
    }
    KeyDeletionIndex::from_map(map)
}

fn build_row_keys(total: usize) -> Vec<Box<[u8]>> {
    (0..total)
        .map(|i| {
            let mut buf = [0_u8; 16];
            buf[..8].copy_from_slice(&(i as i64).to_be_bytes());
            buf[8..].copy_from_slice(&(i as i64).to_be_bytes());
            Box::from(buf)
        })
        .collect()
}

fn bench_row_keys_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_row_keys_probe");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    let row_keys = build_row_keys(ROWS_PER_BATCH);
    let empty_inserts = KeyDeletionIndex::empty();

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
                            Some(del_seq) => empty_inserts
                                .get(key.as_ref())
                                .is_some_and(|ins_seq| ins_seq > del_seq),
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

criterion_group!(
    benches,
    bench_int64_probe,
    bench_row_keys_probe,
    bench_concurrent_load_under_publish
);
criterion_main!(benches);
