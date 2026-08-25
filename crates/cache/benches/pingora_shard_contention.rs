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

//! Concurrent contention benchmark for `PingoraBackend`'s 16-shard metadata
//! locking (`crates/cache/src/backend/pingora.rs`) -- the structure a
//! recent correctness fix touched (reading and expiring an entry under one
//! hold of its shard lock), which had no performance coverage:
//! `cache_throughput.rs` benches `SimpleCache`/`LruCache` generically and
//! never exercises this backend. Sweeps a key distribution spread evenly
//! across all 16 shards against one concentrated onto a single shard, to
//! price the contention the sharding exists to avoid.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use cache::{CacheBackend, PingoraBackend, Sizeable};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const OPERATIONS_PER_THREAD: usize = 2_000;
const THREADS: usize = 8;
const NUM_SHARDS: u64 = 16;

// The inner value is never read back -- BenchValue is an opaque cache
// payload here, only its presence/absence is checked -- so the field is
// intentionally write-only.
#[derive(Clone)]
struct BenchValue(#[allow(dead_code)] u64);

impl Sizeable for BenchValue {
    fn get_memory_size(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

fn create_bench_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create benchmark runtime")
}

/// Keys spread evenly across all 16 shards -- the low-contention baseline.
fn spread_key(thread: usize, i: usize) -> u64 {
    (thread * OPERATIONS_PER_THREAD + i) as u64
}

/// Keys that all land on shard 0 (`key % NUM_SHARDS == 0`) -- every
/// thread's metadata access serializes through the same shard lock.
fn skewed_key(thread: usize, i: usize) -> u64 {
    spread_key(thread, i) * NUM_SHARDS
}

fn run_concurrent_mixed(
    backend: Arc<PingoraBackend<BenchValue>>,
    key_fn: fn(usize, usize) -> u64,
) {
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    let threads: Vec<_> = (0..THREADS)
        .map(|thread| {
            let backend = Arc::clone(&backend);
            let handle = handle.clone();
            std::thread::spawn(move || {
                handle.block_on(async {
                    for i in 0..OPERATIONS_PER_THREAD {
                        let key = key_fn(thread, i);
                        backend.insert(key, BenchValue(key)).await;
                        black_box(backend.get(&key).await);
                    }
                });
            })
        })
        .collect();

    for t in threads {
        t.join().expect("thread panicked");
    }
}

fn bench_shard_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("pingora_shard_contention");
    group.throughput(Throughput::Elements(
        (THREADS * OPERATIONS_PER_THREAD) as u64,
    ));
    group.sample_size(10);

    let variants: [(&str, fn(usize, usize) -> u64); 2] = [
        ("spread_across_shards", spread_key),
        ("skewed_single_shard", skewed_key),
    ];

    for (name, key_fn) in variants {
        group.bench_function(BenchmarkId::new("concurrent_mixed", name), |b| {
            b.iter(|| {
                let backend = Arc::new(PingoraBackend::<BenchValue>::with_params(
                    1_000_000,
                    Duration::from_secs(3600),
                ));
                run_concurrent_mixed(backend, key_fn);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_shard_contention);
criterion_main!(benches);
