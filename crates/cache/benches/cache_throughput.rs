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

#![allow(clippy::expect_used)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unit_arg)]

use cache::{
    AsTableRefs, CacheMetrics, CacheProvider, EvictionReason, HashBuilder, LruCache, SimpleCache,
    Sizeable, get_hash_builder,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::sql::TableReference;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use spicepod::component::caching::{CacheEngine, CachingPolicy, HashingAlgorithm};
use std::collections::HashSet;
use std::hash::Hasher;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const CACHE_WEIGHT: u64 = 100_000;
const KEY_SPACE: u64 = 100_000;
const OPERATIONS_PER_THREAD: usize = 10_000;

/// Bytes per cached value, from [`random_value`]. Used to size a cache in
/// entries rather than in weight.
const VALUE_BYTES: u64 = 32;

/// Distinct keys the contention benchmark touches.
const HOT_WORKING_SET: u64 = 20_000;

/// Capacity for 80% of the working set, so the cache runs full and evicting
/// while most reads still hit.
const HOT_CACHE_WEIGHT: u64 = HOT_WORKING_SET * VALUE_BYTES * 8 / 10;

/// Creates a runtime that can be shared across benchmark worker threads.
fn create_bench_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create benchmark runtime")
}

// Wrapper type for benchmarking LruCache
#[derive(Clone)]
struct BenchValue(String);

impl Sizeable for BenchValue {
    fn get_memory_size(&self) -> usize {
        self.0.len()
    }
}

impl CacheMetrics for BenchValue {
    fn record_hit() {}
    fn record_miss() {}
    fn record_request() {}
    fn record_item_count(_count: u64) {}
    fn record_size(_size: u64) {}
    fn record_max_size(_size: u64) {}
    fn record_eviction(_reason: EvictionReason) {}
    fn record_stale_rejection() {}
    fn update_hit_ratio(_hits: u64, _total: u64) {}
    fn publish_counters_at_zero() {}
}

impl AsTableRefs for BenchValue {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::new(HashSet::new())
    }
}

// Get all hash algorithms to benchmark
fn all_hash_algorithms() -> Vec<(&'static str, HashingAlgorithm)> {
    vec![
        ("siphash", HashingAlgorithm::Siphash),
        ("ahash", HashingAlgorithm::Ahash),
        ("xxh3", HashingAlgorithm::XXH3),
        ("xxh64", HashingAlgorithm::XXH64),
    ]
}

/// Engine and policy combinations to benchmark.
///
/// Not a cartesian product: Pingora has no `TinyLFU` admission and builds an
/// LRU instead, so a `pingora_tinylfu` arm would duplicate `pingora_lru`.
///
/// The Pingora arm is gated because with the feature off `LruCache::new` falls
/// back to Moka, which would publish Moka results labelled `pingora`.
fn all_engine_policy_pairs() -> Vec<(&'static str, CacheEngine, CachingPolicy)> {
    #[cfg_attr(
        not(feature = "pingora"),
        expect(unused_mut, reason = "only the pingora arm below pushes")
    )]
    let mut pairs = vec![
        ("moka_lru", CacheEngine::Moka, CachingPolicy::Lru),
        ("moka_tinylfu", CacheEngine::Moka, CachingPolicy::TinyLfu),
    ];
    #[cfg(feature = "pingora")]
    pairs.push(("pingora_lru", CacheEngine::Pingora, CachingPolicy::Lru));
    pairs
}

fn random_value(rng: &mut StdRng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

fn bench_simple_cache_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_cache_concurrent_get");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");

    for thread_count in [1, 4, 8, 16] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                let hash_builder = hash_builder.clone();
                b.iter_batched(
                    || {
                        let cache: Arc<
                            SimpleCache<String, HashBuilder, Box<dyn Hasher + Send + Sync>>,
                        > = Arc::new(SimpleCache::new(
                            CACHE_WEIGHT,
                            Duration::from_mins(1),
                            hash_builder.clone(),
                        ));
                        let mut rng = StdRng::seed_from_u64(42);
                        handle.block_on(async {
                            for i in 0..5000 {
                                let key = (i as u64 * 17) % KEY_SPACE;
                                let value = random_value(&mut rng);
                                cache.put_raw_key(&key, value).await;
                            }
                        });
                        cache
                    },
                    |cache| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let cache = Arc::clone(&cache);
                                let handle = handle.clone();
                                std::thread::spawn(move || {
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    handle.block_on(async {
                                        for _ in 0..OPERATIONS_PER_THREAD {
                                            let key = rng.random_range(0..KEY_SPACE);
                                            black_box(cache.get_raw_key(&key).await);
                                        }
                                    });
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.join().expect("thread panicked");
                        }
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_simple_cache_concurrent_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_cache_concurrent_put");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");

    for thread_count in [1, 4, 8, 16] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                let hash_builder = hash_builder.clone();
                b.iter_batched(
                    || {
                        Arc::new(SimpleCache::<
                            String,
                            HashBuilder,
                            Box<dyn Hasher + Send + Sync>,
                        >::new(
                            CACHE_WEIGHT, Duration::from_mins(1), hash_builder.clone()
                        ))
                    },
                    |cache| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let cache = Arc::clone(&cache);
                                let handle = handle.clone();
                                std::thread::spawn(move || {
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    handle.block_on(async {
                                        for _ in 0..OPERATIONS_PER_THREAD {
                                            let key = rng.random_range(0..KEY_SPACE);
                                            let value = random_value(&mut rng);
                                            black_box(cache.put_raw_key(&key, value).await);
                                        }
                                    });
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.join().expect("thread panicked");
                        }
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_simple_cache_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_cache_concurrent_mixed_80_20");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");

    for thread_count in [1, 4, 8, 16] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                let hash_builder = hash_builder.clone();
                b.iter_batched(
                    || {
                        let cache: Arc<
                            SimpleCache<String, HashBuilder, Box<dyn Hasher + Send + Sync>>,
                        > = Arc::new(SimpleCache::new(
                            CACHE_WEIGHT,
                            Duration::from_mins(1),
                            hash_builder.clone(),
                        ));
                        let mut rng = StdRng::seed_from_u64(42);
                        handle.block_on(async {
                            for i in 0..5000 {
                                let key = (i as u64 * 17) % KEY_SPACE;
                                let value = random_value(&mut rng);
                                cache.put_raw_key(&key, value).await;
                            }
                        });
                        cache
                    },
                    |cache| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let cache = Arc::clone(&cache);
                                let handle = handle.clone();
                                std::thread::spawn(move || {
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    handle.block_on(async {
                                        for _ in 0..OPERATIONS_PER_THREAD {
                                            let key = rng.random_range(0..KEY_SPACE);
                                            if rng.random_bool(0.8) {
                                                black_box(cache.get_raw_key(&key).await);
                                            } else {
                                                let value = random_value(&mut rng);
                                                black_box(cache.put_raw_key(&key, value).await);
                                            }
                                        }
                                    });
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.join().expect("thread panicked");
                        }
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_lru_cache_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_cache_concurrent_get");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    // Benchmark all combinations of caching policy and hash algorithm
    for (pair_name, engine, policy) in all_engine_policy_pairs() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{pair_name}_{hash_name}_{thread_count}threads");

                group.bench_with_input(
                    BenchmarkId::from_parameter(&bench_name),
                    &thread_count,
                    |b, &threads| {
                        let hash_builder = hash_builder.clone();
                        b.iter_batched(
                            || {
                                let cache: Arc<
                                    LruCache<
                                        BenchValue,
                                        HashBuilder,
                                        Box<dyn Hasher + Send + Sync>,
                                    >,
                                > = Arc::new(LruCache::new(
                                    CACHE_WEIGHT,
                                    Duration::from_mins(1),
                                    hash_builder.clone(),
                                    policy,
                                    engine,
                                ));
                                let mut rng = StdRng::seed_from_u64(42);
                                handle.block_on(async {
                                    for i in 0..5000 {
                                        let key = (i as u64 * 17) % KEY_SPACE;
                                        let value = BenchValue(random_value(&mut rng));
                                        cache.put_raw_key(&key, value).await;
                                    }
                                });
                                cache
                            },
                            |cache| {
                                let handles: Vec<_> = (0..threads)
                                    .map(|thread_id| {
                                        let cache = Arc::clone(&cache);
                                        let handle = handle.clone();
                                        std::thread::spawn(move || {
                                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                            handle.block_on(async {
                                                for _ in 0..OPERATIONS_PER_THREAD {
                                                    let key = rng.random_range(0..KEY_SPACE);
                                                    black_box(cache.get_raw_key(&key).await);
                                                }
                                            });
                                        })
                                    })
                                    .collect();
                                for handle in handles {
                                    handle.join().expect("thread panicked");
                                }
                            },
                            criterion::BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

fn bench_lru_cache_concurrent_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_cache_concurrent_put");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    // Benchmark all combinations of caching policy and hash algorithm
    for (pair_name, engine, policy) in all_engine_policy_pairs() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{pair_name}_{hash_name}_{thread_count}threads");

                group.bench_with_input(
                    BenchmarkId::from_parameter(&bench_name),
                    &thread_count,
                    |b, &threads| {
                        let hash_builder = hash_builder.clone();
                        b.iter_batched(
                            || {
                                Arc::new(LruCache::<
                                    BenchValue,
                                    HashBuilder,
                                    Box<dyn Hasher + Send + Sync>,
                                >::new(
                                    CACHE_WEIGHT,
                                    Duration::from_mins(1),
                                    hash_builder.clone(),
                                    policy,
                                    engine,
                                ))
                            },
                            |cache| {
                                let handles: Vec<_> = (0..threads)
                                    .map(|thread_id| {
                                        let cache = Arc::clone(&cache);
                                        let handle = handle.clone();
                                        std::thread::spawn(move || {
                                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                            handle.block_on(async {
                                                for _ in 0..OPERATIONS_PER_THREAD {
                                                    let key = rng.random_range(0..KEY_SPACE);
                                                    let value = BenchValue(random_value(&mut rng));
                                                    black_box(cache.put_raw_key(&key, value).await);
                                                }
                                            });
                                        })
                                    })
                                    .collect();
                                for handle in handles {
                                    handle.join().expect("thread panicked");
                                }
                            },
                            criterion::BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

fn bench_lru_cache_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_cache_concurrent_mixed_80_20");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    // Benchmark all combinations of caching policy and hash algorithm
    for (pair_name, engine, policy) in all_engine_policy_pairs() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{pair_name}_{hash_name}_{thread_count}threads");

                group.bench_with_input(
                    BenchmarkId::from_parameter(&bench_name),
                    &thread_count,
                    |b, &threads| {
                        let hash_builder = hash_builder.clone();
                        b.iter_batched(
                            || {
                                let cache: Arc<
                                    LruCache<
                                        BenchValue,
                                        HashBuilder,
                                        Box<dyn Hasher + Send + Sync>,
                                    >,
                                > = Arc::new(LruCache::new(
                                    CACHE_WEIGHT,
                                    Duration::from_mins(1),
                                    hash_builder.clone(),
                                    policy,
                                    engine,
                                ));
                                let mut rng = StdRng::seed_from_u64(42);
                                handle.block_on(async {
                                    for i in 0..5000 {
                                        let key = (i as u64 * 17) % KEY_SPACE;
                                        let value = BenchValue(random_value(&mut rng));
                                        cache.put_raw_key(&key, value).await;
                                    }
                                });
                                cache
                            },
                            |cache| {
                                let handles: Vec<_> = (0..threads)
                                    .map(|thread_id| {
                                        let cache = Arc::clone(&cache);
                                        let handle = handle.clone();
                                        std::thread::spawn(move || {
                                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                            handle.block_on(async {
                                                for _ in 0..OPERATIONS_PER_THREAD {
                                                    let key = rng.random_range(0..KEY_SPACE);
                                                    if rng.random_bool(0.8) {
                                                        black_box(cache.get_raw_key(&key).await);
                                                    } else {
                                                        let value =
                                                            BenchValue(random_value(&mut rng));
                                                        black_box(
                                                            cache.put_raw_key(&key, value).await,
                                                        );
                                                    }
                                                }
                                            });
                                        })
                                    })
                                    .collect();
                                for handle in handles {
                                    handle.join().expect("thread panicked");
                                }
                            },
                            criterion::BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

/// Read/write contention on a full cache whose reads mostly hit.
///
/// The other `LruCache` benchmarks read a 100k key space holding at most 5,000
/// entries, so ~95% of their reads miss. That matters for the engine comparison
/// because a Pingora miss is one metadata lookup, while a hit removes the entry
/// and re-admits it — `pingora-lru` has no `peek_value`. Reads that miss never
/// reach the path where the engines differ.
///
/// Here the read key space is the working set, so reads hit, and capacity holds
/// 80% of it, so writes evict. Thread counts straddle the 16 metadata shards:
/// at 16 threads shard collisions are incidental, at 32 they are structural.
fn bench_lru_cache_hot_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_cache_hot_read_write");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    // Raw-key operations bypass the hasher, so the hash algorithm cannot affect
    // this measurement.
    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");

    for (pair_name, engine, policy) in all_engine_policy_pairs() {
        for (mix_name, read_percent) in [("95_5", 95u32), ("80_20", 80)] {
            for thread_count in [16usize, 32] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{pair_name}_{mix_name}_{thread_count}threads");

                group.bench_with_input(
                    BenchmarkId::from_parameter(&bench_name),
                    &thread_count,
                    |b, &threads| {
                        let hash_builder = hash_builder.clone();
                        b.iter_batched(
                            || {
                                let cache: Arc<
                                    LruCache<
                                        BenchValue,
                                        HashBuilder,
                                        Box<dyn Hasher + Send + Sync>,
                                    >,
                                > = Arc::new(LruCache::new(
                                    HOT_CACHE_WEIGHT,
                                    Duration::from_mins(10),
                                    hash_builder.clone(),
                                    policy,
                                    engine,
                                ));
                                let mut rng = StdRng::seed_from_u64(42);
                                handle.block_on(async {
                                    // Insert the whole working set; the cache
                                    // evicts down to capacity and starts full.
                                    for key in 0..HOT_WORKING_SET {
                                        let value = BenchValue(random_value(&mut rng));
                                        cache.put_raw_key(&key, value).await;
                                    }
                                    cache.checkpoint().await;
                                });
                                cache
                            },
                            |cache| {
                                let handles: Vec<_> = (0..threads)
                                    .map(|thread_id| {
                                        let cache = Arc::clone(&cache);
                                        let handle = handle.clone();
                                        std::thread::spawn(move || {
                                            let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                            handle.block_on(async {
                                                for _ in 0..OPERATIONS_PER_THREAD {
                                                    let key =
                                                        rng.random_range(0..HOT_WORKING_SET);
                                                    if rng.random_range(0..100) < read_percent {
                                                        black_box(cache.get_raw_key(&key).await);
                                                    } else {
                                                        // Rewriting a key already in the
                                                        // working set is what a refreshed
                                                        // result does; it still evicts,
                                                        // because the cache is full.
                                                        let value =
                                                            BenchValue(random_value(&mut rng));
                                                        black_box(
                                                            cache.put_raw_key(&key, value).await,
                                                        );
                                                    }
                                                }
                                            });
                                        })
                                    })
                                    .collect();
                                for handle in handles {
                                    handle.join().expect("thread panicked");
                                }
                            },
                            criterion::BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_simple_cache_concurrent_get,
    bench_simple_cache_concurrent_put,
    bench_simple_cache_concurrent_mixed,
    bench_lru_cache_concurrent_get,
    bench_lru_cache_concurrent_put,
    bench_lru_cache_concurrent_mixed,
    bench_lru_cache_hot_read_write
);
criterion_main!(benches);
