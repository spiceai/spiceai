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
    Sizeable, TabledCacheProvider, get_hash_builder,
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

/// Number of distinct tables entries are spread over in the invalidation
/// benchmark. Mirrors a pod serving a handful of accelerated datasets, where a
/// refresh of one table invalidates only the entries that read it.
const INVALIDATION_TABLE_COUNT: u64 = 8;

/// A value that reports the table it read, so `invalidate_for_table` can match
/// it.
///
/// [`BenchValue`] reports an empty set, which no table reference matches, so
/// invalidating against it would walk the cache and remove nothing — timing the
/// scan but never the removals.
#[derive(Clone)]
struct TabledBenchValue {
    payload: String,
    tables: Arc<HashSet<TableReference>>,
}

impl TabledBenchValue {
    fn new(payload: String, table_idx: u64) -> Self {
        let mut tables = HashSet::new();
        tables.insert(TableReference::bare(format!("table_{table_idx}")));
        Self {
            payload,
            tables: Arc::new(tables),
        }
    }
}

impl Sizeable for TabledBenchValue {
    fn get_memory_size(&self) -> usize {
        self.payload.len()
    }
}

impl CacheMetrics for TabledBenchValue {
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

impl AsTableRefs for TabledBenchValue {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::clone(&self.tables)
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

/// The engine/policy combinations worth measuring.
///
/// Not a cartesian product of the two: Pingora has no `TinyLFU` admission, so
/// `LruCache::new` warns and builds an LRU. A `pingora_tinylfu` arm would
/// re-measure `pingora_lru` under a name claiming otherwise.
///
/// Pingora is behind a feature flag, and when it is off `LruCache::new` falls
/// back to Moka *silently* as far as a benchmark can tell. Gating the arm here
/// rather than filtering later is what keeps a `cargo bench` without
/// `--features pingora` from publishing Moka numbers labelled `pingora`.
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

/// Invalidating one table's entries — what an accelerated refresh triggers on
/// every cycle, once per dataset.
///
/// Scaled by *total* entries while the number removed stays a fixed share:
/// Moka matches through its own index, while the Pingora path has no
/// closure-based API and walks every key, reading each value to test it
/// (`LruCache::invalidate_for_table`). Cost that tracks cache size rather than
/// match count is the difference this is here to catch, so cache size is the
/// axis.
///
/// `checkpoint()` is inside the measured region deliberately. Moka's
/// `invalidate_entries_if` only registers a predicate and applies it during
/// later maintenance, so timing the call alone would compare registering a
/// predicate against completing a scan. `checkpoint()` forces the deferred work
/// for Moka; for Pingora the removals are already done and it only settles the
/// weight.
fn bench_lru_cache_invalidate_for_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_cache_invalidate_for_table");
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    // Raw-key operations hand the u64 straight to the backend without consulting
    // the hasher, so the hash algorithm cannot move this measurement.
    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");

    for (pair_name, engine, policy) in all_engine_policy_pairs() {
        // 50 is not a rounding-down of the others: a pod whose workload has a
        // small set of distinct queries holds a cache this size, and it is the
        // point where a per-entry cost is still cheap enough to be invisible.
        for entry_count in [50u64, 1_000, 10_000, 50_000] {
            // Elements are the entries the implementation may have to consider,
            // which is what makes the per-entry cost readable off the throughput.
            group.throughput(Throughput::Elements(entry_count));

            let bench_name = format!("{pair_name}_{entry_count}entries");

            group.bench_with_input(
                BenchmarkId::from_parameter(&bench_name),
                &entry_count,
                |b, &entries| {
                    let hash_builder = hash_builder.clone();
                    b.iter_batched(
                        || {
                            // Capacity well above the population: an eviction
                            // during setup would leave each iteration
                            // invalidating a different-sized cache.
                            let cache: Arc<
                                LruCache<
                                    TabledBenchValue,
                                    HashBuilder,
                                    Box<dyn Hasher + Send + Sync>,
                                >,
                            > = Arc::new(LruCache::new(
                                entries * 128,
                                Duration::from_mins(10),
                                hash_builder.clone(),
                                policy,
                                engine,
                            ));
                            let mut rng = StdRng::seed_from_u64(42);
                            handle.block_on(async {
                                for i in 0..entries {
                                    let value = TabledBenchValue::new(
                                        random_value(&mut rng),
                                        i % INVALIDATION_TABLE_COUNT,
                                    );
                                    cache.put_raw_key(&i, value).await;
                                }
                                // Settle the population so the first
                                // invalidation is not charged for setup work.
                                cache.checkpoint().await;
                            });
                            cache
                        },
                        |cache| {
                            handle.block_on(async {
                                black_box(
                                    cache
                                        .invalidate_for_table(TableReference::bare("table_3"))
                                        .await,
                                )
                                .expect("invalidation failed");
                                cache.checkpoint().await;
                            });
                        },
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
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
    bench_lru_cache_invalidate_for_table
);
criterion_main!(benches);
