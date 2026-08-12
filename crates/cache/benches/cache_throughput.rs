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

// Get all caching policies to benchmark
fn all_caching_policies() -> Vec<(&'static str, CachingPolicy)> {
    vec![
        ("lru", CachingPolicy::Lru),
        ("tinylfu", CachingPolicy::TinyLfu),
    ]
}

// Get all cache engines to benchmark. Pingora is listed only when the feature is
// enabled: without it, requesting `CacheEngine::Pingora` silently falls back to
// Moka, and the comparison would measure Moka against itself.
fn all_cache_engines() -> Vec<(&'static str, CacheEngine)> {
    vec![
        ("moka", CacheEngine::Moka),
        #[cfg(feature = "pingora")]
        ("pingora", CacheEngine::Pingora),
    ]
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
    for (policy_name, policy) in all_caching_policies() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{policy_name}_{hash_name}_{thread_count}threads");

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
                                    CacheEngine::Moka,
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
    for (policy_name, policy) in all_caching_policies() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{policy_name}_{hash_name}_{thread_count}threads");

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
                                    CacheEngine::Moka,
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
    for (policy_name, policy) in all_caching_policies() {
        for (hash_name, hash_algo) in all_hash_algorithms() {
            let hash_builder = get_hash_builder(hash_algo).expect("Failed to get hash builder");

            for thread_count in [1, 4, 8, 16] {
                group.throughput(Throughput::Elements(
                    (thread_count * OPERATIONS_PER_THREAD) as u64,
                ));

                let bench_name = format!("{policy_name}_{hash_name}_{thread_count}threads");

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
                                    CacheEngine::Moka,
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

type EngineBenchCache = LruCache<BenchValue, HashBuilder, Box<dyn Hasher + Send + Sync>>;

/// Builds a cache on the given engine, prepopulated with `prepopulate` entries.
///
/// Policy and hasher are fixed (LRU, xxh3) so the engine is the only variable:
/// the policy only selects between Moka's internal algorithms anyway, and the
/// Pingora backend is always plain LRU.
fn new_engine_cache(
    engine: CacheEngine,
    handle: &tokio::runtime::Handle,
    prepopulate: usize,
) -> Arc<EngineBenchCache> {
    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");
    let cache: Arc<EngineBenchCache> = Arc::new(LruCache::new(
        CACHE_WEIGHT,
        Duration::from_mins(1),
        hash_builder,
        CachingPolicy::Lru,
        engine,
    ));
    if prepopulate > 0 {
        let mut rng = StdRng::seed_from_u64(42);
        handle.block_on(async {
            for i in 0..prepopulate {
                let key = (i as u64 * 17) % KEY_SPACE;
                let value = BenchValue(random_value(&mut rng));
                cache.put_raw_key(&key, value).await;
            }
        });
    }
    cache
}

/// Runs `OPERATIONS_PER_THREAD` operations on each of `threads` OS threads and
/// waits for them all, mirroring the runner the per-policy benchmarks use.
/// `put_probability` selects the workload: 0.0 is get-only, 1.0 is put-only.
fn run_engine_workload(
    cache: &Arc<EngineBenchCache>,
    handle: &tokio::runtime::Handle,
    threads: usize,
    put_probability: f64,
) {
    let handles: Vec<_> = (0..threads)
        .map(|thread_id| {
            let cache = Arc::clone(cache);
            let handle = handle.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                handle.block_on(async {
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let key = rng.random_range(0..KEY_SPACE);
                        if rng.random_bool(put_probability) {
                            let value = BenchValue(random_value(&mut rng));
                            black_box(cache.put_raw_key(&key, value).await);
                        } else {
                            black_box(cache.get_raw_key(&key).await);
                        }
                    }
                });
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("thread panicked");
    }
}

/// Moka vs Pingora on one workload. Criterion's per-iteration time is the
/// latency read; with `Throughput::Elements` it also reports ops/sec.
fn bench_cache_engines(
    c: &mut Criterion,
    group_name: &str,
    prepopulate: usize,
    put_probability: f64,
) {
    let mut group = c.benchmark_group(group_name);
    let rt = create_bench_runtime();
    let handle = rt.handle().clone();

    for (engine_name, engine) in all_cache_engines() {
        for thread_count in [1, 4, 8, 16] {
            group.throughput(Throughput::Elements(
                (thread_count * OPERATIONS_PER_THREAD) as u64,
            ));

            let bench_name = format!("{engine_name}_{thread_count}threads");

            group.bench_with_input(
                BenchmarkId::from_parameter(&bench_name),
                &thread_count,
                |b, &threads| {
                    b.iter_batched(
                        || new_engine_cache(engine, &handle, prepopulate),
                        |cache| run_engine_workload(&cache, &handle, threads, put_probability),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_cache_engine_concurrent_get(c: &mut Criterion) {
    bench_cache_engines(c, "cache_engine_concurrent_get", 5000, 0.0);
}

fn bench_cache_engine_concurrent_put(c: &mut Criterion) {
    bench_cache_engines(c, "cache_engine_concurrent_put", 0, 1.0);
}

fn bench_cache_engine_concurrent_mixed(c: &mut Criterion) {
    bench_cache_engines(c, "cache_engine_concurrent_mixed_80_20", 5000, 0.2);
}

criterion_group!(
    benches,
    bench_simple_cache_concurrent_get,
    bench_simple_cache_concurrent_put,
    bench_simple_cache_concurrent_mixed,
    bench_lru_cache_concurrent_get,
    bench_lru_cache_concurrent_put,
    bench_lru_cache_concurrent_mixed,
    bench_cache_engine_concurrent_get,
    bench_cache_engine_concurrent_put,
    bench_cache_engine_concurrent_mixed
);
criterion_main!(benches);
