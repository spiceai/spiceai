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
    AsTableRefs, CacheMetrics, CacheProvider, HashBuilder, LruCache, SimpleCache, Sizeable,
    get_hash_builder,
};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::sql::TableReference;
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use spicepod::component::caching::{CacheEngine, CachingPolicy, HashingAlgorithm};
use std::collections::HashSet;
use std::hash::Hasher;
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
    fn record_eviction() {}
    fn update_hit_ratio(_hits: u64, _total: u64) {}
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
                            Duration::from_secs(60),
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
                                            let key = rng.gen_range(0..KEY_SPACE);
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
                            CACHE_WEIGHT,
                            Duration::from_secs(60),
                            hash_builder.clone(),
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
                                            let key = rng.gen_range(0..KEY_SPACE);
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
                            Duration::from_secs(60),
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
                                            let key = rng.gen_range(0..KEY_SPACE);
                                            if rng.gen_bool(0.8) {
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
                                    Duration::from_secs(60),
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
                                                    let key = rng.gen_range(0..KEY_SPACE);
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
                                    Duration::from_secs(60),
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
                                                    let key = rng.gen_range(0..KEY_SPACE);
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
                                    Duration::from_secs(60),
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
                                                    let key = rng.gen_range(0..KEY_SPACE);
                                                    if rng.gen_bool(0.8) {
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

criterion_group!(
    benches,
    bench_simple_cache_concurrent_get,
    bench_simple_cache_concurrent_put,
    bench_simple_cache_concurrent_mixed,
    bench_lru_cache_concurrent_get,
    bench_lru_cache_concurrent_put,
    bench_lru_cache_concurrent_mixed
);
criterion_main!(benches);
