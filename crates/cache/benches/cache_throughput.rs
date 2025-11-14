#![allow(clippy::expect_used)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unit_arg)]

use cache::{CacheProvider, SimpleCache};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;

const CACHE_WEIGHT: u64 = 100_000;
const KEY_SPACE: u64 = 100_000;
const OPERATIONS_PER_THREAD: usize = 10_000;

#[cfg(feature = "xxhash")]
fn default_hasher() -> impl std::hash::BuildHasher + Clone + Send + Sync + 'static {
    use twox_hash::XxHash3_64;
    std::hash::BuildHasherDefault::<XxHash3_64>::default()
}

#[cfg(not(feature = "xxhash"))]
fn default_hasher() -> impl std::hash::BuildHasher + Clone + Send + Sync + 'static {
    RandomState::default()
}

fn random_value(rng: &mut StdRng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

fn bench_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_get");
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    for thread_count in [1, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                b.iter_batched(
                    || {
                        let cache: Arc<SimpleCache<String, _>> = Arc::new(SimpleCache::new(
                            CACHE_WEIGHT,
                            Duration::from_secs(60),
                            default_hasher(),
                        ));
                        let mut rng = StdRng::seed_from_u64(42);
                        rt.block_on(async {
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
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new()
                                        .expect("Failed to create runtime");
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    rt.block_on(async {
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

fn bench_concurrent_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_put");

    for thread_count in [1, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                b.iter_batched(
                    || {
                        Arc::new(SimpleCache::<String, _>::new(
                            CACHE_WEIGHT,
                            Duration::from_secs(60),
                            default_hasher(),
                        ))
                    },
                    |cache| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let cache = Arc::clone(&cache);
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new()
                                        .expect("Failed to create runtime");
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    rt.block_on(async {
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

fn bench_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed_80_20");
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    for thread_count in [1, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements(
            (thread_count * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{thread_count}_threads")),
            &thread_count,
            |b, &threads| {
                b.iter_batched(
                    || {
                        let cache: Arc<SimpleCache<String, _>> = Arc::new(SimpleCache::new(
                            CACHE_WEIGHT,
                            Duration::from_secs(60),
                            default_hasher(),
                        ));
                        let mut rng = StdRng::seed_from_u64(42);
                        rt.block_on(async {
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
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new()
                                        .expect("Failed to create runtime");
                                    let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                    rt.block_on(async {
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

criterion_group!(
    benches,
    bench_concurrent_get,
    bench_concurrent_put,
    bench_concurrent_mixed
);
criterion_main!(benches);
