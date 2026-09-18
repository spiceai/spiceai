/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Bakeoff: Spice sharded cache vs Moka vs Pingora (when the feature is on).
//!
//! Compares the `CacheBackend` implementations the results caches used to
//! select via `engine`. `LruCache` now always uses Spice; these benches
//! construct the old backends directly so the cutover has numbers.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unit_arg)]

use cache::{
    CacheBackend, CacheBackendBuilder, CacheMetrics, EvictionReason, InvalidationMode, MokaBackend,
    Sizeable, SpiceBackend, StaleRejectionReason,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sharded_cache::EvictionPolicy;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "pingora")]
use cache::PingoraBackend;

const CACHE_WEIGHT: u64 = 8 * 1024 * 1024;
const KEY_SPACE: u64 = 50_000;
const PREFILL: u64 = 8_000;
/// Key space for the near-100% hit-rate group. Prefill writes every key, and
/// the 8 MiB budget holds all of them (`HOT_KEY_SPACE * 32` bytes).
const HOT_KEY_SPACE: u64 = 8_000;
const OPERATIONS_PER_THREAD: usize = 8_000;
/// Thread counts for every bakeoff group, including the 32/64 contention cells.
const THREAD_COUNTS: [usize; 5] = [1, 8, 16, 32, 64];

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
    fn record_stale_rejection(_reason: StaleRejectionReason) {}
    fn record_table_invalidation(_mode: InvalidationMode) {}
    fn update_hit_ratio(_hits: u64, _total: u64) {}
    fn publish_counters_at_zero() {}
}

fn random_value(rng: &mut StdRng) -> String {
    use rand::distr::Alphanumeric;
    rng.sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime")
}

fn spice_backend() -> Arc<SpiceBackend<BenchValue>> {
    Arc::new(SpiceBackend::new(
        CACHE_WEIGHT,
        Duration::from_mins(1),
        EvictionPolicy::Lru,
    ))
}

fn moka_backend() -> Arc<MokaBackend<BenchValue, std::hash::RandomState>> {
    let builder = CacheBackendBuilder::new(CACHE_WEIGHT, Duration::from_mins(1));
    Arc::new(MokaBackend::new(&builder, std::hash::RandomState::new()))
}

#[cfg(feature = "pingora")]
fn pingora_backend() -> Arc<PingoraBackend<BenchValue>> {
    let builder = CacheBackendBuilder::new(CACHE_WEIGHT, Duration::from_mins(1));
    Arc::new(PingoraBackend::new(&builder))
}

async fn prefill<B: CacheBackend<BenchValue>>(backend: &B) {
    let mut rng = StdRng::seed_from_u64(42);
    for i in 0..PREFILL {
        let key = (i * 17) % KEY_SPACE;
        backend
            .insert(key, BenchValue(random_value(&mut rng)))
            .await;
    }
}

async fn prefill_hot<B: CacheBackend<BenchValue>>(backend: &B) {
    let mut rng = StdRng::seed_from_u64(42);
    for key in 0..HOT_KEY_SPACE {
        backend
            .insert(key, BenchValue(random_value(&mut rng)))
            .await;
    }
}

fn run_gets<B: CacheBackend<BenchValue> + Send + Sync + 'static>(
    handle: &tokio::runtime::Handle,
    backend: &Arc<B>,
    threads: usize,
    key_space: u64,
) {
    let joins: Vec<_> = (0..threads)
        .map(|thread_id| {
            let backend = Arc::clone(backend);
            let handle = handle.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                handle.block_on(async {
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let key = rng.random_range(0..key_space);
                        black_box(backend.get(&key).await);
                    }
                });
            })
        })
        .collect();
    for join in joins {
        join.join().expect("worker panicked");
    }
}

fn run_mixed<B: CacheBackend<BenchValue> + Send + Sync + 'static>(
    handle: &tokio::runtime::Handle,
    backend: &Arc<B>,
    threads: usize,
) {
    let joins: Vec<_> = (0..threads)
        .map(|thread_id| {
            let backend = Arc::clone(backend);
            let handle = handle.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                handle.block_on(async {
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let key = rng.random_range(0..KEY_SPACE);
                        if rng.random_bool(0.8) {
                            black_box(backend.get(&key).await);
                        } else {
                            black_box(
                                backend
                                    .insert(key, BenchValue(random_value(&mut rng)))
                                    .await,
                            );
                        }
                    }
                });
            })
        })
        .collect();
    for join in joins {
        join.join().expect("worker panicked");
    }
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));
    group.sample_size(20);
}

/// Uniform samples over `KEY_SPACE` after prefilling `PREFILL` keys (~16% hits).
/// The miss-heavy path is the common results-cache case; [`bench_concurrent_get_hot`]
/// covers near-100% hits.
fn bench_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bakeoff_get");
    configure_group(&mut group);
    let rt = runtime();
    let handle = rt.handle().clone();

    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements(
            (threads * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(BenchmarkId::new("spice", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("moka", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = moka_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        #[cfg(feature = "pingora")]
        group.bench_with_input(BenchmarkId::new("pingora", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = pingora_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_concurrent_get_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bakeoff_get_hot");
    configure_group(&mut group);
    let rt = runtime();
    let handle = rt.handle().clone();

    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements(
            (threads * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(BenchmarkId::new("spice", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend();
                    handle.block_on(prefill_hot(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, HOT_KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("moka", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = moka_backend();
                    handle.block_on(prefill_hot(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, HOT_KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        #[cfg(feature = "pingora")]
        group.bench_with_input(BenchmarkId::new("pingora", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = pingora_backend();
                    handle.block_on(prefill_hot(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, HOT_KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bakeoff_mixed_80_20");
    configure_group(&mut group);
    let rt = runtime();
    let handle = rt.handle().clone();

    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements(
            (threads * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(BenchmarkId::new("spice", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_mixed(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("moka", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = moka_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_mixed(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });

        #[cfg(feature = "pingora")]
        group.bench_with_input(BenchmarkId::new("pingora", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = pingora_backend();
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_mixed(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_get,
    bench_concurrent_get_hot,
    bench_concurrent_mixed
);
criterion_main!(benches);
