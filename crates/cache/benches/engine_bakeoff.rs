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

//! Bakeoff: Spice (LRU / LFU / W-TinyLFU) vs Moka LRU vs Pingora.
//!
//! The headline group targets ~70% hits / 30% misses. Older ~16% and ~100%
//! groups remain for regression context.

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
use std::time::{Duration, Instant};

#[cfg(feature = "pingora")]
use cache::PingoraBackend;

const CACHE_WEIGHT: u64 = 8 * 1024 * 1024;
const KEY_SPACE: u64 = 50_000;
const PREFILL: u64 = 8_000;
/// Key space for the near-100% hit-rate group. Prefill writes every key, and
/// the 8 MiB budget holds all of them (`HOT_KEY_SPACE * 32` bytes).
const HOT_KEY_SPACE: u64 = 8_000;
/// Hot set for the ~70% hit group. Fully prefilled and fits in budget; 70% of
/// gets sample this set and 30% sample `HIT70_HOT_SET..KEY_SPACE` (all misses).
const HIT70_HOT_SET: u64 = 8_000;
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

fn spice_backend(policy: EvictionPolicy) -> Arc<SpiceBackend<BenchValue>> {
    Arc::new(SpiceBackend::new(
        CACHE_WEIGHT,
        Duration::from_mins(1),
        policy,
    ))
}

fn moka_backend() -> Arc<MokaBackend<BenchValue, std::hash::RandomState>> {
    let builder = CacheBackendBuilder::new(CACHE_WEIGHT, Duration::from_mins(1));
    Arc::new(MokaBackend::lru(&builder, std::hash::RandomState::new()))
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

async fn prefill_hit70<B: CacheBackend<BenchValue>>(backend: &B) {
    let mut rng = StdRng::seed_from_u64(42);
    for key in 0..HIT70_HOT_SET {
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

/// ~70% hits / 30% misses: with probability 0.7 sample the prefilled hot set,
/// otherwise sample keys in `HIT70_HOT_SET..KEY_SPACE` (misses outside the hot set).
fn run_gets_hit70<B: CacheBackend<BenchValue> + Send + Sync + 'static>(
    handle: &tokio::runtime::Handle,
    backend: &Arc<B>,
    threads: usize,
) {
    let joins: Vec<_> = (0..threads)
        .map(|thread_id| {
            let backend = Arc::clone(backend);
            let handle = handle.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64 + 100);
                handle.block_on(async {
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let key = if rng.random_bool(0.7) {
                            rng.random_range(0..HIT70_HOT_SET)
                        } else {
                            rng.random_range(HIT70_HOT_SET..KEY_SPACE)
                        };
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

/// Sorted-sample percentile of individual get latencies (nanoseconds).
fn percentile_ns(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

struct Hit70Latency {
    mops: f64,
    p50_ns: u64,
    p999_ns: u64,
    samples: usize,
}

/// Measure ~70/30 get throughput and **per-get** P50 / P99.9 latency.
fn measure_hit70_latency<B: CacheBackend<BenchValue> + Send + Sync + 'static>(
    handle: &tokio::runtime::Handle,
    backend: &Arc<B>,
    threads: usize,
) -> Hit70Latency {
    let wall_start = Instant::now();
    let joins: Vec<_> = (0..threads)
        .map(|thread_id| {
            let backend = Arc::clone(backend);
            let handle = handle.clone();
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64 + 100);
                let mut samples = Vec::with_capacity(OPERATIONS_PER_THREAD);
                handle.block_on(async {
                    for _ in 0..OPERATIONS_PER_THREAD {
                        let key = if rng.random_bool(0.7) {
                            rng.random_range(0..HIT70_HOT_SET)
                        } else {
                            rng.random_range(HIT70_HOT_SET..KEY_SPACE)
                        };
                        let t0 = Instant::now();
                        black_box(backend.get(&key).await);
                        samples.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    }
                });
                samples
            })
        })
        .collect();
    let mut all = Vec::with_capacity(threads * OPERATIONS_PER_THREAD);
    for join in joins {
        all.extend(join.join().expect("worker panicked"));
    }
    let wall = wall_start.elapsed().as_secs_f64().max(1e-9);
    all.sort_unstable();
    let total_ops = (threads * OPERATIONS_PER_THREAD) as f64;
    Hit70Latency {
        mops: (total_ops / wall) / 1_000_000.0,
        p50_ns: percentile_ns(&all, 0.50),
        p999_ns: percentile_ns(&all, 0.999),
        samples: all.len(),
    }
}

fn print_hit70_latency_table(handle: &tokio::runtime::Handle) {
    eprintln!();
    eprintln!("=== hit70 individual-get latency (sorted samples P50 / P99.9) ===");
    eprintln!("engine\tthreads\tMops/s\tp50_ns\tp99.9_ns\tsamples");
    for threads in THREAD_COUNTS {
        for (name, policy) in [
            ("spice_lru", EvictionPolicy::Lru),
            ("spice_lfu", EvictionPolicy::Lfu),
            ("spice_tinylfu", EvictionPolicy::TinyLfu),
        ] {
            let backend = spice_backend(policy);
            handle.block_on(prefill_hit70(backend.as_ref()));
            let s = measure_hit70_latency(handle, &backend, threads);
            eprintln!(
                "{name}\t{threads}\t{:.2}\t{}\t{}\t{}",
                s.mops, s.p50_ns, s.p999_ns, s.samples
            );
        }
        {
            let backend = moka_backend();
            handle.block_on(prefill_hit70(backend.as_ref()));
            let s = measure_hit70_latency(handle, &backend, threads);
            eprintln!(
                "moka_lru\t{threads}\t{:.2}\t{}\t{}\t{}",
                s.mops, s.p50_ns, s.p999_ns, s.samples
            );
        }
        #[cfg(feature = "pingora")]
        {
            let backend = pingora_backend();
            handle.block_on(prefill_hit70(backend.as_ref()));
            let s = measure_hit70_latency(handle, &backend, threads);
            eprintln!(
                "pingora\t{threads}\t{:.2}\t{}\t{}\t{}",
                s.mops, s.p50_ns, s.p999_ns, s.samples
            );
        }
    }
    eprintln!("=== end hit70 latency table ===");
    eprintln!();
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);
}

fn bench_id(engine: &str, threads: usize) -> BenchmarkId {
    BenchmarkId::new(engine, threads)
}

/// Headline bakeoff: ~70% hit / 30% miss. Spice LRU + LFU + TinyLFU vs Moka LRU
/// vs Pingora.
fn bench_concurrent_get_hit70(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bakeoff_get_hit70");
    configure_group(&mut group);
    let rt = runtime();
    let handle = rt.handle().clone();
    // One-shot per-get P99.9 table (sorted samples) printed before Criterion
    // throughput so the PR can cite both Mops/s and tail latency.
    print_hit70_latency_table(&handle);

    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements(
            (threads * OPERATIONS_PER_THREAD) as u64,
        ));

        for (name, policy) in [
            ("spice_lru", EvictionPolicy::Lru),
            ("spice_lfu", EvictionPolicy::Lfu),
            ("spice_tinylfu", EvictionPolicy::TinyLfu),
        ] {
            group.bench_with_input(bench_id(name, threads), &threads, |b, &n| {
                b.iter_batched(
                    || {
                        let backend = spice_backend(policy);
                        handle.block_on(prefill_hit70(backend.as_ref()));
                        backend
                    },
                    |backend| run_gets_hit70(&handle, &backend, n),
                    criterion::BatchSize::LargeInput,
                );
            });
        }

        group.bench_with_input(bench_id("moka_lru", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = moka_backend();
                    handle.block_on(prefill_hit70(backend.as_ref()));
                    backend
                },
                |backend| run_gets_hit70(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });

        #[cfg(feature = "pingora")]
        group.bench_with_input(bench_id("pingora", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = pingora_backend();
                    handle.block_on(prefill_hit70(backend.as_ref()));
                    backend
                },
                |backend| run_gets_hit70(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Uniform samples over `KEY_SPACE` after prefilling `PREFILL` keys (~16% hits).
fn bench_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bakeoff_get");
    configure_group(&mut group);
    let rt = runtime();
    let handle = rt.handle().clone();

    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements(
            (threads * OPERATIONS_PER_THREAD) as u64,
        ));

        group.bench_with_input(bench_id("spice_lru", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend(EvictionPolicy::Lru);
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(bench_id("moka_lru", threads), &threads, |b, &n| {
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
        group.bench_with_input(bench_id("pingora", threads), &threads, |b, &n| {
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

        group.bench_with_input(bench_id("spice_lru", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend(EvictionPolicy::Lru);
                    handle.block_on(prefill_hot(backend.as_ref()));
                    backend
                },
                |backend| run_gets(&handle, &backend, n, HOT_KEY_SPACE),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(bench_id("moka_lru", threads), &threads, |b, &n| {
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
        group.bench_with_input(bench_id("pingora", threads), &threads, |b, &n| {
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

        group.bench_with_input(bench_id("spice_lru", threads), &threads, |b, &n| {
            b.iter_batched(
                || {
                    let backend = spice_backend(EvictionPolicy::Lru);
                    handle.block_on(prefill(backend.as_ref()));
                    backend
                },
                |backend| run_mixed(&handle, &backend, n),
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_with_input(bench_id("moka_lru", threads), &threads, |b, &n| {
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
        group.bench_with_input(bench_id("pingora", threads), &threads, |b, &n| {
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
    bench_concurrent_get_hit70,
    bench_concurrent_get,
    bench_concurrent_get_hot,
    bench_concurrent_mixed
);
criterion_main!(benches);
