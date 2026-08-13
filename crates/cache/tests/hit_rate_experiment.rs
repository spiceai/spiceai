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
#![expect(clippy::expect_used, reason = "integration-test helpers")]

//! Pure-read performance against hit rate and cache size, for both engines.
//!
//! Complements `benches/cache_throughput.rs`. The benchmarks there either sit at
//! a fixed hit rate (`concurrent_get`, ~3%) or refill on a miss
//! (`hot_read_write`), so neither isolates the read path across hit rates. This
//! does: no writes, no eviction, the resident set pinned for the whole run.
//!
//! It also measures the achieved hit rate rather than assuming it, which is what
//! validates the `C/W` sizing the benchmarks rely on.
//!
//! Ignored by default: it takes a few minutes, and it prints a table rather than
//! asserting.
//!
//! ```text
//! cargo test -p cache --features pingora --release --test hit_rate_experiment \
//!   -- --ignored --nocapture
//! ```

use cache::{
    AsTableRefs, CacheMetrics, CacheProvider, EvictionReason, HashBuilder, LruCache, Sizeable,
    get_hash_builder,
};
use datafusion::sql::TableReference;
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use spicepod::component::caching::{CacheEngine, CachingPolicy, HashingAlgorithm};
use std::collections::HashSet;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const VALUE_BYTES: u64 = 32;
const OPS: usize = 10_000;
const THREADS: usize = 16;
/// Measured repetitions per point, plus one discarded warm-up.
const REPS: usize = 5;
const CAPACITIES: [u64; 3] = [1_000, 5_000, 30_000];
const HIT_RATES: [u64; 5] = [3, 50, 80, 95, 99];

#[derive(Clone)]
struct V(String);

impl Sizeable for V {
    fn get_memory_size(&self) -> usize {
        self.0.len()
    }
}

impl CacheMetrics for V {
    fn record_hit() {}
    fn record_miss() {}
    fn record_request() {}
    fn record_item_count(_: u64) {}
    fn record_size(_: u64) {}
    fn record_max_size(_: u64) {}
    fn record_eviction(_: EvictionReason) {}
    fn record_stale_rejection() {}
    fn update_hit_ratio(_: u64, _: u64) {}
    fn publish_counters_at_zero() {}
}

impl AsTableRefs for V {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        Arc::new(HashSet::new())
    }
}

fn random_value(rng: &mut StdRng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

type Cache = LruCache<V, HashBuilder, Box<dyn Hasher + Send + Sync>>;

/// Returns (elapsed, achieved hit rate, resident entries).
fn run(
    engine: CacheEngine,
    policy: CachingPolicy,
    capacity: u64,
    working: u64,
    handle: &tokio::runtime::Handle,
) -> (Duration, f64, u64) {
    let hash_builder =
        get_hash_builder(HashingAlgorithm::XXH3).expect("Failed to get hash builder");
    let cache: Arc<Cache> = Arc::new(LruCache::new(
        capacity * VALUE_BYTES,
        Duration::from_mins(10),
        hash_builder,
        policy,
        engine,
    ));

    let mut rng = StdRng::seed_from_u64(42);
    handle.block_on(async {
        for key in 0..working {
            cache.put_raw_key(&key, V(random_value(&mut rng))).await;
        }
        cache.checkpoint().await;
    });
    let resident = handle.block_on(async { cache.item_count().await });

    let misses = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let threads: Vec<_> = (0..THREADS)
        .map(|tid| {
            let cache = Arc::clone(&cache);
            let handle = handle.clone();
            let misses = Arc::clone(&misses);
            std::thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(tid as u64);
                let mut local = 0u64;
                handle.block_on(async {
                    for _ in 0..OPS {
                        let key = rng.random_range(0..working);
                        // Pure reads: no refill, so the resident set cannot drift
                        // and the hit rate stays what the sizing set it to.
                        if cache.get_raw_key(&key).await.is_none() {
                            local += 1;
                        }
                    }
                });
                misses.fetch_add(local, Ordering::Relaxed);
            })
        })
        .collect();
    for t in threads {
        t.join().expect("thread panicked");
    }
    let elapsed = start.elapsed();

    // Bounded by THREADS * OPS, so u32 holds them and the widening to f64 is
    // lossless.
    let total = f64::from(u32::try_from(THREADS * OPS).expect("operation count fits in u32"));
    let miss_count =
        f64::from(u32::try_from(misses.load(Ordering::Relaxed)).expect("miss count fits in u32"));
    let hit_rate = 1.0 - miss_count / total;
    (elapsed, hit_rate, resident)
}

#[test]
#[ignore = "reports timings; run explicitly with --ignored"]
fn pure_read_vs_hit_rate() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let handle = rt.handle().clone();

    #[cfg_attr(
        not(feature = "pingora"),
        expect(unused_mut, reason = "only the pingora arm below pushes")
    )]
    let mut engines: Vec<(&str, CacheEngine, CachingPolicy)> = vec![
        ("moka_lru", CacheEngine::Moka, CachingPolicy::Lru),
        ("moka_tinylfu", CacheEngine::Moka, CachingPolicy::TinyLfu),
    ];
    #[cfg(feature = "pingora")]
    engines.push(("pingora_lru", CacheEngine::Pingora, CachingPolicy::Lru));

    println!(
        "\npure reads, {THREADS} threads, {OPS} ops/thread, median of {REPS} (+1 warm-up discarded)\n"
    );

    for capacity in CAPACITIES {
        println!("=== capacity {capacity} entries ===");
        println!(
            "{:<14} {:>7} {:>8} {:>10} {:>9} {:>8}",
            "engine", "target", "actual", "resident", "median ms", "spread"
        );
        for target in HIT_RATES {
            let working = capacity * 100 / target;
            for (name, engine, policy) in &engines {
                // Discarded: the first run pays allocator and page-fault costs
                // the later ones do not.
                let _ = run(*engine, *policy, capacity, working, &handle);

                let mut times = Vec::new();
                let (mut actual, mut resident) = (0.0, 0u64);
                for _ in 0..REPS {
                    let (d, h, r) = run(*engine, *policy, capacity, working, &handle);
                    times.push(d.as_secs_f64() * 1000.0);
                    actual = h;
                    resident = r;
                }
                times.sort_by(f64::total_cmp);
                let median = times[REPS / 2];
                let spread = (times[REPS - 1] - times[0]) / median * 100.0;
                println!(
                    "{:<14} {:>6}% {:>7.1}% {:>10} {:>9.1} {:>7.1}%",
                    name,
                    target,
                    actual * 100.0,
                    resident,
                    median,
                    spread
                );
            }
            println!();
        }
    }
}
