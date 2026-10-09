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

//! How maintenance and hit latency scale with the number of resident entries.
//!
//! For each entry count `N` a fresh LRU cache with a long TTL and a budget far
//! above the data (so nothing expires or evicts) is prefilled with `N` random
//! keys and its recency order scrambled by `N` random hits, then measured four
//! ways:
//!
//! - **sweep**: wall time of `run_pending_tasks()` (min / median / max of 5).
//! - **hits**: reader threads `get` random prefilled keys while one writer
//!   inserts new keys at a fixed rate; every get is recorded.
//! - **hits+sweep**: the same, plus a thread calling `run_pending_tasks()`
//!   every second (the runtime calls it once a minute).
//! - **inserts**: per-call latency of the writer's inserts in the hits+sweep
//!   phase, so a change to the write path shows up beside the read path.
//! - **mass expiry**: a separate cache with a 3 s TTL holding `N/2` expired and
//!   `N/2` live entries; readers hit the live half while one
//!   `run_pending_tasks()` reclaims the expired half.
//!
//! Environment overrides: `SIZES` (comma-separated entry counts), `SECS`
//! (seconds per hit phase), `READERS`, `WRITER` (inserts per second, `0` for
//! none), `POLICY` (`lru`, `lfu`, `tinylfu`), and `BUDGET_PERCENT`: a byte
//! budget of that percentage of the prefill instead of one far above it, so
//! prefill and every later insert run the size-eviction path. Readers then
//! miss on evicted keys and count those misses instead of asserting hits.
//!
//! ```text
//! cargo bench -p sharded-cache --bench size_scaling
//! SIZES=100000,1000000 SECS=3 cargo bench -p sharded-cache --bench size_scaling
//! ```

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]

use sharded_cache::{EvictionPolicy, ShardedCache};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_SIZES: [usize; 7] = [
    100_000, 250_000, 500_000, 1_000_000, 1_500_000, 2_000_000, 3_000_000,
];
/// Declared weight of each entry; the budget is far above `N * ENTRY_WEIGHT`.
const ENTRY_WEIGHT: usize = 1024;
const MAX_WEIGHT: u64 = 1 << 50;
/// 48 hours plus one minute: an item TTL plus a stale-while-revalidate window.
const TTL: Duration = Duration::from_mins(48 * 60 + 1);
const DEFAULT_WRITER_INSERTS_PER_SEC: u64 = 2_000;
const SWEEP_SAMPLES: usize = 5;
/// Reader misses in `BUDGET_PERCENT` runs, where evicted keys stop hitting.
static MISSES: AtomicU64 = AtomicU64::new(0);
/// Unrecorded run time at the start of each hit phase, so every arm's threads
/// reach the same steady state (core placement, clock) before measurement
/// rather than inheriting whatever the previous phase left behind.
const WARMUP: Duration = Duration::from_secs(1);

/// Value stored per entry. The sweep walks nodes, not values, so a small value
/// keeps the benchmark's own memory down without changing what is measured.
#[derive(Clone)]
struct Value(u64);

/// splitmix64: a seeded, dependency-free key generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Log-linear latency histogram: exact below 64 ns, then 32 sub-buckets per
/// power of two (about 3% resolution). The maximum is kept exactly.
#[derive(Clone)]
struct Histogram {
    counts: Vec<u64>,
    total: u64,
    max: u64,
}

const SUB_BITS: u32 = 5;
const SUBS: usize = 1 << SUB_BITS;

impl Histogram {
    fn new() -> Self {
        Self {
            counts: vec![0; 64 + 58 * SUBS],
            total: 0,
            max: 0,
        }
    }

    fn index(v: u64) -> usize {
        if v < 64 {
            return v as usize;
        }
        let msb = v.ilog2();
        let sub = ((v >> (msb - SUB_BITS)) as usize) & (SUBS - 1);
        64 + (msb as usize - 6) * SUBS + sub
    }

    /// Upper bound (exclusive) of the bucket at `idx`.
    fn upper(idx: usize) -> u64 {
        if idx < 64 {
            return idx as u64 + 1;
        }
        let msb = (idx - 64) / SUBS + 6;
        let sub = ((idx - 64) % SUBS) as u64;
        let width = 1u64 << (msb as u32 - SUB_BITS);
        (1u64 << msb) + (sub + 1) * width
    }

    fn record(&mut self, v: u64) {
        self.counts[Self::index(v)] += 1;
        self.total += 1;
        self.max = self.max.max(v);
    }

    fn merge(&mut self, other: &Self) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.total += other.total;
        self.max = self.max.max(other.max);
    }

    fn quantile(&self, q: f64) -> u64 {
        let rank = ((self.total as f64) * q).ceil() as u64;
        let mut seen = 0;
        for (idx, count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank.max(1) {
                return Self::upper(idx).min(self.max);
            }
        }
        self.max
    }

    fn count_at_least(&self, v: u64) -> u64 {
        self.counts[Self::index(v)..].iter().sum()
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn policy() -> EvictionPolicy {
    match std::env::var("POLICY").as_deref() {
        Ok("lfu") => EvictionPolicy::Lfu,
        Ok("tinylfu") => EvictionPolicy::TinyLfu,
        _ => EvictionPolicy::Lru,
    }
}

struct Phase {
    hits: Histogram,
    inserts: Histogram,
    sweeps: Vec<Duration>,
}

/// Run readers (and a writer, and optionally a sweeper) for `secs` seconds.
fn hit_phase(
    cache: &Arc<ShardedCache<Value>>,
    keys: &Arc<Vec<u64>>,
    readers: usize,
    secs: u64,
    sweep_every: Option<Duration>,
    seed: u64,
    evicting: bool,
) -> Phase {
    let stop = Arc::new(AtomicBool::new(false));
    let record_from = Instant::now() + WARMUP;
    let reader_handles: Vec<_> = (0..readers)
        .map(|r| {
            let cache = Arc::clone(cache);
            let keys = Arc::clone(keys);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (r as u64 + 1).wrapping_mul(0x51_7CC1));
                let mut hist = Histogram::new();
                while !stop.load(Ordering::Relaxed) {
                    let key = keys[(rng.next() % keys.len() as u64) as usize];
                    let start = Instant::now();
                    let got = cache.get(&key);
                    let elapsed = start.elapsed().as_nanos() as u64;
                    if let Some(got) = got {
                        black_box(got.0);
                    } else {
                        assert!(evicting, "a prefilled key must hit");
                        MISSES.fetch_add(1, Ordering::Relaxed);
                    }
                    if start >= record_from {
                        hist.record(elapsed);
                    }
                }
                hist
            })
        })
        .collect();

    let rate: u64 = env_or("WRITER", DEFAULT_WRITER_INSERTS_PER_SEC);
    let writer = {
        let cache = Arc::clone(cache);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut rng = Rng(seed ^ 0xDEAD_BEEF);
            let mut hist = Histogram::new();
            if rate == 0 {
                return hist;
            }
            let interval = Duration::from_nanos(1_000_000_000 / rate);
            let mut next = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let key = rng.next();
                let start = Instant::now();
                cache.insert(key, Value(key), ENTRY_WEIGHT);
                if start >= record_from {
                    hist.record(start.elapsed().as_nanos() as u64);
                }
                next += interval;
                if let Some(wait) = next.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
            }
            hist
        })
    };

    let sweeper = sweep_every.map(|every| {
        let cache = Arc::clone(cache);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut sweeps = Vec::new();
            let mut next = Instant::now() + every;
            while !stop.load(Ordering::Relaxed) {
                if let Some(wait) = next.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                next += every;
                let start = Instant::now();
                cache.run_pending_tasks();
                sweeps.push(start.elapsed());
            }
            sweeps
        })
    });

    std::thread::sleep(WARMUP + Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);

    let mut hits = Histogram::new();
    for handle in reader_handles {
        hits.merge(&handle.join().expect("reader panicked"));
    }
    let inserts = writer.join().expect("writer panicked");
    let sweeps = sweeper
        .map(|s| s.join().expect("sweeper panicked"))
        .unwrap_or_default();
    Phase {
        hits,
        inserts,
        sweeps,
    }
}

/// Readers hit live keys while one maintenance pass reclaims `n / 2` expired
/// entries. Returns the hit histogram and the duration of that pass.
fn mass_expiry_phase(n: usize, readers: usize, seed: u64) -> (Histogram, Duration) {
    const SHORT_TTL: Duration = Duration::from_secs(3);
    let cache: Arc<ShardedCache<Value>> =
        Arc::new(ShardedCache::new(MAX_WEIGHT, SHORT_TTL, policy()));
    let mut rng = Rng(seed);
    let start = Instant::now();
    for _ in 0..n / 2 {
        let key = rng.next();
        cache.insert(key, Value(key), ENTRY_WEIGHT);
    }
    // Every deadline below is measured from the *last* old insert, so the
    // whole old half has expired when the pass runs however long filling took.
    let old_done = Instant::now();
    // The live half starts at least 1 s after the old half finished, so it
    // outlives the old half's last deadline by more than the read window.
    let live_start = (start + SHORT_TTL / 2).max(old_done + Duration::from_secs(1));
    std::thread::sleep(live_start.saturating_duration_since(Instant::now()));
    let mut live = Vec::with_capacity(n / 2);
    for _ in 0..n / 2 {
        let key = rng.next();
        cache.insert(key, Value(key), ENTRY_WEIGHT);
        live.push(key);
    }
    for _ in 0..n / 2 {
        black_box(cache.get(&live[(rng.next() % live.len() as u64) as usize]));
    }
    std::thread::sleep(
        (old_done + SHORT_TTL + Duration::from_millis(100))
            .saturating_duration_since(Instant::now()),
    );
    let live = Arc::new(live);
    let stop = Arc::new(AtomicBool::new(false));
    let reader_handles: Vec<_> = (0..readers)
        .map(|r| {
            let cache = Arc::clone(&cache);
            let live = Arc::clone(&live);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (r as u64 + 7).wrapping_mul(0x51_7CC1));
                let mut hist = Histogram::new();
                while !stop.load(Ordering::Relaxed) {
                    let key = live[(rng.next() % live.len() as u64) as usize];
                    let t = Instant::now();
                    let got = cache.get(&key);
                    let elapsed = t.elapsed().as_nanos() as u64;
                    black_box(got.expect("the live half must hit").0);
                    hist.record(elapsed);
                }
                hist
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(100));
    let sweep_start = Instant::now();
    cache.run_pending_tasks();
    let sweep = sweep_start.elapsed();
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let mut hits = Histogram::new();
    for handle in reader_handles {
        hits.merge(&handle.join().expect("reader panicked"));
    }
    assert_eq!(
        cache.len(),
        live.len(),
        "the expired half must be reclaimed (old half filled in {:?})",
        old_done - start
    );
    (hits, sweep)
}

fn hist_row(label: &str, n: usize, h: &Histogram) {
    println!(
        "| {n:>9} | {label:<10} | {:>10} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>9} | {:>8} | {:>7} |",
        h.total,
        fmt_ns(h.quantile(0.50)),
        fmt_ns(h.quantile(0.99)),
        fmt_ns(h.quantile(0.999)),
        fmt_ns(h.quantile(0.9999)),
        fmt_ns(h.max),
        h.count_at_least(100_000),
        h.count_at_least(1_000_000),
        h.count_at_least(5_000_000),
    );
}

fn main() {
    // `cargo bench` passes `--bench`; nothing here takes arguments.
    let sizes: Vec<usize> = std::env::var("SIZES").ok().map_or_else(
        || DEFAULT_SIZES.to_vec(),
        |s| {
            s.split(',')
                .map(|n| {
                    n.trim()
                        .parse()
                        .expect("SIZES must be comma-separated integers")
                })
                .collect()
        },
    );
    let secs: u64 = env_or("SECS", 5);
    let readers: usize = env_or("READERS", 8);
    let policy = policy();
    println!(
        "policy={policy:?} readers={readers} secs={secs} writer={}/s ttl={TTL:?}",
        env_or("WRITER", DEFAULT_WRITER_INSERTS_PER_SEC)
    );

    let mut sweep_rows = Vec::new();
    let mut hist_rows = Vec::new();
    for (i, &n) in sizes.iter().enumerate() {
        let budget_percent: Option<u64> = std::env::var("BUDGET_PERCENT")
            .ok()
            .map(|v| v.parse().expect("BUDGET_PERCENT must be an integer"));
        let evicting = budget_percent.is_some();
        let max_weight =
            budget_percent.map_or(MAX_WEIGHT, |pct| n as u64 * ENTRY_WEIGHT as u64 * pct / 100);
        let cache: Arc<ShardedCache<Value>> = Arc::new(ShardedCache::new(max_weight, TTL, policy));
        let mut rng = Rng(0x5EED ^ n as u64);
        let mut keys = Vec::with_capacity(n);
        let fill_start = Instant::now();
        for _ in 0..n {
            let key = rng.next();
            cache.insert(key, Value(key), ENTRY_WEIGHT);
            keys.push(key);
        }
        let fill = fill_start.elapsed();
        if !evicting {
            assert_eq!(cache.len(), n, "nothing may evict or expire during prefill");
        }
        // Prefill links each shard's LRU list in slab order, which a sweep walks
        // as a sequential scan. Production hits relink entries to the front in
        // request order, so scramble recency with one random hit per entry
        // before timing anything.
        for _ in 0..n {
            black_box(cache.get(&keys[(rng.next() % n as u64) as usize]));
        }
        let keys = Arc::new(keys);

        let mut sweeps: Vec<Duration> = (0..SWEEP_SAMPLES)
            .map(|_| {
                let start = Instant::now();
                cache.run_pending_tasks();
                start.elapsed()
            })
            .collect();
        sweeps.sort();

        let seed = 0xB00 + i as u64;
        let quiet = hit_phase(&cache, &keys, readers, secs, None, seed, evicting);
        let swept = hit_phase(
            &cache,
            &keys,
            readers,
            secs,
            Some(Duration::from_secs(1)),
            seed,
            evicting,
        );
        let mut live_sweeps = swept.sweeps.clone();
        live_sweeps.sort();
        eprintln!(
            "N={n}: prefill {fill:?}, sweep median {:?}, live sweeps {:?}, resident {}, reader misses {}",
            sweeps[SWEEP_SAMPLES / 2],
            live_sweeps,
            cache.len(),
            MISSES.swap(0, Ordering::Relaxed)
        );
        let mass = mass_expiry_phase(n, readers, seed);
        eprintln!("N={n}: mass expiry of {} entries took {:?}", n / 2, mass.1);
        sweep_rows.push((n, sweeps, live_sweeps));
        hist_rows.push((n, quiet, swept, mass));
    }

    println!();
    println!("## A. run_pending_tasks wall time (idle, 5 samples; live = during hits+sweep)");
    println!(
        "| entries | min | median | max | ~per-shard hold (median/16) | live median | live max |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for (n, sweeps, live) in &sweep_rows {
        let median = sweeps[sweeps.len() / 2];
        let live_median = live.get(live.len() / 2).copied().unwrap_or_default();
        let live_max = live.last().copied().unwrap_or_default();
        println!(
            "| {n} | {:?} | {median:?} | {:?} | {:?} | {live_median:?} | {live_max:?} |",
            sweeps[0],
            sweeps[sweeps.len() - 1],
            median / 16,
        );
    }

    println!();
    println!("## B/C. get latency (B = hits, C = hits+sweep every 1s); inserts = writer in C");
    println!(
        "| {:>9} | {:<10} | {:>10} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>9} | {:>8} | {:>7} |",
        "entries",
        "phase",
        "ops",
        "p50",
        "p99",
        "p99.9",
        "p99.99",
        "max",
        ">=100us",
        ">=1ms",
        ">=5ms"
    );
    println!(
        "|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|",
        "-".repeat(10) + ":",
        "-".repeat(12),
        "-".repeat(11) + ":",
        "-".repeat(9) + ":",
        "-".repeat(9) + ":",
        "-".repeat(9) + ":",
        "-".repeat(9) + ":",
        "-".repeat(9) + ":",
        "-".repeat(10) + ":",
        "-".repeat(9) + ":",
        "-".repeat(8) + ":",
    );
    for (n, quiet, swept, (mass, _)) in &hist_rows {
        hist_row("B hits", *n, &quiet.hits);
        hist_row("C +sweep", *n, &swept.hits);
        hist_row("C inserts", *n, &swept.inserts);
        hist_row("E massexp", *n, mass);
    }

    println!();
    println!("## E. one run_pending_tasks reclaiming N/2 expired entries");
    println!("| entries | expired | pass time |");
    println!("|---:|---:|---:|");
    for (n, _, _, (_, sweep)) in &hist_rows {
        println!("| {n} | {} | {sweep:?} |", n / 2);
    }
}
