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

// Benchmark code has different lint requirements than production code
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

//! Baseline micro-benchmarks for the split-block bloom filter
//! ([`SplitBlockBloomFilter`]) hot loops — the scalar 8-word `insert` /
//! `might_contain` block loops that were flagged as an AVX2/SIMD candidate.
//!
//! Purpose: establish the pre-SIMD baseline AND answer the decisive question
//! before writing any `unsafe` — by sweeping the filter size across the cache
//! hierarchy. A split-block probe touches exactly one 32-byte block chosen by a
//! random hash, so:
//!   - if throughput stays high as the filter grows past the LLC, the probe is
//!     **compute-bound** (the 8-word block loop dominates) and SIMD-within-a-
//!     block can help;
//!   - if throughput collapses once the filter exceeds cache, the probe is
//!     **memory-latency-bound** (one cache miss per probe dominates the 8-word
//!     compute), and SIMD-within-a-block cannot help the real (large-filter)
//!     case — which also moots the atomic-load soundness question.
//!
//! `miss` (all-absent keys) exercises the common bloom fast-reject path (the
//! `.all()` short-circuits at the first unset word); `hit` (all-present keys)
//! forces all 8 word loads + compares.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::{SplitBlockBloomFilter, hash_key};
use std::hint::black_box;

/// One `RecordBatch` worth of probe keys — the deletion filter probes a PK
/// column a batch at a time.
const PROBE_BATCH: usize = 8_192;

/// Filter capacities spanning the cache hierarchy. Filter bytes ≈ capacity * 2
/// (16 bits/item, packed into 32-byte blocks), so: ~16 KB (fits L1), ~1.6 MB
/// (L2/LLC), ~80 MB (far exceeds any LLC → RAM-bound random block access).
const SIZES: [usize; 3] = [8_192, 800_000, 40_000_000];

fn build_filter(n: usize) -> SplitBlockBloomFilter {
    let filter = SplitBlockBloomFilter::new(n);
    for k in 0..n as i64 {
        filter.insert(hash_key(&k));
    }
    filter
}

/// Probe throughput (miss and hit) across filter sizes. This is the exact shape
/// a sound frozen-snapshot SIMD batch-probe would target: sweep a batch of
/// precomputed hashes, counting survivors.
fn bench_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("sbbf_probe");
    group.throughput(Throughput::Elements(PROBE_BATCH as u64));

    for n in SIZES {
        let filter = build_filter(n);

        // All-miss: keys just past the inserted range (bar the ~0.04% FPR).
        let miss: Vec<u64> = (n as i64..(n as i64 + PROBE_BATCH as i64))
            .map(|k| hash_key(&k))
            .collect();
        group.bench_with_input(BenchmarkId::new("miss", n), &n, |b, _| {
            b.iter(|| {
                let mut survivors = 0_usize;
                for &h in &miss {
                    survivors += usize::from(filter.might_contain(h));
                }
                black_box(survivors);
            });
        });

        // All-hit: keys known to be present (forces all 8 word checks).
        let hit: Vec<u64> = (0..PROBE_BATCH as i64)
            .map(|i| hash_key(&(i % n as i64)))
            .collect();
        group.bench_with_input(BenchmarkId::new("hit", n), &n, |b, _| {
            b.iter(|| {
                let mut survivors = 0_usize;
                for &h in &hit {
                    survivors += usize::from(filter.might_contain(h));
                }
                black_box(survivors);
            });
        });
    }
    group.finish();
}

/// Insert throughput: build a filter of `PROBE_BATCH` keys. Baselines the
/// writer's 8-word `fetch_or` block loop (the filter is sized to the batch so
/// allocation does not dominate). Insert can't be soundly SIMD'd — the per-word
/// atomics are load-bearing for lock-free probe-during-insert — so this is
/// context, not a SIMD candidate.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sbbf_insert");
    group.throughput(Throughput::Elements(PROBE_BATCH as u64));

    let keys: Vec<u64> = (0..PROBE_BATCH as i64).map(|k| hash_key(&k)).collect();
    group.bench_function("build_batch", |b| {
        b.iter(|| {
            let filter = SplitBlockBloomFilter::new(PROBE_BATCH);
            for &h in &keys {
                filter.insert(h);
            }
            black_box(filter);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_probe, bench_insert);
criterion_main!(benches);
