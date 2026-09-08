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

//! Out-of-cache probe bench for the deletion index.
//!
//! The companion `deletion_index_probe` bench builds indexes of at most one
//! probe batch (8192 entries), so every tier is cache-resident and the numbers
//! mostly measure the hash pipeline. This bench builds indexes of millions of
//! entries — the SF10 changes-mode regime where per-row deletion probes were
//! profiled as the dominant executor cost — so present-key probes pay the real
//! memory-hierarchy price of the backing map.
//!
//! Lanes per index size:
//! - `hit`: every probed key is present (the upsert-churn scan shape, where
//!   superseded rows genuinely populate the index). This is the lane that the
//!   layered frozen-base design targets: a flat-table lookup instead of a
//!   multi-level HAMT pointer chase.
//! - `miss`: no probed key is present (append-mostly scan shape) — dominated
//!   by the bloom filter's rejection path.
//!
//! Probed keys are spread with a large odd multiplier so consecutive probes
//! touch unrelated cache lines (no streaming-prefetch flattery).
//!
//! `cargo bench --bench deletion_index_probe_large -p cayenne`.

use std::collections::HashMap;
use std::hint::black_box;

use cayenne::provider::deletion_index::{DeletionIndex, KeyDeletionIndex};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Probed keys per iteration (one Arrow batch worth).
const PROBES_PER_BATCH: usize = 8_192;

/// Index entry counts. 1M ≈ tens of MB resident (out of L2), 4M ≈ out of L3
/// on most parts.
const INDEX_SIZES: &[usize] = &[1_000_000, 4_000_000];

/// Large odd stride so probe i touches an unpredictable slot of the key space.
const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

fn spread_key(i: usize, modulus: usize) -> i64 {
    let mixed = (i as u64).wrapping_mul(SPREAD);
    i64::try_from(mixed % (modulus as u64)).unwrap_or(0)
}

fn bench_int64_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_int64_probe_large");
    group.throughput(Throughput::Elements(PROBES_PER_BATCH as u64));
    group.sample_size(50);

    for &n in INDEX_SIZES {
        let index = DeletionIndex::from_map(
            (0..n)
                .map(|pk| (i64::try_from(pk).unwrap_or(0), 1_i64))
                .collect::<HashMap<i64, i64>>(),
        );

        // Present keys, spread across the whole index.
        let hit_keys: Vec<i64> = (0..PROBES_PER_BATCH).map(|i| spread_key(i, n)).collect();
        // Absent keys, beyond the populated range.
        let miss_keys: Vec<i64> = (0..PROBES_PER_BATCH)
            .map(|i| spread_key(i, n) + i64::try_from(2 * n).unwrap_or(i64::MAX / 2))
            .collect();

        group.bench_with_input(BenchmarkId::new("hit", n), &n, |b, _| {
            b.iter(|| {
                let mut found = 0_usize;
                for &pk in &hit_keys {
                    found += usize::from(index.get(black_box(pk)).is_some());
                }
                black_box(found);
            });
        });

        group.bench_with_input(BenchmarkId::new("miss", n), &n, |b, _| {
            b.iter(|| {
                let mut found = 0_usize;
                for &pk in &miss_keys {
                    found += usize::from(index.get(black_box(pk)).is_some());
                }
                black_box(found);
            });
        });
    }

    group.finish();
}

fn bench_row_keys_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_index_row_keys_probe_large");
    group.throughput(Throughput::Elements(PROBES_PER_BATCH as u64));
    group.sample_size(50);

    for &n in INDEX_SIZES {
        let index = KeyDeletionIndex::from_map(
            (0..n)
                .map(|key| {
                    (
                        (key as u64).to_be_bytes().to_vec().into_boxed_slice(),
                        1_i64,
                    )
                })
                .collect::<HashMap<Box<[u8]>, i64>>(),
        );

        let hit_keys: Vec<[u8; 8]> = (0..PROBES_PER_BATCH)
            .map(|i| (spread_key(i, n) as u64).to_be_bytes())
            .collect();
        let miss_keys: Vec<[u8; 8]> = (0..PROBES_PER_BATCH)
            .map(|i| ((spread_key(i, n) as u64) + 2 * (n as u64)).to_be_bytes())
            .collect();

        group.bench_with_input(BenchmarkId::new("hit", n), &n, |b, _| {
            b.iter(|| {
                let mut found = 0_usize;
                for key in &hit_keys {
                    found += usize::from(index.get(black_box(key)).is_some());
                }
                black_box(found);
            });
        });

        group.bench_with_input(BenchmarkId::new("miss", n), &n, |b, _| {
            b.iter(|| {
                let mut found = 0_usize;
                for key in &miss_keys {
                    found += usize::from(index.get(black_box(key)).is_some());
                }
                black_box(found);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_int64_large, bench_row_keys_large);
criterion_main!(benches);
