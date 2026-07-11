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

//! Per-value NDV (`HyperLogLog`) hashing cost, in isolation from the sketch's
//! register update.
//!
//! ## What this measures and why
//!
//! Directional SF10 HTAP profiling showed `hash_index::hash_key_bytes` +
//! streaming XXH3-64 at ~6.3% of compaction `spiced` CPU, driven entirely by
//! `ColumnStatsAccumulator::add_column_to_hll`
//! (`crates/cayenne/src/provider/column_stats.rs`): every non-null value of
//! every NDV-tracked column calls `HyperLogLog::add_i128`/`add_bytes`
//! (`crates/cayenne/src/hll.rs`), each of which used to construct a fresh
//! streaming `XxHash3_64` hasher for a single small write — one 16-byte `i128`
//! or one short string.
//!
//! This bench compares three ways to produce that one hash, over the same
//! value shapes `add_i128`/`add_bytes` see:
//!
//! - `streaming_xxh3` — the prior path: `hash_index::hash_key_bytes`, which
//!   constructs a fresh streaming `XxHash3_64` hasher and does one `write` +
//!   `finish` per value.
//! - `oneshot_xxh3` — the SHIPPED fix: `hash_index::hash_key_bytes_oneshot`
//!   (`XxHash3_64::oneshot_with_seed`), skipping the streaming hasher's setup
//!   cost. Byte-identical to `streaming_xxh3` (pinned by
//!   `hash_key_bytes_oneshot_matches_streaming` in `hash-index`), so this is a
//!   pure constant-factor win — persisted sketches are unaffected.
//! - `fxhash` — `rustc_hash::FxHasher`, evaluated per the task but NOT
//!   adopted: it produces different hash values than XXH3, so switching would
//!   desync newly folded registers from already-persisted sketches (merged
//!   over time — see `NdvSketches::merge`/`merge_serialized`) unless the
//!   sketch format version were bumped and every existing sketch rebuilt. Kept
//!   here only as an evaluation data point.
//!
//! Two value shapes, matching the two `add_column_to_hll` fold arms:
//! - `i128` — the integer/temporal fold arm (`add_i128`), a fixed 16-byte key.
//! - `utf8` — the string fold arm (`add_bytes`), a variable-length key.
//!
//! `cargo bench --bench hll_ndv_hashing -p cayenne`.

use std::hash::Hasher;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::{hash_key_bytes, hash_key_bytes_oneshot};
use rustc_hash::FxHasher;

/// Values per bench iteration: small (single CDC burst column), large
/// (compaction-sized fold).
const VALUE_COUNTS: &[usize] = &[1_000, 100_000];

fn i128_values(n: usize) -> Vec<[u8; 16]> {
    (0..n as i128)
        .map(|v| v.wrapping_mul(2_654_435_761).to_le_bytes())
        .collect()
}

fn utf8_values(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("name-{i:08}").into_bytes())
        .collect()
}

fn fold_streaming(values: &[impl AsRef<[u8]>]) -> u64 {
    let mut acc = 0u64;
    for v in values {
        acc ^= hash_key_bytes(&[v.as_ref()]);
    }
    acc
}

fn fold_oneshot(values: &[impl AsRef<[u8]>]) -> u64 {
    let mut acc = 0u64;
    for v in values {
        acc ^= hash_key_bytes_oneshot(v.as_ref());
    }
    acc
}

fn fold_fxhash(values: &[impl AsRef<[u8]>]) -> u64 {
    let mut acc = 0u64;
    for v in values {
        let mut hasher = FxHasher::default();
        hasher.write(v.as_ref());
        acc ^= hasher.finish();
    }
    acc
}

fn bench_hll_ndv_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hll_ndv_hashing");

    for &n in VALUE_COUNTS {
        let values = i128_values(n);
        group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

        group.bench_with_input(BenchmarkId::new("streaming_xxh3/i128", n), &n, |b, _| {
            b.iter(|| black_box(fold_streaming(black_box(&values))));
        });
        group.bench_with_input(BenchmarkId::new("oneshot_xxh3/i128", n), &n, |b, _| {
            b.iter(|| black_box(fold_oneshot(black_box(&values))));
        });
        group.bench_with_input(BenchmarkId::new("fxhash/i128", n), &n, |b, _| {
            b.iter(|| black_box(fold_fxhash(black_box(&values))));
        });
    }

    for &n in VALUE_COUNTS {
        let values = utf8_values(n);
        group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

        group.bench_with_input(BenchmarkId::new("streaming_xxh3/utf8", n), &n, |b, _| {
            b.iter(|| black_box(fold_streaming(black_box(&values))));
        });
        group.bench_with_input(BenchmarkId::new("oneshot_xxh3/utf8", n), &n, |b, _| {
            b.iter(|| black_box(fold_oneshot(black_box(&values))));
        });
        group.bench_with_input(BenchmarkId::new("fxhash/utf8", n), &n, |b, _| {
            b.iter(|| black_box(fold_fxhash(black_box(&values))));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_hll_ndv_hashing);
criterion_main!(benches);
