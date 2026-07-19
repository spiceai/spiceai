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

//! Shape bench: the `RawTable::reserve_rehash` doubling tax on the per-shard
//! upsert accumulators in `CayenneTableProvider::validate_one_shard`
//! (`crates/cayenne/src/provider/table.rs`).
//!
//! `validate_one_shard` builds two `PkDigestSet` accumulators —
//! `incoming_keys` and `kept_keys` — that grow to (about) the shard's total
//! incoming row count as it folds every sub-batch's misses (`absorb`/
//! `extend_ref`) and validated keeps. They were previously constructed
//! `PkDigestSet::default()` (capacity 0), so filling them forced a chain of
//! `hashbrown::RawTable::reserve_rehash` reallocations (grow-by-doubling: each
//! doubling reallocates the bucket array and re-inserts every live entry).
//! CPU profiling of a CH-benCHmark run attributed ~1.3% steady-state self-time
//! to `reserve_rehash`. The sibling per-batch sites already pre-size with
//! `PkDigestSet::with_capacity(batch.num_rows())`; this bench validates that
//! pre-sizing the *shard-total* accumulators the same way removes the doubling
//! chain without regressing the small-shard case.
//!
//! `PkDigestSet` is `HashMap<u128, OwnedRow, PrehashedBuildHasher>` — the u128
//! is already the key digest, so its hasher is effectively identity (no re-hash
//! on rehash). This bench models that faithfully with an identity hasher over
//! u128 so the measured cost is the bucket realloc + entry re-insert (memcpy of
//! `(u128, Box<[u8]>)` pairs), NOT SipHash recompute — matching production. A
//! default SipHash map would overstate the grow-from-zero penalty.
//!
//! ## Lanes
//! - `grow_from_zero/<rows>` — `HashMap::default()` (capacity 0), insert `rows`
//!   entries. Mirrors the old `PkDigestSet::default()` accumulator: N inserts
//!   across ~log2(N/base) reserve_rehash doublings.
//! - `presized/<rows>` — `HashMap::with_capacity(rows)`, insert `rows` entries.
//!   Mirrors the fix: one allocation, zero rehashes.
//!
//! ## How to read
//! `cargo bench --bench validate_shard_keyset_presize_rehash -p cayenne`.
//! At `rows=100_000` the `grow_from_zero` slope carries the rehash doublings;
//! `presized` is the floor (insert cost only). The delta is the per-shard
//! allocator/rehash tax the fix removes. At `rows=1_024` the two lanes should
//! be within noise — the guard that pre-sizing never hurts the small shard.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Row counts straddling realistic per-shard incoming totals: a small append,
/// a moderate coalesced burst, and an upsert-heavy shard burst at CH-benCH
/// SF100 shape (a ~100K-row coalesced `customer`/`stock` commit landing on one
/// of a few PK-hash shards).
const ROW_COUNTS: &[usize] = &[1_024, 8_192, 100_000];

/// Retained-key payload width — matches Arrow `RowConverter` output for a
/// single `Int64` PK (16 bytes incl. the 1-byte null header). The `OwnedRow`
/// (`Box<[u8]>`) is what each entry stores; the rehash re-inserts these pairs.
const KEY_WIDTH: usize = 16;

/// Identity hasher over `u128` mirroring cayenne's `PrehashedBuildHasher`: the
/// map key IS the precomputed digest, so no work is done on (re)hash.
#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Fold up to 8 bytes into the accumulator; only reached if `Hash for
        // u128` were to route through `write` rather than `write_u128`.
        let mut buf = [0u8; 8];
        let n = bytes.len().min(8);
        buf[..n].copy_from_slice(&bytes[..n]);
        self.0 ^= u64::from_le_bytes(buf);
    }
    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.0 = i as u64;
    }
}
#[derive(Clone, Default)]
struct IdentityBuild;
impl BuildHasher for IdentityBuild {
    type Hasher = IdentityHasher;
    #[inline]
    fn build_hasher(&self) -> IdentityHasher {
        IdentityHasher::default()
    }
}

/// Unique, scattered digest for row `idx` (Knuth-scramble so bucket occupancy
/// matches production cardinality, not a contiguous best case).
#[inline]
fn digest(idx: usize) -> u128 {
    let lo = (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let hi = (idx as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    (u128::from(hi) << 64) | u128::from(lo)
}

#[inline]
fn make_key(idx: usize) -> Box<[u8]> {
    let mut buf = vec![0u8; KEY_WIDTH];
    buf[..8].copy_from_slice(&(idx as u64).to_le_bytes());
    buf.into_boxed_slice()
}

/// Pre-build the `(digest, key)` entries so the per-entry `Box<[u8]>` mallocs
/// happen in criterion's UNTIMED setup. The timed routine then only does bucket
/// allocation + entry moves — isolating the `reserve_rehash` cost, which is
/// what differs between the lanes. (Both lanes pay identical malloc cost, so
/// leaving it in the timed section just adds variance that masks the delta.)
fn build_entries(rows: usize) -> Vec<(u128, Box<[u8]>)> {
    (0..rows).map(|idx| (digest(idx), make_key(idx))).collect()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_shard_keyset_presize_rehash");
    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::new("grow_from_zero", rows), &rows, |b, &rows| {
            b.iter_batched(
                || build_entries(rows),
                |entries| {
                    // Old shape: PkDigestSet::default() == capacity 0.
                    let mut map: HashMap<u128, Box<[u8]>, IdentityBuild> =
                        HashMap::with_hasher(IdentityBuild);
                    for (d, k) in entries {
                        map.insert(d, k);
                    }
                    black_box(map.len())
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("presized", rows), &rows, |b, &rows| {
            b.iter_batched(
                || build_entries(rows),
                |entries| {
                    // Fix: PkDigestSet::with_capacity(total_shard_rows).
                    let mut map: HashMap<u128, Box<[u8]>, IdentityBuild> =
                        HashMap::with_capacity_and_hasher(rows, IdentityBuild);
                    for (d, k) in entries {
                        map.insert(d, k);
                    }
                    black_box(map.len())
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
