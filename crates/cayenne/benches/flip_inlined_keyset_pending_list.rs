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

//! Shape bench: the post-inline-checkpoint `Inlined -> FileUnlocated` flip in
//! `CayenneTableProvider::flip_inlined_keyset_entries_to_file_unlocated`
//! (`crates/cayenne/src/provider/table.rs`) →
//! `CachedPkKeyset::flip_inlined_to_file_unlocated`
//! (`crates/cayenne/src/provider/pk_index.rs`).
//!
//! After an inline checkpoint flushes the memtable to Vortex files, every
//! keyset entry stamped `RowLocation::Inlined` must become `FileUnlocated` so a
//! later upsert tombstones the flushed key by a key-based deletion vector
//! instead of a phantom inline conflict. The previous implementation scanned
//! the ENTIRE resident keyset (`keys.values_mut()`, O(total keys)) checking
//! each `RowLocation` — but a checkpoint flushes only the keys written since
//! the last one, so on a large table the vast majority of iterations inspect an
//! already-`FileUnlocated`/`FilePositioned` entry and do nothing. CPU profiling
//! of a CH-benCHmark run attributed ~12% of the compaction-stage self-time to
//! this scan.
//!
//! The fix tracks the digests stamped `Inlined` since the last flip in a
//! `Vec<u128>` (`inlined_digests`) and drains it — O(recently-inlined) — with
//! an overflow guard (`keys.len()` cap) that falls back to the full scan so
//! worst-case memory and time never regress below the old behavior.
//!
//! ## Lanes (per `<resident>/<inlined>` scenario)
//! - `full_scan` — the OLD shape: iterate every entry, flip if `Inlined`.
//!   Cost scales with `resident`, independent of how many are actually inlined.
//! - `pending_drain` — the NEW shape: drain a `Vec<u128>` of the inlined
//!   digests, `get_mut` + flip each. Cost scales with `inlined`.
//!
//! ## How to read
//! `cargo bench --bench flip_inlined_keyset_pending_list -p cayenne`.
//! The realistic checkpoint case is a large `resident` with a small `inlined`
//! (a big table flushing a recent batch): `1000000/1000` and `100000/1000`.
//! There `pending_drain` should be orders of magnitude faster than `full_scan`.
//! The `<n>/<n>` all-inlined scenario is the overflow/worst case — the two
//! lanes should converge (the fix's guard makes it fall back to the scan), the
//! guard that the change never regresses.
//!
//! Models the exact algorithmic difference with std types (the real
//! `CachedPkKeyset` is `pub(crate)`); the map mirrors
//! `HashMap<u128, PkKeysetEntry, PrehashedBuildHasher>` — u128 digest key,
//! identity hasher (the digest is the precomputed hash), a `RowLocation`-shaped
//! enum in the value.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// `(resident_keys, inlined_since_checkpoint)` scenarios. The first three are
/// the realistic large-table-small-flush case; the `<n>/<n>` pair is the
/// all-inlined overflow/worst case (fix falls back to the scan → lanes match).
const SCENARIOS: &[(usize, usize)] = &[
    (1_000_000, 1_000),
    (1_000_000, 100_000),
    (100_000, 1_000),
    (100_000, 100_000),
];

/// Mirror of `cayenne::provider::pk_index::RowLocation` (same variant shapes so
/// the value width / `matches!` cost is representative).
#[derive(Clone)]
enum Loc {
    Inlined,
    FileUnlocated,
    #[allow(dead_code)]
    FilePositioned {
        file_path: Arc<str>,
        position: u64,
    },
}

/// Mirror of `PkKeysetEntry` — an owned key blob, a location, a sequence.
#[derive(Clone)]
struct Entry {
    #[allow(dead_code)]
    row: Box<[u8]>,
    location: Loc,
    #[allow(dead_code)]
    sequence: i64,
}

/// Identity hasher over `u128`, mirroring cayenne's `PrehashedBuildHasher`.
#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
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

type KeyMap = HashMap<u128, Entry, IdentityBuild>;

#[inline]
fn digest(idx: usize) -> u128 {
    let lo = (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let hi = (idx as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    (u128::from(hi) << 64) | u128::from(lo)
}

/// Build a keyset of `resident` entries, the first `inlined` of which are
/// `Inlined` (the rest `FileUnlocated`), plus the digest list the fast flip
/// would have accumulated for those inlined keys.
fn build(resident: usize, inlined: usize) -> (KeyMap, Vec<u128>) {
    let mut map: KeyMap = HashMap::with_capacity_and_hasher(resident, IdentityBuild);
    let mut pending: Vec<u128> = Vec::with_capacity(inlined);
    for idx in 0..resident {
        let d = digest(idx);
        let location = if idx < inlined {
            pending.push(d);
            Loc::Inlined
        } else {
            Loc::FileUnlocated
        };
        map.insert(
            d,
            Entry {
                row: (idx as u64).to_le_bytes().into(),
                location,
                sequence: 0,
            },
        );
    }
    (map, pending)
}

/// OLD: scan every entry, flip if `Inlined`. Returns flip count.
fn full_scan(map: &mut KeyMap) -> usize {
    let mut flipped = 0;
    for entry in map.values_mut() {
        if matches!(entry.location, Loc::Inlined) {
            entry.location = Loc::FileUnlocated;
            flipped += 1;
        }
    }
    flipped
}

/// NEW: the shipped `CachedPkKeyset::flip_inlined_to_file_unlocated` shape.
/// Drains the digest list (`get_mut` + flip each) — UNLESS the list is as large
/// as the resident set (the `inlined_overflow` guard), in which case the linear
/// `values_mut` scan is no more expensive and has better cache locality, so we
/// fall back to it. This is why the all-inline (`<n>/<n>`) scenario never
/// regresses below `full_scan`.
fn pending_drain(map: &mut KeyMap, pending: &mut Vec<u128>) -> usize {
    if pending.len() >= map.len() {
        // Overflow guard: fall back to the full scan (identical result).
        pending.clear();
        return full_scan(map);
    }
    let mut flipped = 0;
    for d in pending.drain(..) {
        if let Some(entry) = map.get_mut(&d) {
            if matches!(entry.location, Loc::Inlined) {
                entry.location = Loc::FileUnlocated;
                flipped += 1;
            }
        }
    }
    flipped
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("flip_inlined_keyset_pending_list");
    for &(resident, inlined) in SCENARIOS {
        let label = format!("{resident}/{inlined}");
        // Throughput = keys the flip must reconcile (inlined) — the useful work.
        group.throughput(Throughput::Elements(inlined as u64));

        // iter_batched_ref: the built map is passed by &mut and dropped by
        // criterion OUTSIDE the timed section. iter_batched (by-value) would
        // drop the resident-key map (up to 1M Box<[u8]> frees) INSIDE the
        // measurement, swamping the flip cost this bench isolates.
        group.bench_with_input(BenchmarkId::new("full_scan", &label), &(), |b, ()| {
            b.iter_batched_ref(
                || build(resident, inlined).0,
                |map| black_box(full_scan(map)),
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("pending_drain", &label), &(), |b, ()| {
            b.iter_batched_ref(
                || build(resident, inlined),
                |(map, pending)| black_box(pending_drain(map, pending)),
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
