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

//! Anchor for unifying the upsert conflict path's PK-key hashing on XXH3.
//!
//! ## What this measures and why
//!
//! Per CDC apply, the upsert on-conflict loop hashes each incoming row's
//! `RowConverter`-encoded [`OwnedRow`] byte string up to THREE times:
//!   1. the in-batch dedup pre-pass (`survivor: HashMap<&[u8], usize>`),
//!   2. the cross-batch `incoming_keys.contains(&key)` probe, and
//!   3. the existence probe `existing_keys.get(&key)`.
//!
//! These containers used the standard library's default `SipHash`
//! (`RandomState`), while the deletion index one file over already moved to
//! seeded XXH3. This bench compares three hashing strategies over the SAME real
//! `OwnedRow` keys on that three-probe-per-row shape, holding the algorithm
//! fixed while only the hasher varies (both are byte-for-byte
//! behaviour-identical — the hasher choice is unobservable):
//!
//! - `siphash_default` — `std::collections::hash_map::RandomState` (the prior
//!   state).
//! - `xxh3_buildhasher` — [`XxHash3BuildHasher`] as a drop-in `BuildHasher` over
//!   the `OwnedRow`. Rejected: it *regresses* composite PKs, because its hasher
//!   streams (slowly) once the length-prefixed `OwnedRow` encoding exceeds its
//!   32-byte inline buffer.
//! - `xxh3_prehashed_128` — the SHIPPED design: hash each row's bytes ONCE into
//!   a `u128` XXH3-128 digest and key every probe map on it via
//!   [`PrehashedBuildHasher`], mirroring `KeyDeletionIndex`. ~3.5–3.9x over
//!   `siphash_default` across both shapes and sizes.
//!
//! Composite PKs lengthen the `OwnedRow` encoding, so the hash cost scales with
//! key width — the reason this sits on the composite-PK delete-burst path. Two
//! PK shapes are measured to show that scaling:
//! - `i64` — a single 8-byte integer key (narrow).
//! - `i64+utf8` — a `(Int64, Utf8)` composite whose encoding is wider (the
//!   common CDC key shape).
//!
//! The timed operation mirrors one apply against a warm keyset: build the
//! `existing_keys` map from `n` keys, then run the three per-row probes over
//! `n` incoming rows. Keys are borrowed (never cloned), isolating hashing cost.
//!
//! A second group, `pk_key_hashing_i64_native`, evaluates the "specialize
//! integer PKs onto a cheaper integer hasher" follow-up: over NATIVE `i64` keys
//! (the `Int64Pk` identity, no collision risk), [`FxBuildHasher`] beats even the
//! u128 scheme — but capturing it would fork the keyset key type by PK strategy,
//! so it is documented as a follow-up rather than adopted here.
//!
//! `cargo bench --bench pk_key_hashing -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use cayenne::row_converter::{OwnedRow, RowConverter, SortField};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::{PrehashedBuildHasher, XxHash3BuildHasher, hash_key_128};
use rustc_hash::FxBuildHasher;

/// Row counts per apply: 100 a small burst, 1 K a moderate upsert, 10 K the
/// high-fan-out burst the composite-PK delete path hits.
const ROW_COUNTS: &[usize] = &[100, 1_000, 10_000];

/// The two PK shapes: a single narrow integer, and a wider `(Int64, Utf8)`
/// composite whose `OwnedRow` encoding is longer (so its hash costs more).
#[derive(Clone, Copy)]
enum PkShape {
    Int64,
    Int64Utf8,
}

impl PkShape {
    fn label(self) -> &'static str {
        match self {
            PkShape::Int64 => "i64",
            PkShape::Int64Utf8 => "i64+utf8",
        }
    }

    fn sort_fields(self) -> Vec<SortField> {
        match self {
            PkShape::Int64 => vec![SortField::new(DataType::Int64)],
            PkShape::Int64Utf8 => vec![
                SortField::new(DataType::Int64),
                SortField::new(DataType::Utf8),
            ],
        }
    }

    fn columns(self, rows: usize) -> Vec<ArrayRef> {
        // Well-spread values so hashing/probing touches distinct keys and the
        // allocator can't dedup them.
        let ids =
            Int64Array::from_iter_values((0..rows as i64).map(|i| i.wrapping_mul(2_654_435_761)));
        match self {
            PkShape::Int64 => vec![Arc::new(ids)],
            PkShape::Int64Utf8 => {
                let tenants = StringArray::from_iter_values(
                    (0..rows).map(|i| format!("tenant-{i:08}-region")),
                );
                vec![Arc::new(ids), Arc::new(tenants)]
            }
        }
    }
}

/// `n` distinct real `OwnedRow` PK keys for the given shape, encoded exactly as
/// the CDC apply path encodes them (`RowConverter::convert_columns`).
fn owned_rows(shape: PkShape, n: usize) -> Vec<OwnedRow> {
    let converter = RowConverter::new(shape.sort_fields()).expect("row converter");
    let rows = converter
        .convert_columns(&shape.columns(n))
        .expect("convert columns");
    (0..rows.num_rows()).map(|i| rows.row(i).owned()).collect()
}

/// One apply's PK hashing over `keys`, keyed by build hasher `S`: build the
/// `existing_keys` map, then run the three per-row probes (dedup pre-pass,
/// `incoming_keys.contains`, `existing_keys.get`) over every row. Keys are
/// borrowed (never cloned) so the measurement isolates hashing, not allocation.
fn apply_pk_probes<S: BuildHasher + Default>(keys: &[OwnedRow]) -> usize {
    let mut existing: HashMap<&OwnedRow, u64, S> =
        HashMap::with_capacity_and_hasher(keys.len(), S::default());
    for (i, key) in keys.iter().enumerate() {
        existing.insert(key, i as u64);
    }

    let mut survivor: HashMap<&[u8], usize, S> =
        HashMap::with_capacity_and_hasher(keys.len(), S::default());
    let mut incoming: HashSet<&OwnedRow, S> =
        HashSet::with_capacity_and_hasher(keys.len(), S::default());
    let mut hits = 0usize;
    for (i, key) in keys.iter().enumerate() {
        // Probe 1: in-batch dedup pre-pass.
        survivor.insert(key.as_ref(), i);
        // Probe 2: cross-batch duplicate check.
        if incoming.contains(key) {
            continue;
        }
        // Probe 3: existence probe against the cached keyset.
        if existing.contains_key(key) {
            hits += 1;
        }
        incoming.insert(key);
    }
    hits + survivor.len() + incoming.len()
}

/// The phase-2 scheme (mirrors [`KeyDeletionIndex`]): hash each row's raw
/// `OwnedRow` bytes ONCE into a `u128` XXH3-128 digest, then run all three
/// probes through `u128`-keyed maps fronted by [`PrehashedBuildHasher`] (which
/// passes the digest's own entropy through instead of re-hashing). One hash per
/// row serves the dedup pre-pass, `incoming_keys`, and `existing_keys`.
fn apply_pk_probes_prehashed(keys: &[OwnedRow]) -> usize {
    let mut existing: HashMap<u128, u64, PrehashedBuildHasher> =
        HashMap::with_capacity_and_hasher(keys.len(), PrehashedBuildHasher);
    for (i, key) in keys.iter().enumerate() {
        existing.insert(hash_key_128(key.as_ref()), i as u64);
    }

    let mut survivor: HashMap<u128, usize, PrehashedBuildHasher> =
        HashMap::with_capacity_and_hasher(keys.len(), PrehashedBuildHasher);
    let mut incoming: HashSet<u128, PrehashedBuildHasher> =
        HashSet::with_capacity_and_hasher(keys.len(), PrehashedBuildHasher);
    let mut hits = 0usize;
    for (i, key) in keys.iter().enumerate() {
        // One hash computation per row, reused across all three probes.
        let digest = hash_key_128(key.as_ref());
        survivor.insert(digest, i);
        if incoming.contains(&digest) {
            continue;
        }
        if existing.contains_key(&digest) {
            hits += 1;
        }
        incoming.insert(digest);
    }
    hits + survivor.len() + incoming.len()
}

/// The same three-probe apply shape over NATIVE `i64` keys (the exact identity
/// for an `Int64Pk`-strategy table), letting the map's build hasher `S` hash the
/// 8 bytes directly. Evaluates the "specialize integer PKs onto a cheaper
/// integer hasher" idea: `i64` is an exact identity, so — unlike the u128
/// digest — there is no collision concern, and no separate digest pass.
fn apply_pk_probes_i64<S: BuildHasher + Default>(keys: &[i64]) -> usize {
    let mut existing: HashMap<i64, u64, S> =
        HashMap::with_capacity_and_hasher(keys.len(), S::default());
    for (i, &key) in keys.iter().enumerate() {
        existing.insert(key, i as u64);
    }

    let mut survivor: HashMap<i64, usize, S> =
        HashMap::with_capacity_and_hasher(keys.len(), S::default());
    let mut incoming: HashSet<i64, S> = HashSet::with_capacity_and_hasher(keys.len(), S::default());
    let mut hits = 0usize;
    for (i, &key) in keys.iter().enumerate() {
        survivor.insert(key, i);
        if incoming.contains(&key) {
            continue;
        }
        if existing.contains_key(&key) {
            hits += 1;
        }
        incoming.insert(key);
    }
    hits + survivor.len() + incoming.len()
}

fn i64_keys(n: usize) -> Vec<i64> {
    (0..n as i64)
        .map(|i| i.wrapping_mul(2_654_435_761))
        .collect()
}

/// Compares hashers for the native-`i64` PK path: does a cheap integer hasher
/// (`FxBuildHasher`) or seeded XXH3 on the raw `i64` beat the uniform u128
/// prehashed scheme for `Int64Pk` tables?
fn bench_i64_native(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_key_hashing_i64_native");
    for &n in ROW_COUNTS {
        let keys = i64_keys(n);
        group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

        group.bench_with_input(BenchmarkId::new("siphash_default", n), &n, |b, _| {
            b.iter(|| black_box(apply_pk_probes_i64::<RandomState>(black_box(&keys))));
        });
        group.bench_with_input(BenchmarkId::new("xxh3_buildhasher", n), &n, |b, _| {
            b.iter(|| black_box(apply_pk_probes_i64::<XxHash3BuildHasher>(black_box(&keys))));
        });
        group.bench_with_input(BenchmarkId::new("fxhash", n), &n, |b, _| {
            b.iter(|| black_box(apply_pk_probes_i64::<FxBuildHasher>(black_box(&keys))));
        });
    }
    group.finish();
}

fn bench_pk_key_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_key_hashing");
    for shape in [PkShape::Int64, PkShape::Int64Utf8] {
        for &n in ROW_COUNTS {
            let keys = owned_rows(shape, n);
            group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

            group.bench_with_input(
                BenchmarkId::new(format!("siphash_default/{}", shape.label()), n),
                &n,
                |b, _| {
                    b.iter(|| black_box(apply_pk_probes::<RandomState>(black_box(&keys))));
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("xxh3_buildhasher/{}", shape.label()), n),
                &n,
                |b, _| {
                    b.iter(|| black_box(apply_pk_probes::<XxHash3BuildHasher>(black_box(&keys))));
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("xxh3_prehashed_128/{}", shape.label()), n),
                &n,
                |b, _| {
                    b.iter(|| black_box(apply_pk_probes_prehashed(black_box(&keys))));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_pk_key_hashing, bench_i64_native);
criterion_main!(benches);
