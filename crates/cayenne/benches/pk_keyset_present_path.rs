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

//! Before/after regression bench for BEN-18 —
//! `CayenneTableProvider::record_pk_keys_with_location`'s keyset upsert on the
//! "already present" path (CDC update traffic re-touching existing PKs, e.g.
//! `payment`/`delivery`).
//!
//! `record_pk_keys_with_location`'s inner loop (`crates/cayenne/src/provider/table.rs`)
//! called `CachedPkKeyset::contains_digest` (one hash) then unconditionally
//! `CachedPkKeyset::insert_with_digest` (a second hash via `entry()`, plus an
//! `OwnedRow` clone) for every key — even when the key already existed and the
//! insert only had to overwrite its `RowLocation`. The fix
//! (`CachedPkKeyset::try_insert_with_digest`) folds both into a single `entry()`
//! lookup and clones the key only on the vacant (new-key) branch.
//!
//! `pk_index` is `pub(crate)`, so a bench (a separate crate) can't reach
//! `CachedPkKeyset` directly — this reproduces its digest-keyed
//! `HashMap<u128, _, PrehashedBuildHasher>` shape and both call patterns
//! locally, byte-for-byte matching the production methods, so only the code
//! shape being compared varies:
//!
//! - `before_contains_then_insert`: the pre-fix shape.
//! - `after_try_insert`: the shipped fix.
//!
//! The keyset is pre-populated with all `n` keys, then every iteration
//! re-touches those SAME `n` keys — the miss/insert branch (and its clone)
//! never executes on either lane, isolating the present-path cost the ticket
//! profiled. `cargo bench --bench pk_keyset_present_path -p cayenne`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use cayenne::row_converter::{OwnedRow, RowConverter, SortField};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::{PrehashedBuildHasher, hash_key_128};

/// Key counts per apply: 100 a small burst, 1 K a moderate upsert, 10 K the
/// high-fan-out CDC-update burst.
const KEY_COUNTS: &[usize] = &[100, 1_000, 10_000];

/// The two PK shapes: a single narrow integer, and a wider `(Int64, Utf8)`
/// composite whose `OwnedRow` encoding is longer (so a clone costs more).
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

/// `n` distinct real `(digest, OwnedRow)` PK keys for the given shape, encoded
/// exactly as the CDC apply path encodes them (`RowConverter::convert_columns`).
fn digest_keys(shape: PkShape, n: usize) -> Vec<(u128, OwnedRow)> {
    let converter = RowConverter::new(shape.sort_fields()).expect("row converter");
    let rows = converter
        .convert_columns(&shape.columns(n))
        .expect("convert columns");
    (0..rows.num_rows())
        .map(|i| {
            let row = rows.row(i).owned();
            let digest = hash_key_128(row.as_ref());
            (digest, row)
        })
        .collect()
}

/// Mirrors `pk_index::PkKeysetEntry`: the retained key bytes plus a location
/// tag (a `u8` stand-in for the real `RowLocation` enum — irrelevant to the
/// hash/clone cost this bench isolates).
struct LocalEntry {
    row: OwnedRow,
    location: u8,
}

/// Mirrors `pk_index::CachedPkKeyset`'s digest-keyed map (minus the
/// `approx_bytes`/`captured_files` bookkeeping, which neither call pattern
/// touches on the present path).
struct LocalKeyset {
    keys: HashMap<u128, LocalEntry, PrehashedBuildHasher>,
}

impl LocalKeyset {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: HashMap::with_capacity_and_hasher(capacity, PrehashedBuildHasher),
        }
    }

    /// Mirrors the pre-fix `CachedPkKeyset::contains_digest`.
    #[inline]
    fn contains_digest(&self, digest: u128) -> bool {
        self.keys.contains_key(&digest)
    }

    /// Mirrors `CachedPkKeyset::insert_with_digest` (unchanged by the fix; the
    /// bug was calling it after an already-redundant `contains_digest` probe).
    #[inline]
    fn insert_with_digest(&mut self, digest: u128, key: OwnedRow, location: u8) {
        match self.keys.entry(digest) {
            Entry::Occupied(mut entry) => entry.get_mut().location = location,
            Entry::Vacant(entry) => {
                entry.insert(LocalEntry { row: key, location });
            }
        }
    }

    /// Mirrors the shipped fix, `CachedPkKeyset::try_insert_with_digest`
    /// (byte-budget check omitted: it never evaluates on the present path this
    /// bench measures, since the `&&` short-circuits on `Occupied`).
    #[inline]
    fn try_insert_with_digest(&mut self, digest: u128, key: &OwnedRow, location: u8) {
        match self.keys.entry(digest) {
            Entry::Occupied(mut entry) => entry.get_mut().location = location,
            Entry::Vacant(entry) => {
                entry.insert(LocalEntry {
                    row: key.clone(),
                    location,
                });
            }
        }
    }

    /// Reads back every retained key's bytes — used only to give each bench
    /// iteration an observable result (so the optimizer can't treat a lane's
    /// `OwnedRow` clones as dead stores) and as a correctness checksum that
    /// both lanes retain identical keyset contents.
    fn total_row_bytes(&self) -> usize {
        self.keys
            .values()
            .map(|entry| entry.row.as_ref().len())
            .sum()
    }
}

/// BEFORE: `contains_digest` (hash #1) then unconditional `insert_with_digest`
/// (hash #2 via `entry()`, plus an unconditional `key.clone()`).
fn before_contains_then_insert(keyset: &mut LocalKeyset, keys: &[(u128, OwnedRow)]) {
    for (digest, key) in keys {
        if !keyset.contains_digest(*digest) {
            // Over-budget branch — never taken here; every key is already
            // present, matching the production hot path this bench profiles.
        }
        keyset.insert_with_digest(*digest, key.clone(), 1);
    }
}

/// AFTER: one `entry()` hash lookup; the present (`Occupied`) branch only
/// overwrites the location — no clone.
fn after_try_insert(keyset: &mut LocalKeyset, keys: &[(u128, OwnedRow)]) {
    for (digest, key) in keys {
        keyset.try_insert_with_digest(*digest, key, 1);
    }
}

fn bench_pk_keyset_present_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_keyset_present_path");
    for shape in [PkShape::Int64, PkShape::Int64Utf8] {
        for &n in KEY_COUNTS {
            let keys = digest_keys(shape, n);
            group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

            group.bench_with_input(
                BenchmarkId::new(format!("before_contains_then_insert/{}", shape.label()), n),
                &n,
                |b, _| {
                    let mut keyset = LocalKeyset::with_capacity(n);
                    for (digest, key) in &keys {
                        keyset.insert_with_digest(*digest, key.clone(), 0);
                    }
                    b.iter(|| {
                        before_contains_then_insert(black_box(&mut keyset), black_box(&keys));
                        black_box(keyset.total_row_bytes())
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("after_try_insert/{}", shape.label()), n),
                &n,
                |b, _| {
                    let mut keyset = LocalKeyset::with_capacity(n);
                    for (digest, key) in &keys {
                        keyset.insert_with_digest(*digest, key.clone(), 0);
                    }
                    b.iter(|| {
                        after_try_insert(black_box(&mut keyset), black_box(&keys));
                        black_box(keyset.total_row_bytes())
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_pk_keyset_present_path);
criterion_main!(benches);
