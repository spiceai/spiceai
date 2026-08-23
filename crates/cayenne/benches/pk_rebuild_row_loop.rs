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

//! Per-row costs in the PK-index rebuild loop (`process_stream_into_keyset`).
//!
//! ## What this measures and why
//!
//! Three candidate inefficiencies in the row loop, each benched against the shape
//! that would replace it. All use the SHIPPED `RowConverter` and `hash_key_128`, so
//! the encode and digest costs are real; the keyset/bloom shapes are reimplemented
//! locally because the originals are `pub(crate)` — the comparisons are relative, so
//! the reimplementation does not affect which variant wins.
//!
//! 1. **Null check.** The loop runs `pk_columns.iter().any(|c| c.is_null(row_idx))`
//!    per row — O(#PK columns) virtual calls, essentially always false. `null/union`
//!    combines the columns' validity ONCE per batch with `NullBuffer::union` (a
//!    bitwise AND, ~1 bit/row) and drops out entirely when no column has nulls.
//!
//! 2. **Hashes per insert.** A key is currently hashed up to three times per row:
//!    `shard_of_pk` (its own seed), `pk_digest` inside `insert`, and the deletion
//!    probe. `insert/hash_once` derives the shard from the 128-bit digest instead,
//!    which is the same trick `pk_key_hashing.rs` applied to the apply path.
//!
//! 3. **Map capacity.** The builder starts at `CachedPkKeyset::with_capacity(1024)`
//!    and grows to the table's cardinality — ~18 doublings for a 337M-row table,
//!    each rehashing everything and transiently holding two tables. `insert/sized`
//!    reserves from the row count, which `live_rows_hint` now makes available.
//!
//! 4. **Destination.** `dest/bloom_direct` skips the exact phase entirely for a
//!    table known not to fit its budget: no per-row `OwnedRow` allocation, no
//!    HashMap, just the filter the degrade would have produced anyway.
//!
//! Throwaway: this exists to rank the four changes, not to guard them.

#![allow(clippy::expect_used, clippy::cast_possible_truncation, reason = "bench")]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use cayenne::row_converter::{OwnedRow, RowConverter, SortField};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hash_index::hash_key_128;

const ROWS: usize = 100_000;
const SHARDS: usize = 4;

fn converter() -> RowConverter {
    RowConverter::new(
        (0..4)
            .map(|_| SortField::new(DataType::Int64))
            .collect::<Vec<_>>(),
    )
    .expect("row converter")
}

/// Four Int64 PK columns shaped like TPC-C `order_line`.
fn pk_columns(rows: usize, with_nulls: bool) -> Vec<ArrayRef> {
    let mk = |f: &dyn Fn(i64) -> i64| -> ArrayRef {
        let vals: Vec<Option<i64>> = (0..rows as i64)
            .map(|i| {
                if with_nulls && i % 10_000 == 0 {
                    None
                } else {
                    Some(f(i))
                }
            })
            .collect();
        Arc::new(Int64Array::from(vals))
    };
    vec![
        mk(&|i| i % 1_000),
        mk(&|i| i % 10),
        mk(&|i| i / 10),
        mk(&|i| i % 15),
    ]
}

/// Today: O(#columns) virtual calls per row.
fn null_per_row(cols: &[ArrayRef], rows: usize) -> usize {
    let mut n = 0;
    for row in 0..rows {
        if cols.iter().any(|c| c.is_null(row)) {
            n += 1;
        }
    }
    n
}

/// Proposed: combine validity once per batch, then one bit test per row — and skip
/// the row loop entirely when no column carries nulls.
fn null_union(cols: &[ArrayRef], rows: usize) -> usize {
    let combined = cols
        .iter()
        .fold(None, |acc: Option<NullBuffer>, c| {
            NullBuffer::union(acc.as_ref(), c.nulls())
        });
    match combined {
        None => 0,
        Some(nb) if nb.null_count() == 0 => 0,
        Some(nb) => (0..rows).filter(|&row| nb.is_null(row)).count(),
    }
}

type Entry = (OwnedRow, i64);

/// Today: `shard_of_pk` hashes with its own seed, then `insert` hashes again.
fn insert_hash_thrice(rows_enc: &cayenne::row_converter::Rows, rows: usize, cap: usize) -> usize {
    let mut shards: Vec<HashMap<u128, Entry>> =
        (0..SHARDS).map(|_| HashMap::with_capacity(cap)).collect();
    for i in 0..rows {
        let row = rows_enc.row(i);
        let bytes = row.as_ref();
        let shard = (hash_key_128(bytes) as usize).wrapping_mul(31) % SHARDS; // stand-in for the seeded shard hash
        let digest = hash_key_128(bytes);
        shards[shard].insert(digest, (row.owned(), 0));
    }
    shards.iter().map(HashMap::len).sum()
}

/// Proposed: one hash, shard taken from its high bits.
fn insert_hash_once(rows_enc: &cayenne::row_converter::Rows, rows: usize, cap: usize) -> usize {
    let mut shards: Vec<HashMap<u128, Entry>> =
        (0..SHARDS).map(|_| HashMap::with_capacity(cap)).collect();
    for i in 0..rows {
        let row = rows_enc.row(i);
        let digest = hash_key_128(row.as_ref());
        let shard = (digest >> 96) as usize % SHARDS;
        shards[shard].insert(digest, (row.owned(), 0));
    }
    shards.iter().map(HashMap::len).sum()
}

/// Proposed: bloom-destined tables never materialise an `OwnedRow` or a map entry.
fn insert_bloom_direct(rows_enc: &cayenne::row_converter::Rows, rows: usize) -> usize {
    let blocks = (rows * 10 / 256).next_power_of_two().max(1);
    let mut filters: Vec<Vec<[u32; 8]>> = (0..SHARDS).map(|_| vec![[0u32; 8]; blocks]).collect();
    let mask = blocks as u64 - 1;
    for i in 0..rows {
        let digest = hash_key_128(rows_enc.row(i).as_ref());
        let h = digest as u64;
        let shard = (digest >> 96) as usize % SHARDS;
        let block = &mut filters[shard][(h & mask) as usize];
        let mut probe = h.rotate_left(17) | 1;
        for lane in block.iter_mut() {
            *lane |= 1u32 << (probe & 31);
            probe = probe.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13);
        }
    }
    black_box(&filters);
    rows
}

fn bench(c: &mut Criterion) {
    let conv = converter();
    let clean = pk_columns(ROWS, false);
    let dirty = pk_columns(ROWS, true);
    let encoded = conv.convert_columns(&clean).expect("encode");

    let mut g = c.benchmark_group("pk_rebuild_row_loop");
    g.throughput(Throughput::Elements(ROWS as u64));
    g.sample_size(20);

    g.bench_function("null/per_row_no_nulls", |b| {
        b.iter(|| black_box(null_per_row(&clean, ROWS)));
    });
    g.bench_function("null/union_no_nulls", |b| {
        b.iter(|| black_box(null_union(&clean, ROWS)));
    });
    g.bench_function("null/per_row_with_nulls", |b| {
        b.iter(|| black_box(null_per_row(&dirty, ROWS)));
    });
    g.bench_function("null/union_with_nulls", |b| {
        b.iter(|| black_box(null_union(&dirty, ROWS)));
    });

    g.bench_function("insert/hash_thrice_cap1024", |b| {
        b.iter(|| black_box(insert_hash_thrice(&encoded, ROWS, 1024)));
    });
    g.bench_function("insert/hash_once_cap1024", |b| {
        b.iter(|| black_box(insert_hash_once(&encoded, ROWS, 1024)));
    });
    g.bench_function("insert/hash_once_sized", |b| {
        b.iter(|| black_box(insert_hash_once(&encoded, ROWS, ROWS / SHARDS)));
    });
    g.bench_function("dest/bloom_direct", |b| {
        b.iter(|| black_box(insert_bloom_direct(&encoded, ROWS)));
    });

    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
