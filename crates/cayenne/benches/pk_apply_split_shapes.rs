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

//! Per-row costs in the CDC apply path's bloom split (`bloom_split_shard_batch`)
//! and the rebuild builder's budget check.
//!
//! ## What this measures and why
//!
//! The rebuild runs once per index discard; the apply path runs on every coalesced
//! CDC batch, so the same per-row shapes matter more here. Three of them:
//!
//! 1. **Owning the key.** The split loop takes `rows.row(i).owned()` — a heap
//!    allocation per row — before deciding anything, but every probe it then runs
//!    (cold bloom, warm bloom, digest sets) reads `&[u8]`. Only a MISS row keeps
//!    the key. `split/borrowed` owns on the keeping branch instead, so the
//!    allocation count follows the MISS fraction rather than the row count. Benched
//!    at three MISS fractions because that is exactly what the change trades on:
//!    an all-HIT batch is the steady-state upsert shape.
//!
//! 2. **The mask.** `Vec<bool>` (a byte per row), cloned into the MISS predicate,
//!    then a second `Vec<bool>` for the complement. `split/*_bits` builds one
//!    `BooleanBufferBuilder` (a bit per row) and negates it with an Arrow kernel.
//!
//! 3. **The budget check.** `BoundedShardedPkIndexBuilder` summed EVERY shard's
//!    `approx_bytes` on EVERY insert to compare against the budget — O(shards) per
//!    row. `budget/running_total` folds the touched shard's delta into a running
//!    sum instead. Benched across shard counts because the cost is proportional to
//!    them.
//!
//! ## Measured (M-series, 8192-row batch, 4-column Int64 PK)
//!
//! | shape | before | after | |
//! |---|---|---|---|
//! | `split` all-HIT | 174.4 us | 51.2 us | 3.41x |
//! | `split` half-HIT | 232.2 us | 149.7 us | 1.55x |
//! | `split` all-MISS | 297.1 us | 226.8 us | 1.31x |
//! | `budget` 4 shards | 54.6 us | 43.2 us | 1.27x |
//! | `budget` 16 shards | 91.3 us | 43.2 us | 2.11x |
//! | `budget` 64 shards | 145.5 us | 43.4 us | 3.35x |
//!
//! The split gradient is the point: the win tracks the HIT fraction, because a HIT
//! row is exactly the one whose allocation is now skipped, and an all-HIT batch is
//! the steady-state upsert shape. The budget row is flat at ~43 us across all three
//! shard counts, which is what O(1) looks like.
//!
//! `borrowed_boolbuilder` answers "why not `BooleanArray::builder`": its
//! `append_value` also pokes a `NullBufferBuilder` on every row, which this mask
//! never needs. It measures +2.4% on the all-HIT and half-HIT shapes and +0.9% on
//! all-MISS -- small, but consistent, non-overlapping, and largest exactly where
//! the mask is the biggest share of the loop. `BooleanBufferBuilder` also lets the
//! predicate be built as `BooleanArray::new(buffer, None)`, which states at the
//! construction site that a filter predicate cannot be null, rather than leaving
//! it a property of nobody having called `append_null`.
//!
//! Throwaway: this exists to rank the changes, not to guard them.

#![allow(
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "bench"
)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::row_converter::{OwnedRow, RowConverter, Rows, SortField};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hash_index::hash_key_128;

const ROWS: usize = 8_192;

/// Stand-in for `PkBloom` sized as the shipped filter is (~10 bits/key, split
/// block). Only its cost shape matters here, not its exact bit pattern.
struct Bloom {
    blocks: Vec<[u32; 8]>,
    mask: u64,
    /// Fraction of probes forced to report a hit, so the bench can hold the MISS
    /// rate at an exact target rather than at whatever the filter happens to give.
    hit_every: usize,
}

impl Bloom {
    fn new(keys: usize, hit_every: usize) -> Self {
        let blocks = (keys * 10 / 256).next_power_of_two().max(1);
        Self {
            blocks: vec![[0u32; 8]; blocks],
            mask: blocks as u64 - 1,
            hit_every,
        }
    }

    fn maybe_contains(&self, key: &[u8], row: usize) -> bool {
        if self.hit_every != 0 && row % self.hit_every == 0 {
            return true;
        }
        let h = hash_key_128(key) as u64;
        let block = &self.blocks[(h & self.mask) as usize];
        let mut probe = h.rotate_left(17) | 1;
        block.iter().all(|lane| {
            let bit = 1u32 << (probe & 31);
            probe = probe.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(13);
            lane & bit != 0
        })
    }
}

fn converter() -> RowConverter {
    RowConverter::new(
        (0..4)
            .map(|_| SortField::new(DataType::Int64))
            .collect::<Vec<_>>(),
    )
    .expect("row converter")
}

fn batch(rows: usize) -> (RecordBatch, Vec<ArrayRef>) {
    let cols: Vec<ArrayRef> = [1_000i64, 10, 1, 15]
        .iter()
        .map(|m| -> ArrayRef {
            Arc::new(Int64Array::from(
                (0..rows as i64).map(|i| i % m.max(&1)).collect::<Vec<_>>(),
            ))
        })
        .collect();
    let schema = Arc::new(Schema::new(
        (0..cols.len())
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let rb = RecordBatch::try_new(schema, cols.clone()).expect("batch");
    (rb, cols)
}

/// Today: own every row up front, mask as `Vec<bool>`, complement as a second one.
fn split_owned_vec(rows_enc: &Rows, rows: usize, bloom: &Bloom) -> (usize, usize) {
    let mut incoming: HashMap<u128, OwnedRow> = HashMap::with_capacity(rows);
    let mut miss_mask = Vec::with_capacity(rows);
    for i in 0..rows {
        let key = rows_enc.row(i).owned();
        let digest = hash_key_128(key.as_ref());
        let is_miss = !bloom.maybe_contains(key.as_ref(), i) && !incoming.contains_key(&digest);
        if is_miss {
            incoming.insert(digest, key);
        }
        miss_mask.push(is_miss);
    }
    let miss_pred = BooleanArray::from(miss_mask.clone());
    let hit_mask: Vec<bool> = miss_mask.iter().map(|m| !*m).collect();
    let hit_pred = BooleanArray::from(hit_mask);
    black_box((&miss_pred, &hit_pred));
    (miss_pred.true_count(), incoming.len())
}

/// Proposed: probe on borrowed bytes and own only on the keeping branch; one bit
/// mask, complemented by the Arrow negate kernel.
fn split_borrowed_bits(rows_enc: &Rows, rows: usize, bloom: &Bloom) -> (usize, usize) {
    let mut incoming: HashMap<u128, OwnedRow> = HashMap::with_capacity(rows);
    let mut miss_mask = BooleanBufferBuilder::new(rows);
    for i in 0..rows {
        let key = rows_enc.row(i);
        let key_bytes = key.as_ref();
        let digest = hash_key_128(key_bytes);
        let is_miss = !bloom.maybe_contains(key_bytes, i) && !incoming.contains_key(&digest);
        if is_miss {
            incoming.insert(digest, key.owned());
        }
        miss_mask.append(is_miss);
    }
    let miss_pred = BooleanArray::new(miss_mask.finish(), None);
    let hit_pred = arrow::compute::not(&miss_pred).expect("negate");
    black_box((&miss_pred, &hit_pred));
    (miss_pred.true_count(), incoming.len())
}

/// Today: sum every shard's byte count on every insert.
/// `BooleanBuilder` (`BooleanArray::builder`) instead of `BooleanBufferBuilder`:
/// the same bit-packed values buffer, plus a `NullBufferBuilder` that
/// `append_value` pokes on every row even when no null is ever appended.
fn split_borrowed_boolean_builder(rows_enc: &Rows, rows: usize, bloom: &Bloom) -> (usize, usize) {
    let mut incoming: HashMap<u128, OwnedRow> = HashMap::with_capacity(rows);
    let mut miss_mask = BooleanArray::builder(rows);
    for i in 0..rows {
        let key = rows_enc.row(i);
        let key_bytes = key.as_ref();
        let digest = hash_key_128(key_bytes);
        let is_miss = !bloom.maybe_contains(key_bytes, i) && !incoming.contains_key(&digest);
        if is_miss {
            incoming.insert(digest, key.owned());
        }
        miss_mask.append_value(is_miss);
    }
    let miss_pred = miss_mask.finish();
    let hit_pred = arrow::compute::not(&miss_pred).expect("negate");
    black_box((&miss_pred, &hit_pred));
    (miss_pred.true_count(), incoming.len())
}

fn budget_scan(rows_enc: &Rows, rows: usize, shards: usize) -> usize {
    let mut bytes = vec![0usize; shards];
    let mut degrades = 0;
    for i in 0..rows {
        let digest = hash_key_128(rows_enc.row(i).as_ref());
        let s = (digest >> 96) as usize % shards;
        bytes[s] += 64;
        let total: usize = bytes.iter().sum();
        if total > usize::MAX / 2 {
            degrades += 1;
        }
    }
    black_box(&bytes);
    degrades
}

/// Proposed: fold the touched shard's delta into a running sum.
fn budget_running(rows_enc: &Rows, rows: usize, shards: usize) -> usize {
    let mut bytes = vec![0usize; shards];
    let mut total = 0usize;
    let mut degrades = 0;
    for i in 0..rows {
        let digest = hash_key_128(rows_enc.row(i).as_ref());
        let s = (digest >> 96) as usize % shards;
        let before = bytes[s];
        bytes[s] += 64;
        total += bytes[s] - before;
        if total > usize::MAX / 2 {
            degrades += 1;
        }
    }
    black_box(&bytes);
    degrades
}

fn bench(c: &mut Criterion) {
    let conv = converter();
    let (_rb, cols) = batch(ROWS);
    let encoded = conv.convert_columns(&cols).expect("encode");

    let mut g = c.benchmark_group("pk_apply_split");
    g.throughput(Throughput::Elements(ROWS as u64));
    g.sample_size(50);

    // hit_every: 1 = every row HITs (steady-state upsert), 2 = half, 0 = none
    // (a batch of entirely new keys).
    for (label, hit_every) in [("all_hit", 1usize), ("half_hit", 2), ("all_miss", 0)] {
        let bloom = Bloom::new(ROWS, hit_every);
        g.bench_function(format!("split/owned_vec_{label}"), |b| {
            b.iter(|| black_box(split_owned_vec(&encoded, ROWS, &bloom)));
        });
        g.bench_function(format!("split/borrowed_bits_{label}"), |b| {
            b.iter(|| black_box(split_borrowed_bits(&encoded, ROWS, &bloom)));
        });
        g.bench_function(format!("split/borrowed_boolbuilder_{label}"), |b| {
            b.iter(|| black_box(split_borrowed_boolean_builder(&encoded, ROWS, &bloom)));
        });
    }

    for shards in [4usize, 16, 64] {
        g.bench_function(format!("budget/scan_{shards}shards"), |b| {
            b.iter(|| black_box(budget_scan(&encoded, ROWS, shards)));
        });
        g.bench_function(format!("budget/running_{shards}shards"), |b| {
            b.iter(|| black_box(budget_running(&encoded, ROWS, shards)));
        });
    }

    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
