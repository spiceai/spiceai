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

//! A/B lanes for the CDC apply-path vectorization quick wins (write side; the
//! read-side `KeyBasedDeletionFilterExec` is already batch-swept and is NOT
//! covered here):
//!
//! 1. **Int64 PK extract** — `extract_primary_keys_from_batch`-shaped loop
//!    (`provider/table.rs`): per-row `is_null` + `value(i)` + `push` versus a
//!    `null_count() == 0` gate + one `extend_from_slice(values())` memcpy.
//! 2. **Batched tombstone sweep** — `filter_inlined_batch_for_deletions`-shaped
//!    merge-on-read sweep (`provider/table.rs`): per-row
//!    `DeletionIndex::get(pk)` fused probe + `chain(inline_map)` + `Vec<bool>`
//!    push, versus `DeletionIndex::get_batch` (bloom-sweep the batch, tier-walk
//!    survivors only — `provider/deletion_index.rs`) + an inline-map pass that
//!    is skipped when the map is empty. `probe_only` lanes isolate the
//!    `get`-loop vs `get_batch` API delta for both key types.
//! 3. **Keep-mask construction** — `Vec<bool>` → `BooleanArray::from` versus
//!    `BooleanBufferBuilder` → `BooleanArray::new`, at the 16K coalesced-
//!    envelope batch size. NOTE: `int64_pk_filter_keep_mask_alloc` measured
//!    this as a no-win at 8K rows on aarch64 and documents it as a dead end;
//!    these lanes re-test that verdict at the apply path's 16K batch shape.
//!
//! Shapes follow the CDC freshness-tail profile: 16 384-row batches (the
//! coalesced envelope size), ~64-byte rows (Int64 PK + 7 Int64 payload
//! columns), deletion indexes at 1M and 8M entries (out-of-cache tier walks),
//! and 10% / 90% conflict mixes.
//!
//! Every scalar/batched lane pair is checked for bit-identical output before
//! the timed runs (`verify_equivalence`), so a lane can't "win" by diverging.
//!
//! `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
//!   cargo bench --bench cdc_apply_simd -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::too_many_lines)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::provider::deletion_index::{DeletionIndex, KeyDeletionIndex, Tombstone};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::XxHash3BuildHasher;

/// Mirror of `InlinedDeletionMaps::int64_pk` (`provider/mem_tier.rs`
/// `InMemTombstones`): an `im::HashMap` with the XXH3 hasher, NOT a std
/// `HashMap` — the inline-map probe cost must match production.
type InlineMap = im::HashMap<i64, i64, XxHash3BuildHasher>;

/// Coalesced CDC envelope size — the apply path's per-batch row count.
const ROWS: usize = 16_384;
/// Deletion-index occupancies bracketing the freshness-tail tables (stock /
/// order_line accumulate millions of tombstones between compactions).
const INDEX_SIZES: &[usize] = &[1_000_000, 8_000_000];
/// Fraction of batch PKs present in the deletion index (conflict mix).
const MIXES: &[(&str, u64)] = &[("hit10", 1), ("hit90", 9)];
/// Sequence of the incoming (inlined) data being re-filtered.
const DATA_SEQ: i64 = 50;
/// Sequence of the recorded deletions — newer than `DATA_SEQ`, so a probe hit
/// drops the row (the merge-on-read polarity the apply path exercises).
const DELETE_SEQ: i64 = 100;
/// Entries in the (non-empty) inline-deletion map both lanes must fold in.
const INLINE_MAP_ENTRIES: i64 = 1024;

const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

/// Deterministic batch PKs for a conflict mix: `hit_tenths/10` of the rows
/// probe a key present in the index (spread across its whole range), the rest
/// probe keys above every populated range (index keys, inline-map keys).
fn batch_pks(index_size: usize, hit_tenths: u64) -> Vec<i64> {
    (0..ROWS as u64)
        .map(|i| {
            if i.wrapping_mul(7) % 10 < hit_tenths {
                (i.wrapping_mul(SPREAD) % index_size as u64) as i64
            } else {
                (index_size as u64 * 2 + i) as i64
            }
        })
        .collect()
}

/// 64-byte-row batch: Int64 PK + 7 Int64 payload columns, PK column non-null
/// (PK columns are non-null by construction on the apply path).
fn make_batch(pks: &[i64]) -> RecordBatch {
    let mut fields = vec![Field::new("pk", DataType::Int64, false)];
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(pks.to_vec()))];
    for c in 0..7_i64 {
        fields.push(Field::new(format!("v{c}"), DataType::Int64, false));
        let payload: Vec<i64> = (0..pks.len() as i64).map(|i| i ^ c).collect();
        columns.push(Arc::new(Int64Array::from(payload)));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch")
}

/// Index with keys `0..n` deleted at [`DELETE_SEQ`].
fn build_index(n: usize) -> DeletionIndex {
    DeletionIndex::from_map((0..n as i64).map(|pk| (pk, DELETE_SEQ)).collect())
}

/// Inline-deletion map (the `InlinedDeletionMaps::int64_pk` analogue): keys
/// disjoint from both the index and the batch, so it costs a per-row probe
/// without changing which rows drop — identical work for both lanes.
fn build_inline_map(index_size: usize) -> InlineMap {
    (0..INLINE_MAP_ENTRIES)
        .map(|i| (-(i + 1), DELETE_SEQ))
        .map(|(k, v)| (k.wrapping_sub(index_size as i64), v))
        .collect()
}

// ---------------------------------------------------------------------------
// Win 2 lanes: `filter_inlined_batch_for_deletions` Int64 arm
// ---------------------------------------------------------------------------

/// BEFORE — mirror of the current per-row loop in
/// `CayenneTableProvider::filter_inlined_batch_for_deletions`
/// (`provider/table.rs`, `PkDeletionStrategyWithCache::Int64Pk` arm): fused
/// bloom+tier probe per row, inline-map chain per row, `Vec<bool>` push.
fn sweep_scalar_per_row(
    batch: &RecordBatch,
    index: &DeletionIndex,
    inline_map: &InlineMap,
) -> Option<RecordBatch> {
    let pk_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("pk column");
    let mut keep_mask = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        assert!(!pk_array.is_null(row_index), "PK must be non-null");
        let pk = pk_array.value(row_index);
        let max_delete_sequence = index
            .get(pk)
            .map(|tombstone| tombstone.delete_sequence)
            .into_iter()
            .chain(inline_map.get(&pk).copied())
            .max();
        keep_mask
            .push(max_delete_sequence.is_none_or(|delete_sequence| DATA_SEQ > delete_sequence));
    }
    // Same tail as the production function: all-keep and all-drop shortcuts,
    // then the filter kernel.
    if keep_mask.iter().all(|keep| *keep) {
        return Some(batch.clone());
    }
    if keep_mask.iter().all(|keep| !*keep) {
        return None;
    }
    let filter_array = BooleanArray::from(keep_mask);
    Some(filter_record_batch(batch, &filter_array).expect("filter"))
}

/// AFTER — the proposed batched caller: `null_count()==0` gate + `values()`
/// slice (win 1), `get_batch` bloom sweep (win 2), inline-map pass skipped
/// when empty, drop-count early-outs before the filter kernel.
fn sweep_batched(
    batch: &RecordBatch,
    index: &DeletionIndex,
    inline_map: &InlineMap,
) -> Option<RecordBatch> {
    let pk_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("pk column");
    assert!(pk_array.null_count() == 0, "PK must be non-null");
    let pks: &[i64] = pk_array.values();

    let mut keep_mask = vec![true; pks.len()];
    let mut dropped = 0_usize;
    // max(file_seq, inline_seq) >= DATA_SEQ  <=>  either side >= DATA_SEQ,
    // so the fused `.chain(...).max()` fold decomposes into independent
    // passes over each side.
    index.get_batch(pks, |i, tombstone| {
        if DATA_SEQ <= tombstone.delete_sequence && keep_mask[i] {
            keep_mask[i] = false;
            dropped += 1;
        }
    });
    if !inline_map.is_empty() {
        for (keep, pk) in keep_mask.iter_mut().zip(pks) {
            if *keep && inline_map.get(pk).is_some_and(|&seq| DATA_SEQ <= seq) {
                *keep = false;
                dropped += 1;
            }
        }
    }
    if dropped == pks.len() {
        return None;
    }
    if dropped == 0 {
        return Some(batch.clone());
    }
    let filter_array = BooleanArray::from(keep_mask);
    Some(filter_record_batch(batch, &filter_array).expect("filter"))
}

// ---------------------------------------------------------------------------
// Win 1 lanes: Int64 PK extraction
// ---------------------------------------------------------------------------

/// BEFORE — mirror of `extract_primary_keys_from_batch` (`provider/table.rs`,
/// Int64 arm): per-row `is_null` + `value(i)` + `push`.
fn extract_scalar_per_row(pk_array: &Int64Array) -> Vec<i64> {
    let mut values = Vec::with_capacity(pk_array.len());
    for row_index in 0..pk_array.len() {
        assert!(!pk_array.is_null(row_index), "PK must be non-null");
        values.push(pk_array.value(row_index));
    }
    values
}

/// AFTER — `null_count() == 0` gate + bulk `extend_from_slice(values())`.
fn extract_bulk_slice(pk_array: &Int64Array) -> Vec<i64> {
    assert!(pk_array.null_count() == 0, "PK must be non-null");
    let mut values = Vec::with_capacity(pk_array.len());
    values.extend_from_slice(pk_array.values());
    values
}

// ---------------------------------------------------------------------------
// Win 3 lanes: keep-mask construction
// ---------------------------------------------------------------------------

fn mask_vec_bool(decisions: &[bool]) -> BooleanArray {
    let mut keep_mask = Vec::with_capacity(decisions.len());
    for &keep in decisions {
        keep_mask.push(keep);
    }
    BooleanArray::from(keep_mask)
}

fn mask_buffer_builder(decisions: &[bool]) -> BooleanArray {
    let mut builder = BooleanBufferBuilder::new(decisions.len());
    for &keep in decisions {
        builder.append(keep);
    }
    BooleanArray::new(builder.finish(), None)
}

// ---------------------------------------------------------------------------
// Composite-key probe lanes (stock / order_line have composite PKs)
// ---------------------------------------------------------------------------

fn composite_key(value: u64) -> Box<[u8]> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&value.to_be_bytes());
    bytes.extend_from_slice(&(value ^ SPREAD).to_be_bytes());
    bytes.into_boxed_slice()
}

fn build_key_index(n: usize) -> KeyDeletionIndex {
    KeyDeletionIndex::from_map(
        (0..n as u64)
            .map(|i| (composite_key(i), DELETE_SEQ))
            .collect(),
    )
}

fn batch_keys(index_size: usize, hit_tenths: u64) -> Vec<Box<[u8]>> {
    (0..ROWS as u64)
        .map(|i| {
            if i.wrapping_mul(7) % 10 < hit_tenths {
                composite_key(i.wrapping_mul(SPREAD) % index_size as u64)
            } else {
                composite_key(index_size as u64 * 2 + i)
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Equivalence gate (runs before any timed lane)
// ---------------------------------------------------------------------------

fn collect_batch_hits(index: &DeletionIndex, pks: &[i64]) -> Vec<Option<Tombstone>> {
    let mut out = vec![None; pks.len()];
    index.get_batch(pks, |i, tombstone| out[i] = Some(tombstone));
    out
}

fn verify_equivalence() {
    for &n in INDEX_SIZES {
        let index = build_index(n);
        let inline_map = build_inline_map(n);
        for &(_, hit_tenths) in MIXES {
            let pks = batch_pks(n, hit_tenths);
            let batch = make_batch(&pks);

            let scalar = sweep_scalar_per_row(&batch, &index, &inline_map);
            let batched = sweep_batched(&batch, &index, &inline_map);
            assert_eq!(
                scalar, batched,
                "sweep lanes diverged at n={n} hit_tenths={hit_tenths}"
            );

            let per_row: Vec<Option<Tombstone>> = pks.iter().map(|&pk| index.get(pk)).collect();
            assert_eq!(
                collect_batch_hits(&index, &pks),
                per_row,
                "get_batch diverged from get at n={n} hit_tenths={hit_tenths}"
            );

            let pk_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("pk column");
            assert_eq!(
                extract_scalar_per_row(pk_array),
                extract_bulk_slice(pk_array)
            );
        }
    }
    let decisions: Vec<bool> = (0..ROWS).map(|i| i % 2 == 0).collect();
    assert_eq!(mask_vec_bool(&decisions), mask_buffer_builder(&decisions));
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

fn bench_int64_pk_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_apply_simd_int64_pk_extract");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(60);

    let pks = batch_pks(1_000_000, 1);
    let batch = make_batch(&pks);
    let pk_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("pk column");

    group.bench_function("scalar_per_row", |b| {
        b.iter(|| black_box(extract_scalar_per_row(black_box(pk_array))));
    });
    group.bench_function("bulk_slice_copy", |b| {
        b.iter(|| black_box(extract_bulk_slice(black_box(pk_array))));
    });
    group.finish();
}

fn bench_tombstone_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_apply_simd_tombstone_sweep");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(30);

    for &n in INDEX_SIZES {
        let index = build_index(n);
        let inline_map = build_inline_map(n);
        for &(mix, hit_tenths) in MIXES {
            let pks = batch_pks(n, hit_tenths);
            let batch = make_batch(&pks);
            let label = format!("n{n}_{mix}");

            group.bench_with_input(
                BenchmarkId::new("scalar_per_row", &label),
                &batch,
                |b, batch| {
                    b.iter(|| {
                        black_box(sweep_scalar_per_row(black_box(batch), &index, &inline_map))
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new("batched_sweep", &label),
                &batch,
                |b, batch| {
                    b.iter(|| black_box(sweep_batched(black_box(batch), &index, &inline_map)));
                },
            );
        }
    }
    group.finish();
}

fn bench_probe_only_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_apply_simd_probe_only_int64");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(50);

    for &n in INDEX_SIZES {
        let index = build_index(n);
        for &(mix, hit_tenths) in MIXES {
            let pks = batch_pks(n, hit_tenths);
            let label = format!("n{n}_{mix}");

            group.bench_with_input(BenchmarkId::new("get_loop", &label), &pks, |b, pks| {
                b.iter(|| {
                    let mut hits = 0_usize;
                    for &pk in pks {
                        hits += usize::from(index.get(black_box(pk)).is_some());
                    }
                    black_box(hits);
                });
            });
            group.bench_with_input(BenchmarkId::new("get_batch", &label), &pks, |b, pks| {
                b.iter(|| {
                    let mut hits = 0_usize;
                    index.get_batch(black_box(pks.as_slice()), |_, _| hits += 1);
                    black_box(hits);
                });
            });
        }
    }
    group.finish();
}

fn bench_probe_only_composite(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_apply_simd_probe_only_composite");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(50);

    let n = 1_000_000;
    let index = build_key_index(n);
    for &(mix, hit_tenths) in MIXES {
        let keys = batch_keys(n, hit_tenths);

        group.bench_with_input(BenchmarkId::new("get_loop", mix), &keys, |b, keys| {
            b.iter(|| {
                let mut hits = 0_usize;
                for key in keys {
                    hits += usize::from(index.get(black_box(key.as_ref())).is_some());
                }
                black_box(hits);
            });
        });
        group.bench_with_input(BenchmarkId::new("get_batch", mix), &keys, |b, keys| {
            b.iter(|| {
                let mut hits = 0_usize;
                index.get_batch(black_box(keys.iter().map(AsRef::as_ref)), |_, _| hits += 1);
                black_box(hits);
            });
        });
    }
    group.finish();
}

fn bench_keep_mask(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdc_apply_simd_keep_mask");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(60);

    let shapes: &[(&str, fn(usize) -> bool)] = &[
        ("all_keep", |_| true),
        ("alternating", |i| i % 2 == 0),
        ("drop10", |i| i % 10 != 0),
    ];
    for (shape, f) in shapes {
        let decisions: Vec<bool> = (0..ROWS).map(f).collect();

        group.bench_with_input(
            BenchmarkId::new("vec_bool", shape),
            &decisions,
            |b, decisions| {
                b.iter(|| black_box(mask_vec_bool(black_box(decisions))));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("boolean_buffer_builder", shape),
            &decisions,
            |b, decisions| {
                b.iter(|| black_box(mask_buffer_builder(black_box(decisions))));
            },
        );
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    verify_equivalence();
    bench_int64_pk_extract(c);
    bench_tombstone_sweep(c);
    bench_probe_only_int64(c);
    bench_probe_only_composite(c);
    bench_keep_mask(c);
}

criterion_group!(cdc_apply_simd, benches);
criterion_main!(cdc_apply_simd);
