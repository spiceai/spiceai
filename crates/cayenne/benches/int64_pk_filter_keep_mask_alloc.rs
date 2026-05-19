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

//! Comparison bench for the per-batch keep-mask construction inside
//! `Int64PkDeletionFilterStream`.
//!
//! `crates/cayenne/src/provider/delete/filter_exec.rs:684` builds the keep
//! mask as a `Vec<bool>` (one byte per row) and then immediately converts it
//! to a packed [`BooleanArray`] via `BooleanArray::from(Vec<bool>)`. The
//! intuition was that this carries an 8 KiB allocation plus a second walk to
//! pack into a 1 KiB `BooleanBuffer`, and that a single-pass
//! `BooleanBufferBuilder` would be faster.
//!
//! This bench tests that hypothesis at three visibility shapes
//! (`all_visible` / `half_visible` / `none_visible`) and 8 192-row batches.
//! The measured result on aarch64 (Apple Silicon) is **no win**: the
//! `BooleanBufferBuilder` lane is within ±5 % of the `Vec<bool>` lane across
//! all three shapes. The `Vec<bool>` write-per-row is just a byte store that
//! LLVM auto-vectorises, and Arrow's `BooleanArray::from(Vec<bool>)` packs
//! using SIMD too; `BooleanBufferBuilder::append` pays a per-bit
//! read-modify-write that cancels the alloc-size win at this batch size.
//!
//! Keep this bench as a regression guard against re-introducing the
//! `BooleanBufferBuilder` rewrite under the assumption that it is "obviously
//! cheaper" — and as a documented dead end for future iterations of the
//! deletion-filter hot path.
//!
//! The benchmark keeps three visibility shapes so allocation and packing costs
//! are visible at the all-keep, mixed-keep, and no-keep extremes:
//!
//!   - `all_visible`     — every row passes the visibility check (matches the
//!     hot path for non-deleted batches; pre-fix code still pays the
//!     allocation cost even though the batch is returned as-is).
//!   - `half_visible`    — 50 % of rows pass, exercising the
//!     `filter_record_batch` step.
//!   - `none_visible`    — 0 % pass; the stream `continue`s after the count
//!     check but the keep-mask is still built.
//!
//! Two lanes per shape:
//!
//!   - `vec_bool` — current code: `Vec::<bool>::with_capacity(batch_size)`
//!     populated in a hot loop, then `BooleanArray::from(Vec<bool>)`.
//!   - `boolean_buffer_builder` — proposed code: `BooleanBufferBuilder` with
//!     `append(b)` per row, then `BooleanArray::new(builder.finish(), None)`.
//!
//! Both lanes produce a bit-identical `BooleanArray`. The difference is purely
//! allocation + packing overhead.
//!
//! `cargo bench --bench int64_pk_filter_keep_mask_alloc -p cayenne`.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{BooleanArray, BooleanBufferBuilder, Int64Array, RecordBatch, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::provider::deletion_index::DeletionIndex;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const ROWS_PER_BATCH: usize = 8_192;
/// Three visibility shapes — the second value is the deletion ratio used
/// to populate the deletion index (50 % visible == 50 % of input PKs are in
/// the deletion index).
const SHAPES: &[(&str, f64)] = &[
    ("all_visible", 0.0),
    ("half_visible", 0.5),
    ("none_visible", 1.0),
];

fn make_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..rows as i64).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("row_{i}")).collect();
    let names_array = StringArray::from(names);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids)), Arc::new(names_array)],
    )
    .expect("batch")
}

/// Build a `DeletionIndex` that contains the first `(rows * ratio)` PK values.
/// `ratio==1.0` means every PK in the batch is in the deletion index;
/// `ratio==0.0` means none are.
fn build_index(rows: usize, ratio: f64) -> DeletionIndex {
    let count = ((rows as f64) * ratio) as usize;
    let mut map = HashMap::with_capacity(count);
    for i in 0..count {
        map.insert(i as i64, 1_i64);
    }
    DeletionIndex::from_map(map)
}

/// Mirror of the current production code path in
/// `Int64PkDeletionFilterStream::poll_next` (`filter_exec.rs:683-741`).
fn keep_mask_vec_bool(
    batch: &RecordBatch,
    pk_column_index: usize,
    deleted_pk_values: &DeletionIndex,
    insert_records: &DeletionIndex,
) -> RecordBatch {
    let batch_size = batch.num_rows();
    let pk_column = batch.column(pk_column_index);
    let pk_array = pk_column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64");
    let pk_slice = pk_array.values();
    let mut keep_mask: Vec<bool> = Vec::with_capacity(batch_size);
    let mut keep_count: usize = 0;
    for &pk in pk_slice {
        let visible = match deleted_pk_values.get(pk) {
            None => true,
            Some(delete_seq) => insert_records
                .get(pk)
                .is_some_and(|insert_seq| insert_seq > delete_seq),
        };
        keep_mask.push(visible);
        keep_count += usize::from(visible);
    }
    if keep_count == 0 {
        // Mirror the stream's `continue` branch: still produces an empty
        // batch so the bench output is meaningful even at none_visible.
        return RecordBatch::new_empty(batch.schema());
    }
    if keep_count == batch_size {
        return batch.clone();
    }
    let filter_array = BooleanArray::from(keep_mask);
    filter_record_batch(batch, &filter_array).expect("filter")
}

/// Proposed allocation path — build the keep mask directly into a packed
/// `BooleanBufferBuilder` (one bit per row, written in 64-bit chunks) and
/// finish into a `BooleanArray` with no intermediate `Vec<bool>`.
fn keep_mask_boolean_buffer(
    batch: &RecordBatch,
    pk_column_index: usize,
    deleted_pk_values: &DeletionIndex,
    insert_records: &DeletionIndex,
) -> RecordBatch {
    let batch_size = batch.num_rows();
    let pk_column = batch.column(pk_column_index);
    let pk_array = pk_column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64");
    let pk_slice = pk_array.values();
    let mut builder = BooleanBufferBuilder::new(batch_size);
    let mut keep_count: usize = 0;
    for &pk in pk_slice {
        let visible = match deleted_pk_values.get(pk) {
            None => true,
            Some(delete_seq) => insert_records
                .get(pk)
                .is_some_and(|insert_seq| insert_seq > delete_seq),
        };
        builder.append(visible);
        keep_count += usize::from(visible);
    }
    if keep_count == 0 {
        return RecordBatch::new_empty(batch.schema());
    }
    if keep_count == batch_size {
        return batch.clone();
    }
    let filter_array = BooleanArray::new(builder.finish(), None);
    filter_record_batch(batch, &filter_array).expect("filter")
}

fn bench_keep_mask(c: &mut Criterion) {
    let mut group = c.benchmark_group("int64_pk_filter_keep_mask_alloc");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    let batch = make_batch(ROWS_PER_BATCH);
    let empty_inserts = DeletionIndex::empty();

    for (shape_name, ratio) in SHAPES {
        let index = build_index(ROWS_PER_BATCH, *ratio);

        group.bench_with_input(
            BenchmarkId::new("vec_bool", shape_name),
            shape_name,
            |b, _| {
                b.iter(|| {
                    let out = keep_mask_vec_bool(
                        black_box(&batch),
                        black_box(0),
                        black_box(&index),
                        black_box(&empty_inserts),
                    );
                    black_box(out);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("boolean_buffer_builder", shape_name),
            shape_name,
            |b, _| {
                b.iter(|| {
                    let out = keep_mask_boolean_buffer(
                        black_box(&batch),
                        black_box(0),
                        black_box(&index),
                        black_box(&empty_inserts),
                    );
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_keep_mask);
criterion_main!(benches);
