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

#![allow(clippy::expect_used)]

//! Per-batch cost of the null count that in-memory segment statistics take for every
//! column (`statistics_from_record_batches` in
//! `crates/cayenne/src/provider/file_pruning.rs`): `Array::null_count`, which counts null
//! bits, against `Array::logical_null_count`, which also counts a dictionary key that
//! names a null value and every row of a `Null` column.
//!
//! The statistics are computed once per RAM-tier segment and per inline entry, so this
//! sits on the change-apply path. The same statistics also take each column's minimum
//! and maximum, an O(rows) pass; `min_max_int64_column` is that pass over one column of
//! the same batch, for scale.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, DictionaryArray, Int32Array, Int64Array, NullArray, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::{max, min};
use arrow::datatypes::Int32Type;
use criterion::{Criterion, criterion_group, criterion_main};

/// Rows in one batch, the size change batches are applied in.
const ROWS: i32 = 8192;

/// An `order_line`-shaped batch: integer keys, a quantity, a decimal amount, a string and
/// a timestamp, with no nulls.
fn order_line_columns() -> Vec<ArrayRef> {
    vec![
        Arc::new(Int32Array::from_iter_values(0..ROWS)),
        Arc::new(Int32Array::from_iter_values((0..ROWS).map(|i| i % 10))),
        Arc::new(Int64Array::from_iter_values((0..ROWS).map(i64::from))),
        Arc::new(Int32Array::from_iter_values((0..ROWS).map(|i| i % 15))),
        Arc::new(
            Decimal128Array::from_iter_values((0..ROWS).map(i128::from))
                .with_precision_and_scale(6, 2)
                .expect("decimal(6, 2)"),
        ),
        Arc::new(StringArray::from_iter_values(
            (0..ROWS).map(|i| format!("dist-info-{i:014}")),
        )),
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..ROWS).map(i64::from),
        )),
    ]
}

/// A dictionary column whose dictionary holds a null that every other key names, and a
/// `Null` column: the two kinds of null that carry no null bit.
fn unmarked_null_columns() -> Vec<ArrayRef> {
    let tags = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from_iter_values((0..ROWS).map(|i| i % 2)),
        Arc::new(StringArray::from(vec![None, Some("x")])) as ArrayRef,
    )
    .expect("dictionary");
    vec![
        Arc::new(Int32Array::from_iter_values(0..ROWS)),
        Arc::new(tags),
        Arc::new(NullArray::new(8192)),
    ]
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("segment_null_count");
    for (name, columns) in [
        ("order_line", order_line_columns()),
        ("dictionary_and_null", unmarked_null_columns()),
    ] {
        group.bench_function(format!("{name}/null_count"), |b| {
            b.iter(|| {
                black_box(&columns)
                    .iter()
                    .map(|column| column.null_count())
                    .sum::<usize>()
            });
        });
        group.bench_function(format!("{name}/logical_null_count"), |b| {
            b.iter(|| {
                black_box(&columns)
                    .iter()
                    .map(|column| column.logical_null_count())
                    .sum::<usize>()
            });
        });
    }
    let keys = Int64Array::from_iter_values((0..ROWS).map(i64::from));
    group.bench_function("order_line/min_max_int64_column", |b| {
        b.iter(|| (min(black_box(&keys)), max(black_box(&keys))));
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
