/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Criterion benchmarks for the truncation fast-paths and formatting helpers
//! in arrow_tools.
//!
//! Run with: cargo bench -p arrow_tools --bench truncate
//!
//! The benchmarks deliberately separate "fast path" (no actual work needed,
//! exercises the zero-copy clone() paths added in the audit) from
//! "actual truncation" (exercises the slice+concat / collect paths).

use arrow::array::{Int32Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_tools::record_batch::{truncate_numeric_column_length, truncate_string_columns};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

/// Creates a batch where *no* string needs truncation at the given limit.
/// This exercises the new UTF8 fast-path (cheap any() + early Arc::clone).
fn make_all_short_string_batch(n: usize, max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    // All strings are ASCII and well under the limit
    let short = "short".repeat(3); // ~15 chars
    let strings: Vec<Option<String>> = (0..n).map(|_| Some(short.clone())).collect();
    let arr = StringArray::from(strings);
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

/// Creates a batch where a significant fraction of strings *do* need
/// character truncation. Exercises the collect + truncation path.
fn make_many_long_string_batch(n: usize, max_chars: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let long = "x".repeat(max_chars + 20); // will need truncation
    let short = "s";
    let strings: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                Some(long.clone())
            } else {
                Some(short.to_string())
            }
        })
        .collect();
    let arr = StringArray::from(strings);
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

/// Creates a List<i32> batch where no list exceeds the element limit.
/// Exercises the list fast-path (try_fold + clone).
fn make_all_short_list_batch(n: usize, max_elems: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = max_elems.min(5);
    let total_values = n * per_list;
    let values = Int32Array::from((0..total_values).collect::<Vec<_>>());
    let offsets: Vec<i32> = (0..=n).map(|i| (i * per_list as i32)).collect();
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::<i32>::new(offsets.into()),
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

/// Creates a List<i32> batch where truncation is required for every list.
fn make_long_list_batch(n: usize, truncate_to: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let per_list = truncate_to + 15; // will need truncation
    let total_values = n * per_list;
    let values = Int32Array::from((0..total_values).collect::<Vec<_>>());
    let offsets: Vec<i32> = (0..=n).map(|i| (i * per_list as i32)).collect();
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::<i32>::new(offsets.into()),
        Arc::new(values),
        None,
    );
    RecordBatch::try_new(schema, vec![Arc::new(list)]).expect("valid batch")
}

pub fn bench_truncate(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_tools_truncate");

    // String fast path (the new UTF8 early-exit + Arc::clone)
    let short_strings = make_all_short_string_batch(2000, 50);
    group.bench_function("string_fast_path_2000_rows_all_short", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&short_strings), 50).unwrap();
        })
    });

    // String actual work
    let long_strings = make_many_long_string_batch(2000, 50);
    group.bench_function("string_with_truncation_2000_rows_mixed", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&long_strings), 50).unwrap();
        })
    });

    // List fast path (the try_fold + clone path)
    let short_lists = make_all_short_list_batch(800, 5);
    group.bench_function("list_fast_path_800_rows_all_short", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&short_lists), 5).unwrap();
        })
    });

    // List actual truncation work
    let long_lists = make_long_list_batch(800, 5);
    group.bench_function("list_with_truncation_800_rows", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&long_lists), 5).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_truncate);
criterion_main!(benches);
