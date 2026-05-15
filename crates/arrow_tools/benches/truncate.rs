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
//! in arrow_tools. Run with:
//!   cargo bench -p arrow_tools --bench truncate

use arrow::array::{Int32Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_tools::record_batch::{truncate_numeric_column_length, truncate_string_columns};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;

fn make_mixed_string_batch(n: usize) -> RecordBatch {
    // Mix of short and some longer strings to exercise both fast and slow paths
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
    let strings: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 10 == 0 {
                Some(format!("long_string_{:0<80$}", i)) // will need truncation at 50
            } else {
                Some(format!("s{}", i))
            }
        })
        .collect();
    let arr = StringArray::from(strings);
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("valid batch")
}

fn make_numeric_list_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nums",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]));
    let values = Int32Array::from((0..n * 20).collect::<Vec<_>>());
    let offsets: Vec<i32> = (0..=n).map(|i| (i * 20) as i32).collect();
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
    let string_batch = make_mixed_string_batch(2000);
    let list_batch = make_numeric_list_batch(800);

    group.bench_function("truncate_string_columns_2000_rows_mixed", |b| {
        b.iter(|| {
            truncate_string_columns(black_box(&string_batch), 50).unwrap();
        })
    });

    group.bench_function("truncate_numeric_list_800_rows_to_5", |b| {
        b.iter(|| {
            truncate_numeric_column_length(black_box(&list_batch), 5).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_truncate);
criterion_main!(benches);
