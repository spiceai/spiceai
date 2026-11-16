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

//! The cost of encoding rows for a model: the bounded encoder against `arrow-json`'s row
//! objects, which the `sql` tool wrote before.
//!
//! Run with: `cargo bench -p runtime-tools --bench result_encoder`

#![expect(
    clippy::expect_used,
    reason = "a benchmark built from fixed inputs fails loudly if one is invalid"
)]

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use arrow_json::writer::JsonArray;
use criterion::{Criterion, criterion_group, criterion_main};
use runtime_tools::result::{ResultBudget, encode_batches};

/// `rows` rows of a mixed table: integers, floats, a decimal, and short text with NULLs.
fn mixed_batch(rows: usize) -> RecordBatch {
    let n = i64::try_from(rows).expect("rows fit in i64");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("amount", DataType::Decimal128(12, 2), false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..n)),
        Arc::new(Float64Array::from_iter_values((0..n).map(|i| {
            #[expect(clippy::cast_precision_loss, reason = "bench data")]
            let value = i as f64;
            value * 1.25
        }))),
        Arc::new(
            Decimal128Array::from_iter_values((0..n).map(|i| i128::from(i) * 101))
                .with_precision_and_scale(12, 2)
                .expect("valid decimal"),
        ),
        Arc::new(StringArray::from_iter(
            (0..n).map(|i| (i % 7 != 0).then(|| format!("customer-{i}"))),
        )),
    ];
    RecordBatch::try_new(schema, columns).expect("valid batch")
}

fn arrow_json_rows(batch: &RecordBatch) -> Vec<u8> {
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    writer.write(batch).expect("writes");
    writer.finish().expect("finishes");
    writer.into_inner()
}

fn bench_encoders(c: &mut Criterion) {
    let hundred = mixed_batch(100);
    let schema = hundred.schema();
    let full = mixed_batch(8_192);

    let mut group = c.benchmark_group("result_encoder");
    group.bench_function("arrow_json_row_objects_100_rows", |b| {
        b.iter(|| black_box(arrow_json_rows(black_box(&hundred))));
    });
    group.bench_function("bounded_100_rows", |b| {
        b.iter(|| {
            black_box(
                encode_batches(
                    &schema,
                    black_box(std::slice::from_ref(&hundred)),
                    ResultBudget::default(),
                )
                .expect("encodes")
                .into_value()
                .expect("valid JSON"),
            )
        });
    });
    group.bench_function("arrow_json_row_objects_8192_rows", |b| {
        b.iter(|| black_box(arrow_json_rows(black_box(&full))));
    });
    group.bench_function("bounded_8192_rows_stops_at_budget", |b| {
        b.iter(|| {
            black_box(
                encode_batches(
                    &schema,
                    black_box(std::slice::from_ref(&full)),
                    ResultBudget::default(),
                )
                .expect("encodes")
                .into_value()
                .expect("valid JSON"),
            )
        });
    });
    group.finish();
}

criterion_group!(benches, bench_encoders);
criterion_main!(benches);
