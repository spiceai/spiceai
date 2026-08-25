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

//! Throughput benchmark for `runtime::flight::encode_flight_batch` -- the
//! `RecordBatch`-to-wire encode boundary every Flight `do_get` result pays.
//! Production runs this synchronously on the CPU runtime behind a 2-deep
//! channel (`FLIGHT_ENCODE_CHANNEL_CAPACITY`,
//! `crates/runtime/src/flight/mod.rs`), so its throughput gates overall
//! query throughput -- and it had zero benchmark coverage before this file.
//!
//! Sweeps: batch width (narrow/wide), dictionary-encoded string columns
//! (plain/dictionary), and compression mode (none/LZ4/ZSTD); a separate
//! group isolates the `Utf8View -> LargeUtf8` cast production applies when
//! the advertised schema was expanded for a client that doesn't support
//! views (`needs_view_cast`).

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::builder::StringDictionaryBuilder;
use arrow::array::{ArrayRef, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator};
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use runtime::flight::encode_flight_batch;

const ROWS: usize = 8192;
const WIDE_NUMERIC_COLS: usize = 20;
const WIDE_STRING_COLS: usize = 10;
/// Distinct string values per string column in the wide+dictionary shape --
/// low enough that dictionary encoding is meaningfully smaller on the wire.
const DICTIONARY_CARDINALITY: usize = 16;

fn plain_string_column(cardinality: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..ROWS).map(|i| format!("value-{}", i % cardinality)),
    ))
}

fn dictionary_string_column(cardinality: usize) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for i in 0..ROWS {
        builder.append_value(format!("value-{}", i % cardinality));
    }
    Arc::new(builder.finish())
}

fn narrow_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
    let value = plain_string_column(ROWS);
    RecordBatch::try_new(schema, vec![id, value]).expect("narrow batch")
}

fn wide_batch(dictionary: bool) -> RecordBatch {
    let string_type = if dictionary {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    } else {
        DataType::Utf8
    };
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    for i in 0..WIDE_NUMERIC_COLS {
        fields.push(Field::new(format!("num_{i}"), DataType::Int64, false));
    }
    for i in 0..WIDE_STRING_COLS {
        fields.push(Field::new(format!("str_{i}"), string_type.clone(), true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64))];
    for _ in 0..WIDE_NUMERIC_COLS {
        columns.push(Arc::new(Int64Array::from_iter_values(0..ROWS as i64)));
    }
    let cardinality = if dictionary { DICTIONARY_CARDINALITY } else { ROWS };
    for _ in 0..WIDE_STRING_COLS {
        columns.push(if dictionary {
            dictionary_string_column(cardinality)
        } else {
            plain_string_column(cardinality)
        });
    }
    RecordBatch::try_new(schema, columns).expect("wide batch")
}

fn compression_options(compression: Option<CompressionType>) -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(compression)
        .expect("valid compression option")
}

fn encode_once(
    batch: &RecordBatch,
    needs_view_cast: bool,
    schema: &Arc<Schema>,
    options: &IpcWriteOptions,
) {
    let encoder = IpcDataGenerator::default();
    let mut dict_tracker = DictionaryTracker::new(true);
    let mut compression_context = CompressionContext::default();
    let encoded = encode_flight_batch(
        batch.clone(),
        needs_view_cast,
        schema,
        &encoder,
        &mut dict_tracker,
        options,
        &mut compression_context,
    )
    .expect("encode succeeds");
    black_box(encoded);
}

fn bench_encode_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("flight_encode");

    let shapes: [(&str, RecordBatch); 3] = [
        ("narrow", narrow_batch()),
        ("wide_plain", wide_batch(false)),
        ("wide_dictionary", wide_batch(true)),
    ];
    let compressions: [(&str, Option<CompressionType>); 3] = [
        ("none", None),
        ("lz4", Some(CompressionType::LZ4_FRAME)),
        ("zstd", Some(CompressionType::ZSTD)),
    ];

    for (shape_name, batch) in &shapes {
        let schema = batch.schema();
        group.throughput(Throughput::Elements(batch.num_rows() as u64));
        for (compression_name, compression) in compressions {
            let options = compression_options(compression);
            group.bench_function(BenchmarkId::new(*shape_name, compression_name), |b| {
                b.iter(|| encode_once(batch, false, &schema, &options));
            });
        }
    }

    group.finish();
}

fn bench_view_cast_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("flight_encode_view_cast");
    let options = compression_options(None);

    // The real shape: a Utf8View-typed batch, advertised as LargeUtf8 (what
    // production does for a client that doesn't support views) -- exercises
    // the actual cast_view_columns path.
    let view_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8View, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
    let view_value: ArrayRef = Arc::new(StringViewArray::from_iter_values(
        (0..ROWS).map(|i| format!("value-{i}")),
    ));
    let view_batch =
        RecordBatch::try_new(Arc::clone(&view_schema), vec![Arc::clone(&id), view_value])
            .expect("view batch");
    let advertised_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::LargeUtf8, true),
    ]));

    group.bench_function("with_cast", |b| {
        b.iter(|| encode_once(&view_batch, true, &advertised_schema, &options));
    });

    // Baseline: same shape, already in the advertised type, so
    // `needs_view_cast = false` is the correct (no-op) path -- isolates the
    // cast's own cost as the delta against `with_cast` above.
    let large_value: ArrayRef = Arc::new(LargeStringArray::from_iter_values(
        (0..ROWS).map(|i| format!("value-{i}")),
    ));
    let baseline_batch = RecordBatch::try_new(Arc::clone(&advertised_schema), vec![id, large_value])
        .expect("baseline batch");
    group.bench_function("baseline_no_cast", |b| {
        b.iter(|| encode_once(&baseline_batch, false, &advertised_schema, &options));
    });

    group.finish();
}

criterion_group!(benches, bench_encode_shapes, bench_view_cast_overhead);
criterion_main!(benches);
