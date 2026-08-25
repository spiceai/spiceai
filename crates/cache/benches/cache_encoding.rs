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

//! Benchmarks `Encoder::encode`/`decode` (`ZstdEncoder`,
//! `crates/cache/src/encoding.rs`) -- the compression layer the query
//! results cache applies to cached `RecordBatch`es -- across compression
//! levels, on a realistic result-shaped batch. Unbenched before this file.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cache::encoding::{Encoder, ZstdEncoder};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const ROWS: usize = 8192;

fn result_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
    let name: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..ROWS).map(|i| format!("row-{i}")),
    ));
    let value: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|v| v * 7),
    ));
    vec![RecordBatch::try_new(schema, vec![id, name, value]).expect("result batch")]
}

fn bench_encode_decode(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let batches = result_batches();
    let byte_len: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();

    let mut group = c.benchmark_group("cache_result_encoding");
    group.throughput(Throughput::Bytes(byte_len as u64));

    // The compression each level buys, printed once, so the timings below have a
    // trade-off attached rather than only a cost. Level 6 is what `get_encoder`
    // ships (`ZstdEncoder::default()`); the rest are unreachable today.
    eprintln!("uncompressed (get_array_memory_size): {byte_len} B");

    for level in [1, 3, 6, 12, 19] {
        let encoder = ZstdEncoder::new(level);
        let encoded = rt.block_on(encoder.encode(&batches)).expect("encode");
        eprintln!(
            "zstd level {level:>2}: {:>7} B  ({:.2}x smaller)",
            encoded.len(),
            byte_len as f64 / encoded.len() as f64
        );

        group.bench_function(BenchmarkId::new("encode", level), |b| {
            b.iter(|| black_box(rt.block_on(encoder.encode(&batches)).expect("encode")));
        });
        group.bench_function(BenchmarkId::new("decode", level), |b| {
            b.iter(|| black_box(rt.block_on(encoder.decode(&encoded)).expect("decode")));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_encode_decode);
criterion_main!(benches);
