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

//! Head-to-head decode microbench for the Kafka change-stream JSON path:
//! `direct` (raw bytes -> Arrow) vs `roundtrip` (bytes -> serde_json::Value ->
//! to_string() -> Arrow). Run with:
//!   cargo bench -p data_components --features bench,kafka --bench kafka_decode

#![allow(clippy::expect_used, clippy::redundant_closure_for_method_calls)]

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use data_components::kafka::bench_wrappers::{decode_direct, decode_roundtrip};
use std::hint::black_box;
use std::sync::Arc;

fn record_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(38, 18), false),
        Field::new("ts", DataType::Utf8, false),
    ]))
}

/// Mixed-type messages: a string key, a category, an 18-dp decimal amount, and a
/// timestamp string. The 18-decimal field exercises the precision-sensitive path.
fn make_payloads(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let frac = 123_456_789_012_345_678u64 + (i as u64 % 1000);
            format!(
                "{{\"key\":\"item_{i:04}\",\"category\":\"cat_{}\",\"amount\":{}.{frac:018},\"ts\":\"2026-01-01T00:00:{:02}Z\"}}",
                i % 8,
                600 + (i % 50),
                i % 60
            )
            .into_bytes()
        })
        .collect()
}

fn bench_decode(c: &mut Criterion) {
    let schema = record_schema();
    let mut group = c.benchmark_group("kafka_json_decode");
    for &n in &[100usize, 1_000, 10_000] {
        let owned = make_payloads(n);
        let payloads: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("direct", n), &payloads, |b, p| {
            b.iter(|| black_box(decode_direct(black_box(p), &schema).expect("direct")));
        });
        group.bench_with_input(BenchmarkId::new("roundtrip", n), &payloads, |b, p| {
            b.iter(|| black_box(decode_roundtrip(black_box(p), &schema).expect("roundtrip")));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
