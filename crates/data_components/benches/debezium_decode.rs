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

//! Microbench for Debezium JSON change events into a change batch, with the
//! Arrow schema inferred from the events and with timestamp columns declared as
//! nanoseconds. `parse_decode` starts from the raw Kafka message bytes, as the
//! connector does; `inferred` and `declared_ns` start from parsed events. Run with:
//! `cargo bench -p data_components --features debezium --bench debezium_decode`

#![allow(clippy::expect_used)]

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use data_components::debezium::{
    arrow::{changes::vector_to_change_batch, convert_fields_to_arrow_schema},
    change_event::ChangeEvent,
};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;

/// Plain columns before the timestamps, so a lookup by name walks a wide row.
const FILLER_COLUMNS: usize = 16;

fn row_fields() -> Vec<serde_json::Value> {
    let mut fields = vec![json!({"type": "int64", "optional": false, "field": "id"})];
    for i in 0..FILLER_COLUMNS {
        fields.push(json!({"type": "string", "optional": true, "field": format!("s{i}")}));
    }
    fields.extend([
        json!({"type": "string", "optional": true, "name": "io.debezium.time.ZonedTimestamp", "field": "at_tz"}),
        json!({"type": "int64", "optional": true, "name": "io.debezium.time.MicroTimestamp", "field": "at_us"}),
    ]);
    fields
}

fn make_events(n: usize) -> Vec<ChangeEvent> {
    let row = json!({"type": "struct", "optional": true, "fields": row_fields()});
    let schema = json!({"type": "struct", "optional": false, "fields": [
        {"field": "before", "type": "struct", "optional": true, "fields": row["fields"]},
        {"field": "after", "type": "struct", "optional": true, "fields": row["fields"]},
    ]});
    (0..n)
        .map(|i| {
            let mut after = serde_json::Map::new();
            after.insert("id".to_string(), json!(i));
            for c in 0..FILLER_COLUMNS {
                after.insert(format!("s{c}"), json!(format!("value {i} {c}")));
            }
            after.insert(
                "at_tz".to_string(),
                json!(format!("2026-11-18T21:{:02}:{:02}.123456Z", i % 60, i % 59)),
            );
            after.insert(
                "at_us".to_string(),
                json!(1_795_035_600_123_456_i64 + i64::try_from(i).expect("fits")),
            );
            serde_json::from_value(json!({
                "schema": schema,
                "payload": {"before": null, "after": after, "op": "c", "ts_ms": 0},
            }))
            .expect("valid change event")
        })
        .collect()
}

fn declared_ns(inferred: &Schema) -> Schema {
    Schema::new(
        inferred
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Timestamp(_, tz) => Field::new(
                    f.name(),
                    DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                    f.is_nullable(),
                ),
                _ => f.as_ref().clone(),
            })
            .collect::<Vec<_>>(),
    )
}

fn bench_decode(c: &mut Criterion) {
    let primary_key = vec!["id".to_string()];
    let mut group = c.benchmark_group("debezium_json_decode");
    // Every sample decodes the same number of batches, so per-sample times are
    // comparable and give tail percentiles.
    group.sampling_mode(SamplingMode::Flat);
    for &n in &[1_000usize, 10_000] {
        let events = make_events(n);
        let refs: Vec<&ChangeEvent> = events.iter().collect();
        let inferred = Arc::new(
            convert_fields_to_arrow_schema(events[0].get_schema_fields().expect("schema"))
                .expect("inferred schema"),
        );
        group.throughput(Throughput::Elements(n as u64));

        let raw: Vec<Vec<u8>> = events
            .iter()
            .map(|e| serde_json::to_vec(e).expect("serialize"))
            .collect();
        group.bench_with_input(BenchmarkId::new("parse_decode", n), &raw, |b, raw| {
            b.iter(|| {
                let parsed: Vec<ChangeEvent> = black_box(raw)
                    .iter()
                    .map(|m| serde_json::from_slice(m).expect("parse"))
                    .collect();
                let refs: Vec<&ChangeEvent> = parsed.iter().collect();
                black_box(
                    vector_to_change_batch(&inferred, &primary_key, &refs, None).expect("decode"),
                )
            });
        });
        group.bench_with_input(BenchmarkId::new("inferred", n), &refs, |b, refs| {
            b.iter(|| {
                black_box(
                    vector_to_change_batch(&inferred, &primary_key, black_box(refs), None)
                        .expect("decode"),
                )
            });
        });
        group.bench_with_input(BenchmarkId::new("declared_ns", n), &refs, |b, refs| {
            let declared = Arc::new(declared_ns(&inferred));
            b.iter(|| {
                black_box(
                    vector_to_change_batch(&declared, &primary_key, black_box(refs), None)
                        .expect("decode"),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
