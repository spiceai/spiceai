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

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use data_components::mongodb::stream::{change_events_to_change_batch, default_unnest_parameters};
use mongodb::bson::{Document, doc, from_document};
use mongodb::change_stream::event::ChangeStreamEvent;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
    ]))
}

fn event(document: Document) -> ChangeStreamEvent<Document> {
    from_document(document).expect("valid change stream event")
}

fn insert_events(size: usize) -> Vec<ChangeStreamEvent<Document>> {
    (0..size)
        .map(|index| {
            let id = i32::try_from(index).expect("benchmark size should fit in i32");
            event(doc! {
                "_id": { "_data": format!("insert-token-{index}") },
                "operationType": "insert",
                "ns": { "db": "db", "coll": "users" },
                "documentKey": { "_id": id },
                "fullDocument": {
                    "_id": id,
                    "name": format!("name-{index}"),
                    "score": i64::from(id)
                }
            })
        })
        .collect()
}

fn mixed_events(size: usize) -> Vec<ChangeStreamEvent<Document>> {
    (0..size)
        .map(|index| {
            let id = i32::try_from(index).expect("benchmark size should fit in i32");
            match index % 3 {
                0 => event(doc! {
                    "_id": { "_data": format!("insert-token-{index}") },
                    "operationType": "insert",
                    "ns": { "db": "db", "coll": "users" },
                    "documentKey": { "_id": id },
                    "fullDocument": {
                        "_id": id,
                        "name": format!("created-{index}"),
                        "score": i64::from(id)
                    }
                }),
                1 => event(doc! {
                    "_id": { "_data": format!("update-token-{index}") },
                    "operationType": "update",
                    "ns": { "db": "db", "coll": "users" },
                    "documentKey": { "_id": id },
                    "fullDocument": {
                        "_id": id,
                        "name": format!("updated-{index}"),
                        "score": i64::from(id) + 1
                    }
                }),
                _ => event(doc! {
                    "_id": { "_data": format!("delete-token-{index}") },
                    "operationType": "delete",
                    "ns": { "db": "db", "coll": "users" },
                    "documentKey": { "_id": id }
                }),
            }
        })
        .collect()
}

fn bench_mongodb_change_stream_conversion(c: &mut Criterion) {
    let schema = schema();
    let primary_keys = vec!["_id".to_string()];
    let unnest_parameters = default_unnest_parameters(0);
    let mut group = c.benchmark_group("mongodb_change_stream_conversion");

    for size in [100, 1_000, 5_000] {
        group.bench_with_input(BenchmarkId::new("insert_events", size), &size, |b, size| {
            b.iter_batched(
                || insert_events(*size),
                |events| {
                    let batch = change_events_to_change_batch(
                        black_box(events),
                        black_box(&schema),
                        black_box(&primary_keys),
                        black_box(&unnest_parameters),
                    )
                    .expect("conversion should succeed")
                    .expect("batch should not be empty");
                    black_box(batch);
                },
                BatchSize::LargeInput,
            );
        });
    }

    for size in [100, 1_000, 5_000] {
        group.bench_with_input(BenchmarkId::new("mixed_events", size), &size, |b, size| {
            b.iter_batched(
                || mixed_events(*size),
                |events| {
                    let batch = change_events_to_change_batch(
                        black_box(events),
                        black_box(&schema),
                        black_box(&primary_keys),
                        black_box(&unnest_parameters),
                    )
                    .expect("conversion should succeed")
                    .expect("batch should not be empty");
                    black_box(batch);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_mongodb_change_stream_conversion);
criterion_main!(benches);
