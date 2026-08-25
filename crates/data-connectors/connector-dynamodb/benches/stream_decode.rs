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

//! Benchmarks `process_batch` (`crates/data-connectors/connector-dynamodb/src/stream.rs`)
//! -- the boundary where a DynamoDB Streams `GetRecords` batch becomes a
//! normalized `ChangeBatch` -- replaying SDK-builder-constructed `Record`s
//! instead of a live stream. This is the same pattern
//! `connector-mongodb/benches/mongodb_change_stream.rs` uses, and answers
//! directly whether DynamoDB CDC decode is testable without AWS: it is --
//! `process_batch` is a plain sync function over owned, builder-constructed
//! types, exactly as twelve in-crate unit tests already exercise it.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_lossless)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use aws_sdk_dynamodbstreams::types::{
    AttributeValue as StreamsAttributeValue, OperationType, Record, StreamRecord,
};
use connector_dynamodb::stream::process_batch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use dynamodb_streams::DynamoDBStreamBatch;
use dynamodb_streams::checkpoint::Checkpoint;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
    ]))
}

fn item(id: i64, name: &str, score: i64) -> HashMap<String, StreamsAttributeValue> {
    HashMap::from([
        ("id".to_string(), StreamsAttributeValue::N(id.to_string())),
        (
            "name".to_string(),
            StreamsAttributeValue::S(name.to_string()),
        ),
        (
            "score".to_string(),
            StreamsAttributeValue::N(score.to_string()),
        ),
    ])
}

fn key(id: i64) -> HashMap<String, StreamsAttributeValue> {
    HashMap::from([("id".to_string(), StreamsAttributeValue::N(id.to_string()))])
}

fn insert_record(id: i64) -> Record {
    Record::builder()
        .event_name(OperationType::Insert)
        .dynamodb(
            StreamRecord::builder()
                .set_new_image(Some(item(id, &format!("name-{id}"), id)))
                .set_keys(Some(key(id)))
                .build(),
        )
        .build()
}

fn modify_record(id: i64) -> Record {
    Record::builder()
        .event_name(OperationType::Modify)
        .dynamodb(
            StreamRecord::builder()
                .set_new_image(Some(item(id, &format!("updated-{id}"), id + 1)))
                .set_keys(Some(key(id)))
                .build(),
        )
        .build()
}

fn remove_record(id: i64) -> Record {
    Record::builder()
        .event_name(OperationType::Remove)
        .dynamodb(StreamRecord::builder().set_keys(Some(key(id))).build())
        .build()
}

fn batch_of(records: Vec<Record>) -> DynamoDBStreamBatch {
    DynamoDBStreamBatch {
        records,
        checkpoint: Checkpoint {
            shards: HashMap::default(),
        },
        watermark: None,
    }
}

fn insert_batch(size: usize) -> DynamoDBStreamBatch {
    batch_of((0..size).map(|i| insert_record(i as i64)).collect())
}

fn mixed_batch(size: usize) -> DynamoDBStreamBatch {
    let records = (0..size)
        .map(|i| {
            let id = i as i64;
            match i % 3 {
                0 => insert_record(id),
                1 => modify_record(id),
                _ => remove_record(id),
            }
        })
        .collect();
    batch_of(records)
}

fn bench_dynamodb_stream_decode(c: &mut Criterion) {
    let table_schema = schema();
    let primary_keys = vec!["id".to_string()];
    let mut group = c.benchmark_group("dynamodb_stream_decode");

    for size in [100, 1_000, 5_000] {
        group.bench_with_input(BenchmarkId::new("insert_batch", size), &size, |b, &size| {
            b.iter_batched(
                || insert_batch(size),
                |batch| {
                    let (change_batch, _checkpoint, _watermark) =
                        process_batch(batch, &table_schema, &primary_keys, None, "", None)
                            .expect("process_batch succeeds");
                    black_box(change_batch);
                },
                BatchSize::LargeInput,
            );
        });
    }

    for size in [100, 1_000, 5_000] {
        group.bench_with_input(BenchmarkId::new("mixed_batch", size), &size, |b, &size| {
            b.iter_batched(
                || mixed_batch(size),
                |batch| {
                    let (change_batch, _checkpoint, _watermark) =
                        process_batch(batch, &table_schema, &primary_keys, None, "", None)
                            .expect("process_batch succeeds");
                    black_box(change_batch);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_dynamodb_stream_decode);
criterion_main!(benches);
