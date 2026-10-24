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

//! Per-query cost of pruning in-memory segments and inline entries by their
//! statistics: one pruning predicate per container against one predicate for all
//! of them. A scan pays this for every segment the table holds in memory, before
//! it reads any rows, so it grows with the segment count.
//!
//! The containers mirror a caching-mode table: two random string key columns,
//! a ~3 KB response body, and a few small columns, 250 rows each. The predicate
//! is a point lookup on both keys, which statistics cannot prune for random keys.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use cayenne::__bench_segment_pruning::{
    build_listing_pruning_predicate, should_prune_statistics, statistics_from_record_batches,
    statistics_may_match,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::Statistics;
use datafusion_expr::{col, lit};

const ROWS_PER_CONTAINER: usize = 250;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("request_path", DataType::Utf8, false),
        Field::new("request_query", DataType::Utf8, false),
        Field::new("request_body", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, true),
        Field::new("response_status", DataType::Int64, true),
        Field::new("response_headers", DataType::Utf8, true),
        Field::new("fetched_at", DataType::Int64, true),
    ]))
}

fn container(schema: &SchemaRef, seed: &mut u64) -> Arc<Statistics> {
    let mut next = || {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed >> 33
    };
    let paths: Vec<String> = (0..ROWS_PER_CONTAINER)
        .map(|_| format!("/devices/{}", next() % 2_500_000))
        .collect();
    let content: Vec<String> = (0..ROWS_PER_CONTAINER)
        .map(|_| format!("{{\"v\":{},\"pad\":\"{}\"}}", next(), "x".repeat(3_000)))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(paths)),
        Arc::new(StringArray::from(vec!["v=1"; ROWS_PER_CONTAINER])),
        Arc::new(StringArray::from(vec![""; ROWS_PER_CONTAINER])),
        Arc::new(StringArray::from(content)),
        Arc::new(Int64Array::from(vec![200_i64; ROWS_PER_CONTAINER])),
        Arc::new(StringArray::from(vec![
            "content-type: application/json";
            ROWS_PER_CONTAINER
        ])),
        Arc::new(Int64Array::from(
            (0..ROWS_PER_CONTAINER)
                .map(|i| i64::try_from(i).unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::clone(schema), columns).expect("container batch");
    Arc::new(statistics_from_record_batches(schema, &[batch]))
}

fn bench(c: &mut Criterion) {
    let schema = schema();
    let predicate = build_listing_pruning_predicate(
        &schema,
        &[col("request_path")
            .eq(lit("/devices/1234567"))
            .and(col("request_query").eq(lit("v=1")))],
    )
    .expect("predicate")
    .expect("a prunable predicate");

    let mut group = c.benchmark_group("segment_statistics_pruning");
    for containers in [10_usize, 50, 170, 500] {
        let mut seed = 0x5eed_0000_0000_0001_u64 ^ containers as u64;
        let stats: Vec<Arc<Statistics>> = (0..containers)
            .map(|_| container(&schema, &mut seed))
            .collect();
        let refs: Vec<&Arc<Statistics>> = stats.iter().collect();

        let per_container: Vec<bool> = stats
            .iter()
            .map(|s| !should_prune_statistics(s, &schema, &predicate))
            .collect();
        assert_eq!(
            statistics_may_match(&refs, &schema, &predicate),
            per_container,
            "both strategies must keep the same containers"
        );

        group.bench_with_input(
            BenchmarkId::new("per_container", containers),
            &stats,
            |b, stats| {
                b.iter(|| {
                    stats
                        .iter()
                        .filter(|s| !should_prune_statistics(s, &schema, &predicate))
                        .count()
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("batched", containers), &refs, |b, refs| {
            b.iter(|| {
                statistics_may_match(refs, &schema, &predicate)
                    .into_iter()
                    .filter(|keep| *keep)
                    .count()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
