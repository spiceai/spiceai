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

//! Microbenchmarks for `ExactLeftAccumulator` dynamic join filters.
//!
//! These measure the two important paths for Cayenne large joins:
//! accumulating exact in-list keys while below the memory cap, and switching to
//! the conservative range fallback when the exact key set would exceed it.
//!
//! Run with: `cargo bench -p runtime-datafusion --bench join_accumulator`

#![allow(clippy::expect_used)]

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::physical_plan::{
    PhysicalExpr,
    expressions::col,
    joins::{CollectLeftAccumulator, ColumnBounds},
};
use runtime_datafusion::join_accumulator::ExactLeftAccumulator;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "join_key",
        DataType::UInt64,
        false,
    )]))
}

fn batch_with_row_count(row_count: usize) -> RecordBatch {
    batch_with_range(0, row_count)
}

fn batch_with_range(start: u64, row_count: usize) -> RecordBatch {
    let schema = schema();
    let row_count = u64::try_from(row_count).expect("row count should fit in u64");
    let end = start
        .checked_add(row_count)
        .expect("benchmark row range should fit in u64");
    let values = UInt64Array::from_iter_values(start..end);
    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
        .expect("record batch should be valid")
}

fn join_key_expr(schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
    col("join_key", schema).expect("join key column should exist")
}

fn exact_bounds(batch: &RecordBatch, expr: Arc<dyn PhysicalExpr>) -> Arc<dyn ColumnBounds> {
    let mut accumulator = ExactLeftAccumulator::try_new(expr, &batch.schema())
        .expect("accumulator should be created");
    accumulator
        .update_batch(batch)
        .expect("batch should update exact accumulator");
    accumulator
        .evaluate()
        .expect("exact accumulator should evaluate")
}

fn range_fallback_bounds(
    batch: &RecordBatch,
    expr: Arc<dyn PhysicalExpr>,
) -> Arc<dyn ColumnBounds> {
    let mut accumulator = ExactLeftAccumulator::new_with_memory_limit(expr, 0);
    accumulator
        .update_batch(batch)
        .expect("batch should update range fallback accumulator");
    accumulator
        .evaluate()
        .expect("range fallback accumulator should evaluate")
}

fn bench_update_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_accumulator_update_batch");

    for row_count in [1_024usize, 16_384, 65_536] {
        let batch = batch_with_row_count(row_count);
        let expr = join_key_expr(&batch.schema());
        group.throughput(Throughput::Elements(
            u64::try_from(row_count).expect("row count should fit in u64"),
        ));

        group.bench_with_input(
            BenchmarkId::new("exact_in_list", row_count),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let schema = batch.schema();
                    let mut accumulator =
                        ExactLeftAccumulator::try_new(Arc::clone(&expr), black_box(&schema))
                            .expect("accumulator should be created");
                    accumulator
                        .update_batch(black_box(batch))
                        .expect("batch should update accumulator");
                    black_box(accumulator);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_fallback", row_count),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let mut accumulator =
                        ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&expr), 0);
                    accumulator
                        .update_batch(black_box(batch))
                        .expect("batch should update accumulator");
                    black_box(accumulator);
                });
            },
        );
    }

    group.finish();
}

fn bench_transition_to_range_fallback(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_accumulator_transition_to_range_fallback");

    for row_count in [1_024usize, 16_384] {
        let first_batch = batch_with_row_count(row_count);
        let second_batch = batch_with_range(1_000_000, row_count);
        let expr = join_key_expr(&first_batch.schema());
        let max_memory_size = first_batch.column(0).get_array_memory_size();

        group.throughput(Throughput::Elements(
            u64::try_from(row_count * 2).expect("row count should fit in u64"),
        ));

        group.bench_with_input(
            BenchmarkId::new("exact_until_limit_then_range", row_count),
            &(first_batch, second_batch),
            |b, (first_batch, second_batch)| {
                b.iter(|| {
                    let mut accumulator = ExactLeftAccumulator::new_with_memory_limit(
                        Arc::clone(&expr),
                        max_memory_size,
                    );
                    accumulator
                        .update_batch(black_box(first_batch))
                        .expect("first batch should update accumulator");
                    accumulator
                        .update_batch(black_box(second_batch))
                        .expect("second batch should update accumulator");
                    black_box(accumulator);
                });
            },
        );
    }

    group.finish();
}

fn bench_physical_expr(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_accumulator_physical_expr");

    for row_count in [1_024usize, 16_384] {
        let batch = batch_with_row_count(row_count);
        let build_expr = join_key_expr(&batch.schema());
        let probe_expr = join_key_expr(&batch.schema());
        let exact_bounds = exact_bounds(&batch, Arc::clone(&build_expr));
        let range_bounds = range_fallback_bounds(&batch, build_expr);

        group.throughput(Throughput::Elements(
            u64::try_from(row_count).expect("row count should fit in u64"),
        ));

        group.bench_with_input(
            BenchmarkId::new("exact_in_list", row_count),
            &exact_bounds,
            |b, bounds| {
                b.iter(|| {
                    let physical_expr = bounds
                        .physical_expr(Arc::clone(&probe_expr))
                        .expect("exact physical expression should be created");
                    black_box(physical_expr);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_fallback", row_count),
            &range_bounds,
            |b, bounds| {
                b.iter(|| {
                    let physical_expr = bounds
                        .physical_expr(Arc::clone(&probe_expr))
                        .expect("range fallback physical expression should be created");
                    black_box(physical_expr);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_update_batch,
    bench_transition_to_range_fallback,
    bench_physical_expr
);
criterion_main!(benches);
