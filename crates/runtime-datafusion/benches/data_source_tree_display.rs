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

//! Microbenchmarks for `DataSourceTreeDisplayOptimizer`.
//!
//! The optimizer walks the physical plan once on every query to surface
//! pushed-down `limit=N` values in `EXPLAIN`-tree output. This bench measures
//! its per-query cost on plans of varying size, both when no scan has a fetch
//! limit (the hot path — early `Transformed::no` at each node) and when every
//! scan has one (worst case for the wrapping path).
//!
//! Run with: `cargo bench -p runtime-datafusion --bench data_source_tree_display`

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use runtime_datafusion::extension::data_source_tree_display::DataSourceTreeDisplayOptimizer;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn data_source_exec(fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0i64..10))],
    )
    .expect("batch");
    let source: Arc<dyn DataSource> =
        Arc::new(MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("memory source"));
    let source = if let Some(limit) = fetch {
        source.with_fetch(Some(limit)).expect("with_fetch")
    } else {
        source
    };
    Arc::new(DataSourceExec::new(source))
}

/// Build a representative plan: a `UnionExec` over `n_scans` `DataSourceExec`s,
/// each either with or without a fetch limit.
fn plan_with_n_scans(n_scans: usize, with_fetch: bool) -> Arc<dyn ExecutionPlan> {
    let scans: Vec<Arc<dyn ExecutionPlan>> = (0..n_scans)
        .map(|_| data_source_exec(with_fetch.then_some(2)))
        .collect();
    Arc::new(UnionExec::new(scans))
}

fn bench_optimizer(c: &mut Criterion) {
    let optimizer = DataSourceTreeDisplayOptimizer::new();
    let config = ConfigOptions::new();

    let mut group = c.benchmark_group("data_source_tree_display_optimizer");

    // Cold path: scans without any fetch limit — every node short-circuits via
    // `Transformed::no` after a single `downcast_ref` + `fetch().is_none()` check.
    for n in [1usize, 10, 50, 100] {
        let plan = plan_with_n_scans(n, false);
        group.bench_with_input(BenchmarkId::new("no_fetch", n), &plan, |b, plan| {
            b.iter(|| {
                let optimized = optimizer
                    .optimize(Arc::clone(plan), &config)
                    .expect("optimize");
                black_box(optimized);
            });
        });
    }

    // Worst case: every scan has a fetch limit, so each node also pays for the
    // `Arc::clone` + wrapper allocation + `with_data_source`.
    for n in [1usize, 10, 50, 100] {
        let plan = plan_with_n_scans(n, true);
        group.bench_with_input(BenchmarkId::new("all_fetch", n), &plan, |b, plan| {
            b.iter(|| {
                let optimized = optimizer
                    .optimize(Arc::clone(plan), &config)
                    .expect("optimize");
                black_box(optimized);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_optimizer);
criterion_main!(benches);
