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

//! Quantifies the refresh pipeline's serial-vs-pipelined ceiling described
//! in `docs/dev/refresh_pipelining.md` (status: proposal, not yet
//! implemented — no `PrefetchExec` exists in the codebase today): a dataset
//! refresh runs `fetch -> embed -> index -> write` as a strictly serial
//! pull-one-batch, fully-transform, write-one-batch loop today, so per-batch
//! wall-clock is `t_fetch + t_embed + t_index + t_write` (sum). A pipelining
//! boundary that overlaps stage N+1 with stage N would let steady-state
//! wall-clock approach `max(t_fetch, t_embed, t_index, t_write)` instead.
//!
//! This bench does not call the real pipeline stages — reaching them needs
//! the full `EmbeddingModelStore`/`IndexerExec` machinery, out of scope for
//! an isolated component bench. It models each stage as an async unit of
//! work with an injectable latency, using the same shape the real code has:
//! `EmbeddingTableExec::execute` awaits its embed step before yielding
//! (`crates/runtime-search/src/embeddings/execution_plan.rs:117-132`), and
//! `IndexerExec` chains with `.and_then(...)`
//! (`crates/runtime-datafusion-index/src/analyzer/index_table_scan.rs`).
//! The `serial` arm reproduces that shape; the `pipelined` arm measures the
//! ceiling a real `PrefetchExec` could reach. The delta between them is
//! exactly the opportunity this bench exists to quantify, and is the target
//! a pipelining implementation should be judged against.
//!
//! The second group, `embed_column_fanout`, isolates one concrete instance
//! of the same pattern:
//! `compute_additional_embedding_columns` (`execution_plan.rs:247-361`)
//! embeds each configured column in a strictly serial `for` loop
//! (`execution_plan.rs:271`). This group times N independent per-column
//! embed calls serially vs. concurrently, for a directly actionable
//! "parallelize this loop" comparison.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime as TokioRuntime;

/// One pipeline stage: I/O-adjacent async work with an injectable cost.
/// `tokio::time::sleep` stands in for the awaited work every real stage
/// does (a network fetch, a model call, an index write) without needing a
/// live source, model, or accelerator.
async fn stage(cost: Duration) -> u64 {
    tokio::time::sleep(cost).await;
    black_box(1)
}

/// Runs the four stages strictly serially, in fetch -> embed -> index ->
/// write order -- the shape of today's refresh loop.
async fn run_serial(costs: [Duration; 4]) -> u64 {
    let mut total = 0;
    for cost in costs {
        total += stage(cost).await;
    }
    total
}

/// Runs the four stages concurrently -- the steady-state ceiling a
/// pipelining boundary (overlapping stage N+1 with stage N across batches)
/// would approach, per `docs/dev/refresh_pipelining.md`'s own cost model.
async fn run_pipelined(costs: [Duration; 4]) -> u64 {
    let (a, b, c, d) = tokio::join!(
        stage(costs[0]),
        stage(costs[1]),
        stage(costs[2]),
        stage(costs[3]),
    );
    a + b + c + d
}

fn bench_pipeline_stages(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("refresh_pipeline_stages");
    group.sample_size(20);

    // A representative stage-cost mix: fetch and write dominate, embed and
    // index are cheaper per batch but still non-trivial. Values are kept in
    // the low-hundreds-of-microseconds range so a full criterion sample
    // completes quickly -- the *ratio* between serial and pipelined is what
    // this bench reports, not the absolute magnitude.
    let costs = [
        Duration::from_micros(800), // fetch
        Duration::from_micros(600), // embed
        Duration::from_micros(400), // index
        Duration::from_micros(700), // write
    ];

    group.bench_function("serial", |b| {
        b.iter(|| rt.block_on(run_serial(costs)));
    });
    group.bench_function("pipelined", |b| {
        b.iter(|| rt.block_on(run_pipelined(costs)));
    });

    group.finish();
}

async fn embed_columns_serial(per_column_cost: Duration, n: usize) {
    for _ in 0..n {
        stage(per_column_cost).await;
    }
}

async fn embed_columns_concurrent(per_column_cost: Duration, n: usize) {
    let futures = (0..n).map(|_| stage(per_column_cost));
    futures::future::join_all(futures).await;
}

fn bench_embed_column_fanout(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("embed_column_fanout");
    group.sample_size(20);

    let per_column_cost = Duration::from_micros(500);
    for &n in &[1usize, 2, 4, 8] {
        group.bench_with_input(BenchmarkId::new("serial", n), &n, |b, &n| {
            b.iter(|| rt.block_on(embed_columns_serial(per_column_cost, n)));
        });
        group.bench_with_input(BenchmarkId::new("concurrent", n), &n, |b, &n| {
            b.iter(|| rt.block_on(embed_columns_concurrent(per_column_cost, n)));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pipeline_stages, bench_embed_column_fanout);
criterion_main!(benches);
