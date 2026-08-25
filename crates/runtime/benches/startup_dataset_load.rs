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

//! Times Spice's dataset-load path -- `Runtime::builder().with_app(app).build()`
//! followed by `load_components()` until every dataset reports ready --
//! against a generated spicepod of N synthetic, purely local `file:`
//! datasets, sweeping N and `runtime.dataset_load_parallelism`.
//!
//! No startup/load duration is measured anywhere in the codebase today
//! (the only related metrics are lifecycle counters -- `dataset_load_state`,
//! `dataset_load_errors` -- not timings), and `dataset_load_parallelism` is
//! a knob nobody can currently tune against real numbers. This bench exists
//! to fill both gaps.
//!
//! Each dataset is a tiny, freshly generated CSV file (generated into a
//! tempdir before the timed region), so this is fully hermetic -- no
//! network, no external services, no committed fixture.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use runtime::Runtime;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::Runtime as RuntimeConfig;
use tempfile::TempDir;
use tokio::runtime::Runtime as TokioRuntime;

/// Writes N tiny CSV files into `dir` and returns one [`Dataset`] per file,
/// each pointed at its own file via the `file:` connector.
fn make_datasets(dir: &std::path::Path, n: usize) -> Vec<Dataset> {
    (0..n)
        .map(|i| {
            let path = dir.join(format!("dataset_{i}.csv"));
            std::fs::write(&path, "id,name\n1,alpha\n2,beta\n3,gamma\n").expect("write csv");
            Dataset::new(
                format!("file:{}", path.to_string_lossy()),
                format!("bench_ds_{i}"),
            )
        })
        .collect()
}

async fn boot_and_wait_ready(n: usize, dataset_load_parallelism: Option<usize>) {
    let temp = TempDir::new().expect("temp dir");
    let datasets = make_datasets(temp.path(), n);

    let mut app_builder = AppBuilder::new("startup_bench");
    for dataset in datasets {
        app_builder = app_builder.with_dataset(dataset);
    }
    if let Some(parallelism) = dataset_load_parallelism {
        app_builder = app_builder.with_runtime(RuntimeConfig {
            dataset_load_parallelism: Some(parallelism),
            ..Default::default()
        });
    }
    let app = app_builder.build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&rt).load_components().await;

    // Poll the actual readiness condition with a bounded timeout, rather
    // than a fixed sleep -- per the repo's own testing convention
    // (`runtime_ready_check` in crates/runtime/tests/utils/mod.rs is not
    // reachable here since it's test-only code, so this reimplements the
    // same poll-with-timeout shape directly against `RuntimeStatus`).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !rt.status().is_ready() {
        if tokio::time::Instant::now() >= deadline {
            panic!("runtime did not become ready within 30s for n={n}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    black_box(rt);
}

fn bench_startup_by_dataset_count(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("startup_dataset_load");
    group.sample_size(10);

    for &n in &[1usize, 4, 16, 64] {
        group.bench_with_input(BenchmarkId::new("datasets", n), &n, |b, &n| {
            b.iter(|| rt.block_on(boot_and_wait_ready(n, None)));
        });
    }

    group.finish();
}

fn bench_startup_by_parallelism(c: &mut Criterion) {
    let rt = TokioRuntime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("startup_dataset_load_parallelism");
    group.sample_size(10);

    const N: usize = 32;
    for &parallelism in &[1usize, 4, 16] {
        group.bench_with_input(
            BenchmarkId::new("parallelism", parallelism),
            &parallelism,
            |b, &parallelism| {
                b.iter(|| rt.block_on(boot_and_wait_ready(N, Some(parallelism))));
            },
        );
    }
    group.bench_function("default", |b| {
        b.iter(|| rt.block_on(boot_and_wait_ready(N, None)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_startup_by_dataset_count,
    bench_startup_by_parallelism
);
criterion_main!(benches);
