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

//! Criterion benchmarks for `cosine_distance` UDF.
//!
//! Two groups are measured:
//! - `fsl_f32`: `FixedSizeList<Float32, N>` inputs → SIMD path via `simsimd`.
//! - `list_f32`: `List<Float32>` inputs → scalar fallback path.

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Builder, ListBuilder};
use arrow_schema::{DataType, Field};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use runtime_datafusion_udfs::cosine_distance::cosine_distance_inner;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Array builders
// ---------------------------------------------------------------------------

/// Build a `FixedSizeList<Float32, dim>` array with `rows` rows of random data.
fn build_fsl_f32(rows: usize, dim: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let dim_i32 = i32::try_from(dim).expect("dim fits in i32");
    let mut values = Float32Builder::with_capacity(rows * dim);
    // Deterministic pseudo-random values — just use row/col indices scaled to [0,1].
    for row in 0..rows {
        for col in 0..dim {
            #[expect(clippy::cast_precision_loss, reason = "bench data, precision unimportant")]
            values.append_value((row * dim + col) as f32 / (rows * dim) as f32);
        }
    }
    Arc::new(
        FixedSizeListArray::try_new(field, dim_i32, Arc::new(values.finish()), None)
            .expect("valid FixedSizeListArray"),
    ) as ArrayRef
}

/// Build a `List<Float32>` array with `rows` rows of `dim` elements each.
fn build_list_f32(rows: usize, dim: usize) -> ArrayRef {
    let mut builder = ListBuilder::new(Float32Builder::with_capacity(rows * dim));
    for row in 0..rows {
        for col in 0..dim {
            #[expect(clippy::cast_precision_loss, reason = "bench data, precision unimportant")]
            builder.values().append_value((row * dim + col) as f32 / (rows * dim) as f32);
        }
        builder.append(true);
    }
    Arc::new(builder.finish()) as ArrayRef
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

fn bench_fsl_simd(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_distance/fsl_f32 (SIMD)");

    for &rows in &[1_000_usize, 10_000_usize] {
        for &dim in &[128_usize, 512_usize, 1536_usize] {
            group.bench_with_input(
                BenchmarkId::new(format!("rows={rows}"), format!("dim={dim}")),
                &(rows, dim),
                |b, &(rows, dim)| {
                    b.iter_batched(
                        || (build_fsl_f32(rows, dim), build_fsl_f32(rows, dim)),
                        |(a, b)| cosine_distance_inner(&[std::hint::black_box(a), std::hint::black_box(b)])
                            .expect("ok"),
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_list_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_distance/list_f32 (scalar)");

    for &rows in &[1_000_usize, 10_000_usize] {
        for &dim in &[128_usize, 512_usize, 1536_usize] {
            group.bench_with_input(
                BenchmarkId::new(format!("rows={rows}"), format!("dim={dim}")),
                &(rows, dim),
                |b, &(rows, dim)| {
                    b.iter_batched(
                        || (build_list_f32(rows, dim), build_list_f32(rows, dim)),
                        |(a, b)| cosine_distance_inner(&[std::hint::black_box(a), std::hint::black_box(b)])
                            .expect("ok"),
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_fsl_simd, bench_list_scalar);
criterion_main!(benches);
