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
//! Three groups are measured:
//!
//! - `fsl_f32/simd`: `FixedSizeList<Float32, N>` inputs via `CosineDistance`
//!   (dispatches to simsimd).
//! - `fsl_f32/scalar`: `FixedSizeList<Float32, N>` inputs with a plain Rust loop
//!   that bypasses SIMD entirely (dot product + norms).
//! - `list_f32/spice_scalar`: `List<Float32>` inputs via `CosineDistance`
//!   (hits the scalar fallback path).
//! - `list_f32/datafusion`: `List<Float32>` inputs via `datafusion_functions_nested`
//!   `ArrayDistance`.

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float32Builder, ListBuilder,
};
use arrow_schema::{DataType, Field};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::distance::ArrayDistance;
use runtime_datafusion_udfs::cosine_distance::CosineDistance;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Array builders
// ---------------------------------------------------------------------------

/// Build a `FixedSizeList<Float32, dim>` array with `rows` rows of random data.
#[expect(clippy::expect_used, reason = "bench setup — panics are acceptable")]
fn build_fsl_f32(rows: usize, dim: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let dim_i32 = i32::try_from(dim).expect("dim fits in i32");
    let mut values = Float32Builder::with_capacity(rows * dim);
    // Deterministic pseudo-random values — just use row/col indices scaled to [0,1].
    for row in 0..rows {
        for col in 0..dim {
            #[expect(
                clippy::cast_precision_loss,
                reason = "bench data, precision unimportant"
            )]
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
            #[expect(
                clippy::cast_precision_loss,
                reason = "bench data, precision unimportant"
            )]
            builder
                .values()
                .append_value((row * dim + col) as f32 / (rows * dim) as f32);
        }
        builder.append(true);
    }
    Arc::new(builder.finish()) as ArrayRef
}

// ---------------------------------------------------------------------------
// Scalar FSL cosine distance — plain Rust loop, no SIMD
//
// Extracts the flat f32 buffer from both `FixedSizeList<Float32>` arrays and
// computes cosine distance with a simple dot-product + norm loop so that
// benchmarks can measure the raw SIMD speed-up without calling
// `cosine_distance_inner`.
// ---------------------------------------------------------------------------

#[expect(clippy::expect_used, reason = "bench setup — panics are acceptable")]
fn scalar_fsl_cosine_distance(a: &ArrayRef, b: &ArrayRef) -> Vec<Option<f64>> {
    let fsl_a = a
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("FixedSizeListArray");
    let fsl_b = b
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("FixedSizeListArray");
    let dim = usize::try_from(fsl_a.value_length()).expect("dim fits in usize");

    // The values child of a FixedSizeListArray is a flat Float32Array.
    let flat_a = fsl_a
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32Array values");
    let flat_b = fsl_b
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32Array values");

    let rows = fsl_a.len();
    let mut results = Vec::with_capacity(rows);

    for row in 0..rows {
        let start = row * dim;
        let end = start + dim;

        let slice_a = &flat_a.values()[start..end];
        let slice_b = &flat_b.values()[start..end];

        let mut dot: f64 = 0.0;
        let mut norm_a: f64 = 0.0;
        let mut norm_b: f64 = 0.0;

        for (&x, &y) in slice_a.iter().zip(slice_b.iter()) {
            let xf = f64::from(x);
            let yf = f64::from(y);
            dot += xf * yf;
            norm_a += xf * xf;
            norm_b += yf * yf;
        }

        let denom = norm_a.sqrt() * norm_b.sqrt();
        let similarity = if denom == 0.0 || !denom.is_finite() {
            0.0 // zero-magnitude → orthogonal convention, matches production paths
        } else {
            dot / denom
        };
        let dist = Some((1.0 - similarity) / 2.0);
        results.push(dist);
    }

    results
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

#[expect(clippy::expect_used, reason = "bench setup — panics are acceptable")]
fn invoke(a: ArrayRef, b: ArrayRef, rows: usize) -> ColumnarValue {
    let return_field = Arc::new(Field::new("f", DataType::Float64, true));
    CosineDistance::new()
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(std::hint::black_box(a)),
                ColumnarValue::Array(std::hint::black_box(b)),
            ],
            arg_fields: vec![],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
        .expect("ok")
}

/// Group 1: `FixedSizeList<Float32>` — SIMD (via `CosineDistance`) vs scalar loop.
fn bench_fsl_f32(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_distance/fsl_f32");

    for &rows in &[1_000_usize, 10_000_usize] {
        for &dim in &[128_usize, 512_usize, 1536_usize] {
            let id = BenchmarkId::new(format!("simd/rows={rows}"), format!("dim={dim}"));
            group.bench_with_input(id, &(rows, dim), |b, &(rows, dim)| {
                b.iter_batched(
                    || (build_fsl_f32(rows, dim), build_fsl_f32(rows, dim)),
                    |(a, b)| invoke(a, b, rows),
                    BatchSize::LargeInput,
                );
            });

            let id = BenchmarkId::new(format!("scalar/rows={rows}"), format!("dim={dim}"));
            group.bench_with_input(id, &(rows, dim), |b, &(rows, dim)| {
                b.iter_batched(
                    || (build_fsl_f32(rows, dim), build_fsl_f32(rows, dim)),
                    |(a, b)| {
                        std::hint::black_box(scalar_fsl_cosine_distance(
                            &std::hint::black_box(a),
                            &std::hint::black_box(b),
                        ))
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

/// Group 2: `List<Float32>` — Spice scalar fallback vs DataFusion `ArrayDistance`.
#[expect(clippy::expect_used, reason = "bench setup — panics are acceptable")]
fn bench_list_f32(c: &mut Criterion) {
    let mut group = c.benchmark_group("cosine_distance/list_f32");

    let return_field = Arc::new(arrow_schema::Field::new("result", DataType::Float64, true));

    for &rows in &[1_000_usize, 10_000_usize] {
        for &dim in &[128_usize, 512_usize, 1536_usize] {
            let id = BenchmarkId::new(format!("spice_scalar/rows={rows}"), format!("dim={dim}"));
            group.bench_with_input(id, &(rows, dim), |b, &(rows, dim)| {
                b.iter_batched(
                    || (build_list_f32(rows, dim), build_list_f32(rows, dim)),
                    |(a, b)| invoke(a, b, rows),
                    BatchSize::LargeInput,
                );
            });

            let id = BenchmarkId::new(format!("datafusion/rows={rows}"), format!("dim={dim}"));
            let return_field = Arc::clone(&return_field);
            group.bench_with_input(id, &(rows, dim), |b, &(rows, dim)| {
                b.iter_batched(
                    || (build_list_f32(rows, dim), build_list_f32(rows, dim)),
                    |(a, b)| {
                        let args = ScalarFunctionArgs {
                            args: vec![
                                ColumnarValue::Array(std::hint::black_box(a)),
                                ColumnarValue::Array(std::hint::black_box(b)),
                            ],
                            arg_fields: vec![],
                            number_rows: rows,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::new(ConfigOptions::default()),
                        };
                        ArrayDistance::new().invoke_with_args(args).expect("ok")
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_fsl_f32, bench_list_f32);
criterion_main!(benches);
