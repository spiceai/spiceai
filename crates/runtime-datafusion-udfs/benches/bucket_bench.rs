/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#![allow(
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use arrow::array::{Float64Array, Int64Array, StringArray};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::scalar::ScalarValue;
use runtime_datafusion_udfs::bucket::Bucket;
use std::sync::Arc;

fn create_string_array(size: usize) -> StringArray {
    let values: Vec<String> = (0..size).map(|i| format!("value_{i:08}")).collect();
    StringArray::from(values)
}

fn create_int64_array(size: usize) -> Int64Array {
    let values: Vec<i64> = (0..size).map(|i| i as i64).collect();
    Int64Array::from(values)
}

fn create_float64_array(size: usize) -> Float64Array {
    let values: Vec<f64> = (0..size).map(|i| i as f64 * 1.5).collect();
    Float64Array::from(values)
}

fn bench_bucket_array_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_array_strings");
    let udf = Bucket::new();

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        let array = create_string_array(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                        ColumnarValue::Array(Arc::new(array.clone())),
                    ],
                    number_rows: size,
                    arg_fields: vec![],
                    return_field: Arc::new(arrow_schema::Field::new(
                        "bucket",
                        DataType::Int32,
                        false,
                    )),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
            });
        });
    }

    group.finish();
}

fn bench_bucket_array_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_array_int64");
    let udf = Bucket::new();

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        let array = create_int64_array(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                        ColumnarValue::Array(Arc::new(array.clone())),
                    ],
                    number_rows: size,
                    arg_fields: vec![],
                    return_field: Arc::new(arrow_schema::Field::new(
                        "bucket",
                        DataType::Int32,
                        false,
                    )),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
            });
        });
    }

    group.finish();
}

fn bench_bucket_array_float64(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_array_float64");
    let udf = Bucket::new();

    for size in [100, 1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        let array = create_float64_array(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                        ColumnarValue::Array(Arc::new(array.clone())),
                    ],
                    number_rows: size,
                    arg_fields: vec![],
                    return_field: Arc::new(arrow_schema::Field::new(
                        "bucket",
                        DataType::Int32,
                        false,
                    )),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
            });
        });
    }

    group.finish();
}

fn bench_bucket_varying_num_buckets(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_varying_num_buckets");
    let udf = Bucket::new();
    let size = 10_000;
    let array = create_string_array(size);

    for num_buckets in [10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_buckets),
            &num_buckets,
            |b, &nb| {
                b.iter(|| {
                    let args = ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(nb))),
                            ColumnarValue::Array(Arc::new(array.clone())),
                        ],
                        number_rows: size,
                        arg_fields: vec![],
                        return_field: Arc::new(arrow_schema::Field::new(
                            "bucket",
                            DataType::Int32,
                            false,
                        )),
                        config_options: Arc::new(ConfigOptions::default()),
                    };
                    std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
                });
            },
        );
    }

    group.finish();
}

fn bench_bucket_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_scalar");
    let udf = Bucket::new();

    group.bench_function("string_scalar", |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("test_value".to_string()))),
                ],
                number_rows: 1,
                arg_fields: vec![],
                return_field: Arc::new(arrow_schema::Field::new("bucket", DataType::Int32, false)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
        });
    });

    group.bench_function("int64_scalar", |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(12345))),
                ],
                number_rows: 1,
                arg_fields: vec![],
                return_field: Arc::new(arrow_schema::Field::new("bucket", DataType::Int32, false)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            std::hint::black_box(udf.invoke_with_args(args).expect("invoke UDF"))
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_bucket_array_strings,
    bench_bucket_array_int64,
    bench_bucket_array_float64,
    bench_bucket_varying_num_buckets,
    bench_bucket_scalar
);
criterion_main!(benches);
