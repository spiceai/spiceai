#![allow(clippy::expect_used)]

use std::{hint::black_box, sync::Arc};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;

/// Benchmark parameter binding time (`RecordBatch` → `ParamValues` conversion)
/// This measures the overhead of parameterization vs literal values
fn bench_parameter_binding(c: &mut Criterion) {
    let mut group = c.benchmark_group("parameter_binding");

    // Create sample parameter batches of different sizes
    let sizes = vec![1, 10, 100, 1000];

    for size in sizes {
        // Test with mixed types: int and string
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_param", DataType::Int32, false),
            Field::new("string_param", DataType::Utf8, false),
        ]));

        let int_array = Int32Array::from(vec![42; size]);
        let string_array = StringArray::from(vec!["test_value"; size]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(int_array), Arc::new(string_array)],
        )
        .expect("Failed to create record batch");

        group.bench_with_input(
            BenchmarkId::new("convert_to_param_values", size),
            &batch,
            |b, batch| {
                b.iter(|| {
                    black_box(
                        arrow_tools::record_batch::record_to_param_values(batch)
                            .expect("Failed to convert"),
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark parameterized vs non-parameterized query planning
/// This compares the cost of creating plans with parameters vs literal values
fn bench_parameterized_vs_literal_planning(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    let mut group = c.benchmark_group("query_planning");

    let ctx = SessionContext::new();

    // Note: We compare queries without actually planning them with untyped parameters
    // since DataFusion requires type hints for parameter placeholders.
    // Instead, we measure the RecordBatch creation + parameter conversion overhead.

    // Literal query (no parameterization needed)
    let literal_query = "SELECT 10 + 20 as sum, 10 * 20 as product, 10 - 20 as diff";

    // Benchmark: Plan creation for literal query (baseline)
    group.bench_function("literal_query_plan", |b| {
        b.to_async(&rt).iter(|| async {
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");
            black_box(());
        });
    });

    // Benchmark: Parameter binding overhead (what parameterization costs)
    group.bench_function("parameter_binding_overhead", |b| {
        b.iter(|| {
            // This is the overhead of parameterization: creating the RecordBatch
            // and converting to ParamValues (with mixed int and string types)
            let schema = Arc::new(Schema::new(vec![
                Field::new("int_param", DataType::Int32, false),
                Field::new("string_param", DataType::Utf8, false),
            ]));

            let int_array = Int32Array::from(vec![42]);
            let string_array = StringArray::from(vec!["test_value"]);

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(int_array), Arc::new(string_array)],
            )
            .expect("Failed to create record batch");

            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");

            black_box(());
        });
    });

    group.finish();
}

/// Benchmark complete parameterized workflow
/// This measures the full cost: parameter binding + query planning
fn bench_parameterized_workflow(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    let mut group = c.benchmark_group("parameterized_workflow");

    let ctx = SessionContext::new();

    // Literal query (no parameters)
    let literal_query = "SELECT 10 + 20 as sum, 10 * 20 as product";

    // Benchmark: Full workflow with parameter binding overhead
    group.bench_function("with_parameterization", |b| {
        b.to_async(&rt).iter(|| async {
            // 1. Create plan (simulating the query)
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");

            // 2. Parameter binding overhead (with mixed int and string types)
            let schema = Arc::new(Schema::new(vec![
                Field::new("int_param", DataType::Int32, false),
                Field::new("string_param", DataType::Utf8, false),
            ]));

            let int_array = Int32Array::from(vec![42]);
            let string_array = StringArray::from(vec!["test_value"]);

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(int_array), Arc::new(string_array)],
            )
            .expect("Failed to create record batch");

            // 3. Convert to param values (the parameterization cost)
            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");

            black_box(());
        });
    });

    // Benchmark: Full workflow without parameterization
    group.bench_function("without_parameterization", |b| {
        b.to_async(&rt).iter(|| async {
            // Just create the plan - no parameter overhead
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");

            black_box(());
        });
    });

    group.finish();
}

/// Benchmark `RecordBatch` creation overhead
/// This measures just the Arrow data structure creation cost
fn bench_recordbatch_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("recordbatch_creation");

    // Test with mixed types: int and string
    let schema = Arc::new(Schema::new(vec![
        Field::new("int_param", DataType::Int32, false),
        Field::new("string_param", DataType::Utf8, false),
    ]));

    group.bench_function("create_2_params", |b| {
        b.iter(|| {
            let int_array = Int32Array::from(vec![42]);
            let string_array = StringArray::from(vec!["test_value"]);

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(int_array), Arc::new(string_array)],
            )
            .expect("Failed to create record batch");

            black_box(batch)
        });
    });

    // Test with multiple mixed-type parameters
    let schema_many = Arc::new(Schema::new(vec![
        Field::new("p1", DataType::Int32, false),
        Field::new("p2", DataType::Utf8, false),
        Field::new("p3", DataType::Int32, false),
        Field::new("p4", DataType::Utf8, false),
        Field::new("p5", DataType::Int32, false),
        Field::new("p6", DataType::Utf8, false),
        Field::new("p7", DataType::Int32, false),
        Field::new("p8", DataType::Utf8, false),
        Field::new("p9", DataType::Int32, false),
        Field::new("p10", DataType::Utf8, false),
    ]));

    // Pre-allocate arrays outside the benchmark loop
    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..10)
        .map(|i| {
            if i % 2 == 0 {
                Arc::new(Int32Array::from(vec![i])) as Arc<dyn arrow::array::Array>
            } else {
                Arc::new(StringArray::from(vec![format!("value_{i}")]))
                    as Arc<dyn arrow::array::Array>
            }
        })
        .collect();

    group.bench_function("create_10_params", |b| {
        b.iter(|| {
            // Clone Arc pointers (cheap) instead of allocating new arrays
            let batch = RecordBatch::try_new(Arc::clone(&schema_many), arrays.clone())
                .expect("Failed to create record batch");

            black_box(batch)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parameter_binding,
    bench_parameterized_vs_literal_planning,
    bench_parameterized_workflow,
    bench_recordbatch_creation
);
criterion_main!(benches);
