#![allow(clippy::expect_used)]

use std::{hint::black_box, sync::Arc};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;

#[derive(Clone, Copy)]
enum ParamScenario {
    Named,
    List,
    Mixed,
}

impl ParamScenario {
    fn label(self) -> &'static str {
        match self {
            Self::Named => "named",
            Self::List => "list",
            Self::Mixed => "mixed",
        }
    }
}

fn create_param_batch(size: usize, scenario: ParamScenario) -> RecordBatch {
    let int_values = Int32Array::from(vec![42; size]);
    let string_values = StringArray::from(vec!["test_value"; size]);

    match scenario {
        ParamScenario::Named => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("int_param", DataType::Int32, false),
                Field::new("string_param", DataType::Utf8, false),
            ]));

            RecordBatch::try_new(schema, vec![Arc::new(int_values), Arc::new(string_values)])
                .expect("failed to create record batch")
        }
        ParamScenario::List => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("$1", DataType::Int32, false),
                Field::new("$2", DataType::Utf8, false),
            ]));

            RecordBatch::try_new(schema, vec![Arc::new(int_values), Arc::new(string_values)])
                .expect("failed to create record batch")
        }
        ParamScenario::Mixed => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("$1", DataType::Int32, false),
                Field::new("param2", DataType::Utf8, false),
            ]));

            RecordBatch::try_new(schema, vec![Arc::new(int_values), Arc::new(string_values)])
                .expect("failed to create record batch")
        }
    }
}

/// Benchmark parameter binding time (`RecordBatch` → `ParamValues` conversion).
/// Measures both named (`$index` absent) and positional (`$index`) scenarios.
fn bench_parameter_binding(c: &mut Criterion) {
    let mut group = c.benchmark_group("parameter_binding");

    // Create sample parameter batches of different sizes
    let sizes = vec![1, 10, 100, 1000];

    let scenarios = [
        ParamScenario::Named,
        ParamScenario::List,
        ParamScenario::Mixed,
    ];

    for &scenario in &scenarios {
        for size in &sizes {
            let batch = create_param_batch(*size, scenario);

            group.bench_with_input(
                BenchmarkId::new(scenario.label(), size),
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
    group.bench_function("parameter_binding_overhead_named", |b| {
        b.iter(|| {
            let batch = create_param_batch(1, ParamScenario::Named);
            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");
            black_box(());
        });
    });

    group.bench_function("parameter_binding_overhead_list", |b| {
        b.iter(|| {
            let batch = create_param_batch(1, ParamScenario::List);
            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");
            black_box(());
        });
    });

    group.bench_function("parameter_binding_overhead_mixed", |b| {
        b.iter(|| {
            let batch = create_param_batch(1, ParamScenario::Mixed);
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

    // Benchmark: Full workflow with named parameters
    group.bench_function("with_parameterization_named", |b| {
        b.to_async(&rt).iter(|| async {
            // 1. Create plan (simulating the query)
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");

            // 2. Parameter binding overhead (with mixed int and string types)
            let batch = create_param_batch(1, ParamScenario::Named);
            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");

            black_box(());
        });
    });

    group.bench_function("with_parameterization_list", |b| {
        b.to_async(&rt).iter(|| async {
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");

            let batch = create_param_batch(1, ParamScenario::List);
            let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                .expect("Failed to convert");

            black_box(());
        });
    });

    group.bench_function("with_parameterization_mixed", |b| {
        b.to_async(&rt).iter(|| async {
            let _plan = ctx
                .state()
                .create_logical_plan(literal_query)
                .await
                .expect("Failed to create plan");

            let batch = create_param_batch(1, ParamScenario::Mixed);
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

/// Benchmark multi-threaded parameter binding performance
/// This measures how well parameter conversion scales across threads
fn bench_multithreaded_parameter_binding(c: &mut Criterion) {
    let mut group = c.benchmark_group("multithreaded_parameter_binding");

    let thread_counts = vec![2, 4, 8];
    let param_size = 10; // Use moderate parameter count for realistic scenario

    for thread_count in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("list_params", thread_count),
            &thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            std::thread::spawn(move || {
                                let batch = create_param_batch(param_size, ParamScenario::List);
                                arrow_tools::record_batch::record_to_param_values(&batch)
                                    .expect("Failed to convert")
                            })
                        })
                        .collect();

                    for handle in handles {
                        black_box(handle.join().expect("Thread panicked"));
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("named_params", thread_count),
            &thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            std::thread::spawn(move || {
                                let batch = create_param_batch(param_size, ParamScenario::Named);
                                arrow_tools::record_batch::record_to_param_values(&batch)
                                    .expect("Failed to convert")
                            })
                        })
                        .collect();

                    for handle in handles {
                        black_box(handle.join().expect("Thread panicked"));
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark multi-session query planning performance
/// This simulates multiple independent sessions executing parameterized queries
fn bench_multisession_query_planning(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    let mut group = c.benchmark_group("multisession_query_planning");

    let session_counts = vec![2, 4, 8];
    let query = "SELECT 10 + 20 as sum, 10 * 20 as product";

    for session_count in session_counts {
        group.bench_with_input(
            BenchmarkId::new("independent_sessions", session_count),
            &session_count,
            |b, &session_count| {
                b.to_async(&rt).iter(|| async move {
                    // Create independent session contexts (simulating different clients)
                    let sessions: Vec<_> =
                        (0..session_count).map(|_| SessionContext::new()).collect();

                    // Execute query planning concurrently across all sessions
                    let futures = sessions.iter().map(|ctx| async {
                        let _plan = ctx
                            .state()
                            .create_logical_plan(query)
                            .await
                            .expect("Failed to create plan");

                        // Simulate parameter binding for each session
                        let batch = create_param_batch(5, ParamScenario::List);
                        let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                            .expect("Failed to convert");
                    });

                    futures::future::join_all(futures).await;
                    black_box(());
                });
            },
        );

        // Benchmark shared session context (simulating connection pooling)
        group.bench_with_input(
            BenchmarkId::new("shared_session", session_count),
            &session_count,
            |b, &session_count| {
                b.to_async(&rt).iter(|| async move {
                    // Single shared session context (simulating connection pool)
                    let ctx = SessionContext::new();

                    // Execute query planning concurrently with the same session
                    let futures = (0..session_count).map(|_| async {
                        let _plan = ctx
                            .state()
                            .create_logical_plan(query)
                            .await
                            .expect("Failed to create plan");

                        // Simulate parameter binding
                        let batch = create_param_batch(5, ParamScenario::List);
                        let _params = arrow_tools::record_batch::record_to_param_values(&batch)
                            .expect("Failed to convert");
                    });

                    futures::future::join_all(futures).await;
                    black_box(());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parameter_binding,
    bench_parameterized_vs_literal_planning,
    bench_parameterized_workflow,
    bench_recordbatch_creation,
    bench_multithreaded_parameter_binding,
    bench_multisession_query_planning
);
criterion_main!(benches);
