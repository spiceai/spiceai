#![allow(
    clippy::expect_used,
    clippy::redundant_closure_for_method_calls,
    clippy::borrow_deref_ref
)]

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_components::arrow::write::MemTable;
use data_components::arrow::write::bench_wrappers::{
    check_and_filter_unique_constraint, extract_primary_keys_str, filter_existing,
};
use datafusion::catalog::TableProvider;
use datafusion::common::{Constraint, Constraints};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion_table_providers::util::on_conflict::OnConflict;
use datafusion_table_providers::util::test::MockExec;
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

fn create_test_batch(size: usize) -> (RecordBatch, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let ids: Vec<String> = (0..size).map(|i| format!("id_{i:05}")).collect();
    let values: Vec<String> = (0..size).map(|i| format!("value_{i}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("Failed to create batch");

    (batch, schema)
}

// Benchmark: Primary key uniqueness checking for large datasets
fn bench_check_unique_constraint(c: &mut Criterion) {
    let mut group = c.benchmark_group("check_unique_constraint");

    for size in [1_000, 10_000, 50_000, 100_000] {
        let ids_owned: Vec<String> = (0..size).map(|i| format!("id_{i:05}")).collect();
        let ids: Vec<&str> = ids_owned.iter().map(std::ops::Deref::deref).collect();

        group.bench_with_input(BenchmarkId::new("unique_ids", size), &ids, |b, ids| {
            b.iter(|| {
                let result = check_and_filter_unique_constraint(black_box(ids), None);
                black_box(result).expect("Should succeed");
            });
        });
    }

    // Benchmark with existing set to check against
    for size in [1_000, 10_000, 50_000] {
        let ids_owned: Vec<String> = (0..size).map(|i| format!("new_id_{i:05}")).collect();
        let ids: Vec<&str> = ids_owned.iter().map(std::ops::Deref::deref).collect();
        let existing: HashSet<String> = (0..size).map(|i| format!("old_id_{i:05}")).collect();

        group.bench_with_input(
            BenchmarkId::new("unique_ids_with_existing", size),
            &(ids, existing),
            |b, (ids, existing)| {
                b.iter(|| {
                    let result = check_and_filter_unique_constraint(
                        black_box(ids),
                        Some(black_box(existing)),
                    );
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Primary key extraction from RecordBatch
fn bench_extract_primary_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_primary_keys");

    for size in [1_000, 10_000, 50_000, 100_000] {
        let (batch, _schema) = create_test_batch(size);
        let pk_indices = vec![0]; // Single column primary key

        group.bench_with_input(
            BenchmarkId::new("single_column_pk", size),
            &(batch, pk_indices),
            |b, (batch, pk_indices)| {
                b.iter(|| {
                    let result = extract_primary_keys_str(black_box(batch), black_box(pk_indices));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    // Composite primary key benchmark
    for size in [1_000, 10_000, 50_000] {
        let (batch, _schema) = create_test_batch(size);
        let pk_indices = vec![0, 1]; // Composite key

        group.bench_with_input(
            BenchmarkId::new("composite_pk", size),
            &(batch, pk_indices),
            |b, (batch, pk_indices)| {
                b.iter(|| {
                    let result = extract_primary_keys_str(black_box(batch), black_box(pk_indices));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Filter existing batches (for upsert operations)
fn bench_filter_existing(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_existing");

    for batch_size in [1_000, 10_000, 50_000] {
        for conflict_ratio in [10, 50, 90] {
            // conflict_ratio is the percentage of rows that conflict
            let (batch, _schema) = create_test_batch(batch_size);
            let existing_batches = vec![batch];

            let pk_indices = vec![0];
            let conflict_count = (batch_size * conflict_ratio) / 100;
            let overwriting_keys: HashSet<String> =
                (0..conflict_count).map(|i| format!("id_{i:05}")).collect();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("size_{batch_size}_conflict_{conflict_ratio}pct"),
                    batch_size,
                ),
                &(
                    existing_batches.clone(),
                    overwriting_keys.clone(),
                    pk_indices.clone(),
                ),
                |b, (batches, keys, pk_idx)| {
                    b.iter(|| {
                        let mut batches_copy = batches.clone();
                        let result = filter_existing(
                            black_box(&mut batches_copy),
                            black_box(keys),
                            black_box(pk_idx),
                        );
                        black_box(result).expect("Should succeed");
                    });
                },
            );
        }
    }

    group.finish();
}

// Benchmark: Full insert operation with primary keys
fn bench_insert_with_primary_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_with_primary_key");
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    for size in [100, 1_000, 10_000] {
        let (initial_batch, schema) = create_test_batch(size);
        // Create insert batch with different IDs (offset by size to avoid conflicts for append)
        let insert_ids: Vec<String> = (size..size * 2).map(|i| format!("id_{i:05}")).collect();
        let insert_values: Vec<String> = (size..size * 2).map(|i| format!("value_{i}")).collect();
        let insert_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(insert_ids)),
                Arc::new(StringArray::from(insert_values)),
            ],
        )
        .expect("Failed to create insert batch");

        group.bench_with_input(BenchmarkId::new("append", size), &size, |b, _size| {
            b.iter(|| {
                rt.block_on(async {
                    let table =
                        MemTable::try_new(Arc::clone(&schema), vec![vec![initial_batch.clone()]])
                            .expect("Failed to create table")
                            .try_with_constraints(Constraints::new_unverified(vec![
                                Constraint::PrimaryKey(vec![0]),
                            ]))
                            .await
                            .expect("Failed to set constraints");

                    let ctx = SessionContext::new();
                    let state = ctx.state();
                    let exec = Arc::new(MockExec::new(
                        vec![Ok(insert_batch.clone())],
                        Arc::clone(&schema),
                    ));

                    let insertion = table
                        .insert_into(&state, exec, InsertOp::Append)
                        .await
                        .expect("Failed to create insertion plan");

                    let result = collect(insertion, ctx.task_ctx())
                        .await
                        .expect("Failed to execute");
                    black_box(result);
                });
            });
        });
    }

    group.finish();
}

// Benchmark: Upsert operations
fn bench_upsert_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("upsert_operation");
    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    for size in [100, 1_000, 5_000] {
        for conflict_ratio in [10, 50, 90] {
            let (initial_batch, schema) = create_test_batch(size);

            // Create insert batch with some conflicting keys
            let conflict_count = (size * conflict_ratio) / 100;
            let ids: Vec<String> = (0..conflict_count)
                .map(|i| format!("id_{i:05}")) // Conflicting IDs
                .chain((conflict_count..size).map(|i| format!("new_id_{i:05}"))) // New IDs
                .collect();
            let values: Vec<String> = (0..size).map(|i| format!("updated_value_{i}")).collect();

            let insert_batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .expect("Failed to create batch");

            group.bench_with_input(
                BenchmarkId::new(format!("size_{size}_conflict_{conflict_ratio}pct"), size),
                &size,
                |b, _size| {
                    b.iter(|| {
                        rt.block_on(async {
                            let table = MemTable::try_new(
                                Arc::clone(&schema),
                                vec![vec![initial_batch.clone()]],
                            )
                            .expect("Failed to create table")
                            .try_with_constraints(Constraints::new_unverified(vec![
                                Constraint::PrimaryKey(vec![0]),
                            ]))
                            .await
                            .expect("Failed to set constraints")
                            .with_on_conflict(
                                OnConflict::try_from("upsert:id").expect("create on_conflict"),
                            );

                            let ctx = SessionContext::new();
                            let state = ctx.state();
                            let exec = Arc::new(MockExec::new(
                                vec![Ok(insert_batch.clone())],
                                Arc::clone(&schema),
                            ));

                            let insertion = table
                                .insert_into(&state, exec, InsertOp::Append)
                                .await
                                .expect("Failed to create insertion plan");

                            let result = collect(insertion, ctx.task_ctx())
                                .await
                                .expect("Failed to execute");
                            black_box(result);
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_check_unique_constraint,
    bench_extract_primary_keys,
    bench_filter_existing,
    bench_insert_with_primary_key,
    bench_upsert_operation,
);
criterion_main!(benches);
