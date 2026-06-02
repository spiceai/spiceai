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

//! Model-based mutation tests for Cayenne.
//!
//! These tests complement the existing targeted regression coverage with bounded,
//! exhaustive state-machine checks over compact operation domains. They validate
//! that small but adversarial histories of inserts/upserts/deletes always match a
//! simple in-memory model both:
//!
//! 1. immediately after each operation, and
//! 2. after reopening the table provider from catalog metadata.
//!
//! This gives strong coverage for protected-snapshot ordering, deletion cache
//! reloads, insert-record persistence, and mixed single-row / batch mutation
//! histories.

#![allow(clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    metadata::CreateTableOptions,
};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{Expr, col, lit};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

type Int64Model = BTreeMap<i64, i64>;
type CompositeKey = (String, i64);
type CompositeModel = BTreeMap<CompositeKey, i64>;

#[derive(Clone, Debug)]
enum Int64MutationOp {
    Upsert { key: i64, value: i64 },
    Delete { key: i64 },
    DeleteAll,
}

#[derive(Clone, Debug)]
enum Int64BatchMutationOp {
    BatchUpsert { value_1: i64, value_2: i64 },
    Delete { key: i64 },
    DeleteAll,
}

#[derive(Clone, Debug)]
enum CompositeMutationOp {
    Upsert {
        region: &'static str,
        id: i64,
        value: i64,
    },
    Delete {
        region: &'static str,
        id: i64,
    },
    DeleteAll,
}

fn create_int64_upsert_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_composite_upsert_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_int64_upsert_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_int64_upsert_schema();
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

async fn setup_composite_upsert_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext, Arc<Schema>)> {
    let schema = create_composite_upsert_schema();
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "region".to_string(),
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    Ok((table, ctx, schema))
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<u64> {
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

async fn read_int64_rows(ctx: &SessionContext, table_name: &str) -> TestResult<Vec<(i64, i64)>> {
    let df = ctx
        .sql(&format!("SELECT id, value FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;

    let mut rows = Vec::new();
    for batch in &results {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");

        for idx in 0..batch.num_rows() {
            rows.push((ids.value(idx), values.value(idx)));
        }
    }

    Ok(rows)
}

async fn read_composite_rows(
    ctx: &SessionContext,
    table_name: &str,
) -> TestResult<Vec<(String, i64, i64)>> {
    let df = ctx
        .sql(&format!(
            "SELECT region, id, value FROM {table_name} ORDER BY region, id"
        ))
        .await?;
    let results = df.collect().await?;

    let mut rows = Vec::new();
    for batch in &results {
        let regions = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column should be Utf8");
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");

        for idx in 0..batch.num_rows() {
            rows.push((
                regions.value(idx).to_string(),
                ids.value(idx),
                values.value(idx),
            ));
        }
    }

    Ok(rows)
}

async fn reopen_and_read_int64_rows(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<Vec<(i64, i64)>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .open(table_name)
        .await?;
    ctx.register_table(table_name, Arc::new(provider) as Arc<dyn TableProvider>)?;
    read_int64_rows(&ctx, table_name).await
}

async fn reopen_and_read_composite_rows(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<Vec<(String, i64, i64)>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .open(table_name)
        .await?;
    ctx.register_table(table_name, Arc::new(provider) as Arc<dyn TableProvider>)?;
    read_composite_rows(&ctx, table_name).await
}

fn expected_int64_rows(model: &Int64Model) -> Vec<(i64, i64)> {
    model.iter().map(|(&key, &value)| (key, value)).collect()
}

fn expected_composite_rows(model: &CompositeModel) -> Vec<(String, i64, i64)> {
    model
        .iter()
        .map(|((region, id), value)| (region.clone(), *id, *value))
        .collect()
}

fn enumerate_sequences<T: Clone>(ops: &[T], depth: usize) -> Vec<Vec<T>> {
    if depth == 0 {
        return vec![Vec::new()];
    }

    let shorter = enumerate_sequences(ops, depth - 1);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "depth is always small in tests"
    )]
    let mut sequences = Vec::with_capacity(ops.len().pow(depth as u32));
    for prefix in shorter {
        for op in ops {
            let mut sequence = prefix.clone();
            sequence.push(op.clone());
            sequences.push(sequence);
        }
    }
    sequences
}

fn int64_single_row_ops() -> Vec<Int64MutationOp> {
    vec![
        Int64MutationOp::Upsert { key: 1, value: 10 },
        Int64MutationOp::Upsert { key: 1, value: 20 },
        Int64MutationOp::Upsert { key: 2, value: 10 },
        Int64MutationOp::Upsert { key: 2, value: 20 },
        Int64MutationOp::Delete { key: 1 },
        Int64MutationOp::Delete { key: 2 },
        Int64MutationOp::DeleteAll,
    ]
}

fn int64_batch_ops() -> Vec<Int64BatchMutationOp> {
    vec![
        Int64BatchMutationOp::BatchUpsert {
            value_1: 10,
            value_2: 10,
        },
        Int64BatchMutationOp::BatchUpsert {
            value_1: 10,
            value_2: 20,
        },
        Int64BatchMutationOp::BatchUpsert {
            value_1: 20,
            value_2: 10,
        },
        Int64BatchMutationOp::BatchUpsert {
            value_1: 20,
            value_2: 20,
        },
        Int64BatchMutationOp::Delete { key: 1 },
        Int64BatchMutationOp::Delete { key: 2 },
        Int64BatchMutationOp::DeleteAll,
    ]
}

fn composite_single_row_ops() -> Vec<CompositeMutationOp> {
    vec![
        CompositeMutationOp::Upsert {
            region: "east",
            id: 1,
            value: 10,
        },
        CompositeMutationOp::Upsert {
            region: "east",
            id: 1,
            value: 20,
        },
        CompositeMutationOp::Upsert {
            region: "west",
            id: 1,
            value: 10,
        },
        CompositeMutationOp::Upsert {
            region: "west",
            id: 1,
            value: 20,
        },
        CompositeMutationOp::Delete {
            region: "east",
            id: 1,
        },
        CompositeMutationOp::Delete {
            region: "west",
            id: 1,
        },
        CompositeMutationOp::DeleteAll,
    ]
}

async fn apply_int64_mutation(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    op: &Int64MutationOp,
) -> TestResult<()> {
    match op {
        Int64MutationOp::Upsert { key, value } => {
            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(vec![*key])),
                    Arc::new(Int64Array::from(vec![*value])),
                ],
            )?;
            let _ = insert_batch(table, batch).await?;
        }
        Int64MutationOp::Delete { key } => {
            let _ = delete_records(table, col("id").eq(lit(*key))).await?;
        }
        Int64MutationOp::DeleteAll => {
            let _ = delete_records(table, lit(true)).await?;
        }
    }

    Ok(())
}

async fn apply_int64_batch_mutation(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    op: &Int64BatchMutationOp,
) -> TestResult<()> {
    match op {
        Int64BatchMutationOp::BatchUpsert { value_1, value_2 } => {
            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
                    Arc::new(Int64Array::from(vec![*value_1, *value_2])),
                ],
            )?;
            let _ = insert_batch(table, batch).await?;
        }
        Int64BatchMutationOp::Delete { key } => {
            let _ = delete_records(table, col("id").eq(lit(*key))).await?;
        }
        Int64BatchMutationOp::DeleteAll => {
            let _ = delete_records(table, lit(true)).await?;
        }
    }

    Ok(())
}

async fn apply_composite_mutation(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    op: &CompositeMutationOp,
) -> TestResult<()> {
    match op {
        CompositeMutationOp::Upsert { region, id, value } => {
            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(StringArray::from(vec![*region])),
                    Arc::new(Int64Array::from(vec![*id])),
                    Arc::new(Int64Array::from(vec![*value])),
                ],
            )?;
            let _ = insert_batch(table, batch).await?;
        }
        CompositeMutationOp::Delete { region, id } => {
            let _ = delete_records(
                table,
                col("region").eq(lit(*region)).and(col("id").eq(lit(*id))),
            )
            .await?;
        }
        CompositeMutationOp::DeleteAll => {
            let _ = delete_records(table, lit(true)).await?;
        }
    }

    Ok(())
}

fn apply_int64_model(model: &mut Int64Model, op: &Int64MutationOp) {
    match op {
        Int64MutationOp::Upsert { key, value } => {
            model.insert(*key, *value);
        }
        Int64MutationOp::Delete { key } => {
            model.remove(key);
        }
        Int64MutationOp::DeleteAll => {
            model.clear();
        }
    }
}

fn apply_int64_batch_model(model: &mut Int64Model, op: &Int64BatchMutationOp) {
    match op {
        Int64BatchMutationOp::BatchUpsert { value_1, value_2 } => {
            model.insert(1, *value_1);
            model.insert(2, *value_2);
        }
        Int64BatchMutationOp::Delete { key } => {
            model.remove(key);
        }
        Int64BatchMutationOp::DeleteAll => {
            model.clear();
        }
    }
}

fn apply_composite_model(model: &mut CompositeModel, op: &CompositeMutationOp) {
    match op {
        CompositeMutationOp::Upsert { region, id, value } => {
            model.insert(((*region).to_string(), *id), *value);
        }
        CompositeMutationOp::Delete { region, id } => {
            model.remove(&(((*region).to_string()), *id));
        }
        CompositeMutationOp::DeleteAll => {
            model.clear();
        }
    }
}

async fn assert_int64_state(
    fixture: &TestFixture,
    ctx: &SessionContext,
    table_name: &str,
    expected: &Int64Model,
    sequence_index: usize,
    step_index: usize,
) -> TestResult<()> {
    let expected_rows = expected_int64_rows(expected);
    let live_rows = read_int64_rows(ctx, table_name).await?;
    assert_eq!(
        live_rows, expected_rows,
        "live state mismatch for {table_name} at sequence {sequence_index}, step {step_index}"
    );

    let reopened_rows = reopen_and_read_int64_rows(fixture, table_name).await?;
    assert_eq!(
        reopened_rows, expected_rows,
        "reopened state mismatch for {table_name} at sequence {sequence_index}, step {step_index}"
    );

    Ok(())
}

async fn assert_composite_state(
    fixture: &TestFixture,
    ctx: &SessionContext,
    table_name: &str,
    expected: &CompositeModel,
    sequence_index: usize,
    step_index: usize,
) -> TestResult<()> {
    let expected_rows = expected_composite_rows(expected);
    let live_rows = read_composite_rows(ctx, table_name).await?;
    assert_eq!(
        live_rows, expected_rows,
        "live state mismatch for {table_name} at sequence {sequence_index}, step {step_index}"
    );

    let reopened_rows = reopen_and_read_composite_rows(fixture, table_name).await?;
    assert_eq!(
        reopened_rows, expected_rows,
        "reopened state mismatch for {table_name} at sequence {sequence_index}, step {step_index}"
    );

    Ok(())
}

async fn test_exhaustive_int64_single_row_sequences_impl(fixture: TestFixture) -> TestResult<()> {
    let sequences = enumerate_sequences(&int64_single_row_ops(), 3);

    for (sequence_index, sequence) in sequences.iter().enumerate() {
        let table_name = format!("int64_single_model_{sequence_index}");
        let (table, ctx, schema) = setup_int64_upsert_table(&fixture, &table_name).await?;
        let mut expected = Int64Model::new();

        for (step_index, op) in sequence.iter().enumerate() {
            apply_int64_mutation(&table, &schema, op).await?;
            apply_int64_model(&mut expected, op);
            assert_int64_state(
                &fixture,
                &ctx,
                &table_name,
                &expected,
                sequence_index,
                step_index,
            )
            .await?;
        }
    }

    Ok(())
}

test_with_backends!(test_exhaustive_int64_single_row_sequences_impl);

async fn test_exhaustive_int64_batch_sequences_impl(fixture: TestFixture) -> TestResult<()> {
    let sequences = enumerate_sequences(&int64_batch_ops(), 3);

    for (sequence_index, sequence) in sequences.iter().enumerate() {
        let table_name = format!("int64_batch_model_{sequence_index}");
        let (table, ctx, schema) = setup_int64_upsert_table(&fixture, &table_name).await?;
        let mut expected = Int64Model::new();

        for (step_index, op) in sequence.iter().enumerate() {
            apply_int64_batch_mutation(&table, &schema, op).await?;
            apply_int64_batch_model(&mut expected, op);
            assert_int64_state(
                &fixture,
                &ctx,
                &table_name,
                &expected,
                sequence_index,
                step_index,
            )
            .await?;
        }
    }

    Ok(())
}

test_with_backends!(test_exhaustive_int64_batch_sequences_impl);

async fn test_exhaustive_composite_single_row_sequences_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let sequences = enumerate_sequences(&composite_single_row_ops(), 3);

    for (sequence_index, sequence) in sequences.iter().enumerate() {
        let table_name = format!("composite_model_{sequence_index}");
        let (table, ctx, schema) = setup_composite_upsert_table(&fixture, &table_name).await?;
        let mut expected = CompositeModel::new();

        for (step_index, op) in sequence.iter().enumerate() {
            apply_composite_mutation(&table, &schema, op).await?;
            apply_composite_model(&mut expected, op);
            assert_composite_state(
                &fixture,
                &ctx,
                &table_name,
                &expected,
                sequence_index,
                step_index,
            )
            .await?;
        }
    }

    Ok(())
}

test_with_backends!(test_exhaustive_composite_single_row_sequences_impl);
