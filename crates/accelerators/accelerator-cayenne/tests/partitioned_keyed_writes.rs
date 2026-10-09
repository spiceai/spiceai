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

//! Writes to a partitioned Cayenne acceleration with a primary key keep one row
//! per key, whether the key arrives again in an `INSERT` or a CDC update, and
//! the table reopens holding the same rows (#14947). Each table is created
//! through `AcceleratorEngineRegistry::create_accelerator_table`, the runtime's
//! own path, so it carries the constraints and `on_conflict` a dataset gets, and
//! every write goes through the cross-partition coordinator.

use std::{path::Path, sync::Arc};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Array, Int64Array, ListArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use data_components::cdc;
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use datafusion_table_providers::util::column_reference::ColumnReference;
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode, RefreshMode},
    change_sink::{ChangeBatch, ChangeSink, ChangeSinkContext, Recovery, WriteOptions},
    testing::TestAccelerationSource,
};
use tokio::runtime::Handle;

type Row = (i64, i64, i64);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// A file-mode Cayenne acceleration of `t (id, w, v)` partitioned on `w`, keyed
/// on `id` when `keyed`, as the runtime creates it for a `refresh_mode: changes`
/// dataset.
async fn partitioned_table(
    ctx: &SessionContext,
    dir: &Path,
    keyed: bool,
) -> Arc<dyn TableProvider> {
    let source = TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: keyed.then(|| ColumnReference::new(vec!["id".to_string()])),
        partition_by: vec![spicepod::partitioning::PartitionedBy {
            name: "w".to_string(),
            expression: "w".to_string(),
        }],
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join("data").to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(Engine::Cayenne, Arc::new(CayenneAccelerator::new()))
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            schema(),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(ctx.clone()),
        )
        .await
        .expect("partitioned table")
}

/// Every row of `t`, ordered.
async fn rows(ctx: &SessionContext) -> Vec<Row> {
    let batches = ctx
        .sql("SELECT id, w, v FROM t ORDER BY id, w, v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("rows");
    let column = |batch: &RecordBatch, index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column")
            .clone()
    };
    batches
        .iter()
        .flat_map(|batch| {
            let (id, w, v) = (column(batch, 0), column(batch, 1), column(batch, 2));
            (0..batch.num_rows())
                .map(|row| (id.value(row), w.value(row), v.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn sql(ctx: &SessionContext, statement: &str) {
    ctx.sql(statement)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("statement");
}

/// Reopen `t` from its data directory, as a restart does.
async fn reopen(ctx: &SessionContext, dir: &Path, keyed: bool, table: Arc<dyn TableProvider>) {
    drop(table);
    ctx.deregister_table("t").expect("deregister");
    let reopened = partitioned_table(ctx, dir, keyed).await;
    ctx.register_table("t", reopened)
        .expect("register reopened");
}

/// A CDC change batch: `op` (`u` or `d`) for each row.
fn changes(schema: &SchemaRef, op: &str, rows: &[Row]) -> cdc::ChangeBatch {
    let data = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .expect("source rows");
    let count = i32::try_from(rows.len()).expect("row count");
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new((0..=count).collect::<Vec<_>>().into()),
        Arc::new(StringArray::from(vec!["id"; rows.len()])),
        None,
    )
    .expect("source primary keys");
    let record = RecordBatch::try_new(
        Arc::new(cdc::changes_schema(schema)),
        vec![
            Arc::new(StringArray::from(vec![op; rows.len()])),
            Arc::new(keys),
            Arc::new(StructArray::from(data)),
        ],
    )
    .expect("source changes");
    cdc::ChangeBatch::try_new(record).expect("CDC batch")
}

async fn apply(sink: &ChangeSink, batch: cdc::ChangeBatch) {
    let receipt = sink
        .reserve()
        .await
        .expect("capacity")
        .submit(
            ChangeBatch::cdc(batch),
            WriteOptions {
                recovery: Recovery::Replayable,
                ..WriteOptions::default()
            },
        )
        .expect("admission")
        .wait()
        .await
        .expect("receipt");
    receipt.published().await.expect("published");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_insert_of_a_stored_key_replaces_its_row() {
    let dir = tempfile::tempdir().expect("dir");
    let ctx = SessionContext::new();
    let table = partitioned_table(&ctx, dir.path(), true).await;
    ctx.register_table("t", Arc::clone(&table))
        .expect("register");

    sql(&ctx, "INSERT INTO t VALUES (1, 1, 0), (2, 0, 0), (3, 1, 0)").await;
    sql(&ctx, "INSERT INTO t VALUES (1, 1, 1)").await;
    assert_eq!(rows(&ctx).await, [(1, 1, 1), (2, 0, 0), (3, 1, 0)]);
    sql(&ctx, "INSERT INTO t VALUES (1, 1, 2), (2, 0, 2)").await;
    assert_eq!(rows(&ctx).await, [(1, 1, 2), (2, 0, 2), (3, 1, 0)]);

    reopen(&ctx, dir.path(), true, table).await;
    assert_eq!(rows(&ctx).await, [(1, 1, 2), (2, 0, 2), (3, 1, 0)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_updates_and_deletes_keep_one_row_per_key() {
    let dir = tempfile::tempdir().expect("dir");
    let ctx = SessionContext::new();
    let table = partitioned_table(&ctx, dir.path(), true).await;
    ctx.register_table("t", Arc::clone(&table))
        .expect("register");
    let sink = CayenneAccelerator::new()
        .change_sink(
            ChangeSinkContext::new(TableReference::bare("t"), Arc::clone(&table)),
            &Handle::current(),
            2,
        )
        .await
        .expect("bind")
        .expect("sink");
    let schema = schema();

    let initial: Vec<Row> = (1..=8).map(|id| (id, id % 2, 0)).collect();
    apply(&sink, changes(&schema, "u", &initial)).await;
    apply(&sink, changes(&schema, "u", &[(1, 1, 1), (2, 0, 1)])).await;
    apply(&sink, changes(&schema, "u", &[(1, 1, 2), (4, 0, 2)])).await;
    apply(&sink, changes(&schema, "d", &[(3, 1, 0), (6, 0, 0)])).await;
    // A deleted key that comes back is one row again.
    apply(&sink, changes(&schema, "u", &[(3, 1, 9)])).await;

    let expected = [
        (1, 1, 2),
        (2, 0, 1),
        (3, 1, 9),
        (4, 0, 2),
        (5, 1, 0),
        (7, 1, 0),
        (8, 0, 0),
    ];
    assert_eq!(rows(&ctx).await, expected);
    sink.begin_close().wait().await.expect("close");

    reopen(&ctx, dir.path(), true, table).await;
    assert_eq!(rows(&ctx).await, expected);
}

/// Each round updates every key, so each partition publishes one more
/// protected snapshot per round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_updates_of_every_key_keep_one_row_per_key() {
    let dir = tempfile::tempdir().expect("dir");
    let ctx = SessionContext::new();
    let table = partitioned_table(&ctx, dir.path(), true).await;
    ctx.register_table("t", Arc::clone(&table))
        .expect("register");
    let sink = CayenneAccelerator::new()
        .change_sink(
            ChangeSinkContext::new(TableReference::bare("t"), Arc::clone(&table)),
            &Handle::current(),
            2,
        )
        .await
        .expect("bind")
        .expect("sink");
    let schema = schema();

    let round = |v: i64| (1..=40).map(|id| (id, id % 4, v)).collect::<Vec<Row>>();
    for v in 0..12 {
        apply(&sink, changes(&schema, "u", &round(v))).await;
    }
    assert_eq!(rows(&ctx).await, round(11));
    sink.begin_close().wait().await.expect("close");

    reopen(&ctx, dir.path(), true, table).await;
    assert_eq!(rows(&ctx).await, round(11));
}

/// Without a primary key, a partitioned table appends: a repeated row is
/// stored again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unkeyed_partitioned_table_keeps_every_row() {
    let dir = tempfile::tempdir().expect("dir");
    let ctx = SessionContext::new();
    let table = partitioned_table(&ctx, dir.path(), false).await;
    ctx.register_table("t", Arc::clone(&table))
        .expect("register");

    sql(&ctx, "INSERT INTO t VALUES (1, 1, 0), (2, 0, 0)").await;
    sql(&ctx, "INSERT INTO t VALUES (1, 1, 0), (3, 1, 5)").await;
    let expected = [(1, 1, 0), (1, 1, 0), (2, 0, 0), (3, 1, 5)];
    assert_eq!(rows(&ctx).await, expected);

    reopen(&ctx, dir.path(), false, table).await;
    assert_eq!(rows(&ctx).await, expected);
}
