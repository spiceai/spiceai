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

//! Reproducers for an overwrite/delete/re-upsert resurrection bug: after
//! `INSERT OVERWRITE` creates a row, deleting it and then re-upserting the same
//! key loses the re-upsert — the row stays hidden, as if the delete tombstone
//! still applied. No compaction is involved.
//!
//! Root cause (key-delete path): the upsert's on-conflict insert-record is only
//! recorded for keys present in the visible PK existence index. `INSERT
//! OVERWRITE` clears that index, so the re-inserted key (which now carries only
//! a pending delete tombstone) takes the plain-insert path and records no
//! insert-record, leaving it hidden (a row is visible iff `insert_seq >
//! delete_seq`). The fix records an insert-record for any re-inserted key that
//! has a pending tombstone.
//!
//! Run per deletion mode so both the key-index and position paths are covered.

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{col, lit};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Explicit `deletion_mode: key` — the deletion index is authoritative.
    Key,
    /// Default (`auto` resolves to position even for PK tables).
    Position,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn config(mode: Mode) -> VortexConfig {
    // `inline_max_rows: 0` so every write lands as a snapshot file (matches the
    // shape that surfaced the bug).
    let base = VortexConfig {
        inline_max_rows: 0,
        ..VortexConfig::default()
    };
    match mode {
        Mode::Key => VortexConfig {
            deletion_mode: DeletionMode::Key,
            ..base
        },
        Mode::Position => base,
    }
}

async fn create_table(
    fixture: &TestFixture,
    name: &str,
    mode: Mode,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let opts = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: config(mode),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, opts, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("valid batch")
}

async fn overwrite(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) -> TestResult<()> {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch(rows)]], schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn upsert(table: &Arc<CayenneTableProvider>, key: i64, value: i64) -> TestResult<()> {
    common::insert_batch(table.as_ref(), batch(&[(key, value)])).await?;
    Ok(())
}

async fn delete_key(table: &Arc<CayenneTableProvider>, key: i64) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(key))])
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn read_value(ctx: &SessionContext, name: &str, key: i64) -> TestResult<Option<i64>> {
    let df = ctx
        .sql(&format!("SELECT value FROM {name} WHERE id = {key}"))
        .await?;
    let results = df.collect().await?;
    for b in &results {
        if b.num_rows() > 0 {
            let v = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64");
            return Ok(Some(v.value(0)));
        }
    }
    Ok(None)
}

/// Row created via OVERWRITE, deleted, then re-upserted. Expected: present.
async fn reupsert_after_deleting_overwritten_row(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "ov_single", mode).await?;
    overwrite(&table, &[(1, 100)]).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 200).await?;
    assert_eq!(
        read_value(&ctx, "ov_single", 1).await?,
        Some(200),
        "{mode:?}: re-upsert after deleting an OVERWRITE-created row must resurrect it"
    );
    Ok(())
}

/// Same, with a second untouched key, to confirm only the re-upserted key is affected.
async fn reupsert_after_deleting_overwritten_row_multi_key(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "ov_multi", mode).await?;
    overwrite(&table, &[(1, 100), (2, 200)]).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 999).await?;
    assert_eq!(
        read_value(&ctx, "ov_multi", 1).await?,
        Some(999),
        "{mode:?}: key 1 re-upsert lost"
    );
    assert_eq!(
        read_value(&ctx, "ov_multi", 2).await?,
        Some(200),
        "{mode:?}: key 2 must remain"
    );
    Ok(())
}

/// Control: row created via UPSERT (not overwrite), deleted, re-upserted. Always
/// worked; included so a regression there would also be caught.
async fn reupsert_after_deleting_upserted_row_control(
    fixture: &TestFixture,
    mode: Mode,
) -> TestResult<()> {
    let (table, ctx) = create_table(fixture, "up_single", mode).await?;
    upsert(&table, 1, 100).await?;
    delete_key(&table, 1).await?;
    upsert(&table, 1, 200).await?;
    assert_eq!(
        read_value(&ctx, "up_single", 1).await?,
        Some(200),
        "{mode:?}: control re-upsert after deleting an UPSERT-created row must resurrect it"
    );
    Ok(())
}

async fn run_all(fixture: TestFixture, mode: Mode) -> TestResult<()> {
    reupsert_after_deleting_overwritten_row(&fixture, mode).await?;
    reupsert_after_deleting_overwritten_row_multi_key(&fixture, mode).await?;
    reupsert_after_deleting_upserted_row_control(&fixture, mode).await?;
    Ok(())
}

async fn overwrite_resurrection_key_impl(fixture: TestFixture) -> TestResult<()> {
    run_all(fixture, Mode::Key).await
}
async fn overwrite_resurrection_position_impl(fixture: TestFixture) -> TestResult<()> {
    run_all(fixture, Mode::Position).await
}

test_with_backends!(overwrite_resurrection_key_impl);
test_with_backends!(overwrite_resurrection_position_impl);
