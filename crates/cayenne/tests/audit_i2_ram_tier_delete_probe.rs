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

#![allow(clippy::expect_used)]

//! Audit probe (cayenne-perf audit 2026-09-01, finding I-2): a predicate
//! `DELETE` on a `cdc_durability: memory` upsert table whose newest row
//! versions live only in the RAM tier (no checkpoint yet) must delete exactly
//! the rows whose *live* version matches the predicate — SQL semantics.
//!
//! Scenario A: CDC `(7,10),(8,5)` → checkpoint (durable); CDC `(7,60),(9,20)`
//! with no checkpoint (RAM only); `DELETE WHERE value < 50` must leave exactly
//! `[(7,60)]`: `(8,5)` matches (durable), `(9,20)` matches (RAM only), and the
//! live `(7,60)` does not match even though its superseded durable version
//! `(7,10)` does.
//!
//! Scenario B: CDC `(8,5)` → checkpoint; CDC `(9,20)` with no checkpoint;
//! `DELETE WHERE value = 20` must leave exactly `[(8,5)]`.
//!
//! Every observed row set is printed (`--nocapture`) so the run is evidence
//! whichever way the assertions go.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog, SlotAdvancer};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(i2_predicate_delete_spanning_durable_and_ram_rows_impl);
test_with_backends!(i2_predicate_delete_ram_only_row_impl);

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

fn id_value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// A PK/upsert table on the RAM CDC tier (`cdc_durability: memory`, key
/// deletes, background compaction effectively off) with a no-op slot advancer
/// so the memory path is armed exactly as the accelerator arms it.
async fn make_memory_cdc_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: &Arc<Schema>,
) -> TestResult<(SessionContext, Arc<CayenneTableProvider>)> {
    let table_options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((ctx, table))
}

/// CDC-apply `rows` through the in-memory tier (`write_cdc_append_stream`).
async fn cdc_upsert(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    rows: &[(i64, i64)],
) -> TestResult<()> {
    let ids: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(batch_to_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

async fn collect_pairs(ctx: &SessionContext, sql: &str) -> TestResult<Vec<(i64, i64)>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column Int64");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

async fn delete_where(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    predicate: Expr,
) -> TestResult<u64> {
    let plan = table.delete_from(&ctx.state(), vec![predicate]).await?;
    let results = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
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

/// Number of files under the table's data directory — a durable write adds
/// files, a RAM-tier write does not, so an unchanged count is the precondition
/// that the second CDC write really stayed in RAM.
fn count_files(dir: &std::path::Path) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                n += 1;
            }
        }
    }
    n
}

async fn i2_predicate_delete_spanning_durable_and_ram_rows_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = id_value_schema();
    let (ctx, table) = make_memory_cdc_table(&fixture, "i2_a", &schema).await?;
    let sql = "SELECT id, value FROM i2_a ORDER BY id";

    cdc_upsert(&table, &schema, &[(7, 10), (8, 5)]).await?;
    let durable_epoch = table.checkpoint_mem_tier().await?;
    let files_after_checkpoint = count_files(&fixture.data_path);
    let rows = collect_pairs(&ctx, sql).await?;
    eprintln!(
        "[i2-A] durable CDC (7,10),(8,5) + checkpoint (epoch {durable_epoch}, {files_after_checkpoint} data files): {rows:?}"
    );
    assert_eq!(rows, vec![(7, 10), (8, 5)], "durable baseline");

    cdc_upsert(&table, &schema, &[(7, 60), (9, 20)]).await?;
    let files_after_ram_write = count_files(&fixture.data_path);
    let rows = collect_pairs(&ctx, sql).await?;
    eprintln!(
        "[i2-A] RAM-only CDC (7,60),(9,20), no checkpoint ({files_after_ram_write} data files; RAM-resident precondition = {}): {rows:?}",
        files_after_ram_write == files_after_checkpoint
    );
    assert_eq!(
        rows,
        vec![(7, 60), (8, 5), (9, 20)],
        "RAM rows must be visible before the delete"
    );

    let deleted = delete_where(&table, &ctx, col("value").lt(lit(50i64))).await?;
    let after_delete = collect_pairs(&ctx, sql).await?;
    eprintln!(
        "[i2-A] DELETE WHERE value < 50 reported {deleted} row(s); visible now: {after_delete:?} — expected [(7, 60)]"
    );

    let _ = table.checkpoint_mem_tier().await?;
    let after_checkpoint = collect_pairs(&ctx, sql).await?;
    eprintln!("[i2-A] after the next checkpoint: {after_checkpoint:?} — expected [(7, 60)]");

    assert_eq!(
        after_delete,
        vec![(7, 60)],
        "I-2: rows visible right after a predicate DELETE spanning durable and RAM versions"
    );
    assert_eq!(
        after_checkpoint,
        vec![(7, 60)],
        "I-2: rows visible after the following checkpoint"
    );
    Ok(())
}

async fn i2_predicate_delete_ram_only_row_impl(fixture: common::TestFixture) -> TestResult<()> {
    let schema = id_value_schema();
    let (ctx, table) = make_memory_cdc_table(&fixture, "i2_b", &schema).await?;
    let sql = "SELECT id, value FROM i2_b ORDER BY id";

    cdc_upsert(&table, &schema, &[(8, 5)]).await?;
    let _ = table.checkpoint_mem_tier().await?;
    let files_after_checkpoint = count_files(&fixture.data_path);

    cdc_upsert(&table, &schema, &[(9, 20)]).await?;
    let files_after_ram_write = count_files(&fixture.data_path);
    let rows = collect_pairs(&ctx, sql).await?;
    eprintln!(
        "[i2-B] durable (8,5) + checkpoint, then RAM-only (9,20) (RAM-resident precondition = {}): {rows:?}",
        files_after_ram_write == files_after_checkpoint
    );
    assert_eq!(
        rows,
        vec![(8, 5), (9, 20)],
        "both rows visible before the delete"
    );

    let deleted = delete_where(&table, &ctx, col("value").eq(lit(20i64))).await?;
    let after_delete = collect_pairs(&ctx, sql).await?;
    eprintln!(
        "[i2-B] DELETE WHERE value = 20 reported {deleted} row(s); visible now: {after_delete:?} — expected [(8, 5)]"
    );

    let _ = table.checkpoint_mem_tier().await?;
    let after_checkpoint = collect_pairs(&ctx, sql).await?;
    eprintln!("[i2-B] after the next checkpoint: {after_checkpoint:?} — expected [(8, 5)]");

    assert_eq!(
        after_delete,
        vec![(8, 5)],
        "I-2: a RAM-only row matching the predicate must be deleted"
    );
    assert_eq!(
        after_checkpoint,
        vec![(8, 5)],
        "I-2: and stay deleted across the next checkpoint"
    );
    Ok(())
}
