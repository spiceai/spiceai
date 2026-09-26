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

//! A CDC upsert that falls back to the durable path because the global
//! in-memory CDC tier budget stays full must still supersede the keys' earlier
//! mem-tier versions on a `cdc_durability: memory` upsert table.
//!
//! The fallback first spills this table's mem tier to files, then writes the
//! batch durably with the conflict deletions computed while the keys were
//! recorded as mem-tier (inline) rows. The upsert must not leave the spilled
//! copies visible.
//!
//! Its own test binary: the budget is process-global.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog, SlotAdvancer, set_global_mem_tier_bytes};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(budget_fallback_upsert_supersedes_mem_tier_rows_impl);

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

const KEYS: i64 = 100;

fn batch(schema: &Arc<Schema>, value: i64) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(
                (1..=KEYS)
                    .map(|k| format!("key-{k:04}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![
                value;
                usize::try_from(KEYS).expect(
                    "positive key count fits usize"
                )
            ])),
        ],
    )?)
}

fn stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

async fn cdc_upsert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<()> {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// `(rows served, distinct ids, rows whose value is not `want`)`.
async fn served(ctx: &SessionContext, want: i64) -> TestResult<(i64, i64, i64)> {
    let sql = format!(
        "SELECT COUNT(*), COUNT(DISTINCT id), COALESCE(SUM(CASE WHEN value <> {want} THEN 1 ELSE 0 END), 0) FROM t"
    );
    let batches = ctx.sql(&sql).await?.collect().await?;
    let col = |i: usize| {
        batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 aggregate")
            .value(0)
    };
    Ok((col(0), col(1), col(2)))
}

async fn budget_fallback_upsert_supersedes_mem_tier_rows_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: "t".to_string(),
        schema: Arc::clone(&schema),
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
    assert!(
        table.is_cdc_memory_mode(),
        "the in-memory CDC tier is armed"
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Room for the first batch: it lands in the mem tier.
    set_global_mem_tier_bytes(64 << 20);
    cdc_upsert(&table, batch(&schema, 1)?).await?;
    println!("after the mem-tier upsert: {:?}", served(&ctx, 1).await?);

    // No room for the second: it waits, spills this table's tier, and falls
    // back to the durable path.
    set_global_mem_tier_bytes(1);
    cdc_upsert(&table, batch(&schema, 2)?).await?;
    let got = served(&ctx, 2).await?;
    // The fallback spills the tier before its durable write, so a checkpoint
    // now finds nothing left in RAM.
    let left_in_ram = table.checkpoint_mem_tier().await?;
    println!(
        "after the budget-fallback upsert (rows, distinct ids, stale values) = {got:?}; {left_in_ram} rows were still in the mem tier"
    );
    set_global_mem_tier_bytes(0);
    assert_eq!(left_in_ram, 0, "the fallback must drain the mem tier");
    assert_eq!(
        got,
        (KEYS, KEYS, 0),
        "every key served once with the fallback upsert's value"
    );
    Ok(())
}
