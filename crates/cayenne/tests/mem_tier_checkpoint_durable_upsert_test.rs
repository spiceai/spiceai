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

//! A durable upsert of keys whose rows reached files through a mem-tier
//! checkpoint must supersede those rows on a `cdc_durability: memory` upsert
//! table.
//!
//! The PK keyset can record a mem-tier key's location as inline even after a
//! checkpoint moves its row into a file. Conflict resolution must cover both
//! locations, while counting each superseded row once.
//!
//! - Scenario A: CDC upsert (mem tier), checkpoint, durable `INSERT` upsert of
//!   the same keys. Every key must be served once, with the new value.
//! - Scenario B (control): the same writes without the checkpoint.
//!
//! These exercise the provider's direct durable `INSERT` path; the CDC budget
//! fallback is covered by `mem_tier_budget_fallback_upsert_test`.
//!
//! Every observed row set is printed (`--nocapture`), so a run documents what
//! the table served whichever way the assertions go.

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

test_with_backends!(durable_upsert_after_mem_tier_checkpoint_supersedes_impl);
test_with_backends!(durable_upsert_without_checkpoint_supersedes_impl);

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

const KEYS: i64 = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(schema: &Arc<Schema>, value: i64) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from((1..=KEYS).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(vec![
                value;
                usize::try_from(KEYS).expect(
                    "positive key count fits usize"
                )
            ])),
        ],
    )?)
}

async fn make_table(
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
    assert!(
        table.is_cdc_memory_mode(),
        "the in-memory CDC tier is armed"
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((ctx, table))
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
async fn served(ctx: &SessionContext, table: &str, want: i64) -> TestResult<(i64, i64, i64)> {
    let sql = format!(
        "SELECT COUNT(*), COUNT(DISTINCT id), COALESCE(SUM(CASE WHEN value <> {want} THEN 1 ELSE 0 END), 0) FROM {table}"
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

async fn durable_upsert_after_mem_tier_checkpoint_supersedes_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = schema();
    let (ctx, table) = make_table(&fixture, "ckpt_then_durable", &schema).await?;
    cdc_upsert(&table, batch(&schema, 1)?).await?;
    let flushed = table.checkpoint_mem_tier().await?;
    println!("scenario A: checkpoint flushed {flushed} rows");
    assert_eq!(
        flushed,
        u64::try_from(KEYS).expect("positive key count fits u64"),
        "the checkpoint must move every key to files"
    );
    println!(
        "scenario A: after checkpoint {:?}",
        served(&ctx, "ckpt_then_durable", 1).await?
    );
    common::insert_batch(table.as_ref(), batch(&schema, 2)?).await?;
    let got = served(&ctx, "ckpt_then_durable", 2).await?;
    println!("scenario A: after durable upsert (rows, distinct ids, stale values) = {got:?}");
    assert_eq!(
        got,
        (KEYS, KEYS, 0),
        "every key served once with the durable upsert's value"
    );
    Ok(())
}

async fn durable_upsert_without_checkpoint_supersedes_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = schema();
    let (ctx, table) = make_table(&fixture, "no_ckpt_durable", &schema).await?;
    cdc_upsert(&table, batch(&schema, 1)?).await?;
    common::insert_batch(table.as_ref(), batch(&schema, 2)?).await?;
    let got = served(&ctx, "no_ckpt_durable", 2).await?;
    println!("scenario B: after durable upsert (rows, distinct ids, stale values) = {got:?}");
    assert_eq!(
        got,
        (KEYS, KEYS, 0),
        "every key served once with the durable upsert's value"
    );
    Ok(())
}
