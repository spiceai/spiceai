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
//! batch durably. The spill moves the rows the batch's conflicts were first
//! resolved against, so the upsert must not leave the spilled copies visible.
//! Covers both PK encodings (`Int64` and row-converted `Utf8`), and keys whose
//! earlier version a durable write inlined into the metastore while other keys
//! sat in the mem tier: that spill flushes both.
//!
//! Its own test binary with a single test: the budget is process-global, so
//! every backend and case runs in sequence rather than on parallel test
//! threads.

use crate::common;

use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog, SlotAdvancer, set_global_mem_tier_bytes};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// Not `test_with_backends!`: its per-backend tests would run concurrently and
// race on the process-global budget.
#[test]
fn budget_fallback_upsert_supersedes_mem_tier_rows() -> Result<(), String> {
    common::run_with_backend_blocking(
        common::BackendType::Sqlite,
        budget_fallback_upsert_supersedes_mem_tier_rows_impl,
    )?;
    #[cfg(feature = "turso")]
    common::run_with_backend_blocking(
        common::BackendType::Turso,
        budget_fallback_upsert_supersedes_mem_tier_rows_impl,
    )?;
    Ok(())
}

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

const KEYS: i64 = 100;

/// `(rows served, distinct ids, rows whose value is stale, rows left in RAM)`.
type Observed = (i64, i64, i64, u64);

/// Where the superseded versions live when the fallback upsert arrives.
#[derive(Clone, Copy, Debug)]
enum Prior {
    /// Every upserted key in the mem tier.
    MemTier,
    /// The upserted keys inlined into the metastore by a durable write, and
    /// other keys in the mem tier.
    InlineAndMemTier,
}

fn ids(id_type: &DataType, keys: RangeInclusive<i64>) -> ArrayRef {
    match id_type {
        DataType::Utf8 => Arc::new(StringArray::from_iter_values(
            keys.map(|k| format!("key-{k:04}")),
        )),
        _ => Arc::new(Int64Array::from_iter_values(keys)),
    }
}

fn batch(schema: &Arc<Schema>, keys: RangeInclusive<i64>, value: i64) -> TestResult<RecordBatch> {
    let ids = ids(schema.field(0).data_type(), keys);
    let values = Arc::new(Int64Array::from(vec![value; ids.len()]));
    Ok(RecordBatch::try_new(Arc::clone(schema), vec![ids, values])?)
}

async fn cdc_upsert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<()> {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// `(rows served, distinct ids, rows whose value is not `want`)`.
async fn served(ctx: &SessionContext, name: &str, want: i64) -> TestResult<(i64, i64, i64)> {
    let sql = format!(
        "SELECT COUNT(*), COUNT(DISTINCT id), COALESCE(SUM(CASE WHEN value <> {want} THEN 1 ELSE 0 END), 0) FROM {name}"
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
    // Run every case before asserting, so one failure does not hide another.
    let mut got = Vec::new();
    let mut want = Vec::new();
    for (name, id_type, prior) in [
        ("t_int64", DataType::Int64, Prior::MemTier),
        ("t_utf8", DataType::Utf8, Prior::MemTier),
        ("t_int64_inline", DataType::Int64, Prior::InlineAndMemTier),
        ("t_utf8_inline", DataType::Utf8, Prior::InlineAndMemTier),
    ] {
        let result = run_fallback_upsert(&fixture, name, id_type, prior).await;
        set_global_mem_tier_bytes(0);
        let (observed, expected) = result?;
        got.push((name, observed));
        want.push((name, expected));
    }
    assert_eq!(
        got, want,
        "every key served once with the fallback upsert's value, and the mem tier drained \
         (rows, distinct ids, stale values, rows left in RAM)"
    );
    Ok(())
}

async fn run_fallback_upsert(
    fixture: &common::TestFixture,
    name: &str,
    id_type: DataType,
    prior: Prior,
) -> TestResult<(Observed, Observed)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", id_type, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: name.to_string(),
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
        "{name}: the in-memory CDC tier is armed"
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Room for the earlier writes: CDC lands in the mem tier.
    set_global_mem_tier_bytes(64 << 20);
    let expected_keys = match prior {
        Prior::MemTier => {
            cdc_upsert(&table, batch(&schema, 1..=KEYS, 1)?).await?;
            KEYS
        }
        Prior::InlineAndMemTier => {
            common::insert_batch(table.as_ref(), batch(&schema, 1..=KEYS, 1)?).await?;
            cdc_upsert(&table, batch(&schema, KEYS + 1..=2 * KEYS, 2)?).await?;
            2 * KEYS
        }
    };

    // No room for the second: it waits, spills this table's tier, and falls
    // back to the durable path.
    set_global_mem_tier_bytes(1);
    cdc_upsert(&table, batch(&schema, 1..=KEYS, 2)?).await?;
    let (rows, distinct, stale) = served(&ctx, name, 2).await?;
    // The fallback spills the tier before its durable write, so a checkpoint
    // now finds nothing left in RAM.
    let left_in_ram = table.checkpoint_mem_tier().await?;
    Ok((
        (rows, distinct, stale, left_in_ram),
        (expected_keys, expected_keys, 0, 0),
    ))
}
