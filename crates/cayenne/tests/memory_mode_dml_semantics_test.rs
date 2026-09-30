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

//! Two `mode: memory` DML behaviours that the end-to-end tests do not pin, both
//! decided inside this crate rather than by the runtime:
//!
//! * **A NULL predicate keeps its row.** SQL deletes a row only where the
//!   predicate is TRUE, so NULL and FALSE both mean keep. The mem-tier rebuild
//!   inverts a match mask to decide what survives, and `not(NULL)` is NULL — which
//!   `filter_record_batch` drops. Getting this wrong silently deletes exactly the
//!   rows the predicate could not evaluate.
//! * **`DoNothingAll` drops the incoming row.** A declared `primary_key` with no
//!   `on_conflict` resolves to that policy, and the memory append reaches it only
//!   because it now runs primary-key validation at all. A client `INSERT` cannot
//!   reach this through the runtime — without `on_conflict`,
//!   `select_accelerated_write_mode` sends client writes `WriteThrough` to the
//!   source — so the refresh path is the only way in, and the policy is exercised
//!   here directly.

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(a_null_predicate_keeps_its_row_impl);
test_with_backends!(do_nothing_drops_the_incoming_row_impl);

/// `value` is NULLABLE — the whole point of the first test.
fn nullable_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn name_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// A `mode: memory` table, built the way `apply_memory_mode_overrides` builds one.
async fn memory_table(
    fixture: &common::TestFixture,
    name: &str,
    schema: &Arc<Schema>,
    on_conflict: OnConflict,
) -> TestResult<(SessionContext, Arc<CayenneTableProvider>)> {
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(on_conflict),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            memory_mode: true,
            cdc_mem_tier_shards: 1,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            compaction_background_interval_ms: 0,
            inline_max_rows: 0,
            inline_max_bytes: 0,
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((ctx, table))
}

async fn ids(ctx: &SessionContext, sql: &str) -> TestResult<Vec<i64>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut out = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column Int64");
        out.extend(col.values().iter().copied());
    }
    out.sort_unstable();
    Ok(out)
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

/// A row whose predicate evaluates to NULL is KEPT, by both a matching and a
/// non-matching comparison — `value = 10` and `value <> 10` are each NULL for it,
/// and neither is TRUE.
async fn a_null_predicate_keeps_its_row_impl(fixture: common::TestFixture) -> TestResult<()> {
    let schema = nullable_schema();
    let (ctx, table) = memory_table(
        &fixture,
        "null_pred",
        &schema,
        OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()])),
    )
    .await?;
    let sql = "SELECT id FROM null_pred ORDER BY id";

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
        ],
    )?;
    common::insert_batches(&table, vec![batch]).await?;
    assert_eq!(
        ids(&ctx, sql).await?,
        vec![1, 2, 3],
        "precondition: all three rows resident, one with a NULL value"
    );

    // `value = 10` is TRUE for id 1, FALSE for id 3, NULL for id 2.
    let deleted = delete_where(&table, &ctx, col("value").eq(lit(10_i64))).await?;
    assert_eq!(deleted, 1, "only the row where the predicate is TRUE");
    assert_eq!(
        ids(&ctx, sql).await?,
        vec![2, 3],
        "the NULL row must survive a predicate it cannot satisfy"
    );

    // `value <> 10` is NULL for id 2 as well — still not TRUE, still kept.
    let deleted = delete_where(&table, &ctx, col("value").not_eq(lit(10_i64))).await?;
    assert_eq!(deleted, 1, "only id 3, whose value is 30");
    assert_eq!(
        ids(&ctx, sql).await?,
        vec![2],
        "a NULL row is not deleted by the negation either — NULL is not TRUE"
    );

    Ok(())
}

/// `primary_key` with no `on_conflict` resolves to `DoNothingAll`: a re-delivered
/// key leaves the RESIDENT row in place and drops the incoming one.
async fn do_nothing_drops_the_incoming_row_impl(fixture: common::TestFixture) -> TestResult<()> {
    let schema = name_schema();
    let (ctx, table) =
        memory_table(&fixture, "do_nothing", &schema, OnConflict::DoNothingAll).await?;
    let sql = "SELECT id FROM do_nothing ORDER BY id";

    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )?;
    common::insert_batches(&table, vec![seed]).await?;
    assert_eq!(ids(&ctx, sql).await?, vec![1, 2], "precondition: two rows");

    let conflicting = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["two-again", "three"])),
        ],
    )?;
    common::insert_batches(&table, vec![conflicting]).await?;

    assert_eq!(
        ids(&ctx, sql).await?,
        vec![1, 2, 3],
        "the genuinely new key lands and the conflicting one is dropped, not duplicated"
    );
    let names = ctx
        .sql("SELECT name FROM do_nothing WHERE id = 2")
        .await?
        .collect()
        .await?;
    let name = names[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .map(|a| a.value(0).to_string())
        .or_else(|| {
            names[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(0).to_string())
        })
        .expect("name column");
    assert_eq!(
        name, "two",
        "DoNothing keeps the RESIDENT row; the incoming one is discarded"
    );

    Ok(())
}
