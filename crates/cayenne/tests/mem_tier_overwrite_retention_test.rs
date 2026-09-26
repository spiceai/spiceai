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

//! Operations over "every row of the table" must include the rows a
//! `cdc_durability: memory` table still holds in its in-memory CDC tier.
//!
//! - A whole-table overwrite (what the CDC rebuild issues after the source's
//!   history is gone) replaces every row. Rows left in the tier were read over the
//!   new snapshot, and the next checkpoint made them durable.
//! - A retention pass judges each key by the version it can scan. Without the
//!   tier it tombstoned a key whose superseded durable version matched
//!   `retention_sql`, and the tombstone hid the live replacement in RAM.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CdcDurability, CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use common::TestFixture;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

struct NoopSlotAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// An upsert table on the in-memory CDC tier with every background trigger off,
/// so the test alone decides when rows move into files.
fn memory_tier_options(
    fixture: &TestFixture,
    name: &str,
    schema: &Arc<Schema>,
) -> CreateTableOptions {
    CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    }
}

async fn cdc_apply(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<()> {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// Every row of `table` rendered as `id,value`, sorted.
async fn table_rows(table: &Arc<CayenneTableProvider>) -> TestResult<Vec<String>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)?;
    let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut rendered = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                rendered.push(array_value_to_string(column, row)?);
            }
            rows.push(rendered.join(","));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

fn id_name_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn id_name_batch(rows: &[(i64, &str)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        id_name_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

async fn overwrite(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> TestResult<()> {
    let ctx = SessionContext::new();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], id_name_schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Overwrite)
        .await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn overwrite_replaces_the_in_memory_tier_impl(fixture: TestFixture) -> TestResult<()> {
    const NAME: &str = "overwrite_replaces_mem_tier";
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            memory_tier_options(&fixture, NAME, &id_name_schema()),
            ctx.runtime_env(),
        )
        .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));

    overwrite(&table, id_name_batch(&[(1, "a"), (2, "b")])?).await?;
    // A newer version of key 1 and a new key 3 reach the in-memory tier, and a seal
    // makes them durable as a shadow the next bake would normally clear.
    cdc_apply(&table, id_name_batch(&[(1, "a2"), (3, "c")])?).await?;
    let sealed = table.seal_mem_tier_durable().await?;
    assert!(
        sealed > 0,
        "precondition: the seal must shadow the in-memory rows"
    );
    assert_eq!(
        table_rows(&table).await?,
        ["1,a2", "2,b", "3,c"],
        "precondition: the in-memory rows are visible before the overwrite"
    );

    // The source now holds {(1, a3), (2, b)}: key 3 was deleted there.
    overwrite(&table, id_name_batch(&[(1, "a3"), (2, "b")])?).await?;
    let expected = ["1,a3", "2,b"];
    assert_eq!(
        table_rows(&table).await?,
        expected,
        "an overwrite must replace the rows still in the in-memory tier"
    );
    table.checkpoint_mem_tier().await?;
    assert_eq!(
        table_rows(&table).await?,
        expected,
        "a checkpoint after the overwrite must not bring the replaced rows back"
    );

    // Nothing durable describes the replaced rows: a restart serves the overwrite.
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), ctx.runtime_env())
            .open(NAME)
            .await?,
    );
    reopened.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    assert_eq!(
        table_rows(&reopened).await?,
        expected,
        "after a restart the table must hold exactly the overwrite's rows"
    );

    // A key the overwrite removed can be written again.
    cdc_apply(&reopened, id_name_batch(&[(3, "c2")])?).await?;
    assert_eq!(table_rows(&reopened).await?, ["1,a3", "2,b", "3,c2"]);
    Ok(())
}

test_with_backends!(overwrite_replaces_the_in_memory_tier_impl);

fn id_value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn id_value_batch(rows: &[(i64, i64)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        id_value_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

async fn retention_judges_a_key_by_its_live_version_impl(fixture: TestFixture) -> TestResult<()> {
    const NAME: &str = "retention_live_version";
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table_with_retention(
            catalog,
            memory_tier_options(&fixture, NAME, &id_value_schema()),
            vec![col("value").lt(lit(50_i64))],
            ctx.runtime_env(),
        )
        .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));

    // (7, 10) matches `value < 50` and is durable; (8, 70) does not match.
    cdc_apply(&table, id_value_batch(&[(7, 10), (8, 70)])?).await?;
    table.checkpoint_mem_tier().await?;
    // Key 7's live version (7, 60) does not match, and exists only in RAM.
    cdc_apply(&table, id_value_batch(&[(7, 60)])?).await?;
    assert_eq!(
        table_rows(&table).await?,
        ["7,60", "8,70"],
        "precondition: the live version is visible before retention runs"
    );

    table.flush_pending_maintenance().await?;
    assert_eq!(
        table_rows(&table).await?,
        ["7,60", "8,70"],
        "retention must not delete a key whose live version does not match `retention_sql`"
    );
    table.checkpoint_mem_tier().await?;
    assert_eq!(table_rows(&table).await?, ["7,60", "8,70"]);

    // Rows that do match are still deleted: (9, 5) is durable, and the checkpoint
    // that made it so arms the pass; (10, 3) is still in RAM when the pass runs.
    cdc_apply(&table, id_value_batch(&[(9, 5)])?).await?;
    table.checkpoint_mem_tier().await?;
    cdc_apply(&table, id_value_batch(&[(10, 3)])?).await?;
    table.flush_pending_maintenance().await?;
    assert_eq!(
        table_rows(&table).await?,
        ["7,60", "8,70"],
        "retention must still delete rows that match `retention_sql`, durable or in RAM"
    );
    Ok(())
}

test_with_backends!(retention_judges_a_key_by_its_live_version_impl);
