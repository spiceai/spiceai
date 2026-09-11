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

//! The CDC apply loop's per-key delete on a `mode: memory` table (#12008).
//!
//! A memory-resident table is excluded from the deferred-commit queue, so the
//! runtime never installs a slot advancer on it. The apply loop checks
//! `has_slot_advancer()` directly and declines to absorb the delete in RAM
//! (`refresh_task::changes`, `"no_advancer"`), falling through to
//! `delete_from_cdc_fast` — which reaches the same deletion sink a client `DELETE`
//! does. Cayenne's own gate inside `write_cdc_delete_keys_in_memory` is a second,
//! defensive check on the same condition that production therefore never reaches;
//! this test drives it anyway, because it is the gate this crate owns.
//!
//! Before the mem-tier rebuild landed in that sink, it scanned only the durable
//! tiers, which in memory mode are permanently empty: it reported the delete as
//! HANDLED while removing nothing. Neutering the rebuild turns the assertion below
//! from `Some(1)` back into `Some(0)`, so this test does exercise it.
//!
//! What is NOT established is which production configuration reaches this function
//! on a memory-resident table. A `cdc:` (Debezium push) source does not — see
//! `cdc_ingest_delete_removes_a_cayenne_memory_mode_row`, which stays green with
//! the rebuild neutered. This covers the function and its contract; the routing
//! that would make it reachable is unproven.
//!
//! End-to-end coverage of the client `DELETE`/`UPDATE`/`INSERT` statements lives
//! in `crates/runtime/tests/acceleration/cayenne_memory.rs`. This path needs a
//! real CDC source to reach through the runtime, so it is driven directly here.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
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

test_with_backends!(cdc_key_delete_removes_a_memory_mode_row_impl);

fn id_value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn id_value_batch(schema: &Arc<Schema>, rows: &[(i64, i64)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )?)
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

async fn cdc_key_delete_removes_a_memory_mode_row_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = id_value_schema();

    // The `mode: memory` shape `apply_memory_mode_overrides` builds: the mem-tier
    // is the permanent in-RAM store and nothing is ever encoded to Vortex.
    let table_options = CreateTableOptions {
        table_name: "mem_cdc_delete".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            memory_mode: true,
            cdc_mem_tier_shards: 1,
            cdc_mem_tier_max_age_ms: 0,
            cdc_mem_tier_checkpoint_interval_ms: 0,
            cdc_mem_tier_seal_age_ms: 0,
            compaction_background_interval_ms: 0,
            cold_tier_location: None,
            inline_max_rows: 0,
            inline_max_bytes: 0,
            inline_max_buffer_bytes: 0,
            cdc_mem_tier_max_bytes: 0,
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            ..VortexConfig::default()
        },
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "mem_cdc_delete",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;
    let sql = "SELECT id, value FROM mem_cdc_delete ORDER BY id";

    // No slot advancer is installed, matching the state the runtime leaves a
    // memory-resident table in: `refresh_task::changes` builds the deferred-commit
    // queue only for `is_cdc_memory_mode() && !is_memory_resident_mode()` and
    // installs the advancer only when that queue exists. Asserting that here would
    // be a tautology — nothing in this crate installs one — so the invariant is
    // stated rather than pinned; pinning it belongs to a test over that predicate
    // in `runtime-table`.

    common::insert_batches(
        &table,
        vec![id_value_batch(&schema, &[(1, 10), (2, 20), (3, 30)])?],
    )
    .await?;
    assert_eq!(
        collect_pairs(&ctx, sql).await?,
        vec![(1, 10), (2, 20), (3, 30)],
        "precondition: the rows are resident in the mem-tier"
    );

    // Precondition: the in-RAM absorb path declines, so the apply loop really does
    // fall through to the path under test rather than tombstoning the tier itself.
    let absorbed = table
        .write_cdc_delete_keys_in_memory(&id_value_batch(&schema, &[(2, 20)])?)
        .await?;
    assert!(
        absorbed.is_none(),
        "precondition: write_cdc_delete_keys_in_memory must decline without a slot \
         advancer, leaving delete_from_cdc_fast as the path that must remove the row"
    );

    // The call the CDC apply loop makes for a per-key delete.
    let handled = table
        .delete_from_cdc_fast(&[col("id").eq(lit(2_i64))])
        .await?;
    assert_eq!(
        handled,
        Some(1),
        "delete_from_cdc_fast reports the delete as handled, so the count it returns is \
         the only record of what it removed"
    );
    assert_eq!(
        collect_pairs(&ctx, sql).await?,
        vec![(1, 10), (3, 30)],
        "a source DELETE must remove the mem-tier row it names"
    );

    Ok(())
}
