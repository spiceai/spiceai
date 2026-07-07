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

#![allow(clippy::expect_used)]

//! Integration tests for listing-time Vortex file pruning and correctness.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, SlotAdvancer};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

test_with_backends!(test_listing_file_pruning_disjoint_id_ranges_impl);
test_with_backends!(test_mem_tier_upsert_point_lookup_not_pruned_by_own_tombstone_impl);

fn files_scanned_from_plan(plan: &str) -> Option<usize> {
    plan.lines().find_map(|line| {
        line.split(',')
            .find(|part| part.trim().starts_with("files_scanned="))
            .and_then(|part| part.split('=').nth(1))
            .and_then(|n| n.trim().parse().ok())
    })
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

async fn collect_pairs(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<(i64, i64)>, Box<dyn std::error::Error>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
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
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    Ok(rows)
}

/// Writes two disjoint id ranges into separate Vortex files, then verifies that
/// a point lookup on one range prunes files from the other range at listing time.
async fn test_listing_file_pruning_disjoint_id_ranges_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "file_pruning_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            sort_columns: vec!["id".to_string()],
            target_vortex_file_size_mb: 1,
            ..VortexConfig::default()
        },
    };

    let ctx = SessionContext::new();
    let catalog = Arc::clone(&fixture.catalog);
    let provider = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "file_pruning_table",
        Arc::clone(&provider) as Arc<dyn TableProvider>,
    )?;

    // Two large appends (> INLINE_MAX_ROWS) with disjoint id ranges so each
    // lands in its own Vortex file(s) with non-overlapping min/max on `id`.
    let low_ids: Vec<i64> = (0..2_000).collect();
    let high_ids: Vec<i64> = (10_000..12_000).collect();
    for ids in [low_ids, high_ids] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(ids)),
            ],
        )?;
        common::insert_batch(provider.as_ref(), batch).await?;
    }

    let unfiltered = ctx
        .sql("SELECT id FROM file_pruning_table")
        .await?
        .create_physical_plan()
        .await?;
    let unfiltered_plan = datafusion::physical_plan::displayable(unfiltered.as_ref())
        .indent(true)
        .to_string();
    let unfiltered_files = files_scanned_from_plan(&unfiltered_plan).unwrap_or(0);
    assert!(
        unfiltered_files >= 2,
        "expected at least two Vortex files, got {unfiltered_files} in plan:\n{unfiltered_plan}"
    );

    let filtered = ctx
        .sql("SELECT id FROM file_pruning_table WHERE id = 42")
        .await?
        .create_physical_plan()
        .await?;
    let filtered_plan = datafusion::physical_plan::displayable(filtered.as_ref())
        .indent(true)
        .to_string();
    let filtered_files = files_scanned_from_plan(&filtered_plan).unwrap_or(0);
    assert!(
        filtered_files < unfiltered_files,
        "listing-time pruning should drop disjoint files: unfiltered={unfiltered_files} filtered={filtered_files}\n{filtered_plan}"
    );

    let rows = ctx
        .sql("SELECT id FROM file_pruning_table WHERE id = 42")
        .await?
        .collect()
        .await?;
    let count: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, 1, "point lookup must return exactly one row");

    let rows = ctx
        .sql("SELECT id FROM file_pruning_table WHERE id = 11000")
        .await?
        .collect()
        .await?;
    let count: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        count, 1,
        "high-range point lookup must return exactly one row"
    );

    Ok(())
}

/// Memory-mode upserts append replacement rows to the RAM tier and keep their
/// tombstones in the same snapshot. The tombstone-derived scan filter can prune
/// disk/inline data, but it must not be used to prune the RAM segment that holds
/// the replacement row itself.
async fn test_mem_tier_upsert_point_lookup_not_pruned_by_own_tombstone_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    struct TestAdvancer;
    #[async_trait::async_trait]
    impl SlotAdvancer for TestAdvancer {
        async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "mem_tier_pruning".to_string(),
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
            ..VortexConfig::default()
        },
    };

    let ctx = SessionContext::new();
    let catalog = Arc::clone(&fixture.catalog);
    let provider = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "mem_tier_pruning",
        Arc::clone(&provider) as Arc<dyn TableProvider>,
    )?;

    common::insert_batch(
        provider.as_ref(),
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )?,
    )
    .await?;

    provider.install_slot_advancer(Arc::new(TestAdvancer));

    let replacement = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )?;
    let write = provider
        .write_cdc_append_stream(batch_to_stream(replacement), &ctx.task_ctx())
        .await?;
    assert_eq!(
        write.in_memory_epoch(),
        Some(1),
        "precondition: replacement row must be published through the RAM tier"
    );

    let rows = collect_pairs(&ctx, "SELECT id, value FROM mem_tier_pruning WHERE id = 42").await?;
    assert_eq!(
        rows,
        vec![(42, 2)],
        "point lookup must keep the RAM-tier replacement row above its own tombstone"
    );

    let rows = collect_pairs(&ctx, "SELECT id, value FROM mem_tier_pruning").await?;
    assert_eq!(
        rows,
        vec![(42, 2)],
        "full scan must hide the old row and keep the RAM-tier replacement"
    );

    Ok(())
}
