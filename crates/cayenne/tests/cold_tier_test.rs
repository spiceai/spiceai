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

//! Integration tests for the cold object-store tier (storage-cascade bottom
//! tier): whole-table promotion to a (local `file://`) cold store, cross-tier
//! scan correctness, and the key-delete-after-promotion invariant.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(test_cold_tier_promotion_cross_tier_scan_and_delete_impl);

async fn row_count(ctx: &SessionContext, table: &str) -> TestResult<i64> {
    let results = ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await?
        .collect()
        .await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
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

async fn delete_id(table: &Arc<CayenneTableProvider>, id: i64) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
        .await?;
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

async fn test_cold_tier_promotion_cross_tier_scan_and_delete_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Local `file://` cold tier — no object-store config needed (the default
    // local store resolves it).
    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;
    let cold_url = format!("file://{}", cold_dir.to_string_lossy());

    let table_options = CreateTableOptions {
        table_name: "cold_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            // Cold tier on the local fs, clustered by `id`, triggered by ANY
            // warm file so the test is deterministic.
            cold_tier_location: Some(cold_url),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
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
    ctx.register_table("cold_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // Insert 200 rows (value = id * 2) across two batches.
    for range in [0i64..100, 100..200] {
        let ids: Vec<i64> = range.collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        common::insert_batch(table.as_ref(), batch).await?;
    }

    // Flush the in-RAM/inline tiers to durable warm Vortex files so the
    // promotion trigger (which reads the warm file set) fires.
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;

    // Graduate the warm tier to the cold object store (Z-order clustered).
    let promoted = table.promote_warm_to_cold().await?;
    assert!(
        promoted,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );

    // Cold files are registered in the metastore manifest with the full row set.
    let cold = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    assert!(
        !cold.is_empty(),
        "expected cold-tier files registered after promotion"
    );
    let cold_rows: i64 = cold.iter().map(|f| f.row_count).sum();
    assert_eq!(cold_rows, 200, "all 200 rows graduated to cold");
    assert!(
        cold.iter().all(|f| !f.statistics_blob.is_empty()),
        "each cold file carries a footer statistics blob for listing-time pruning"
    );

    // The physical cold files exist on the local cold store.
    let physical_cold_files = count_vortex_files(&cold_dir);
    assert!(
        physical_cold_files >= 1,
        "expected at least one physical .vortex file on the cold store, got {physical_cold_files}"
    );

    // Cross-tier scan: warm is now an empty snapshot, so returning all rows
    // proves the cold branch is read + unioned correctly.
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        200,
        "cross-tier scan returns all promoted rows from the cold tier"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42").await?,
        vec![(42, 84)],
        "point lookup over the cold tier returns the right row"
    );
    assert_eq!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t ORDER BY id LIMIT 3").await?,
        vec![(0, 0), (1, 2), (2, 4)],
        "ordered cross-tier scan with a limit returns correct rows"
    );

    // Key-delete-after-promotion: a delete must hide a row that now lives ONLY
    // in the cold tier (the cold branch applies the shared key-delete filter).
    let deleted = delete_id(&table, 42).await?;
    eprintln!("[cold_tier_test] DELETE id=42 reported rows-affected = {deleted}");
    // The data-correctness invariant: the row is hidden and the live count drops,
    // regardless of the reported rows-affected count (which is a separate concern).
    assert!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42")
            .await?
            .is_empty(),
        "a delete after promotion hides the cold-resident row (Ignore-filter invariant)"
    );
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        199,
        "exactly one row removed across the cold tier"
    );

    // Insert more warm rows, then promote AGAIN. Whole-table promotion
    // re-materializes the entire visible table (warm + prior cold) into a new
    // cold generation; replace-all in the commit must prevent gen-1 rows from
    // being double-counted alongside gen-2.
    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![200i64, 201, 202])),
            Arc::new(Int64Array::from(vec![400i64, 402, 404])),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch2).await?;
    let _ = table.checkpoint_inlined_data().await;
    let _ = table.checkpoint_mem_tier().await;
    assert!(
        table.promote_warm_to_cold().await?,
        "second promotion should fire"
    );

    // 199 (post-delete) + 3 new = 202, with NO gen-1 duplication.
    assert_eq!(
        row_count(&ctx, "cold_t").await?,
        202,
        "repeated whole-table promotion must not double-count the prior cold generation"
    );
    assert!(
        collect_pairs(&ctx, "SELECT id, value FROM cold_t WHERE id = 42")
            .await?
            .is_empty(),
        "the delete stays applied across a second promotion"
    );
    let cold2 = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    let regraduated_rows: i64 = cold2.iter().map(|f| f.row_count).sum();
    assert_eq!(
        regraduated_rows, 202,
        "cold manifest holds exactly the live row set after replace-all promotion"
    );

    Ok(())
}

/// Recursively count `.vortex` files under `dir`.
fn count_vortex_files(dir: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_vortex_files(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("vortex") {
                count += 1;
            }
        }
    }
    count
}
