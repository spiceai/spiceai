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

//! A selective query must read fewer datalake files than a full scan.
//!
//! Each cold file carries a Vortex footer statistics blob in the metastore
//! manifest, and the planner drops files whose per-column min/max cannot match
//! the query's filters, before any object-store round-trip. A regression that
//! stopped pruning would pass every correctness test and only show up as a slow
//! benchmark.
//!
//! Pruning happens at planning time, so the plan is the observable. The query
//! answers are re-checked too: a pruner that skips too much is worse than one
//! that skips nothing.

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Rows per promotion. Each promotion writes a disjoint `id` band, which is what
/// gives the pruner something to skip. How many objects a band splits into is the
/// writer's business and is not asserted here.
const BAND_SIZE: i64 = 100;
const BANDS: i64 = 4;

test_with_backends!(test_cold_tier_selective_query_prunes_files_impl);

/// Data files the planned scan will open, over every file-scan node in the tree.
/// The warm snapshot is empty after promotion, so every file counted is a
/// datalake file.
fn planned_file_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
        return exec
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .map_or(0, |config| {
                config.file_groups.iter().map(FileGroup::len).sum()
            });
    }
    plan.children().into_iter().map(planned_file_count).sum()
}

/// Plan `sql` and count the datalake files it would open.
///
/// Goes through SQL rather than calling `TableProvider::scan` with hand-built
/// filters: that would hand the provider its predicate directly and keep passing
/// even if `supports_filters_pushdown` regressed and real queries stopped pruning.
async fn planned_files_for(ctx: &SessionContext, sql: &str) -> TestResult<usize> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(planned_file_count(&plan))
}

async fn collect_ids(ctx: &SessionContext, sql: &str) -> TestResult<Vec<i64>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column Int64");
        ids.extend(column.values().iter().copied());
    }
    ids.sort_unstable();
    Ok(ids)
}

async fn test_cold_tier_selective_query_prunes_files_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let table_options = CreateTableOptions {
        table_name: "prune_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cold_clustering_columns: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
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
    ctx.register_table("prune_t", Arc::clone(&table) as Arc<dyn TableProvider>)?;

    // One promotion per `id` band. Promotions carry untouched cold files forward
    // by manifest reference, so every band keeps its own tight `id` range.
    let mut files_per_band: Vec<usize> = Vec::new();
    let mut files_before = 0;
    for band in 0..BANDS {
        let ids: Vec<i64> = (band * BAND_SIZE..(band + 1) * BAND_SIZE).collect();
        let values: Vec<i64> = ids.iter().map(|i| i * 2).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        common::insert_batch(table.as_ref(), batch).await?;
        table.checkpoint_inlined_data().await?;
        table.checkpoint_mem_tier().await?;
        assert!(
            table.promote_warm_to_cold().await?,
            "promotion {band} should fire with cold_tier_warm_max_files = 1"
        );

        // Files this band contributed. Promotions carry untouched cold files
        // forward, so the manifest only grows, and the delta is the band's own
        // share. Measured rather than assumed: how many objects a band splits
        // into is the writer's business, and hard-coding it would make a change
        // in file size or write parallelism look like a pruning regression.
        let total = fixture
            .catalog
            .list_cold_tier_files(table.table_id())
            .await?
            .len();
        files_per_band.push(total - files_before);
        files_before = total;
    }

    let all_files = planned_files_for(&ctx, "SELECT id FROM prune_t").await?;
    assert_eq!(
        all_files, files_before,
        "an unfiltered scan plans every datalake file"
    );

    // Only the band whose statistics cover the key can satisfy a point filter, so
    // exactly that band's files survive. More means the blob is not consulted.
    let point_files = planned_files_for(&ctx, "SELECT id FROM prune_t WHERE id = 150").await?;
    assert!(
        point_files < all_files,
        "the point filter pruned nothing: {point_files} of {all_files} files planned"
    );
    assert_eq!(
        point_files, files_per_band[1],
        "a point filter must prune the datalake down to the band holding the key \
         (planned {point_files} of {all_files})"
    );

    // A filter that matches nothing prunes every file: the plan opens no data.
    let empty_files = planned_files_for(&ctx, "SELECT id FROM prune_t WHERE id = -1").await?;
    assert_eq!(
        empty_files, 0,
        "a filter outside every file's statistics prunes the whole datalake branch"
    );

    // Pruning must not change answers.
    assert_eq!(
        collect_ids(&ctx, "SELECT id FROM prune_t WHERE id = 150").await?,
        vec![150],
        "the pruned-to file still returns the matching row"
    );
    assert_eq!(
        collect_ids(&ctx, "SELECT id FROM prune_t WHERE id BETWEEN 95 AND 105").await?,
        (95..=105).collect::<Vec<_>>(),
        "a range spanning two datalake files returns rows from both"
    );
    Ok(())
}
