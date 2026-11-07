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

#![expect(clippy::expect_used, reason = "integration-test helpers")]

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

use crate::common;

use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
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
            cluster_by: vec!["id".to_string()],
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

test_with_backends!(test_cold_tier_normalized_clustering_prunes_both_dimensions_impl);
test_with_backends!(test_warm_tier_normalized_clustering_prunes_both_dimensions_impl);

/// `DataFusion`'s default write batch is 8192 rows. The Vortex sink checks the
/// 1 MB cold target at batch boundaries, so a pad just over 128 B makes each
/// batch its own file. Sixteen files is the coarsest layout that can still
/// show a tenant point probe skipping files: in arrival/timestamp order every
/// file would still carry all 16 tenants.
const CLUSTER_BATCH_ROWS: i64 = 8192;
const CLUSTER_FILE_COUNT: i64 = 16;
const CLUSTER_ROWS: i64 = CLUSTER_BATCH_ROWS * CLUSTER_FILE_COUNT;
const CLUSTER_TENANTS: i64 = 16;
const CLUSTER_TS_BASE: i64 = 1_700_000_000_000_000;
const CLUSTER_TS_STEP: i64 = 1_000;
const CLUSTER_PAD_BYTES: usize = 256;
const CLUSTER_PAD_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

fn cluster_pad(row: i64) -> Vec<u8> {
    // Filled by extension rather than `vec![0; N]` then overwrite: every byte
    // is replaced, so the zeroing pass is pure waste — and at this fixture's
    // size it is tens of MiB of memset before the engine sees a row.
    let mut buf = Vec::with_capacity(CLUSTER_PAD_BYTES);
    let mut x = row
        .cast_unsigned()
        .wrapping_mul(CLUSTER_PAD_MIX)
        .wrapping_add(1);
    while buf.len() < CLUSTER_PAD_BYTES {
        x = x.wrapping_mul(CLUSTER_PAD_MIX).wrapping_add(1);
        buf.extend_from_slice(&x.to_le_bytes());
    }
    buf
}

fn int64_bound(value: &datafusion_common::stats::Precision<ScalarValue>) -> Option<i64> {
    match value.get_value()? {
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    }
}

/// A warm-tier rewrite must use the explicit multi-dimensional cluster key and
/// ignore the inference-derived sort key. This exercises the real Vortex write,
/// footer-statistics, planning, and scan paths before any datalake promotion.
async fn test_warm_tier_normalized_clustering_prunes_both_dimensions_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("tenant", DataType::Int64, false),
        Field::new("pad", DataType::Binary, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "warm_cluster_prune_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cluster_by: vec!["ts".to_string(), "tenant".to_string()],
            // Schema inference commonly supplies the primary key as an inferred
            // sort. Explicit clustering must suppress it, not reject the table
            // or lexicographically sort by it.
            sort_columns: vec!["id".to_string()],
            sort_columns_origin: cayenne::metadata::SortColumnsOrigin::Inferred,
            target_vortex_file_size_mb: 1,
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
        "warm_cluster_prune_t",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    let ids: Vec<i64> = (0..CLUSTER_ROWS).collect();
    let timestamps: Vec<i64> = (0..CLUSTER_ROWS)
        .map(|i| CLUSTER_TS_BASE + i * CLUSTER_TS_STEP)
        .collect();
    let tenants: Vec<i64> = (0..CLUSTER_ROWS)
        .map(|i| (i * 7) % CLUSTER_TENANTS)
        .collect();
    let pads: Vec<Vec<u8>> = (0..CLUSTER_ROWS).map(cluster_pad).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(tenants)),
            Arc::new(BinaryArray::from_iter_values(pads)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    table.flush_pending_maintenance().await?;
    table.checkpoint_inlined_data().await?;
    table.checkpoint_mem_tier().await?;

    assert!(
        table.effective_sort_columns_for_rewrite().is_empty(),
        "automatic/inferred sort columns must be disabled by explicit cluster_by"
    );
    table.sort_and_rewrite_data(1024 * 1024).await?;

    let all_files = planned_files_for(&ctx, "SELECT id FROM warm_cluster_prune_t").await?;
    assert!(
        all_files >= 8,
        "the warm rewrite must roll several files so pruning is observable, got {all_files}"
    );

    let probe_row = CLUSTER_ROWS / 2;
    let probe_ts = CLUSTER_TS_BASE + probe_row * CLUSTER_TS_STEP;
    let probe_tenant = (probe_row * 7) % CLUSTER_TENANTS;
    let ts_files = planned_files_for(
        &ctx,
        &format!("SELECT id FROM warm_cluster_prune_t WHERE ts = {probe_ts}"),
    )
    .await?;
    let tenant_files = planned_files_for(
        &ctx,
        &format!("SELECT id FROM warm_cluster_prune_t WHERE tenant = {probe_tenant}"),
    )
    .await?;
    assert!(
        ts_files < all_files,
        "a warm-tier timestamp point filter pruned nothing: {ts_files} of {all_files} files planned"
    );
    assert!(
        tenant_files < all_files,
        "a warm-tier tenant point filter pruned nothing: {tenant_files} of {all_files} files planned"
    );

    assert_eq!(
        collect_ids(
            &ctx,
            &format!("SELECT id FROM warm_cluster_prune_t WHERE ts = {probe_ts}")
        )
        .await?,
        vec![probe_row]
    );
    let expected_tenant: Vec<i64> = (0..CLUSTER_ROWS)
        .filter(|i| (*i * 7) % CLUSTER_TENANTS == probe_tenant)
        .collect();
    assert_eq!(
        collect_ids(
            &ctx,
            &format!("SELECT id FROM warm_cluster_prune_t WHERE tenant = {probe_tenant}")
        )
        .await?,
        expected_tenant
    );
    Ok(())
}

/// One promotion of the timestamp/tenant fixture. Predicates on *both*
/// differently-scaled clustering columns must drop cold files — the defect the
/// normalized `Hilbert` key exists to fix. Kernel tests can pass while statistics
/// lookup, stream augmentation, sorting, or Vortex footers are unwired.
async fn test_cold_tier_normalized_clustering_prunes_both_dimensions_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("tenant", DataType::Int64, false),
        Field::new("pad", DataType::Binary, false),
    ]));

    let cold_dir = fixture.temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir)?;

    let table_options = CreateTableOptions {
        table_name: "cluster_prune_t".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cluster_by: vec!["ts".to_string(), "tenant".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 1,
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
        "cluster_prune_t",
        Arc::clone(&table) as Arc<dyn TableProvider>,
    )?;

    let ids: Vec<i64> = (0..CLUSTER_ROWS).collect();
    let timestamps: Vec<i64> = (0..CLUSTER_ROWS)
        .map(|i| CLUSTER_TS_BASE + i * CLUSTER_TS_STEP)
        .collect();
    let tenants: Vec<i64> = (0..CLUSTER_ROWS)
        .map(|i| (i * 7) % CLUSTER_TENANTS)
        .collect();
    let pads: Vec<Vec<u8>> = (0..CLUSTER_ROWS).map(cluster_pad).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(tenants)),
            Arc::new(BinaryArray::from_iter_values(pads)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    table.flush_pending_maintenance().await?;
    table.checkpoint_inlined_data().await?;
    table.checkpoint_mem_tier().await?;
    let maintained = table
        .optimizer_table_statistics()
        .expect("maintained statistics must be warm before promotion");
    assert!(
        int64_bound(&maintained.column_statistics[1].min_value).is_some()
            && int64_bound(&maintained.column_statistics[2].min_value).is_some(),
        "promotion has no [min, max] for ts/tenant, so the kernel would fall back to raw keys"
    );

    assert!(
        table.promote_warm_to_cold().await?,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );

    let cold_files = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    assert!(
        cold_files.len() >= 8,
        "one clustered promotion must roll several cold files so pruning is observable, got {}",
        cold_files.len()
    );

    let mut tenant_ranges = Vec::new();
    for file in &cold_files {
        let stats = cayenne::stats::file_statistics_to_df(
            &cayenne::stats::deserialize_file_statistics(&file.statistics_blob, schema.as_ref())?,
            schema.as_ref(),
            file.row_count,
        );
        let lo = int64_bound(&stats.column_statistics[2].min_value);
        let hi = int64_bound(&stats.column_statistics[2].max_value);
        tenant_ranges.push((lo, hi));
    }
    assert!(
        tenant_ranges.iter().any(|(lo, hi)| match (lo, hi) {
            (Some(lo), Some(hi)) => *hi - *lo < CLUSTER_TENANTS - 1,
            _ => false,
        }),
        "every cold file still spans all {CLUSTER_TENANTS} tenants {tenant_ranges:?} — clustering did not tighten the narrow dimension"
    );

    let all_files = planned_files_for(&ctx, "SELECT id FROM cluster_prune_t").await?;
    assert!(
        all_files >= 8,
        "one clustered promotion must roll several cold files so pruning is observable, got {all_files}"
    );

    let probe_row = CLUSTER_ROWS / 2;
    let probe_ts = CLUSTER_TS_BASE + probe_row * CLUSTER_TS_STEP;
    let probe_tenant = (probe_row * 7) % CLUSTER_TENANTS;

    let ts_files = planned_files_for(
        &ctx,
        &format!("SELECT id FROM cluster_prune_t WHERE ts = {probe_ts}"),
    )
    .await?;
    assert!(
        ts_files < all_files,
        "a timestamp point filter pruned nothing: {ts_files} of {all_files} files planned"
    );

    let tenant_files = planned_files_for(
        &ctx,
        &format!("SELECT id FROM cluster_prune_t WHERE tenant = {probe_tenant}"),
    )
    .await?;
    assert!(
        tenant_files < all_files,
        "a tenant point filter pruned nothing: {tenant_files} of {all_files} files planned"
    );

    assert_eq!(
        collect_ids(
            &ctx,
            &format!("SELECT id FROM cluster_prune_t WHERE ts = {probe_ts}")
        )
        .await?,
        vec![probe_row],
        "the timestamp-pruned plan still returns the matching row"
    );

    let tenant_ids = collect_ids(
        &ctx,
        &format!("SELECT id FROM cluster_prune_t WHERE tenant = {probe_tenant}"),
    )
    .await?;
    let expected_tenant: Vec<i64> = (0..CLUSTER_ROWS)
        .filter(|i| (*i * 7) % CLUSTER_TENANTS == probe_tenant)
        .collect();
    assert_eq!(
        tenant_ids, expected_tenant,
        "the tenant-pruned plan still returns every matching row"
    );
    Ok(())
}

/// Distinct data files (not byte-range splits of them) a plan will open.
fn distinct_planned_files(
    plan: &Arc<dyn ExecutionPlan>,
    out: &mut std::collections::BTreeSet<String>,
) {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        for group in &config.file_groups {
            for file in group.iter() {
                out.insert(file.object_meta.location.to_string());
            }
        }
        return;
    }
    for child in plan.children() {
        distinct_planned_files(child, out);
    }
}

async fn planned_distinct_files_for(ctx: &SessionContext, sql: &str) -> TestResult<usize> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let mut files = std::collections::BTreeSet::new();
    distinct_planned_files(&plan, &mut files);
    Ok(files.len())
}

test_with_backends!(test_cold_tier_pk_bloom_prunes_point_lookups_impl);
test_with_backends!(test_cold_tier_pk_bloom_composite_key_impl);

/// The id of the row at position `p` (also its `ts`): a bijection over
/// `0..CLUSTER_ROWS` that scatters ids, so files holding consecutive `ts` values
/// each hold ids from the whole range and footer min/max on `id` cannot skip them.
fn scattered_id(p: i64) -> i64 {
    (p * 40_503) % CLUSTER_ROWS
}

/// A promoted table of `CLUSTER_ROWS` rows with an upsert primary key
/// (`primary_key`), clustered on a column unrelated to the key's order.
async fn pk_bloom_table(
    fixture: &common::TestFixture,
    name: &str,
    primary_key: &[&str],
) -> TestResult<(SessionContext, Arc<CayenneTableProvider>, usize)> {
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
        Field::new("pad", DataType::Binary, false),
    ]));
    let cold_dir = fixture.temp_dir.path().join(format!("cold_{name}"));
    std::fs::create_dir_all(&cold_dir)?;
    let primary_key: Vec<String> = primary_key.iter().map(|c| (*c).to_string()).collect();
    let table_options = CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::clone(&schema),
        primary_key: primary_key.clone(),
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(primary_key))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.to_string_lossy())),
            cluster_by: vec!["ts".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 1,
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
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;

    let ids: Vec<i64> = (0..CLUSTER_ROWS).map(scattered_id).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from((0..CLUSTER_ROWS).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                ids.iter().map(|i| i % 3).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from_iter_values(
                (0..CLUSTER_ROWS).map(cluster_pad),
            )),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    table.flush_pending_maintenance().await?;
    table.checkpoint_inlined_data().await?;
    table.checkpoint_mem_tier().await?;
    assert!(
        table.promote_warm_to_cold().await?,
        "promotion should fire with cold_tier_warm_max_files = 1"
    );
    let cold_files = fixture
        .catalog
        .list_cold_tier_files(table.table_id())
        .await?;
    assert!(
        cold_files.len() >= 8,
        "the promotion must roll several cold files so pruning is observable, got {}",
        cold_files.len()
    );
    assert!(
        cold_files.iter().all(|f| f.pk_bloom.is_some()),
        "every cold file must carry a primary-key bloom for an upsert table"
    );
    let mut id_ranges = Vec::with_capacity(cold_files.len());
    for file in &cold_files {
        let stats = cayenne::stats::file_statistics_to_df(
            &cayenne::stats::deserialize_file_statistics(&file.statistics_blob, schema.as_ref())?,
            schema.as_ref(),
            file.row_count,
        );
        id_ranges.push((
            int64_bound(&stats.column_statistics[0].min_value),
            int64_bound(&stats.column_statistics[0].max_value),
        ));
    }
    for probe in [CLUSTER_ROWS / 3, CLUSTER_ROWS / 5] {
        let overlapping = id_ranges
            .iter()
            .filter(
                |(lo, hi)| matches!((lo, hi), (Some(lo), Some(hi)) if *lo <= probe && probe <= *hi),
            )
            .count();
        assert!(
            overlapping * 2 > cold_files.len(),
            "fixture must defeat min/max at {probe}: {id_ranges:?}"
        );
    }
    let all_files = planned_distinct_files_for(&ctx, &format!("SELECT id FROM {name}")).await?;
    assert_eq!(all_files, cold_files.len());
    Ok((ctx, table, cold_files.len()))
}

/// Primary-key point lookups must skip cold files whose bloom rejects the key,
/// and must never skip a file holding a matching row.
async fn test_cold_tier_pk_bloom_prunes_point_lookups_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let (ctx, _table, total_files) = pk_bloom_table(&fixture, "bloom_t", &["id"]).await?;

    // A wide interval exceeds the finite-key lookup bound. Equal lower/upper
    // bounds would be simplified to equality and could probe the bloom.
    let probe_id = CLUSTER_ROWS / 3;
    let range_end = probe_id + 4_096;
    let range_sql = format!("SELECT id FROM bloom_t WHERE id >= {probe_id} AND id <= {range_end}");
    let min_max_only = planned_distinct_files_for(&ctx, &range_sql).await?;
    assert!(
        min_max_only * 2 > total_files,
        "fixture must defeat min/max pruning: {min_max_only} of {total_files} files survive a range"
    );
    assert_eq!(
        collect_ids(&ctx, &range_sql).await?,
        (probe_id..=range_end).collect::<Vec<_>>()
    );

    // Present keys, including the ends of the key space: exact rows, few files.
    let present = [
        0,
        1,
        probe_id,
        CLUSTER_ROWS / 2,
        CLUSTER_BATCH_ROWS - 1,
        CLUSTER_BATCH_ROWS,
        CLUSTER_ROWS - 1,
    ];
    for id in &present {
        let sql = format!("SELECT id FROM bloom_t WHERE id = {id}");
        let planned = planned_distinct_files_for(&ctx, &sql).await?;
        assert!(
            (1..=3).contains(&planned),
            "id = {id}: expected the key's file (plus rare bloom false positives), planned {planned} of {total_files}"
        );
        assert_eq!(collect_ids(&ctx, &sql).await?, vec![*id], "id = {id}");
        // Int32 literal against the Int64 key.
        let sql32 = format!("SELECT id FROM bloom_t WHERE id = CAST({id} AS INT)");
        assert_eq!(
            collect_ids(&ctx, &sql32).await?,
            vec![*id],
            "int32 id = {id}"
        );
        assert!(
            planned_distinct_files_for(&ctx, &sql32).await? <= 3,
            "int32 literal id = {id} must prune as the int64 literal does"
        );
        // Conjunction with another predicate still pins the key.
        let sql_and = format!(
            "SELECT id FROM bloom_t WHERE id = {id} AND region = {}",
            id % 3
        );
        assert_eq!(
            collect_ids(&ctx, &sql_and).await?,
            vec![*id],
            "and id = {id}"
        );
    }

    // Absent keys: no rows, and (barring false positives) no files.
    for id in [-1_i64, CLUSTER_ROWS, CLUSTER_ROWS + 12_345] {
        let sql = format!("SELECT id FROM bloom_t WHERE id = {id}");
        assert!(
            collect_ids(&ctx, &sql).await?.is_empty(),
            "absent id = {id}"
        );
    }
    let absent_outside_range =
        planned_distinct_files_for(&ctx, "SELECT id FROM bloom_t WHERE id = -1").await?;
    assert_eq!(absent_outside_range, 0, "min/max already excludes -1");

    // IN lists: every present key is returned; the files read are the union.
    let in_list = [3_i64, 70_001, 131_071, 999_999];
    let sql = format!(
        "SELECT id FROM bloom_t WHERE id IN ({})",
        in_list
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert_eq!(
        collect_ids(&ctx, &sql).await?,
        vec![3, 70_001, 131_071],
        "IN list with an absent key"
    );
    assert!(
        planned_distinct_files_for(&ctx, &sql).await? * 2 <= total_files,
        "an IN list of three present keys must read well under half of the files"
    );

    // Shapes that are not a conjunctive pin of the key must not prune.
    for predicate in [
        format!("id = {probe_id} OR region = 1"),
        format!("id + 0 = {probe_id}"),
        format!("id <> {probe_id}"),
    ] {
        let sql = format!("SELECT id FROM bloom_t WHERE {predicate}");
        let expected: Vec<i64> = match predicate.as_str() {
            p if p.contains("OR region") => (0..CLUSTER_ROWS)
                .filter(|i| *i == probe_id || i % 3 == 1)
                .collect(),
            p if p.contains("<>") => (0..CLUSTER_ROWS).filter(|i| *i != probe_id).collect(),
            _ => vec![probe_id],
        };
        assert_eq!(collect_ids(&ctx, &sql).await?, expected, "{predicate}");
    }

    // A key deleted after promotion is gone: the bloom still holds it, so its
    // file is read and the deletion applies; a neighbor is untouched.
    ctx.sql(&format!("DELETE FROM bloom_t WHERE id = {probe_id}"))
        .await?
        .collect()
        .await?;
    assert!(
        collect_ids(
            &ctx,
            &format!("SELECT id FROM bloom_t WHERE id = {probe_id}")
        )
        .await?
        .is_empty(),
        "deleted key must stay deleted"
    );
    assert_eq!(
        collect_ids(
            &ctx,
            &format!("SELECT id FROM bloom_t WHERE id = {}", probe_id + 1)
        )
        .await?,
        vec![probe_id + 1]
    );
    Ok(())
}

/// A composite key prunes only when every key column is pinned.
async fn test_cold_tier_pk_bloom_composite_key_impl(
    fixture: common::TestFixture,
) -> TestResult<()> {
    let (ctx, _table, total_files) =
        pk_bloom_table(&fixture, "bloom_comp_t", &["id", "region"]).await?;
    let id = CLUSTER_ROWS / 5;
    let region = id % 3;

    let full = format!("SELECT id FROM bloom_comp_t WHERE id = {id} AND region = {region}");
    assert_eq!(collect_ids(&ctx, &full).await?, vec![id]);
    assert!(
        planned_distinct_files_for(&ctx, &full).await? <= 3,
        "a fully pinned composite key must prune to the key's file"
    );

    // Wrong region for this id: no row, and no file holds the pair.
    let wrong = format!(
        "SELECT id FROM bloom_comp_t WHERE id = {id} AND region = {}",
        (region + 1) % 3
    );
    assert!(collect_ids(&ctx, &wrong).await?.is_empty());
    assert!(
        planned_distinct_files_for(&ctx, &wrong).await? <= 2,
        "a pair no file holds is rejected by the blooms"
    );

    // Only one of the two columns pinned: nothing to probe, nothing pruned, and
    // the answer is still exact.
    let partial = format!("SELECT id FROM bloom_comp_t WHERE id = {id}");
    assert_eq!(collect_ids(&ctx, &partial).await?, vec![id]);
    assert!(
        planned_distinct_files_for(&ctx, &partial).await? * 2 > total_files,
        "a partially pinned key must not be bloom-pruned"
    );
    Ok(())
}
