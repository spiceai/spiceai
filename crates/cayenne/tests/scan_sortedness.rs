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

//! Sorted-file batches through Cayenne's optimized query scans.
#![expect(
    clippy::expect_used,
    reason = "integration fixture and diagnostic assertions"
)]
use crate::{
    common,
    write_path_sortedness_audit::{batch, config, insert, keys, report, schema, sql},
};
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use cayenne::{CayenneTableProvider, MetadataCatalog, metadata::CreateTableOptions};
use datafusion::{datasource::TableProvider, prelude::*};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::sync::Arc;

fn filter_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    let own = if plan.name() == "KeyBasedDeletionFilterExec" {
        plan.metrics().map_or(0, |metrics| {
            metrics
                .iter()
                .filter(|metric| metric.value().name() == name)
                .map(|metric| metric.value().as_usize())
                .sum()
        })
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| filter_metric(child, name))
        .sum::<usize>()
}

fn file_batch(start: i32, rows: i32) -> RecordBatch {
    let b = batch(start, rows);
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values((0..b.num_rows()).map(|i| {
        let mut x = (i as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (x ^ (x >> 31)).cast_signed()
    })));
    let mut columns = b.columns().to_vec();
    columns[4] = values;
    RecordBatch::try_new(schema(), columns).expect("incompressible file")
}

/// Exercise the optimized query scan, rather than the explicitly ordered audit reader.
#[tokio::test]
async fn sorted_files_real_scan_48_partitions() {
    use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
    fn counters(plan: &Arc<dyn ExecutionPlan>, result: &mut [usize; 3]) {
        if plan.name() == "KeyBasedDeletionFilterExec"
            && let Some(metrics) = plan.metrics()
        {
            for metric in metrics.iter() {
                let index = match metric.value().name() {
                    "sorted_path_batches" => 0,
                    "permuted_batches" => 1,
                    "sorted_runs" => 2,
                    _ => continue,
                };
                result[index] += metric.value().as_usize();
            }
        }
        for child in plan.children() {
            counters(child, result);
        }
    }
    fn ranges(
        plan: &Arc<dyn ExecutionPlan>,
        files: &mut std::collections::BTreeSet<String>,
    ) -> usize {
        if let Some(scan) = plan.downcast_ref::<datafusion_datasource::source::DataSourceExec>()
            && let Some(config) =
                scan.data_source()
                    .downcast_ref::<datafusion_datasource::file_scan_config::FileScanConfig>()
        {
            return config
                .file_groups
                .iter()
                .flat_map(datafusion_datasource::file_groups::FileGroup::iter)
                .map(|file| {
                    files.insert(file.object_meta.location.to_string());
                    usize::from(file.range.is_some())
                })
                .sum();
        }
        plan.children()
            .into_iter()
            .map(|child| ranges(child, files))
            .sum()
    }
    for branch in ["main", "protected_snapshot", "cold"] {
        let f = common::TestFixture::new(common::BackendType::Sqlite)
            .await
            .expect("scan fixture");
        let mut cfg = config();
        cfg.write_concurrency = Some(1);
        if branch == "cold" {
            let cold = f.temp_dir.path().join("cold");
            std::fs::create_dir_all(&cold).expect("cold directory");
            cfg.cold_tier_location = Some(format!("file://{}", cold.display()));
            cfg.cold_tier_warm_max_files = 1;
            cfg.cold_tier_background_interval_ms = 0;
        }
        let mut session = SessionConfig::new().with_target_partitions(48);
        session.options_mut().optimizer.repartition_file_min_size = 1;
        let ctx = SessionContext::new_with_config(session);
        let t = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&f.catalog) as Arc<dyn MetadataCatalog>,
                CreateTableOptions {
                    table_name: format!("scan_{branch}"),
                    schema: schema(),
                    primary_key: keys(),
                    on_conflict: (branch == "protected_snapshot")
                        .then(|| OnConflict::Upsert(ColumnReference::new(keys()))),
                    base_path: f.data_path.to_string_lossy().into_owned(),
                    partition_column: None,
                    vortex_config: cfg,
                },
                ctx.runtime_env(),
            )
            .await
            .expect("scan table"),
        );
        ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)
            .expect("register scan table");
        let rows_per_file = if branch == "main" { 800_000 } else { 200_000 };
        for part in 0..3 {
            insert(&t, file_batch(part * rows_per_file, rows_per_file)).await;
        }
        if branch == "cold" {
            assert!(
                t.promote_warm_to_cold()
                    .await
                    .expect("promote sorted files")
            );
        }
        sql(&ctx, "DELETE FROM t WHERE line=5 AND o % 17=0").await;
        // Audit the actual bytes independently, requesting physical order only
        // on this reader; the measured query below advertises no false ordering.
        report(&t, &f.catalog, &format!("scan_{branch}"), true, 0).await;
        for predicate in ["", " WHERE line=1 AND o % 97=0"] {
            let frame = ctx
                .sql(&format!("SELECT * FROM t{predicate}"))
                .await
                .expect("scan query");
            let plan = frame.create_physical_plan().await.expect("optimized scan");
            println!(
                "REAL_SCAN_48 branch={branch} predicate={predicate}\n{}",
                displayable(plan.as_ref()).indent(true)
            );
            let mut files = std::collections::BTreeSet::new();
            let split_ranges = ranges(&plan, &mut files);
            println!(
                "REAL_SCAN_48 branch={branch} files={} split_ranges={split_ranges}",
                files.len()
            );
            if branch == "cold" && predicate.is_empty() {
                assert!(
                    files.len() < 48 && split_ranges > files.len(),
                    "must split sorted files across byte ranges: files={} ranges={split_ranges}",
                    files.len()
                );
            }
            let batches = collect(Arc::clone(&plan), ctx.task_ctx())
                .await
                .expect("real scan");
            let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            let expected = (0..3 * rows_per_file)
                .filter(|id| {
                    let line = id % 10;
                    let order = id / 10 % 1000;
                    !(line == 5 && order % 17 == 0)
                        && (predicate.is_empty() || line == 1 && order % 97 == 0)
                })
                .count();
            assert_eq!(
                rows, expected,
                "branch={branch} predicate={predicate}: query row model"
            );
            let mut measured = [0; 3];
            counters(&plan, &mut measured);
            println!(
                "REAL_SCAN_48 branch={branch} predicate={predicate} counters={measured:?} rows={}",
                batches.iter().map(RecordBatch::num_rows).sum::<usize>()
            );
            assert!(
                measured[0] > 0,
                "branch={branch}: must exercise sorted filter: {measured:?}"
            );
            assert_eq!(
                measured[1], 0,
                "branch={branch} predicate={predicate}: sorted files must reach the filter in sorted batches: {measured:?}"
            );
        }
    }
}

/// An adaptive rewrite intentionally preserves its resolved clustering order.
#[tokio::test]
async fn observed_layout_compaction_uses_permutation() {
    use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
    fn permuted(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = if plan.name() == "KeyBasedDeletionFilterExec" {
            plan.metrics().map_or(0, |metrics| {
                metrics
                    .iter()
                    .filter(|metric| metric.value().name() == "permuted_batches")
                    .map(|metric| metric.value().as_usize())
                    .sum()
            })
        } else {
            0
        };
        own + plan.children().into_iter().map(permuted).sum::<usize>()
    }
    let f = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("adaptive fixture");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(48));
    let mut cfg = config();
    cfg.write_concurrency = Some(1);
    cfg.compaction_trigger_files = 4;
    cfg.target_vortex_file_size_mb = 1;
    let t = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&f.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "observed_layout".into(),
                schema: schema(),
                primary_key: keys(),
                on_conflict: None,
                base_path: f.data_path.to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: cfg,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("adaptive table"),
    );
    ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)
        .expect("register adaptive table");
    insert(&t, file_batch(0, 10_000)).await;
    report(&t, &f.catalog, "before_observed_layout", true, 0).await;
    // Observation, rather than explicit user layout, selects these non-PK-leading columns.
    for _ in 0..4 {
        sql(&ctx, "SELECT * FROM t WHERE line>=0 AND o>=0").await;
    }
    println!(
        "ADAPTIVE_LAYOUT selected={:?}",
        t.effective_sort_columns_for_rewrite()
    );
    for part in 1..4 {
        insert(&t, file_batch(part * 10_000, 10_000)).await;
    }
    t.flush_pending_maintenance()
        .await
        .expect("adaptive maintenance");
    t.compact_current_snapshot_small_files()
        .await
        .expect("force adaptive compaction");
    t.drain_in_flight_maintenance()
        .await
        .expect("settle adaptive rewrite");
    println!(
        "ADAPTIVE_LAYOUT compact_path={:?}",
        t.last_small_file_compact_path()
    );
    assert_eq!(
        t.last_small_file_compact_path(),
        cayenne::LastSmallFileCompactPath::Full,
        "must exercise full adaptive compaction"
    );
    let unsorted = crate::write_path_sortedness_audit::report_files(
        &t,
        &f.catalog,
        "adaptive_full_compaction",
        true,
        0,
        false,
    )
    .await;
    assert!(
        unsorted > 0,
        "adaptive rewrite must change physical file PK order"
    );
    sql(&ctx, "DELETE FROM t WHERE w=0 AND d=0 AND o=2 AND line=8").await;
    let plan = ctx
        .sql("SELECT * FROM t")
        .await
        .expect("adaptive query")
        .create_physical_plan()
        .await
        .expect("adaptive scan plan");
    println!(
        "ADAPTIVE_LAYOUT plan\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("adaptive scan");
    let count = permuted(&plan);
    println!(
        "ADAPTIVE_LAYOUT permuted_batches={count} rows={}",
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        39_999,
        "adaptive row model"
    );
    assert!(
        count > 0,
        "a non-PK adaptive layout must exercise permutation"
    );
}

/// Range routing on an observed column preserves full-PK order inside each file.
#[tokio::test]
async fn observed_range_bake_keeps_pk_order() {
    use datafusion::physical_plan::{collect, displayable};
    let f = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("range bake fixture");
    let mut cfg = config();
    cfg.target_vortex_file_size_mb = 1;
    let (ctx, t) = crate::write_path_sortedness_audit::table(&f, cfg).await;
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 48;
    insert(&t, file_batch(0, 100_000)).await;
    for _ in 0..8 {
        sql(&ctx, "SELECT * FROM t WHERE v=123").await;
    }
    for part in 1..7 {
        insert(&t, file_batch(part * 100_000, 100_000)).await;
    }
    report(&t, &f.catalog, "before_observed_bake", true, 0).await;
    assert!(
        t.bake_seq_prefix_protected_snapshots()
            .await
            .expect("range bake"),
        "must publish the old protected prefix"
    );
    let unsorted = crate::write_path_sortedness_audit::report_files(
        &t,
        &f.catalog,
        "observed_range_bake",
        true,
        0,
        true,
    )
    .await;
    assert_eq!(unsorted, 0, "range routing must preserve file PK order");
    sql(&ctx, "DELETE FROM t WHERE w=0 AND d=0 AND o=2 AND line=8").await;
    let plan = ctx
        .sql("SELECT * FROM t")
        .await
        .expect("baked query")
        .create_physical_plan()
        .await
        .expect("baked scan plan");
    println!(
        "OBSERVED_RANGE_BAKE plan\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("baked scan");
    let sorted_batches = filter_metric(&plan, "sorted_path_batches");
    let count = filter_metric(&plan, "permuted_batches");
    assert!(
        sorted_batches > 0,
        "baked rows must reach the sorted deletion filter"
    );
    println!(
        "OBSERVED_RANGE_BAKE sorted_batches={sorted_batches} permuted_batches={count} rows={}",
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        699_999,
        "baked row model"
    );
    assert_eq!(count, 0, "PK-sorted baked files must avoid permutation");
}
