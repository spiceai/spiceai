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

//! Differential of committed tap actions against the authoritative SQL catalog.

#![expect(clippy::expect_used, reason = "test setup and typed query assertions")]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::metastore_tap::{MetastoreTap, TapReport};
use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

use crate::common;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

async fn workload(broken: bool) -> TestResult<TapReport> {
    workload_with_shadow(broken, None).await.map(|(tap, _)| tap)
}

async fn workload_with_shadow(
    broken: bool,
    shadow: Option<(Arc<dyn object_store::ObjectStore>, bool)>,
) -> TestResult<(TapReport, cayenne::metastore_tap::ShadowReport)> {
    let shadow_enabled = shadow.is_some();
    let shadow_negative = shadow.as_ref().is_some_and(|(_, negative)| *negative);
    let dir = tempfile::tempdir()?;
    let tap = MetastoreTap::new(Some(dir.path().join("tap.jsonl")), true)?;
    let tap = if let Some((store, negative)) = shadow {
        let tap = if negative {
            tap.breaking_shadow_of("AddDeleteFiles")
        } else {
            tap
        };
        tap.with_shadow(
            store,
            object_store::path::Path::from("shadow"),
            object_store_occ::headlog::HeadLogConfig {
                head_budget_bytes: 1024 * 1024,
                batch_budget_bytes: 128 * 1024,
                // Records above max_record_bytes / 2 spill to objects; 1 KiB makes
                // this workload exercise both inline and spilled records.
                max_record_bytes: 1024,
                batch_max_records: 1,
            },
        )
    } else {
        tap
    };
    let tap = Arc::new(if broken {
        tap.breaking_apply_of("AddDeleteFiles")
    } else {
        tap
    });
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            dir.path().join("catalog.db").display()
        ))?
        .with_tap(Arc::clone(&tap)),
    );
    catalog.init().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    std::fs::create_dir_all(dir.path().join("cold"))?;
    let table = CayenneTableProviderBuilder::new(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        ctx.runtime_env(),
    )
    .create(CreateTableOptions {
        table_name: "tap_rows".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: dir.path().join("data").to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            deletion_mode: DeletionMode::Key,
            inline_max_rows: 0,
            compaction_trigger_protected_snapshots: 2,
            compaction_background_interval_ms: 0,
            cold_tier_location: Some(format!("file://{}", dir.path().join("cold").display())),
            cold_tier_warm_max_files: 1,
            cluster_by: vec!["id".to_string()],
            ..VortexConfig::default()
        },
    })
    .await?;
    let table = Arc::new(table);
    ctx.register_table("tap_rows", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let batch = |ids: Vec<i64>, values: Vec<i64>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
    };
    let compaction_guard = table.hold_compaction().await;
    for (ids, values) in [
        (vec![1, 2, 3], vec![10, 20, 30]),
        (vec![4, 5], vec![40, 50]),
        (vec![1, 2], vec![11, 22]),
        (vec![2, 6], vec![23, 60]),
    ] {
        common::insert_batch(&table, batch(ids, values)?).await?;
    }
    let plan = table
        .delete_from(&ctx.state(), vec![col("id").eq(lit(3_i64))])
        .await?;
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    table.checkpoint_mem_tier().await?;
    table.checkpoint_inlined_data().await?;
    drop(compaction_guard);
    assert!(
        table.compact_protected_snapshots_subset(usize::MAX).await?,
        "workload must commit a compaction"
    );
    table.drain_in_flight_maintenance().await?;
    assert!(
        table.promote_warm_to_cold().await?,
        "workload must promote to cold storage"
    );
    let count = ctx
        .sql("SELECT COUNT(*) FROM tap_rows")
        .await?
        .collect()
        .await?;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        5
    );
    common::write_batches(
        &table,
        vec![batch(vec![7, 8], vec![70, 80])?],
        InsertOp::Overwrite,
    )
    .await?;
    table.flush_pending_maintenance().await?;
    let count = ctx
        .sql("SELECT COUNT(*) FROM tap_rows")
        .await?
        .collect()
        .await?;
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0),
        2
    );
    exercise_catalog(&catalog, &dir.path().join("metadata"), &schema).await?;
    let table_id = catalog.get_table("tap_rows").await?.table_id;
    catalog.reserve_sequence_numbers(&table_id, 1).await?;
    // This count is captured before explicit verification: only verification
    // triggered by a subsequent write can have populated it.
    assert!(
        tap.report().verified_tables > 0,
        "per-commit verification did not run"
    );
    assert!(
        tap.report().verified_tables >= tap.report().commits - 1,
        "every preceding single-table commit must be verified: {:#?}",
        tap.report()
    );
    let report = catalog.tap_verify().await.expect("tap attached");
    println!(
        "tap differential broken={broken}: commits={} verified_tables={} differences={} apply_errors={} unconverted={}",
        report.commits,
        report.verified_tables,
        report.differences.len(),
        report.apply_errors.len(),
        report.unconverted.len()
    );
    println!("tap ops broken={broken}: {:?}", report.ops);
    assert!(report.apply_errors.is_empty(), "{report:#?}");
    assert!(report.unconverted.is_empty(), "{report:#?}");
    assert!(report.commits > 0);
    assert!(report.verified_tables > 0);
    let shadow = catalog.shadow_verify().await;
    println!("shadow report: {shadow:#?}");
    if shadow_enabled {
        let lines = std::fs::read_to_string(dir.path().join("tap.jsonl.shadow.jsonl"))?;
        let mut totals = std::collections::BTreeMap::new();
        let mut accepted = 0;
        let mut spilled = 0;
        for line in lines.lines() {
            let v: serde_json::Value = serde_json::from_str(line)?;
            for key in ["creates", "cas", "gets", "list", "deletes"] {
                assert!(v["requests"][key].is_u64(), "{v}");
            }
            spilled = spilled.max(v["spilled_bytes"].as_u64().expect("spilled bytes"));
            if v["outcome"] == "failed" {
                eprintln!("shadow commit failed: {}", v["error"]);
            }
            if v["outcome"] == "accepted" {
                // One drain can pack its records into several group commits.
                accepted += v["group_commits"].as_u64().expect("group commits");
                assert!(v["commit_latency_ms"].is_number(), "{v}");
                assert!(v["head_bytes"].as_u64().expect("head bytes") > 0);
            }
            totals.insert(
                v["table_id"].as_str().expect("table ID").to_string(),
                (
                    v["committed_metadata_bytes"]
                        .as_u64()
                        .expect("metadata bytes"),
                    v["checkpoint_bytes_written"]
                        .as_u64()
                        .expect("checkpoint bytes"),
                ),
            );
        }
        // A negative control's dropped payload makes a later shadow commit fail,
        // which disables that table's shadow; the counters then legitimately differ.
        assert!(spilled > 0, "no record was spilled to an object");
        if !shadow_negative {
            assert_eq!(accepted, shadow.commits);
            assert_eq!(
                totals.values().map(|(m, _)| m).sum::<u64>(),
                shadow.committed_metadata_bytes
            );
        }
        assert_eq!(
            totals.values().map(|(_, c)| c).sum::<u64>(),
            shadow.checkpoint_bytes_written
        );
        println!(
            "shadow JSONL: accepted={accepted} tables={} counters match report",
            totals.len()
        );
    }
    Ok((report, shadow))
}

#[tokio::test]
async fn metastore_tap_differential_matches_sql() -> TestResult<()> {
    let report = workload(false).await?;
    assert!(report.differences.is_empty(), "{report:#?}");
    Ok(())
}

#[tokio::test]
async fn metastore_tap_differential_detects_broken_apply() -> TestResult<()> {
    let report = workload(true).await?;
    assert!(
        !report.differences.is_empty(),
        "negative control did not detect missing deletion vectors: {report:#?}"
    );
    Ok(())
}

/// Exercise catalog-only metadata operations whose payloads need not be valid
/// Vortex/Arrow files: their oracle is the independently executed SQL readback.
#[expect(
    clippy::too_many_lines,
    reason = "sequential catalog differential cases"
)]
async fn exercise_catalog(
    catalog: &CayenneCatalog,
    root: &std::path::Path,
    schema: &Arc<Schema>,
) -> TestResult<()> {
    use cayenne::metadata::{
        ColdTierFile, IndexRunRecord, InlinedData, InlinedDelete, PartitionMetadata, SnapshotFile,
        SnapshotFileStatistics, TableStatistics,
    };
    let id = catalog
        .create_table(CreateTableOptions {
            table_name: "metadata_cases".to_string(),
            schema: Arc::clone(schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: root.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await?;
    let _ = catalog.shadow_verify().await;
    let snapshot = uuid::Uuid::now_v7().to_string();
    let other = uuid::Uuid::now_v7().to_string();
    let partition = PartitionMetadata {
        partition_id: String::new(),
        table_id: id.clone(),
        partition_columns: vec!["id".to_string()],
        partition_values: vec!["1".to_string()],
        path: "part".to_string(),
        path_is_relative: true,
        record_count: 1,
        file_size_bytes: 2,
    };
    let partition_id = catalog.add_partition(partition.clone()).await?;
    let _ = catalog.shadow_verify().await;
    assert_eq!(catalog.add_partition(partition).await?, partition_id);
    for ndv in [None, Some(vec![]), Some(vec![1, 2])] {
        catalog
            .upsert_table_statistics(&TableStatistics {
                table_id: id.clone(),
                statistics_blob: vec![3],
                num_rows: 4,
                num_rows_exact: false,
                ndv_sketches: ndv,
            })
            .await?;
        let _ = catalog.shadow_verify().await;
    }
    catalog.clear_table_statistics(&id).await?;
    let _ = catalog.shadow_verify().await;
    let file = |snapshot: &str| SnapshotFile {
        table_id: id.clone(),
        snapshot_id: snapshot.to_string(),
        file_path: "file.vortex".to_string(),
        row_count: 2,
        file_size_bytes: 3,
        min_sequence: 1,
        max_sequence: 2,
        digest: Some("digest".to_string()),
    };
    let stat = |snapshot: &str| SnapshotFileStatistics {
        table_id: id.clone(),
        snapshot_id: snapshot.to_string(),
        file_path: "file.vortex".to_string(),
        file_size_bytes: 3,
        num_rows: 2,
        statistics_blob: vec![4, 5],
    };
    for snap in [&snapshot, &other] {
        catalog.upsert_snapshot_file(&file(snap)).await?;
        let _ = catalog.shadow_verify().await;
        catalog.upsert_snapshot_file_statistics(&stat(snap)).await?;
        let _ = catalog.shadow_verify().await;
    }
    catalog.clear_snapshot_files_except(&id, &snapshot).await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .clear_snapshot_file_statistics_except(&id, &snapshot)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .clear_snapshot_cached_metadata(&id, &snapshot)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .replace_snapshot_files(&id, &snapshot, &[file(&snapshot)])
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog.replace_snapshot_files(&id, &snapshot, &[]).await?;
    let _ = catalog.shadow_verify().await;
    catalog.clear_snapshot_file_statistics(&id).await?;
    let _ = catalog.shadow_verify().await;
    catalog.clear_snapshot_files(&id).await?;
    let _ = catalog.shadow_verify().await;
    catalog.upsert_pk_index(&id, &snapshot, &[9]).await?;
    let _ = catalog.shadow_verify().await;
    catalog.clear_pk_index(&id).await?;
    let _ = catalog.shadow_verify().await;
    for count in [2, u64::MAX] {
        catalog
            .register_index_run(&IndexRunRecord {
                table_id: id.clone(),
                index_key: "index".to_string(),
                run_name: "run".to_string(),
                row_count: count,
                size_bytes: count,
            })
            .await?;
        let _ = catalog.shadow_verify().await;
    }
    catalog.remove_index_run(&id, "index", "run").await?;
    let _ = catalog.shadow_verify().await;
    let inline = || InlinedData {
        inlined_id: String::new(),
        table_id: id.clone(),
        partition_key: Some("partition".to_string()),
        data_ipc: vec![1, 2],
        record_count: 1,
        sequence_number: 1,
        created_at: String::new(),
    };
    let inline_id = catalog.add_inlined_data(inline()).await?;
    let _ = catalog.shadow_verify().await;
    let mut updated = inline();
    updated.inlined_id.clone_from(&inline_id);
    updated.data_ipc = vec![3];
    catalog
        .commit_delete_files_with_inlined_rewrite(
            vec![],
            &id,
            vec![updated.clone()],
            vec![],
            vec![],
        )
        .await?;
    let _ = catalog.shadow_verify().await;
    let mut missing = updated.clone();
    missing.inlined_id = "absent".to_string();
    assert!(
        catalog
            .commit_delete_files_with_inlined_rewrite(vec![], &id, vec![missing], vec![], vec![])
            .await
            .is_err()
    );
    catalog
        .commit_inlined_mutation(&id, vec![updated], vec![], vec![inline()], 2)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .commit_inlined_mutation(&id, vec![], vec![inline_id], vec![], 2)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog.clear_inlined_data(&id).await?;
    let _ = catalog.shadow_verify().await;
    let tombstone = || InlinedDelete {
        inlined_id: String::new(),
        table_id: id.clone(),
        delete_ipc: vec![4],
        delete_count: 1,
        sequence_number: 3,
        published: false,
        created_at: String::new(),
    };
    let tombstone_id = catalog.add_inlined_delete(tombstone()).await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .mark_inlined_delete_published(&id, &tombstone_id)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .mark_inlined_delete_unpublished(&id, &tombstone_id)
        .await?;
    let _ = catalog.shadow_verify().await;
    assert_eq!(catalog.publish_orphan_inlined_deletes(&id).await?, 1);
    assert_eq!(catalog.publish_orphan_inlined_deletes(&id).await?, 0);
    catalog.clear_inlined_deletes(&id).await?;
    let _ = catalog.shadow_verify().await;
    catalog.add_inlined_data(inline()).await?;
    let _ = catalog.shadow_verify().await;
    catalog.add_inlined_delete(tombstone()).await?;
    let _ = catalog.shadow_verify().await;
    catalog.clear_inlined_data_and_deletes(&id).await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .commit_on_conflict_deletions(vec![], &id, vec![], 4, None)
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .commit_on_conflict_deletions_and_clear_inlined(vec![], &id, vec![], 4, None)
        .await?;
    let _ = catalog.shadow_verify().await;
    let tombstone_id = catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![],
            &id,
            vec![],
            4,
            None,
            Some(tombstone()),
            &[],
        )
        .await?
        .expect("tombstone id");
    catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![],
            &id,
            vec![],
            4,
            None,
            None,
            &[tombstone_id],
        )
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .commit_overwrite_to_cold(
            &id,
            &snapshot,
            &[ColdTierFile {
                table_id: id.clone(),
                file_url: "file://cold.vortex".to_string(),
                row_count: 1,
                file_size_bytes: 2,
                min_sequence: 1,
                max_sequence: 4,
                statistics_blob: vec![5],
                pk_bloom: Some(vec![]),
            }],
        )
        .await?;
    let _ = catalog.shadow_verify().await;
    catalog
        .update_table_schema_dropping_statistics(&id, schema)
        .await?;
    let _ = catalog.shadow_verify().await;
    assert!(catalog.drop_table("metadata_cases").await?);
    assert!(!catalog.drop_table("metadata_cases").await?);
    Ok(())
}

#[tokio::test]
async fn metastore_shadow_memory_matches_sql() -> TestResult<()> {
    let (_, report) = workload_with_shadow(
        false,
        Some((Arc::new(object_store::memory::InMemory::new()), false)),
    )
    .await?;
    assert_shadow(&report);
    Ok(())
}

#[tokio::test]
async fn metastore_shadow_local_matches_sql() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let store = Arc::new(object_store_occ::LocalConditionalPut::new_durable(
        dir.path(),
    )?);
    let (_, report) = workload_with_shadow(false, Some((store, false))).await?;
    assert_shadow(&report);
    Ok(())
}

fn assert_shadow(report: &cayenne::metastore_tap::ShadowReport) {
    assert!(report.differences.is_empty(), "{report:#?}");
    assert!(report.failures.is_empty(), "{report:#?}");
    assert!(report.verified_tables >= 2, "{report:#?}");
    assert!(report.checkpoints > 0, "{report:#?}");
    assert!(report.published_checkpoints > 0, "{report:#?}");
    assert_eq!(
        report.checkpoints, report.verified_checkpoints,
        "{report:#?}"
    );
    assert_eq!(report.checkpoint_mismatches, 0, "{report:#?}");
}

#[tokio::test]
async fn metastore_shadow_detects_dropped_payload() -> TestResult<()> {
    let (_, report) = workload_with_shadow(
        false,
        Some((Arc::new(object_store::memory::InMemory::new()), true)),
    )
    .await?;
    // Dropping the first record with `AddDeleteFiles` drops its deletion
    // vectors and the group it registers. Depending on how the queued records
    // fall into group commits, the shadow sees that either at a checkpoint or
    // SQL comparison (missing `delete_files`), or when a later `RemoveGroups`
    // replaces the unregistered group and its guard refuses.
    assert!(
        report.differences.iter().any(|difference| {
            difference.contains("delete_files")
                || difference.contains("RemoveGroups on table")
                    && difference.contains("every replaced protected snapshot is still registered")
        }),
        "negative control: {report:#?}"
    );
    Ok(())
}
