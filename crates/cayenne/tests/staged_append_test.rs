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

//! Tests for staged append writes.
//!
//! Validates that append writes go through a `_staging/` directory and that
//! partial writes from stream errors are cleaned up without polluting the
//! active snapshot.

#![allow(clippy::expect_used)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use cayenne::metadata::CreateTableOptions;
use cayenne::{
    CayenneStagedAppend, CayenneTableProvider, MetadataCatalog, PreparedStagedAppend,
    STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME,
};

use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

// ============================================================================
// Test 1: Basic staged append — data correct, staging empty after write
// ============================================================================

test_with_backends!(test_staged_append_basic_impl);

async fn test_staged_append_basic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_basic").await;

    ctx.sql("INSERT INTO staged_basic VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;

    let rows = query_all(&ctx, "staged_basic").await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Charlie".to_string()),
        ]
    );

    assert_staging_empty(&staging_dir(&table));

    Ok(())
}

test_with_backends!(test_cdc_stage_a_does_not_wait_for_prior_finalize_impl);

async fn test_cdc_stage_a_does_not_wait_for_prior_finalize_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let vortex_config = cayenne::metadata::VortexConfig {
        inline_max_rows: 0,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };
    let (table, ctx) =
        setup_table_with_vortex_config(&fixture, "cdc_stage_a_overlap", vortex_config).await;
    let task_ctx = ctx.task_ctx();

    let first = table
        .write_cdc_append_stream(batch_stream(make_batch(&[1, 2], &["A", "B"])), &task_ctx)
        .await?;
    assert!(
        first.has_pending_finalize(),
        "first CDC write should return after Stage A with Stage B pending"
    );

    let second = tokio::time::timeout(
        Duration::from_secs(2),
        table.write_cdc_append_stream(batch_stream(make_batch(&[3, 4], &["C", "D"])), &task_ctx),
    )
    .await
    .expect("second Stage A should not wait for first Stage B")?;
    assert!(
        second.has_pending_finalize(),
        "second CDC write should also stage before finalizing"
    );

    first.finish().await?;
    second.finish().await?;

    let rows = query_all(&ctx, "cdc_stage_a_overlap").await;
    assert_eq!(
        rows,
        vec![
            (1, "A".to_string()),
            (2, "B".to_string()),
            (3, "C".to_string()),
            (4, "D".to_string()),
        ]
    );

    Ok(())
}

// ============================================================================
// Test 2: Stream error — partial writes cleaned up, no data corruption
// ============================================================================

test_with_backends!(test_staged_append_stream_error_no_partial_data_impl);

async fn test_staged_append_stream_error_no_partial_data_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_err").await;

    // Insert baseline data
    ctx.sql("INSERT INTO staged_err VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .await?
        .collect()
        .await?;

    assert_eq!(row_count(&ctx, "staged_err").await, 3);

    // Build a stream that yields 2 batches then errors
    let schema = test_schema();
    let batch1 = make_batch(&[10, 11], &["X", "Y"]);
    let batch2 = make_batch(&[12, 13], &["Z", "W"]);

    let items: Vec<datafusion_common::Result<RecordBatch>> = vec![
        Ok(batch1),
        Ok(batch2),
        Err(DataFusionError::Execution(
            "simulated stream error".to_string(),
        )),
    ];
    let failing_stream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(items),
    ));

    let input = Arc::new(FailingStreamExec::new(Arc::clone(&schema), failing_stream));

    let insert_plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await?;

    let result = collect(insert_plan, ctx.task_ctx()).await;
    assert!(result.is_err(), "Expected stream error to propagate");

    // Verify: only original 3 rows remain — no partial data from failed write
    let rows = query_all(&ctx, "staged_err").await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Charlie".to_string()),
        ],
        "Partial data from failed stream should not be visible"
    );

    // Verify: staging dir is clean
    assert_staging_empty(&staging_dir(&table));

    Ok(())
}

// ============================================================================
// Test 3: Self-healing — leftover files in _staging/ cleaned on next append
// ============================================================================

test_with_backends!(test_staged_append_self_healing_leftover_impl);

async fn test_staged_append_self_healing_leftover_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_heal").await;

    // Insert initial data to ensure table dirs exist
    ctx.sql("INSERT INTO staged_heal VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Manually plant a leftover file in _staging/ (simulates crash)
    let staging = staging_dir(&table);
    std::fs::create_dir_all(&staging)?;
    std::fs::write(staging.join("leftover.vortex"), b"fake leftover data")?;
    assert!(staging.join("leftover.vortex").exists());

    // Next append should clear _staging/ first (self-healing)
    ctx.sql("INSERT INTO staged_heal VALUES (2, 'Bob')")
        .await?
        .collect()
        .await?;

    // Leftover is gone
    assert!(!staging.join("leftover.vortex").exists());
    assert_staging_empty(&staging);

    // Only real data is queryable
    let rows = query_all(&ctx, "staged_heal").await;
    assert_eq!(
        rows,
        vec![(1, "Alice".to_string()), (2, "Bob".to_string()),]
    );

    Ok(())
}

// ============================================================================
// Test 4: Multiple appends accumulate correctly
// ============================================================================

test_with_backends!(test_staged_append_multi_append_accumulates_impl);

async fn test_staged_append_multi_append_accumulates_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "staged_multi").await;
    let staging = staging_dir(&table);

    // Append 1
    ctx.sql("INSERT INTO staged_multi VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 3);
    assert_staging_empty(&staging);

    // Append 2
    ctx.sql("INSERT INTO staged_multi VALUES (4, 'D'), (5, 'E'), (6, 'F')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 6);
    assert_staging_empty(&staging);

    // Append 3
    ctx.sql("INSERT INTO staged_multi VALUES (7, 'G'), (8, 'H'), (9, 'I')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "staged_multi").await, 9);
    assert_staging_empty(&staging);

    // Verify all data
    let rows = query_all(&ctx, "staged_multi").await;
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0], (1, "A".to_string()));
    assert_eq!(rows[8], (9, "I".to_string()));

    Ok(())
}

// ============================================================================
// Test 5: WAL presence blocks table construction (open)
// ============================================================================

test_with_backends!(test_wal_blocks_table_open_impl);

async fn test_wal_blocks_table_open_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_open").await;

    // Insert data so table dirs exist
    ctx.sql("INSERT INTO wal_open VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Plant a fake WAL file in an isolated staging dir to simulate interrupted move
    let wal_content = serde_json::json!({
        "table_name": "wal_open",
        "target_snapshot": "fake_snapshot_id",
        "staged_files": ["part-0.vortex", "part-1.vortex"],
        "created_at": "2026-02-28T00:00:00Z"
    });
    let wal_path = write_manual_staging_wal(&table, "manual-open", &wal_content)?;
    assert!(wal_path.exists());

    // Try to re-open the table — should fail with IncompleteWrite
    let meta = table.metadata();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let open_result = cayenne::CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .open(&meta.table_name)
        .await;

    assert!(
        open_result.is_err(),
        "Opening a table with a leftover WAL should fail"
    );

    Ok(())
}

// ============================================================================
// Test 6: WAL presence blocks new writes
// ============================================================================

test_with_backends!(test_wal_blocks_new_writes_impl);

async fn test_wal_blocks_new_writes_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_write").await;

    // Insert initial data
    ctx.sql("INSERT INTO wal_write VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "wal_write").await, 1);

    // Plant a WAL file in an isolated staging dir
    let wal_content = serde_json::json!({
        "table_name": "wal_write",
        "target_snapshot": "fake_snapshot_id",
        "staged_files": ["part-0.vortex"],
        "created_at": "2026-02-28T00:00:00Z"
    });
    write_manual_staging_wal(&table, "manual-write", &wal_content)?;

    // Attempt another write — should fail
    let result = ctx
        .sql("INSERT INTO wal_write VALUES (2, 'Bob')")
        .await?
        .collect()
        .await;

    assert!(result.is_err(), "Write with a leftover WAL should fail");

    // Original data should still be intact
    assert_eq!(row_count(&ctx, "wal_write").await, 1);

    Ok(())
}

// ============================================================================
// Test 7: Successful append removes WAL file
// ============================================================================

test_with_backends!(test_wal_removed_on_successful_append_impl);

async fn test_wal_removed_on_successful_append_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_lifecycle").await;

    // Perform a successful append
    ctx.sql("INSERT INTO wal_lifecycle VALUES (1, 'Alice'), (2, 'Bob')")
        .await?
        .collect()
        .await?;

    // After successful write, WAL must NOT exist
    let staging = staging_dir(&table);
    assert!(
        staging_wal_paths(&table).is_empty(),
        "WAL file should be removed after successful append"
    );

    // Staging dir should be empty
    assert_staging_empty(&staging);

    Ok(())
}

// ============================================================================
// Test 8: Corrupted snapshot dir causes move failure — WAL persists
// ============================================================================

test_with_backends!(test_wal_persists_on_move_failure_impl);

async fn test_wal_persists_on_move_failure_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_move_fail").await;

    // Step 1: Insert initial data so snapshot directory exists
    ctx.sql("INSERT INTO wal_move_fail VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx, "wal_move_fail").await, 1);

    // Step 2: Corrupt the target snapshot directory — replace it with a regular file.
    //
    // This simulates a real filesystem corruption where the target snapshot
    // directory is unusable, causing the move phase to fail while the WAL
    // has already been written.
    let meta = table.metadata();
    let snapshot_dir = PathBuf::from(&meta.path)
        .join(&meta.table_id)
        .join(&meta.current_snapshot_id);
    std::fs::remove_dir_all(&snapshot_dir)?;
    std::fs::write(&snapshot_dir, b"not a directory")?;

    // Step 3: Attempt another insert — should fail during the move phase.
    // Build the batch directly (>1024 rows so the data-inlining fast-path is
    // bypassed and the write goes through the Vortex staging WAL path). Using
    // a RecordBatch instead of a large `INSERT ... VALUES (...)` SQL string
    // avoids SQL parser / statement-size limits that would make the failure
    // assertion less meaningful.
    let ids: Vec<i64> = (2..1030).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("name_{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let schema = table.schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
        ],
    )?;
    let result = common::insert_batch(&table, batch).await;

    assert!(
        result.is_err(),
        "Insert should fail when snapshot directory is corrupted"
    );

    // Step 4: Verify WAL persists in an isolated staging dir after the failed move.
    let wal_paths = staging_wal_paths(&table);
    assert_eq!(
        wal_paths.len(),
        1,
        "WAL file should persist after a failed move — indicates incomplete write"
    );

    Ok(())
}

// ============================================================================
// Test 9 (issue #10125): prepare → apply_under_barrier → finish is observably
// equivalent to the legacy commit() path. The legacy commit() is now
// implemented on top of the new lifecycle, so passing this test exercises the
// parity guarantee.
// ============================================================================

test_with_backends!(test_prepared_lifecycle_matches_commit_impl);

async fn test_prepared_lifecycle_matches_commit_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "lifecycle_parity").await;

    // Drive the staged-append API directly, then walk the three-phase lifecycle.
    let staged = begin_staged_append_with_rows(&table, &[(1, "Alice"), (2, "Bob")]).await?;
    let staged_rows = staged.row_count();
    let wal_path = staged.staging_wal_path();

    let prepared: PreparedStagedAppend = staged.prepare().await?;

    // After prepare(), the WAL exists and the staged data is NOT yet visible.
    let staging = staging_dir(&table);
    assert!(wal_path.exists(), "prepare() must write the staging WAL");
    assert_eq!(
        row_count(&ctx, "lifecycle_parity").await,
        0,
        "staged data must not be visible before apply_under_barrier"
    );

    prepared.apply_under_barrier().await?;

    // After apply_under_barrier(), files are moved, the WAL is removed, and the
    // listing table is refreshed. Removing the WAL inside apply_under_barrier
    // preserves the invariant that "WAL absent ⇒ files moved successfully"; a
    // crash between WAL removal and listing refresh is self-healing.
    assert!(
        !wal_path.exists(),
        "apply_under_barrier() must remove the staging WAL"
    );
    assert_eq!(
        row_count(&ctx, "lifecycle_parity").await,
        2,
        "staged data must be visible after apply_under_barrier"
    );

    let returned = prepared.finish().await?;
    assert_eq!(returned, staged_rows);

    // Sanity: a follow-up insert via the existing path (which now flows through
    // the same lifecycle internally) lands on top correctly.
    ctx.sql("INSERT INTO lifecycle_parity VALUES (3, 'Charlie')")
        .await?
        .collect()
        .await?;

    let rows = query_all(&ctx, "lifecycle_parity").await;
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string()),
            (2, "Bob".to_string()),
            (3, "Charlie".to_string()),
        ]
    );
    assert_staging_empty(&staging);

    Ok(())
}

// ============================================================================
// Test 10 (issue #10125): rollback() on a PreparedStagedAppend clears the
// staging directory (including the WAL), so subsequent writes are not blocked.
// ============================================================================

test_with_backends!(test_prepared_rollback_clears_staging_impl);

async fn test_prepared_rollback_clears_staging_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "lifecycle_rollback").await;

    let staged = begin_staged_append_with_rows(&table, &[(10, "X"), (11, "Y")]).await?;
    let wal_path = staged.staging_wal_path();
    let prepared = staged.prepare().await?;
    assert!(wal_path.exists());

    prepared.rollback().await?;

    // Staging dir must be empty: no leftover files, no WAL.
    assert_staging_empty(&staging_dir(&table));

    // The table must remain writable — no IncompleteWrite block.
    ctx.sql("INSERT INTO lifecycle_rollback VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;
    let rows = query_all(&ctx, "lifecycle_rollback").await;
    assert_eq!(rows, vec![(1, "Alice".to_string())]);

    Ok(())
}

// ============================================================================
// Test 11: WAL appears atomically at the final path (no `_wal.json.tmp` left
// behind after a successful prepare). Regression: prior to the atomic
// rename + parent-dir fsync fix the WAL was written directly to its final
// path and a torn write would have left a partial `_wal.json`; we now write
// to `_wal.json.tmp` and rename, so the final path is either absent or a
// complete WAL document.
// ============================================================================

test_with_backends!(test_wal_atomic_appearance_impl);

async fn test_wal_atomic_appearance_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, _ctx) = setup_table(&fixture, "wal_atomic").await;

    let staged = begin_staged_append_with_rows(&table, &[(1, "Alice")]).await?;
    let final_path = staged.staging_wal_path();
    let staging = final_path
        .parent()
        .expect("WAL path has parent")
        .to_path_buf();
    let prepared = staged.prepare().await?;

    let tmp_path = staging.join(STAGING_WAL_TMP_FILENAME);

    assert!(
        final_path.exists(),
        "prepare() must publish the WAL at its final path"
    );
    assert!(
        !tmp_path.exists(),
        "prepare() must rename the tmp WAL away; a lingering `_wal.json.tmp` \
         indicates the atomic rename never ran"
    );

    // The published WAL must parse — never observe a partial document.
    let content = std::fs::read_to_string(&final_path).expect("read WAL");
    let parsed: serde_json::Value = serde_json::from_str(&content).expect("WAL must be valid JSON");
    assert_eq!(parsed["table_name"], "wal_atomic");
    assert!(
        parsed["staged_files"].as_array().is_some(),
        "WAL must contain staged_files: {parsed:?}"
    );

    prepared.rollback().await?;
    Ok(())
}

// ============================================================================
// Test 12: A bare `_wal.json.tmp` (no committed `_wal.json`) does NOT block
// new writes. The tmp is bookkeeping; only the renamed final file represents
// committed intent. Without this, a process killed between writing the tmp
// and the rename would leave the table permanently unwritable.
// ============================================================================

test_with_backends!(test_leftover_tmp_does_not_block_writes_impl);

async fn test_leftover_tmp_does_not_block_writes_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_tmp_only").await;

    ctx.sql("INSERT INTO wal_tmp_only VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Plant ONLY the tmp — never the committed WAL.
    let staging = staging_dir(&table);
    std::fs::create_dir_all(&staging)?;
    std::fs::write(
        staging.join(STAGING_WAL_TMP_FILENAME),
        b"{\"this\": \"is a partial write that crashed mid-fsync\"}",
    )?;
    assert!(staging.join(STAGING_WAL_TMP_FILENAME).exists());
    assert!(!staging.join(STAGING_WAL_FILENAME).exists());

    // The next write must succeed — the tmp was never promoted, so no
    // committed intent exists.
    ctx.sql("INSERT INTO wal_tmp_only VALUES (2, 'Bob')")
        .await?
        .collect()
        .await?;

    let rows = query_all(&ctx, "wal_tmp_only").await;
    assert_eq!(rows, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);

    Ok(())
}

// ============================================================================
// Test 13: A leftover `_wal.json.tmp` is never promoted into the snapshot.
// Without this guarantee, a crashed prior write could leave a non-vortex
// scratch file that move_files_to_current_snapshot would rename into the
// snapshot directory, corrupting the listing table's view of the snapshot.
// ============================================================================

test_with_backends!(test_leftover_tmp_not_moved_to_snapshot_impl);

async fn test_leftover_tmp_not_moved_to_snapshot_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_tmp_skip").await;

    ctx.sql("INSERT INTO wal_tmp_skip VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Begin a staged append, then plant a tmp before the commit phase walks
    // the staging dir. The tmp is junk that must be excluded from the move.
    let staged = begin_staged_append_with_rows(&table, &[(2, "Bob")]).await?;
    let staging = staged
        .staging_wal_path()
        .parent()
        .expect("WAL path has parent")
        .to_path_buf();
    std::fs::write(staging.join(STAGING_WAL_TMP_FILENAME), b"prior crashed tmp")?;
    staged.commit().await?;

    // Snapshot dir must NOT contain the tmp.
    let meta = table.metadata();
    let snapshot_dir = PathBuf::from(&meta.path)
        .join(&meta.table_id)
        .join(&meta.current_snapshot_id);
    let snapshot_entries: Vec<String> = std::fs::read_dir(&snapshot_dir)
        .expect("read snapshot dir")
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect();
    assert!(
        !snapshot_entries.contains(&STAGING_WAL_TMP_FILENAME.to_string()),
        "Leftover `_wal.json.tmp` was promoted into the snapshot dir: {snapshot_entries:?}"
    );

    let rows = query_all(&ctx, "wal_tmp_skip").await;
    assert_eq!(rows, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);

    Ok(())
}

// ============================================================================
// Test 14: A leftover `_wal.json.tmp` is not listed in the next WAL's
// `staged_files`. Otherwise we would record a non-data file as part of the
// commit intent, and a partial-recovery tool walking `staged_files` would
// trip over a path that doesn't exist (because move skips the tmp).
// ============================================================================

test_with_backends!(test_leftover_tmp_excluded_from_staged_files_impl);

async fn test_leftover_tmp_excluded_from_staged_files_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, _ctx) = setup_table(&fixture, "wal_tmp_excluded").await;

    // Stage some data, then plant a stray tmp before prepare()
    let staged =
        begin_staged_append_with_rows(&table, &[(1, "Alice"), (2, "Bob"), (3, "Carol")]).await?;
    let final_path = staged.staging_wal_path();
    let staging = final_path
        .parent()
        .expect("WAL path has parent")
        .to_path_buf();
    std::fs::write(staging.join(STAGING_WAL_TMP_FILENAME), b"junk")?;

    let prepared = staged.prepare().await?;

    let content = std::fs::read_to_string(&final_path).expect("read final WAL");
    let parsed: serde_json::Value = serde_json::from_str(&content).expect("WAL must parse");
    let files = parsed["staged_files"]
        .as_array()
        .expect("staged_files array");
    for file in files {
        let file_str = file.as_str().expect("string filename");
        assert_ne!(
            file_str, STAGING_WAL_TMP_FILENAME,
            "WAL's staged_files must not include `_wal.json.tmp`: {files:?}"
        );
        assert_ne!(
            file_str, STAGING_WAL_FILENAME,
            "WAL's staged_files must not include the WAL itself"
        );
    }

    prepared.rollback().await?;
    Ok(())
}

// ============================================================================
// Test 15: Repeated `write_staging_wal` calls atomically replace the prior
// WAL — no partial document is ever observable. Ensures the rename pattern
// upholds the "WAL is either absent or fully valid" invariant under repeated
// commit attempts.
// ============================================================================

test_with_backends!(test_repeated_wal_writes_are_atomic_impl);

async fn test_repeated_wal_writes_are_atomic_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_atomic_replace").await;

    // First insert — leaves no WAL behind.
    ctx.sql("INSERT INTO wal_atomic_replace VALUES (1, 'A')")
        .await?
        .collect()
        .await?;

    assert!(
        staging_wal_paths(&table).is_empty(),
        "WAL must not persist after a successful commit"
    );

    // Drive a second insert; after the prepare() the WAL exists and parses.
    let staged = begin_staged_append_with_rows(&table, &[(2, "B")]).await?;
    let first_wal_path = staged.staging_wal_path();
    let prepared = staged.prepare().await?;
    let first_content = std::fs::read_to_string(&first_wal_path).expect("read 1st WAL");
    serde_json::from_str::<serde_json::Value>(&first_content).expect("1st WAL parses");
    prepared.rollback().await?;

    // Drive a third staged append from scratch — the WAL must be a fresh,
    // valid document, not a half-overwritten remnant of the previous one.
    let staged = begin_staged_append_with_rows(&table, &[(3, "C"), (4, "D")]).await?;
    let second_wal_path = staged.staging_wal_path();
    let second_staging = second_wal_path
        .parent()
        .expect("WAL path has parent")
        .to_path_buf();
    let prepared = staged.prepare().await?;
    let second_content = std::fs::read_to_string(&second_wal_path).expect("read 2nd WAL");
    let parsed: serde_json::Value = serde_json::from_str(&second_content).expect("2nd WAL parses");
    assert_eq!(parsed["table_name"], "wal_atomic_replace");
    assert!(
        !second_staging.join(STAGING_WAL_TMP_FILENAME).exists(),
        "Tmp file must be renamed away by prepare()"
    );

    prepared.apply_under_barrier().await?;
    prepared.finish().await?;

    let rows = query_all(&ctx, "wal_atomic_replace").await;
    assert_eq!(
        rows,
        vec![
            (1, "A".to_string()),
            (3, "C".to_string()),
            (4, "D".to_string()),
        ]
    );

    Ok(())
}

// ============================================================================
// Test 16: ensure_no_incomplete_write audits WAL-listed files before
// auto-recovery. A WAL that names files which exist in neither `_staging/`
// nor the current snapshot directory indicates genuine data loss
// (filesystem corruption or external interference); the recovery code MUST
// refuse to swallow the WAL silently, since doing so would allow writes to
// resume against a snapshot state that has lost rows the user once
// committed.
//
// Regression: an earlier iteration of automated recovery would call
// `move_files_to_current_snapshot()` regardless of what the WAL listed,
// treat a no-op move as success, and unlink the WAL — turning genuine
// corruption into a silent loss event. The audit re-establishes the
// "WAL exists ⇒ writes block ⇒ operator investigates" contract for the
// corruption case, while still self-healing the benign "crash between
// rename and WAL removal" case (covered by other tests).
// ============================================================================

test_with_backends!(test_wal_with_missing_files_blocks_recovery_impl);

async fn test_wal_with_missing_files_blocks_recovery_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_corrupt").await;

    // Establish a snapshot directory by performing a clean insert.
    ctx.sql("INSERT INTO wal_corrupt VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;

    // Plant a WAL that references files that exist nowhere on disk —
    // simulates the "filesystem corruption that lost staged files" scenario.
    let wal_content = serde_json::json!({
        "table_name": "wal_corrupt",
        "target_snapshot": "missing_snapshot_id",
        "staged_files": ["part-000.vortex", "part-001.vortex"],
        "created_at": "2026-03-01T12:00:00Z"
    });
    write_manual_staging_wal(&table, "manual-corrupt", &wal_content)?;

    // Attempt a fresh write — the audit must refuse to silently recover
    // the corrupt WAL, so the write fails.
    let result = ctx
        .sql("INSERT INTO wal_corrupt VALUES (2, 'Bob')")
        .await?
        .collect()
        .await;
    assert!(
        result.is_err(),
        "audit must refuse silent recovery when the WAL references files \
         missing from both staging and the current snapshot — otherwise we \
         lose the previously-committed contents of those files"
    );

    // The original row must still be visible — the audit must not have
    // disturbed live data, only blocked the corrupt-WAL recovery.
    let rows = query_all(&ctx, "wal_corrupt").await;
    assert_eq!(rows, vec![(1, "Alice".to_string())]);

    Ok(())
}

// ============================================================================
// Test 17: Auto-recovery proceeds (does not error) when the WAL lists
// files that are all already in the current snapshot — i.e., the prior
// commit's move loop completed but the WAL removal step did not. The
// audit must recognise this benign case and let recovery unlink the WAL.
// ============================================================================

test_with_backends!(test_wal_with_files_in_snapshot_self_heals_impl);

async fn test_wal_with_files_in_snapshot_self_heals_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "wal_benign").await;

    // Force the write through the Vortex-file path (bypassing the inline
    // memtable's <INLINE_MAX_ROWS fast path) by inserting a large batch
    // directly. After this, the current snapshot directory holds real
    // `.vortex` files we can reference in a stale WAL.
    let large_rows: i64 = 2000;
    let ids: Vec<i64> = (1..=large_rows).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("n_{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let schema = table.schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
        ],
    )?;
    common::insert_batch(&table, batch).await?;

    let meta = table.metadata();
    let snapshot_dir = PathBuf::from(&meta.path)
        .join(&meta.table_id)
        .join(&meta.current_snapshot_id);
    let vortex_files: Vec<String> = std::fs::read_dir(&snapshot_dir)?
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".vortex") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(
        !vortex_files.is_empty(),
        "test setup requires at least one Vortex file in the snapshot \
         after the large batch insert; got an inline-only write instead"
    );

    // Plant a WAL referencing those (already-moved) files. Staging is
    // empty — the audit should still recognise the files in the snapshot
    // and let recovery unlink the stale WAL.
    let wal_content = serde_json::json!({
        "table_name": "wal_benign",
        "target_snapshot": &meta.current_snapshot_id,
        "staged_files": &vortex_files,
        "created_at": "2026-03-01T12:00:00Z"
    });
    let wal_path = write_manual_staging_wal(&table, "manual-benign", &wal_content)?;

    // A subsequent staged write must succeed — recovery removes the stale
    // WAL because the audit verifies every WAL-listed file is reachable in
    // the snapshot directory. Use begin_staged_append to drive through the
    // ensure_no_incomplete_write path on the staging side.
    let staged = begin_staged_append_with_rows(&table, &[(9001, "Z")]).await?;
    staged.commit().await?;

    assert!(
        !wal_path.exists(),
        "auto-recovery must unlink the stale WAL once it has verified that \
         all listed files are accounted for in the snapshot"
    );

    let total = row_count(&ctx, "wal_benign").await;
    assert_eq!(
        total,
        usize::try_from(large_rows).expect("row count fits") + 1
    );

    Ok(())
}

// ============================================================================
// Test 18: Writer with pending staging WAL while inline compaction runs.
// This exercises the mutation writer + compaction interaction under the
// new pre-recovery audit. A writer that has written its WAL but not yet
// moved the files must not lose data when compaction commits a new snapshot
// and potentially triggers old snapshot cleanup.
// ============================================================================

test_with_backends!(test_writer_wal_survives_inline_compaction_impl);

async fn test_writer_wal_survives_inline_compaction_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use aggressive compaction config so a moderate write triggers compaction.
    let (table, ctx) = setup_table_with_compaction(&fixture, "writer_compact").await;

    // Large write that goes through staging + WAL (bypasses inline memtable).
    let large_rows: i64 = 5000;
    let ids: Vec<i64> = (1..=large_rows).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("n_{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let schema = table.schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
        ],
    )?;

    // Begin staged append (writes the WAL) but do not commit yet.
    let staged = begin_staged_append_with_batch(&table, batch).await?;

    // While the WAL is pending, explicitly trigger compaction.
    // This may create a new snapshot and schedule old snapshot cleanup.
    let _compacted = table.maybe_compact_small_files().await?;

    // Now let the writer finish (move files + remove WAL).
    // The move should target the *current* live snapshot (whatever compaction left),
    // and the pre-recovery audit (if the WAL is seen as stale) must not
    // refuse a benign pending writer.
    staged.commit().await?;

    // Data must be present after the writer completes.
    let total = row_count(&ctx, "writer_compact").await;
    assert_eq!(total, usize::try_from(large_rows).expect("row count fits"));

    // No leftover WAL.
    assert!(
        staging_wal_paths(&table).is_empty(),
        "writer's WAL must be removed after successful commit across compaction boundary"
    );

    Ok(())
}

// ============================================================================
// Test 19: Writer with pending staging WAL while compaction is triggered.
// Verifies that a writer that has written its WAL can still successfully
// commit after compaction has run (the move targets the live snapshot and
// the pre-recovery audit does not incorrectly refuse a benign pending WAL).
// ============================================================================

test_with_backends!(test_pending_writer_wal_survives_compaction_trigger_impl);

async fn test_pending_writer_wal_survives_compaction_trigger_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (table, ctx) = setup_table(&fixture, "writer_compact_race").await;

    // Perform a staged append (writes a WAL). Use enough rows to ensure
    // the write goes through the staging path.
    let staged = begin_staged_append_with_rows(&table, &[(1, "A"), (2, "B"), (3, "C")]).await?;

    // Explicitly trigger compaction while the writer's WAL is pending.
    // This exercises the writer + compaction interaction and ensures the
    // pre-recovery audit / move logic does not break a benign pending writer
    // when the snapshot pointer moves.
    let _ = table.maybe_compact_small_files().await?;

    // The writer must still be able to commit successfully.
    staged.commit().await?;

    let total = row_count(&ctx, "writer_compact_race").await;
    assert_eq!(total, 3);

    Ok(())
}

// ============================================================================
// S3-specific regression test for pre-recovery audit with partial upload
// ============================================================================

// Test that the S3 pre-recovery audit (list-based) correctly refuses
// automated recovery when a WAL references a file that is "missing"
// (simulating a partial multipart upload that was never completed).
//
// This is the key S3 edge case for the new pre-recovery audit + automated
// recovery feature. The test uses an `InMemory` object store to simulate S3.
// The S3 pre-recovery audit path is symmetric to the local-FS path tested
// directly in `test_wal_with_missing_files_blocks_recovery_impl` (Test 16).
// A full S3 mocked recovery test would require wiring an in-memory object
// store through the CayenneTableProvider builder; that is left as a
// follow-up. The S3 audit code is exercised at runtime via integration
// tests that use a real or in-memory store and call
// `ensure_no_incomplete_write` after a partial commit.

// ---------------------------------------------------------------------------
// Minimal ExecutionPlan that wraps a SendableRecordBatchStream — used to
// inject a failing stream into `insert_into` without depending on cayenne's
// `pub(crate)` StreamingExec.
// ---------------------------------------------------------------------------

struct FailingStreamExec {
    schema: SchemaRef,
    stream: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    properties: PlanProperties,
}

impl FailingStreamExec {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            schema,
            stream: std::sync::Mutex::new(Some(stream)),
            properties,
        }
    }
}

impl std::fmt::Debug for FailingStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingStreamExec").finish()
    }
}

impl DisplayAs for FailingStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FailingStreamExec")
    }
}

impl ExecutionPlan for FailingStreamExec {
    fn name(&self) -> &'static str {
        "FailingStreamExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self
            .stream
            .lock()
            .map_err(|e| DataFusionError::Execution(format!("Stream lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| DataFusionError::Execution("Stream already consumed".to_string()))?;
        Ok(stream)
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("valid batch")
}

/// Build the `_staging/` directory path for a table.
fn staging_dir(table: &CayenneTableProvider) -> PathBuf {
    let meta = table.metadata();
    PathBuf::from(&meta.path)
        .join(&meta.table_id)
        .join(STAGING_DIR_NAME)
}

fn staging_child_dir(table: &CayenneTableProvider, child: &str) -> PathBuf {
    staging_dir(table).join(child)
}

fn write_manual_staging_wal(
    table: &CayenneTableProvider,
    child: &str,
    wal_content: &serde_json::Value,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let staging = staging_child_dir(table, child);
    std::fs::create_dir_all(&staging)?;
    let wal_path = staging.join(STAGING_WAL_FILENAME);
    std::fs::write(&wal_path, serde_json::to_string_pretty(wal_content)?)?;
    Ok(wal_path)
}

fn staging_wal_paths(table: &CayenneTableProvider) -> Vec<PathBuf> {
    let root = staging_dir(table);
    if !root.exists() {
        return Vec::new();
    }

    std::fs::read_dir(root)
        .expect("read staging dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            let wal_path = entry.path().join(STAGING_WAL_FILENAME);
            wal_path.exists().then_some(wal_path)
        })
        .collect()
}

/// Assert that `_staging/` is empty (no files).
fn assert_staging_empty(staging: &std::path::Path) {
    if !staging.exists() {
        return; // non-existent is fine — means nothing was left behind
    }
    let entries: Vec<_> = std::fs::read_dir(staging)
        .expect("read staging dir")
        .collect();
    assert!(
        entries.is_empty(),
        "Expected _staging/ to be empty but found {} entries",
        entries.len()
    );
}

/// Query total row count from a registered table.
async fn row_count(ctx: &SessionContext, table_name: &str) -> usize {
    let df = ctx
        .sql(&format!("SELECT * FROM {table_name}"))
        .await
        .expect("query");
    let results = df.collect().await.expect("collect");
    results.iter().map(RecordBatch::num_rows).sum()
}

/// Query all (id, name) pairs ordered by id.
async fn query_all(ctx: &SessionContext, table_name: &str) -> Vec<(i64, String)> {
    let df = ctx
        .sql(&format!("SELECT id, name FROM {table_name} ORDER BY id"))
        .await
        .expect("query");
    let batches = df.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows
}

/// Create a table and register it with a `SessionContext`.
async fn setup_table(
    fixture: &common::TestFixture,
    table_name: &str,
) -> (Arc<CayenneTableProvider>, SessionContext) {
    setup_table_with_vortex_config(
        fixture,
        table_name,
        cayenne::metadata::VortexConfig::default(),
    )
    .await
}

async fn setup_table_with_compaction(
    fixture: &common::TestFixture,
    table_name: &str,
) -> (Arc<CayenneTableProvider>, SessionContext) {
    let vortex_config = cayenne::metadata::VortexConfig {
        compaction_trigger_files: 2,
        compaction_max_levels: 1,
        compaction_max_files_per_pick: 2,
        compaction_background_interval_ms: 0,
        ..Default::default()
    };
    setup_table_with_vortex_config(fixture, table_name, vortex_config).await
}

async fn setup_table_with_vortex_config(
    fixture: &common::TestFixture,
    table_name: &str,
    vortex_config: cayenne::metadata::VortexConfig,
) -> (Arc<CayenneTableProvider>, SessionContext) {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: test_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env())
        .await
        .expect("create table");
    let table = Arc::new(table);

    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)
        .expect("register");

    (table, ctx)
}

async fn begin_staged_append_with_batch(
    table: &CayenneTableProvider,
    batch: RecordBatch,
) -> Result<CayenneStagedAppend, Box<dyn std::error::Error>> {
    let stream = batch_stream(batch);
    Ok(table.begin_staged_append(stream, 1).await?)
}

fn batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
    ))
}

/// Drive `CayenneTableProvider::begin_staged_append` with a fixed-shape batch
/// of `(id, name)` rows, returning the `CayenneStagedAppend` handle so the
/// caller can walk the three-phase lifecycle directly.
async fn begin_staged_append_with_rows(
    table: &CayenneTableProvider,
    rows: &[(i64, &str)],
) -> Result<CayenneStagedAppend, Box<dyn std::error::Error>> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let names: Vec<&str> = rows.iter().map(|(_, name)| *name).collect();
    let batch = make_batch(&ids, &names);
    let stream = batch_stream(batch);
    Ok(table.begin_staged_append(stream, 1).await?)
}
