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

//! Comprehensive tests for streaming compaction in Cayenne.
//!
//! Tests cover:
//! 1. `deleted_rows_count` accuracy for each deletion strategy
//! 2. `should_compact` threshold detection (deleted rows + bytes written)
//! 3. `streaming_compact` data correctness, snapshot changes, cache clearing
//! 4. Restart detection: reopened tables load deletion caches from catalog
//! 5. Auto-compaction triggered via insert when thresholds are met
//! 6. Sequential compactions produce correct results

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    COMPACTION_DELETED_ROWS_THRESHOLD,
};
use common::TestFixture;
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_int64_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_no_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn setup_int64_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = create_int64_pk_schema();
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProvider::create_table(catalog, table_options).await?,
    ))
}

async fn setup_no_pk_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<Arc<CayenneTableProvider>> {
    let schema = create_no_pk_schema();
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    Ok(Arc::new(
        CayenneTableProvider::create_table(catalog, table_options).await?,
    ))
}

async fn insert_int64_pk_batch(
    table: &Arc<CayenneTableProvider>,
    ids: Vec<i64>,
    names: Vec<&str>,
    values: Vec<i64>,
) -> TestResult<u64> {
    let schema = create_int64_pk_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn insert_no_pk_batch(
    table: &Arc<CayenneTableProvider>,
    ids: Vec<i64>,
    values: Vec<i64>,
) -> TestResult<u64> {
    let schema = create_no_pk_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch)
        .await
        .map_err(Into::into)
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), &[filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
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

async fn query_ids(table: &Arc<CayenneTableProvider>, table_name: &str) -> TestResult<Vec<i64>> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
    let df = ctx
        .sql(&format!("SELECT id FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;
    let ids: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    Ok(ids)
}

async fn query_values(table: &Arc<CayenneTableProvider>, table_name: &str) -> TestResult<Vec<i64>> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
    let df = ctx
        .sql(&format!("SELECT value FROM {table_name} ORDER BY value"))
        .await?;
    let results = df.collect().await?;
    let values: Vec<i64> = results
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column")
                .values()
                .iter()
                .copied()
        })
        .collect();
    Ok(values)
}

async fn query_row_count(table: &Arc<CayenneTableProvider>, table_name: &str) -> TestResult<i64> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(table) as Arc<dyn TableProvider>)?;
    let df = ctx
        .sql(&format!("SELECT COUNT(*) FROM {table_name}"))
        .await?;
    let results = df.collect().await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
}

// =============================================================================
// Test: deleted_rows_count accuracy with Int64 PK strategy
// =============================================================================
test_with_backends!(test_deleted_rows_count_int64_pk_impl);

async fn test_deleted_rows_count_int64_pk_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "deleted_count_i64").await?;

    // Initially no deletions
    assert_eq!(table.deleted_rows_count()?, 0, "no deletions initially");

    // Insert 5 rows
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;

    // Still no deletions
    assert_eq!(table.deleted_rows_count()?, 0, "no deletions after insert");

    // Delete 2 rows
    delete_records(&table, col("id").lt(lit(3i64))).await?;
    assert_eq!(
        table.deleted_rows_count()?,
        2,
        "should count 2 deleted rows"
    );

    // Delete 1 more row
    delete_records(&table, col("id").eq(lit(4i64))).await?;
    assert_eq!(
        table.deleted_rows_count()?,
        3,
        "should count 3 deleted rows cumulatively"
    );

    Ok(())
}

// =============================================================================
// Test: deleted_rows_count accuracy with position-based (no PK) strategy
// =============================================================================
test_with_backends!(test_deleted_rows_count_position_based_impl);

async fn test_deleted_rows_count_position_based_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_no_pk_table(&fixture, "deleted_count_pos").await?;

    // Insert 5 rows
    insert_no_pk_batch(&table, vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]).await?;

    assert_eq!(table.deleted_rows_count()?, 0, "no deletions initially");

    // Delete rows with value < 30
    delete_records(&table, col("value").lt(lit(30i64))).await?;
    assert_eq!(
        table.deleted_rows_count()?,
        2,
        "should count 2 deleted rows (position-based)"
    );

    Ok(())
}

// =============================================================================
// Test: should_compact returns false when below all thresholds
// =============================================================================
test_with_backends!(test_should_compact_below_thresholds_impl);

async fn test_should_compact_below_thresholds_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "compact_below").await?;

    // Insert and delete a small number of rows
    insert_int64_pk_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]).await?;
    delete_records(&table, col("id").eq(lit(1i64))).await?;

    assert_eq!(table.deleted_rows_count()?, 1);
    assert!(
        !table.should_compact()?,
        "should NOT compact with only 1 deleted row (threshold: {})",
        COMPACTION_DELETED_ROWS_THRESHOLD
    );

    Ok(())
}

// =============================================================================
// Test: streaming_compact data correctness (Int64 PK)
// =============================================================================
test_with_backends!(test_streaming_compact_data_correctness_int64_pk_impl);

async fn test_streaming_compact_data_correctness_int64_pk_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "compact_correct_i64").await?;

    // Insert 5 rows
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;

    // Delete rows 2 and 4
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;

    // Verify pre-compaction query
    let ids_before = query_ids(&table, "compact_correct_i64_pre").await?;
    assert_eq!(ids_before, vec![1, 3, 5], "pre-compaction query");

    // Run streaming compaction
    let snapshot_before = table.get_current_snapshot_id()?;
    table.streaming_compact().await?;

    // Verify post-compaction query returns the same correct results
    let ids_after = query_ids(&table, "compact_correct_i64_post").await?;
    assert_eq!(
        ids_after, ids_before,
        "post-compaction data must match pre-compaction data"
    );

    // Verify snapshot changed
    let snapshot_after = table.get_current_snapshot_id()?;
    assert_ne!(
        snapshot_before, snapshot_after,
        "snapshot ID should change after compaction"
    );

    // Verify deletion caches are cleared
    assert_eq!(
        table.deleted_rows_count()?,
        0,
        "deletion caches should be cleared after compaction"
    );

    // Verify should_compact returns false now
    assert!(
        !table.should_compact()?,
        "should_compact should return false after compaction"
    );

    // Verify delete files are cleared from catalog
    let delete_files = table
        .catalog()
        .get_table_delete_files(table.metadata().table_id)
        .await?;
    assert!(
        delete_files.is_empty(),
        "catalog should have no delete files after compaction"
    );

    Ok(())
}

// =============================================================================
// Test: streaming_compact data correctness (position-based, no PK)
// =============================================================================
test_with_backends!(test_streaming_compact_data_correctness_position_based_impl);

async fn test_streaming_compact_data_correctness_position_based_impl(
    fixture: TestFixture,
) -> TestResult<()> {
    let table = setup_no_pk_table(&fixture, "compact_correct_pos").await?;

    // Insert 5 rows
    insert_no_pk_batch(&table, vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]).await?;

    // Delete rows with value < 30 (ids 1, 2)
    delete_records(&table, col("value").lt(lit(30i64))).await?;

    // Verify pre-compaction query
    let values_before = query_values(&table, "compact_correct_pos_pre").await?;
    assert_eq!(values_before, vec![30, 40, 50], "pre-compaction query");

    // Run streaming compaction
    table.streaming_compact().await?;

    // Verify post-compaction query
    let values_after = query_values(&table, "compact_correct_pos_post").await?;
    assert_eq!(
        values_after, values_before,
        "post-compaction data must match pre-compaction data"
    );

    // Verify caches cleared
    assert_eq!(table.deleted_rows_count()?, 0);

    Ok(())
}

// =============================================================================
// Test: streaming_compact with no pending deletions (no-op behavior)
// =============================================================================
test_with_backends!(test_streaming_compact_no_deletions_impl);

async fn test_streaming_compact_no_deletions_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "compact_no_del").await?;

    insert_int64_pk_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]).await?;

    let snapshot_before = table.get_current_snapshot_id()?;
    let count_before = query_row_count(&table, "compact_no_del_pre").await?;

    // Compact with no deletions - should still work (rewrites data to new snapshot)
    table.streaming_compact().await?;

    let count_after = query_row_count(&table, "compact_no_del_post").await?;
    assert_eq!(count_before, count_after, "row count should be preserved");

    let snapshot_after = table.get_current_snapshot_id()?;
    assert_ne!(
        snapshot_before, snapshot_after,
        "snapshot should still change even with no deletions"
    );

    Ok(())
}

// =============================================================================
// Test: insert after streaming compaction produces correct results
// =============================================================================
test_with_backends!(test_insert_after_compaction_impl);

async fn test_insert_after_compaction_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "insert_after_compact").await?;

    // Insert, delete, compact
    insert_int64_pk_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]).await?;
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    table.streaming_compact().await?;

    // Insert new rows after compaction
    insert_int64_pk_batch(&table, vec![4, 5], vec!["d", "e"], vec![40, 50]).await?;

    let ids = query_ids(&table, "insert_after_compact_q").await?;
    assert_eq!(ids, vec![1, 3, 4, 5], "should include original + new rows");

    Ok(())
}

// =============================================================================
// Test: delete after streaming compaction works correctly
// =============================================================================
test_with_backends!(test_delete_after_compaction_impl);

async fn test_delete_after_compaction_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "delete_after_compact").await?;

    // Insert, delete, compact
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    table.streaming_compact().await?;

    // Delete more rows after compaction
    delete_records(&table, col("id").eq(lit(4i64))).await?;
    assert_eq!(
        table.deleted_rows_count()?,
        1,
        "should have new deletion after compaction"
    );

    let ids = query_ids(&table, "delete_after_compact_q").await?;
    assert_eq!(ids, vec![1, 3, 5]);

    Ok(())
}

// =============================================================================
// Test: sequential compactions produce correct results
// =============================================================================
test_with_backends!(test_sequential_compactions_impl);

async fn test_sequential_compactions_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "seq_compact").await?;

    // Round 1: insert, delete, compact
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    let snapshot_1 = table.get_current_snapshot_id()?;
    table.streaming_compact().await?;
    let snapshot_2 = table.get_current_snapshot_id()?;
    assert_ne!(snapshot_1, snapshot_2);

    let ids_after_1 = query_ids(&table, "seq_compact_r1").await?;
    assert_eq!(ids_after_1, vec![2, 3, 4, 5]);

    // Round 2: insert more, delete, compact again
    insert_int64_pk_batch(&table, vec![6, 7], vec!["f", "g"], vec![60, 70]).await?;
    delete_records(&table, col("id").eq(lit(3i64))).await?;
    delete_records(&table, col("id").eq(lit(5i64))).await?;
    table.streaming_compact().await?;
    let snapshot_3 = table.get_current_snapshot_id()?;
    assert_ne!(snapshot_2, snapshot_3);

    let ids_after_2 = query_ids(&table, "seq_compact_r2").await?;
    assert_eq!(ids_after_2, vec![2, 4, 6, 7]);

    // Round 3: compact with no new deletions (all data clean)
    table.streaming_compact().await?;
    let ids_after_3 = query_ids(&table, "seq_compact_r3").await?;
    assert_eq!(
        ids_after_3, ids_after_2,
        "compaction with no deletions should preserve data"
    );

    Ok(())
}

// =============================================================================
// Test: restart detection — reopened table has deletion caches populated
// =============================================================================
test_with_backends!(test_restart_detection_caches_populated_impl);

async fn test_restart_detection_caches_populated_impl(fixture: TestFixture) -> TestResult<()> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_name = "restart_detect";

    // Create table, insert, delete
    let table = setup_int64_pk_table(&fixture, table_name).await?;
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;

    let deleted_before_restart = table.deleted_rows_count()?;
    assert_eq!(deleted_before_restart, 2);

    // Verify delete files exist in catalog
    let delete_files = table
        .catalog()
        .get_table_delete_files(table.metadata().table_id)
        .await?;
    assert!(
        !delete_files.is_empty(),
        "catalog should have delete files before restart"
    );

    // Drop original table and reopen (simulates process restart)
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );

    // Verify deletion caches are populated from catalog after "restart"
    let deleted_after_restart = reopened.deleted_rows_count()?;
    assert_eq!(
        deleted_after_restart, deleted_before_restart,
        "reopened table should have same deletion count as before restart"
    );

    // Verify query returns correct data
    let ids = query_ids(&reopened, "restart_detect_q").await?;
    assert_eq!(ids, vec![1, 3, 5], "data should be correct after restart");

    Ok(())
}

// =============================================================================
// Test: compaction after restart — reopened table can compact successfully
// =============================================================================
test_with_backends!(test_compaction_after_restart_impl);

async fn test_compaction_after_restart_impl(fixture: TestFixture) -> TestResult<()> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_name = "compact_restart";

    // Create table, insert, delete
    let table = setup_int64_pk_table(&fixture, table_name).await?;
    insert_int64_pk_batch(
        &table,
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![10, 20, 30, 40, 50],
    )
    .await?;
    delete_records(&table, col("id").eq(lit(2i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;

    // Verify data before restart
    let ids_before = query_ids(&table, "compact_restart_pre").await?;
    assert_eq!(ids_before, vec![1, 3, 5]);

    // Simulate restart: drop original, reopen
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog))
            .open(table_name)
            .await?,
    );

    // Compaction should work on the reopened table
    assert_eq!(
        reopened.deleted_rows_count()?,
        2,
        "reopened table should detect 2 pending deletions from catalog"
    );
    reopened.streaming_compact().await?;

    // Verify data is correct after compaction
    let ids_after = query_ids(&reopened, "compact_restart_post").await?;
    assert_eq!(
        ids_after, ids_before,
        "data must be correct after restart + compaction"
    );

    // Verify caches cleared
    assert_eq!(reopened.deleted_rows_count()?, 0);

    // Verify delete files cleared from catalog
    let delete_files = reopened
        .catalog()
        .get_table_delete_files(reopened.metadata().table_id)
        .await?;
    assert!(
        delete_files.is_empty(),
        "catalog should have no delete files after compaction"
    );

    // Verify a second reopen also sees clean state
    drop(reopened);
    let reopened2 = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );
    assert_eq!(
        reopened2.deleted_rows_count()?,
        0,
        "second reopen should see clean state"
    );
    let ids_final = query_ids(&reopened2, "compact_restart_final").await?;
    assert_eq!(ids_final, vec![1, 3, 5]);

    Ok(())
}

// =============================================================================
// Test: should_compact detects deletions loaded from catalog after restart
// =============================================================================
test_with_backends!(test_should_compact_after_restart_impl);

async fn test_should_compact_after_restart_impl(fixture: TestFixture) -> TestResult<()> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_name = "should_compact_restart";

    let table = setup_int64_pk_table(&fixture, table_name).await?;

    // Insert rows and delete a small number (below compaction threshold)
    insert_int64_pk_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]).await?;
    delete_records(&table, col("id").eq(lit(1i64))).await?;

    assert_eq!(table.deleted_rows_count()?, 1);
    assert!(
        !table.should_compact()?,
        "should NOT compact with only 1 deletion (threshold: {COMPACTION_DELETED_ROWS_THRESHOLD})"
    );

    // Simulate restart
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );

    // After restart, deleted_rows_count should still be 1
    assert_eq!(
        reopened.deleted_rows_count()?,
        1,
        "reopened table should detect 1 deletion from catalog"
    );

    // should_compact should still be false (below threshold)
    assert!(
        !reopened.should_compact()?,
        "should NOT compact after restart with 1 deletion"
    );

    Ok(())
}

// =============================================================================
// Test: compact all rows deleted produces empty table
// =============================================================================
test_with_backends!(test_compact_all_rows_deleted_impl);

async fn test_compact_all_rows_deleted_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "compact_all_del").await?;

    insert_int64_pk_batch(&table, vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]).await?;

    // Delete all rows
    delete_records(&table, col("id").gt(lit(0i64))).await?;
    assert_eq!(table.deleted_rows_count()?, 3);

    // Verify empty before compaction
    let count_before = query_row_count(&table, "compact_all_del_pre").await?;
    assert_eq!(count_before, 0);

    // Compact
    table.streaming_compact().await?;

    // Should still be empty after compaction
    let count_after = query_row_count(&table, "compact_all_del_post").await?;
    assert_eq!(count_after, 0);
    assert_eq!(table.deleted_rows_count()?, 0);

    // Insert new data after compacting to empty
    insert_int64_pk_batch(&table, vec![10, 20], vec!["x", "y"], vec![100, 200]).await?;
    let ids = query_ids(&table, "compact_all_del_new").await?;
    assert_eq!(
        ids,
        vec![10, 20],
        "should be able to insert after compacting to empty"
    );

    Ok(())
}

// =============================================================================
// Test: compaction with position-based deletions after restart
// =============================================================================
test_with_backends!(test_position_based_compaction_after_restart_impl);

async fn test_position_based_compaction_after_restart_impl(fixture: TestFixture) -> TestResult<()> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table_name = "pos_compact_restart";

    let table = setup_no_pk_table(&fixture, table_name).await?;

    insert_no_pk_batch(&table, vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]).await?;

    // Delete rows with value < 30
    delete_records(&table, col("value").lt(lit(30i64))).await?;
    assert_eq!(table.deleted_rows_count()?, 2);

    let values_before = query_values(&table, "pos_compact_restart_pre").await?;
    assert_eq!(values_before, vec![30, 40, 50]);

    // Simulate restart
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog)
            .open(table_name)
            .await?,
    );

    // After restart, deletion caches should be populated from catalog
    assert_eq!(
        reopened.deleted_rows_count()?,
        2,
        "position-based deletions should be loaded from catalog after restart"
    );

    // Compact after restart
    reopened.streaming_compact().await?;

    // Verify data is correct
    let values_after = query_values(&reopened, "pos_compact_restart_post").await?;
    assert_eq!(values_after, vec![30, 40, 50]);
    assert_eq!(reopened.deleted_rows_count()?, 0);

    Ok(())
}

// =============================================================================
// Test: compaction preserves data across multiple insert batches
// =============================================================================
test_with_backends!(test_compact_multiple_batches_impl);

async fn test_compact_multiple_batches_impl(fixture: TestFixture) -> TestResult<()> {
    let table = setup_int64_pk_table(&fixture, "compact_multi_batch").await?;

    // Insert in multiple batches
    insert_int64_pk_batch(&table, vec![1, 2], vec!["a", "b"], vec![10, 20]).await?;
    insert_int64_pk_batch(&table, vec![3, 4], vec!["c", "d"], vec![30, 40]).await?;
    insert_int64_pk_batch(&table, vec![5, 6], vec!["e", "f"], vec![50, 60]).await?;

    // Delete from different batches
    delete_records(&table, col("id").eq(lit(1i64))).await?;
    delete_records(&table, col("id").eq(lit(4i64))).await?;
    delete_records(&table, col("id").eq(lit(6i64))).await?;

    let ids_before = query_ids(&table, "compact_multi_batch_pre").await?;
    assert_eq!(ids_before, vec![2, 3, 5]);

    // Compact
    table.streaming_compact().await?;

    let ids_after = query_ids(&table, "compact_multi_batch_post").await?;
    assert_eq!(ids_after, vec![2, 3, 5]);
    assert_eq!(table.deleted_rows_count()?, 0);

    Ok(())
}

// =============================================================================
// Test: constants have expected values
// =============================================================================
#[test]
fn test_compaction_constants() {
    assert_eq!(
        COMPACTION_DELETED_ROWS_THRESHOLD, 10_000,
        "COMPACTION_DELETED_ROWS_THRESHOLD should be 10,000"
    );
}
