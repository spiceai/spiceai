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

//! Regression tests for write-through acceleration data duplication in distributed mode.
//!
//! In distributed mode (scheduler + executors), write-through accelerated tables had a
//! bug where the Full refresh task on each executor would race with write-through inserts.
//! After a write-through insert wrote data to both the shared federated source (Iceberg)
//! and the local accelerator (Cayenne), the Full refresh would then read ALL data from
//! the federated source and overwrite the local accelerator with it. Since the federated
//! source contains data from ALL executors (not just this one's partition), each executor's
//! accelerator ended up with the complete dataset instead of just its partition, causing
//! queries to return 2x the expected data.
//!
//! The fix: write-through tables skip the refresh task entirely, since write-through
//! already keeps the accelerator in sync with the federated source directly.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use runtime::accelerated_table::AcceleratedTable;
use runtime::accelerated_table::refresh::Refresh;
use runtime::component::dataset::acceleration::RefreshMode;
use runtime::federated_table::FederatedTable;
use runtime::status;
use tokio::runtime::Handle;

/// Helper to create a Cayenne table provider backed by a temp directory.
/// Each call uses a unique subdirectory so multiple tables don't conflict.
async fn create_cayenne_table(
    schema: Arc<Schema>,
    base_dir: &std::path::Path,
    table_name: &str,
) -> CayenneTableProvider {
    let table_dir = base_dir.join(table_name);
    let cayenne_dir = table_dir.join("cayenne");
    let metadata_db = table_dir.join("metadata.db");
    std::fs::create_dir_all(&cayenne_dir).expect("create cayenne dir");

    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: cayenne_dir.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("create cayenne catalog"));
    catalog.init().await.expect("init cayenne catalog");
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

    let ctx = SessionContext::new();
    CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env())
        .await
        .expect("create cayenne table")
}

/// Count the total number of rows in a table provider by scanning it.
async fn count_rows(provider: &Arc<dyn TableProvider>) -> usize {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan should succeed");
    let batches = collect(plan, ctx.task_ctx())
        .await
        .expect("collect should succeed");
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn test_batch(ids: &[i64], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .expect("create test batch")
}

/// Build a write-through AcceleratedTable backed by a Cayenne accelerator.
async fn build_write_through_table(
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
    table_name: &str,
) -> AcceleratedTable {
    let runtime_status = status::RuntimeStatus::new();
    let refresh = Refresh::new(RefreshMode::Full);
    let mut builder = AcceleratedTable::builder(
        runtime_status,
        TableReference::bare(table_name),
        federated,
        "test_source".to_string(),
        accelerator,
        refresh,
        Handle::current(),
    );
    builder.write_through();
    builder
        .build()
        .await
        .expect("build accelerated table should succeed")
}

/// Regression test: write-through accelerated tables must NOT start a refresh task.
///
/// Without the fix, the Full refresh loads data from the federated source into the
/// local accelerator, duplicating data that write-through already wrote directly.
/// In distributed mode with 2 executors, this causes each executor to hold ALL data
/// instead of just its partition, resulting in 2x rows on query.
#[tokio::test]
#[cfg(not(target_os = "windows"))]
async fn test_write_through_acceleration_does_not_refresh_from_federated() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let schema = test_schema();

    // Federated source has 6 rows — simulates the shared Iceberg table that
    // contains data from ALL executors (e.g. 3 rows from executor1 + 3 from executor2).
    let federated_batch = test_batch(&[1, 2, 3, 4, 5, 6], &[10, 20, 30, 40, 50, 60]);
    let federated_mem = MemTable::try_new(Arc::clone(&schema), vec![vec![federated_batch]])
        .expect("create federated MemTable");
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::new(federated_mem)));

    // Create an empty Cayenne accelerator — simulates the executor's local acceleration
    // before any write-through insert has occurred.
    let cayenne =
        create_cayenne_table(Arc::clone(&schema), temp_dir.path(), "no_refresh").await;
    let accelerator: Arc<dyn TableProvider> = Arc::new(cayenne);

    let table = build_write_through_table(
        Arc::clone(&accelerator),
        federated,
        "no_refresh",
    )
    .await;

    // Give the refresh task time to run if it was started.
    // The refresh task uses Duration::ZERO delay for initial load, so 2 seconds
    // is more than enough for it to complete if it was started.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify: the local accelerator must be EMPTY.
    //
    // With the bug (refresh running), the accelerator would contain all 6 rows
    // from the federated source. In a real distributed scenario, this means each
    // executor's accelerator has data from ALL executors, and a UNION query across
    // both executors returns 12 rows instead of 6.
    //
    // With the fix (refresh skipped for write-through), the accelerator stays empty
    // because write-through will populate it directly during INSERT operations.
    let row_count = count_rows(&accelerator).await;
    assert_eq!(
        row_count, 0,
        "Write-through accelerator should be empty (refresh must not run). \
         Found {row_count} rows — the Full refresh loaded federated data into the \
         accelerator, which causes data duplication in distributed mode."
    );

    // Verify initial_load_completed is true (table is immediately ready).
    assert!(
        table.refresher().initial_load_completed(),
        "Write-through table should be immediately ready without waiting for refresh"
    );

    drop(table);
}

/// End-to-end regression test: simulates the distributed scenario where two executors
/// each have a write-through accelerated table sharing the same federated source.
///
/// After inserting each executor's partition via write-through, verifies that:
/// 1. Each executor's local accelerator contains ONLY its partition (not all data)
/// 2. The federated source contains the combined data from both executors
/// 3. The total row count across both accelerators equals the source count (no duplication)
///
/// Without the fix, the refresh task on each executor would load the full federated
/// dataset into each local accelerator, and a distributed query unioning both
/// executors would return 2x the expected rows.
#[tokio::test]
#[cfg(not(target_os = "windows"))]
async fn test_write_through_distributed_no_duplication() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let schema = test_schema();

    // Create a shared MemTable as the federated source (simulates shared Iceberg).
    // Start empty — the write-through inserts will populate it.
    let shared_federated_mem =
        MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("create shared MemTable");
    let shared_federated_provider: Arc<dyn TableProvider> = Arc::new(shared_federated_mem);

    // Executor 1: owns partition with ids 1-3
    let cayenne_exec1 =
        create_cayenne_table(Arc::clone(&schema), temp_dir.path(), "exec1").await;
    let accel_exec1: Arc<dyn TableProvider> = Arc::new(cayenne_exec1);
    let federated_exec1 =
        Arc::new(FederatedTable::new_unchecked(Arc::clone(&shared_federated_provider)));
    let table_exec1 = build_write_through_table(
        Arc::clone(&accel_exec1),
        federated_exec1,
        "exec1",
    )
    .await;

    // Executor 2: owns partition with ids 4-6
    let cayenne_exec2 =
        create_cayenne_table(Arc::clone(&schema), temp_dir.path(), "exec2").await;
    let accel_exec2: Arc<dyn TableProvider> = Arc::new(cayenne_exec2);
    let federated_exec2 =
        Arc::new(FederatedTable::new_unchecked(Arc::clone(&shared_federated_provider)));
    let table_exec2 = build_write_through_table(
        Arc::clone(&accel_exec2),
        federated_exec2,
        "exec2",
    )
    .await;

    // Allow time for any background refresh to complete (should not run).
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Both accelerators should start empty (no refresh loaded data).
    assert_eq!(
        count_rows(&accel_exec1).await,
        0,
        "Executor 1 accelerator should be empty before any inserts"
    );
    assert_eq!(
        count_rows(&accel_exec2).await,
        0,
        "Executor 2 accelerator should be empty before any inserts"
    );

    // Simulate distributed INSERT: scheduler routes partition data to each executor.
    // Each executor's write-through writes to both its local Cayenne and the shared
    // federated source.
    //
    // Register each executor's AcceleratedTable in a SessionContext so we can use
    // SQL INSERT to exercise the full write-through pipeline.
    let ctx_exec1 = SessionContext::new();
    ctx_exec1
        .register_table("wt_table", Arc::new(table_exec1))
        .expect("register exec1 table");

    let ctx_exec2 = SessionContext::new();
    ctx_exec2
        .register_table("wt_table", Arc::new(table_exec2))
        .expect("register exec2 table");

    // Executor 1 receives and writes its partition (ids 1-3).
    ctx_exec1
        .sql("INSERT INTO wt_table (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("plan insert for executor 1")
        .collect()
        .await
        .expect("write-through insert for executor 1 should succeed");

    // Executor 2 receives and writes its partition (ids 4-6).
    ctx_exec2
        .sql("INSERT INTO wt_table (id, value) VALUES (4, 40), (5, 50), (6, 60)")
        .await
        .expect("plan insert for executor 2")
        .collect()
        .await
        .expect("write-through insert for executor 2 should succeed");

    // Allow time for any background refresh that might have been started.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify: each executor's accelerator has ONLY its own partition.
    let exec1_rows = count_rows(&accel_exec1).await;
    let exec2_rows = count_rows(&accel_exec2).await;

    assert_eq!(
        exec1_rows, 3,
        "Executor 1 accelerator should contain exactly 3 rows (its partition). \
         Found {exec1_rows} — if > 3, the refresh loaded other executors' data."
    );
    assert_eq!(
        exec2_rows, 3,
        "Executor 2 accelerator should contain exactly 3 rows (its partition). \
         Found {exec2_rows} — if > 3, the refresh loaded other executors' data."
    );

    // Verify: the shared federated source has ALL 6 rows (both partitions).
    let federated_rows = count_rows(&shared_federated_provider).await;
    assert_eq!(
        federated_rows, 6,
        "Shared federated source should contain all 6 rows from both executors. \
         Found {federated_rows}."
    );

    // The key assertion: a distributed query unions both executors and should
    // return exactly 6 rows (no duplication). Without the fix, each accelerator
    // would have all 6 rows (from refresh), and the union would return 12.
    let total_from_accelerators = exec1_rows + exec2_rows;
    assert_eq!(
        total_from_accelerators, 6,
        "Total rows across both accelerators should equal source row count (6). \
         Got {total_from_accelerators} — this means a distributed UNION ALL query \
         would return duplicates."
    );
}
