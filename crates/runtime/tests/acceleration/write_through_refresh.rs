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

//! Regression test for write-through acceleration data duplication in distributed mode.
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
//!
//! This test verifies that a write-through AcceleratedTable does NOT run its refresh
//! task by building one with a pre-populated federated source and confirming that the
//! local accelerator remains empty (i.e., refresh did not load federated data into it).

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
async fn create_cayenne_table(
    schema: Arc<Schema>,
    temp_dir: &tempfile::TempDir,
    table_name: &str,
) -> CayenneTableProvider {
    let cayenne_dir = temp_dir.path().join("cayenne");
    let metadata_db = temp_dir.path().join("metadata.db");
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
    let cayenne = create_cayenne_table(Arc::clone(&schema), &temp_dir, "test_wt").await;
    let accelerator: Arc<dyn TableProvider> = Arc::new(cayenne);

    // Build the AcceleratedTable with write-through + Full refresh.
    let runtime_status = status::RuntimeStatus::new();
    let refresh = Refresh::new(RefreshMode::Full);
    let mut builder = AcceleratedTable::builder(
        runtime_status,
        TableReference::bare("test_wt"),
        federated,
        "test_source".to_string(),
        Arc::clone(&accelerator),
        refresh,
        Handle::current(),
    );
    builder.write_through();
    let table = builder
        .build()
        .await
        .expect("build accelerated table should succeed");

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
