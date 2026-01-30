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

//! Tests for concurrent operations on multiple Cayenne tables sharing the same metastore.
//!
//! This test reproduces the bug from <https://github.com/spiceai/spiceai/issues/8826>
//! where using two Cayenne accelerations with the same metastore causes
//! "Database is busy" errors during concurrent write operations.
//!
//! The bug manifested when:
//! - Two (or more) Cayenne tables share the same metastore backend
//! - Both tables perform write operations concurrently
//! - The shared metastore backend did not wait for write locks (no busy timeout)
//!
//! This was fixed by adding a busy timeout to the metastore connections.
//!
//! Expected behavior: Concurrent writes to different tables with a shared metastore
//! should succeed without "Database is busy" errors.

#![allow(clippy::expect_used)]

mod common;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use common::BackendType;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Barrier;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

// =============================================================================
// Macro for generating multi-threaded test variants for each backend
// =============================================================================

/// Macro to generate multi-threaded test variants for all backends.
/// Creates `{test_fn}_sqlite` and `{test_fn}_turso` variants.
macro_rules! test_with_backends_multithreaded {
    ($test_fn:ident) => {
        paste::paste! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn [<$test_fn _sqlite>]() -> TestResult<()> {
                $test_fn(BackendType::Sqlite).await
            }

            #[cfg(feature = "turso")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn [<$test_fn _turso>]() -> TestResult<()> {
                $test_fn(BackendType::Turso).await
            }
        }
    };
    ($test_fn:ident, workers = $workers:expr) => {
        paste::paste! {
            #[tokio::test(flavor = "multi_thread", worker_threads = $workers)]
            async fn [<$test_fn _sqlite>]() -> TestResult<()> {
                $test_fn(BackendType::Sqlite).await
            }

            #[cfg(feature = "turso")]
            #[tokio::test(flavor = "multi_thread", worker_threads = $workers)]
            async fn [<$test_fn _turso>]() -> TestResult<()> {
                $test_fn(BackendType::Turso).await
            }
        }
    };
}

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Returns the connection string prefix for the given backend type.
fn connection_string_for_backend(backend: BackendType, db_path: &std::path::Path) -> String {
    match backend {
        BackendType::Sqlite => format!("sqlite://{}", db_path.to_string_lossy()),
        #[cfg(feature = "turso")]
        BackendType::Turso => format!("libsql://{}", db_path.to_string_lossy()),
    }
}

/// Creates a shared metastore fixture for the given backend.
async fn create_shared_fixture(
    backend: BackendType,
) -> TestResult<(TempDir, Arc<CayenneCatalog>, std::path::PathBuf)> {
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let db_path = temp_dir.path().join("shared_metastore.db");
    let connection_string = connection_string_for_backend(backend, &db_path);

    let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
    catalog.init().await?;

    Ok((temp_dir, catalog, data_path))
}

/// Creates two separate catalog instances pointing to the same database file.
/// This simulates the actual production scenario where each dataset's acceleration
/// creates its own catalog instance.
async fn create_separate_catalogs(
    backend: BackendType,
) -> TestResult<(
    TempDir,
    Arc<CayenneCatalog>,
    Arc<CayenneCatalog>,
    std::path::PathBuf,
)> {
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let db_path = temp_dir.path().join("separate_catalogs.db");
    let connection_string = connection_string_for_backend(backend, &db_path);

    let catalog1 = Arc::new(CayenneCatalog::new(&connection_string)?);
    catalog1.init().await?;

    let catalog2 = Arc::new(CayenneCatalog::new(&connection_string)?);
    catalog2.init().await?;

    Ok((temp_dir, catalog1, catalog2, data_path))
}

// =============================================================================
// Test: Basic concurrent inserts with shared catalog
// =============================================================================

test_with_backends_multithreaded!(test_shared_metastore_concurrent_inserts);

/// Test concurrent inserts to two tables sharing the same metastore catalog.
async fn test_shared_metastore_concurrent_inserts(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog, data_path) = create_shared_fixture(backend).await?;
    let schema = create_test_schema();
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

    // Create first table
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "customer1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    // Create second table
    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "customer2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    // Create a shared session context with both tables
    let ctx = SessionContext::new();
    ctx.register_table("customer1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("customer2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    // Use a barrier to ensure both inserts start at exactly the same time.
    let barrier = Arc::new(Barrier::new(2));

    let ctx1 = ctx.clone();
    let barrier1 = Arc::clone(&barrier);
    let insert1 = tokio::spawn(async move {
        barrier1.wait().await;
        ctx1.sql("INSERT INTO customer1 VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
            .await?
            .collect()
            .await
    });

    let ctx2 = ctx.clone();
    let barrier2 = Arc::clone(&barrier);
    let insert2 = tokio::spawn(async move {
        barrier2.wait().await;
        ctx2.sql("INSERT INTO customer2 VALUES (1, 'Charlie', 300), (2, 'Diana', 400)")
            .await?
            .collect()
            .await
    });

    let (result1, result2) = tokio::join!(insert1, insert2);

    result1
        .expect("insert1 task panicked")
        .expect("insert1 failed");
    result2
        .expect("insert2 task panicked")
        .expect("insert2 failed");

    // Verify data in both tables
    let batches1 = ctx
        .sql("SELECT id, name, value FROM customer1 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches1.len(), 1);
    assert_eq!(batches1[0].num_rows(), 2);

    let batches2 = ctx
        .sql("SELECT id, name, value FROM customer2 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches2.len(), 1);
    assert_eq!(batches2[0].num_rows(), 2);

    Ok(())
}

// =============================================================================
// Test: Multiple rounds of concurrent inserts
// =============================================================================

test_with_backends_multithreaded!(test_multiple_concurrent_inserts);

/// Test multiple rounds of concurrent inserts to stress the shared metastore.
async fn test_multiple_concurrent_inserts(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog, data_path) = create_shared_fixture(backend).await?;
    let schema = create_test_schema();
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

    // Create tables
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "orders1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "orders2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table("orders1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("orders2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    // Perform multiple rounds of concurrent inserts with barriers
    for round in 0..10 {
        let base_id = round * 10;
        let ctx1 = ctx.clone();
        let ctx2 = ctx.clone();
        let barrier = Arc::new(Barrier::new(2));

        let sql1 = format!(
            "INSERT INTO orders1 VALUES ({}, 'Order1_{}', {}), ({}, 'Order1_{}', {})",
            base_id,
            base_id,
            base_id * 100,
            base_id + 1,
            base_id + 1,
            (base_id + 1) * 100
        );
        let sql2 = format!(
            "INSERT INTO orders2 VALUES ({}, 'Order2_{}', {}), ({}, 'Order2_{}', {})",
            base_id,
            base_id,
            base_id * 100,
            base_id + 1,
            base_id + 1,
            (base_id + 1) * 100
        );

        let barrier1 = Arc::clone(&barrier);
        let insert1 = tokio::spawn(async move {
            barrier1.wait().await;
            ctx1.sql(&sql1).await?.collect().await
        });

        let barrier2 = Arc::clone(&barrier);
        let insert2 = tokio::spawn(async move {
            barrier2.wait().await;
            ctx2.sql(&sql2).await?.collect().await
        });

        let (result1, result2) = tokio::join!(insert1, insert2);

        result1
            .expect("insert1 task panicked")
            .map_err(|e| format!("Round {round}: insert1 failed: {e}"))?;
        result2
            .expect("insert2 task panicked")
            .map_err(|e| format!("Round {round}: insert2 failed: {e}"))?;
    }

    // Verify final row counts: 10 rounds * 2 rows = 20 rows per table
    let count1: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM orders1")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    let count2: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM orders2")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    assert_eq!(count1, 20, "Expected 20 rows in orders1");
    assert_eq!(count2, 20, "Expected 20 rows in orders2");

    Ok(())
}

// =============================================================================
// Test: Separate sessions (simulating separate accelerations)
// =============================================================================

test_with_backends_multithreaded!(test_separate_sessions);

/// Test with separate `DataFusion` sessions simulating separate accelerations in a spicepod.
async fn test_separate_sessions(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog, data_path) = create_shared_fixture(backend).await?;
    let schema = create_test_schema();
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

    // Create two tables sharing the same catalog
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "products1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog_arc),
            CreateTableOptions {
                table_name: "products2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    // Use SEPARATE session contexts (simulating separate accelerations)
    let ctx1 = SessionContext::new();
    ctx1.register_table("products1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;

    let ctx2 = SessionContext::new();
    ctx2.register_table("products2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    let barrier = Arc::new(Barrier::new(2));

    let barrier1 = Arc::clone(&barrier);
    let insert1 = tokio::spawn(async move {
        barrier1.wait().await;
        ctx1.sql("INSERT INTO products1 VALUES (1, 'Widget', 1000), (2, 'Gadget', 2000)")
            .await?
            .collect()
            .await
    });

    let barrier2 = Arc::clone(&barrier);
    let insert2 = tokio::spawn(async move {
        barrier2.wait().await;
        ctx2.sql("INSERT INTO products2 VALUES (1, 'Sprocket', 3000), (2, 'Flange', 4000)")
            .await?
            .collect()
            .await
    });

    let (result1, result2) = tokio::join!(insert1, insert2);

    result1
        .expect("insert1 task panicked")
        .expect("insert1 failed - 'Database is busy' error should no longer occur");
    result2
        .expect("insert2 task panicked")
        .expect("insert2 failed - 'Database is busy' error should no longer occur");

    // Verify data
    let verify_ctx = SessionContext::new();
    verify_ctx.register_table("products1", table1 as Arc<dyn TableProvider>)?;
    verify_ctx.register_table("products2", table2 as Arc<dyn TableProvider>)?;

    let batches1 = verify_ctx
        .sql("SELECT * FROM products1 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches1[0].num_rows(), 2);

    let batches2 = verify_ctx
        .sql("SELECT * FROM products2 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches2[0].num_rows(), 2);

    Ok(())
}

// =============================================================================
// Test: Separate catalog instances pointing to same DB file
// This is the actual scenario that causes issue #8826
// =============================================================================

test_with_backends_multithreaded!(test_separate_catalog_instances_same_db);

/// Test that creates SEPARATE catalog instances pointing to the SAME database file.
/// This is the actual scenario that causes the "Database is busy" bug in production.
async fn test_separate_catalog_instances_same_db(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog1, catalog2, data_path) = create_separate_catalogs(backend).await?;
    let schema = create_test_schema();

    // Create table1 using catalog1
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog1) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "dataset1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    // Create table2 using catalog2 (separate catalog instance, same DB file)
    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "dataset2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let ctx1 = SessionContext::new();
    ctx1.register_table("dataset1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;

    let ctx2 = SessionContext::new();
    ctx2.register_table("dataset2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    let barrier = Arc::new(Barrier::new(2));

    let barrier1 = Arc::clone(&barrier);
    let insert1 = tokio::spawn(async move {
        barrier1.wait().await;
        ctx1.sql("INSERT INTO dataset1 VALUES (1, 'Row1', 100), (2, 'Row2', 200)")
            .await?
            .collect()
            .await
    });

    let barrier2 = Arc::clone(&barrier);
    let insert2 = tokio::spawn(async move {
        barrier2.wait().await;
        ctx2.sql("INSERT INTO dataset2 VALUES (1, 'Row1', 300), (2, 'Row2', 400)")
            .await?
            .collect()
            .await
    });

    let (result1, result2) = tokio::join!(insert1, insert2);

    result1.expect("insert1 task panicked").expect(
        "insert1 failed - concurrent write to shared metastore should succeed without SQLITE_BUSY",
    );
    result2.expect("insert2 task panicked").expect(
        "insert2 failed - concurrent write to shared metastore should succeed without SQLITE_BUSY",
    );

    // Verify data
    let verify_ctx = SessionContext::new();
    verify_ctx.register_table("dataset1", table1 as Arc<dyn TableProvider>)?;
    verify_ctx.register_table("dataset2", table2 as Arc<dyn TableProvider>)?;

    let batches1 = verify_ctx
        .sql("SELECT * FROM dataset1 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches1[0].num_rows(), 2);

    let batches2 = verify_ctx
        .sql("SELECT * FROM dataset2 ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(batches2[0].num_rows(), 2);

    Ok(())
}

// =============================================================================
// Test: Stress test with separate catalogs
// =============================================================================

test_with_backends_multithreaded!(test_separate_catalogs_stress);

/// Stress test with multiple rounds of concurrent inserts using separate catalog instances.
async fn test_separate_catalogs_stress(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog1, catalog2, data_path) = create_separate_catalogs(backend).await?;
    let schema = create_test_schema();

    // Create tables with separate catalogs
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog1) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "stress1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "stress2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table("stress1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("stress2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    // Perform many rounds of concurrent inserts with barriers
    for round in 0..20 {
        let base_id = round * 10;
        let ctx1 = ctx.clone();
        let ctx2 = ctx.clone();
        let barrier = Arc::new(Barrier::new(2));

        let sql1 = format!(
            "INSERT INTO stress1 VALUES ({}, 'S1_{}', {}), ({}, 'S1_{}', {})",
            base_id,
            base_id,
            base_id * 100,
            base_id + 1,
            base_id + 1,
            (base_id + 1) * 100
        );
        let sql2 = format!(
            "INSERT INTO stress2 VALUES ({}, 'S2_{}', {}), ({}, 'S2_{}', {})",
            base_id,
            base_id,
            base_id * 100,
            base_id + 1,
            base_id + 1,
            (base_id + 1) * 100
        );

        let barrier1 = Arc::clone(&barrier);
        let insert1 = tokio::spawn(async move {
            barrier1.wait().await;
            ctx1.sql(&sql1).await?.collect().await
        });

        let barrier2 = Arc::clone(&barrier);
        let insert2 = tokio::spawn(async move {
            barrier2.wait().await;
            ctx2.sql(&sql2).await?.collect().await
        });

        let (result1, result2) = tokio::join!(insert1, insert2);

        result1
            .expect("insert1 task panicked")
            .map_err(|e| format!("Round {round}: insert1 failed with: {e}"))?;
        result2
            .expect("insert2 task panicked")
            .map_err(|e| format!("Round {round}: insert2 failed with: {e}"))?;
    }

    // Verify final row counts: 20 rounds * 2 rows = 40 rows per table
    let count1: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM stress1")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    let count2: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM stress2")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    assert_eq!(count1, 40, "Expected 40 rows in stress1");
    assert_eq!(count2, 40, "Expected 40 rows in stress2");

    Ok(())
}

// =============================================================================
// Test: Highly concurrent inserts (maximum contention)
// =============================================================================

test_with_backends_multithreaded!(test_highly_concurrent_inserts, workers = 8);

const NUM_CONCURRENT_OPS: usize = 10;

/// Most aggressive concurrency test: launches many parallel insert operations at once.
async fn test_highly_concurrent_inserts(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog1, catalog2, data_path) = create_separate_catalogs(backend).await?;
    let schema = create_test_schema();

    // Create tables
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog1) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "highconc1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "highconc2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table("highconc1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("highconc2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    // Use a single barrier to synchronize ALL concurrent operations at once.
    let barrier = Arc::new(Barrier::new(NUM_CONCURRENT_OPS * 2));

    let mut handles = Vec::with_capacity(NUM_CONCURRENT_OPS * 2);

    // Launch concurrent inserts to table1
    for i in 0..NUM_CONCURRENT_OPS {
        let ctx_clone = ctx.clone();
        let barrier_clone = Arc::clone(&barrier);
        let base_id = i * 100;
        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;
            let sql = format!(
                "INSERT INTO highconc1 VALUES ({base_id}, 'T1_{base_id}', {v})",
                v = base_id * 10
            );
            ctx_clone.sql(&sql).await?.collect().await
        }));
    }

    // Launch concurrent inserts to table2
    for i in 0..NUM_CONCURRENT_OPS {
        let ctx_clone = ctx.clone();
        let barrier_clone = Arc::clone(&barrier);
        let base_id = i * 100;
        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;
            let sql = format!(
                "INSERT INTO highconc2 VALUES ({base_id}, 'T2_{base_id}', {v})",
                v = base_id * 10
            );
            ctx_clone.sql(&sql).await?.collect().await
        }));
    }

    // Wait for all operations to complete
    let results = futures::future::join_all(handles).await;

    // Check for any failures
    let mut errors = Vec::new();
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                errors.push(format!("Task {i} failed: {e}"));
            }
            Err(e) => {
                errors.push(format!("Task {i} panicked: {e}"));
            }
        }
    }

    if !errors.is_empty() {
        return Err(format!("Concurrent operations failed: {}", errors.join("; ")).into());
    }

    // Verify row counts
    let count1: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM highconc1")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    let count2: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM highconc2")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    assert_eq!(
        count1,
        i64::try_from(NUM_CONCURRENT_OPS)?,
        "Expected {NUM_CONCURRENT_OPS} rows in highconc1"
    );
    assert_eq!(
        count2,
        i64::try_from(NUM_CONCURRENT_OPS)?,
        "Expected {NUM_CONCURRENT_OPS} rows in highconc2"
    );

    Ok(())
}

// =============================================================================
// Test: Concurrent OVERWRITE operations (triggers commit_compaction transaction)
// This is the actual code path that causes the "Database is busy" bug in #8826
// =============================================================================

test_with_backends_multithreaded!(test_concurrent_overwrite_operations, workers = 8);

/// Test concurrent OVERWRITE operations which trigger the `commit_compaction` transaction.
/// This directly exercises the transaction path that causes "Database is busy" in issue #8826.
///
/// The bug occurred because:
/// 1. Each table's overwrite calls `commit_compaction` which runs:
///    `BEGIN TRANSACTION; DELETE...; DELETE...; UPDATE...; COMMIT;`
/// 2. The shared metastore backend did not wait for write locks (no busy timeout)
/// 3. When two concurrent transactions tried to write, one immediately failed with "Database is busy"
///
/// This was fixed by adding a busy timeout to the metastore connections.
async fn test_concurrent_overwrite_operations(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog1, catalog2, data_path) = create_separate_catalogs(backend).await?;
    let schema = create_test_schema();

    // Create tables
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog1) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "overwrite1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "overwrite2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    // Insert initial data
    let ctx = SessionContext::new();
    ctx.register_table("overwrite1", Arc::clone(&table1) as Arc<dyn TableProvider>)?;
    ctx.register_table("overwrite2", Arc::clone(&table2) as Arc<dyn TableProvider>)?;

    ctx.sql("INSERT INTO overwrite1 VALUES (1, 'Initial1', 100)")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO overwrite2 VALUES (1, 'Initial2', 200)")
        .await?
        .collect()
        .await?;

    // Now perform concurrent OVERWRITE operations
    // This uses INSERT OVERWRITE syntax which triggers commit_compaction
    let barrier = Arc::new(Barrier::new(2));

    let ctx1 = ctx.clone();
    let barrier1 = Arc::clone(&barrier);
    let overwrite1 = tokio::spawn(async move {
        barrier1.wait().await;
        ctx1.sql(
            "INSERT OVERWRITE overwrite1 VALUES (10, 'Overwrite1', 1000), (11, 'Overwrite1', 1100)",
        )
        .await?
        .collect()
        .await
    });

    let ctx2 = ctx.clone();
    let barrier2 = Arc::clone(&barrier);
    let overwrite2 = tokio::spawn(async move {
        barrier2.wait().await;
        ctx2.sql(
            "INSERT OVERWRITE overwrite2 VALUES (20, 'Overwrite2', 2000), (21, 'Overwrite2', 2100)",
        )
        .await?
        .collect()
        .await
    });

    let (result1, result2) = tokio::join!(overwrite1, overwrite2);

    result1.expect("overwrite1 task panicked").expect(
        "overwrite1 failed - unexpected 'Database is busy' (SQLITE_BUSY) regression of issue #8826",
    );
    result2.expect("overwrite2 task panicked").expect(
        "overwrite2 failed - unexpected 'Database is busy' (SQLITE_BUSY) regression of issue #8826",
    );

    // Verify each table has only the overwritten data (2 rows each)
    let count1: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM overwrite1")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    let count2: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM overwrite2")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    assert_eq!(count1, 2, "Expected 2 rows in overwrite1 after OVERWRITE");
    assert_eq!(count2, 2, "Expected 2 rows in overwrite2 after OVERWRITE");

    Ok(())
}

// =============================================================================
// Test: Multiple rounds of concurrent OVERWRITE operations
// =============================================================================

test_with_backends_multithreaded!(test_multiple_concurrent_overwrites, workers = 8);

/// Stress test with multiple rounds of concurrent OVERWRITE operations.
async fn test_multiple_concurrent_overwrites(backend: BackendType) -> TestResult<()> {
    let (_temp_dir, catalog1, catalog2, data_path) = create_separate_catalogs(backend).await?;
    let schema = create_test_schema();

    // Create tables
    let table1 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog1) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "multi_overwrite1".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let table2 = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "multi_overwrite2".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
        )
        .await?,
    );

    let ctx = SessionContext::new();
    ctx.register_table(
        "multi_overwrite1",
        Arc::clone(&table1) as Arc<dyn TableProvider>,
    )?;
    ctx.register_table(
        "multi_overwrite2",
        Arc::clone(&table2) as Arc<dyn TableProvider>,
    )?;

    // Perform multiple rounds of concurrent overwrites
    for round in 0..5 {
        let ctx1 = ctx.clone();
        let ctx2 = ctx.clone();
        let barrier = Arc::new(Barrier::new(2));

        let sql1 = format!(
            "INSERT OVERWRITE multi_overwrite1 VALUES ({}, 'Round{}', {})",
            round * 100,
            round,
            round * 1000
        );
        let sql2 = format!(
            "INSERT OVERWRITE multi_overwrite2 VALUES ({}, 'Round{}', {})",
            round * 100,
            round,
            round * 1000
        );

        let barrier1 = Arc::clone(&barrier);
        let overwrite1 = tokio::spawn(async move {
            barrier1.wait().await;
            ctx1.sql(&sql1).await?.collect().await
        });

        let barrier2 = Arc::clone(&barrier);
        let overwrite2 = tokio::spawn(async move {
            barrier2.wait().await;
            ctx2.sql(&sql2).await?.collect().await
        });

        let (result1, result2) = tokio::join!(overwrite1, overwrite2);

        result1
            .expect("overwrite1 task panicked")
            .map_err(|e| format!("Round {round}: overwrite1 failed: {e}"))?;
        result2
            .expect("overwrite2 task panicked")
            .map_err(|e| format!("Round {round}: overwrite2 failed: {e}"))?;
    }

    // Each table should have exactly 1 row (last overwrite)
    let count1: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM multi_overwrite1")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    let count2: i64 = ctx
        .sql("SELECT COUNT(*) as cnt FROM multi_overwrite2")
        .await?
        .collect()
        .await?[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0);

    assert_eq!(
        count1, 1,
        "Expected 1 row in multi_overwrite1 after all OVERWRITEs"
    );
    assert_eq!(
        count2, 1,
        "Expected 1 row in multi_overwrite2 after all OVERWRITEs"
    );

    Ok(())
}
