/*
Copyright 2025 The Spice.ai OSS Authors

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

//! `DataFusion` `TableProvider` implementation for Cayenne tables.
//!
//! # Virtual File Concept
//!
//! Cayenne treats "files" as virtual files, where each file is actually a Vortex
//! `ListingTable` at a unique directory. The catalog's `DataFile` entries track metadata
//! for these virtual files, but all actual I/O operations delegate to the corresponding
//! `ListingTable`:
//!
//! - **Reading**: Query the `ListingTable` for the specific file directory
//! - **Appending**: Append data via the `ListingTable` (creates new Vortex files)
//! - **Deleting**: Delete the `ListingTable`'s directory
//! - **Stats**: Get statistics from the `ListingTable`
//!
//! A Cayenne table can have multiple virtual files (`ListingTables`), each in its own
//! subdirectory (e.g., `file_000001/`, `file_000002/`). When querying the table,
//! the provider reads from all active virtual files.
//!
//! # Module Organization
//!
//! - [`table`]: Main `CayenneTableProvider` implementation
//! - [`delete`]: Deletion vector handling and filtering
//! - [`streaming`]: Streaming execution plan for write operations
//! - [`utils`]: Numeric conversion utilities
//! - [`constants`]: Shared constants
//! - [`context`]: Shared context for Cayenne operations

pub(crate) mod constants;
pub(crate) mod context;
pub(crate) mod delete;
pub mod deletion_index;
pub(crate) mod deletion_strategy;
pub(crate) mod scan;
pub(crate) mod sink;
pub(crate) mod streaming;
pub(crate) mod table;
pub(crate) mod utils;
pub(crate) mod vortex_format;

// Re-export the main type at the module level for convenience
pub use context::CayenneContext;
pub use deletion_strategy::{PkDeletionStrategy, PkDeletionStrategyWithCache};
pub use scan::CayenneAccelerationExec;
pub use table::{CayenneTableProvider, CayenneTableProviderBuilder};
pub use vortex_format::{attach_deletion_vectors_to_config, DeletionFilteringVortexFormat};

// Re-export deletion utilities for advanced use cases
pub use delete::CayenneDeletionSink;

use snafu::prelude::*;

#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    /// Invalid number of children provided to execution plan
    #[snafu(display(
        "Invalid number of children for CayenneAccelerationExec: expected 1, got {}",
        children_count
    ))]
    InvalidChildrenCount { children_count: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MetadataCatalog;
    use crate::cayenne_catalog::CayenneCatalog;
    use crate::metadata::CreateTableOptions;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::context::SessionContext;
    use datafusion_catalog::TableProvider;
    use datafusion_expr::dml::InsertOp;
    use datafusion_physical_plan::collect;
    use datafusion_table_providers::util::on_conflict::OnConflict;
    use futures::future::join_all;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper to create a test catalog with a table containing sample data
    async fn setup_test_table(
        connection_string: &str,
    ) -> (Arc<CayenneCatalog>, crate::metadata::TableMetadata, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string)
                .expect("Failed to create CayenneCatalog instance"),
        );
        catalog
            .init()
            .await
            .expect("Failed to initialize catalog schema and tables");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let table_name = "test_table";
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::DoNothingAll),
                base_path: temp_dir.path().to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            })
            .await
            .expect("Failed to create test table in catalog");

        let table_metadata = catalog
            .get_table(table_name)
            .await
            .expect("Failed to get table metadata from catalog");

        tracing::info!("Created table '{}' with ID {}", table_name, table_id);

        // Create provider and insert test data
        let ctx = SessionContext::new();
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
        let provider = CayenneTableProvider::new(table_name, catalog_trait)
            .await
            .expect("Failed to create CayenneTableProvider instance");

        // Insert 1000 rows of test data
        let mut id_values = Vec::new();
        let mut name_values = Vec::new();
        for i in 0..1000 {
            id_values.push(i);
            name_values.push(format!("name_{i}"));
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(id_values)),
                Arc::new(StringArray::from(name_values)),
            ],
        )
        .expect("Failed to create RecordBatch with test data");

        // Create a memory exec plan from the batch
        let mem_config = MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)
            .expect("Failed to create MemorySourceConfig from test data");
        let mem_exec = DataSourceExec::new(Arc::new(mem_config));

        let insert_result = provider
            .insert_into(&ctx.state(), Arc::new(mem_exec), InsertOp::Append)
            .await
            .expect("Failed to create insert execution plan");

        // Execute the insert plan to actually write the data
        let batches = collect(insert_result, ctx.task_ctx())
            .await
            .expect("Failed to execute insert plan and write test data");

        tracing::info!("Insert completed, wrote {} batches", batches.len());

        (catalog, table_metadata, temp_dir)
    }

    #[tokio::test]
    async fn test_concurrent_reads_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for concurrent reads test");
        let db_path = temp_dir.path().join("cayenne_concurrent_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_turso() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for concurrent reads test (Turso)");
        let db_path = temp_dir.path().join("cayenne_concurrent_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_impl(&connection_string).await;
    }

    /// Core concurrent read test implementation
    async fn test_concurrent_reads_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        // Create multiple concurrent readers
        let num_readers = 20;
        let num_queries_per_reader = 10;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider in concurrent reader task");

                let mut total_rows = 0;
                for query_num in 0..num_queries_per_reader {
                    // Execute a full table scan
                    let plan = provider
                        .scan(&ctx.state(), None, &[], None)
                        .await
                        .expect("Failed to create scan plan in concurrent reader");

                    let batches = collect(plan, ctx.task_ctx())
                        .await
                        .expect("Failed to collect scan results in concurrent reader");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    total_rows += row_count;

                    if query_num == 0 {
                        tracing::info!(
                            "Reader {} first query returned {} rows",
                            reader_id,
                            row_count
                        );
                    }
                }

                total_rows
            });

            handles.push(handle);
        }

        // Wait for all readers to complete
        let results = join_all(handles).await;

        // Verify all readers completed successfully
        for (idx, result) in results.iter().enumerate() {
            match result {
                Ok(total_rows) => {
                    assert_eq!(
                        *total_rows,
                        1000 * num_queries_per_reader,
                        "Reader {idx} read incorrect number of rows"
                    );
                }
                Err(e) => panic!("Reader {idx} failed: {e}"),
            }
        }

        tracing::info!(
            "✓ {} concurrent readers successfully completed {} queries each",
            num_readers,
            num_queries_per_reader
        );
    }

    #[tokio::test]
    async fn test_concurrent_reads_with_filters_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for filter test");
        let db_path = temp_dir.path().join("cayenne_filter_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_filters_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_with_filters_turso() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for filter test (Turso)");
        let db_path = temp_dir.path().join("cayenne_filter_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_filters_impl(&connection_string).await;
    }

    /// Test concurrent reads with various filter conditions
    async fn test_concurrent_reads_with_filters_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 10;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for filter test reader");

                // Register the table with DataFusion so we can run SQL queries
                ctx.register_table("test_table", Arc::new(provider))
                    .expect("Failed to register table with DataFusion context");

                // Execute various queries with filters
                let queries = vec![
                    ("SELECT COUNT(*) FROM test_table WHERE id < 500", 500),
                    ("SELECT COUNT(*) FROM test_table WHERE id >= 500", 500),
                    ("SELECT COUNT(*) FROM test_table WHERE id % 2 = 0", 500),
                    ("SELECT COUNT(*) FROM test_table", 1000),
                ];

                for (query, expected_count) in &queries {
                    let df = ctx.sql(query).await.expect("Failed to execute SQL query");
                    let batches = df.collect().await.expect("Failed to collect query results");

                    // Extract count from result
                    let count = batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("Failed to downcast count column to Int64Array")
                        .value(0);

                    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let count_usize = count as usize;
                    assert_eq!(
                        count_usize, *expected_count,
                        "Reader {reader_id} query '{query}' returned incorrect count"
                    );
                }

                reader_id
            });

            handles.push(handle);
        }

        // Wait for all readers to complete
        let results = join_all(handles).await;

        // Verify all readers completed successfully
        for result in results {
            result.expect("Filter test concurrent reader task should complete successfully");
        }

        tracing::info!(
            "✓ {} concurrent readers with filters completed successfully",
            num_readers
        );
    }

    #[tokio::test]
    async fn test_concurrent_reads_with_projections_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for projection test");
        let db_path = temp_dir.path().join("cayenne_projection_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_projections_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_with_projections_turso() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for projection test (Turso)");
        let db_path = temp_dir.path().join("cayenne_projection_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_projections_impl(&connection_string).await;
    }

    /// Test concurrent reads with different column projections
    async fn test_concurrent_reads_with_projections_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 15;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for projection test reader");

                ctx.register_table("test_table", Arc::new(provider))
                    .expect("Failed to register table for projection test");

                // Test different projection patterns
                let queries = vec![
                    "SELECT id FROM test_table",
                    "SELECT name FROM test_table",
                    "SELECT id, name FROM test_table",
                    "SELECT name, id FROM test_table",
                ];

                for query in &queries {
                    let df = ctx
                        .sql(query)
                        .await
                        .expect("Failed to execute projection query");
                    let batches = df
                        .collect()
                        .await
                        .expect("Failed to collect projection query results");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(
                        row_count, 1000,
                        "Reader {reader_id} query '{query}' returned incorrect row count"
                    );
                }

                reader_id
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;

        for result in results {
            result.expect("Projection test concurrent reader task should complete successfully");
        }

        tracing::info!(
            "✓ {} concurrent readers with projections completed successfully",
            num_readers
        );
    }

    #[tokio::test]
    async fn test_high_concurrency_stress_sqlite() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for high concurrency stress test");
        let db_path = temp_dir.path().join("cayenne_stress_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_high_concurrency_stress_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_high_concurrency_stress_turso() {
        let temp_dir = TempDir::new().expect(
            "Failed to create temporary directory for high concurrency stress test (Turso)",
        );
        let db_path = temp_dir.path().join("cayenne_stress_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_high_concurrency_stress_impl(&connection_string).await;
    }

    /// Stress test with high concurrency (50 readers, 50 queries each)
    async fn test_high_concurrency_stress_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 50;
        let queries_per_reader = 50;

        let start = std::time::Instant::now();
        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for stress test reader");

                for _ in 0..queries_per_reader {
                    let plan = provider
                        .scan(&ctx.state(), None, &[], None)
                        .await
                        .expect("Failed to create scan plan in stress test");

                    let batches = collect(plan, ctx.task_ctx())
                        .await
                        .expect("Failed to collect scan results in stress test");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(row_count, 1000, "Reader {reader_id} got wrong row count");
                }

                reader_id
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;
        let duration = start.elapsed();

        for result in results {
            result.expect("Stress test concurrent reader task should complete successfully");
        }

        let total_queries = num_readers * queries_per_reader;
        let qps = f64::from(total_queries) / duration.as_secs_f64();

        tracing::info!(
            "✓ Stress test: {} concurrent readers × {} queries = {} total queries in {:.2}s ({:.0} qps)",
            num_readers,
            queries_per_reader,
            total_queries,
            duration.as_secs_f64(),
            qps
        );
    }

    /// Test that data is sorted when `sort_columns` is configured
    #[tokio::test]
    async fn test_sort_columns() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp_dir = TempDir::new().expect("Failed to create temporary directory for sort test");
        let data_path = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_path).expect("Failed to create data directory");

        let connection_string =
            format!("sqlite://{}/cayenne.db", temp_dir.path().to_string_lossy());
        let catalog = Arc::new(
            crate::CayenneCatalog::new(connection_string).expect("Failed to create catalog"),
        );
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Configure table with sort columns
        let vortex_config = crate::metadata::VortexConfig {
            sort_columns: vec!["timestamp".to_string(), "id".to_string()],
            ..Default::default()
        };

        let table_options = crate::metadata::CreateTableOptions {
            table_name: "sorted_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config,
        };

        let table = CayenneTableProvider::create_table(catalog, table_options)
            .await
            .expect("Failed to create table");

        // Insert unsorted data
        let unsorted_ids = vec![5i64, 3, 1, 4, 2];
        let unsorted_timestamps = vec![100i64, 200, 50, 150, 75];
        let unsorted_values = vec![50i64, 30, 10, 40, 20];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(unsorted_ids)),
                Arc::new(Int64Array::from(unsorted_timestamps)),
                Arc::new(Int64Array::from(unsorted_values)),
            ],
        )
        .expect("Failed to create record batch");

        let ctx = SessionContext::new();
        let input_exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("memory exec");
        let insert_plan = table
            .insert_into(&ctx.state(), input_exec, InsertOp::Append)
            .await
            .expect("insert_into");
        collect(insert_plan, ctx.task_ctx())
            .await
            .expect("Failed to insert data");

        // Verify data is sorted by timestamp, then by id
        let ctx = SessionContext::new();
        let scan_plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("Failed to create scan plan");

        let result_batches = collect(scan_plan, ctx.task_ctx())
            .await
            .expect("Failed to collect results");

        assert!(!result_batches.is_empty(), "Should have result batches");

        // Combine all batches
        let combined = arrow::compute::concat_batches(&schema, &result_batches)
            .expect("Failed to concatenate batches");

        let timestamp_col = combined
            .column_by_name("timestamp")
            .expect("timestamp column exists")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp is Int64Array");

        let id_col = combined
            .column_by_name("id")
            .expect("id column exists")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64Array");

        // Verify sorted order: timestamp ascending, then id ascending
        let expected_timestamps = [50i64, 75, 100, 150, 200];
        let expected_ids = [1i64, 2, 5, 4, 3];

        for i in 0..5 {
            assert_eq!(
                timestamp_col.value(i),
                expected_timestamps[i],
                "Row {i} timestamp should be sorted"
            );
            assert_eq!(
                id_col.value(i),
                expected_ids[i],
                "Row {i} id should match expected order"
            );
        }

        tracing::info!("✓ Data sorted correctly by sort_columns");
    }
}
