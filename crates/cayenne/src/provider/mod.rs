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
//! - [`table`]: `CayenneTableProvider` implementation — schema, deletion strategy,
//!   listing-fence, snapshot state, post-write maintenance scheduler, and the
//!   `DataFusion` `TableProvider` impl.
//! - [`scan`]: `CayenneAccelerationExec` wrapper and round-robin repartitioning
//!   used to fan unsorted writes across multiple writer partitions.
//! - [`vortex_format`]: `DeletionFilteringVortexFormat` wrapping
//!   `vortex_datafusion::VortexFormat` to attach per-file position-based
//!   deletion vectors and to gate decimal→float predicate pushdown.
//! - [`sink`]: `CayenneDataSink` — `DataFusion` `DataSink` adapter that the
//!   regular (non-CDC) write path uses for both append and overwrite modes.
//! - [`mutation_writer`]: `AppendMutationWriter` — append-side write logic,
//!   inline-memtable admission, and `write_cdc_pipelined` for the Stage A /
//!   Stage B CDC path consumed by `runtime/src/accelerated_table/refresh_task`.
//! - [`staging_wal`]: Staging WAL for crash-safe staged appends. Three-phase
//!   commit lifecycle: `prepare` (write WAL) → `apply_under_barrier` (move +
//!   listing-cache invalidation) → `finish` (drop write guard).
//! - [`overwrite`]: Catalog-pointer-flip path for overwrite-mode writes.
//! - [`delete`]: Deletion vector handling and filtering.
//!   - [`delete::sink`]: position- and key-based deletion sinks for SQL `DELETE`.
//!   - [`delete::filter_exec`]: `Int64PkDeletionFilterExec` and
//!     `KeyBasedDeletionFilterExec` — per-row PK probes applied at scan time.
//!   - [`delete::vector_io`]: Arrow IPC deletion-vector file writer / reader.
//! - [`deletion_index`]: Bloom-prefiltered `DeletionIndex` (Int64 PKs) and
//!   `KeyDeletionIndex` (composite byte keys) used by the filter execs.
//! - [`deletion_strategy`]: `PkDeletionStrategyWithCache` — the per-table
//!   deletion strategy and its atomically-published `ArcSwap<DeletionSnapshot>`.
//! - [`compaction`]: Tiered small-files picker and `BackgroundCompactor`.
//! - [`retention`]: Time-based retention filter builder + SQL retention DDL.
//! - [`streaming`]: Streaming execution plan for write operations.
//! - [`context`]: `CayenneContext` — shared Vortex format, upload semaphore,
//!   `RuntimeEnv`, and config.
//! - [`utils`]: Numeric conversion utilities.
//! - [`constants`]: Staging-dir name, WAL filename, and other shared constants.
//! - [`partitioned_wal`]: Cross-partition WAL for the partitioned-table
//!   coordinator (feature-gated).
pub(crate) mod compaction;
pub(crate) mod constants;
pub(crate) mod context;
pub(crate) mod delete;
pub mod deletion_index;
pub(crate) mod deletion_strategy;
pub(crate) mod memory_account;
pub(crate) mod mutation_writer;
pub(crate) mod overwrite;
pub mod partitioned_wal;
pub(crate) mod retention;
pub(crate) mod scan;
pub(crate) mod sink;
pub(crate) mod staging_wal;
pub(crate) mod streaming;
pub(crate) mod table;
pub(crate) mod utils;
pub(crate) mod vortex_format;

// Re-export the main type at the module level for convenience
pub use compaction::{set_compaction_runtime_env, set_compaction_runtime_handle};
pub use context::CayenneContext;
pub use overwrite::PreparedOverwrite;
pub use partitioned_wal::{PARTITIONED_WAL_DIR, PartitionedWal, PartitionedWalEntry};
pub use retention::TimeRetentionFilterBuilder;
pub use scan::CayenneAccelerationExec;
pub use staging_wal::{CayenneStagedAppend, PreparedStagedAppend};
pub use table::{CayenneCdcWrite, CayenneTableProvider, CayenneTableProviderBuilder};

// Re-export deletion utilities for advanced use cases
pub use delete::CayenneDeletionSink;

use crate::catalog::CatalogError;
use snafu::prelude::*;

/// Result type for Cayenne table provider operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for Cayenne table provider operations.
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    /// Catalog operation failed (DB commit, sequence increment, partition ops).
    #[snafu(display("{source}"))]
    Catalog { source: CatalogError },

    /// `DataFusion` plan/execution error (scan, sort, insert, listing table ops).
    #[snafu(transparent)]
    DataFusion {
        source: datafusion_common::DataFusionError,
    },

    /// Filesystem I/O error.
    #[snafu(transparent)]
    IoError { source: std::io::Error },

    /// Object store operation failure (list, delete, put).
    #[snafu(display("Failed to {operation} for table '{table}': {source}"))]
    ObjectStore {
        /// What operation was attempted (e.g., "list objects for snapshot cleanup").
        operation: &'static str,
        table: String,
        source: object_store::Error,
    },

    /// Vortex file operation failure (open, scan, read).
    #[snafu(display("Failed to {operation} for table '{table}': {source}"))]
    Vortex {
        /// What operation was attempted (e.g., "open vortex file for deletion scan").
        operation: &'static str,
        table: String,
        source: Box<vortex::error::VortexError>,
    },

    /// Data constraint violation: null PK, duplicate PK, row overflow.
    #[snafu(display("Data validation failed for table '{table}': {message}"))]
    DataValidation { table: String, message: String },

    /// Failed to parse a snapshot or table URL.
    #[snafu(display("Failed to parse URL '{url}': {source}"))]
    UrlParse {
        url: String,
        source: url::ParseError,
    },

    /// Arrow error during schema or type conversion.
    #[snafu(transparent)]
    Arrow { source: arrow::error::ArrowError },

    /// RwLock/Mutex poisoned or semaphore closed. Requires table reload or process restart.
    #[snafu(display("Lock poisoned for table '{table}': {lock}"))]
    LockPoisoned { table: String, lock: &'static str },

    /// Spawned task panicked (`JoinSet` or `spawn_blocking`).
    #[snafu(display("Task panicked for table '{table}': {source}"))]
    TaskPanicked {
        table: String,
        source: tokio::task::JoinError,
    },

    /// Internal invariant violation or missing configuration. Should never happen in normal operation.
    #[snafu(display("Internal error in table '{table}': {message}"))]
    Internal { table: String, message: String },

    #[snafu(display(
        "Unable to open Cayenne acceleration file ({file_path}). Too many Cayenne acceleration files are open. Try increasing your system's maximum open file count, or increase the size of generated Cayenne files with the parameter \"cayenne_target_file_size_mb\". For more details, visit: https://spiceai.org/docs/components/data-accelerators/cayenne#params"
    ))]
    TooManyOpenFiles { file_path: String },

    /// A previous write was interrupted, leaving the table in a potentially
    /// inconsistent state. The staging WAL file must be resolved before the
    /// table can be used.
    #[snafu(display("Table '{table}' may be in an inconsistent state: {message}"))]
    IncompleteWrite { table: String, message: String },

    /// Operation is not yet implemented.
    #[snafu(display("Unsupported operation: {operation}"))]
    Unsupported { operation: &'static str },

    /// Invalid number of children provided to an execution plan.
    #[snafu(display(
        "Invalid number of children for CayenneAccelerationExec: expected 1, got {children_count}"
    ))]
    InvalidChildrenCount { children_count: usize },
}

impl From<CatalogError> for Error {
    fn from(source: CatalogError) -> Self {
        Error::Catalog { source }
    }
}

impl From<Error> for datafusion_common::DataFusionError {
    fn from(err: Error) -> Self {
        match err {
            // Unwrap DataFusion errors back to their original form
            Error::DataFusion { source } => source,
            other => datafusion_common::DataFusionError::External(Box::new(other)),
        }
    }
}

impl From<Error> for CatalogError {
    fn from(err: Error) -> Self {
        match err {
            // Unwrap catalog errors back to their original form
            Error::Catalog { source } => source,
            other => CatalogError::InvalidOperation {
                message: other.to_string(),
                source: Box::new(other),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MetadataCatalog;
    use crate::cayenne_catalog::CayenneCatalog;
    use crate::metadata::CreateTableOptions;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion_catalog::TableProvider;
    use datafusion_expr::dml::InsertOp;
    use datafusion_physical_plan::collect;
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use datafusion_table_providers::util::on_conflict::OnConflict;
    use futures::future::join_all;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Insert a single batch via `insert_into()` (append mode).
    async fn insert_batch(provider: &CayenneTableProvider, batch: RecordBatch) {
        let ctx = SessionContext::new();
        let schema = Arc::clone(batch.schema_ref());
        let input_exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
            .expect("Failed to create MemorySourceConfig");
        let plan = provider
            .insert_into(&ctx.state(), input_exec, InsertOp::Append)
            .await
            .expect("Failed to create insert plan");
        collect(plan, ctx.task_ctx())
            .await
            .expect("Failed to execute insert");
    }

    /// Helper to create a test catalog with a table containing sample data
    async fn setup_test_table(
        connection_string: &str,
        ctx: &SessionContext,
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
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
        let provider = CayenneTableProvider::new(table_name, catalog_trait, ctx.runtime_env())
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
        let ctx = SessionContext::new();
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string, &ctx).await;

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
                let provider =
                    CayenneTableProvider::new(&table_name, catalog_trait, ctx.runtime_env())
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
        let ctx = SessionContext::new();
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string, &ctx).await;

        let num_readers = 10;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider =
                    CayenneTableProvider::new(&table_name, catalog_trait, ctx.runtime_env())
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
        let ctx = SessionContext::new();
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string, &ctx).await;

        let num_readers = 15;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider =
                    CayenneTableProvider::new(&table_name, catalog_trait, ctx.runtime_env())
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
        let ctx = SessionContext::new();
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string, &ctx).await;

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
                let provider =
                    CayenneTableProvider::new(&table_name, catalog_trait, ctx.runtime_env())
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

        let ctx = SessionContext::new();
        let table = CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env())
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

        // Ordinary writes are intentionally unsorted for throughput.
        // Compaction (sort_and_rewrite_data) sorts the data and flushes inline
        // rows to Vortex files with tight zone-map bounds.
        table
            .sort_and_rewrite_data(128 * 1024 * 1024)
            .await
            .expect("Failed to sort and rewrite data");

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

    /// Test that multiple upsert rounds with different PKs survive restart correctly.
    ///
    /// Previously: `load_protected_snapshots` computed a single global `max_delete_seq`
    /// from ALL deletion vectors and only kept snapshots where `seq > max_delete_seq`.
    /// With multiple upsert rounds, later rounds raised the global max, causing earlier
    /// protected snapshots to be dropped and their data lost on restart.
    ///
    /// The test verifies that after a restart, all upserted rows are preserved correctly.
    ///
    /// Scenario:
    ///   Round 1: insert alice(100), bob(200), clint(300)
    ///   Round 2: upsert alice(101), bob(201)  — creates a delete and new snapshot A
    ///   Round 3: upsert clint(301)            — creates a delete and new snapshot B
    ///   Restart → should still have exactly 3 rows: alice(101), bob(201), clint(301)
    #[tokio::test]
    async fn test_multi_upsert_rounds_survive_restart() {
        let temp_dir =
            TempDir::new().expect("Failed to create temp directory for multi-upsert restart test");
        let db_path = temp_dir.path().join("cayenne_multi_upsert.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir)
            .expect("Failed to create data directory for multi-upsert restart test");

        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
            Field::new("username", DataType::Utf8, false),
            Field::new("items_bought", DataType::Int64, false),
        ]));

        // Create catalog
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string.clone())
                .expect("Failed to create CayenneCatalog for multi-upsert restart test"),
        );
        catalog
            .init()
            .await
            .expect("Failed to initialize catalog for multi-upsert restart test");
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;

        let table_options = CreateTableOptions {
            table_name: "users".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["email".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "email".to_string(),
            ]))),
            base_path: data_dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        let provider = CayenneTableProvider::create_table(
            Arc::clone(&catalog_trait),
            table_options,
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("Failed to create table for multi-upsert restart test");
        let provider = Arc::new(provider);

        // ---- Round 1: Initial insert of all 3 users ----
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "alice@sample.com",
                    "bob@umbrellacorp.com",
                    "clint@bobsumbrellas.com",
                ])),
                Arc::new(StringArray::from(vec!["alice", "bob", "clint"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .expect("to create batch");
        insert_batch(&provider, batch1).await;

        // ---- Round 2: Upsert alice and bob only (clint unchanged) ----
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "alice@sample.com",
                    "bob@umbrellacorp.com",
                ])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int64Array::from(vec![101, 201])),
            ],
        )
        .expect("to create batch");
        insert_batch(&provider, batch2).await;

        // ---- Round 3: Upsert clint only (alice and bob unchanged) ----
        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["clint@bobsumbrellas.com"])),
                Arc::new(StringArray::from(vec!["clint"])),
                Arc::new(Int64Array::from(vec![301])),
            ],
        )
        .expect("to create batch");
        insert_batch(&provider, batch3).await;

        // Verify pre-restart: should have exactly 3 rows with latest values
        let ctx = SessionContext::new();
        ctx.register_table("users", Arc::clone(&provider) as Arc<dyn TableProvider>)
            .expect("Failed to register table for pre-restart check");
        let df = ctx
            .sql("SELECT email, items_bought FROM users ORDER BY email")
            .await
            .expect("Failed to query pre-restart");
        let pre_batches = df.collect().await.expect("Failed to collect pre-restart");
        let pre_results = format!(
            "{}",
            pretty_format_batches(&pre_batches).expect("format pre-restart results")
        );
        insta::assert_snapshot!("restart_after_upserts_before_restart", pre_results);

        // ---- Restart: drop provider, re-open from fresh catalog ----
        drop(provider);
        drop(ctx);

        let catalog2 = Arc::new(
            CayenneCatalog::new(connection_string)
                .expect("Failed to re-create CayenneCatalog after restart"),
        );
        catalog2
            .init()
            .await
            .expect("Failed to re-initialize catalog after restart");
        let catalog_trait2: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>;

        let ctx2 = SessionContext::new();
        let provider2 = CayenneTableProviderBuilder::new(catalog_trait2, ctx2.runtime_env())
            .open("users")
            .await
            .expect("Failed to reopen table after restart");
        let provider2 = Arc::new(provider2);

        ctx2.register_table("users", Arc::clone(&provider2) as Arc<dyn TableProvider>)
            .expect("Failed to register table post-restart");

        let df2 = ctx2
            .sql("SELECT email, items_bought FROM users ORDER BY email")
            .await
            .expect("Failed to query post-restart");
        let post_batches = df2.collect().await.expect("Failed to collect post-restart");
        let post_results = format!(
            "{}",
            pretty_format_batches(&post_batches).expect("format post-restart results")
        );
        insta::assert_snapshot!("restart_after_upserts_after_restart", post_results);

        tracing::info!("✓ Multi-round upsert data survives restart correctly");
    }

    /// Regression test: upsert should persist insert records to catalog so state survives restart.
    #[tokio::test]
    async fn test_upsert_persists_insert_records_for_restart() {
        let temp_dir =
            TempDir::new().expect("Failed to create temp directory for upsert restart test");
        let db_path = temp_dir.path().join("cayenne_upsert_restart.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir)
            .expect("Failed to create data directory for upsert restart test");

        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
            Field::new("items_bought", DataType::Int64, false),
        ]));

        let catalog = Arc::new(
            CayenneCatalog::new(connection_string.clone())
                .expect("Failed to create CayenneCatalog for upsert restart test"),
        );
        catalog
            .init()
            .await
            .expect("Failed to initialize catalog for upsert restart test");
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;

        let table_options = CreateTableOptions {
            table_name: "users".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["email".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "email".to_string(),
            ]))),
            base_path: data_dir.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        let provider = CayenneTableProvider::create_table(
            Arc::clone(&catalog_trait),
            table_options,
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("Failed to create table for upsert restart test");

        // Initial insert. Use enough rows to bypass inlining so this test keeps
        // exercising the file-backed upsert path and its insert-record metadata.
        let initial_row_count = table::INLINE_MAX_ROWS + 1;
        let mut emails: Vec<String> = (0..initial_row_count)
            .map(|idx| format!("user{idx}@sample.com"))
            .collect();
        emails[0] = "alice@sample.com".to_string();
        let mut items_bought: Vec<i64> = (0..initial_row_count)
            .map(|idx| i64::try_from(idx).expect("test row index fits in i64"))
            .collect();
        items_bought[0] = 100;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(emails)),
                Arc::new(Int64Array::from(items_bought)),
            ],
        )
        .expect("to create initial batch");
        insert_batch(&provider, batch1).await;

        // Upsert same PK with new value (must create delete+insert sequence metadata).
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alice@sample.com"])),
                Arc::new(Int64Array::from(vec![101])),
            ],
        )
        .expect("to create upsert batch");
        insert_batch(&provider, batch2).await;

        // Restart by creating fresh catalog/provider instances.
        drop(provider);
        drop(catalog);

        let catalog2 = Arc::new(
            CayenneCatalog::new(connection_string)
                .expect("Failed to re-create CayenneCatalog after restart"),
        );
        catalog2
            .init()
            .await
            .expect("Failed to re-initialize catalog after restart");
        let catalog_trait2: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog2) as Arc<dyn MetadataCatalog>;

        let ctx2 = SessionContext::new();
        let provider2 = CayenneTableProviderBuilder::new(catalog_trait2, ctx2.runtime_env())
            .open("users")
            .await
            .expect("Failed to reopen table after restart");

        let provider2 = Arc::new(provider2);
        ctx2.register_table("users", Arc::clone(&provider2) as Arc<dyn TableProvider>)
            .expect("Failed to register reopened table");

        let df = ctx2
            .sql("SELECT items_bought FROM users WHERE email = 'alice@sample.com'")
            .await
            .expect("Failed to query reopened table");
        let batches = df
            .collect()
            .await
            .expect("Failed to collect query results after restart");

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 1, "Expected a single row for alice");

        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("items_bought should be Int64")
            .value(0);
        assert_eq!(value, 101, "Latest upserted value should be visible");
    }

    /// Verifies that `CayenneDataSink::write_all` normalizes incoming batch schemas
    /// to match the table schema. CDC (Debezium) batches can arrive with `NonNullable`
    /// columns when the table schema declares them as `Nullable`, which would cause a
    /// Vortex assertion failure without normalization.
    #[tokio::test]
    async fn test_insert_normalizes_nullable_schema_mismatch() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for schema normalization test");
        let db_path = temp_dir.path().join("cayenne_schema_norm_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());

        let catalog = Arc::new(
            CayenneCatalog::new(&connection_string)
                .expect("Failed to create CayenneCatalog instance"),
        );
        catalog.init().await.expect("to initialize catalog");

        // Table schema: id NOT NULL, name NULLABLE
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let table_options = CreateTableOptions {
            table_name: "schema_norm_test".to_string(),
            schema: Arc::clone(&table_schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: temp_dir.path().to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        let ctx = SessionContext::new();
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
        let provider =
            CayenneTableProvider::create_table(catalog_trait, table_options, ctx.runtime_env())
                .await
                .expect("to create Cayenne table");

        // Input schema: id NULLABLE (mismatches table's NOT NULL), name NULLABLE
        // This simulates what CDC/Debezium sends — all columns as nullable.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let input_batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            ],
        )
        .expect("to create input batch");

        // Insert with mismatched nullability — should succeed after normalization
        let input_exec =
            MemorySourceConfig::try_new_exec(&[vec![input_batch]], Arc::clone(&input_schema), None)
                .expect("to create MemorySourceConfig");

        let insert_plan = provider
            .insert_into(&ctx.state(), input_exec, InsertOp::Append)
            .await
            .expect("to insert into table with mismatched nullability");

        collect(insert_plan, ctx.task_ctx())
            .await
            .expect("to execute insert");
        // Verify the data is readable and the output schema matches the table schema
        let scan_plan = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("to create scan plan after normalized insert");

        let result = collect(scan_plan, ctx.task_ctx())
            .await
            .expect("to collect scan results");

        let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2, "Expected 2 rows after insert");

        // The output schema must match the table schema (Nullable for name),
        // not the input schema
        assert_eq!(
            result[0].schema(),
            table_schema,
            "Output schema should match the table schema, not the input schema"
        );
    }
}
