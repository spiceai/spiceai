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

//! `DataFusion` `TableProvider` implementation for Pepper tables.
//!
//! # Virtual File Concept
//!
//! Pepper treats "files" as virtual files, where each file is actually a Vortex
//! `ListingTable` at a unique directory. The catalog's `DataFile` entries track metadata
//! for these virtual files, but all actual I/O operations delegate to the corresponding
//! `ListingTable`:
//!
//! - **Reading**: Query the `ListingTable` for the specific file directory
//! - **Appending**: Append data via the `ListingTable` (creates new Vortex files)
//! - **Deleting**: Delete the `ListingTable`'s directory
//! - **Stats**: Get statistics from the `ListingTable`
//!
//! A Pepper table can have multiple virtual files (`ListingTables`), each in its own
//! subdirectory (e.g., `file_000001/`, `file_000002/`). When querying the table,
//! the provider reads from all active virtual files.

use super::catalog::{CatalogResult, MetadataCatalog};
use super::metadata::{CreateTableOptions, TableMetadata};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::Constraints;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::collect;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use std::any::Any;
use std::borrow::Cow;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};
use tokio::task;
use vortex_datafusion::VortexFormat;

/// Execution plan that filters out deleted rows based on deletion vectors.
///
/// This wraps another execution plan and removes rows whose positions
/// match the deleted row IDs loaded from deletion vector files.
struct DeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    deleted_row_ids: std::collections::HashSet<i64>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl DeletionFilterExec {
    fn new(input: Arc<dyn ExecutionPlan>, deleted_row_ids: std::collections::HashSet<i64>) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            deleted_row_ids,
            properties,
        }
    }
}

impl std::fmt::Debug for DeletionFilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeletionFilterExec")
    }
}

impl DisplayAs for DeletionFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DeletionFilterExec: filtered_rows={}",
            self.deleted_row_ids.len()
        )
    }
}

impl ExecutionPlan for DeletionFilterExec {
    fn name(&self) -> &'static str {
        "DeletionFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "DeletionFilterExec requires exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.deleted_row_ids.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted_row_ids = self.deleted_row_ids.clone();
        let schema = input_stream.schema();

        Ok(Box::pin(DeletionFilterStream {
            input: input_stream,
            deleted_row_ids,
            schema,
            global_row_offset: 0,
        }))
    }
}

/// Stream that filters out deleted rows from an input stream.
struct DeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_row_ids: std::collections::HashSet<i64>,
    schema: arrow_schema::SchemaRef,
    global_row_offset: i64,
}

impl futures::Stream for DeletionFilterStream {
    type Item = datafusion_common::Result<arrow::array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match std::pin::Pin::new(&mut self.input).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    let current_offset = self.global_row_offset;
                    let batch_size = batch.num_rows();

                    // Validate batch size upfront to avoid redundant error handling in hot path
                    let batch_size_i64 = match convert_to_i64(batch_size, "batch size") {
                        Ok(value) => value,
                        Err(err) => return std::task::Poll::Ready(Some(Err(err))),
                    };

                    self.global_row_offset =
                        match self.global_row_offset.checked_add(batch_size_i64) {
                            Some(value) => value,
                            None => {
                                return std::task::Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::Execution(
                                        "Row ID overflow while updating global offset".to_string(),
                                    ),
                                )))
                            }
                        };

                    tracing::debug!(
                        "DeletionFilterStream: processing batch with {} rows, offset {}, deleted_ids: {:?}",
                        batch_size, current_offset, self.deleted_row_ids
                    );

                    // Find which rows in this batch should be kept
                    let mut keep_indices = Vec::new();
                    for row_idx in 0..batch_size {
                        // Convert once - we've already validated batch_size fits in i64
                        let row_offset = match convert_to_i64(row_idx, "row index") {
                            Ok(value) => value,
                            Err(err) => {
                                return std::task::Poll::Ready(Some(Err(err)));
                            }
                        };
                        let Some(global_row_id) = current_offset.checked_add(row_offset) else {
                            return std::task::Poll::Ready(Some(Err(
                                datafusion_common::DataFusionError::Execution(
                                    "Row ID overflow while calculating global row id".to_string(),
                                ),
                            )));
                        };
                        if !self.deleted_row_ids.contains(&global_row_id) {
                            keep_indices.push(row_idx as u64);
                        }
                    }

                    tracing::debug!(
                        "DeletionFilterStream: keeping {} of {} rows",
                        keep_indices.len(),
                        batch_size
                    );

                    // If all rows are deleted, skip this batch and continue to next
                    if keep_indices.is_empty() {
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is
                    if keep_indices.len() == batch_size {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Filter the batch using take kernel
                    let indices = arrow::array::UInt64Array::from(keep_indices);
                    let filtered_columns: datafusion_common::Result<Vec<_>> = batch
                        .columns()
                        .iter()
                        .map(|col| {
                            arrow::compute::take(col.as_ref(), &indices, None).map_err(|e| {
                                datafusion_common::DataFusionError::ArrowError(Box::new(e), None)
                            })
                        })
                        .collect();

                    match filtered_columns {
                        Ok(columns) => {
                            match arrow::array::RecordBatch::try_new(batch.schema(), columns) {
                                Ok(filtered_batch) => {
                                    return std::task::Poll::Ready(Some(Ok(filtered_batch)));
                                }
                                Err(e) => {
                                    return std::task::Poll::Ready(Some(Err(
                                        datafusion_common::DataFusionError::ArrowError(
                                            Box::new(e),
                                            None,
                                        ),
                                    )));
                                }
                            }
                        }
                        Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                    }
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

impl datafusion_execution::RecordBatchStream for DeletionFilterStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Pepper table provider that reads from Vortex virtual files.
///
/// This provider manages a table composed of multiple "virtual files", where each file
/// is a Vortex `ListingTable` at its own directory.
///
/// Currently, the implementation uses a single `ListingTable` that scans the entire table
/// directory. In a future optimization, this could be enhanced to manage multiple
/// `ListingTables` (one per virtual file) and union their results for better control
/// over file-level operations.
pub struct PepperTableProvider {
    /// Table metadata from the catalog
    table_metadata: TableMetadata,
    /// Reference to the metadata catalog for file operations
    catalog: Arc<dyn MetadataCatalog>,
    /// Underlying Vortex `ListingTable` that scans all virtual files in the table directory.
    /// Note: Each `DataFile` in the catalog represents a subdirectory (virtual file),
    /// but this `ListingTable` currently scans all of them together.
    /// Wrapped in `RwLock` to allow updating the listing table on overwrite operations.
    /// Uses `std::sync::RwLock` instead of `tokio::sync::RwLock` because we need
    /// synchronous access in `TableProvider` trait methods (`supports_filters_pushdown`
    /// and `statistics`), and the lock is held for very short durations (just Arc clones).
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
}

impl std::fmt::Debug for PepperTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PepperTableProvider")
            .field("table_metadata", &self.table_metadata)
            .finish_non_exhaustive()
    }
}

impl PepperTableProvider {
    /// Construct the path to a snapshot directory.
    ///
    /// Directory structure: `[table_path]/[table_id]/[snapshot_id]/`
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    fn snapshot_dir_path(table_path: &str, table_id: i64, snapshot_id: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(table_path)
            .join(table_id.to_string())
            .join(snapshot_id)
    }

    /// Convert a directory path to a `DataFusion`-compatible URL string with trailing slash.
    ///
    /// `DataFusion` requires directory URLs to end with a trailing slash.
    fn dir_to_url_string(dir: &std::path::Path) -> String {
        let mut url_str = dir.to_string_lossy().to_string();
        if !url_str.ends_with('/') {
            url_str.push('/');
        }
        url_str
    }

    /// Create a new `ListingTable` for a snapshot directory.
    ///
    /// # Arguments
    ///
    /// * `snapshot_dir` - Path to the snapshot directory
    /// * `schema` - Arrow schema for the table
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be created.
    fn create_listing_table(
        snapshot_dir: &std::path::Path,
        schema: SchemaRef,
    ) -> CatalogResult<Arc<ListingTable>> {
        let dir_url_str = Self::dir_to_url_string(snapshot_dir);

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to parse table URL: {e}"),
            }
        })?;

        let format = Arc::new(VortexFormat::default());
        let listing_options = ListingOptions::new(format);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table = ListingTable::try_new(config).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to create listing table: {e}"),
            }
        })?;

        Ok(Arc::new(listing_table))
    }

    /// Ensure a snapshot directory exists, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    async fn ensure_snapshot_dir_exists(
        snapshot_dir: &std::path::Path,
    ) -> datafusion_common::Result<()> {
        if !snapshot_dir.exists() {
            tokio::fs::create_dir_all(snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }
        Ok(())
    }

    /// Create a new Pepper table provider.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new(table_name: &str, catalog: Arc<dyn MetadataCatalog>) -> CatalogResult<Self> {
        let table_metadata = catalog.get_table(table_name).await?;

        // Construct path to current snapshot
        // Directory structure: [table_path]/[table_id]/[snapshot_id]/
        // All tables have a snapshot ID (created on table initialization)
        let snapshot_dir = Self::snapshot_dir_path(
            &table_metadata.path,
            table_metadata.table_id,
            &table_metadata.current_snapshot_id,
        );

        let listing_table = Self::create_listing_table(
            &snapshot_dir,
            Arc::<arrow_schema::Schema>::clone(&table_metadata.schema),
        )?;

        Ok(Self {
            table_metadata,
            catalog,
            listing_table: Arc::new(RwLock::new(listing_table)),
        })
    }

    /// Create a new table in Pepper.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
    ) -> CatalogResult<Self> {
        let _table_id = catalog.create_table(options.clone()).await?;
        Self::new(&options.table_name, catalog).await
    }
    /// Get a reference to the catalog.
    ///
    /// This is useful for testing and advanced use cases that need direct catalog access.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Get the table metadata.
    #[must_use]
    pub fn metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// Insert data from a record batch stream.
    ///
    /// This method writes data to the Vortex `ListingTable`. The actual file writing is
    /// delegated to `DataFusion`'s `ListingTable` via `insert_into`, which uses `VortexSink`
    /// to create Vortex files in the table directory.
    ///
    /// # Implementation Notes
    ///
    /// The insert operation is handled by the underlying `ListingTable`, which:
    /// 1. Receives the record batch stream
    /// 2. Writes Vortex files to the table directory
    /// 3. Returns the number of rows written
    ///
    /// Note: Currently this doesn't create per-file virtual file entries in the Pepper
    /// catalog. In a future enhancement, we could track individual Vortex files as
    /// separate `DataFile` entries by:
    /// - Intercepting the `VortexSink` output to discover written files
    /// - Creating unique subdirectories per "virtual file"
    /// - Adding one `DataFile` entry per subdirectory to the catalog
    ///
    /// For now, the data is successfully written to the `ListingTable`'s directory and
    /// will be readable on the next scan, even though we're not tracking individual
    /// files in the Pepper catalog metadata yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted.
    #[allow(clippy::items_after_statements)]
    #[allow(clippy::too_many_lines)]
    pub async fn insert(&self, stream: SendableRecordBatchStream) -> CatalogResult<u64> {
        let schema = stream.schema();

        // Create a streaming execution plan that forwards batches without buffering
        // Uses tokio::sync::Mutex to properly handle async context
        struct StreamingExec {
            schema: arrow_schema::SchemaRef,
            stream: tokio::sync::Mutex<Option<DFStream>>,
            properties: datafusion_physical_plan::PlanProperties,
        }

        impl std::fmt::Debug for StreamingExec {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("StreamingExec").finish()
            }
        }

        impl DisplayAs for StreamingExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "StreamingExec")
            }
        }

        impl ExecutionPlan for StreamingExec {
            fn name(&self) -> &'static str {
                "StreamingExec"
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn schema(&self) -> arrow_schema::SchemaRef {
                Arc::<arrow_schema::Schema>::clone(&self.schema)
            }

            fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
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
                _context: Arc<datafusion_execution::TaskContext>,
            ) -> datafusion_common::Result<DFStream> {
                // Use async-aware RecordBatchStreamAdapter to properly forward the stream
                let schema = Arc::<arrow_schema::Schema>::clone(&self.schema);
                let stream_mutex = Arc::new(tokio::sync::Mutex::new(
                    self.stream
                        .try_lock()
                        .map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                "Stream is locked (concurrent access detected)".to_string(),
                            )
                        })?
                        .take()
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Execution(
                                "Stream already consumed".to_string(),
                            )
                        })?,
                ));

                use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
                let adapter = RecordBatchStreamAdapter::new(
                    schema,
                    async_stream::stream! {
                        let mut stream = stream_mutex.lock().await;
                        while let Some(batch) = stream.next().await {
                            yield batch;
                        }
                    },
                );

                Ok(Box::pin(adapter))
            }
        }

        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
        use datafusion_physical_plan::PlanProperties;

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::<arrow_schema::Schema>::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        let stream_exec = Arc::new(StreamingExec {
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            stream: tokio::sync::Mutex::new(Some(stream)),
            properties,
        });

        // Create a session context for executing the insert
        let ctx = SessionContext::new();
        let state = ctx.state();

        // Delegate to ListingTable's insert_into to write Vortex files
        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                super::catalog::CatalogError::InvalidOperation {
                    message: format!("Failed to acquire read lock on listing table: {e}"),
                }
            })?;
            Arc::clone(&guard)
        };
        let insert_plan = listing_table
            .insert_into(&state, stream_exec, InsertOp::Append)
            .await
            .map_err(|e| super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to create insert plan: {e}"),
            })?;

        // Execute the insert plan
        let results = collect(insert_plan, state.task_ctx()).await.map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to execute insert: {e}"),
            }
        })?;

        // The insert plan returns statistics about the insert operation
        // DataFusion's insert operations typically return a RecordBatch with a count column
        // indicating the number of rows actually written
        let row_count: u64 = if results.is_empty() {
            // No results means no rows were written
            0
        } else if results.len() == 1 && results[0].num_columns() == 1 {
            // Standard DataFusion insert result: single batch with single count column
            let batch = &results[0];
            if batch.num_rows() == 1 {
                // Try to extract the count value from the first column
                use arrow::array::AsArray;
                let array = batch.column(0);
                if let Some(count_array) = array.as_primitive_opt::<arrow::datatypes::UInt64Type>()
                {
                    count_array.value(0)
                } else {
                    // Fallback: sum all rows in all batches if format is unexpected
                    results.iter().map(|b| b.num_rows() as u64).sum()
                }
            } else {
                // Multiple rows in result batch - unexpected, use fallback
                results.iter().map(|b| b.num_rows() as u64).sum()
            }
        } else {
            // Multiple batches or unexpected format - sum rows as fallback
            results.iter().map(|b| b.num_rows() as u64).sum()
        };

        tracing::debug!("Insert completed, wrote {} rows to Vortex", row_count);

        // Refresh the listing table to pick up new files and update statistics
        // This ensures that query plans have access to up-to-date table statistics
        // after the insert operation completes
        self.refresh_listing_table()?;

        Ok(row_count)
    }

    /// Delete rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn delete_by_primary_key(&self, _key_values: Vec<Vec<u8>>) -> CatalogResult<u64> {
        // Implementation would:
        // 1. Scan data files for matching primary keys
        // 2. Create/update deletion vectors
        // 3. Write deletion vector files
        // 4. Add delete file entries to catalog
        // 5. Return number of rows deleted
        Err(super::catalog::CatalogError::InvalidOperation {
            message: "Delete not yet implemented".to_string(),
        })
    }

    /// Update rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn update_by_primary_key(
        &self,
        _key_values: Vec<Vec<u8>>,
        _new_values: Vec<arrow::array::RecordBatch>,
    ) -> CatalogResult<u64> {
        // Implementation would:
        // 1. Delete old rows using deletion vectors
        // 2. Insert new rows
        // 3. Return number of rows updated
        Err(super::catalog::CatalogError::InvalidOperation {
            message: "Update not yet implemented".to_string(),
        })
    }

    /// Refresh the underlying `ListingTable` to pick up new files and update statistics.
    ///
    /// This method should be called after insert operations to ensure that:
    /// - The `ListingTable` discovers newly written Vortex files
    /// - Table statistics (row counts, column stats) are updated
    /// - Query plans can use fresh statistics for optimization
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be refreshed.
    fn refresh_listing_table(&self) -> CatalogResult<()> {
        // Construct path to current snapshot
        let snapshot_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &self.table_metadata.current_snapshot_id,
        );

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir,
            Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema),
        )?;

        // Update the listing table with write lock
        let mut guard = self.listing_table.write().map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to acquire write lock for listing table refresh: {e}"),
            }
        })?;
        *guard = new_listing_table;

        tracing::debug!(
            "Refreshed listing table for {} to pick up new files and update statistics",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Read deletion vectors from files and return a set of deleted row IDs.
    ///
    /// # Blocking I/O Warning
    ///
    /// This function performs **blocking file system I/O** operations using `std::fs::File::open`
    /// and must be called from within `tokio::task::spawn_blocking` to avoid blocking the async
    /// runtime. The caller is responsible for offloading this to a blocking task context.
    ///
    /// See: Project coding guidelines on Async/Blocking Patterns
    fn read_deletion_vectors(
        delete_files: Vec<super::metadata::DeleteFile>,
    ) -> datafusion_common::Result<std::collections::HashSet<i64>> {
        use arrow::array::Array;
        use arrow::ipc::reader::FileReader;
        use std::collections::HashSet;

        let mut deleted_row_ids = HashSet::new();
        let file_count = delete_files.len();

        tracing::debug!(
            "read_deletion_vectors: processing {} delete files",
            file_count
        );

        for delete_file in delete_files {
            let path = std::path::Path::new(&delete_file.path);
            tracing::debug!("read_deletion_vectors: reading file {:?}", path);

            // Read deletion vector file
            let file = std::fs::File::open(path).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to open deletion vector file {}: {e}",
                    path.display()
                ))
            })?;

            let reader = FileReader::try_new(file, None).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read deletion vector file {}: {e}",
                    path.display()
                ))
            })?;

            // Read all batches and extract row IDs
            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to read batch from deletion vector: {e}"
                    ))
                })?;

                // Get row_id column (first column)
                let row_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected Int64Array for row_id column".to_string(),
                        )
                    })?;

                // Add all row IDs to the set
                for i in 0..row_id_array.len() {
                    if !row_id_array.is_null(i) {
                        let row_id = row_id_array.value(i);
                        deleted_row_ids.insert(row_id);
                    }
                }
            }
        }

        tracing::debug!(
            "Loaded {} deleted row IDs from {} deletion vector files",
            deleted_row_ids.len(),
            file_count
        );

        Ok(deleted_row_ids)
    }
}

#[async_trait]
impl TableProvider for PepperTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Delegate to the underlying listing table first
        // Clone the Arc and drop the lock before awaiting to avoid holding locks across await points
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire read lock on listing table: {e}"
                ))
            })?;
            Arc::clone(&guard)
        };
        let plan = listing_table
            .scan(state, projection, filters, limit)
            .await?;

        // Load deletion vectors from catalog
        let delete_files = self
            .catalog
            .get_table_delete_files(self.table_metadata.table_id)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to load deletion vectors: {e}"
                ))
            })?;

        // If there are any deletion vectors, load and apply filtering
        if !delete_files.is_empty() {
            let total_deleted = delete_files.iter().map(|df| df.delete_count).sum::<i64>();
            tracing::debug!(
                "Applying {} deletion vectors ({} deleted rows) to scan of table {}",
                delete_files.len(),
                total_deleted,
                self.table_metadata.table_name
            );

            // Read all deletion vectors and build a set of deleted row IDs
            let delete_files_for_read = delete_files.clone();
            let deleted_row_ids =
                task::spawn_blocking(move || Self::read_deletion_vectors(delete_files_for_read))
                    .await
                    .map_err(|err| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Deletion vector reader task panicked or was cancelled: {err}"
                        ))
                    })
                    .and_then(|result| {
                        result.map_err(|err| {
                            datafusion_common::DataFusionError::Execution(format!(
                                "Failed to read deletion vectors: {err}"
                            ))
                        })
                    })?;

            if !deleted_row_ids.is_empty() {
                // Wrap the plan with our deletion filter
                return Ok(Arc::new(DeletionFilterExec::new(plan, deleted_row_ids)));
            }
        }

        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        // Synchronous method - clone Arc quickly and release lock immediately
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire read lock on listing table: {e}"
                ))
            })?;
            Arc::clone(&guard)
        };
        listing_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion_common::Statistics> {
        // Delegate statistics tracking to the underlying Vortex ListingTable.
        // The ListingTable aggregates statistics from all Vortex files in the table directory,
        // providing metrics such as:
        // - Total number of rows across all files
        // - Total size in bytes
        // - Column-level statistics (min, max, null count, distinct count if available)
        //
        // This allows the query optimizer to make informed decisions about:
        // - Partition pruning
        // - Join ordering
        // - Aggregation strategies
        //
        // Note: Statistics are cached by the ListingTable and may not reflect
        // very recent writes until the table metadata is refreshed.
        //
        // Clone Arc quickly and release lock immediately to minimize contention
        let listing_table = {
            let guard = self.listing_table.read().ok()?;
            Arc::clone(&guard)
        };
        listing_table.statistics()
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Handle overwrite by creating a new snapshot
        // Directory structure: [data_dir]/[table_id]/[snapshot_id]/
        if overwrite == InsertOp::Overwrite {
            // Generate a new UUIDv7 for the snapshot
            let new_snapshot_id = uuid::Uuid::now_v7().to_string();

            // Create snapshot directory: [table_path]/[table_id]/[snapshot_id]/
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                self.table_metadata.table_id,
                &new_snapshot_id,
            );

            // Create the snapshot directory
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;

            // Create a new ListingTable pointing to the snapshot directory
            let new_listing_table =
                Self::create_listing_table(&snapshot_dir, Arc::clone(&self.table_metadata.schema))
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to create listing table for new snapshot: {e}"
                        ))
                    })?;

            // Perform the insert using the new listing table with append mode
            // (Vortex only supports append at the file level)
            let result = new_listing_table
                .insert_into(state, input, InsertOp::Append)
                .await?;

            // Update the catalog to point to the new snapshot
            self.catalog
                .set_current_snapshot(self.table_metadata.table_id, &new_snapshot_id)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to update snapshot after overwrite: {e}"
                    ))
                })?;

            // Update the provider's listing table to point to the new snapshot
            // This ensures subsequent queries in the same context will read from the new data
            let mut listing_table_guard = self.listing_table.write().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire write lock on listing table: {e}"
                ))
            })?;
            *listing_table_guard = new_listing_table;

            return Ok(result);
        }

        // For regular appends, use the existing snapshot and listing table
        // Ensure the snapshot directory exists (it might not if this is the first write to a newly created table)
        let snapshot_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &self.table_metadata.current_snapshot_id,
        );

        Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;

        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire read lock on listing table: {e}"
                ))
            })?;
            Arc::clone(&guard)
        };
        let result = listing_table
            .insert_into(state, input, InsertOp::Append)
            .await?;

        // Refresh the listing table to pick up new files and update statistics
        // This ensures query plans have access to up-to-date statistics after the insert
        self.refresh_listing_table().map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to refresh listing table after insert: {e}"
            ))
        })?;

        Ok(result)
    }
}

// Implement DeletionTableProvider for Pepper
#[async_trait]
impl DeletionTableProvider for PepperTableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(PepperDeletionSink::new(
                self.table_metadata.clone(),
                Arc::clone(&self.catalog),
                Arc::clone(&self.listing_table),
                Arc::clone(&self.table_metadata.schema),
                filters,
            )),
            &self.table_metadata.schema,
        )))
    }
}

/// Deletion sink for Pepper tables
struct PepperDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
}

impl PepperDeletionSink {
    fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        schema: SchemaRef,
        filters: &[Expr],
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
        }
    }

    /// Create a deletion vector schema (row IDs to delete).
    fn deletion_vector_schema() -> SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("deleted_at", DataType::Int64, false), // Unix timestamp in microseconds
        ]))
    }

    async fn delete_all_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let scan_plan = listing_table.scan(&ctx.state(), None, &[], None).await?;
        let batches = collect(scan_plan, ctx.task_ctx()).await?;
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let total_rows_i64 = convert_to_i64_box(total_rows, "total row count")?;

        let row_ids: Vec<i64> = (0..total_rows_i64).collect();

        let deletion_vector_path = std::path::PathBuf::from(&self.table_metadata.path)
            .join(&self.table_metadata.current_snapshot_id)
            .join("deletions")
            .join(format!("delete_{}.vortex", uuid::Uuid::now_v7()));

        let deleted_count = self
            .write_deletion_vector(row_ids, &deletion_vector_path)
            .await?;

        let file_size = tokio::fs::metadata(&deletion_vector_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let deleted_count_i64 = convert_to_i64_box(deleted_count, "deleted row count")?;
        let file_size_i64 = convert_to_i64_box(file_size, "deletion vector file size")?;

        let delete_file = super::metadata::DeleteFile {
            delete_file_id: 0,
            table_id: self.table_metadata.table_id,
            data_file_id: 0,
            path: deletion_vector_path.to_string_lossy().to_string(),
            path_is_relative: false,
            format: "arrow_ipc".to_string(),
            delete_count: deleted_count_i64,
            file_size_bytes: file_size_i64,
        };

        self.catalog
            .add_delete_file(delete_file)
            .await
            .map_err(|e| format!("Failed to register deletion file: {e}"))?;

        Ok(deleted_count)
    }

    async fn delete_filtered_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::{DataType, Field};

        let scan_plan = listing_table.scan(&ctx.state(), None, &[], None).await?;
        let batches = collect(scan_plan, ctx.task_ctx()).await?;

        // If no data, nothing to delete
        if batches.is_empty() {
            return Ok(0);
        }

        // Flatten all batches into one for simpler processing
        let concatenated_batch = arrow::compute::concat_batches(&self.schema, &batches)?;
        let total_rows = concatenated_batch.num_rows();

        // Create a batch with row_id column added
        let row_id_array = arrow::array::Int64Array::from_iter_values(
            0..convert_to_i64_box(total_rows, "total rows")?,
        );

        let mut fields = vec![Field::new("__row_id", DataType::Int64, false)];
        for field in self.schema.fields() {
            fields.push((**field).clone());
        }
        let schema_with_rowid = Arc::new(arrow::datatypes::Schema::new(fields));

        let mut columns_with_rowid = vec![Arc::new(row_id_array) as Arc<dyn Array>];
        columns_with_rowid.extend_from_slice(concatenated_batch.columns());

        let batch_with_rowid =
            arrow::array::RecordBatch::try_new(Arc::clone(&schema_with_rowid), columns_with_rowid)?;

        // Create a new session context with the row_id data
        let ctx_new = SessionContext::new();
        let mem_table_with_rowid = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema_with_rowid),
            vec![vec![batch_with_rowid]],
        )?;
        ctx_new.register_table("data_with_rowid", Arc::new(mem_table_with_rowid))?;

        // Start with selecting all columns so filters can reference them
        let mut filtered_df = ctx_new.sql("SELECT * FROM data_with_rowid").await?;

        // Apply all filters
        for filter in &self.filters {
            filtered_df = filtered_df.filter(filter.clone())?;
        }

        // Now select just the row IDs
        let row_ids_df = filtered_df.select_columns(&["__row_id"])?;

        // Collect the filtered row IDs
        let filtered_rowid_batches = row_ids_df.collect().await?;
        let mut row_ids = Vec::new();

        for batch in filtered_rowid_batches {
            let row_id_column = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..row_id_column.len() {
                if !row_id_column.is_null(i) {
                    row_ids.push(row_id_column.value(i));
                }
            }
        }

        let deleted_count = convert_to_u64_box(row_ids.len(), "deleted row count")?;

        if deleted_count == 0 {
            return Ok(0);
        }

        let deletion_vector_path = std::path::PathBuf::from(&self.table_metadata.path)
            .join(&self.table_metadata.current_snapshot_id)
            .join("deletions")
            .join(format!("delete_{}.vortex", uuid::Uuid::now_v7()));

        self.write_deletion_vector(row_ids, &deletion_vector_path)
            .await?;

        let file_size = tokio::fs::metadata(&deletion_vector_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let deleted_count_i64 = convert_to_i64_box(deleted_count, "deleted row count")?;
        let file_size_i64 = convert_to_i64_box(file_size, "deletion vector file size")?;

        let delete_file = super::metadata::DeleteFile {
            delete_file_id: 0,
            table_id: self.table_metadata.table_id,
            data_file_id: 0,
            path: deletion_vector_path.to_string_lossy().to_string(),
            path_is_relative: false,
            format: "arrow_ipc".to_string(),
            delete_count: deleted_count_i64,
            file_size_bytes: file_size_i64,
        };

        self.catalog
            .add_delete_file(delete_file)
            .await
            .map_err(|e| format!("Failed to register deletion file: {e}"))?;

        Ok(deleted_count)
    }

    /// Write deletion vector to a file.
    ///
    /// Currently writes as Arrow IPC format for simplicity and compatibility.
    /// Row IDs represent the logical position of rows in the table (0-indexed).
    async fn write_deletion_vector(
        &self,
        row_ids: Vec<i64>,
        output_path: &std::path::Path,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::ipc::writer::FileWriter;

        if row_ids.is_empty() {
            return Ok(0);
        }

        let deleted_count = row_ids.len();
        let deleted_at = chrono::Utc::now().timestamp_micros();

        // Create arrays for the deletion vector
        let row_id_array = Int64Array::from(row_ids);
        let deleted_at_array = Int64Array::from(vec![deleted_at; deleted_count]);

        // Create RecordBatch
        let schema = Self::deletion_vector_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(row_id_array) as Arc<dyn arrow::array::Array>,
                Arc::new(deleted_at_array),
            ],
        )?;

        // Create directory if it doesn't exist
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write as Arrow IPC file format
        let output_path_owned = output_path.to_path_buf();
        let schema_for_write = Arc::clone(&schema);
        let batch_for_write = batch;
        let blocking_result = tokio::task::spawn_blocking(
            move || -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
                let file = std::fs::File::create(&output_path_owned)
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
                let mut writer = FileWriter::try_new(file, &schema_for_write)
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
                writer
                    .write(&batch_for_write)
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;
                writer
                    .finish()
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;

                let file_size = std::fs::metadata(&output_path_owned)
                    .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?
                    .len();

                Ok(file_size)
            },
        )
        .await
        .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })?;

        let file_size = blocking_result?;

        tracing::debug!(
            "Deletion vector written: {} row(s), {} bytes at {:?}",
            deleted_count,
            file_size,
            output_path
        );

        Ok(deleted_count as u64)
    }
}

#[async_trait]
impl DeletionSink for PepperDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();

        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|e| format!("Failed to acquire read lock on listing table: {e}"))?;
            Arc::clone(&guard)
        };

        if self.filters.is_empty() {
            return self.delete_all_rows(&ctx, Arc::clone(&listing_table)).await;
        }

        self.delete_filtered_rows(&ctx, listing_table).await
    }
}

/// Generic conversion function that handles type conversion with proper error handling.
///
/// This is the core conversion utility that all type conversion functions delegate to.
/// It provides consistent error messages with context about what value failed to convert
/// and includes both the source and target types in the error message.
///
/// # Design Rationale
///
/// This function consolidates conversion logic to ensure consistent error handling across
/// all numeric conversions in Pepper. Without it, we would have duplicate error handling
/// code for each conversion pair (usize→i64, u64→i64, etc.), making it harder to maintain
/// consistent error messages.
///
/// # Generic Parameters
///
/// * `T` - Source type (must implement `TryInto<U>`, `Copy`, and `Display`)
/// * `U` - Target type (must implement `Display`)
///
/// # Why `Copy` is Required
///
/// The `Copy` bound is required because:
/// 1. The value is used twice: once for the conversion attempt and once in the error message
/// 2. `TryInto::try_into` consumes `self`, so without `Copy` we would move the value
/// 3. All numeric types (`usize`, `u64`, `i64`, etc.) implement `Copy`, so this isn't restrictive
///
/// # When to Use
///
/// **Do NOT call this function directly.** Instead, use the type-specific wrapper functions:
/// - `convert_to_i64()` - For conversions within `DataFusion` error context
/// - `convert_to_i64_box()` - For conversions in async/trait methods with boxed errors
/// - `convert_to_u64_box()` - For conversions to `u64` with boxed errors
///
/// The wrapper functions provide better type inference and appropriate error type handling
/// for their specific use cases.
///
/// # Examples
///
/// ```ignore
/// // GOOD - Use wrapper functions
/// let value = convert_to_i64(batch.num_rows(), "batch size")?;
///
/// // BAD - Don't call try_convert directly
/// let value = try_convert::<usize, i64>(batch.num_rows(), "batch size")?;
/// ```
fn try_convert<T, U>(value: T, context: &str) -> datafusion_common::Result<U>
where
    T: TryInto<U> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
    U: std::fmt::Display,
{
    value.try_into().map_err(|err| {
        datafusion_common::DataFusionError::Execution(format!(
            "Failed to convert {context} value {value} to {}: {err}",
            std::any::type_name::<U>()
        ))
    })
}

/// Convert a numeric value to `i64` with `DataFusion` error type.
///
/// Use this function when converting numeric values (typically `usize` or `u64`) to `i64`
/// within `DataFusion` `TableProvider` implementations or execution plans, where the error
/// type is `datafusion_common::Result<T>`.
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "batch size", "row count")
///
/// # Examples
///
/// ```ignore
/// // Converting batch size in hot path
/// let batch_size_i64 = convert_to_i64(batch.num_rows(), "batch size")?;
///
/// // Converting row index
/// let row_offset = convert_to_i64(row_idx, "row index")?;
/// ```
fn convert_to_i64<T>(value: T, context: &str) -> datafusion_common::Result<i64>
where
    T: TryInto<i64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    try_convert(value, context)
}

/// Convert a numeric value to `i64` with boxed error type.
///
/// Use this function when converting numeric values to `i64` in contexts that require
/// boxed errors, such as:
/// - Async trait methods (`DeletionSink::delete_from`)
/// - Functions returning `Result<T, Box<dyn Error>>`
/// - Code that needs to bridge between different error types
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "deleted row count")
///
/// # Examples
///
/// ```ignore
/// // In deletion sink with boxed error return type
/// let total_rows_i64 = convert_to_i64_box(total_rows, "total row count")?;
/// let deleted_count_i64 = convert_to_i64_box(deleted_count, "deleted row count")?;
/// ```
fn convert_to_i64_box<T>(
    value: T,
    context: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
where
    T: TryInto<i64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    convert_to_i64(value, context)
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

/// Convert a numeric value to `u64` with boxed error type.
///
/// Use this function when converting numeric values to `u64` in contexts that require
/// boxed errors. This is primarily used for return values that must be `u64`, such as
/// row counts returned from deletion operations.
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "deleted row count")
///
/// # Examples
///
/// ```ignore
/// // Converting deletion count from usize to u64
/// let deleted_count = convert_to_u64_box(row_ids.len(), "deleted row count")?;
/// ```
fn convert_to_u64_box<T>(
    value: T,
    context: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
where
    T: TryInto<u64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    try_convert::<T, u64>(value, context)
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_table_provider_creation() {
        // Tests will be added once SQLite catalog implementation is complete
    }
}

// # Deletion Vector Implementation Notes
//
// Pepper implements DELETE operations using deletion vectors, following the Delta Lake approach:
//
// ## Architecture
// 1. **Deletion Vectors**: Separate Vortex files containing deleted row IDs
// 2. **Catalog Tracking**: `DeleteFile` metadata registered in SQLite catalog
// 3. **Lazy Deletion**: Rows are marked as deleted but not immediately removed from data files
// 4. **Read-Time Filtering**: Scans apply deletion vectors to filter out deleted rows
//
// ## Implementation Status
// - ✅ `DeletionTableProvider` trait implemented for `PepperTableProvider`
// - ✅ `PepperDeletionSink` writes deletion vectors to Vortex files
// - ✅ Deletion vectors registered in catalog via `add_delete_file()`
// - ⏳ Read-time filtering NOT YET IMPLEMENTED (see TODOs in `scan()` method)
// - ⏳ SQL DELETE support requires DataFusion logical plan rewriting (runtime-level integration)
//
// ## Testing
// Direct SQL `DELETE` statements will fail with "NotImplemented" until:
// 1. Runtime adds logical plan optimizer rule to rewrite DELETE DML to `delete_from()` calls
// 2. OR tests call `DeletionTableProvider::delete_from()` directly (bypassing SQL)
//
// ## Compaction
// Over time, tables accumulate deletion vectors. A compaction process should:
// 1. Read all data files and their associated deletion vectors
// 2. Rewrite data files excluding deleted rows
// 3. Remove obsolete deletion vector files
// 4. Update catalog metadata
//
// This is similar to Delta Lake's OPTIMIZE command and will be added in a future iteration.
