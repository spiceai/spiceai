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
use std::sync::{Arc, RwLock};
use vortex_datafusion::VortexFormat;

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
    _catalog: Arc<dyn MetadataCatalog>,
    /// Underlying Vortex `ListingTable` that scans all virtual files in the table directory
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
        let snapshot_dir = std::path::PathBuf::from(&table_metadata.path)
            .join(table_metadata.table_id.to_string())
            .join(&table_metadata.current_snapshot_id);

        // DataFusion requires trailing slash for directory URLs
        let mut dir_url_str = snapshot_dir.to_string_lossy().to_string();
        if !dir_url_str.ends_with('/') {
            dir_url_str.push('/');
        }

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: e.to_string(),
            }
        })?;

        let format = Arc::new(VortexFormat::default());
        let listing_options = ListingOptions::new(format);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(Arc::<arrow_schema::Schema>::clone(&table_metadata.schema));

        let listing_table = ListingTable::try_new(config).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: e.to_string(),
            }
        })?;

        Ok(Self {
            table_metadata,
            _catalog: catalog,
            listing_table: Arc::new(RwLock::new(Arc::new(listing_table))),
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
        // Delegate to the underlying listing table
        // Clone the Arc and drop the lock before awaiting to avoid holding locks across await points
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire read lock on listing table: {e}"
                ))
            })?;
            Arc::clone(&guard)
        };
        listing_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        // Synchronous method - can use blocking read() since we're using std::sync::RwLock
        let listing_table = self.listing_table.read().map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to acquire read lock on listing table: {e}"
            ))
        })?;
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
        // Synchronous method - can use blocking read() since we're using std::sync::RwLock
        let listing_table = self.listing_table.read().ok()?;
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
            let snapshot_dir = std::path::PathBuf::from(&self.table_metadata.path)
                .join(self.table_metadata.table_id.to_string())
                .join(&new_snapshot_id);

            // Create the snapshot directory
            tokio::fs::create_dir_all(&snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

            // DataFusion requires trailing slash for directory URLs
            let mut snapshot_dir_str = snapshot_dir.to_string_lossy().to_string();
            if !snapshot_dir_str.ends_with('/') {
                snapshot_dir_str.push('/');
            }

            // Create a new ListingTable pointing to the snapshot directory
            let table_url = ListingTableUrl::parse(&snapshot_dir_str)
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

            let format = Arc::new(VortexFormat::default());
            let listing_options = ListingOptions::new(format);

            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options)
                .with_schema(Arc::clone(&self.table_metadata.schema));

            let new_listing_table = Arc::new(ListingTable::try_new(config)?);

            // Perform the insert using the new listing table with append mode
            // (Vortex only supports append at the file level)
            let result = new_listing_table
                .insert_into(state, input, InsertOp::Append)
                .await?;

            // Update the catalog to point to the new snapshot
            self._catalog
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
        let snapshot_dir = std::path::PathBuf::from(&self.table_metadata.path)
            .join(self.table_metadata.table_id.to_string())
            .join(&self.table_metadata.current_snapshot_id);

        if !snapshot_dir.exists() {
            tokio::fs::create_dir_all(&snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }

        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self.listing_table.read().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to acquire read lock on listing table: {e}"
                ))
            })?;
            Arc::clone(&guard)
        };
        listing_table
            .insert_into(state, input, InsertOp::Append)
            .await
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_table_provider_creation() {
        // Tests will be added once SQLite catalog implementation is complete
    }
}
