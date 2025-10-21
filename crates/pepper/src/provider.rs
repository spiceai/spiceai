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
use data_components::update::{UpdateExec, UpdateSink, UpdateTableProvider};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion::logical_expr::is_not_true;
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
use std::error::Error;
use std::sync::Arc;
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
    /// but this `ListingTable` currently scans all of them together
    listing_table: Arc<ListingTable>,
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

        // Create listing table for the Vortex files in the table directory
        let table_path = &table_metadata.path;
        let dir_url_str = if table_path.ends_with('/') {
            table_path.to_string()
        } else {
            format!("{table_path}/")
        };

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: e.to_string(),
            }
        })?;

        let format = Arc::new(VortexFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension(".vortex");

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
            listing_table: Arc::new(listing_table),
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

    /// Create a new table in Pepper, wrapped with deletion support.
    ///
    /// This is the recommended way to create Pepper tables as it enables
    /// SQL DELETE operations through DataFusion.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table_with_deletion(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
    ) -> CatalogResult<Arc<dyn TableProvider>> {
        let table = Self::create_table(catalog, options).await?;
        Ok(Arc::new(
            data_components::delete::DeletionTableProviderAdapter::new(Arc::new(table)),
        ))
    }

    /// Load an existing table from Pepper catalog, wrapped with deletion support.
    ///
    /// Use this to reload a table that already exists in the catalog (e.g., after DELETE operations).
    /// Unlike `create_table_with_deletion`, this does not create a new catalog entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog.
    pub async fn load_table_with_deletion(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
    ) -> CatalogResult<Arc<dyn TableProvider>> {
        let table = Self::new(table_name, catalog).await?;
        Ok(Arc::new(
            data_components::delete::DeletionTableProviderAdapter::new(Arc::new(table)),
        ))
    }

    /// Get the table metadata.
    #[must_use]
    pub fn metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// Create a new virtual file directory for storing data.
    ///
    /// Creates a unique subdirectory under the table's base path using a UUID.
    ///
    /// # Returns
    ///
    /// Returns the full path to the new virtual file directory.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails.
    async fn create_virtual_file_directory(&self) -> CatalogResult<String> {
        // Generate a unique directory name using UUID
        let uuid = uuid::Uuid::new_v4();
        let virtual_dir_name = format!("file_{}", uuid.simple());
        let virtual_file_path = std::path::Path::new(&self.table_metadata.path)
            .join(&virtual_dir_name)
            .to_string_lossy()
            .to_string();

        // Create the physical directory
        tokio::fs::create_dir_all(&virtual_file_path)
            .await
            .map_err(|e| super::catalog::CatalogError::InvalidOperation {
                message: format!(
                    "Failed to create virtual file directory {}: {}",
                    virtual_file_path, e
                ),
            })?;

        Ok(virtual_file_path)
    }

    /// Insert data into the Pepper table.
    ///
    /// # Implementation
    ///
    /// Each insert operation creates a new virtual file (subdirectory):
    /// 1. Allocates a unique file ID from the catalog
    /// 2. Creates a dedicated subdirectory (e.g., `file_000001/`)
    /// 3. Creates a new `ListingTable` for the subdirectory
    /// 4. Writes data using the Vortex `ListingTable`
    /// 5. Registers the virtual file in the catalog metadata
    ///
    /// This ensures proper append-only semantics where each INSERT creates
    /// a new immutable virtual file tracked in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted or if catalog operations fail.
    #[allow(clippy::items_after_statements)]
    #[allow(clippy::too_many_lines)]
    pub async fn insert(&self, stream: SendableRecordBatchStream) -> CatalogResult<u64> {
        let schema = stream.schema();

        // Step 1: Create a new virtual file directory
        let virtual_file_path = self.create_virtual_file_directory().await?;

        // Step 2: Create a new ListingTable for this virtual file directory
        let virtual_table_url = ListingTableUrl::parse(&virtual_file_path).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to parse virtual file path: {e}"),
            }
        })?;

        let vortex_format = Arc::new(VortexFormat::default());
        let listing_options = ListingOptions::new(vortex_format);

        let virtual_table_config = ListingTableConfig::new(virtual_table_url)
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&self.table_metadata.schema));

        let virtual_listing_table =
            Arc::new(ListingTable::try_new(virtual_table_config).map_err(|e| {
                super::catalog::CatalogError::InvalidOperation {
                    message: format!("Failed to create virtual ListingTable: {e}"),
                }
            })?);

        // Step 3: Insert data into the virtual file's ListingTable

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

        // Delegate to the virtual ListingTable's insert_into to write Vortex files
        let insert_plan = virtual_listing_table
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

        tracing::debug!(
            "Insert completed, wrote {} rows to virtual file {}",
            row_count,
            virtual_file_path
        );

        // Step 4: Register the virtual file in the catalog metadata
        // Get the max file_order to determine the next order
        let existing_files = self
            ._catalog
            .get_data_files(self.table_metadata.table_id)
            .await?;
        let next_order = existing_files
            .iter()
            .map(|f| f.file_order)
            .max()
            .unwrap_or(0)
            + 1;

        // Calculate file size (sum of all .vortex files in the directory)
        let mut total_size = 0i64;
        if let Ok(mut entries) = tokio::fs::read_dir(&virtual_file_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    if entry.path().extension().and_then(|s| s.to_str()) == Some("vortex") {
                        total_size += metadata.len() as i64;
                    }
                }
            }
        }

        let data_file = super::metadata::DataFile {
            data_file_id: 0, // Will be assigned by catalog
            table_id: self.table_metadata.table_id,
            file_order: next_order,
            path: virtual_file_path.clone(),
            path_is_relative: false,
            file_format: "vortex".to_string(),
            record_count: row_count as i64,
            file_size_bytes: total_size,
            row_id_start: 0, // Could be calculated from existing files if needed
        };

        let data_file_id = self._catalog.add_data_file(data_file).await?;

        tracing::info!(
            "Registered virtual file {} (data_file_id={}) with {} rows in catalog",
            virtual_file_path,
            data_file_id,
            row_count
        );

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

    /// Delete a specific virtual file (directory).
    ///
    /// This removes a virtual file from both the catalog metadata and the file system.
    /// The virtual file is identified by its `data_file_id`.
    ///
    /// # Implementation
    ///
    /// 1. Retrieve the file metadata from the catalog
    /// 2. Delete the file entry from the catalog
    /// 3. Delete the physical directory and all Vortex files within it
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file is not found in the catalog
    /// - The directory cannot be deleted
    /// - Database operations fail
    pub async fn delete_virtual_file(&self, data_file_id: i64) -> CatalogResult<()> {
        // Get the data file metadata to find its path
        let data_files = self
            ._catalog
            .get_data_files(self.table_metadata.table_id)
            .await?;

        let data_file = data_files
            .iter()
            .find(|f| f.data_file_id == data_file_id)
            .ok_or_else(|| super::catalog::CatalogError::InvalidOperation {
                message: format!("Data file {data_file_id} not found"),
            })?;

        let file_path = &data_file.path;

        // Delete from catalog first (safer - can retry physical deletion if it fails)
        self._catalog.delete_data_file(data_file_id).await?;

        // Delete the physical directory
        self._catalog.delete_file_directory(file_path).await?;

        tracing::info!(
            "Deleted virtual file {} (data_file_id={}) from table {}",
            file_path,
            data_file_id,
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Delete all virtual files for this table.
    ///
    /// This removes all virtual files from both the catalog and the file system.
    /// Useful for table truncation or cleanup operations.
    ///
    /// # Implementation
    ///
    /// 1. Retrieve all data files for the table
    /// 2. Delete all file entries from the catalog
    /// 3. Delete all physical directories
    ///
    /// # Errors
    ///
    /// Returns an error if any deletion operation fails. Note that this operation
    /// is not atomic - some files may be deleted even if the operation fails partway through.
    pub async fn delete_all_virtual_files(&self) -> CatalogResult<()> {
        let table_id = self.table_metadata.table_id;

        // Get all data files before deleting them
        let data_files = self._catalog.get_data_files(table_id).await?;

        tracing::info!(
            "Deleting {} virtual files from table {}",
            data_files.len(),
            self.table_metadata.table_name
        );

        // Delete from catalog first
        self._catalog.delete_all_data_files(table_id).await?;

        // Delete physical directories
        for data_file in &data_files {
            if let Err(e) = self._catalog.delete_file_directory(&data_file.path).await {
                tracing::error!(
                    "Failed to delete directory {} for data_file_id {}: {}",
                    data_file.path,
                    data_file.data_file_id,
                    e
                );
                // Continue deleting other files even if one fails
            }
        }

        tracing::info!(
            "Deleted all virtual files from table {}",
            self.table_metadata.table_name
        );

        Ok(())
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
        self.listing_table
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.listing_table.supports_filters_pushdown(filters)
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
        self.listing_table.statistics()
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
        // Block overwrite operations until multiple ListingTables are implemented
        // Overwrite requires atomic replacement of all virtual files, which needs
        // proper catalog tracking of individual files (not yet implemented)
        if overwrite == InsertOp::Overwrite {
            return Err(datafusion_common::DataFusionError::NotImplemented(
                "INSERT OVERWRITE is not yet supported for Pepper tables. \
                 Use INSERT INTO for append operations."
                    .to_string(),
            ));
        }

        // Delegate to the main ListingTable to write Vortex files to the table directory
        // Note: Vortex will create uniquely named files automatically
        let insert_plan = self
            .listing_table
            .insert_into(state, input, overwrite)
            .await?;

        // Wrap the execution plan to register the insert in the catalog after execution
        let catalog = Arc::clone(&self._catalog);
        let table_id = self.table_metadata.table_id;
        let table_path = self.table_metadata.path.clone();
        let schema = Arc::clone(&self.table_metadata.schema);

        Ok(Arc::new(CatalogTrackingInsertExec {
            inner: insert_plan,
            catalog,
            table_id,
            table_path,
            schema,
        }))
    }
}

/// Execution plan wrapper that registers inserts in the catalog after execution completes.
struct CatalogTrackingInsertExec {
    inner: Arc<dyn ExecutionPlan>,
    catalog: Arc<dyn MetadataCatalog>,
    table_id: i64,
    table_path: String,
    schema: SchemaRef,
}

impl std::fmt::Debug for CatalogTrackingInsertExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogTrackingInsertExec").finish()
    }
}

impl DisplayAs for CatalogTrackingInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CatalogTrackingInsertExec")
    }
}

impl ExecutionPlan for CatalogTrackingInsertExec {
    fn name(&self) -> &'static str {
        "CatalogTrackingInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "CatalogTrackingInsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            inner: Arc::clone(&children[0]),
            catalog: Arc::clone(&self.catalog),
            table_id: self.table_id,
            table_path: self.table_path.clone(),
            schema: Arc::clone(&self.schema),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let catalog = Arc::clone(&self.catalog);
        let table_id = self.table_id;
        let table_path = self.table_path.clone();
        let schema = Arc::clone(&self.schema);

        // Wrap the stream to register in catalog after all batches are consumed
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        let tracking_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            async_stream::stream! {
                let mut row_count = 0u64;
                let mut stream = inner_stream;

                // Forward all batches and count rows
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            // Extract count from insert result batch
                            if batch.num_columns() == 1 && batch.num_rows() == 1 {
                                use arrow::array::AsArray;
                                if let Some(count_array) = batch.column(0).as_primitive_opt::<arrow::datatypes::UInt64Type>() {
                                    row_count = count_array.value(0);
                                }
                            }
                            yield Ok(batch);
                        }
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }

                // After all batches consumed, register in catalog
                if row_count > 0 {
                    // Get total table size (all .vortex files)
                    let mut total_size = 0i64;
                    if let Ok(mut entries) = tokio::fs::read_dir(&table_path).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if let Ok(metadata) = entry.metadata().await {
                                if entry.path().extension().and_then(|s| s.to_str()) == Some("vortex") {
                                    total_size += metadata.len() as i64;
                                }
                            }
                        }
                    }

                    // Get next file_order
                    let existing_files = match catalog.get_data_files(table_id).await {
                        Ok(files) => files,
                        Err(e) => {
                            tracing::error!("Failed to get existing files: {}", e);
                            vec![]
                        }
                    };
                    let next_order = existing_files.iter().map(|f| f.file_order).max().unwrap_or(0) + 1;

                    // Register this insert operation in catalog (one logical "file" entry per insert)
                    let data_file = super::metadata::DataFile {
                        data_file_id: 0,
                        table_id,
                        file_order: next_order,
                        path: table_path.clone(),
                        path_is_relative: false,
                        file_format: "vortex".to_string(),
                        record_count: row_count as i64,
                        file_size_bytes: total_size,
                        row_id_start: 0,
                    };

                    match catalog.add_data_file(data_file).await {
                        Ok(file_id) => {
                            tracing::info!(
                                "Registered data file {} (data_file_id={}) with {} rows in catalog",
                                table_path,
                                file_id,
                                row_count
                            );
                        }
                        Err(e) => {
                            tracing::error!("Failed to register data file in catalog: {}", e);
                        }
                    }
                }
            },
        );

        Ok(Box::pin(tracking_stream))
    }
}

/// Implementation of `DeletionTableProvider` for Pepper tables.
///
/// This enables SQL DELETE support through DataFusion. Currently, deletion is not
/// fully implemented for virtual files, as it requires proper row-level deletion
/// tracking via deletion vectors. For now, this returns an error.
#[async_trait]
impl DeletionTableProvider for PepperTableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Create a deletion sink that will handle the actual deletion
        let deletion_sink = Arc::new(PepperDeletionSink {
            catalog: Arc::clone(&self._catalog),
            table_metadata: self.table_metadata.clone(),
            filters: filters.to_vec(),
            listing_table: Arc::clone(&self.listing_table),
        });

        Ok(Arc::new(DeletionExec::new(deletion_sink, &self.schema())))
    }
}

/// Deletion sink for Pepper tables.
///
/// This implements the actual deletion logic by:
/// 1. Reading existing data from the table
/// 2. Filtering out rows matching the deletion criteria
/// 3. Rewriting the table with the remaining rows
/// 4. Returning the count of deleted rows
struct PepperDeletionSink {
    catalog: Arc<dyn MetadataCatalog>,
    table_metadata: TableMetadata,
    filters: Vec<Expr>,
    listing_table: Arc<ListingTable>,
}

#[async_trait]
impl DeletionSink for PepperDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        use datafusion::execution::context::SessionContext;
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::DataFrame;

        // Create a DataFusion context to query the existing data
        let ctx = SessionContext::new();

        // Create a DataFrame from the listing table
        let provider = Arc::clone(&self.listing_table) as Arc<dyn TableProvider>;
        let logical_plan =
            LogicalPlanBuilder::scan("?table?", provider_as_source(provider), None)?.build()?;

        let mut df = DataFrame::new(ctx.state(), logical_plan);

        // Count total rows before deletion
        let count_before = df.clone().count().await?;

        // Apply filters: keep rows that DON'T match the deletion criteria
        // The filters specify which rows to DELETE, so we need to invert them
        for filter in &self.filters {
            df = df.filter(is_not_true(filter.clone()))?;
        }

        // Count rows after filtering (these are the rows we're keeping)
        let count_after = df.clone().count().await?;
        let deleted_count = count_before - count_after;

        // Collect the filtered data (rows that should remain)
        let remaining_batches = df.collect().await?;

        // Rewrite the table with the remaining data
        // For now, we'll write all remaining data as a single new file
        // In the future, this could be optimized to only rewrite affected files

        // Delete all existing virtual files
        let data_files = self
            .catalog
            .get_data_files(self.table_metadata.table_id)
            .await?;
        for data_file in data_files {
            self.catalog
                .delete_data_file(data_file.data_file_id)
                .await?;
            // Also delete the physical directory
            let file_path = std::path::Path::new(&data_file.path);
            if file_path.exists() {
                self.catalog.delete_file_directory(&data_file.path).await?;
            }
        }

        // Also delete all .vortex files in the base table directory
        // This handles files created by regular INSERT operations (not virtual files)
        let table_dir = std::path::Path::new(&self.table_metadata.path);
        if table_dir.exists() && table_dir.is_dir() {
            let mut entries = tokio::fs::read_dir(table_dir).await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read table directory: {e}"
                ))
            })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read directory entry: {e}"
                ))
            })? {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("vortex") {
                    tokio::fs::remove_file(&path).await.map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to delete vortex file {}: {e}",
                            path.display()
                        ))
                    })?;
                }
            }
        }

        // If there are remaining rows, write them back to the table directory
        if !remaining_batches.is_empty() && deleted_count > 0 {
            use datafusion::datasource::memory::MemorySourceConfig;
            use datafusion::datasource::source::DataSourceExec;

            // Write the remaining batches back to the table directory using the listing table
            let source_config = MemorySourceConfig::try_new(
                &[remaining_batches],
                Arc::clone(&self.table_metadata.schema),
                None,
            )?;
            let data_source_exec = Arc::new(DataSourceExec::new(Arc::new(source_config)));

            let insert_ctx = SessionContext::new();
            let insert_plan = self
                .listing_table
                .insert_into(&insert_ctx.state(), data_source_exec, InsertOp::Append)
                .await?;

            // Execute the insert plan to actually write the data
            let insert_results =
                datafusion::physical_plan::collect(insert_plan, insert_ctx.task_ctx()).await?;

            // Extract row count from insert results
            let rows_written: u64 = if !insert_results.is_empty()
                && insert_results[0].num_columns() == 1
                && insert_results[0].num_rows() == 1
            {
                use arrow::array::AsArray;
                let array = insert_results[0].column(0);
                if let Some(count_array) = array.as_primitive_opt::<arrow::datatypes::UInt64Type>()
                {
                    count_array.value(0)
                } else {
                    count_after as u64
                }
            } else {
                count_after as u64
            };

            // Register the written data in catalog
            let mut total_size = 0i64;
            let table_path = &self.table_metadata.path;
            if let Ok(mut entries) = tokio::fs::read_dir(table_path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Ok(metadata) = entry.metadata().await {
                        if entry.path().extension().and_then(|s| s.to_str()) == Some("vortex") {
                            total_size += metadata.len() as i64;
                        }
                    }
                }
            }

            // Get next file_order
            let existing_files = self
                .catalog
                .get_data_files(self.table_metadata.table_id)
                .await?;
            let next_order = existing_files
                .iter()
                .map(|f| f.file_order)
                .max()
                .unwrap_or(0)
                + 1;

            let data_file = super::metadata::DataFile {
                data_file_id: 0,
                table_id: self.table_metadata.table_id,
                file_order: next_order,
                path: table_path.clone(),
                path_is_relative: false,
                file_format: "vortex".to_string(),
                record_count: rows_written as i64,
                file_size_bytes: total_size,
                row_id_start: 0,
            };

            let new_file_id = self.catalog.add_data_file(data_file).await?;

            tracing::info!(
                "Wrote {} remaining rows (data_file_id={}) to table after DELETE",
                rows_written,
                new_file_id
            );
        }

        Ok(deleted_count as u64)
    }
}

/// Implementation of `UpdateTableProvider` trait for Pepper tables.
///
/// Pepper tables support UPDATE operations by:
/// 1. Reading existing data
/// 2. Applying filters to find rows to update
/// 3. Applying assignments to update matching rows
/// 4. Deleting old data files
/// 5. Writing updated data back
/// 6. Creating new catalog entries for the updated data
#[async_trait]
impl UpdateTableProvider for PepperTableProvider {
    async fn update(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
        assignments: std::collections::HashMap<String, Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Create an update sink that will handle the actual update
        let update_sink = Arc::new(PepperUpdateSink {
            catalog: Arc::clone(&self._catalog),
            table_metadata: self.table_metadata.clone(),
            filters: filters.to_vec(),
            assignments,
            listing_table: Arc::clone(&self.listing_table),
        });

        Ok(Arc::new(UpdateExec::new(update_sink, &self.schema())))
    }
}

/// Update sink for Pepper tables.
///
/// This implements the actual update logic by:
/// 1. Reading existing data from the table
/// 2. Applying filters to find rows to update
/// 3. Applying assignments to update matching rows
/// 4. Deleting old data files
/// 5. Writing the updated data back
/// 6. Returning the count of updated rows
struct PepperUpdateSink {
    catalog: Arc<dyn MetadataCatalog>,
    table_metadata: TableMetadata,
    filters: Vec<Expr>,
    assignments: std::collections::HashMap<String, Expr>,
    listing_table: Arc<ListingTable>,
}

#[async_trait]
impl UpdateSink for PepperUpdateSink {
    async fn update(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        use datafusion::execution::context::SessionContext;
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::DataFrame;

        // Create a DataFusion context to query the existing data
        let ctx = SessionContext::new();

        // Create a DataFrame from the listing table
        let provider = Arc::clone(&self.listing_table) as Arc<dyn TableProvider>;
        let logical_plan =
            LogicalPlanBuilder::scan("?table?", provider_as_source(provider), None)?.build()?;

        let mut df = DataFrame::new(ctx.state(), logical_plan);

        // Count total rows before update
        let count_before = df.clone().count().await?;

        // Create two DataFrames: one for rows to update, one for rows to keep unchanged
        let mut df_to_update = df.clone();
        let mut df_unchanged = df;

        // Apply filters to get rows to update
        for filter in &self.filters {
            df_to_update = df_to_update.filter(filter.clone())?;
        }

        // Apply inverse filters to get rows to keep unchanged
        for filter in &self.filters {
            df_unchanged = df_unchanged.filter(is_not_true(filter.clone()))?;
        }

        // Count updated rows
        let update_count = df_to_update.clone().count().await?;

        if update_count == 0 {
            // No rows to update, return early
            return Ok(0);
        }

        // Apply assignments to the rows to update
        for (col_name, expr) in &self.assignments {
            df_to_update = df_to_update.with_column(col_name, expr.clone())?;
        }

        // Collect both DataFrames
        let updated_batches = df_to_update.collect().await?;
        let unchanged_batches = df_unchanged.collect().await?;

        // Delete all existing virtual files
        let data_files = self
            .catalog
            .get_data_files(self.table_metadata.table_id)
            .await?;
        for data_file in data_files {
            self.catalog
                .delete_data_file(data_file.data_file_id)
                .await?;
            // Also delete the physical directory
            let file_path = std::path::Path::new(&data_file.path);
            if file_path.exists() {
                self.catalog.delete_file_directory(&data_file.path).await?;
            }
        }

        // Also delete all .vortex files in the base table directory
        let table_dir = std::path::Path::new(&self.table_metadata.path);
        if table_dir.exists() && table_dir.is_dir() {
            let mut entries = tokio::fs::read_dir(table_dir).await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read table directory: {e}"
                ))
            })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read directory entry: {e}"
                ))
            })? {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("vortex") {
                    tokio::fs::remove_file(&path).await.map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to delete vortex file {}: {e}",
                            path.display()
                        ))
                    })?;
                }
            }
        }

        // Combine updated and unchanged batches
        let all_batches = [unchanged_batches, updated_batches].concat();

        // Write all data back to the table directory
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;

        let source_config = MemorySourceConfig::try_new(
            &[all_batches],
            Arc::clone(&self.table_metadata.schema),
            None,
        )?;
        let data_source_exec = Arc::new(DataSourceExec::new(Arc::new(source_config)));

        let insert_ctx = SessionContext::new();
        let insert_plan = self
            .listing_table
            .insert_into(&insert_ctx.state(), data_source_exec, InsertOp::Append)
            .await?;

        // Execute the insert plan to actually write the data
        let insert_results =
            datafusion::physical_plan::collect(insert_plan, insert_ctx.task_ctx()).await?;

        // Extract row count from insert results
        let rows_written: u64 = if !insert_results.is_empty()
            && insert_results[0].num_columns() == 1
            && insert_results[0].num_rows() == 1
        {
            use arrow::array::AsArray;
            let array = insert_results[0].column(0);
            if let Some(count_array) = array.as_primitive_opt::<arrow::datatypes::UInt64Type>() {
                count_array.value(0)
            } else {
                count_before as u64
            }
        } else {
            count_before as u64
        };

        // Register the written data in catalog
        let mut total_size = 0i64;
        let table_path = &self.table_metadata.path;
        if let Ok(mut entries) = tokio::fs::read_dir(table_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    if entry.path().extension().and_then(|s| s.to_str()) == Some("vortex") {
                        total_size += metadata.len() as i64;
                    }
                }
            }
        }

        // Get next file_order
        let existing_files = self
            .catalog
            .get_data_files(self.table_metadata.table_id)
            .await?;
        let next_order = existing_files
            .iter()
            .map(|f| f.file_order)
            .max()
            .unwrap_or(0)
            + 1;

        let data_file = super::metadata::DataFile {
            data_file_id: 0,
            table_id: self.table_metadata.table_id,
            file_order: next_order,
            path: table_path.clone(),
            path_is_relative: false,
            file_format: "vortex".to_string(),
            record_count: rows_written as i64,
            file_size_bytes: total_size,
            row_id_start: 0,
        };

        let new_file_id = self.catalog.add_data_file(data_file).await?;

        tracing::info!(
            "Wrote {} rows (updated {}, data_file_id={}) to table after UPDATE",
            rows_written,
            update_count,
            new_file_id
        );

        Ok(update_count as u64)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_table_provider_creation() {
        // Tests will be added once SQLite catalog implementation is complete
    }
}
