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
use datafusion_common::Result as DataFusionResult;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::collect;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use std::any::Any;
use std::borrow::Cow;
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

        // Create listing table for the Vortex files
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
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::items_after_statements)]
    pub async fn insert(&self, stream: SendableRecordBatchStream) -> CatalogResult<u64> {
        // Count rows as we collect the stream
        let mut row_count = 0u64;
        let schema = stream.schema();

        let batches: Vec<_> = stream
            .collect::<Vec<DataFusionResult<_>>>()
            .await
            .into_iter()
            .collect::<DataFusionResult<Vec<_>>>()
            .map_err(|e| super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to collect record batches: {e}"),
            })?;

        for batch in &batches {
            row_count += batch.num_rows() as u64;
        }

        if row_count == 0 {
            return Ok(0);
        }

        // Create a new stream from the collected batches for insert_into
        let batch_stream = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, datafusion_common::DataFusionError>),
        );
        let stream_adapter = RecordBatchStreamAdapter::new(
            Arc::<arrow_schema::Schema>::clone(&schema),
            batch_stream,
        );
        let new_stream: DFStream = Box::pin(stream_adapter);

        // Create a simple execution plan that emits the stream
        struct StreamExec {
            schema: arrow_schema::SchemaRef,
            stream: tokio::sync::Mutex<Option<DFStream>>,
        }

        impl std::fmt::Debug for StreamExec {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("StreamExec").finish()
            }
        }

        impl DisplayAs for StreamExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "StreamExec")
            }
        }

        impl ExecutionPlan for StreamExec {
            fn name(&self) -> &'static str {
                "StreamExec"
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn schema(&self) -> arrow_schema::SchemaRef {
                Arc::<arrow_schema::Schema>::clone(&self.schema)
            }

            fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
                unimplemented!("properties not needed for simple insert")
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
                let stream = self.stream.blocking_lock().take();
                stream.ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Stream already consumed".to_string(),
                    )
                })
            }
        }

        let stream_exec = Arc::new(StreamExec {
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            stream: tokio::sync::Mutex::new(Some(new_stream)),
        });

        // Create a session context for executing the insert
        let ctx = SessionContext::new();
        let state = ctx.state();

        // Delegate to ListingTable's insert_into to write Vortex files
        let insert_plan = self
            .listing_table
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

        // The results should contain a single row with the count of inserted rows
        // but we already counted them ourselves, so we can just return that
        tracing::debug!("Insert completed, wrote {} rows to Vortex", row_count);
        tracing::debug!("Insert plan returned {} result batches", results.len());

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
        // Delegate to the underlying listing table for Vortex file writing
        // The listing table will use VortexSink to write the data
        // In the future, we would also:
        // 1. Track the written files in the Pepper catalog
        // 2. Handle primary key constraints if needed
        self.listing_table
            .insert_into(state, input, overwrite)
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
