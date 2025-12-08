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

//! Deletion logic for Cayenne tables.
//!
//! This module provides:
//! - `DeletionFilterExec`: An execution plan that filters out deleted rows
//! - `DeletionFilterStream`: A stream that applies deletion filtering
//! - `CayenneDeletionSink`: Handles writing deletion vectors to storage

use super::constants::{DEFAULT_DATA_FILE_ID, DELETION_CACHE_LOCK_POISONED};
use super::utils::{convert_to_i64, convert_to_i64_box, convert_to_u64_box};
use crate::catalog::{CatalogError, MetadataCatalog};
use crate::deletion::{DeletionVectorWriteSpec, DeletionVectorWriter};
use crate::metadata::TableMetadata;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::Expr;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use roaring::RoaringBitmap;
use std::any::Any;
use std::sync::{Arc, RwLock};

/// Execution plan that filters out deleted rows based on deletion vectors.
///
/// This wraps another execution plan and removes rows whose positions
/// match the deleted row IDs loaded from deletion vector files.
///
/// # Zero-Copy Design
///
/// The deleted row IDs are wrapped in `Arc` to enable zero-copy sharing across
/// concurrent scans. This avoids cloning potentially large bitmaps on every scan,
/// aligning with the project's zero-copy principles for Arrow data.
pub struct DeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    deleted_row_ids: Arc<RoaringBitmap>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl DeletionFilterExec {
    /// Create a new deletion filter execution plan.
    pub fn new(input: Arc<dyn ExecutionPlan>, deleted_row_ids: Arc<RoaringBitmap>) -> Self {
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
            Arc::clone(&self.deleted_row_ids),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        // Zero-copy Arc clone - just increments reference count
        let deleted_row_ids = Arc::clone(&self.deleted_row_ids);
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
pub struct DeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_row_ids: Arc<RoaringBitmap>,
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
                        "DeletionFilterStream: processing batch with {} rows, offset {}, deleted_row_ids count: {}",
                        batch_size, current_offset, self.deleted_row_ids.len()
                    );

                    // Optimization: Build a boolean mask for vectorized filtering using Arrow compute kernels
                    // This is more efficient than building indices and using the take kernel
                    // Pre-allocate with capacity for the entire batch
                    let mut keep_mask = Vec::with_capacity(batch_size);

                    // Vectorized row filtering: batch the contains() checks for better performance
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

                        // Check if row is deleted using RoaringBitmap's u32 API
                        // RoaringBitmap uses u32 internally. Row IDs >= 2^32 should trigger compaction
                        // rather than being supported directly.
                        let is_deleted = if let Ok(row_id_u32) = u32::try_from(global_row_id) {
                            self.deleted_row_ids.contains(row_id_u32)
                        } else {
                            // Row ID exceeds u32::MAX - this indicates table needs compaction
                            tracing::warn!(
                                "Row ID {} exceeds u32::MAX - table should be compacted to clear deletion vectors",
                                global_row_id
                            );
                            false
                        };

                        // Build boolean mask: true = keep, false = delete
                        keep_mask.push(!is_deleted);
                    }

                    // Count how many rows we're keeping
                    let keep_count = keep_mask.iter().filter(|&&v| v).count();

                    tracing::debug!(
                        "DeletionFilterStream: keeping {} of {} rows",
                        keep_count,
                        batch_size
                    );

                    // If all rows are deleted, skip this batch and continue to next
                    if keep_count == 0 {
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is (fast path)
                    if keep_count == batch_size {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Use Arrow's filter kernel with boolean array for SIMD-optimized filtering
                    // This is faster than the take kernel with indices for this use case
                    let filter_array = arrow::array::BooleanArray::from(keep_mask);
                    let filtered_batch =
                        match arrow::compute::filter_record_batch(&batch, &filter_array) {
                            Ok(filtered) => filtered,
                            Err(e) => {
                                return std::task::Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    ),
                                )));
                            }
                        };

                    return std::task::Poll::Ready(Some(Ok(filtered_batch)));
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

/// Deletion sink for Cayenne tables.
///
/// This sink handles the process of marking rows as deleted by writing
/// deletion vectors to storage.
pub struct CayenneDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Reference to the cached deletion vectors to invalidate after writing new deletions.
    /// Uses Arc-wrapped `RoaringBitmap` for zero-copy sharing across concurrent operations.
    cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
}

impl CayenneDeletionSink {
    /// Create a new deletion sink.
    pub fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        schema: SchemaRef,
        filters: &[Expr],
        cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            cached_deleted_row_ids,
        }
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

        self.persist_deletions(row_ids, DEFAULT_DATA_FILE_ID).await
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

        self.persist_deletions(row_ids, DEFAULT_DATA_FILE_ID).await
    }

    async fn persist_deletions(
        &self,
        row_ids: Vec<i64>,
        data_file_id: i64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_row_ids = self.filter_existing_deletions(row_ids).await?;

        if filtered_row_ids.is_empty() {
            return Ok(0);
        }

        let writer = DeletionVectorWriter::new(&self.table_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new(
                data_file_id,
                filtered_row_ids,
            )])
            .await
            .map_err(catalog_error_to_box)?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog
            .add_delete_file(result.delete_file)
            .await
            .map_err(catalog_error_to_box)?;

        // Update the cached deletion vectors with the newly deleted row IDs
        // This avoids needing to reload from SQLite on the next scan.
        //
        // We create a new Arc with the updated HashSet to maintain zero-copy semantics
        // for concurrent readers who still hold references to the old Arc. This requires
        // cloning the entire HashSet, which is acceptable because:
        //
        // 1. **Write infrequency**: Deletions happen much less frequently than reads
        // 2. **Concurrent reader safety**: Cloning prevents disrupting ongoing scans that
        //    hold Arc references to the old deletion set
        // 3. **Cache coherence**: The alternative (in-place mutation) would require either:
        //    - Taking write locks during scans (blocks all concurrent queries)
        //    - Complex lock-free data structures (higher complexity, potential performance issues)
        //
        // For tables with very large deletion sets (millions of deleted rows), consider
        // running compaction to physically remove deleted rows and reset the deletion vectors.
        {
            let mut guard = self
                .cached_deleted_row_ids
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            // Clone the entire RoaringBitmap and add new deletions.
            // Cost: O(n) where n = existing deleted rows, but this is write path (infrequent).
            // Benefit: Zero-copy Arc clones for concurrent readers (frequent).
            let mut updated_set = (**guard).clone();
            // Convert i64 row IDs to u32 for RoaringBitmap
            for &row_id in &result.row_ids {
                if let Ok(row_id_u32) = u32::try_from(row_id) {
                    updated_set.insert(row_id_u32);
                } else {
                    tracing::warn!(
                        "Skipping row ID {} that exceeds u32::MAX - table should be compacted",
                        row_id
                    );
                }
            }

            // Replace with new Arc - concurrent readers still have old Arc
            *guard = Arc::new(updated_set);
        }

        let deleted_count = convert_to_u64_box(result.row_ids.len(), "deleted row count")?;

        tracing::debug!(
            "Deletion vector written and cache updated: {} row(s) at {:?}",
            deleted_count,
            result.path
        );

        Ok(deleted_count)
    }

    async fn filter_existing_deletions(
        &self,
        row_ids: Vec<i64>,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let delete_files = self
            .catalog
            .get_table_delete_files(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        if delete_files.is_empty() {
            return Ok(row_ids);
        }

        let delete_files_for_read = delete_files.clone();
        let existing_row_ids =
            tokio::task::spawn_blocking(move || read_deletion_vectors(delete_files_for_read))
                .await
                .map_err(|source| catalog_error_to_box(CatalogError::TaskJoin { source }))?
                .map_err(|err| {
                    catalog_error_to_box(CatalogError::InvalidOperation {
                        message: format!("Failed to read existing deletion vectors: {err}"),
                    })
                })?;

        // Filter out row_ids that already exist in deletion vectors
        Ok(row_ids
            .into_iter()
            .filter(|&row_id| {
                // Convert i64 to u32 for RoaringBitmap lookup
                if let Ok(row_id_u32) = u32::try_from(row_id) {
                    !existing_row_ids.contains(row_id_u32)
                } else {
                    // Row ID exceeds u32::MAX - keep it (not in bitmap)
                    true
                }
            })
            .collect())
    }
}

fn catalog_error_to_box(err: CatalogError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(err)
}

#[async_trait]
impl DeletionSink for CayenneDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();

        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|_| super::constants::LISTING_TABLE_LOCK_POISONED.to_string())?;
            Arc::clone(&guard)
        };

        if self.filters.is_empty() {
            return self.delete_all_rows(&ctx, Arc::clone(&listing_table)).await;
        }

        self.delete_filtered_rows(&ctx, listing_table).await
    }
}

/// Read deletion vectors from files and return a `RoaringBitmap` of deleted row IDs.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** operations using `std::fs::File::open`
/// and must be called from within `tokio::task::spawn_blocking` to avoid blocking the async
/// runtime. The caller is responsible for offloading this to a blocking task context.
///
/// See: Project coding guidelines on Async/Blocking Patterns
///
/// # Design Constraints
///
/// `RoaringBitmap` uses u32 internally, supporting row IDs 0 to ~4 billion. Row IDs beyond
/// `u32::MAX` are logged as warnings and skipped. Tables with excessive deleted rows should
/// trigger compaction to remove deleted rows and clear deletion vectors. Large deletion vector
/// sets indicate poor table health and severely degrade query performance.
///
/// # Performance Optimizations
///
/// Uses `RoaringBitmap` for:
/// - SIMD-optimized contains operations (used in hot read path)
/// - 50-90% memory savings vs `HashSet` for sparse deletions
/// - Efficient bulk insertion using Arrow's contiguous arrays
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
pub fn read_deletion_vectors(
    delete_files: Vec<crate::metadata::DeleteFile>,
) -> datafusion_common::Result<RoaringBitmap> {
    use arrow::array::Array;
    use arrow::ipc::reader::FileReader;

    let mut deleted_row_ids = RoaringBitmap::new();
    let file_count = delete_files.len();

    tracing::debug!(
        "read_deletion_vectors: processing {} delete files",
        file_count
    );

    // Track overflow occurrences to log once at the end instead of per-row
    let mut overflow_count: u64 = 0;
    let mut first_overflow_id: Option<i64> = None;

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

            // Optimized bulk insertion using Arrow's contiguous values slice
            // This is SIMD-friendly and avoids per-element overhead
            let values = row_id_array.values(); // &[i64] - contiguous memory

            if row_id_array.null_count() == 0 {
                // Fast path: no nulls, bulk insert entire slice
                for &row_id in values {
                    // Convert i64 to u32 for RoaringBitmap
                    // Row IDs >= 4 billion should have triggered compaction
                    if let Ok(row_id_u32) = u32::try_from(row_id) {
                        deleted_row_ids.insert(row_id_u32);
                    } else {
                        // Track overflow for single warning at end
                        if first_overflow_id.is_none() {
                            first_overflow_id = Some(row_id);
                        }
                        overflow_count += 1;
                    }
                }
            } else {
                // Slow path: check validity bitmap for nulls
                for i in 0..row_id_array.len() {
                    if row_id_array.is_valid(i) {
                        let row_id = values[i];
                        if let Ok(row_id_u32) = u32::try_from(row_id) {
                            deleted_row_ids.insert(row_id_u32);
                        } else {
                            // Track overflow for single warning at end
                            if first_overflow_id.is_none() {
                                first_overflow_id = Some(row_id);
                            }
                            overflow_count += 1;
                        }
                    }
                }
            }
        }
    }

    // Log once if any overflows occurred, avoiding hot path log spam
    if overflow_count > 0 {
        tracing::warn!(
            "Skipped {} row ID(s) that exceed u32::MAX (first: {}) - table should be compacted to remove deleted rows",
            overflow_count,
            first_overflow_id.unwrap_or(0)
        );
    }

    tracing::debug!(
        "Loaded {} deleted row IDs from {} deletion vector files",
        deleted_row_ids.len(),
        file_count
    );

    Ok(deleted_row_ids)
}
