/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! Deletion sink for Cayenne tables.
//!
//! This module provides `CayenneDeletionSink`, which handles the process of marking
//! rows as deleted by writing deletion vectors to storage.
//!
//! # Deletion Strategies
//!
//! The sink supports three deletion strategies based on table configuration:
//!
//! - **Position-based**: For tables WITHOUT a primary key.
//!   Scans the table to find matching rows and records their file-local positions.
//!   Creates per-file deletion vectors that map to `RoaringBitmap` for efficient
//!   exclusion during Vortex scans.
//!
//! - **Int64 PK-based**: For tables with a single-column Int64 primary key.
//!   Optimized path that extracts PK values directly without `RowConverter` overhead.
//!   Uses `HashMap<i64, i64>` for PK -> `delete_sequence` mapping.
//!
//! - **Key-based**: For tables with composite or non-integer primary keys.
//!   Uses Arrow's `RowConverter` to create deterministic byte keys for lookup.
//!   Uses `HashMap<Box<[u8]>, i64>` for key -> `delete_sequence` mapping.
//!
//! # Workflow
//!
//! 1. Receive deletion request with filter expressions
//! 2. Scan table (and protected snapshots) to find matching rows
//! 3. Extract identifiers (positions or keys) based on deletion strategy
//! 4. Write deletion vectors to storage via `DeletionVectorWriter`
//! 5. Register delete files in catalog
//! 6. Update in-memory caches for immediate query consistency

use super::super::constants::{DELETION_CACHE_LOCK_POISONED, LISTING_TABLE_LOCK_POISONED};
use super::super::table::PkDeletionStrategy;
use super::super::utils::{convert_to_i64_box, convert_to_u64_box};
use super::vector_io::{DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter};
use crate::catalog::{CatalogError, MetadataCatalog};
use crate::metadata::TableMetadata;
use arrow::array::ArrayRef;
use arrow_row::RowConverter;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_catalog::TableProvider;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use datafusion_physical_plan::collect;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Position-based deletion methods implemented in sink/position_based.rs
mod position_based;

/// Deletion sink for Cayenne tables.
///
/// This sink handles the process of marking rows as deleted by writing
/// deletion vectors to storage. Supports three deletion strategies:
/// - Position-based deletion (for tables without primary key)
/// - Int64 PK deletion (for tables with single-column Int64 primary key)
/// - Key-based deletion (for tables with composite/non-integer primary key)
pub struct CayenneDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Reference to the cached position-based deletion vectors.
    /// Maps data file path -> `RoaringBitmap` of file-local row positions.
    /// Uses Arc-wrapped `HashMap` for zero-copy sharing across concurrent operations.
    cached_deleted_row_ids: Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>,
    /// Reference to the cached Int64 PK deletion vectors.
    /// Uses Arc-wrapped `HashMap<i64, i64>` for direct PK lookup (PK -> `delete_sequence`).
    cached_deleted_pk_i64: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
    /// Reference to the cached key-based deletion vectors.
    /// Uses Arc-wrapped `HashMap<Box<[u8]>, i64>` for zero-copy sharing (PK bytes -> `delete_sequence`).
    #[expect(clippy::type_complexity)]
    cached_deleted_row_keys: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
    /// Deletion strategy for this table.
    pk_deletion_strategy: PkDeletionStrategy,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Additional listing tables from protected snapshots that should also be scanned for deletions.
    protected_snapshot_tables: Vec<Arc<ListingTable>>,
}

impl CayenneDeletionSink {
    /// Create a new deletion sink.
    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::type_complexity)]
    pub fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        schema: SchemaRef,
        filters: &[Expr],
        cached_deleted_row_ids: Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>,
        cached_deleted_pk_i64: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
        cached_deleted_row_keys: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
        pk_deletion_strategy: PkDeletionStrategy,
        pk_row_converter: Option<Arc<RowConverter>>,
        pk_column_indices: Vec<usize>,
        protected_snapshot_tables: Vec<Arc<ListingTable>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            cached_deleted_row_ids,
            cached_deleted_pk_i64,
            cached_deleted_row_keys,
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            protected_snapshot_tables,
        }
    }

    async fn delete_all_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // For position-based deletions, we need per-file row tracking
        // For PK-based deletions, we can still batch across all files
        match self.pk_deletion_strategy {
            PkDeletionStrategy::Int64Pk => {
                // Int64 PK deletion - collect all batches and extract PK values
                let mut all_batches = Vec::new();
                for table in tables {
                    let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
                    let batches = collect(scan_plan, ctx.task_ctx()).await?;
                    all_batches.extend(batches);
                }

                if all_batches.is_empty() {
                    return Ok(0);
                }

                let concatenated_batch =
                    arrow::compute::concat_batches(&self.schema, &all_batches)?;
                let pk_values = self.extract_int64_pk_values(&concatenated_batch)?;
                self.persist_int64_pk_deletions(pk_values).await
            }
            PkDeletionStrategy::RowConverterBased => {
                // RowConverter-based deletion for composite/non-integer PKs
                let Some(ref row_converter) = self.pk_row_converter else {
                    return Err("RowConverter not available for RowConverterBased strategy".into());
                };

                let mut all_batches = Vec::new();
                for table in tables {
                    let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
                    let batches = collect(scan_plan, ctx.task_ctx()).await?;
                    all_batches.extend(batches);
                }

                if all_batches.is_empty() {
                    return Ok(0);
                }

                let concatenated_batch =
                    arrow::compute::concat_batches(&self.schema, &all_batches)?;
                let row_keys = self.extract_row_keys(&concatenated_batch, row_converter)?;
                self.persist_key_based_deletions(row_keys).await
            }
            PkDeletionStrategy::PositionBased => {
                // Position-based deletion for "delete all" (delete w/o filters)
                // Note: Delete all is NOT available using retention so this is unreachable
                Err("Position-based deletion without primary key is not yet supported for delete-all operations".into())
            }
        }
    }

    // NOTE: delete_filtered_rows_streaming_position_based is implemented in sink/position_based.rs

    /// Extract Int64 primary key values from a batch.
    fn extract_int64_pk_values(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::Int64Array;

        // For Int64 PK strategy, we only have one PK column
        let pk_column_index = self
            .pk_column_indices
            .first()
            .ok_or("Int64 PK strategy requires exactly one PK column index")?;

        let pk_column = batch.column(*pk_column_index);
        let pk_array = pk_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!(
                    "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                    pk_column.data_type()
                )
            })?;

        let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
        Ok(pk_values)
    }

    /// Extract row keys from a batch using the `RowConverter`.
    fn extract_row_keys(
        &self,
        batch: &arrow::array::RecordBatch,
        row_converter: &RowConverter,
    ) -> Result<Vec<Box<[u8]>>, Box<dyn std::error::Error + Send + Sync>> {
        let pk_columns: Vec<ArrayRef> = self
            .pk_column_indices
            .iter()
            .map(|&idx| Arc::clone(batch.column(idx)))
            .collect();

        let rows = row_converter.convert_columns(&pk_columns)?;

        let row_keys: Vec<Box<[u8]>> = rows
            .iter()
            .map(|row| row.as_ref().to_vec().into_boxed_slice())
            .collect();

        Ok(row_keys)
    }

    async fn delete_filtered_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Array, Int64Array};
        use arrow::datatypes::{DataType, Field};

        // For position-based deletion, use the streaming per-file approach directly.
        // This avoids loading all data into memory and provides correct file-local row IDs.
        if self.pk_deletion_strategy == PkDeletionStrategy::PositionBased {
            return self.delete_filtered_rows_position_based(ctx, tables).await;
        }

        // PK-based strategies (Int64Pk, RowConverterBased) need to scan all data
        // to extract primary key values for deletion.

        // Collect batches from all tables
        let mut all_batches = Vec::new();
        for table in tables {
            let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
            let batches = collect(scan_plan, ctx.task_ctx()).await?;
            all_batches.extend(batches);
        }

        // If no data, nothing to delete
        if all_batches.is_empty() {
            return Ok(0);
        }

        // Flatten all batches into one for simpler processing
        let concatenated_batch = arrow::compute::concat_batches(&self.schema, &all_batches)?;
        let total_rows = concatenated_batch.num_rows();

        // Create a batch with row_id column added (needed for filtering)
        let row_id_array =
            Int64Array::from_iter_values(0..convert_to_i64_box(total_rows, "total rows")?);

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

        // Apply all filters with type coercion
        // The filter expressions may have been parsed with different literal types (e.g., Int64)
        // than the actual column types in the data (e.g., Int32). We use TypeCoercionRewriter
        // to insert appropriate CAST operations to make the comparison valid.
        let df_schema = DFSchema::try_from(schema_with_rowid.as_ref().clone())?;
        for filter in &self.filters {
            let mut rewriter = TypeCoercionRewriter::new(&df_schema);
            let coerced_filter = filter.clone().rewrite(&mut rewriter)?.data;
            filtered_df = filtered_df.filter(coerced_filter)?;
        }

        // Collect filtered results (includes __row_id and all original columns)
        let filtered_batches = filtered_df.collect().await?;

        if filtered_batches.is_empty() {
            return Ok(0);
        }

        // Use the appropriate deletion strategy based on table configuration
        match self.pk_deletion_strategy {
            PkDeletionStrategy::Int64Pk => {
                // Int64 PK deletion - extract PK values directly
                // Note: column indices are offset by 1 because __row_id is at index 0
                let first_batch = filtered_batches
                    .first()
                    .ok_or("Expected at least one batch after filtering (checked above)")?;
                let filtered_concat =
                    arrow::compute::concat_batches(&first_batch.schema(), &filtered_batches)?;

                let pk_column_index = self
                    .pk_column_indices
                    .first()
                    .ok_or("Int64 PK strategy requires exactly one PK column index")?;

                let pk_column = filtered_concat.column(*pk_column_index + 1); // +1 for __row_id offset
                let pk_array =
                    pk_column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            format!(
                        "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                        pk_column.data_type()
                    )
                        })?;

                let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
                self.persist_int64_pk_deletions(pk_values).await
            }
            PkDeletionStrategy::RowConverterBased => {
                // RowConverter-based deletion for composite/non-integer PKs
                if let Some(ref row_converter) = self.pk_row_converter {
                    let filtered_concat = arrow::compute::concat_batches(
                        &filtered_batches[0].schema(),
                        &filtered_batches,
                    )?;

                    let pk_columns: Vec<ArrayRef> = self
                        .pk_column_indices
                        .iter()
                        .map(|&idx| Arc::clone(filtered_concat.column(idx + 1))) // +1 for __row_id offset
                        .collect();

                    let rows = row_converter.convert_columns(&pk_columns)?;
                    let row_keys: Vec<Box<[u8]>> = rows
                        .iter()
                        .map(|row| row.as_ref().to_vec().into_boxed_slice())
                        .collect();

                    self.persist_key_based_deletions(row_keys).await
                } else {
                    Err("RowConverter not available for RowConverterBased strategy".into())
                }
            }
            PkDeletionStrategy::PositionBased => {
                unreachable!("PositionBased strategy should have returned early via delete_filtered_rows_streaming_position_based")
            }
        }
    }

    async fn persist_key_based_deletions(
        &self,
        row_keys: Vec<Box<[u8]>>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_row_keys = Self::filter_existing_key_deletions(row_keys);

        if filtered_row_keys.is_empty() {
            return Ok(0);
        }

        // Count how many keys are NEW deletions (not already in the cache).
        // This gives an accurate count of newly deleted rows for the return value.
        let new_deletion_count = {
            let guard = self
                .cached_deleted_row_keys
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;
            filtered_row_keys
                .iter()
                .filter(|key| !guard.contains_key(key.as_ref()))
                .count()
        };

        // Get a fresh sequence number from the catalog for this deletion operation.
        // This ensures each delete has a unique, monotonically increasing sequence number
        // that's higher than any previous operation.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        // Create a temporary metadata with the fresh sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(
                filtered_row_keys,
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

        // Extract row keys from the result
        let written_row_keys = match &result.identifiers {
            DeletionIdentifier::KeyBased(keys) => keys,
            DeletionIdentifier::PositionBased { .. } => {
                return Err("Unexpected position-based deletion in key-based sink".into());
            }
        };

        // Update the cached deletion keys with sequence number
        {
            let mut guard = self
                .cached_deleted_row_keys
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            let mut updated_map = (**guard).clone();
            for key in written_row_keys {
                // Update with max sequence if key already exists
                updated_map
                    .entry(key.clone())
                    .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                    .or_insert(delete_sequence);
            }

            *guard = Arc::new(updated_map);
        }

        let deleted_count = convert_to_u64_box(new_deletion_count, "deleted row count")?;

        tracing::debug!(
            "Key-based deletion vector written and cache updated: {} key(s) (seq={}) at {:?}",
            deleted_count,
            delete_sequence,
            result.path
        );

        Ok(deleted_count)
    }

    async fn persist_int64_pk_deletions(
        &self,
        pk_values: Vec<i64>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_pk_values = Self::filter_existing_int64_pk_deletions(pk_values);

        if filtered_pk_values.is_empty() {
            return Ok(0);
        }

        // Count how many PKs are NEW deletions (not already in the cache).
        // This gives an accurate count of newly deleted rows for the return value.
        let new_deletion_count = {
            let guard = self
                .cached_deleted_pk_i64
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;
            filtered_pk_values
                .iter()
                .filter(|pk| !guard.contains_key(*pk))
                .count()
        };

        // Get a fresh sequence number from the catalog for this deletion operation.
        // This ensures each delete has a unique, monotonically increasing sequence number
        // that's higher than any previous operation.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        // For Int64 PK deletions, we store them as key-based deletions
        // where each key is the 8-byte big-endian representation of the i64 value.
        // This allows efficient storage and lookup.
        let row_keys: Vec<Box<[u8]>> = filtered_pk_values
            .iter()
            .map(|&pk| pk.to_be_bytes().to_vec().into_boxed_slice())
            .collect();

        // Create a temporary metadata with the fresh sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(row_keys)])
            .await
            .map_err(catalog_error_to_box)?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog
            .add_delete_file(result.delete_file)
            .await
            .map_err(catalog_error_to_box)?;

        // Update the cached Int64 PK deletion map with sequence number
        {
            let mut guard = self
                .cached_deleted_pk_i64
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            let mut updated_map = (**guard).clone();
            for &pk_value in &filtered_pk_values {
                // Update with max sequence if key already exists
                updated_map
                    .entry(pk_value)
                    .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                    .or_insert(delete_sequence);
            }

            *guard = Arc::new(updated_map);
        }

        let deleted_count = convert_to_u64_box(new_deletion_count, "deleted row count")?;

        tracing::debug!(
            "Int64 PK deletion vector written and cache updated: {} key(s) (seq={}) at {:?}",
            deleted_count,
            delete_sequence,
            result.path
        );

        Ok(deleted_count)
    }

    fn filter_existing_int64_pk_deletions(pk_values: Vec<i64>) -> Vec<i64> {
        // For sequence-based ordering, we MUST write new deletion files even for
        // PKs that were already deleted, because the new deletion has a higher
        // sequence number. This ensures proper ordering: data written after the
        // first delete but before the second delete will be properly filtered.
        //
        // We only deduplicate within the current batch (in DeletionVectorWriter).
        pk_values
    }

    fn filter_existing_key_deletions(row_keys: Vec<Box<[u8]>>) -> Vec<Box<[u8]>> {
        // For sequence-based ordering, we MUST write new deletion files even for
        // PKs that were already deleted, because the new deletion has a higher
        // sequence number. This ensures proper ordering: data written after the
        // first delete but before the second delete will be properly filtered.
        //
        // We only deduplicate within the current batch (in DeletionVectorWriter).
        row_keys
    }
}

pub(super) fn catalog_error_to_box(err: CatalogError) -> Box<dyn std::error::Error + Send + Sync> {
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
                .map_err(|_| LISTING_TABLE_LOCK_POISONED.to_string())?;
            Arc::clone(&guard)
        };

        // Collect all tables to scan: main listing table + protected snapshots
        let mut all_tables = vec![Arc::clone(&listing_table)];
        for protected_table in &self.protected_snapshot_tables {
            all_tables.push(Arc::clone(protected_table));
        }

        if self.filters.is_empty() {
            return self.delete_all_rows_from_tables(&ctx, &all_tables).await;
        }

        self.delete_filtered_rows_from_tables(&ctx, &all_tables)
            .await
    }
}
