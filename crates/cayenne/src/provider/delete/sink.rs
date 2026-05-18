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

use super::super::Error;
use super::super::deletion_strategy::{
    Int64PkDeletionSnapshot, PkDeletionStrategyWithCache, RowConverterDeletionSnapshot,
};
use super::super::utils::convert_to_u64_box;
use super::vector_io::{DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter};
use crate::catalog::MetadataCatalog;
use crate::metadata::TableMetadata;
use arc_swap::ArcSwap;
use arrow::array::ArrayRef;
use arrow_row::RowConverter;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_plan::{collect, execute_stream};
use datafusion_catalog::TableProvider;
use datafusion_common::DFSchema;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

// Position-based deletion methods implemented in sink/position_based.rs
mod position_based;

// File-based deletion for time-based retention (deletes entire expired files)
pub(crate) mod file_based;

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
    listing_table: Arc<ArcSwap<ListingTable>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Deletion strategy for this table, with embedded caches.
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Additional listing tables from protected snapshots that should also be scanned for deletions.
    protected_snapshot_tables: Vec<Arc<ListingTable>>,
    /// Shared `RuntimeEnv` for S3 object store access.
    runtime_env: Arc<RuntimeEnv>,
    /// Shared write lock to prevent concurrent writes/refreshes from racing with deletions.
    /// `None` when the caller already holds the write lock (e.g. retention filters applied
    /// during `write_all_append`).
    write_lock: Option<Arc<TokioMutex<()>>>,
}

impl CayenneDeletionSink {
    /// Create a new deletion sink.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<ArcSwap<ListingTable>>,
        schema: SchemaRef,
        filters: &[Expr],
        pk_deletion_strategy: PkDeletionStrategyWithCache,
        pk_row_converter: Option<Arc<RowConverter>>,
        pk_column_indices: Vec<usize>,
        protected_snapshot_tables: Vec<Arc<ListingTable>>,
        runtime_env: Arc<RuntimeEnv>,
        write_lock: Option<Arc<TokioMutex<()>>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            protected_snapshot_tables,
            runtime_env,
            write_lock,
        }
    }

    async fn delete_all_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> super::super::Result<u64> {
        let table_name = &self.table_metadata.table_name;
        // For position-based deletions, we need per-file row tracking
        // For PK-based deletions, we can still batch across all files
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
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
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                // RowConverter-based deletion for composite/non-integer PKs
                let Some(ref row_converter) = self.pk_row_converter else {
                    return Err(Error::Internal {
                        table: table_name.clone(),
                        message: "RowConverter not available for RowConverterBased strategy"
                            .to_string(),
                    });
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
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based deletion for "delete all" (delete w/o filters)
                // Note: Delete all is NOT available using retention so this is unreachable
                Err(Error::Internal {
                    table: table_name.clone(),
                    message: "Position-based deletion without primary key is not yet supported for delete-all operations".to_string(),
                })
            }
        }
    }

    // NOTE: delete_filtered_rows_streaming_position_based is implemented in sink/position_based.rs

    /// Extract Int64 primary key values from a batch.
    fn extract_int64_pk_values(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> super::super::Result<Vec<i64>> {
        use arrow::array::Int64Array;

        let table_name = &self.table_metadata.table_name;

        // For Int64 PK strategy, we only have one PK column
        let pk_column_index = self
            .pk_column_indices
            .first()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: "Int64 PK strategy requires exactly one PK column index".to_string(),
            })?;

        let pk_column = batch.column(*pk_column_index);
        let pk_array = pk_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: format!(
                    "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                    pk_column.data_type()
                ),
            })?;

        let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
        Ok(pk_values)
    }

    /// Extract row keys from a batch using the `RowConverter`.
    fn extract_row_keys(
        &self,
        batch: &arrow::array::RecordBatch,
        row_converter: &RowConverter,
    ) -> super::super::Result<Vec<Box<[u8]>>> {
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
    ) -> super::super::Result<u64> {
        const PK_DELETE_FLUSH_BATCH_SIZE: usize = 50_000;

        let table_name = &self.table_metadata.table_name;

        // For position-based deletion, use the streaming per-file approach directly.
        // This avoids loading all data into memory and provides correct file-local row IDs.
        if self.pk_deletion_strategy.is_position_based() {
            return self.delete_filtered_rows_position_based(ctx, tables).await;
        }

        let coerced_filters = self.coerce_filters_for_schema()?;
        let physical_filters = self.build_physical_filters(&coerced_filters)?;

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                let mut pending_pk_values: Vec<i64> =
                    Vec::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence: Option<i64> = None;
                let mut deleted_rows: u64 = 0;

                for table in tables {
                    let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;

                    while let Some(batch_result) = stream.next().await {
                        let batch =
                            self.apply_physical_filters_to_batch(batch_result?, &physical_filters)?;
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        pending_pk_values.extend(self.extract_int64_pk_values(&batch)?);

                        if pending_pk_values.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let chunk_values = std::mem::take(&mut pending_pk_values);
                            let chunk_deleted = self
                                .persist_int64_pk_chunk_with_shared_sequence(
                                    chunk_values,
                                    &mut delete_sequence,
                                )
                                .await?;
                            deleted_rows =
                                deleted_rows.checked_add(chunk_deleted).ok_or_else(|| {
                                    Error::Internal {
                                        table: table_name.clone(),
                                        message: "Deleted row count overflowed u64".to_string(),
                                    }
                                })?;
                        }
                    }
                }

                if !pending_pk_values.is_empty() {
                    let chunk_values = std::mem::take(&mut pending_pk_values);
                    let chunk_deleted = self
                        .persist_int64_pk_chunk_with_shared_sequence(
                            chunk_values,
                            &mut delete_sequence,
                        )
                        .await?;
                    deleted_rows =
                        deleted_rows
                            .checked_add(chunk_deleted)
                            .ok_or_else(|| Error::Internal {
                                table: table_name.clone(),
                                message: "Deleted row count overflowed u64".to_string(),
                            })?;
                }

                Ok(deleted_rows)
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let Some(row_converter) = self.pk_row_converter.as_ref() else {
                    return Err(Error::Internal {
                        table: table_name.clone(),
                        message: "RowConverter not available for RowConverterBased strategy"
                            .to_string(),
                    });
                };

                let mut pending_row_keys: Vec<Box<[u8]>> =
                    Vec::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence: Option<i64> = None;
                let mut deleted_rows: u64 = 0;

                for table in tables {
                    let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;

                    while let Some(batch_result) = stream.next().await {
                        let batch =
                            self.apply_physical_filters_to_batch(batch_result?, &physical_filters)?;
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        pending_row_keys.extend(self.extract_row_keys(&batch, row_converter)?);

                        if pending_row_keys.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let chunk_keys = std::mem::take(&mut pending_row_keys);
                            let chunk_deleted = self
                                .persist_key_based_chunk_with_shared_sequence(
                                    chunk_keys,
                                    &mut delete_sequence,
                                )
                                .await?;
                            deleted_rows =
                                deleted_rows.checked_add(chunk_deleted).ok_or_else(|| {
                                    Error::Internal {
                                        table: table_name.clone(),
                                        message: "Deleted row count overflowed u64".to_string(),
                                    }
                                })?;
                        }
                    }
                }

                if !pending_row_keys.is_empty() {
                    let chunk_keys = std::mem::take(&mut pending_row_keys);
                    let chunk_deleted = self
                        .persist_key_based_chunk_with_shared_sequence(
                            chunk_keys,
                            &mut delete_sequence,
                        )
                        .await?;
                    deleted_rows =
                        deleted_rows
                            .checked_add(chunk_deleted)
                            .ok_or_else(|| Error::Internal {
                                table: table_name.clone(),
                                message: "Deleted row count overflowed u64".to_string(),
                            })?;
                }

                Ok(deleted_rows)
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                unreachable!(
                    "PositionBased strategy should have returned early via delete_filtered_rows_position_based"
                )
            }
        }
    }

    fn coerce_filters_for_schema(&self) -> super::super::Result<Vec<Expr>> {
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let mut coerced_filters = Vec::with_capacity(self.filters.len());

        for filter in &self.filters {
            let mut rewriter = TypeCoercionRewriter::new(&df_schema);
            coerced_filters.push(filter.clone().rewrite(&mut rewriter)?.data);
        }

        Ok(coerced_filters)
    }

    fn build_physical_filters(
        &self,
        filters: &[Expr],
    ) -> super::super::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        let physical_filters = filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, &execution_props))
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        Ok(physical_filters)
    }

    fn apply_physical_filters_to_batch(
        &self,
        mut batch: arrow::record_batch::RecordBatch,
        physical_filters: &[Arc<dyn PhysicalExpr>],
    ) -> super::super::Result<arrow::record_batch::RecordBatch> {
        let table_name = &self.table_metadata.table_name;

        for filter in physical_filters {
            if batch.num_rows() == 0 {
                break;
            }

            let filter_value = filter.evaluate(&batch)?;
            let filter_array = filter_value.into_array(batch.num_rows())?;
            let filter_array = filter_array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: format!(
                        "Filter expression did not evaluate to BooleanArray, got {:?}",
                        filter_array.data_type()
                    ),
                })?;

            batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
        }

        Ok(batch)
    }

    async fn persist_key_based_chunk_with_shared_sequence(
        &self,
        row_keys: Vec<Box<[u8]>>,
        delete_sequence: &mut Option<i64>,
    ) -> super::super::Result<u64> {
        if row_keys.is_empty() {
            return Ok(0);
        }

        let sequence = if let Some(sequence) = delete_sequence {
            *sequence
        } else {
            let sequence = self
                .catalog
                .increment_sequence_number(&self.table_metadata.table_id)
                .await?;
            *delete_sequence = Some(sequence);
            sequence
        };

        self.persist_key_based_deletions_with_sequence(row_keys, sequence)
            .await
    }

    async fn persist_int64_pk_chunk_with_shared_sequence(
        &self,
        pk_values: Vec<i64>,
        delete_sequence: &mut Option<i64>,
    ) -> super::super::Result<u64> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let sequence = if let Some(sequence) = delete_sequence {
            *sequence
        } else {
            let sequence = self
                .catalog
                .increment_sequence_number(&self.table_metadata.table_id)
                .await?;
            *delete_sequence = Some(sequence);
            sequence
        };

        self.persist_int64_pk_deletions_with_sequence(pk_values, sequence)
            .await
    }

    async fn persist_key_based_deletions(
        &self,
        row_keys: Vec<Box<[u8]>>,
    ) -> super::super::Result<u64> {
        let filtered_row_keys = Self::filter_existing_key_deletions(row_keys);

        if filtered_row_keys.is_empty() {
            return Ok(0);
        }

        let delete_sequence = self
            .catalog
            .increment_sequence_number(&self.table_metadata.table_id)
            .await?;

        self.persist_key_based_deletions_with_sequence(filtered_row_keys, delete_sequence)
            .await
    }

    async fn persist_key_based_deletions_with_sequence(
        &self,
        row_keys: Vec<Box<[u8]>>,
        delete_sequence: i64,
    ) -> super::super::Result<u64> {
        let table_name = &self.table_metadata.table_name;

        // Get the row keys snapshot from the PkDeletionStrategy (only valid for RowConverterBased)
        let deletion_snapshot = self
            .pk_deletion_strategy
            .row_keys_snapshot()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: "persist_key_based_deletions called with incompatible PkDeletionStrategy"
                    .to_string(),
            })?;

        if row_keys.is_empty() {
            return Ok(0);
        }

        // Count how many keys are NEW deletions (not already in the cache).
        // This gives an accurate count of newly deleted rows for the return value.
        // ArcSwap load is wait-free; the snapshot is immutable for the lifetime of `current`.
        let current = deletion_snapshot.load_full();
        let new_deletion_count = row_keys
            .iter()
            .filter(|key| current.deleted_row_keys.get(key.as_ref()).is_none())
            .count();

        // Create a temporary metadata with the delete sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(row_keys)])
            .await?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog.add_delete_file(result.delete_file).await?;

        // Extract row keys from the result
        let written_row_keys = match &result.identifiers {
            DeletionIdentifier::KeyBased(keys) => keys,
            DeletionIdentifier::PositionBased { .. } => {
                return Err(Error::Internal {
                    table: table_name.clone(),
                    message: "Unexpected position-based deletion in key-based sink".to_string(),
                });
            }
        };

        // Build a fresh snapshot with the new deletions and publish via ArcSwap.
        // Writes are serialised by the per-table write lock so the load+rebuild+store
        // sequence is race-free.
        let updated = current.deleted_row_keys.extend_max(
            written_row_keys
                .iter()
                .map(|key| (key.clone(), delete_sequence)),
        );
        deletion_snapshot.store(Arc::new(RowConverterDeletionSnapshot::from_arcs(
            Arc::new(updated),
            Arc::clone(&current.insert_records),
        )));

        let deleted_count =
            convert_to_u64_box(new_deletion_count, "deleted row count").map_err(|e| {
                Error::Internal {
                    table: table_name.clone(),
                    message: e.to_string(),
                }
            })?;

        tracing::debug!(
            "Key-based deletion vector written and cache updated: {} key(s) (seq={}) at {:?}",
            deleted_count,
            delete_sequence,
            result.path
        );

        Ok(deleted_count)
    }

    async fn persist_int64_pk_deletions(&self, pk_values: Vec<i64>) -> super::super::Result<u64> {
        let filtered_pk_values = Self::filter_existing_int64_pk_deletions(pk_values);

        if filtered_pk_values.is_empty() {
            return Ok(0);
        }

        let delete_sequence = self
            .catalog
            .increment_sequence_number(&self.table_metadata.table_id)
            .await?;

        self.persist_int64_pk_deletions_with_sequence(filtered_pk_values, delete_sequence)
            .await
    }

    async fn persist_int64_pk_deletions_with_sequence(
        &self,
        pk_values: Vec<i64>,
        delete_sequence: i64,
    ) -> super::super::Result<u64> {
        let table_name = &self.table_metadata.table_name;

        // Get the int64 pk snapshot from the PkDeletionStrategy (only valid for Int64Pk)
        let deletion_snapshot = self
            .pk_deletion_strategy
            .int64_pk_snapshot()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: "persist_int64_pk_deletions called with incompatible PkDeletionStrategy"
                    .to_string(),
            })?;

        if pk_values.is_empty() {
            return Ok(0);
        }

        // Count how many PKs are NEW deletions (not already in the cache).
        // ArcSwap load is wait-free; the snapshot is immutable for the lifetime of `current`.
        let current = deletion_snapshot.load_full();
        let new_deletion_count = pk_values
            .iter()
            .filter(|pk| current.deleted_pk.get(**pk).is_none())
            .count();

        // For Int64 PK deletions, we store them as key-based deletions
        // where each key is the 8-byte big-endian representation of the i64 value.
        // This allows efficient storage and lookup.
        let row_keys: Vec<Box<[u8]>> = pk_values
            .iter()
            .map(|&pk| pk.to_be_bytes().to_vec().into_boxed_slice())
            .collect();

        // Create a temporary metadata with the delete sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(row_keys)])
            .await?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog.add_delete_file(result.delete_file).await?;

        // Build a fresh snapshot with the new deletions and publish via ArcSwap.
        // Writes are serialised by the per-table write lock so the load+rebuild+store
        // sequence is race-free.
        let updated = current
            .deleted_pk
            .extend_max(pk_values.iter().map(|&pk| (pk, delete_sequence)));
        deletion_snapshot.store(Arc::new(Int64PkDeletionSnapshot::from_arcs(
            Arc::new(updated),
            Arc::clone(&current.insert_records),
        )));

        let deleted_count =
            convert_to_u64_box(new_deletion_count, "deleted row count").map_err(|e| {
                Error::Internal {
                    table: table_name.clone(),
                    message: e.to_string(),
                }
            })?;

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

#[async_trait]
impl DeletionSink for CayenneDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Acquire write lock (if provided) to prevent racing with concurrent inserts or catalog refreshes.
        // When called from within write_all_append (e.g. retention filters), the caller already
        // holds the lock, so write_lock is None to avoid deadlocking the non-reentrant mutex.
        let _write_guard = match &self.write_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };

        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default(),
            Arc::clone(&self.runtime_env),
        );

        // Wait-free ArcSwap snapshot. Concurrent listing-table refreshes are
        // serialized against this code path by `self.write_lock`, which the
        // caller holds (or, for sub-sinks, is held by the orchestrating
        // operation), so we never observe a torn swap here.
        let listing_table = self.listing_table.load_full();

        // Collect all tables to scan: main listing table + protected snapshots
        let mut all_tables = vec![Arc::clone(&listing_table)];
        for protected_table in &self.protected_snapshot_tables {
            all_tables.push(Arc::clone(protected_table));
        }

        if self.filters.is_empty() {
            return self
                .delete_all_rows_from_tables(&ctx, &all_tables)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }

        self.delete_filtered_rows_from_tables(&ctx, &all_tables)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}
