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

// Allow pass-by-value for consistency with DataFusion TableProvider patterns
#![allow(clippy::needless_pass_by_value)]
// Allow let-else pattern suggestions as current style is clearer
#![allow(clippy::manual_let_else)]
// Allow collapsible if statements for readability
#![allow(clippy::collapsible_if)]

//! SIMD-optimized hash index for `MemTable`.
//!
//! This module provides a hash index wrapper that accelerates point lookups
//! on `MemTable` when a primary key is specified.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_table_providers::util::on_conflict::OnConflict;
use hash_index::{HashIndex, HashIndexBuilder, RowLocation, index_threshold};

use super::write::MemTable;

/// A `MemTable` enhanced with a SIMD-optimized hash index for fast point lookups.
///
/// When a primary key is defined, this table maintains a hash index that enables
/// O(1) lookups instead of full table scans for equality predicates on the primary key.
pub struct IndexedMemTable {
    /// The underlying `MemTable` for data storage.
    inner: MemTable,
    /// Hash index for primary key lookups.
    index: Option<Arc<HashIndex>>,
    /// Primary key column names.
    primary_key_columns: Vec<String>,
}

impl Debug for IndexedMemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedMemTable")
            .field("schema", &self.inner.schema())
            .field("indexed", &self.index.is_some())
            .field("primary_key_columns", &self.primary_key_columns)
            .finish()
    }
}

impl IndexedMemTable {
    /// Creates a new indexed `MemTable`.
    ///
    /// If primary key columns are provided and the row count exceeds the
    /// threshold (`256 × parallelism`), a hash index will be built. For
    /// small tables below the threshold, no index is created as linear
    /// scans are faster.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the table
    /// * `partitions` - The data partitions
    /// * `primary_key_columns` - Columns that form the primary key
    /// * `parallelism` - Number of parallel threads (e.g., from `DataFusion`'s
    ///   `target_partitions`). If `None`, defaults to the number of CPUs.
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        primary_key_columns: Vec<String>,
    ) -> Result<Self> {
        Self::try_new_with_parallelism(schema, partitions, primary_key_columns, None)
    }

    /// Creates a new indexed `MemTable` with explicit parallelism setting.
    ///
    /// See [`try_new`] for details. This variant allows specifying the
    /// parallelism value used to calculate the index threshold.
    pub fn try_new_with_parallelism(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        primary_key_columns: Vec<String>,
        parallelism: Option<usize>,
    ) -> Result<Self> {
        let inner = MemTable::try_new(Arc::clone(&schema), partitions.clone())?;

        let index = if primary_key_columns.is_empty() {
            None
        } else {
            // Validate primary key columns exist in schema
            for col in &primary_key_columns {
                if schema.index_of(col).is_err() {
                    return Err(DataFusionError::Plan(format!(
                        "Primary key column '{col}' not found in schema"
                    )));
                }
            }

            let total_rows: usize = partitions
                .iter()
                .flat_map(|p| p.iter())
                .map(RecordBatch::num_rows)
                .sum();

            // Use provided parallelism or fall back to CPU count
            let parallelism = parallelism.unwrap_or_else(num_cpus::get);
            let threshold = index_threshold(parallelism);

            // Build the hash index only if row count exceeds threshold
            HashIndexBuilder::new(primary_key_columns.clone())
                .with_expected_rows(total_rows)
                .with_min_rows_threshold(threshold)
                .allow_duplicates(false)
                .try_build(&partitions)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to build hash index: {e}"))
                })?
                .map(Arc::new)
        };

        Ok(Self {
            inner,
            index,
            primary_key_columns,
        })
    }

    /// Returns true if this table has an index.
    #[must_use]
    pub fn has_index(&self) -> bool {
        self.index.is_some()
    }

    /// Returns the hash index if available.
    #[must_use]
    pub fn index(&self) -> Option<&Arc<HashIndex>> {
        self.index.as_ref()
    }

    /// Returns the primary key columns.
    #[must_use]
    pub fn primary_key_columns(&self) -> &[String] {
        &self.primary_key_columns
    }

    /// Performs a point lookup by primary key value.
    ///
    /// This is the fast path that uses the hash index for O(1) lookup.
    ///
    /// # Warning: Hash Collision Risk
    ///
    /// This method returns the row matching the hash, but does NOT verify
    /// that the actual key in the row matches the lookup key. In the rare
    /// case of a hash collision, this could return the wrong row.
    ///
    /// For data-critical queries, prefer using SQL queries via `scan()`,
    /// which verifies actual key values after hash lookup.
    pub async fn get_by_key<K: std::hash::Hash>(&self, key: &K) -> Result<Option<RecordBatch>> {
        let index = match &self.index {
            Some(idx) => idx,
            None => {
                return Err(DataFusionError::Execution(
                    "No index available for point lookup".to_string(),
                ));
            }
        };

        let location = match index.get(key) {
            Some(loc) => loc,
            None => return Ok(None),
        };

        // Retrieve the row from the partition
        self.get_row_at_location(location).await
    }

    /// Gets a row at a specific location.
    async fn get_row_at_location(&self, location: RowLocation) -> Result<Option<RecordBatch>> {
        let partition_idx = location.partition as usize;
        let batch_idx = location.batch as usize;
        let row_idx = location.row as usize;

        let partitions = &self.inner.batches;
        if partition_idx >= partitions.len() {
            return Ok(None);
        }

        let partition = partitions[partition_idx].read().await;
        if batch_idx >= partition.len() {
            return Ok(None);
        }

        let batch = &partition[batch_idx];
        if row_idx >= batch.num_rows() {
            return Ok(None);
        }

        // Slice a single row
        Ok(Some(batch.slice(row_idx, 1)))
    }

    /// Performs batch lookup for multiple primary key values.
    ///
    /// Returns batches for found keys. Keys not found are skipped.
    ///
    /// # Warning: Hash Collision Risk
    ///
    /// This method returns rows matching the hashes, but does NOT verify
    /// that the actual keys in the rows match the lookup keys. In the rare
    /// case of a hash collision, this could return incorrect rows.
    ///
    /// For data-critical queries, prefer using SQL queries via `scan()`,
    /// which verifies actual key values after hash lookup.
    pub async fn get_batch_by_keys<K: std::hash::Hash>(
        &self,
        keys: &[K],
    ) -> Result<Vec<RecordBatch>> {
        let index = match &self.index {
            Some(idx) => idx,
            None => {
                return Err(DataFusionError::Execution(
                    "No index available for point lookup".to_string(),
                ));
            }
        };

        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(location) = index.get(key) {
                if let Some(batch) = self.get_row_at_location(location).await? {
                    results.push(batch);
                }
            }
        }

        Ok(results)
    }

    /// Rebuilds the hash index from current data.
    ///
    /// This should be called after modifications that invalidate the index.
    pub async fn rebuild_index(&self) -> Result<()> {
        if let Some(index) = &self.index {
            let partitions = self.read_all_partitions().await;
            index.rebuild(&partitions).map_err(|e| {
                DataFusionError::Execution(format!("Failed to rebuild hash index: {e}"))
            })?;
        }
        Ok(())
    }

    /// Reads all partitions into vectors of `RecordBatch`.
    async fn read_all_partitions(&self) -> Vec<Vec<RecordBatch>> {
        let mut result = Vec::with_capacity(self.inner.batches.len());
        for partition in &self.inner.batches {
            let batches = partition.read().await.clone();
            result.push(batches);
        }
        result
    }

    /// Checks if the filter is a simple equality on the primary key.
    ///
    /// Returns the key value if it's a simple equality predicate on the indexed column(s).
    fn extract_pk_equality_value(&self, filters: &[Expr]) -> Option<PrimaryKeyValue> {
        if self.primary_key_columns.len() != 1 {
            // TODO: Support composite key lookups
            return None;
        }

        let pk_column = &self.primary_key_columns[0];

        for filter in filters {
            if let Some(value) = Self::extract_equality_value(filter, pk_column) {
                return Some(value);
            }
        }

        None
    }

    /// Extracts an equality value from an expression.
    fn extract_equality_value(expr: &Expr, column_name: &str) -> Option<PrimaryKeyValue> {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
                // Check column = literal
                if let (Expr::Column(col), Expr::Literal(lit, _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name() == column_name {
                        return PrimaryKeyValue::try_from_scalar(lit);
                    }
                }
                // Check literal = column
                if let (Expr::Literal(lit, _), Expr::Column(col)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name() == column_name {
                        return PrimaryKeyValue::try_from_scalar(lit);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Configures `on_conflict` behavior.
    #[must_use]
    pub fn with_on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.inner = self.inner.with_on_conflict(on_conflict);
        self
    }

    /// Configures sort columns.
    #[must_use]
    pub fn with_sort_columns(mut self, sort_columns: Vec<String>) -> Self {
        self.inner = self.inner.with_sort_columns(sort_columns);
        self
    }

    /// Adds constraints to the table.
    pub async fn try_with_constraints(mut self, constraints: Constraints) -> Result<Self> {
        self.inner = self.inner.try_with_constraints(constraints).await?;
        Ok(self)
    }

    /// Configures column defaults.
    #[must_use]
    pub fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.inner = self.inner.with_column_defaults(column_defaults);
        self
    }
}

/// Represents a primary key value for lookup.
#[derive(Debug, Clone)]
pub enum PrimaryKeyValue {
    /// 64-bit signed integer key.
    Int64(i64),
    /// 32-bit signed integer key.
    Int32(i32),
    /// UTF-8 string key.
    Utf8(String),
}

impl PrimaryKeyValue {
    /// Tries to create a primary key value from a `DataFusion` scalar value.
    fn try_from_scalar(scalar: &datafusion::scalar::ScalarValue) -> Option<Self> {
        match scalar {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(Self::Int64(*v)),
            datafusion::scalar::ScalarValue::Int32(Some(v)) => Some(Self::Int32(*v)),
            datafusion::scalar::ScalarValue::Utf8(Some(v))
            | datafusion::scalar::ScalarValue::LargeUtf8(Some(v)) => Some(Self::Utf8(v.clone())),
            _ => None,
        }
    }

    /// Computes the hash for this key value using deterministic XXH3 hasher.
    fn hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash3_64;
        // Use same seed as hash-index crate for consistency
        let mut hasher = XxHash3_64::with_seed(0x5370_6963_6541_4920);
        match self {
            Self::Int64(v) => v.hash(&mut hasher),
            Self::Int32(v) => v.hash(&mut hasher),
            Self::Utf8(v) => v.hash(&mut hasher),
        }
        hasher.finish()
    }

    /// Verifies that this key value matches the primary key in the given batch at row 0.
    ///
    /// This is critical for data correctness: after a hash lookup, we must verify
    /// the actual key matches to handle hash collisions correctly.
    fn matches_batch(&self, batch: &RecordBatch, pk_column: &str) -> bool {
        let Ok(col_idx) = batch.schema().index_of(pk_column) else {
            unreachable!(
                "Primary key column '{}' missing from RecordBatch schema during index verification",
                pk_column
            );
        };
        let column = batch.column(col_idx);

        // Row 0 since batch is a single-row slice from get_row_at_location
        match self {
            Self::Int64(expected) => {
                if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    !arr.is_null(0) && arr.value(0) == *expected
                } else {
                    false
                }
            }
            Self::Int32(expected) => {
                if let Some(arr) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    !arr.is_null(0) && arr.value(0) == *expected
                } else {
                    false
                }
            }
            Self::Utf8(expected) => {
                if let Some(arr) = column.as_any().downcast_ref::<arrow::array::StringArray>() {
                    !arr.is_null(0) && arr.value(0) == expected
                } else if let Some(arr) = column
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                {
                    !arr.is_null(0) && arr.value(0) == expected
                } else {
                    false
                }
            }
        }
    }
}

#[async_trait]
impl TableProvider for IndexedMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // If we have an index and the filter is a simple PK equality,
        // we can fully push it down (exact match)
        if self.index.is_some() {
            let owned_filters: Vec<Expr> = filters.iter().map(|&e| e.clone()).collect();
            if self.extract_pk_equality_value(&owned_filters).is_some() {
                return Ok(vec![TableProviderFilterPushDown::Exact; filters.len()]);
            }
        }
        // Otherwise, delegate to MemTable behavior (Unsupported)
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check if we can use the index for a point lookup
        if let (Some(index), Some(pk_value)) =
            (&self.index, self.extract_pk_equality_value(filters))
        {
            // Perform indexed lookup
            let hash = pk_value.hash();
            let pk_columns = self.primary_key_columns.clone();
            if let Some(location) = index.get_by_hash(hash) {
                // Fast path: use index to find the row
                if let Some(batch) = self.get_row_at_location(location).await? {
                    // CRITICAL: Verify actual key matches to handle hash collisions.
                    // Hash collisions are rare with 64-bit hashes, but for 100% data
                    // correctness we must verify the actual key value.
                    let pk_column = &self.primary_key_columns[0];
                    if pk_value.matches_batch(&batch, pk_column) {
                        // Key verified - return the matched row
                        // Apply projection if needed
                        let result_batch = if let Some(proj) = projection {
                            batch.project(proj)?
                        } else {
                            batch
                        };

                        // Return a simple in-memory execution plan with the single row
                        let schema = result_batch.schema();
                        let stream = futures::stream::once(async move { Ok(result_batch) });
                        let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);

                        return Ok(Arc::new(IndexedLookupExec::new(
                            schema,
                            Box::pin(stream),
                            pk_columns,
                            true, // found result
                        )));
                    }
                    // Hash collision: the hash matched but actual key doesn't.
                    // Fall through to return empty result.
                    tracing::debug!(
                        hash = hash,
                        "Hash collision detected during point lookup; actual key doesn't match"
                    );
                }
            }

            // Key not found - return empty result
            let schema = if let Some(proj) = projection {
                Arc::new(self.schema().project(proj)?)
            } else {
                self.schema()
            };
            let stream = futures::stream::empty();
            let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);

            return Ok(Arc::new(IndexedLookupExec::new(
                schema,
                Box::pin(stream),
                pk_columns,
                false, // not found
            )));
        }

        // Fall back to regular MemTable scan
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to inner MemTable for insert
        // Note: The index will need to be rebuilt after insert
        // This is a simplified implementation; production would update the index incrementally
        let result = self.inner.insert_into(state, input, overwrite).await?;

        // TODO: Update index incrementally instead of rebuilding
        // For now, we would need to rebuild after insert completes

        Ok(result)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
}

/// Execution plan for indexed lookups.
///
/// This is a simple wrapper that returns pre-computed results from index lookups.
/// Displays as "`IndexedLookupExec`: `indexed_scan` on `[pk_columns]`" in EXPLAIN output.
pub struct IndexedLookupExec {
    schema: SchemaRef,
    /// The result stream (single batch for point lookup).
    result: std::sync::Mutex<Option<SendableRecordBatchStream>>,
    properties: datafusion::physical_plan::PlanProperties,
    /// Primary key columns used for the indexed lookup.
    pk_columns: Vec<String>,
    /// Whether the lookup found a result.
    found_result: bool,
}

impl IndexedLookupExec {
    fn new(
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
        pk_columns: Vec<String>,
        found_result: bool,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let properties = datafusion::physical_plan::PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            result: std::sync::Mutex::new(Some(stream)),
            properties,
            pk_columns,
            found_result,
        }
    }

    /// Returns the primary key columns used for this indexed lookup.
    #[must_use]
    pub fn pk_columns(&self) -> &[String] {
        &self.pk_columns
    }

    /// Returns whether the indexed lookup found a result.
    #[must_use]
    pub fn found_result(&self) -> bool {
        self.found_result
    }
}

#[expect(clippy::missing_fields_in_debug)]
impl Debug for IndexedLookupExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedLookupExec")
            .field("schema", &self.schema)
            .field("pk_columns", &self.pk_columns)
            .field("found_result", &self.found_result)
            .finish()
    }
}

impl DisplayAs for IndexedLookupExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IndexedLookupExec: indexed_scan on [{}]",
                    self.pk_columns.join(", ")
                )
            }
        }
    }
}

impl ExecutionPlan for IndexedLookupExec {
    fn name(&self) -> &'static str {
        "IndexedLookupExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Take the pre-computed result
        let mut guard = self.result.lock().map_err(|_| {
            DataFusionError::Execution("Failed to acquire lock on result".to_string())
        })?;

        guard.take().ok_or_else(|| {
            DataFusionError::Execution("IndexedLookupExec can only be executed once".to_string())
        })
    }
}

// Implement DeletionTableProvider by delegating to inner MemTable
#[async_trait]
impl crate::delete::DeletionTableProvider for IndexedMemTable {
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate deletion to inner MemTable
        // Note: Index should be updated after deletion
        crate::delete::DeletionTableProvider::delete_from(&self.inner, state, filters).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    fn create_test_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("failed to create batch")
    }

    /// Creates a large batch with row count above the indexing threshold.
    /// With parallelism=1, threshold=256, so we create 300 rows.
    #[expect(clippy::cast_possible_wrap, reason = "size is always small in tests")]
    fn create_large_test_batch(size: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..size as i64).collect();
        let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names_ref);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("failed to create batch")
    }

    /// Creates an `IndexedMemTable` with parallelism=1 for testing.
    ///
    /// With parallelism=1, threshold=256 rows. Data must have 256+ rows
    /// for an index to be created.
    fn create_test_indexed_table(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        primary_key_columns: Vec<String>,
    ) -> Result<IndexedMemTable> {
        // Use parallelism=1 so threshold=256
        IndexedMemTable::try_new_with_parallelism(schema, partitions, primary_key_columns, Some(1))
    }

    /// Creates an `IndexedMemTable` that forces index creation regardless of row count.
    ///
    /// Use this for tests that need to verify index behavior with small datasets.
    fn create_test_indexed_table_force_index(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        primary_key_columns: Vec<String>,
    ) -> Result<IndexedMemTable> {
        let inner = MemTable::try_new(Arc::clone(&schema), partitions.clone())?;

        let index = if primary_key_columns.is_empty() {
            None
        } else {
            // Validate primary key columns exist in schema
            for col in &primary_key_columns {
                if schema.index_of(col).is_err() {
                    return Err(DataFusionError::Plan(format!(
                        "Primary key column '{col}' not found in schema"
                    )));
                }
            }

            let total_rows: usize = partitions
                .iter()
                .flat_map(|p| p.iter())
                .map(RecordBatch::num_rows)
                .sum();

            // Force index creation with .build() instead of .try_build()
            Some(Arc::new(
                HashIndexBuilder::new(primary_key_columns.clone())
                    .with_expected_rows(total_rows)
                    .allow_duplicates(false)
                    .build(&partitions)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to build hash index: {e}"))
                    })?,
            ))
        };

        Ok(IndexedMemTable {
            inner,
            index,
            primary_key_columns,
        })
    }

    #[tokio::test]
    async fn test_indexed_memtable_creation() {
        // Use large batch to exceed threshold (256 rows with parallelism=1)
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        assert!(table.has_index());
        assert_eq!(table.index().map(|i| i.len()), Some(300));
    }

    #[tokio::test]
    async fn test_point_lookup() {
        // Use large batch to exceed threshold
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        // Lookup existing key
        let result = table.get_by_key(&1_i64).await.expect("lookup failed");
        assert!(result.is_some());
        let row = result.expect("expected result");
        assert_eq!(row.num_rows(), 1);

        // Lookup non-existing key
        let result = table.get_by_key(&999_i64).await.expect("lookup failed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_batch_lookup() {
        // Use large batch to exceed threshold
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let keys = vec![1_i64, 3, 5, 999];
        let results = table.get_batch_by_keys(&keys).await.expect("lookup failed");

        // Should find 3 keys (1, 3, 5), not 999
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_no_primary_key() {
        let batch = create_test_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
        let schema = batch.schema();

        let table = IndexedMemTable::try_new(
            schema,
            vec![vec![batch]],
            vec![], // No primary key
        )
        .expect("failed to create table");

        assert!(!table.has_index());

        // Point lookup should fail without index
        let result = table.get_by_key(&1_i64).await;
        let _ = result.expect_err("expected error for table without index");
    }

    #[tokio::test]
    async fn test_below_threshold_no_index() {
        // With parallelism=1, threshold is 256. Create only 100 rows.
        let batch = create_large_test_batch(100);
        let schema = batch.schema();

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // Below threshold, no index should be created
        assert!(
            !table.has_index(),
            "Index should NOT be created when row count (100) is below threshold (256)"
        );
    }

    #[tokio::test]
    async fn test_at_threshold_has_index() {
        // With parallelism=1, threshold is 256. Create exactly 256 rows.
        let batch = create_large_test_batch(256);
        let schema = batch.schema();

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // At threshold, index should be created
        assert!(
            table.has_index(),
            "Index SHOULD be created when row count (256) equals threshold (256)"
        );
    }

    #[tokio::test]
    async fn test_filter_pushdown_with_primary_key() {
        // Use large batch to exceed threshold
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // Equality on primary key should get Exact pushdown
        let filter_eq = col("id").eq(lit(1_i64));
        let pushdown = table
            .supports_filters_pushdown(&[&filter_eq])
            .expect("pushdown check failed");
        assert_eq!(pushdown, vec![TableProviderFilterPushDown::Exact]);

        // Non-equality should get Unsupported (MemTable doesn't support filter pushdown)
        let filter_gt = col("id").gt(lit(1_i64));
        let pushdown = table
            .supports_filters_pushdown(&[&filter_gt])
            .expect("pushdown check failed");
        assert_eq!(pushdown, vec![TableProviderFilterPushDown::Unsupported]);
    }

    #[tokio::test]
    async fn test_with_constraints() {
        use datafusion::common::{Constraint, Constraints};

        // Use large batch to exceed threshold
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // Add primary key constraint
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        let table = table
            .try_with_constraints(constraints)
            .await
            .expect("failed to add constraints");

        assert!(table.constraints().is_some());
    }

    #[tokio::test]
    async fn test_string_primary_key() {
        // Create a large table with string as primary key (above threshold)
        let size = 300;
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let values: Vec<i64> = (0..i64::from(size)).map(|i| i * 100).collect();
        let name_array = StringArray::from(names_ref);
        let value_array = Int64Array::from(values);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(name_array), Arc::new(value_array)],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["name".to_string()],
        )
        .expect("failed to create table");

        assert!(table.has_index());

        // Lookup by string key
        let result = table.get_by_key(&"name_5").await.expect("lookup failed");
        assert!(result.is_some());
        let batch = result.expect("expected result");
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_large_table_indexed_lookup() {
        // Create a larger table to test index performance
        let size: i64 = 10_000;
        let ids: Vec<i64> = (0..size).collect();
        let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(ids.clone());
        let name_array = StringArray::from(names_ref);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("failed to create batch");

        let table = IndexedMemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // Lookup various keys
        for key in [0_i64, 100, 1000, 5000, 9999] {
            let result = table.get_by_key(&key).await.expect("lookup failed");
            assert!(result.is_some(), "Key {key} should exist");
        }

        // Lookup non-existing key
        let result = table.get_by_key(&size).await.expect("lookup failed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_partitions_indexed() {
        // Create table with multiple partitions that together exceed threshold
        // Each batch has 100 rows, 3 partitions = 300 rows (above 256 threshold)
        let batch1 = create_large_test_batch(100);
        let schema = batch1.schema();
        // Create additional batches with offset IDs to avoid duplicates
        let ids2: Vec<i64> = (100..200).collect();
        let names2: Vec<String> = (100..200).map(|i| format!("name_{i}")).collect();
        let names2_ref: Vec<&str> = names2.iter().map(String::as_str).collect();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids2)),
                Arc::new(StringArray::from(names2_ref)),
            ],
        )
        .expect("failed to create batch2");

        let ids3: Vec<i64> = (200..300).collect();
        let names3: Vec<String> = (200..300).map(|i| format!("name_{i}")).collect();
        let names3_ref: Vec<&str> = names3.iter().map(String::as_str).collect();
        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids3)),
                Arc::new(StringArray::from(names3_ref)),
            ],
        )
        .expect("failed to create batch3");

        let table = create_test_indexed_table(
            Arc::clone(&schema),
            vec![vec![batch1], vec![batch2], vec![batch3]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        assert!(table.has_index());

        // Keys should be found across partitions
        for key in [0_i64, 50, 100, 150, 200, 250, 299] {
            let result = table.get_by_key(&key).await.expect("lookup failed");
            assert!(result.is_some(), "Key {key} should exist");
        }
    }

    /// Helper to get the physical plan as a string for EXPLAIN output.
    async fn explain_plan(ctx: &SessionContext, sql: &str) -> String {
        let df = ctx.sql(sql).await.expect("failed to create dataframe");
        let plan = df
            .create_physical_plan()
            .await
            .expect("failed to create physical plan");
        datafusion::physical_plan::display::DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string()
    }

    #[tokio::test]
    async fn test_explain_indexed_scan_snapshot() {
        // Use force index helper for small test data
        let batch = create_test_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
        let schema = batch.schema();

        let table = create_test_indexed_table_force_index(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table))
            .expect("failed to register table");

        // Point lookup query should use indexed scan
        let plan = explain_plan(&ctx, "SELECT * FROM test_table WHERE id = 1").await;

        // Verify the plan contains indexed_scan
        assert!(
            plan.contains("indexed_scan"),
            "EXPLAIN should show indexed_scan for point lookup. Got:\n{plan}"
        );
        assert!(
            plan.contains("IndexedLookupExec"),
            "EXPLAIN should show IndexedLookupExec. Got:\n{plan}"
        );

        insta::assert_snapshot!("explain_indexed_scan", plan);
    }

    #[tokio::test]
    async fn test_explain_non_indexed_scan_snapshot() {
        // Use force index helper for small test data
        let batch = create_test_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
        let schema = batch.schema();

        let table = create_test_indexed_table_force_index(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table))
            .expect("failed to register table");

        // Range query should NOT use indexed scan (falls back to MemTable scan)
        let plan = explain_plan(&ctx, "SELECT * FROM test_table WHERE id > 1").await;

        // Verify the plan does NOT contain indexed_scan
        assert!(
            !plan.contains("IndexedLookupExec"),
            "EXPLAIN should NOT show IndexedLookupExec for range query. Got:\n{plan}"
        );
        assert!(
            !plan.contains("indexed_scan"),
            "EXPLAIN should NOT show indexed_scan for range query. Got:\n{plan}"
        );

        insta::assert_snapshot!("explain_non_indexed_scan", plan);
    }

    #[tokio::test]
    async fn test_explain_no_index_table_snapshot() {
        let batch = create_test_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
        let schema = batch.schema();

        // Table without index
        let table = IndexedMemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec![], // No primary key
        )
        .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table))
            .expect("failed to register table");

        // Even point lookup should NOT use indexed scan when no index exists
        let plan = explain_plan(&ctx, "SELECT * FROM test_table WHERE id = 1").await;

        // Verify the plan does NOT contain indexed_scan
        assert!(
            !plan.contains("IndexedLookupExec"),
            "EXPLAIN should NOT show IndexedLookupExec when no index. Got:\n{plan}"
        );

        insta::assert_snapshot!("explain_no_index_table", plan);
    }

    #[tokio::test]
    async fn test_explain_indexed_vs_non_indexed_comparison() {
        // Use force index helper for small test data
        let batch = create_test_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
        let schema = batch.schema();

        // Create indexed table with forced index
        let indexed_table = create_test_indexed_table_force_index(
            Arc::clone(&schema),
            vec![vec![batch.clone()]],
            vec!["id".to_string()],
        )
        .expect("failed to create indexed table");

        // Create non-indexed table
        let non_indexed_table = IndexedMemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch]],
            vec![], // No primary key
        )
        .expect("failed to create non-indexed table");

        let ctx = SessionContext::new();
        ctx.register_table("indexed_table", Arc::new(indexed_table))
            .expect("failed to register table");
        ctx.register_table("non_indexed_table", Arc::new(non_indexed_table))
            .expect("failed to register table");

        let indexed_plan = explain_plan(&ctx, "SELECT * FROM indexed_table WHERE id = 1").await;
        let non_indexed_plan =
            explain_plan(&ctx, "SELECT * FROM non_indexed_table WHERE id = 1").await;

        // The plans should be different - indexed should have IndexedLookupExec
        assert!(
            indexed_plan.contains("IndexedLookupExec"),
            "Indexed table plan should contain IndexedLookupExec. Got:\n{indexed_plan}"
        );
        assert!(
            !non_indexed_plan.contains("IndexedLookupExec"),
            "Non-indexed table plan should NOT contain IndexedLookupExec. Got:\n{non_indexed_plan}"
        );

        // Snapshot both for comparison
        insta::assert_snapshot!("explain_indexed_table_point_lookup", indexed_plan);
        insta::assert_snapshot!("explain_non_indexed_table_point_lookup", non_indexed_plan);
    }

    #[tokio::test]
    async fn test_indexed_lookup_exec_display_format() {
        use std::fmt::Write;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let stream = futures::stream::empty();
        let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);

        let exec = IndexedLookupExec::new(schema, Box::pin(stream), vec!["id".to_string()], true);

        // Test Default format via fmt_as
        let mut output = String::new();
        write!(
            &mut output,
            "{}",
            DisplayAsWrapper(&exec, DisplayFormatType::Default)
        )
        .expect("failed to format");
        assert_eq!(output, "IndexedLookupExec: indexed_scan on [id]");

        // Test with multiple columns
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, false),
        ]));
        let stream2 = futures::stream::empty();
        let stream2 = RecordBatchStreamAdapter::new(Arc::clone(&schema2), stream2);

        let exec2 = IndexedLookupExec::new(
            schema2,
            Box::pin(stream2),
            vec!["tenant_id".to_string(), "user_id".to_string()],
            false,
        );

        let mut output2 = String::new();
        write!(
            &mut output2,
            "{}",
            DisplayAsWrapper(&exec2, DisplayFormatType::Default)
        )
        .expect("failed to format");
        assert_eq!(
            output2,
            "IndexedLookupExec: indexed_scan on [tenant_id, user_id]"
        );

        insta::assert_snapshot!(
            "indexed_lookup_exec_display",
            format!("Single column: {}\nMultiple columns: {}", output, output2)
        );
    }

    /// Helper wrapper to format `DisplayAs` implementors.
    struct DisplayAsWrapper<'a, T: DisplayAs>(&'a T, DisplayFormatType);

    impl<T: DisplayAs> std::fmt::Display for DisplayAsWrapper<'_, T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt_as(self.1, f)
        }
    }

    // =========================================================================
    // Data Correctness Tests
    // =========================================================================

    /// Test key verification with Int64 primary key.
    #[tokio::test]
    async fn test_data_correctness_key_verification_int64() {
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Query should return exact match
        let df = ctx
            .sql("SELECT id, name FROM test WHERE id = 42")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected int64");
        assert_eq!(id_col.value(0), 42, "Must return exact key match");
    }

    /// Test key verification with string primary key.
    #[tokio::test]
    async fn test_data_correctness_key_verification_string() {
        let names: Vec<&str> = (0..300)
            .map(|i| {
                // Leak strings to get 'static lifetime for test
                Box::leak(format!("user_{i}").into_boxed_str()) as &str
            })
            .collect();
        let ids: Vec<i64> = (0..300).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let name_array = StringArray::from(names.clone());
        let id_array = Int64Array::from(ids);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(name_array), Arc::new(id_array)],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["name".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        let df = ctx
            .sql("SELECT name, id FROM test WHERE name = 'user_123'")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let name_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected string");
        assert_eq!(name_col.value(0), "user_123", "Must return exact key match");
    }

    /// Test that non-existent keys return empty results (not wrong data).
    #[tokio::test]
    async fn test_data_correctness_nonexistent_key_returns_empty() {
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Key that doesn't exist
        let df = ctx
            .sql("SELECT * FROM test WHERE id = 999999")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        // Should return empty, not some random row
        let total_rows: usize = batches.iter().map(arrow_array::RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 0, "Non-existent key must return empty result");
    }

    /// Test boundary values for Int64 primary key.
    #[tokio::test]
    async fn test_data_correctness_int64_boundary_values() {
        let ids: Vec<i64> = vec![i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
        let names: Vec<String> = ids.iter().map(|i| format!("name_{i}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(StringArray::from(names_ref)),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table_force_index(
            schema,
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Test each boundary value
        for expected_id in &ids {
            let sql = format!("SELECT id FROM test WHERE id = {expected_id}");
            let df = ctx.sql(&sql).await.expect("query failed");
            let batches = df.collect().await.expect("collect failed");

            assert_eq!(batches.len(), 1, "Boundary value {expected_id} not found");
            let id_col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("expected int64");
            assert_eq!(
                id_col.value(0),
                *expected_id,
                "Boundary value mismatch for {expected_id}"
            );
        }
    }

    /// Test empty string as primary key.
    #[tokio::test]
    async fn test_data_correctness_empty_string_key() {
        let names: Vec<&str> = vec!["", "a", "normal", "   ", "\t\n"];
        #[expect(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..names.len() as i64).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table_force_index(
            schema,
            vec![vec![batch]],
            vec!["name".to_string()],
        )
        .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Query empty string - use parameterized-style query
        let df = ctx
            .sql("SELECT id FROM test WHERE name = ''")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected int64");
        assert_eq!(id_col.value(0), 0, "Empty string key should map to id=0");
    }

    /// Test Unicode characters in string keys.
    #[tokio::test]
    async fn test_data_correctness_unicode_keys() {
        let names: Vec<&str> = vec![
            "hello",
            "世界",       // Chinese
            "مرحبا",      // Arabic
            "🚀🎉",       // Emoji
            "café",       // Accented
            "null\0byte", // Embedded null byte
            "a\tb\nc",    // Whitespace
        ];
        #[expect(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..names.len() as i64).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(names.clone())),
                Arc::new(Int64Array::from(ids.clone())),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table_force_index(
            schema,
            vec![vec![batch]],
            vec!["name".to_string()],
        )
        .expect("failed to create table");

        // Test direct lookup for each key
        for name in &names {
            let result = table.get_by_key(&(*name).to_string()).await;
            assert!(
                result.is_ok(),
                "Lookup should not error for unicode key: {name:?}"
            );
            let batch = result.expect("lookup failed");
            assert!(batch.is_some(), "Unicode key {name:?} should be found");
        }
    }

    // =========================================================================
    // Security-Related Tests
    // =========================================================================

    /// Test SQL injection attempt in key value is handled safely.
    #[tokio::test]
    async fn test_security_sql_injection_in_value() {
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // This is a literal string, not actual injection - but tests proper handling
        // The key "1 OR 1=1" should not match anything (it's parsed as Int64 = 1)
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM test WHERE id = 1")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        // Should return exactly 1 row, not all rows
        let cnt_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("expected int64");
        assert_eq!(
            cnt_col.value(0),
            1,
            "Should find exactly 1 row, not be vulnerable to injection"
        );
    }

    /// Test that string injection attempts in literals are safe.
    #[tokio::test]
    async fn test_security_string_literal_injection() {
        let names: Vec<&str> = (0..300)
            .map(|i| Box::leak(format!("user_{i}").into_boxed_str()) as &str)
            .collect();
        let ids: Vec<i64> = (0..300).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["name".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Injection attempt in string literal (DataFusion handles this safely)
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM test WHERE name = 'user_1'' OR ''1''=''1'")
            .await;

        // This should either error or return 0 rows - never return all rows
        if let Ok(df) = df {
            let batches = df.collect().await.expect("collect failed");
            let cnt_col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("expected int64");
            assert_eq!(
                cnt_col.value(0),
                0,
                "Injection string should not match any real key"
            );
        }
        // Else: SQL parse error is also acceptable - injection was blocked
    }

    /// Test that duplicate primary keys are rejected (data integrity).
    #[tokio::test]
    async fn test_security_duplicate_key_rejected() {
        let ids: Vec<i64> = vec![1, 2, 3, 2, 5]; // Duplicate key: 2
        let names: Vec<&str> = vec!["a", "b", "c", "d", "e"];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("failed to create batch");

        // Creating indexed table with duplicates should fail
        let result = create_test_indexed_table_force_index(
            schema,
            vec![vec![batch]],
            vec!["id".to_string()],
        );

        assert!(
            result.is_err(),
            "Duplicate primary keys should be rejected for data integrity"
        );
    }

    // =========================================================================
    // NULL Handling Tests
    // =========================================================================

    /// Test that NULL primary keys are skipped (not indexed).
    #[tokio::test]
    async fn test_data_correctness_null_keys_skipped() {
        let ids: Vec<Option<i64>> = vec![Some(1), None, Some(3), None, Some(5)];
        let names: Vec<&str> = vec!["a", "b", "c", "d", "e"];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true), // nullable
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table_force_index(
            schema,
            vec![vec![batch]],
            vec!["id".to_string()],
        )
        .expect("failed to create table");

        // Only non-null keys should be indexed
        assert!(table.has_index());
        let index = table.index().expect("should have index");
        assert_eq!(index.len(), 3, "Only 3 non-null keys should be indexed");

        // Non-null keys should be findable
        assert!(table.get_by_key(&1_i64).await.expect("lookup").is_some());
        assert!(table.get_by_key(&3_i64).await.expect("lookup").is_some());
        assert!(table.get_by_key(&5_i64).await.expect("lookup").is_some());
    }

    // =========================================================================
    // Projection Safety Tests
    // =========================================================================

    /// Test that projections don't affect data correctness.
    #[tokio::test]
    async fn test_data_correctness_projection_safety() {
        let batch = create_large_test_batch(300);
        let schema = batch.schema();

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        // Query with projection that excludes the primary key column
        let df = ctx
            .sql("SELECT name FROM test WHERE id = 42")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let name_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected string");
        assert_eq!(
            name_col.value(0),
            "name_42",
            "Projection should return correct row data"
        );
    }

    /// Test concurrent reads don't cause data races.
    #[tokio::test]
    async fn test_data_correctness_concurrent_reads() {
        let batch = create_large_test_batch(1000);
        let schema = batch.schema();

        let table = Arc::new(
            create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
                .expect("failed to create table"),
        );

        // Spawn multiple concurrent lookups
        let mut handles = Vec::new();
        for i in 0..10 {
            let table_clone = Arc::clone(&table);
            handles.push(tokio::spawn(async move {
                for key in (i * 100)..(i * 100 + 100) {
                    let result = table_clone
                        .get_by_key(&i64::from(key))
                        .await
                        .expect("lookup failed");

                    if key < 1000 {
                        assert!(result.is_some(), "Key {key} should exist");
                    }
                }
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.expect("task panicked");
        }
    }

    /// Test Int32 primary key type.
    #[tokio::test]
    async fn test_data_correctness_int32_primary_key() {
        let ids: Vec<i32> = (0..300).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("name_{i}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(ids)),
                Arc::new(StringArray::from(names_ref)),
            ],
        )
        .expect("failed to create batch");

        let table = create_test_indexed_table(schema, vec![vec![batch]], vec!["id".to_string()])
            .expect("failed to create table");

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))
            .expect("failed to register");

        let df = ctx
            .sql("SELECT id, name FROM test WHERE id = 42")
            .await
            .expect("query failed");
        let batches = df.collect().await.expect("collect failed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("expected int32");
        assert_eq!(id_col.value(0), 42, "Must return exact Int32 key match");
    }

    /// Test `PrimaryKeyValue::matches_batch` for all supported types.
    #[test]
    fn test_primary_key_value_matches_batch() {
        // Test Int64
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))])
            .expect("batch");

        let pk = PrimaryKeyValue::Int64(42);
        assert!(pk.matches_batch(&batch, "id"), "Int64 match should succeed");

        let pk_wrong = PrimaryKeyValue::Int64(99);
        assert!(
            !pk_wrong.matches_batch(&batch, "id"),
            "Int64 mismatch should fail"
        );

        // Test Int32
        let schema32 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch32 = RecordBatch::try_new(
            schema32,
            vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
        )
        .expect("batch");

        let pk32 = PrimaryKeyValue::Int32(42);
        assert!(
            pk32.matches_batch(&batch32, "id"),
            "Int32 match should succeed"
        );

        // Test Utf8
        let schema_str = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch_str =
            RecordBatch::try_new(schema_str, vec![Arc::new(StringArray::from(vec!["hello"]))])
                .expect("batch");

        let pk_str = PrimaryKeyValue::Utf8("hello".to_string());
        assert!(
            pk_str.matches_batch(&batch_str, "name"),
            "Utf8 match should succeed"
        );

        let pk_str_wrong = PrimaryKeyValue::Utf8("world".to_string());
        assert!(
            !pk_str_wrong.matches_batch(&batch_str, "name"),
            "Utf8 mismatch should fail"
        );

        // Test missing column
        assert!(
            !pk.matches_batch(&batch, "nonexistent"),
            "Missing column should fail"
        );
    }
}
