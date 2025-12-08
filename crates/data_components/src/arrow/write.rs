// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`MemTable`] for querying `Vec<RecordBatch>` by `DataFusion`.

use arrow::array::{Array, BooleanBuilder};
use arrow::compute::filter_record_batch;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::stream;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};

use std::sync::{Arc, Mutex};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::common::{Constraint, Constraints, SchemaExt};
use datafusion::datasource::{TableProvider, TableType, provider_as_source};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, is_not_true};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;

/// A wrapper around `XxHash3_64` that uses a fixed seed (0) for deterministic hashing.
/// This is necessary because `XxHash3_64::default()` may use a random seed for DOS protection,
/// which would make `HashSets` with different hasher instances incompatible for lookups.
#[derive(Clone)]
struct XxHash3_64WithFixedSeed {
    hasher: twox_hash::XxHash3_64,
}

impl Default for XxHash3_64WithFixedSeed {
    fn default() -> Self {
        Self::new()
    }
}

impl XxHash3_64WithFixedSeed {
    fn new() -> Self {
        Self {
            hasher: twox_hash::XxHash3_64::with_seed(7),
        }
    }
}

impl std::hash::Hasher for XxHash3_64WithFixedSeed {
    fn finish(&self) -> u64 {
        self.hasher.clone().finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hasher.write(bytes);
    }
}

/// Type alias for partition data
pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

/// In-memory data source for presenting a `Vec<RecordBatch>` as a
/// data source that can be queried by `DataFusion`. This allows data to
/// be pre-loaded into memory and then repeatedly queried without
/// incurring additional file I/O overhead.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    pub(crate) batches: Vec<PartitionData>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    /// inserting data into this table removes the order
    pub sort_order: Arc<Mutex<Vec<Vec<Expr>>>>,

    pub on_conflict: Option<OnConflict>,

    /// Optional columns to sort by during insert operations.
    /// When specified, data is sorted before being written to improve
    /// zone map efficiency for range queries.
    sort_columns: Vec<String>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef, mut partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        for batches in partitions.iter().flatten() {
            let batches_schema = batches.schema();
            if !schema.contains(&batches_schema) {
                tracing::debug!(
                    "mem table schema does not contain batches schema. \
                        Target_schema: {schema:?}. Batches Schema: {batches_schema:?}"
                );
                return Err(DataFusionError::Plan(
                    "Mismatch between schema and batches".to_string(),
                ));
            }
        }

        // Add at least one partition
        if partitions.is_empty() {
            partitions.extend([vec![]]);
        }

        Ok(Self {
            schema,
            batches: partitions
                .into_iter()
                .map(|e| Arc::new(RwLock::new(e)))
                .collect::<Vec<_>>(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::new(),
            sort_order: Arc::new(Mutex::new(vec![])),
            on_conflict: None,
            sort_columns: Vec::new(),
        })
    }

    #[must_use]
    pub fn with_sort_columns(mut self, sort_columns: Vec<String>) -> Self {
        self.sort_columns = sort_columns;
        self
    }

    #[must_use]
    pub fn with_on_conflict(mut self, on_conflict: OnConflict) -> Self {
        if !matches!(on_conflict, OnConflict::Upsert(_)) {
            tracing::warn!(
                "In-memory tables only support Upsert on_conflict, but got: {on_conflict:?}. Setting will be ignored."
            );
            return self;
        }

        self.on_conflict = Some(on_conflict);
        self
    }

    pub async fn try_with_constraints(mut self, constraints: Constraints) -> Result<Self> {
        self.ensure_batches_satisfy_constraints(&constraints)
            .await?;
        self.constraints = constraints;
        Ok(self)
    }

    async fn ensure_batches_satisfy_constraints(&self, constraints: &Constraints) -> Result<()> {
        if constraints.iter().len() == 0 {
            return Ok(());
        }
        // Keep track of uniquness of rows per constraint.
        let mut constraint_keys: Vec<
            HashSet<String, std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>>,
        > = Vec::with_capacity(constraints.iter().len());
        for b in &self.batches {
            let p = &*b.read().await;
            let p: Vec<_> = p.iter().collect();
            for (i, c) in constraints.iter().enumerate() {
                let valid_ids = match c {
                    Constraint::PrimaryKey(pk) => {
                        let pks = primary_key_identifier(&p, pk)?;
                        check_and_filter_non_null_unique_primary_keys::<
                            std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>,
                        >(&pks, constraint_keys.get(i))?
                    }
                    Constraint::Unique(u) => {
                        let ids = constraint_identifiers(&p, u)?;
                        let as_str: Vec<_> = ids.iter().map(String::as_str).collect();
                        check_and_filter_unique_constraint::<
                            std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>,
                        >(&as_str, constraint_keys.get(i))?
                    }
                };
                // Keep track of ids to ensure uniqueness across all partitions.
                if let Some(existing) = constraint_keys.get_mut(i) {
                    existing.extend(valid_ids);
                } else {
                    constraint_keys.insert(i, valid_ids);
                }
            }
        }

        Ok(())
    }

    /// Attempt to retrieve the primary key from the constraints, and ensure that there are no unsupported [`Constraint::Unique`].
    fn get_and_ensure_only_primary_keys(&self) -> Result<Option<Vec<usize>>> {
        if let Some(constraints) = self.constraints() {
            match constraints.iter().next() {
                Some(Constraint::PrimaryKey(pk)) => {
                    return Ok(Some(pk.clone()));
                }
                Some(Constraint::Unique(_)) => {
                    return Err(DataFusionError::Execution(
                        "Unique constraints are not supported for in-memory tables. If possible, consider using a primary key.".to_string(),
                    ));
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    fn verify_on_conflict_matches_primary_key(
        &self,
        pk: &[usize],
        on_conflict: &ColumnReference,
    ) -> Result<()> {
        let schema = self.schema();

        let pk_names: HashSet<&str> = pk
            .iter()
            .map(|&idx| schema.field(idx).name().as_str())
            .collect();

        let on_conflict_set: HashSet<&str> = on_conflict.iter().collect();

        if pk_names != on_conflict_set {
            return Err(DataFusionError::Execution(
                "Primary key columns must match the on_conflict definition".to_string(),
            ));
        }
        Ok(())
    }

    /// Assign column defaults
    #[must_use]
    pub fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in &self.batches {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone());
        }
        Ok(Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, self.schema(), projection.cloned())?,
        ))))
    }

    /// Returns an `ExecutionPlan` that inserts the execution results of a given [`ExecutionPlan`] into this [`MemTable`].
    ///
    /// The [`ExecutionPlan`] must have the same schema as this [`MemTable`].
    ///
    /// # Arguments
    ///
    /// * `state` - The [`SessionState`] containing the context for executing the plan.
    /// * `input` - The [`ExecutionPlan`] to execute and insert.
    ///
    /// # Returns
    ///
    /// * A plan that returns the number of rows written.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if let Err(e) = self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
        {
            return Err(DataFusionError::Execution(format!(
                "Inserting query must have the same schema with the table. {e}"
            )));
        }

        let primary_key = self.get_and_ensure_only_primary_keys()?;

        // In-memory tables only support primary keys constraints. Support for `OnConflict` is limited to `Upsert` matching the primary key.
        // So we verify that the `on_conflict` and  the primary key matches
        if let (Some(OnConflict::Upsert(on_conflict)), Some(pk)) = (&self.on_conflict, &primary_key)
        {
            self.verify_on_conflict_matches_primary_key(pk, on_conflict)?;
        }

        let sink = Arc::new(MemSink::new(
            self.batches.clone(),
            overwrite,
            primary_key,
            self.schema(),
            self.on_conflict.clone(),
            self.sort_columns.clone(),
        ));
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

/// Implements for writing to a [`MemTable`]
struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
    overwrite: InsertOp,

    /// Optional primary key columns. If present, primary key values must be unique, ordered ascendingly.
    primary_key: Option<Vec<usize>>,
    schema: SchemaRef,
    on_conflict: Option<OnConflict>,

    /// Optional columns to sort by before writing
    sort_columns: Vec<String>,
}

impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for MemSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let partition_count = self.batches.len();
                write!(f, "MemoryTable (partitions={partition_count})")
            }
        }
    }
}

impl MemSink {
    fn new(
        batches: Vec<PartitionData>,
        overwrite: InsertOp,
        primary_key: Option<Vec<usize>>,
        schema: SchemaRef,
        on_conflict: Option<OnConflict>,
        sort_columns: Vec<String>,
    ) -> Self {
        Self {
            batches,
            overwrite,
            primary_key: primary_key.map(|pks| {
                let mut z = pks;
                z.sort_unstable();
                z
            }),
            schema,
            on_conflict,
            sort_columns,
        }
    }
}

/// Check that all primary key ids are non-null and unique.
///
/// If `existing_pks` is provided, also check uniqueness of `pks` against `existing_pks`.
///
/// Returns a set of unique, non-null primary key ids.
///
/// # Visibility
/// This function is public for benchmarking purposes.
pub(crate) fn check_and_filter_non_null_unique_primary_keys<S: std::hash::BuildHasher + Default>(
    pks: &[Option<String>],
    existing_pks: Option<&HashSet<String, S>>,
) -> Result<HashSet<String, S>> {
    let num_pks = pks.len();

    // First check uniqueness
    let non_null_pks: Vec<&str> = pks.iter().filter_map(|opt| opt.as_deref()).collect();
    let unique_set = check_and_filter_unique_constraint::<S>(&non_null_pks, existing_pks)?;

    if num_pks != non_null_pks.len() {
        return Err(DataFusionError::Execution(
            "Primary key values must be non-null".to_string(),
        ));
    }
    Ok(unique_set)
}

/// Check that all non-null primary key ids are unique.
///
/// If `existing_ids` is provided, also check uniqueness of `ids` against `existing_ids`. Do
/// not check for nullity, or uniqueness of null values.
///
/// Returns a set of unique ids.
///
/// # Visibility
/// This function is public for benchmarking purposes.
pub(crate) fn check_and_filter_unique_constraint<S: std::hash::BuildHasher + Default>(
    ids: &[&str],
    existing_ids: Option<&HashSet<String, S>>,
) -> Result<HashSet<String, S>> {
    // Optimization: For large datasets, sort first then check for duplicates
    // This can be faster than HashSet insertion for very large batches due to better cache locality
    if ids.len() > 10_000 {
        // For large datasets, sort and check for consecutive duplicates
        let mut sorted_ids: Vec<&str> = ids.to_vec();
        sorted_ids.sort_unstable();

        // Build HashSet incrementally while validating uniqueness
        // This avoids a separate O(n) allocation pass after validation
        let mut unique_set = HashSet::with_capacity_and_hasher(ids.len(), S::default());

        if let Some(existing) = existing_ids {
            // Path with existing IDs check
            let mut prev: Option<&str> = None;
            for &id in &sorted_ids {
                // Check for consecutive duplicates in sorted array
                if prev.is_some_and(|p| p == id) {
                    return Err(DataFusionError::Execution(
                        "Primary key values must be unique".to_string(),
                    ));
                }

                // Check against existing ids
                if existing.contains(id) {
                    return Err(DataFusionError::Execution(format!(
                        "Primary key ({id}) already exists and is not unique"
                    )));
                }

                // Insert into set while validating (only allocate once per string)
                unique_set.insert(id.to_string());
                prev = Some(id);
            }
        } else {
            // Fast path without existing IDs check
            let mut prev: Option<&str> = None;
            for &id in &sorted_ids {
                // Check for consecutive duplicates in sorted array
                if prev.is_some_and(|p| p == id) {
                    return Err(DataFusionError::Execution(
                        "Primary key values must be unique".to_string(),
                    ));
                }

                // Insert into set while validating (only allocate once per string)
                unique_set.insert(id.to_string());
                prev = Some(id);
            }
        }

        Ok(unique_set)
    } else {
        // For smaller datasets, use HashSet (better for small sizes)
        let mut unique_set = HashSet::with_capacity_and_hasher(ids.len(), S::default());
        ids.iter()
            .map(|&id| {
                if unique_set.insert(id.to_string()) {
                    if existing_ids.is_some_and(|existing| existing.contains(id)) {
                        return Err(DataFusionError::Execution(format!(
                            "Primary key ({id}) already exists and is not unique"
                        )));
                    }
                    Ok(())
                } else {
                    Err(DataFusionError::Execution(
                        "Primary key values must be unique".to_string(),
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(unique_set)
    }
}

/// Create primary key values for a [`RecordBatch`]. For composite keys, values are concatenated with a delimiter '|'.
///
/// `pk_indices_ordered` should be in ascending order.
///
/// If any primary key value is `Null`, the entire key is [`Option::None`].
///
/// # Visibility
/// This function is public for benchmarking purposes.
#[expect(clippy::too_many_lines)]
pub(crate) fn extract_primary_keys_str(
    batch: &RecordBatch,
    pk_indices_ordered: &[usize],
) -> Result<Vec<Option<String>>> {
    use arrow::datatypes::DataType;

    let num_rows = batch.num_rows();

    // Optimization: Fast path for single-column primary keys
    // Avoids ScalarValue conversion and string concatenation overhead
    if pk_indices_ordered.len() == 1 {
        let col = batch.column(pk_indices_ordered[0]);
        let mut keys = Vec::with_capacity(num_rows);

        // Further optimization: Use direct downcasting for common primitive types
        // This avoids the expensive ScalarValue::try_from_array() conversion
        match col.data_type() {
            DataType::Int8 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int8Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to Int8Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::Int16 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int16Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to Int16Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::Int32 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to Int32Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::Int64 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to Int64Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::UInt8 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to UInt8Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::UInt16 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::UInt16Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to UInt16Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::UInt32 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to UInt32Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::UInt64 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to UInt64Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::Utf8 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("Failed to downcast to StringArray".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            DataType::LargeUtf8 => {
                let array = col
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Failed to downcast to LargeStringArray".to_string(),
                        )
                    })?;
                for row_idx in 0..num_rows {
                    keys.push(if array.is_null(row_idx) {
                        None
                    } else {
                        Some(array.value(row_idx).to_string())
                    });
                }
            }
            // Fallback to ScalarValue conversion for less common types
            _ => {
                for row_idx in 0..num_rows {
                    if col.is_null(row_idx) {
                        keys.push(None);
                    } else {
                        let val = ScalarValue::try_from_array(col, row_idx)
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                        keys.push(Some(val.to_string()));
                    }
                }
            }
        }
        return Ok(keys);
    }

    // Composite key path: must concatenate multiple columns
    let mut keys = Vec::with_capacity(num_rows);

    'row: for row_idx in 0..num_rows {
        let mut parts = Vec::with_capacity(pk_indices_ordered.len());
        for &col_idx in pk_indices_ordered {
            let col = batch.column(col_idx);

            // Optimization: Check nullity first before expensive conversion
            if col.is_null(row_idx) {
                keys.push(None);
                continue 'row;
            }

            let val = ScalarValue::try_from_array(col, row_idx)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            parts.push(val.to_string());
        }
        // Join all PK parts with a delimiter
        let key = parts.join("|");
        keys.push(Some(key));
    }

    Ok(keys)
}

fn extract_constraint_keys_str(
    batch: &RecordBatch,
    pk_indices_ordered: &[usize],
) -> Result<Vec<String>> {
    let num_rows = batch.num_rows();
    let mut keys = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut parts = Vec::with_capacity(pk_indices_ordered.len());
        for &col_idx in pk_indices_ordered {
            let col = batch.column(col_idx);
            let val = ScalarValue::try_from_array(col, row_idx)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            parts.push(val.to_string());
        }
        // Join all parts with a delimiter
        let key = parts.join("|");
        keys.push(key);
    }

    Ok(keys)
}

fn constraint_identifiers(rb: &[&RecordBatch], constraint_idx: &[usize]) -> Result<Vec<String>> {
    // Create unique string for each constraint columns across all `new_batches` rows.
    let new_keys: Vec<_> = rb
        .iter()
        .map(|b| extract_constraint_keys_str(b, constraint_idx))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(new_keys)
}

/// Filter elements of `existing_batches` that have primary keys from `overwriting_primary_keys`.
///
/// This is one part of `InsertOp::Replace` functionality, and still requires the new rows (with conflicting PKs), to be added.
///
/// This function modifies `existing_batches` in place.
///
/// # Visibility
/// This function is public for benchmarking purposes.
pub(crate) fn filter_existing<S: std::hash::BuildHasher>(
    existing_batches: &mut Vec<RecordBatch>,
    overwriting_primary_keys: &HashSet<String, S>,
    pk_indices_ordered: &[usize],
) -> Result<()> {
    if existing_batches.is_empty() {
        return Ok(());
    }

    // Optimization: For large key sets, convert to sorted vector for binary search
    // Binary search is O(log n) vs HashSet's O(1), but has better cache locality
    // and can be faster for moderately sized sets due to fewer cache misses
    let use_sorted_search = overwriting_primary_keys.len() > 1000;
    let sorted_keys = if use_sorted_search {
        let mut keys: Vec<&str> = overwriting_primary_keys
            .iter()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        Some(keys)
    } else {
        None
    };

    // Instead of concatenating, we can filter each batch individually
    let mut filtered = Vec::with_capacity(existing_batches.len());
    for batch in existing_batches.drain(..) {
        let keys = extract_primary_keys_str(&batch, pk_indices_ordered)?;

        // Pre-allocate with exact capacity for better performance
        let mut keep_row_builder = BooleanBuilder::with_capacity(keys.len());

        for k in keys {
            if let Some(k) = k {
                let should_remove = if let Some(ref sorted) = sorted_keys {
                    sorted.binary_search(&k.as_str()).is_ok()
                } else {
                    overwriting_primary_keys.contains(&k)
                };
                keep_row_builder.append_value(!should_remove);
            } else {
                unreachable!(
                    "Primary keys in `MemSink` record batch contain(s) null(s). This should be impossible, We check non-nullity of primary keys at insertion."
                );
            }
        }
        let filtered_batch = filter_record_batch(&batch, &keep_row_builder.finish())?;
        if filtered_batch.num_rows() > 0 {
            filtered.push(filtered_batch);
        }
    }

    *existing_batches = filtered;
    Ok(())
}

// Public wrappers for benchmarking with standard hasher
#[cfg(feature = "bench")]
pub mod bench_wrappers {
    use std::collections::{HashSet, hash_map::RandomState};

    use super::{
        RecordBatch, Result,
        check_and_filter_non_null_unique_primary_keys as check_and_filter_pks_impl,
        check_and_filter_unique_constraint as check_constraint_impl,
        extract_primary_keys_str as extract_pks_impl, filter_existing as filter_existing_impl,
    };

    /// Public wrapper for benchmarking `check_and_filter_non_null_unique_primary_keys`
    #[expect(clippy::implicit_hasher)]
    pub fn check_and_filter_non_null_unique_primary_keys(
        pks: &[Option<String>],
        existing_pks: Option<&HashSet<String>>,
    ) -> Result<HashSet<String>> {
        check_and_filter_pks_impl::<RandomState>(pks, existing_pks)
    }

    /// Public wrapper for benchmarking `check_and_filter_unique_constraint`
    #[expect(clippy::implicit_hasher)]
    pub fn check_and_filter_unique_constraint(
        ids: &[&str],
        existing_ids: Option<&HashSet<String>>,
    ) -> Result<HashSet<String>> {
        check_constraint_impl::<RandomState>(ids, existing_ids)
    }

    /// Public wrapper for benchmarking `extract_primary_keys_str`
    pub fn extract_primary_keys_str(
        batch: &RecordBatch,
        pk_indices_ordered: &[usize],
    ) -> Result<Vec<Option<String>>> {
        extract_pks_impl(batch, pk_indices_ordered)
    }

    /// Public wrapper for benchmarking `filter_existing`
    #[expect(clippy::implicit_hasher)]
    pub fn filter_existing(
        existing_batches: &mut Vec<RecordBatch>,
        overwriting_primary_keys: &HashSet<String>,
        pk_indices_ordered: &[usize],
    ) -> Result<()> {
        filter_existing_impl::<RandomState>(
            existing_batches,
            overwriting_primary_keys,
            pk_indices_ordered,
        )
    }
}

fn primary_key_identifier(
    rb: &[&RecordBatch],
    primary_keys_ordered: &[usize],
) -> Result<Vec<Option<String>>> {
    // Create unique string for each primary key across all `new_batches` rows.
    let new_keys: Vec<_> = rb
        .iter()
        .map(|b| extract_primary_keys_str(b, primary_keys_ordered))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(new_keys)
}

#[async_trait]
impl DataSink for MemSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.batches.len();

        // Collect data into partitions (round-robin distribution)
        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        let mut data = data;
        while let Some(batch) = data
            .next()
            .await
            .transpose()
            .map_err(check_and_mark_retriable_error)?
        {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        // Ensure new data has no primary key conflicts internally, and generate primary key ids for later comparison to existing partition data.
        // We must also check for null values in primary keys. With that we can safely assume [`self.batches`] has no null primary keys.
        //
        // For InsertOp::Replace, we allow duplicate primary keys in new data because the operation will:
        // 1. Remove all existing rows matching ANY of the new primary keys
        // 2. Insert all new rows (even if they share primary keys)
        // This is essential for caching scenarios where multiple result rows share the same request metadata.
        let mut new_key_set: HashSet<
            String,
            std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>,
        > = HashSet::default();
        if let Some(ref pks) = self.primary_key {
            let batch_flat: Vec<_> = new_batches.iter().flatten().collect();
            let new_primary_key_ids = primary_key_identifier(&batch_flat, pks)?;

            // For InsertOp::Replace, we don't require unique primary keys in new data
            // because we'll remove all existing rows with these keys before inserting
            if matches!(self.overwrite, InsertOp::Replace) {
                // Just collect unique keys and check for nulls, don't enforce uniqueness
                for id in &new_primary_key_ids {
                    if let Some(key) = id {
                        new_key_set.insert(key.to_string());
                    } else {
                        return Err(DataFusionError::Execution(
                            "Primary key values cannot be null".to_string(),
                        ));
                    }
                }
            } else {
                // For Append/Overwrite, require unique primary keys
                new_key_set = check_and_filter_non_null_unique_primary_keys::<
                    std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>,
                >(&new_primary_key_ids, None)?;
            }
        }

        let mut writable_targets: Vec<_> =
            futures::future::join_all(self.batches.iter().map(|target| target.write())).await;

        for (target, mut batches) in writable_targets.iter_mut().zip(new_batches.into_iter()) {
            // Depending on [`InsertOp`], we may need to mutate the existing `target` before adding new data.
            match self.overwrite {
                // Ensure no primary key conflicts between new data that is being appended, and existing data (since we are not replacing).
                InsertOp::Append => {
                    if let Some(ref pks) = self.primary_key {
                        // Mem-table only supports on_conflict upsert that matches primary keys, so we
                        // remove existing data that collides with new primary keys similarly to `InsertOp::Replace`.
                        if self.on_conflict.is_some() {
                            filter_existing(&mut *target, &new_key_set, pks)?;
                        }

                        for rb in &**target {
                            let batch_pks = extract_primary_keys_str(rb, pks)?;
                            let _ = check_and_filter_non_null_unique_primary_keys::<
                                std::hash::BuildHasherDefault<XxHash3_64WithFixedSeed>,
                            >(&batch_pks, Some(&new_key_set))?;
                        }
                    }
                }
                // Already handled primary conflicts in new data.
                InsertOp::Overwrite => {
                    target.clear();
                }
                // Remove existing data that collides with new primary keys. New data will be added in their place.
                InsertOp::Replace => {
                    if let Some(ref pks) = self.primary_key {
                        filter_existing(&mut *target, &new_key_set, pks)?;
                    }
                }
            }

            // IMPORTANT: Sort happens AFTER deduplication/filtering to ensure we only sort
            // the final data that will actually be written. This matches Cayenne's behavior
            // where sorting happens after retention filters are applied.
            if !self.sort_columns.is_empty() && !batches.is_empty() {
                // Concatenate batches in this partition for sorting
                let schema = batches[0].schema();
                let combined_batch = if batches.len() == 1 {
                    // SAFETY: We've just checked that batches.len() == 1, so pop() cannot fail
                    match batches.pop() {
                        Some(batch) => batch,
                        None => unreachable!("batches.len() == 1 guarantees pop() succeeds"),
                    }
                } else {
                    arrow::compute::concat_batches(&schema, &batches)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                };

                let sorted_stream = RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    stream::iter(vec![Ok(combined_batch)]),
                );

                let sorted_stream = util::stream_utils::sort_stream(
                    Box::pin(sorted_stream),
                    &self.sort_columns,
                    context,
                )?;

                // Collect sorted batches
                batches = datafusion::physical_plan::common::collect(sorted_stream).await?;
            }

            target.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}

#[async_trait]
impl DeletionTableProvider for MemTable {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(MemDeletionSink::new(
                self.batches.clone(),
                self.schema(),
                filters,
            )),
            &self.schema(),
        )))
    }
}

struct MemDeletionSink {
    batches: Vec<PartitionData>,
    schema: SchemaRef,
    filters: Vec<Expr>,
}

impl MemDeletionSink {
    fn new(batches: Vec<PartitionData>, schema: SchemaRef, filters: &[Expr]) -> Self {
        Self {
            batches,
            schema,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for MemDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let batches = self.batches.clone();

        let ctx = SessionContext::new();
        let mut tmp_batches = vec![vec![]; batches.len()];

        for (i, partition) in batches.iter().enumerate() {
            let mut partition_vec = partition.write().await;
            tmp_batches[i].append(&mut *partition_vec);
        }

        let provider = MemTable::try_new(Arc::clone(&self.schema), tmp_batches)?;

        let mut df = DataFrame::new(
            ctx.state(),
            LogicalPlanBuilder::scan("?table?", provider_as_source(Arc::new(provider)), None)?
                .build()?,
        );

        let mut count = df.clone().count().await?;

        for filter in self.filters.clone() {
            df = df.filter(is_not_true(filter))?;
        }

        count -= df.clone().count().await?;
        let mut new_batches = vec![vec![]; batches.len()];
        let mut i = 0;
        for vec in df.collect_partitioned().await? {
            for batch in vec {
                new_batches[i].push(batch);
            }

            i = (i + 1) % batches.len();
        }

        for (target, mut batches) in batches.iter().zip(new_batches.into_iter()) {
            target.write().await.append(&mut batches);
        }

        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema, SchemaRef},
    };
    use arrow_array::Array;
    use arrow_buffer::ArrowNativeType;
    use datafusion::{
        catalog::TableProvider,
        common::{Constraint, Constraints},
        execution::context::SessionContext,
        logical_expr::{cast, col, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::{on_conflict::OnConflict, test::MockExec};

    use crate::{arrow::write::MemTable, delete::DeletionTableProvider};

    fn create_batch_with_string_columns(data: &[(&str, Vec<&str>)]) -> (RecordBatch, SchemaRef) {
        let fields: Vec<_> = data
            .iter()
            .map(|(name, _)| {
                arrow::datatypes::Field::new((*name).to_string(), DataType::Utf8, false)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays = data
            .iter()
            .map(|(_, values)| {
                let arr = StringArray::from(values.clone());
                Arc::new(arr) as Arc<dyn arrow::array::Array>
            })
            .collect::<Vec<_>>();

        (
            RecordBatch::try_new(Arc::clone(&schema), arrays).expect("data should be created"),
            Arc::clone(&schema),
        )
    }

    fn create_batch_with_nullable_string_columns(
        data: &[(&str, Vec<Option<&str>>)],
    ) -> (RecordBatch, SchemaRef) {
        let fields: Vec<_> = data
            .iter()
            .map(|(name, _)| {
                arrow::datatypes::Field::new((*name).to_string(), DataType::Utf8, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays = data
            .iter()
            .map(|(_, values)| {
                let arr = StringArray::from(values.clone());
                Arc::new(arr) as Arc<dyn arrow::array::Array>
            })
            .collect::<Vec<_>>();

        (
            RecordBatch::try_new(Arc::clone(&schema), arrays).expect("data should be created"),
            Arc::clone(&schema),
        )
    }

    #[tokio::test]
    async fn test_write_all_append_not_primary_key() {
        let (rb, schema) = create_batch_with_string_columns(&[(
            "primary_key",
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
        )]);
        let table = MemTable::try_new(schema, vec![vec![rb]]).expect("mem table should be created");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) = create_batch_with_string_columns(&[(
            "primary_key",
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
        )]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful")
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array")
            .value(0)
            .to_i64()
            .expect("insert_into result should return i64");

        assert_eq!(result, 3);

        // Ensure new values have changed correctly.
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let mut results = vec![];
        for rb in &result {
            let values: Vec<_> = rb
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("result should be StringArray")
                .into_iter()
                .collect();
            results.extend(values.clone());
        }

        assert_eq!(
            vec![
                Some("1970-01-01"),
                Some("2012-12-01T11:11:11Z"),
                Some("2012-12-01T11:11:12Z"),
                Some("1970-01-01"),
                Some("2012-12-01T11:11:11Z"),
                Some("2012-12-01T11:11:12Z")
            ],
            results
        );
    }

    #[tokio::test]
    async fn test_try_with_constraints() {
        // Primary key constraint
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "1970-01-01"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        assert!(
            MemTable::try_new(schema, vec![vec![rb]])
                .expect("mem table should be created")
                .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                    vec![0],
                )]))
                .await
                .is_err(),
            "MemTable::try_with_constraints should check constraints on initial data"
        );

        // Unique constraint
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "constraint",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "1970-01-01"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        assert!(
            MemTable::try_new(schema, vec![vec![rb]])
                .expect("mem table should be created")
                .try_with_constraints(Constraints::new_unverified(vec![Constraint::Unique(vec![
                    0
                ],)]))
                .await
                .is_err(),
            "MemTable::try_with_constraints should check constraints on initial data"
        );

        // Unique constraint, nullity is not checked.
        let (rb, schema) = create_batch_with_nullable_string_columns(&[
            (
                "constraint",
                vec![Some("2012-12-01T11:11:11Z"), None, Some("1970-01-01")],
            ),
            ("value", vec![Some("a"), Some("b"), Some("c")]),
        ]);
        assert!(
            MemTable::try_new(schema, vec![vec![rb]])
                .expect("mem table should be created")
                .try_with_constraints(Constraints::new_unverified(vec![Constraint::Unique(vec![
                    0
                ],)]))
                .await
                .is_ok(),
            "MemTable::try_with_constraints should not check nullity on [`Constraint::Unique`]."
        );
    }

    #[tokio::test]
    async fn test_write_all_replace_primary_key() {
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await
            .expect("satisfy primary key constraints");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            ("primary_key", vec!["2012-12-01T11:11:11Z"]),
            ("value", vec!["y"]),
        ]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Replace,
            )
            .await
            .expect("insertion should be successful");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful")
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array")
            .value(0)
            .to_i64()
            .expect("insert_into result should return i64");

        assert_eq!(result, 1);

        // Ensure new values have changed correctly.
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let mut results = vec![];
        for rb in &result {
            let values: Vec<_> = rb
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("result should be StringArray")
                .into_iter()
                .collect();
            results.extend(values.clone());
        }
        assert_eq!(vec![Some("a"), Some("c"), Some("y")], results);
    }

    #[tokio::test]
    async fn test_write_all_overwrite_primary_key() {
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await
            .expect("satisfy primary key constraints");
        let ctx = SessionContext::new();
        let state = ctx.state();
        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:21Z", "2012-12-01T11:11:22Z"],
            ),
            ("value", vec!["x", "y", "z"]),
        ]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Overwrite,
            )
            .await
            .expect("insertion should be successful");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful")
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array")
            .value(0)
            .to_i64()
            .expect("insert_into result should return i64");

        assert_eq!(result, 3);

        // Ensure new values have changed correctly.
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let mut results = vec![];
        for rb in &result {
            let values: Vec<_> = rb
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("result should be StringArray")
                .into_iter()
                .collect();
            results.extend(values.clone());
        }

        assert_eq!(vec![Some("x"), Some("y"), Some("z")], results);
    }

    #[tokio::test]
    async fn test_write_all_append_primary_key_conflict() {
        let (rb, schema) = create_batch_with_string_columns(&[(
            "primary_key",
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
        )]);
        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await
            .expect("satisfy primary key constraints");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) =
            create_batch_with_string_columns(&[("primary_key", vec!["1970-01-01"])]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        assert!(
            collect(insertion, ctx.task_ctx()).await.is_err(),
            "insertion should fail due to primary key conflict"
        );
    }

    #[tokio::test]
    async fn test_write_all_append_primary_key_on_conflict_upsert() {
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await
            .expect("satisfy primary key constraints")
            .with_on_conflict(
                OnConflict::try_from("upsert:primary_key").expect("create on_conflict"),
            );
        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            ("primary_key", vec!["1970-01-01", "1970-01-02"]),
            ("value", vec!["x", "y"]),
        ]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful")
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array")
            .value(0)
            .to_i64()
            .expect("insert_into result should return i64");

        assert_eq!(result, 2);

        // Ensure new values have changed correctly.
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let mut results = vec![];
        for rb in &result {
            let values: Vec<_> = rb
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("result should be StringArray")
                .into_iter()
                .collect();
            results.extend(values.clone());
        }

        assert_eq!(vec![Some("b"), Some("c"), Some("x"), Some("y")], results);
    }

    #[tokio::test]
    async fn test_write_all_append_primary_key() {
        let (rb, schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            ),
            ("value", vec!["a", "b", "c"]),
        ]);
        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await
            .expect("satisfy primary key constraints");
        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            (
                "primary_key",
                vec!["1970-01-02", "2012-12-01T11:11:21Z", "2012-12-01T11:11:22Z"],
            ),
            ("value", vec!["x", "y", "z"]),
        ]);
        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful")
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array")
            .value(0)
            .to_i64()
            .expect("insert_into result should return i64");

        assert_eq!(result, 3);

        // Ensure new values have changed correctly.
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let mut results = vec![];
        for rb in &result {
            let values: Vec<_> = rb
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("result should be StringArray")
                .into_iter()
                .collect();
            results.extend(values.clone());
        }

        assert_eq!(
            vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("x"),
                Some("y"),
                Some("z")
            ],
            results
        );
    }

    #[tokio::test]
    #[expect(clippy::unreadable_literal)]
    async fn test_delete_from() {
        let (rb, schema) = create_batch_with_string_columns(&[(
            "time_in_string",
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
        )]);
        let table = MemTable::try_new(schema, vec![vec![rb]]).expect("mem table should be created");
        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));

        let plan = table
            .delete_from(&state, &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");

        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_composite_primary_key() {
        // Test composite primary key handling
        let (rb, schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "a", "b", "b"]),
            ("pk2", vec!["1", "2", "1", "2"]),
            ("value", vec!["v1", "v2", "v3", "v4"]),
        ]);

        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![
                Constraint::PrimaryKey(vec![0, 1]), // Composite key on pk1 and pk2
            ]))
            .await
            .expect("satisfy composite primary key constraints");

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Try to insert duplicate composite key
        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "c"]),
            ("pk2", vec!["1", "1"]),
            ("value", vec!["v5", "v6"]),
        ]);

        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        assert!(
            collect(insertion, ctx.task_ctx()).await.is_err(),
            "insertion should fail due to composite primary key conflict on (a,1)"
        );
    }

    #[tokio::test]
    async fn test_multiple_partitions() {
        // Test with multiple partitions
        let (rb1, schema) =
            create_batch_with_string_columns(&[("id", vec!["1", "2"]), ("value", vec!["a", "b"])]);
        let (rb2, _) =
            create_batch_with_string_columns(&[("id", vec!["3", "4"]), ("value", vec!["c", "d"])]);

        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![rb1], vec![rb2]])
            .expect("mem table with multiple partitions should be created");

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Verify scanning returns all data from all partitions
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("scan should succeed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("collect should succeed");

        let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 4, "should have all 4 rows from both partitions");
    }

    #[tokio::test]
    async fn test_null_primary_key_rejection() {
        // Test that null primary keys are rejected
        let (rb, schema) = create_batch_with_nullable_string_columns(&[
            ("id", vec![Some("1"), None, Some("3")]),
            ("value", vec![Some("a"), Some("b"), Some("c")]),
        ]);

        let result = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
            .await;

        assert!(
            result.is_err(),
            "should reject null values in primary key column"
        );
    }

    #[tokio::test]
    async fn test_empty_table_operations() {
        // Test operations on empty table
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Utf8, false),
        ]));

        let table = MemTable::try_new(Arc::clone(&schema), vec![])
            .expect("empty mem table should be created");

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test scan on empty table
        let plan = table
            .scan(&state, None, &[], None)
            .await
            .expect("scan should succeed on empty table");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("collect should succeed");

        assert_eq!(result.len(), 0, "empty table should return no batches");

        // Test insert into empty table
        let (insert_rb, _) =
            create_batch_with_string_columns(&[("id", vec!["1", "2"]), ("value", vec!["a", "b"])]);

        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], Arc::clone(&schema)));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should succeed");

        let result = collect(insertion, ctx.task_ctx())
            .await
            .expect("insert should succeed");

        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("should be u64")
                .value(0),
            2,
            "should have inserted 2 rows"
        );
    }

    #[tokio::test]
    async fn test_extract_primary_keys_str() {
        // Test the primary key extraction function directly
        let (rb, _) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "b", "c"]),
            ("pk2", vec!["1", "2", "3"]),
            ("data", vec!["x", "y", "z"]),
        ]);

        // Single column primary key
        let keys = super::extract_primary_keys_str(&rb, &[0]).expect("extraction should succeed");
        assert_eq!(
            keys,
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );

        // Composite primary key
        let keys =
            super::extract_primary_keys_str(&rb, &[0, 1]).expect("extraction should succeed");
        assert_eq!(
            keys,
            vec![
                Some("a|1".to_string()),
                Some("b|2".to_string()),
                Some("c|3".to_string())
            ]
        );

        // Test with nullable values
        let (rb_nullable, _) = create_batch_with_nullable_string_columns(&[
            ("pk1", vec![Some("a"), None, Some("c")]),
            ("pk2", vec![Some("1"), Some("2"), Some("3")]),
        ]);

        let keys = super::extract_primary_keys_str(&rb_nullable, &[0, 1])
            .expect("extraction should succeed");
        assert_eq!(keys[0], Some("a|1".to_string()));
        assert_eq!(keys[1], None, "should be None when any part is null");
        assert_eq!(keys[2], Some("c|3".to_string()));
    }

    #[tokio::test]
    async fn test_check_and_filter_functions() {
        // Test check_and_filter_non_null_unique_primary_keys
        let pks = vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
        ];

        let result = super::check_and_filter_non_null_unique_primary_keys::<
            std::collections::hash_map::RandomState,
        >(&pks, None)
        .expect("should succeed with unique keys");
        assert_eq!(result.len(), 3);

        // Test with duplicates
        let pks_with_dup = vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("a".to_string()),
        ];

        let result = super::check_and_filter_non_null_unique_primary_keys::<
            std::collections::hash_map::RandomState,
        >(&pks_with_dup, None);
        assert!(result.is_err(), "should fail with duplicate keys");

        // Test with null
        let pks_with_null = vec![Some("a".to_string()), None, Some("c".to_string())];

        let result = super::check_and_filter_non_null_unique_primary_keys::<
            std::collections::hash_map::RandomState,
        >(&pks_with_null, None);
        assert!(result.is_err(), "should fail with null primary key");

        // Test against existing set
        let mut existing = HashSet::new();
        existing.insert("b".to_string());

        let pks_conflict = vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string()),
        ];

        let result = super::check_and_filter_non_null_unique_primary_keys::<
            std::collections::hash_map::RandomState,
        >(&pks_conflict, Some(&existing));
        assert!(
            result.is_err(),
            "should fail when key exists in existing set"
        );
    }

    #[tokio::test]
    async fn test_large_dataset_optimization_path() {
        // Test the optimization path for large datasets (> 10,000 rows)
        let large_ids_owned: Vec<String> = (0..15_000).map(|i| format!("id_{i:05}")).collect();
        let large_ids: Vec<&str> = large_ids_owned.iter().map(String::as_str).collect();

        // Test successful case with unique values
        let result = super::check_and_filter_unique_constraint::<
            std::collections::hash_map::RandomState,
        >(&large_ids, None)
        .expect("should succeed with unique large dataset");
        assert_eq!(result.len(), 15_000);

        // Test with duplicate in large dataset
        let mut large_ids_dup_owned: Vec<String> = large_ids_owned.clone();
        large_ids_dup_owned[10_000] = large_ids_dup_owned[5_000].clone(); // Create duplicate
        let large_ids_dup: Vec<&str> = large_ids_dup_owned.iter().map(String::as_str).collect();

        let result = super::check_and_filter_unique_constraint::<
            std::collections::hash_map::RandomState,
        >(&large_ids_dup, None);
        assert!(
            result.is_err(),
            "should fail with duplicate in large dataset"
        );
    }

    #[tokio::test]
    async fn test_filter_existing() {
        // Test the filter_existing function
        let (rb1, _) = create_batch_with_string_columns(&[
            ("id", vec!["1", "2", "3"]),
            ("value", vec!["a", "b", "c"]),
        ]);
        let (rb2, _) = create_batch_with_string_columns(&[
            ("id", vec!["4", "5", "6"]),
            ("value", vec!["d", "e", "f"]),
        ]);

        let mut existing_batches = vec![rb1, rb2];
        let mut overwriting_keys = HashSet::new();
        overwriting_keys.insert("2".to_string());
        overwriting_keys.insert("5".to_string());

        super::filter_existing(&mut existing_batches, &overwriting_keys, &[0])
            .expect("filter should succeed");

        // Collect remaining IDs
        let mut remaining_ids = Vec::new();
        for batch in &existing_batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("should be string array");
            for i in 0..ids.len() {
                remaining_ids.push(ids.value(i));
            }
        }

        assert_eq!(
            remaining_ids,
            vec!["1", "3", "4", "6"],
            "should filter out ids 2 and 5"
        );
    }

    #[tokio::test]
    async fn test_on_conflict_validation() {
        // Test on_conflict validation with primary key mismatch
        let (rb, schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "b"]),
            ("pk2", vec!["1", "2"]),
            ("value", vec!["v1", "v2"]),
        ]);

        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![
                Constraint::PrimaryKey(vec![0, 1]), // Composite key
            ]))
            .await
            .expect("constraints should be satisfied")
            .with_on_conflict(
                OnConflict::try_from("upsert:pk1").expect("create on_conflict"), // Only one column
            );

        let ctx = SessionContext::new();
        let state = ctx.state();

        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["c"]),
            ("pk2", vec!["3"]),
            ("value", vec!["v3"]),
        ]);

        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let result = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await;

        assert!(
            result.is_err(),
            "should fail when on_conflict columns don't match primary key"
        );
    }

    #[tokio::test]
    async fn test_on_conflict_validation_column_order() {
        // Tests that composite primary key validation works correctly when
        // primary key / on_conflict columns indices are not lexicographically ordered.
        let (rb, schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "b"]),
            ("pk2", vec!["1", "2"]),
            ("value", vec!["v1", "v2"]),
        ]);

        let table = MemTable::try_new(schema, vec![vec![rb]])
            .expect("mem table should be created")
            .try_with_constraints(Constraints::new_unverified(vec![
                Constraint::PrimaryKey(vec![1, 0]), // Composite key
            ]))
            .await
            .expect("constraints should be satisfied")
            .with_on_conflict(
                OnConflict::try_from("upsert:(pk2,pk1)").expect("create on_conflict"),
            );

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Try to insert duplicate composite key
        let (insert_rb, new_schema) = create_batch_with_string_columns(&[
            ("pk1", vec!["a", "c"]),
            ("pk2", vec!["1", "1"]),
            ("value", vec!["v5", "v6"]),
        ]);

        let exec = Arc::new(MockExec::new(vec![Ok(insert_rb)], new_schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should be successful");

        assert!(
            collect(insertion, ctx.task_ctx()).await.is_ok(),
            "insertion should succeed"
        );
    }

    #[tokio::test]
    async fn test_delete_with_multiple_filters() {
        // Test deletion with multiple filters
        let (rb, schema) = create_batch_with_string_columns(&[
            ("id", vec!["1", "2", "3", "4", "5"]),
            ("category", vec!["A", "B", "A", "B", "C"]),
            ("value", vec!["10", "20", "30", "40", "50"]),
        ]);

        let table = MemTable::try_new(schema, vec![vec![rb]]).expect("mem table should be created");

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Delete rows where category = 'A' OR id = '4'
        let filter1 = col("category").eq(lit("A"));
        let filter2 = col("id").eq(lit("4"));

        let plan = table
            .delete_from(&state, &[filter1, filter2])
            .await
            .expect("deletion should succeed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion should complete");

        let deleted_count = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("should be u64")
            .value(0);

        // Should delete rows with id 1, 3 (category A) and 4 (id match)
        assert_eq!(deleted_count, 3, "should delete 3 rows");

        // Verify remaining data
        let scan = table
            .scan(&state, None, &[], None)
            .await
            .expect("scan should succeed");
        let remaining = collect(scan, ctx.task_ctx())
            .await
            .expect("collect should succeed");

        let remaining_ids: Vec<_> = remaining[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("should be string")
            .iter()
            .flatten()
            .collect();

        assert_eq!(
            remaining_ids,
            vec!["2", "5"],
            "only ids 2 and 5 should remain"
        );
    }

    #[tokio::test]
    async fn test_schema_mismatch_detection() {
        // Test that schema mismatches are properly detected
        let schema1 = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Int32, false),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Utf8, false), // Different type
        ]));

        let _batch1 = RecordBatch::try_new(
            Arc::clone(&schema1),
            vec![
                Arc::new(StringArray::from(vec!["1", "2"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            ],
        )
        .expect("batch should be created");

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema2),
            vec![
                Arc::new(StringArray::from(vec!["3", "4"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch should be created");

        let result = MemTable::try_new(schema1, vec![vec![batch2]]);
        assert!(result.is_err(), "should fail with mismatched schema");
    }

    #[tokio::test]
    async fn test_round_robin_partitioning() {
        // Test that inserts are distributed round-robin across partitions
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Utf8,
            false,
        )]));

        // Create table with 3 partitions
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![], vec![], vec![]])
            .expect("3-partition table should be created");

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Insert 9 rows
        let mut batches = Vec::new();
        for i in 0..9 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![format!("{i}")]))],
            )
            .expect("batch should be created");
            batches.push(Ok(batch));
        }

        let exec = Arc::new(MockExec::new(batches, schema));
        let insertion = table
            .insert_into(
                &state,
                exec,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insertion should succeed");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert should complete");

        // Verify distribution across partitions
        for (i, partition) in table.batches.iter().enumerate() {
            let p = partition.read().await;
            let row_count: usize = p.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                row_count, 3,
                "partition {i} should have 3 rows due to round-robin distribution"
            );
        }
    }
}
