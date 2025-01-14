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

use arrow::array::BooleanBuilder;
use arrow::compute::filter_record_batch;
use datafusion::catalog::Session;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};

use std::sync::{Arc, Mutex};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::common::{Constraint, Constraints, SchemaExt};
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{is_not_true, Expr, LogicalPlanBuilder};
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use datafusion_table_providers::util::retriable_error::check_and_mark_retriable_error;

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
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
            sort_order: Arc::new(Mutex::new(vec![])),
        })
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
        let mut constraint_keys: Vec<HashSet<_>> = Vec::with_capacity(constraints.iter().len());
        for b in &self.batches {
            let p = &*b.read().await;
            let p: Vec<_> = p.iter().collect();
            for (i, c) in constraints.iter().enumerate() {
                let valid_ids = match c {
                    Constraint::PrimaryKey(pk) => {
                        let pks = primary_key_identifier(&p, pk)?;
                        check_and_filter_non_null_unique_primary_keys(&pks, constraint_keys.get(i))?
                    }
                    Constraint::Unique(u) => {
                        let ids = constraint_identifiers(&p, u)?;
                        let as_str: Vec<_> = ids.iter().map(String::as_str).collect();
                        check_and_filter_unique_constraint(&as_str, constraint_keys.get(i))?
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
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }

    /// Returns an ExecutionPlan that inserts the execution results of a given [`ExecutionPlan`] into this [`MemTable`].
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
        if !self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
        {
            return Err(DataFusionError::Execution(
                "Inserting query must have the same schema with the table.".to_string(),
            ));
        }

        let primary_key = self.get_and_ensure_only_primary_keys()?;

        let sink = Arc::new(MemSink::new(self.batches.clone(), overwrite, primary_key));
        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            Arc::clone(&self.schema),
            None,
        )))
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
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
    ) -> Self {
        Self {
            batches,
            overwrite,
            primary_key: primary_key.map(|pks| {
                let mut z = pks.clone();
                z.sort_unstable();
                z
            }),
        }
    }
}

/// Check that all primary key ids are non-null and unique.
///
/// If `existing_pks` is provided, also check uniqueness of `pks` against `existing_pks`.
///
/// Returns a set of unique, non-null primary key ids.
fn check_and_filter_non_null_unique_primary_keys(
    pks: &[Option<String>],
    existing_pks: Option<&HashSet<String>>,
) -> Result<HashSet<String>> {
    let num_pks = pks.len();

    // First check uniqueness
    let non_null_pks: Vec<&str> = pks.iter().filter_map(|opt| opt.as_deref()).collect();
    let unique_set = check_and_filter_unique_constraint(&non_null_pks, existing_pks)?;

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
fn check_and_filter_unique_constraint(
    ids: &[&str],
    existing_ids: Option<&HashSet<String>>,
) -> Result<HashSet<String>> {
    let mut unique_set = HashSet::<String>::new();
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

/// Create primary key values for a [`RecordBatch`]. For composite keys, values are concatenated with a delimiter '|'.
///
/// `pk_indices_ordered` should be in ascending order.
///
/// If any primary key value is `Null`, the entire key is [`Option::None`].
fn extract_primary_keys_str(
    batch: &RecordBatch,
    pk_indices_ordered: &[usize],
) -> Result<Vec<Option<String>>> {
    let num_rows = batch.num_rows();
    let mut keys = Vec::with_capacity(num_rows);

    'row: for row_idx in 0..num_rows {
        let mut parts = Vec::with_capacity(pk_indices_ordered.len());
        for &col_idx in pk_indices_ordered {
            let col = batch.column(col_idx);
            let val = ScalarValue::try_from_array(col, row_idx)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            // Early exit creating the entire row if any part is null.
            if val.is_null() {
                keys.push(None);
                continue 'row;
            }
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
fn filter_existing(
    existing_batches: &mut Vec<RecordBatch>,
    overwriting_primary_keys: &HashSet<String>,
    pk_indices_ordered: &[usize],
) -> Result<()> {
    if existing_batches.is_empty() {
        return Ok(());
    }

    // Instead of concatenating, we can filter each batch individually
    let mut filtered = Vec::with_capacity(existing_batches.len());
    for batch in existing_batches.drain(..) {
        let keys = extract_primary_keys_str(&batch, pk_indices_ordered)?;

        let mut keep_row_builder = BooleanBuilder::with_capacity(keys.len());
        for k in keys {
            if let Some(k) = k {
                keep_row_builder.append_value(!overwriting_primary_keys.contains(&k));
            } else {
                unreachable!("Primary keys in `MemSink` record batch contain(s) null(s). This should be impossible, We check non-nullity of primary keys at insertion.");
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

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.batches.len();

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
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
        let mut new_key_set: HashSet<String> = HashSet::new();
        if let Some(ref pks) = self.primary_key {
            let batch_flat: Vec<_> = new_batches.iter().flatten().collect();
            let new_primary_key_ids = primary_key_identifier(&batch_flat, pks)?;
            new_key_set =
                check_and_filter_non_null_unique_primary_keys(&new_primary_key_ids, None)?;
        }

        let mut writable_targets: Vec<_> =
            futures::future::join_all(self.batches.iter().map(|target| target.write())).await;

        for (target, mut batches) in writable_targets.iter_mut().zip(new_batches.into_iter()) {
            // Depending on [`InsertOp`], we may need to mutate the existing `target` before adding new data.
            match self.overwrite {
                // Ensure no primary key conflicts between new data that is being appended, and existing data (since we are not replacing).
                InsertOp::Append => {
                    if let Some(ref pks) = self.primary_key {
                        for rb in &**target {
                            let batch_pks = extract_primary_keys_str(rb, pks)?;
                            let _ = check_and_filter_non_null_unique_primary_keys(
                                &batch_pks,
                                Some(&new_key_set),
                            )?;
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
    use std::sync::Arc;

    use arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema, SchemaRef},
    };
    use arrow_buffer::ArrowNativeType;
    use datafusion::{
        catalog::TableProvider,
        common::{Constraint, Constraints},
        execution::context::SessionContext,
        logical_expr::{cast, col, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;

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
    #[allow(clippy::unreadable_literal)]
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
            .delete_from(&state, &vec![filter])
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
}
