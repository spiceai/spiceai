/*
Copyright 2026, Spice AI, Inc.

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

//! Physical execution plans for single-node Cayenne DDL operations.
//!
//! [`CayenneCreateTableExec`], [`CayenneDropTableExec`], [`CayenneCreateSchemaExec`] are thin
//! wrappers around the corresponding [`super::operations`] functions.
//!
//! [`CayenneMergeExec`] implements local MERGE (join + delete + insert) on a single
//! `TableProvider`; it is also used by the runtime's broadcast planner.
//!
//! The runtime crate provides broadcast variants of Create/Drop that forward DDL to executor
//! nodes after calling the same `operations` functions.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::catalog::CatalogProviderList;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
};

use datafusion_common::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;

use super::operations::{CreateTableParams, create_schema, create_table, drop_table};
use crate::ddl::get_cayenne_provider;
use crate::provider::CayenneTableProvider;
use runtime_table_partition::provider::PartitionTableProvider;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
}

fn ddl_plan_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

fn merge_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

// ── CayenneCreateTableExec ────────────────────────────────────────────────────

/// Single-node physical plan for `CREATE TABLE` on a Cayenne catalog.
///
/// Resolves the [`CayenneCatalogProvider`] via a direct downcast and delegates
/// to [`create_table`].  The runtime's broadcast exec wraps this logic and adds
/// executor forwarding and partition metadata initialisation.
pub struct CayenneCreateTableExec {
    params: CreateTableParams,
    catalog_list: Arc<dyn CatalogProviderList>,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for CayenneCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateTableExec")
            .field("table_name", &self.params.table_name)
            .field("catalog_name", &self.params.catalog_name)
            .field("schema_name", &self.params.schema_name)
            .finish_non_exhaustive()
    }
}

impl CayenneCreateTableExec {
    /// Construct a new single-node  execution plan.
    #[must_use]
    pub fn new(
        params: CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            params,
            catalog_list,
            runtime_env,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for CayenneCreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneCreateTableExec: {}.{}.{}",
            self.params.catalog_name, self.params.schema_name, self.params.table_name
        )
    }
}

impl ExecutionPlan for CayenneCreateTableExec {
    fn name(&self) -> &'static str {
        "CayenneCreateTableExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let catalog_name = self.params.catalog_name.clone();
        let schema_name = self.params.schema_name.clone();
        let table_name = self.params.table_name.clone();
        let arrow_schema = Arc::clone(&self.params.arrow_schema);
        let primary_key = self.params.primary_key.clone();
        let partition_expr_sql = self.params.partition_expr_sql.clone();
        let if_not_exists = self.params.if_not_exists;
        let like_source_table = self.params.like_source_table.clone();
        let ctx = self.params.ctx.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let runtime_env = Arc::clone(&self.runtime_env);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            let params = CreateTableParams {
                table_name: table_name.clone(),
                schema_name,
                catalog_name,
                arrow_schema,
                primary_key,
                partition_expr_sql,
                if_not_exists,
                like_source_table,
                ctx,
            };
            let outcome = create_table(params, cayenne_provider, runtime_env).await?;

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![outcome.message]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── CayenneDropTableExec ──────────────────────────────────────────────────────

/// Single-node physical plan for `DROP TABLE` on a Cayenne catalog.
pub struct CayenneDropTableExec {
    table_name: String,
    if_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for CayenneDropTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDropTableExec")
            .field("table_name", &self.table_name)
            .field("catalog_name", &self.df_catalog_name)
            .finish_non_exhaustive()
    }
}

impl CayenneDropTableExec {
    /// Construct a new single-node  execution plan.
    #[must_use]
    pub fn new(
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for CayenneDropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneDropTableExec: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }
}

impl ExecutionPlan for CayenneDropTableExec {
    fn name(&self) -> &'static str {
        "CayenneDropTableExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let if_exists = self.if_exists;
        let catalog_name = self.df_catalog_name.clone();
        let schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;
            let outcome = drop_table(
                &table_name,
                &schema_name,
                &catalog_name,
                if_exists,
                cayenne_provider,
                &df_catalog,
            )
            .await?;
            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![outcome.message]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── CayenneCreateSchemaExec ───────────────────────────────────────────────────

/// Physical plan for `CREATE SCHEMA` on a Cayenne catalog.
///
/// Schema creation requires no distribution — it is identical in single-node
/// and broadcast mode.
pub struct CayenneCreateSchemaExec {
    schema_name: String,
    if_not_exists: bool,
    df_catalog_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for CayenneCreateSchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateSchemaExec")
            .field("schema_name", &self.schema_name)
            .field("catalog_name", &self.df_catalog_name)
            .finish_non_exhaustive()
    }
}

impl CayenneCreateSchemaExec {
    /// Construct a new  execution plan.
    #[must_use]
    pub fn new(
        schema_name: String,
        if_not_exists: bool,
        df_catalog_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            schema_name,
            if_not_exists,
            df_catalog_name,
            catalog_list,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for CayenneCreateSchemaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneCreateSchemaExec: {}.{}",
            self.df_catalog_name, self.schema_name
        )
    }
}

impl ExecutionPlan for CayenneCreateSchemaExec {
    fn name(&self) -> &'static str {
        "CayenneCreateSchemaExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema_name = self.schema_name.clone();
        let if_not_exists = self.if_not_exists;
        let catalog_name = self.df_catalog_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();
        let runtime_env = context.runtime_env();

        let stream = futures::stream::once(async move {
            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;
            let message = create_schema(
                &schema_name,
                &catalog_name,
                if_not_exists,
                cayenne_provider,
                runtime_env,
            )?;
            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![message]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── CayenneMergeExec ──────────────────────────────────────────────────────────

/// Physical execution plan for MERGE INTO on a Cayenne table.
///
/// Executes a streaming join between target and source (provided as
/// `join_plan`), then deletes matched target rows and inserts the
/// updated rows.
///
/// Memory usage is `O(|source| + |matched|)` — the target table is
/// never fully materialized.
pub struct CayenneMergeExec {
    /// Physical plan that produces the joined + projected rows
    /// (matched rows with updated column values).
    join_plan: Arc<dyn ExecutionPlan>,
    /// Target table provider for `insert_into`.
    target_provider: Arc<dyn TableProvider>,
    /// Session state for creating delete/insert plans.
    session_state: SessionState,
    /// Target-side ON key column names, used to build deletion filters.
    target_key_columns: Vec<String>,
    /// Output properties.
    properties: Arc<PlanProperties>,
}

impl CayenneMergeExec {
    /// Construct a new local execution plan.
    #[must_use]
    pub fn new(
        join_plan: Arc<dyn ExecutionPlan>,
        target_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        target_key_columns: Vec<String>,
    ) -> Self {
        let schema = merge_count_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            join_plan,
            target_provider,
            session_state,
            target_key_columns,
            properties,
        }
    }
}

impl std::fmt::Debug for CayenneMergeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneMergeExec")
            .field("target_key_columns", &self.target_key_columns)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayenneMergeExec: keys={:?}", self.target_key_columns)
    }
}

impl ExecutionPlan for CayenneMergeExec {
    fn name(&self) -> &'static str {
        "CayenneMergeExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.join_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CayenneMergeExec requires exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.target_provider),
            self.session_state.clone(),
            self.target_key_columns.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "CayenneMergeExec only supports partition 0, got {partition}"
            )));
        }

        let join_plan = Arc::clone(&self.join_plan);
        let target_provider = Arc::clone(&self.target_provider);
        let session_state = self.session_state.clone();
        let target_key_columns = self.target_key_columns.clone();
        let schema = merge_count_schema();
        let stream = futures::stream::once(async move {
            execute_merge(
                join_plan,
                target_provider,
                session_state,
                target_key_columns,
                context,
            )
            .await
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Core merge execution: run the join plan, delete matched rows, insert updated rows.
async fn execute_merge(
    join_plan: Arc<dyn ExecutionPlan>,
    target_provider: Arc<dyn TableProvider>,
    session_state: SessionState,
    target_key_columns: Vec<String>,
    context: Arc<TaskContext>,
) -> Result<RecordBatch, DataFusionError> {
    use futures::TryStreamExt;

    // Step 1: Execute the join plan to get matched rows with updated values.
    let join_stream = execute_stream(Arc::clone(&join_plan), Arc::clone(&context))?;
    let updated_batches: Vec<RecordBatch> = join_stream.try_collect().await?;

    let total_rows: usize = updated_batches.iter().map(RecordBatch::num_rows).sum();

    if total_rows == 0 {
        // No matches — nothing to do.
        return Ok(RecordBatch::try_from_iter_with_nullable(vec![(
            "count",
            Arc::new(UInt64Array::from(vec![0u64])) as ArrayRef,
            false,
        )])?);
    }

    // Normalize output to match the target table schema (including nullability).
    let target_schema = target_provider.schema();
    let normalized_batches = updated_batches
        .into_iter()
        .map(|batch| arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(DataFusionError::from)?;

    // Step 2: Validate no duplicate target keys in join output.
    // Per SQL MERGE semantics, each target row must match at most one source row.
    // If source has duplicate keys, the INNER JOIN produces multiple output rows
    // per target row. We must detect this *before* any mutations — otherwise the
    // delete would commit (removing the target row) but the count verification
    // would fail, leaving permanently missing rows.
    let matched_keys = validate_no_duplicate_target_keys(&normalized_batches, &target_key_columns)?;

    // Step 3+4: Delete matched rows from the target.
    //
    // For PositionBased Cayenne tables, use the fast path: build a HashSet of
    // matched key tuples and probe each file's rows with O(1) lookups. This
    // replaces the O(N) filter expression that the legacy path builds and
    // evaluates against every chunk of every file.
    let delete_count = if let Some(count) =
        try_key_probe_delete(&target_provider, &target_key_columns, matched_keys).await?
    {
        count
    } else {
        // Legacy path: build filter expression and push through delete_from.
        let delete_filter = build_delete_filter(&normalized_batches, &target_key_columns)?;
        let delete_plan = target_provider
            .delete_from(&session_state, vec![delete_filter])
            .await?;
        let delete_stream = execute_stream(delete_plan, Arc::clone(&context))?;
        let delete_batches: Vec<RecordBatch> = delete_stream.try_collect().await?;
        extract_dml_count(&delete_batches)
    };

    // Verify the delete count matches the expected number of rows.
    if delete_count != total_rows as u64 {
        return Err(DataFusionError::Execution(format!(
            "MERGE delete count mismatch: expected {total_rows} rows deleted, got {delete_count}"
        )));
    }

    // Step 5: Insert updated rows into the target.
    let input_exec = MemorySourceConfig::try_new_exec(&[normalized_batches], target_schema, None)?;
    let insert_plan = target_provider
        .insert_into(&session_state, input_exec, InsertOp::Append)
        .await?;
    let insert_stream = execute_stream(insert_plan, Arc::clone(&context))?;
    let insert_batches: Vec<RecordBatch> = insert_stream.try_collect().await?;

    // Verify the insert count matches.
    let insert_count = extract_dml_count(&insert_batches);
    if insert_count != total_rows as u64 {
        return Err(DataFusionError::Execution(format!(
            "MERGE insert count mismatch: expected {total_rows} rows inserted, got {insert_count}"
        )));
    }

    // Step 6: Return the count of updated rows.
    Ok(RecordBatch::try_from_iter_with_nullable(vec![(
        "count",
        Arc::new(UInt64Array::from(vec![total_rows as u64])) as ArrayRef,
        false,
    )])?)
}

/// Extract the row count from DML output batches (e.g., from `delete_from` or `insert_into`).
///
/// DML operations return a single batch with a `count` column. This sums all count values.
fn extract_dml_count(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("count")
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .into_iter()
                .flat_map(arrow::array::PrimitiveArray::iter)
                .flatten()
        })
        .sum()
}

/// Validate that the join output contains no duplicate target key tuples.
///
/// Per SQL MERGE semantics, each target row must match at most one source row.
/// If the source has duplicate keys, the INNER JOIN produces multiple output
/// rows per target row. This check runs *before* any mutations to prevent
/// the scenario where delete commits (removing target rows) but the subsequent
/// count verification fails, leaving permanently missing rows.
fn validate_no_duplicate_target_keys(
    batches: &[RecordBatch],
    key_columns: &[String],
) -> Result<HashSet<Vec<ScalarValue>>, DataFusionError> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    for batch in batches {
        // Resolve column indices for this batch.
        let col_indices: Vec<usize> = key_columns
            .iter()
            .map(|key_col| {
                batch.schema().index_of(key_col).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Key column '{key_col}' not found in join output: {e}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        for row_idx in 0..batch.num_rows() {
            // Build a composite key as a Vec<ScalarValue> for hashing.
            let key: Vec<ScalarValue> = col_indices
                .iter()
                .map(|&idx| ScalarValue::try_from_array(batch.column(idx), row_idx))
                .collect::<Result<Vec<_>, _>>()?;

            if !seen.insert(key) {
                let dup_display: Vec<String> = key_columns
                    .iter()
                    .zip(&col_indices)
                    .map(|(name, &idx)| {
                        let val = ScalarValue::try_from_array(batch.column(idx), row_idx)
                            .map_or_else(|_| "?".to_string(), |v| v.to_string());
                        format!("{name}={val}")
                    })
                    .collect();
                return Err(DataFusionError::Execution(format!(
                    "MERGE source has duplicate rows matching target key ({}). \
                     Per SQL MERGE semantics, each target row must match at most one source row. \
                     Deduplicate the source table before running MERGE.",
                    dup_display.join(", ")
                )));
            }
        }
    }
    Ok(seen)
}

/// Build a deletion filter from matched key column values.
///
/// For single-column keys, builds `key_col IN (val1, val2, ...)`.
/// For composite keys, builds tuple-aware OR-of-ANDs to avoid cross-product matches:
///   `(k1 = a1 AND k2 = b1) OR (k1 = a2 AND k2 = b2)`
fn build_delete_filter(
    batches: &[RecordBatch],
    key_columns: &[String],
) -> Result<datafusion::prelude::Expr, DataFusionError> {
    use datafusion::prelude::*;

    // Fast path: single key column uses a single IN-list.
    if key_columns.len() == 1 {
        let key_col = &key_columns[0];
        let mut values: Vec<datafusion::prelude::Expr> = Vec::new();
        for batch in batches {
            let col_idx = batch.schema().index_of(key_col).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Key column '{key_col}' not found in join output: {e}"
                ))
            })?;
            let array = batch.column(col_idx);
            for row_idx in 0..array.len() {
                let scalar = ScalarValue::try_from_array(array, row_idx)?;
                values.push(lit(scalar));
            }
        }
        if values.is_empty() {
            return Err(DataFusionError::Internal(
                "Failed to build delete filters: no key values extracted from matched rows"
                    .to_string(),
            ));
        }
        return Ok(col(key_col).in_list(values, false));
    }

    // Composite keys: build OR-of-ANDs for exact tuple matching.
    // Each matched row produces (k1 = v1 AND k2 = v2 AND ...) and rows are OR'd together.
    let col_indices: Vec<(&String, Vec<usize>)> = key_columns
        .iter()
        .map(|key_col| {
            let indices: Vec<usize> = batches
                .iter()
                .map(|batch| {
                    batch.schema().index_of(key_col).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Key column '{key_col}' not found in join output: {e}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((key_col, indices))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let mut row_predicates: Vec<datafusion::prelude::Expr> = Vec::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            // Build AND of all key column equalities for this row.
            let mut row_and: Option<datafusion::prelude::Expr> = None;
            for (key_col, indices) in &col_indices {
                let eq_expr = col(key_col.as_str()).eq(lit(ScalarValue::try_from_array(
                    batch.column(indices[batch_idx]),
                    row_idx,
                )?));
                row_and = Some(match row_and {
                    Some(existing) => existing.and(eq_expr),
                    None => eq_expr,
                });
            }
            if let Some(predicate) = row_and {
                row_predicates.push(predicate);
            }
        }
    }

    if row_predicates.is_empty() {
        return Err(DataFusionError::Internal(
            "Failed to build delete filters: no row predicates generated from matched rows"
                .to_string(),
        ));
    }

    // Combine row predicates with OR using a balanced binary tree.
    util::expr::combine_exprs_balanced(row_predicates, datafusion::prelude::Expr::or).ok_or(
        DataFusionError::Internal(
            "Failed to build delete filters: no row predicates generated".to_string(),
        ),
    )
}

/// Try the fast key-probe deletion path for `PositionBased` Cayenne tables.
///
/// Returns `Some(delete_count)` if the provider is a `PositionBased` Cayenne table
/// (direct, adapter-wrapped, or partitioned) and the deletion succeeded.
/// Returns `None` if the provider doesn't support the fast path, in which case
/// the caller should fall back to the legacy filter-based deletion.
async fn try_key_probe_delete(
    provider: &Arc<dyn TableProvider>,
    key_columns: &[String],
    matched_keys: HashSet<Vec<datafusion_common::ScalarValue>>,
) -> Result<Option<u64>, DataFusionError> {
    // Case 1: Direct CayenneTableProvider.
    if let Some(cayenne) = provider.as_any().downcast_ref::<CayenneTableProvider>() {
        if cayenne.is_position_based() {
            return cayenne
                .delete_matched_rows_by_key_probe(matched_keys, key_columns)
                .await
                .map(Some);
        }
        return Ok(None);
    }

    // Case 3: PartitionTableProvider wrapping per-partition Cayenne providers.
    // All partitions share the same table metadata and deletion strategy, so
    // checking the first partition is sufficient to decide the fast path.
    if let Some(partitioned) = provider.as_any().downcast_ref::<PartitionTableProvider>() {
        let providers = partitioned.partition_table_providers().await;
        if providers
            .first()
            .and_then(|p| p.as_any().downcast_ref::<CayenneTableProvider>())
            .is_none_or(|cayenne| !cayenne.is_position_based())
        {
            return Ok(None);
        }

        let mut total = 0u64;
        for pp in &providers {
            if let Some(cayenne) = pp.as_any().downcast_ref::<CayenneTableProvider>() {
                // Each partition's listing table only contains its own files, so
                // the full matched_keys set is passed to each — hash probes for
                // keys not in this partition simply find no matches.
                total += cayenne
                    .delete_matched_rows_by_key_probe(matched_keys.clone(), key_columns)
                    .await?;
            }
        }
        return Ok(Some(total));
    }

    Ok(None)
}
