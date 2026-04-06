/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Physical execution plans for planner-produced logical nodes.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, execute_stream,
};
use datafusion_datasource::memory::MemorySourceConfig;

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
    properties: PlanProperties,
}

impl CayenneMergeExec {
    #[must_use]
    pub fn new(
        join_plan: Arc<dyn ExecutionPlan>,
        target_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        target_key_columns: Vec<String>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
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

    fn properties(&self) -> &PlanProperties {
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

        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

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
    validate_no_duplicate_target_keys(&normalized_batches, &target_key_columns)?;

    // Step 3: Build deletion filters from the matched key values.
    // Uses tuple-aware OR-of-ANDs to avoid cross-product matches with composite keys.
    let delete_filters = build_delete_filters(&normalized_batches, &target_key_columns)?;

    // Step 4: Delete matched rows from the target.
    let delete_plan = target_provider
        .delete_from(&session_state, delete_filters)
        .await?;
    let delete_stream = execute_stream(delete_plan, Arc::clone(&context))?;
    let delete_batches: Vec<RecordBatch> = delete_stream.try_collect().await?;

    // Verify the delete count matches the expected number of rows.
    let delete_count = extract_dml_count(&delete_batches);
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
) -> Result<(), DataFusionError> {
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
            let key: Vec<datafusion::common::ScalarValue> = col_indices
                .iter()
                .map(|&idx| {
                    datafusion::common::ScalarValue::try_from_array(batch.column(idx), row_idx)
                })
                .collect::<Result<Vec<_>, _>>()?;

            if !seen.insert(key) {
                let dup_display: Vec<String> = key_columns
                    .iter()
                    .zip(&col_indices)
                    .map(|(name, &idx)| {
                        let val = datafusion::common::ScalarValue::try_from_array(
                            batch.column(idx),
                            row_idx,
                        )
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
    Ok(())
}

/// Build deletion filter expressions from matched key column values.
///
/// For single-column keys, builds `key_col IN (val1, val2, ...)`.
/// For composite keys, builds tuple-aware OR-of-ANDs to avoid cross-product matches:
///   `(k1 = a1 AND k2 = b1) OR (k1 = a2 AND k2 = b2)`
fn build_delete_filters(
    batches: &[RecordBatch],
    key_columns: &[String],
) -> Result<Vec<datafusion::prelude::Expr>, DataFusionError> {
    use datafusion::prelude::*;

    // Fast path: single key column uses simple IN-list.
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
                let scalar = datafusion::common::ScalarValue::try_from_array(array, row_idx)?;
                values.push(lit(scalar));
            }
        }
        if values.is_empty() {
            return Err(DataFusionError::Internal(
                "Failed to build delete filters: no key values extracted from matched rows"
                    .to_string(),
            ));
        }
        return Ok(vec![col(key_col).in_list(values, false)]);
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
                let array = batch.column(indices[batch_idx]);
                let scalar = datafusion::common::ScalarValue::try_from_array(array, row_idx)?;
                let eq_expr = col(key_col.as_str()).eq(lit(scalar));
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

    // Combine all row predicates with OR using a balanced binary tree.
    // A linear fold creates O(N) depth causing stack overflow for large N.
    // A balanced tree keeps depth at O(log N).
    match util::expr::combine_exprs_balanced(row_predicates, datafusion::prelude::Expr::or) {
        Some(combined) => Ok(vec![combined]),
        None => Err(DataFusionError::Internal(
            "Failed to build delete filters: no row predicates generated".to_string(),
        )),
    }
}
