/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Autocommit `UPDATE` that rematerializes inside `write_lock`.
//!
//! Shared [`data_components::update::UpdateExec`] freezes the source scan at
//! `TableProvider::update` and only takes `write_lock` for the later delete and
//! insert. Two plans built against the same version then execute as: the first
//! writes a new version; the second materializes zero rows (live tombstones hide
//! the captured version, the replacement is not in the frozen listing),
//! tombstones the live key, and inserts nothing.
//!
//! This plan keeps the logical read, rebuilds the physical scan after it holds
//! `write_lock`, and runs delete + insert before releasing it — the #13828
//! pattern applied to `UPDATE`.
//!
//! A partitioned sink calls [`PreparableUpdate::prepare`] on every partition
//! before executing any, so a primary-key collision in a later partition
//! refuses the statement with nothing written. `prepare` only validates.
//! `execute` always rematerializes under `write_lock` — writing prepare-time
//! rows would replay a stale capture (two same-key `value = value + 1` plans
//! that both prepared against V1 would both write V2). Snapshot id is not a
//! freshness signal here: an inline write does not rotate it.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use data_components::update::{PreparableUpdate, UpdateValidator};
use datafusion::execution::SessionState;
use datafusion::physical_plan::execute_stream;
use datafusion_catalog::TableProvider;
use datafusion_common::DataFusionError;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::TryStreamExt;

use super::table::CayenneTableProvider;
use super::write_lock_held;

/// Autocommit `UPDATE` for a Cayenne table. See the module docs.
pub(crate) struct CayenneUpdateExec {
    logical_plan: LogicalPlan,
    table: CayenneTableProvider,
    session_state: SessionState,
    filters: Vec<Expr>,
    validator: Option<Arc<dyn UpdateValidator>>,
    properties: Arc<PlanProperties>,
}

impl CayenneUpdateExec {
    pub(crate) fn new(
        logical_plan: LogicalPlan,
        table: CayenneTableProvider,
        session_state: SessionState,
        filters: Vec<Expr>,
        validator: Option<Arc<dyn UpdateValidator>>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            logical_plan,
            table,
            session_state,
            filters,
            validator,
            properties,
        }
    }
}

#[async_trait]
impl PreparableUpdate for CayenneUpdateExec {
    async fn prepare(&self, context: Arc<TaskContext>) -> Result<(), DataFusionError> {
        // Validate only. The next execution rematerializes under `write_lock`
        // so a partitioned prepare-then-apply does not write a stale capture.
        materialize_from_logical(
            &self.logical_plan,
            &self.session_state,
            &self.table,
            self.validator.as_deref(),
            context,
        )
        .await?;
        Ok(())
    }
}

impl std::fmt::Debug for CayenneUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneUpdateExec").finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneUpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayenneUpdateExec")
    }
}

impl ExecutionPlan for CayenneUpdateExec {
    fn name(&self) -> &'static str {
        "CayenneUpdateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            return Ok(self);
        }
        Err(DataFusionError::Internal(
            "CayenneUpdateExec does not take plan children; it rebuilds its scan at execution"
                .to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "CayenneUpdateExec only supports partition 0, got {partition}"
            )));
        }

        let logical_plan = self.logical_plan.clone();
        let table = self.table.clone_for_write();
        let session_state = self.session_state.clone();
        let filters = self.filters.clone();
        let validator = self.validator.clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let stream = futures::stream::once(async move {
            let write_guard = Arc::new(table.write_lock_arc().lock_owned().await);
            table
                .ensure_publication_outcome_known()
                .map_err(DataFusionError::from)?;
            write_lock_held::scope(
                write_guard,
                apply_update(
                    logical_plan,
                    table.clone_for_write(),
                    session_state,
                    filters,
                    validator,
                    context,
                ),
            )
            .await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn apply_update(
    logical_plan: LogicalPlan,
    table: CayenneTableProvider,
    session_state: SessionState,
    filters: Vec<Expr>,
    validator: Option<Arc<dyn UpdateValidator>>,
    context: Arc<TaskContext>,
) -> Result<RecordBatch, DataFusionError> {
    // Always rematerialize under the already-held `write_lock`. Prepare-time
    // rows are a stale capture: an inline write does not rotate `snapshot_id`,
    // so a "rematerialize if the snapshot changed" check would miss it.
    let rows = materialize_from_logical(
        &logical_plan,
        &session_state,
        &table,
        validator.as_deref(),
        Arc::clone(&context),
    )
    .await?;
    let provider: Arc<dyn TableProvider> = Arc::new(table.clone_for_write());
    let target_schema = provider.schema();

    let delete_plan = provider.delete_from(&session_state, filters).await?;
    let delete_stream = execute_stream(delete_plan, Arc::clone(&context))?;
    let delete_batches: Vec<RecordBatch> = delete_stream.try_collect().await?;
    let deleted_count = delete_batches
        .iter()
        .flat_map(RecordBatch::columns)
        .find_map(|arr| {
            arr.as_any()
                .downcast_ref::<UInt64Array>()
                .and_then(|counts| counts.values().first().copied())
        })
        .unwrap_or(0);

    if !rows.is_empty() {
        let input_exec =
            MemorySourceConfig::try_new_exec(&[rows], Arc::clone(&target_schema), None)?;
        let insert_plan = provider
            .insert_into(&session_state, input_exec, InsertOp::Append)
            .await?;
        let insert_stream = execute_stream(insert_plan, context)?;
        let _insert_batches: Vec<RecordBatch> = insert_stream.try_collect().await?;
    }

    RecordBatch::try_from_iter_with_nullable(vec![(
        "count",
        Arc::new(UInt64Array::from(vec![deleted_count])) as ArrayRef,
        false,
    )])
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

async fn materialize_from_logical(
    logical_plan: &LogicalPlan,
    session_state: &SessionState,
    table: &CayenneTableProvider,
    validator: Option<&dyn UpdateValidator>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let source_plan = session_state.create_physical_plan(logical_plan).await?;
    let source_plan: Arc<dyn ExecutionPlan> = if source_plan
        .properties()
        .output_partitioning()
        .partition_count()
        > 1
    {
        Arc::new(CoalescePartitionsExec::new(source_plan))
    } else {
        source_plan
    };
    let provider: Arc<dyn TableProvider> = Arc::new(table.clone_for_write());
    materialize(&source_plan, &provider, validator, context).await
}

async fn materialize(
    source_plan: &Arc<dyn ExecutionPlan>,
    table_provider: &Arc<dyn TableProvider>,
    validator: Option<&dyn UpdateValidator>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let source_stream = execute_stream(Arc::clone(source_plan), Arc::clone(&context))?;
    let updated_batches: Vec<RecordBatch> = source_stream.try_collect().await?;
    let target_schema = table_provider.schema();
    let rows = updated_batches
        .into_iter()
        .map(|batch| arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(DataFusionError::from)?;
    if let Some(validator) = validator {
        validator.validate(&rows, context).await?;
    }
    Ok(rows)
}
