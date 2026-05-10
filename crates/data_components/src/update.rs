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

//! Update execution plans.
//!
//! `UpdateSink` / `UpdateExec` — generic filter-driven update (mirrors `DeletionSink` /
//! `DeletionExec`): the sink owns all update logic and returns a row count.
//!
//! `LocalUpdateExec` — implements UPDATE-as-delete+insert for providers that use a
//! source scan to produce the updated rows, then call `TableProvider::delete_from` +
//! `insert_into`.

use std::{any::Any, error::Error, sync::Arc};

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::execute_stream;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_datasource::memory::MemorySourceConfig;

// ── Generic UpdateSink / UpdateExec ─────────────────────────────────────────

/// A sink that owns all logic for an UPDATE operation and returns the affected row count.
///
/// Mirrors [`data_components::delete::DeletionSink`]: the sink is filter-driven and
/// requires no input record-batch stream.
#[async_trait]
pub trait UpdateSink: Send + Sync {
    async fn update_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>>;
}

/// An [`ExecutionPlan`] that drives an [`UpdateSink`] and returns a count batch.
///
/// Use this wherever a filter-driven UPDATE needs to be represented as a plan node
/// (e.g. WAL-backed accelerated tables). For UPDATE-as-delete+insert use
/// [`LocalUpdateExec`] instead.
pub struct UpdateExec {
    update_sink: Arc<dyn UpdateSink + 'static>,
    properties: PlanProperties,
}

impl UpdateExec {
    pub fn new(update_sink: Arc<dyn UpdateSink>) -> Self {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            update_sink,
            properties,
        }
    }
}

impl std::fmt::Debug for UpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateExec").finish_non_exhaustive()
    }
}

impl DisplayAs for UpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UpdateExec")
    }
}

impl ExecutionPlan for UpdateExec {
    fn name(&self) -> &'static str {
        "UpdateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let update_sink = Arc::clone(&self.update_sink);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            futures::stream::once(async move {
                let count = update_sink
                    .update_from()
                    .await
                    .map_err(DataFusionError::from)?;
                let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
                RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)])
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }),
        )))
    }
}

// ── LocalUpdateExec ──────────────────────────────────────────────────────────

/// An execution plan that implements UPDATE as delete + insert.
///
/// 1. Materializes the updated rows from the source plan.
/// 2. Normalizes output to match the target table schema.
/// 3. Deletes matching rows via `TableProvider::delete_from`.
/// 4. Inserts updated rows via `TableProvider::insert_into`.
/// 5. Returns a single-row batch with the count of affected rows.
pub struct LocalUpdateExec {
    source_plan: Arc<dyn ExecutionPlan>,
    table_provider: Arc<dyn TableProvider>,
    session_state: SessionState,
    filters: Vec<Expr>,
    properties: PlanProperties,
}

impl LocalUpdateExec {
    pub fn new(
        source_plan: Arc<dyn ExecutionPlan>,
        table_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        filters: Vec<Expr>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            source_plan,
            table_provider,
            session_state,
            filters,
            properties,
        }
    }
}

impl std::fmt::Debug for LocalUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalUpdateExec").finish_non_exhaustive()
    }
}

impl DisplayAs for LocalUpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalUpdateExec")
    }
}

impl ExecutionPlan for LocalUpdateExec {
    fn name(&self) -> &'static str {
        "LocalUpdateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.source_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LocalUpdateExec requires exactly one child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.table_provider),
            self.session_state.clone(),
            self.filters.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LocalUpdateExec only supports partition 0, got {partition}"
            )));
        }

        let source_plan = Arc::clone(&self.source_plan);
        let table_provider = Arc::clone(&self.table_provider);
        let session_state = self.session_state.clone();
        let filters = self.filters.clone();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let stream = futures::stream::once(async move {
            use futures::TryStreamExt;

            let source_stream = execute_stream(Arc::clone(&source_plan), Arc::clone(&context))?;
            let updated_batches: Vec<RecordBatch> = source_stream.try_collect().await?;

            // Normalize update output to match the target table schema (including nullability)
            // before performing any destructive operation.
            let target_schema = table_provider.schema();
            let normalized_batches = updated_batches
                .into_iter()
                .map(|batch| {
                    arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataFusionError::from)?;

            let delete_plan = table_provider.delete_from(&session_state, filters).await?;
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

            if !normalized_batches.is_empty() {
                let input_exec = MemorySourceConfig::try_new_exec(
                    &[normalized_batches],
                    Arc::clone(&target_schema),
                    None,
                )?;
                let insert_plan = table_provider
                    .insert_into(&session_state, input_exec, InsertOp::Append)
                    .await?;
                let insert_stream = execute_stream(insert_plan, Arc::clone(&context))?;
                let _insert_batches: Vec<RecordBatch> = insert_stream.try_collect().await?;
            }

            let result = RecordBatch::try_from_iter_with_nullable(vec![(
                "count",
                Arc::new(UInt64Array::from(vec![deleted_count])) as ArrayRef,
                false,
            )])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

            Ok(result)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
