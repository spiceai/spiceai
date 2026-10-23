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

//! Shared `UpdateExec` execution plan for implementing UPDATE-as-delete+insert.
//!
//! Providers that implement `TableProvider::update` can return this plan from their
//! `update()` method. The plan materializes the updated rows from a source plan,
//! deletes the matching rows, then inserts the updated rows back.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::execution::SessionState;
use datafusion::physical_plan::execute_stream;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, LogicalPlan, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_datasource::memory::MemorySourceConfig;
use futures::future::BoxFuture;

/// Checks the rows an UPDATE will write before it deletes anything; an error fails
/// the UPDATE with nothing changed.
#[async_trait::async_trait]
pub trait UpdateValidator: Send + Sync + std::fmt::Debug {
    /// Check `rows`, the updated rows in the target table's schema.
    async fn validate(
        &self,
        rows: &[RecordBatch],
        context: Arc<TaskContext>,
    ) -> Result<(), DataFusionError>;
}

/// Reads and checks the updated rows of a serialized UPDATE, changing nothing.
pub type UpdateRead = BoxFuture<'static, Result<Vec<RecordBatch>, DataFusionError>>;

/// Deletes the rows a serialized UPDATE matched and inserts its updated rows,
/// returning the single-row batch with the count of affected rows.
pub type UpdateWrite =
    Box<dyn FnOnce(Vec<RecordBatch>) -> BoxFuture<'static, Result<RecordBatch, DataFusionError>> + Send>;

/// Runs an UPDATE with the other writers of its table held off.
///
/// A table whose writers serialize on a lock supplies this through
/// [`UpdateExec::serialized`]. The UPDATE then plans and reads the rows it
/// rewrites inside [`UpdateSerializer::run`], not when the statement was planned:
/// two UPDATEs planned against one version of a row apply one after the other,
/// each to the row the other left, instead of the second deleting the first's
/// result or writing over it.
#[async_trait::async_trait]
pub trait UpdateSerializer: Send + Sync + std::fmt::Debug {
    /// Run `read`, then `write` with the rows it returns, with no other writer of
    /// the table running in between or during either.
    ///
    /// A caller that stops waiting during `read` changes nothing. Once `write`
    /// starts, it runs to completion and other writers stay held off until it
    /// finishes: stopping between its delete and its insert would leave the
    /// matched rows deleted and their updated rows never written.
    async fn run(
        &self,
        read: UpdateRead,
        write: UpdateWrite,
    ) -> Result<RecordBatch, DataFusionError>;
}

/// Where an [`UpdateExec`] reads the rows it rewrites.
enum UpdateSource {
    /// A plan built when the statement was planned.
    Plan(Arc<dyn ExecutionPlan>),
    /// Planned and read inside the table's serializer.
    Serialized {
        plan: LogicalPlan,
        serializer: Arc<dyn UpdateSerializer>,
    },
}

/// An execution plan that implements UPDATE as delete + insert.
///
/// 1. Materializes the updated rows from the source plan.
/// 2. Normalizes output to match the target table schema.
/// 3. Checks them with the table's [`UpdateValidator`], if it has one.
/// 4. Deletes matching rows via `TableProvider::delete_from`.
/// 5. Inserts updated rows via `TableProvider::insert_into`.
/// 6. Returns a single-row batch with the count of affected rows.
///
/// Steps 1-3 are [`UpdateExec::prepare`], which a caller writing several tables in
/// one statement runs for every table before executing any. A
/// [`serialized`](UpdateExec::serialized) UPDATE runs steps 1-5 inside its table's
/// [`UpdateSerializer`].
pub struct UpdateExec {
    source: UpdateSource,
    table_provider: Arc<dyn TableProvider>,
    session_state: SessionState,
    filters: Vec<Expr>,
    validator: Option<Arc<dyn UpdateValidator>>,
    /// The rows [`UpdateExec::prepare`] materialized and checked, written by the
    /// next execution, so it writes exactly the rows that were checked.
    prepared: Arc<tokio::sync::Mutex<Option<Vec<RecordBatch>>>>,
    properties: Arc<PlanProperties>,
}

impl UpdateExec {
    pub fn new(
        source_plan: Arc<dyn ExecutionPlan>,
        table_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        filters: Vec<Expr>,
    ) -> Self {
        Self::with_source(
            UpdateSource::Plan(source_plan),
            table_provider,
            session_state,
            filters,
        )
    }

    /// An UPDATE whose updated rows, `plan`, are planned and read inside
    /// `serializer` when it executes. See [`UpdateSerializer`].
    pub fn serialized(
        plan: LogicalPlan,
        serializer: Arc<dyn UpdateSerializer>,
        table_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        filters: Vec<Expr>,
    ) -> Self {
        Self::with_source(
            UpdateSource::Serialized { plan, serializer },
            table_provider,
            session_state,
            filters,
        )
    }

    fn with_source(
        source: UpdateSource,
        table_provider: Arc<dyn TableProvider>,
        session_state: SessionState,
        filters: Vec<Expr>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            table_provider,
            session_state,
            filters,
            validator: None,
            prepared: Arc::new(tokio::sync::Mutex::new(None)),
            properties,
        }
    }

    /// Check the updated rows with `validator` before deleting anything.
    #[must_use]
    pub fn with_validator(mut self, validator: Arc<dyn UpdateValidator>) -> Self {
        self.validator = Some(validator);
        self
    }

    /// Materialize the updated rows and check them, without changing the table. The
    /// next execution writes these rows, unless the UPDATE is
    /// [`serialized`](Self::serialized): it then reads and checks them again inside
    /// its serializer, since another writer may change them before it runs.
    ///
    /// # Errors
    ///
    /// Returns the error reading the rows, or the validator's.
    pub async fn prepare(&self, context: Arc<TaskContext>) -> Result<(), DataFusionError> {
        match &self.source {
            UpdateSource::Plan(source_plan) => {
                let rows = materialize(
                    source_plan,
                    &self.table_provider,
                    self.validator.as_deref(),
                    context,
                )
                .await?;
                *self.prepared.lock().await = Some(rows);
            }
            UpdateSource::Serialized { plan, .. } => {
                let Some(validator) = self.validator.as_deref() else {
                    return Ok(());
                };
                let source_plan = self.session_state.create_physical_plan(plan).await?;
                materialize(&source_plan, &self.table_provider, Some(validator), context).await?;
            }
        }
        Ok(())
    }
}

/// The one-column schema of the batch an UPDATE returns.
fn count_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("count", arrow::datatypes::DataType::UInt64, false),
    ]))
}

/// The updated rows `source_plan` produces, in the schema of `table_provider`, once
/// `validator` accepts them.
async fn materialize(
    source_plan: &Arc<dyn ExecutionPlan>,
    table_provider: &Arc<dyn TableProvider>,
    validator: Option<&dyn UpdateValidator>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use futures::TryStreamExt;

    let source_stream = execute_stream(Arc::clone(source_plan), Arc::clone(&context))?;
    let updated_batches: Vec<RecordBatch> = source_stream.try_collect().await?;

    // Normalize update output to match the target table schema (including nullability)
    // before performing any destructive operation.
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

/// Delete the rows `filters` match, insert `rows` in their place, and return the
/// single-row batch with the count of affected rows.
async fn apply(
    rows: Vec<RecordBatch>,
    table_provider: &Arc<dyn TableProvider>,
    session_state: &SessionState,
    filters: Vec<Expr>,
    context: Arc<TaskContext>,
) -> Result<RecordBatch, DataFusionError> {
    use futures::TryStreamExt;

    let target_schema = table_provider.schema();

    let delete_plan = table_provider.delete_from(session_state, filters).await?;
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
        let insert_plan = table_provider
            .insert_into(session_state, input_exec, InsertOp::Append)
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
        match &self.source {
            UpdateSource::Plan(source_plan) => vec![source_plan],
            // Planned when it executes, inside the serializer.
            UpdateSource::Serialized { .. } => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let source = match &self.source {
            UpdateSource::Plan(_) => {
                let [child] = <[_; 1]>::try_from(children).map_err(|children| {
                    DataFusionError::Internal(format!(
                        "UpdateExec requires exactly one child, got {}",
                        children.len()
                    ))
                })?;
                UpdateSource::Plan(child)
            }
            UpdateSource::Serialized { .. } if children.is_empty() => return Ok(self),
            UpdateSource::Serialized { .. } => {
                return Err(DataFusionError::Internal(format!(
                    "A serialized UpdateExec takes no children, got {}",
                    children.len()
                )));
            }
        };

        let mut exec = Self::with_source(
            source,
            Arc::clone(&self.table_provider),
            self.session_state.clone(),
            self.filters.clone(),
        );
        exec.validator.clone_from(&self.validator);
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "UpdateExec only supports partition 0, got {partition}"
            )));
        }

        let table_provider = Arc::clone(&self.table_provider);
        let session_state = self.session_state.clone();
        let filters = self.filters.clone();
        let validator = self.validator.clone();

        match &self.source {
            UpdateSource::Plan(source_plan) => {
                let source_plan = Arc::clone(source_plan);
                let prepared = Arc::clone(&self.prepared);
                let stream = futures::stream::once(async move {
                    let prepared = prepared.lock().await.take();
                    let rows = match prepared {
                        Some(rows) => rows,
                        None => {
                            materialize(
                                &source_plan,
                                &table_provider,
                                validator.as_deref(),
                                Arc::clone(&context),
                            )
                            .await?
                        }
                    };
                    apply(rows, &table_provider, &session_state, filters, context).await
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    count_schema(),
                    stream,
                )))
            }
            UpdateSource::Serialized { plan, serializer } => {
                let read: UpdateRead = {
                    let plan = plan.clone();
                    let table_provider = Arc::clone(&table_provider);
                    let session_state = session_state.clone();
                    let context = Arc::clone(&context);
                    Box::pin(async move {
                        let source_plan = session_state.create_physical_plan(&plan).await?;
                        materialize(&source_plan, &table_provider, validator.as_deref(), context)
                            .await
                    })
                };
                let write: UpdateWrite = Box::new(move |rows| {
                    Box::pin(async move {
                        apply(rows, &table_provider, &session_state, filters, context).await
                    })
                });
                let serializer = Arc::clone(serializer);
                let stream =
                    futures::stream::once(async move { serializer.run(read, write).await });
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    count_schema(),
                    stream,
                )))
            }
        }
    }
}
