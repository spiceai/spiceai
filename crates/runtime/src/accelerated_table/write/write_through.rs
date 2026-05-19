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

//! Write-through execution path for [`WriteMode::WriteThrough`].
//!
//! Writes are applied simultaneously to the Cayenne accelerator (via staged
//! append) and the federated source. On success both sides commit; on
//! failure the accelerator stage is rolled back and the error is surfaced
//! synchronously. Supports both non-partitioned and partitioned Cayenne
//! accelerators.
//!
//! [`WriteMode::WriteThrough`]: super::WriteMode::WriteThrough

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::{CayenneStagedAppend, CayenneTableProvider};
use data_components::delete::{DeletionExec, DeletionSink};
use data_components::poly::PolyTableProvider;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::accelerated_table::refresh;
use crate::dataaccelerator::cayenne::CayennePartitionCreator;
use crate::dataaccelerator::upsert_dedup::UpsertDedupTableProvider;
use crate::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::insert::partition_batch_composite;
use runtime_table_partition::provider::PartitionTableProvider;

/// Target for Cayenne-based write-through operations.
#[derive(Debug)]
pub(crate) enum CayenneWriteTarget {
    Staged(Box<CayenneTableProvider>),
    Partitioned(Arc<dyn TableProvider>),
}

impl Clone for CayenneWriteTarget {
    fn clone(&self) -> Self {
        match self {
            Self::Staged(provider) => Self::Staged(Box::new(provider.clone_for_write_operations())),
            Self::Partitioned(provider) => Self::Partitioned(Arc::clone(provider)),
        }
    }
}

/// Creates a `DataSinkExec` plan for write-through inserts.
///
/// Called from `AcceleratedTable::insert_into` when the write mode is `WriteThrough`.
pub(crate) fn insert_write_through(
    input: Arc<dyn ExecutionPlan>,
    overwrite: InsertOp,
    cayenne_target: &CayenneWriteTarget,
    federated_provider: Arc<dyn TableProvider>,
    refresher: &Arc<refresh::Refresher>,
    schema: SchemaRef,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    match overwrite {
        InsertOp::Append => Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(WriteThroughDataSink::new(
                cayenne_target.clone(),
                federated_provider,
                Arc::clone(refresher),
                schema,
            )),
            None,
        ))),
        InsertOp::Overwrite | InsertOp::Replace => Err(DataFusionError::Plan(
            "Write-through accelerated catalog tables currently support append writes only"
                .to_string(),
        )),
    }
}

struct WriteThroughDataSink {
    accelerator: CayenneWriteTarget,
    federated: Arc<dyn TableProvider>,
    refresher: Arc<refresh::Refresher>,
    schema: SchemaRef,
}

impl WriteThroughDataSink {
    fn new(
        accelerator: CayenneWriteTarget,
        federated: Arc<dyn TableProvider>,
        refresher: Arc<refresh::Refresher>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            accelerator,
            federated,
            refresher,
            schema,
        }
    }
}

impl std::fmt::Debug for WriteThroughDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteThroughDataSink")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for WriteThroughDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteThroughDataSink")
    }
}

#[async_trait]
impl DataSink for WriteThroughDataSink {
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
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let target_partitions = context.session_config().target_partitions();

        if let CayenneWriteTarget::Partitioned(accelerator) = &self.accelerator {
            return write_all_with_partitioned_cayenne(
                Arc::clone(&self.refresher),
                Arc::clone(accelerator),
                Arc::clone(&self.federated),
                data,
                target_partitions,
            )
            .await;
        }

        let schema = data.schema();
        let (source_tx, source_rx) = mpsc::channel(8);
        let (accelerator_tx, accelerator_rx) = mpsc::channel(8);

        let CayenneWriteTarget::Staged(accelerator) = &self.accelerator else {
            unreachable!("partitioned Cayenne path is handled before staged writes")
        };

        let source_task =
            spawn_federated_insert(Arc::clone(&self.federated), Arc::clone(&schema), source_rx);
        let staged_task = spawn_staged_append(
            accelerator.clone_for_write_operations(),
            Arc::clone(&schema),
            accelerator_rx,
            target_partitions,
        );

        let mut upstream_error: Option<DataFusionError> = None;

        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    if source_tx.send(Ok(batch.clone())).await.is_err()
                        || accelerator_tx.send(Ok(batch)).await.is_err()
                    {
                        upstream_error = Some(DataFusionError::Execution(
                            "Write-through insert stream terminated before both write paths completed"
                                .to_string(),
                        ));
                        break;
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    let _ = source_tx
                        .send(Err(DataFusionError::Execution(message.clone())))
                        .await;
                    let _ = accelerator_tx
                        .send(Err(DataFusionError::Execution(message.clone())))
                        .await;
                    upstream_error = Some(DataFusionError::Execution(message));
                    break;
                }
            }
        }

        drop(source_tx);
        drop(accelerator_tx);

        let staged_result = join_staged_task(staged_task).await;
        let source_result = join_source_task(source_task).await;

        match (staged_result, source_result, upstream_error) {
            (Ok(staged), Ok(()), None) => {
                let row_count = staged.commit().await?;
                self.refresher.set_initial_load_completed(true);
                Ok(row_count)
            }
            (Ok(staged), source_result, upstream_error) => {
                if let Err(error) = staged.rollback().await {
                    tracing::error!("Failed to roll back staged Cayenne write: {error}");
                }

                if let Some(error) = upstream_error {
                    return Err(error);
                }

                match source_result {
                    Ok(()) => Err(DataFusionError::Execution(
                        "Cayenne staged write failed before commit".to_string(),
                    )),
                    Err(error) => Err(error),
                }
            }
            (Err(staged_error), Ok(()), upstream_error) => Err(upstream_error.unwrap_or(staged_error)),
            (Err(staged_error), Err(source_error), upstream_error) => {
                Err(upstream_error.unwrap_or_else(|| DataFusionError::Execution(format!(
                    "Write-through insert failed for both accelerator and federated source: accelerator={staged_error}; source={source_error}"
                ))))
            }
        }
    }
}

async fn write_all_with_partitioned_cayenne(
    refresher: Arc<refresh::Refresher>,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    mut data: SendableRecordBatchStream,
    target_partitions: usize,
) -> datafusion::common::Result<u64> {
    let partitioned = accelerator
        .as_any()
        .downcast_ref::<PartitionTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Write-through partitioned Cayenne path requires a PartitionTableProvider"
                    .to_string(),
            )
        })?;

    let schema = data.schema();
    let physical_exprs = create_partition_physical_exprs(partitioned, Arc::clone(&schema))?;
    let (source_tx, source_rx) = mpsc::channel(8);
    let source_task =
        spawn_federated_insert(Arc::clone(&federated), Arc::clone(&schema), source_rx);

    let mut upstream_error: Option<DataFusionError> = None;
    let mut partition_senders =
        HashMap::<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>>::new();
    let mut partition_handles = Vec::new();

    while let Some(batch_result) = data.next().await {
        match batch_result {
            Ok(batch) => {
                let partitioned_batches = partition_batch_composite(&batch, &physical_exprs)?;
                for (partition_key, (partition_values, partition_batch)) in partitioned_batches {
                    let sender = if let Some(sender) = partition_senders.get(&partition_key) {
                        sender.clone()
                    } else {
                        let partition_provider = partitioned
                            .get_or_create_partition_provider(partition_values)
                            .await?;
                        let cayenne = downcast_to_cayenne(&partition_provider)
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "Write-through partitioned Cayenne path requires Cayenne-backed partition providers"
                                        .to_string(),
                                )
                            })?;

                        let (partition_tx, partition_rx) = mpsc::channel(8);
                        partition_senders.insert(partition_key, partition_tx.clone());
                        partition_handles.push(spawn_staged_append(
                            cayenne.clone_for_write_operations(),
                            Arc::clone(&schema),
                            partition_rx,
                            target_partitions,
                        ));
                        partition_tx
                    };

                    if sender.send(Ok(partition_batch)).await.is_err() {
                        upstream_error = Some(DataFusionError::Execution(
                            "Write-through partitioned accelerator stream terminated before staging completed"
                                .to_string(),
                        ));
                        break;
                    }
                }

                if upstream_error.is_some() {
                    let _ = source_tx
                        .send(Err(DataFusionError::Execution(
                            "Write-through partitioned accelerator stream terminated before staging completed"
                                .to_string(),
                        )))
                        .await;
                    break;
                }

                if source_tx.send(Ok(batch)).await.is_err() {
                    upstream_error = Some(DataFusionError::Execution(
                        "Write-through insert stream terminated before the federated write completed"
                            .to_string(),
                    ));
                    break;
                }
            }
            Err(error) => {
                let message = error.to_string();
                let _ = source_tx
                    .send(Err(DataFusionError::Execution(message.clone())))
                    .await;
                for sender in partition_senders.values() {
                    let _ = sender
                        .send(Err(DataFusionError::Execution(message.clone())))
                        .await;
                }
                upstream_error = Some(DataFusionError::Execution(message));
                break;
            }
        }
    }

    drop(source_tx);
    drop(partition_senders);

    let staged_result = join_partitioned_staged_tasks(partition_handles).await;
    let source_result = join_source_task(source_task).await;

    match (staged_result, source_result, upstream_error) {
        (Ok(staged), Ok(()), None) => {
            let row_count = staged.commit().await?;
            refresher.set_initial_load_completed(true);
            Ok(row_count)
        }
        (Ok(staged), source_result, upstream_error) => {
            if let Err(error) = staged.rollback().await {
                tracing::error!("Failed to roll back staged partitioned Cayenne write: {error}");
            }

            if let Some(error) = upstream_error {
                return Err(error);
            }

            match source_result {
                Ok(()) => Err(DataFusionError::Execution(
                    "Partitioned Cayenne staged write failed before commit".to_string(),
                )),
                Err(error) => Err(error),
            }
        }
        (Err(staged_error), Ok(()), upstream_error) => Err(upstream_error.unwrap_or(staged_error)),
        (Err(staged_error), Err(source_error), upstream_error) => {
            Err(upstream_error.unwrap_or_else(|| DataFusionError::Execution(format!(
                "Write-through insert failed for both partitioned accelerator and federated source: accelerator={staged_error}; source={source_error}"
            ))))
        }
    }
}

/// Attempts to downcast a partition provider to [`CayenneTableProvider`].
fn downcast_to_cayenne(provider: &Arc<dyn TableProvider>) -> Option<&CayenneTableProvider> {
    provider.as_any().downcast_ref::<CayenneTableProvider>()
}

fn spawn_federated_insert(
    federated: Arc<dyn TableProvider>,
    schema: SchemaRef,
    receiver: mpsc::Receiver<datafusion::common::Result<arrow::record_batch::RecordBatch>>,
) -> JoinHandle<datafusion::common::Result<()>> {
    tokio::spawn(async move {
        let ctx = SessionContext::new();
        let stream = RecordBatchStreamAdapter::new(schema, ReceiverStream::new(receiver));
        let input: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(stream))),
            federated.schema(),
        ));

        let insert_plan = federated
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await?;
        let _ = datafusion::physical_plan::collect(insert_plan, ctx.task_ctx()).await?;
        Ok(())
    })
}

struct PartitionedCayenneStagedAppend {
    staged_appends: Vec<CayenneStagedAppend>,
    row_count: u64,
}

impl std::fmt::Debug for PartitionedCayenneStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedCayenneStagedAppend")
            .field("staged_appends", &self.staged_appends.len())
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl PartitionedCayenneStagedAppend {
    async fn commit(self) -> datafusion::common::Result<u64> {
        for staged_append in self.staged_appends {
            staged_append
                .commit()
                .await
                .map_err(DataFusionError::from)?;
        }
        Ok(self.row_count)
    }

    async fn rollback(self) -> datafusion::common::Result<()> {
        for staged_append in self.staged_appends {
            staged_append
                .rollback()
                .await
                .map_err(DataFusionError::from)?;
        }
        Ok(())
    }
}

fn create_partition_physical_exprs(
    partitioned: &PartitionTableProvider,
    schema: SchemaRef,
) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    let input_dfschema = DFSchema::try_from(schema)?;
    let execution_props = ExecutionProps::new();

    partitioned
        .partition_by()
        .iter()
        .map(|partitioned_by| {
            datafusion::physical_expr::create_physical_expr(
                &partitioned_by.expression,
                &input_dfschema,
                &execution_props,
            )
        })
        .collect()
}

async fn join_partitioned_staged_tasks(
    handles: Vec<JoinHandle<datafusion::common::Result<CayenneStagedAppend>>>,
) -> datafusion::common::Result<PartitionedCayenneStagedAppend> {
    let mut staged_appends = Vec::with_capacity(handles.len());
    let mut row_count = 0_u64;

    for handle in handles {
        let staged_append = join_staged_task(handle).await?;
        row_count += staged_append.row_count();
        staged_appends.push(staged_append);
    }

    Ok(PartitionedCayenneStagedAppend {
        staged_appends,
        row_count,
    })
}

pub(crate) fn extract_cayenne_write_target(
    table_provider: &Arc<dyn TableProvider>,
) -> Option<CayenneWriteTarget> {
    if let Some(cayenne) = table_provider
        .as_any()
        .downcast_ref::<CayenneTableProvider>()
    {
        return Some(CayenneWriteTarget::Staged(Box::new(
            cayenne.clone_for_write_operations(),
        )));
    }

    if let Some(partitioned) = table_provider
        .as_any()
        .downcast_ref::<PartitionTableProvider>()
        && partitioned
            .creator()
            .as_any()
            .downcast_ref::<CayennePartitionCreator>()
            .is_some()
    {
        return Some(CayenneWriteTarget::Partitioned(Arc::clone(table_provider)));
    }

    if let Some(poly) = table_provider.as_any().downcast_ref::<PolyTableProvider>() {
        let writer = poly.writer();
        return extract_cayenne_write_target(&writer);
    }

    if let Some(upsert_dedup) = table_provider
        .as_any()
        .downcast_ref::<UpsertDedupTableProvider>()
    {
        return extract_cayenne_write_target(upsert_dedup.inner());
    }

    None
}

fn spawn_staged_append(
    accelerator: CayenneTableProvider,
    schema: SchemaRef,
    receiver: mpsc::Receiver<datafusion::common::Result<arrow::record_batch::RecordBatch>>,
    target_partitions: usize,
) -> JoinHandle<datafusion::common::Result<CayenneStagedAppend>> {
    tokio::spawn(async move {
        let stream = RecordBatchStreamAdapter::new(schema, ReceiverStream::new(receiver));
        accelerator
            .begin_staged_append(Box::pin(stream), target_partitions)
            .await
            .map_err(Into::into)
    })
}

async fn join_source_task(
    handle: JoinHandle<datafusion::common::Result<()>>,
) -> datafusion::common::Result<()> {
    match handle.await {
        Ok(result) => result,
        Err(error) => Err(DataFusionError::Execution(format!(
            "Federated write-through task failed: {error}"
        ))),
    }
}

async fn join_staged_task(
    handle: JoinHandle<datafusion::common::Result<CayenneStagedAppend>>,
) -> datafusion::common::Result<CayenneStagedAppend> {
    match handle.await {
        Ok(result) => result,
        Err(error) => Err(DataFusionError::Execution(format!(
            "Accelerator staged write task failed: {error}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Write-through delete and update
// ---------------------------------------------------------------------------

/// Creates a `DeletionExec` plan for write-through deletes.
///
/// Federated delete runs first; if it succeeds the accelerator delete follows.
/// Both must succeed — if the accelerator delete fails the error is surfaced so
/// the caller knows the operation did not fully complete (the next refresh cycle
/// will reconcile, but the caller should be aware).
pub(crate) async fn delete_write_through(
    state: &dyn Session,
    filters: Vec<Expr>,
    cayenne_target: &CayenneWriteTarget,
    federated_provider: Arc<dyn TableProvider>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let accelerator = cayenne_target_as_provider(cayenne_target);
    let federated_plan = federated_provider
        .delete_from(state, filters.clone())
        .await?;
    let accelerator_plan = accelerator.delete_from(state, filters).await?;
    let session_state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Session is not a SessionState in delete_write_through".to_string(),
            )
        })?
        .clone();
    Ok(Arc::new(DeletionExec::new(Arc::new(
        WriteThroughDeletionSink {
            federated_plan,
            accelerator_plan,
            session_state,
        },
    ))))
}

struct WriteThroughDeletionSink {
    federated_plan: Arc<dyn ExecutionPlan>,
    accelerator_plan: Arc<dyn ExecutionPlan>,
    session_state: SessionState,
}

#[async_trait]
impl DeletionSink for WriteThroughDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let task_ctx = self.session_state.task_ctx();

        let federated_batches = datafusion::physical_plan::collect(
            Arc::clone(&self.federated_plan),
            Arc::clone(&task_ctx),
        )
        .await?;
        let count = super::write_back::extract_dml_count(&federated_batches);

        datafusion::physical_plan::collect(Arc::clone(&self.accelerator_plan), task_ctx).await?;

        Ok(count)
    }
}

/// Creates a `DeletionExec` plan for write-through updates.
///
/// Federated update runs first; if it succeeds the accelerator update follows.
pub(crate) async fn update_write_through(
    state: &dyn Session,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    cayenne_target: &CayenneWriteTarget,
    federated_provider: Arc<dyn TableProvider>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let accelerator = cayenne_target_as_provider(cayenne_target);
    let federated_plan = federated_provider
        .update(state, assignments.clone(), filters.clone())
        .await?;
    let accelerator_plan = accelerator.update(state, assignments, filters).await?;
    let session_state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Session is not a SessionState in update_write_through".to_string(),
            )
        })?
        .clone();
    Ok(Arc::new(DeletionExec::new(Arc::new(
        WriteThroughUpdateSink {
            federated_plan,
            accelerator_plan,
            session_state,
        },
    ))))
}

struct WriteThroughUpdateSink {
    federated_plan: Arc<dyn ExecutionPlan>,
    accelerator_plan: Arc<dyn ExecutionPlan>,
    session_state: SessionState,
}

#[async_trait]
impl DeletionSink for WriteThroughUpdateSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let task_ctx = self.session_state.task_ctx();

        let federated_batches = datafusion::physical_plan::collect(
            Arc::clone(&self.federated_plan),
            Arc::clone(&task_ctx),
        )
        .await?;
        let count = super::write_back::extract_dml_count(&federated_batches);

        datafusion::physical_plan::collect(Arc::clone(&self.accelerator_plan), task_ctx).await?;

        Ok(count)
    }
}

fn cayenne_target_as_provider(target: &CayenneWriteTarget) -> Arc<dyn TableProvider> {
    match target {
        CayenneWriteTarget::Staged(p) => Arc::new(p.clone_for_write_operations()),
        CayenneWriteTarget::Partitioned(p) => Arc::clone(p),
    }
}

#[cfg(test)]
mod tests {
    use super::{WriteThroughDeletionSink, WriteThroughUpdateSink};
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::delete::DeletionSink;
    use datafusion::error::{DataFusionError, Result as DataFusionResult};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    };
    use datafusion::prelude::SessionContext;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use std::any::Any;
    use std::sync::Arc;

    fn count_exec(n: u64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![n]))],
        )
        .expect("valid schema and array");
        let memory =
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("valid memory source");
        Arc::new(DataSourceExec::new(Arc::new(memory)))
    }

    struct ErrorExec {
        properties: PlanProperties,
        message: String,
    }

    impl ErrorExec {
        fn new_arc(message: impl Into<String>) -> Arc<dyn ExecutionPlan> {
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
            Arc::new(Self {
                properties,
                message: message.into(),
            })
        }
    }

    impl std::fmt::Debug for ErrorExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ErrorExec({})", self.message)
        }
    }

    impl DisplayAs for ErrorExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "ErrorExec")
        }
    }

    impl ExecutionPlan for ErrorExec {
        fn name(&self) -> &'static str {
            "ErrorExec"
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
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            Err(DataFusionError::Execution(self.message.clone()))
        }
    }

    // ── WriteThroughDeletionSink ─────────────────────────────────────────

    #[tokio::test]
    async fn write_through_deletion_count_comes_from_federated() {
        let sink = WriteThroughDeletionSink {
            federated_plan: count_exec(5),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let count = sink.delete_from().await.expect("deletion should succeed");
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn write_through_deletion_federated_error_propagates() {
        let sink = WriteThroughDeletionSink {
            federated_plan: ErrorExec::new_arc("federated delete failed"),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let err = sink.delete_from().await.expect_err("deletion should fail");
        assert!(err.to_string().contains("federated delete failed"));
    }

    #[tokio::test]
    async fn write_through_deletion_accelerator_error_propagates() {
        let sink = WriteThroughDeletionSink {
            federated_plan: count_exec(5),
            accelerator_plan: ErrorExec::new_arc("accelerator delete failed"),
            session_state: SessionContext::new().state(),
        };

        let err = sink.delete_from().await.expect_err("deletion should fail");
        assert!(err.to_string().contains("accelerator delete failed"));
    }

    // ── WriteThroughUpdateSink ───────────────────────────────────────────

    #[tokio::test]
    async fn write_through_update_count_comes_from_federated() {
        let sink = WriteThroughUpdateSink {
            federated_plan: count_exec(3),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let count = sink.delete_from().await.expect("update should succeed");
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn write_through_update_federated_error_propagates() {
        let sink = WriteThroughUpdateSink {
            federated_plan: ErrorExec::new_arc("federated update failed"),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let err = sink.delete_from().await.expect_err("update should fail");
        assert!(err.to_string().contains("federated update failed"));
    }

    #[tokio::test]
    async fn write_through_update_accelerator_error_propagates() {
        let sink = WriteThroughUpdateSink {
            federated_plan: count_exec(3),
            accelerator_plan: ErrorExec::new_arc("accelerator update failed"),
            session_state: SessionContext::new().state(),
        };

        let err = sink.delete_from().await.expect_err("update should fail");
        assert!(err.to_string().contains("accelerator update failed"));
    }
}
