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

//! Dual-write execution path for [`WriteMode::DualWrite`].
//!
//! Writes are applied simultaneously to the Cayenne accelerator (via staged
//! append) and the federated source. On success both sides commit; on
//! failure the accelerator stage is rolled back and the error is surfaced
//! synchronously. Supports both non-partitioned and partitioned Cayenne
//! accelerators.
//!
//! This path is reserved for the Iceberg federated catalog cache use case
//! where Cayenne acts as a write-through cache in front of an Iceberg catalog
//! that has no CDC stream to propagate writes. It is *not* exposed through the
//! spicepod `write_mode: write_through` setting — that maps to
//! [`WriteMode::WriteThrough`] (source-sync, accelerator via refresh).
//!
//! [`WriteMode::DualWrite`]: super::WriteMode::DualWrite
//! [`WriteMode::WriteThrough`]: super::WriteMode::WriteThrough

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

use crate::accelerated::refresh;
use data_accelerator_api::upsert_dedup::UpsertDedupTableProvider;
use runtime_acceleration::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::insert::partition_batch_composite;
use runtime_table_partition::provider::PartitionTableProvider;

/// Target for Cayenne-based dual-write operations.
#[derive(Debug)]
pub enum CayenneWriteTarget {
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

/// Creates a `DataSinkExec` plan for dual-write inserts.
///
/// Called from `AcceleratedTable::insert_into` when the write mode is `DualWrite`.
pub(crate) fn insert_dual_write(
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
            Arc::new(DualWriteDataSink::new(
                cayenne_target.clone(),
                federated_provider,
                Arc::clone(refresher),
                schema,
            )),
            None,
        ))),
        InsertOp::Overwrite | InsertOp::Replace => Err(DataFusionError::Plan(
            "Dual-write accelerated catalog tables currently support append writes only"
                .to_string(),
        )),
    }
}

struct DualWriteDataSink {
    accelerator: CayenneWriteTarget,
    federated: Arc<dyn TableProvider>,
    refresher: Arc<refresh::Refresher>,
    schema: SchemaRef,
}

impl DualWriteDataSink {
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

impl std::fmt::Debug for DualWriteDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualWriteDataSink").finish_non_exhaustive()
    }
}

impl DisplayAs for DualWriteDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DualWriteDataSink")
    }
}

#[async_trait]
impl DataSink for DualWriteDataSink {
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

        // `upstream_error` captures real upstream failures (`data.next()` yielding `Err`).
        // `channels_closed_early` records the *symptom* of a downstream task aborting
        // (its receiver got dropped). The real cause then lives in `staged_result` /
        // `source_result`; using the channel-close sentinel here would mask it.
        let mut upstream_error: Option<DataFusionError> = None;
        let mut channels_closed_early = false;

        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    if source_tx.send(Ok(batch.clone())).await.is_err()
                        || accelerator_tx.send(Ok(batch)).await.is_err()
                    {
                        channels_closed_early = true;
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
                if channels_closed_early {
                    if let Err(error) = staged.rollback().await {
                        tracing::error!("Failed to roll back staged Cayenne write: {error}");
                    }
                    return Err(DataFusionError::Execution(
                        "Dual-write insert stream terminated before both write paths completed"
                            .to_string(),
                    ));
                }
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
            (Err(staged_error), Ok(()), _) => Err(staged_error),
            (Err(staged_error), Err(source_error), _) => Err(DataFusionError::Execution(format!(
                "Dual-write insert failed for both accelerator and federated source: accelerator={staged_error}; source={source_error}"
            ))),
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
        .downcast_ref::<PartitionTableProvider>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Dual-write partitioned Cayenne path requires a PartitionTableProvider".to_string(),
            )
        })?;

    // Each partition's staged append below holds that partition's write lock
    // until the commit at the end, and partitions are staged in the order the
    // input reaches them. Holding the table's write coordinator for the whole
    // write keeps this from taking those locks in the opposite order to a
    // concurrent refresh, append, or dual write on the same table.
    let _write_coordinator = partitioned.write_coordinator().lock_owned().await;

    let schema = data.schema();
    let physical_exprs = create_partition_physical_exprs(partitioned, Arc::clone(&schema))?;
    let (source_tx, source_rx) = mpsc::channel(8);
    let source_task =
        spawn_federated_insert(Arc::clone(&federated), Arc::clone(&schema), source_rx);

    // See note in the non-partitioned path. `upstream_error` is for real upstream
    // failures; `channels_closed_early` is the downstream-abort symptom and must not
    // mask the underlying `staged_error` / `source_error`.
    let mut upstream_error: Option<DataFusionError> = None;
    let mut channels_closed_early = false;
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
                                    "Dual-write partitioned Cayenne path requires Cayenne-backed partition providers"
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
                        channels_closed_early = true;
                        break;
                    }
                }

                if channels_closed_early {
                    break;
                }

                if source_tx.send(Ok(batch)).await.is_err() {
                    channels_closed_early = true;
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
            if channels_closed_early {
                if let Err(error) = staged.rollback().await {
                    tracing::error!(
                        "Failed to roll back staged partitioned Cayenne write: {error}"
                    );
                }
                return Err(DataFusionError::Execution(
                    "Dual-write partitioned insert stream terminated before both write paths completed"
                        .to_string(),
                ));
            }
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
        (Err(staged_error), Ok(()), _) => Err(staged_error),
        (Err(staged_error), Err(source_error), _) => Err(DataFusionError::Execution(format!(
            "Dual-write insert failed for both partitioned accelerator and federated source: accelerator={staged_error}; source={source_error}"
        ))),
    }
}

/// Attempts to downcast a partition provider to [`CayenneTableProvider`].
fn downcast_to_cayenne(provider: &Arc<dyn TableProvider>) -> Option<&CayenneTableProvider> {
    provider.downcast_ref::<CayenneTableProvider>()
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

pub fn extract_cayenne_write_target(
    table_provider: &Arc<dyn TableProvider>,
) -> Option<CayenneWriteTarget> {
    if let Some(cayenne) = table_provider.downcast_ref::<CayenneTableProvider>() {
        return Some(CayenneWriteTarget::Staged(Box::new(
            cayenne.clone_for_write_operations(),
        )));
    }

    if let Some(partitioned) = table_provider.downcast_ref::<PartitionTableProvider>()
        && partitioned.creator().accepts_direct_partition_writes()
    {
        return Some(CayenneWriteTarget::Partitioned(Arc::clone(table_provider)));
    }

    if let Some(poly) = spice_table::find_layer::<PolyTableProvider>(
        table_provider.as_ref(),
        spice_table::LayerWalk::Write,
    ) {
        let writer = poly.writer();
        return extract_cayenne_write_target(&writer);
    }

    if let Some(upsert_dedup) = table_provider.downcast_ref::<UpsertDedupTableProvider>() {
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
            "Federated dual-write task failed: {error}"
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
// Dual-write delete and update
// ---------------------------------------------------------------------------

/// Creates a `DeletionExec` plan for dual-write deletes.
///
/// Federated delete runs first; if it succeeds the accelerator delete follows.
/// Both must succeed — if the accelerator delete fails the error is surfaced so
/// the caller knows the operation did not fully complete (the next refresh cycle
/// will reconcile, but the caller should be aware).
pub(crate) async fn delete_dual_write(
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
                "Session is not a SessionState in delete_dual_write".to_string(),
            )
        })?
        .clone();
    Ok(Arc::new(DeletionExec::new(Arc::new(
        DualWriteDeletionSink {
            federated_plan,
            accelerator_plan,
            session_state,
        },
    ))))
}

struct DualWriteDeletionSink {
    federated_plan: Arc<dyn ExecutionPlan>,
    accelerator_plan: Arc<dyn ExecutionPlan>,
    session_state: SessionState,
}

#[async_trait]
impl DeletionSink for DualWriteDeletionSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let federated_batches = datafusion::physical_plan::collect(
            Arc::clone(&self.federated_plan),
            self.session_state.task_ctx(),
        )
        .await?;
        let count = super::write_back::extract_dml_count(&federated_batches);

        // Run the accelerator plan under the LIVE execution context so a Cayenne
        // transaction (if one were active) STAGES rather than publishing — the same
        // reason the write-back sinks thread the context. Dual-write datasets are
        // currently rejected as transaction participants (see `resolve_cayenne_staged`),
        // so this is defense-in-depth and keeps request-scoped config on the write.
        datafusion::physical_plan::collect(Arc::clone(&self.accelerator_plan), context).await?;

        Ok(count)
    }
}

/// Creates a `DeletionExec` plan for dual-write updates.
///
/// Federated update runs first; if it succeeds the accelerator update follows.
pub(crate) async fn update_dual_write(
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
                "Session is not a SessionState in update_dual_write".to_string(),
            )
        })?
        .clone();
    Ok(Arc::new(DeletionExec::new(Arc::new(DualWriteUpdateSink {
        federated_plan,
        accelerator_plan,
        session_state,
    }))))
}

struct DualWriteUpdateSink {
    federated_plan: Arc<dyn ExecutionPlan>,
    accelerator_plan: Arc<dyn ExecutionPlan>,
    session_state: SessionState,
}

#[async_trait]
impl DeletionSink for DualWriteUpdateSink {
    async fn delete_from(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let federated_batches = datafusion::physical_plan::collect(
            Arc::clone(&self.federated_plan),
            self.session_state.task_ctx(),
        )
        .await?;
        let count = super::write_back::extract_dml_count(&federated_batches);

        // Run the accelerator plan under the LIVE execution context so a Cayenne
        // transaction (if one were active) STAGES rather than publishing — the same
        // reason the write-back sinks thread the context. Dual-write datasets are
        // currently rejected as transaction participants (see `resolve_cayenne_staged`),
        // so this is defense-in-depth and keeps request-scoped config on the write.
        datafusion::physical_plan::collect(Arc::clone(&self.accelerator_plan), context).await?;

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
    use super::{DualWriteDeletionSink, DualWriteUpdateSink};
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
        properties: Arc<PlanProperties>,
        message: String,
    }

    impl ErrorExec {
        fn new_arc(message: impl Into<String>) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::UInt64,
                false,
            )]));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
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
        fn properties(&self) -> &Arc<PlanProperties> {
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

    // ── DualWriteDeletionSink ─────────────────────────────────────────

    #[tokio::test]
    async fn dual_write_deletion_count_comes_from_federated() {
        let sink = DualWriteDeletionSink {
            federated_plan: count_exec(5),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let count = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect("deletion should succeed");
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn dual_write_deletion_federated_error_propagates() {
        let sink = DualWriteDeletionSink {
            federated_plan: ErrorExec::new_arc("federated delete failed"),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let err = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect_err("deletion should fail");
        assert!(err.to_string().contains("federated delete failed"));
    }

    #[tokio::test]
    async fn dual_write_deletion_accelerator_error_propagates() {
        let sink = DualWriteDeletionSink {
            federated_plan: count_exec(5),
            accelerator_plan: ErrorExec::new_arc("accelerator delete failed"),
            session_state: SessionContext::new().state(),
        };

        let err = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect_err("deletion should fail");
        assert!(err.to_string().contains("accelerator delete failed"));
    }

    // ── DualWriteUpdateSink ───────────────────────────────────────────

    #[tokio::test]
    async fn dual_write_update_count_comes_from_federated() {
        let sink = DualWriteUpdateSink {
            federated_plan: count_exec(3),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let count = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect("update should succeed");
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn dual_write_update_federated_error_propagates() {
        let sink = DualWriteUpdateSink {
            federated_plan: ErrorExec::new_arc("federated update failed"),
            accelerator_plan: count_exec(0),
            session_state: SessionContext::new().state(),
        };

        let err = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect_err("update should fail");
        assert!(err.to_string().contains("federated update failed"));
    }

    #[tokio::test]
    async fn dual_write_update_accelerator_error_propagates() {
        let sink = DualWriteUpdateSink {
            federated_plan: count_exec(3),
            accelerator_plan: ErrorExec::new_arc("accelerator update failed"),
            session_state: SessionContext::new().state(),
        };

        let err = sink
            .delete_from(Arc::new(TaskContext::default()))
            .await
            .expect_err("update should fail");
        assert!(err.to_string().contains("accelerator update failed"));
    }

    /// A partitioned dual write stages partitions in the order its input reaches
    /// them, holding each partition's write lock until it commits. It must share
    /// the table's write coordinator with every other writer that stages several
    /// partitions, or a writer holding partition `a` that then needs `b`
    /// deadlocks against a dual write holding `b` that needs `a`.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partitioned_dual_write_serializes_with_other_multi_partition_writers() {
        use std::time::Duration;

        use arrow::array::{Int64Array, StringArray};
        use cayenne::metadata::{CreateTableOptions, VortexConfig};
        use cayenne::{
            CayenneCatalog, CayennePartitionCreator, CayenneTableProvider, MetadataCatalog,
        };
        use datafusion::catalog::TableProvider;
        use datafusion::datasource::MemTable;
        use datafusion::logical_expr::col;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::scalar::ScalarValue;
        use datafusion::common::TableReference;
        use datafusion_table_providers::UnsupportedTypeAction;
        use runtime_component::dataset::acceleration::RefreshMode;
        use runtime_table_partition::expression::PartitionedBy;
        use runtime_table_partition::provider::PartitionTableProvider;
        use tokio::sync::{Mutex, RwLock, mpsc};
        use tokio_stream::wrappers::ReceiverStream;

        use crate::accelerated::refresh::{Refresh, Refresher};
        use crate::federated::FederatedTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Utf8, false),
        ]));
        let rows = |ids: Vec<i64>, key: &str| {
            let keys = vec![key; ids.len()];
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(keys)),
                ],
            )
            .expect("rows match the schema")
        };
        let nothing = || -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::empty(),
            ))
        };

        let dir = tempfile::tempdir().expect("temp dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!(
                "sqlite://{}",
                dir.path().join("cayenne.db").display()
            ))
            .expect("catalog"),
        );
        catalog.init().await.expect("catalog initializes");
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).expect("data dir");
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: "events".to_string(),
                schema: Arc::clone(&schema),
                primary_key: Vec::new(),
                on_conflict: None,
                base_path: data_path.display().to_string(),
                partition_column: Some("k".to_string()),
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("partitioned table registers");
        let partition_by = vec![PartitionedBy {
            name: "k".to_string(),
            expression: col("k"),
        }];
        let creator = Arc::new(
            CayennePartitionCreator::new(
                "events".to_string(),
                data_path,
                partition_by.clone(),
                Arc::clone(&schema),
                Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
                table_id,
                UnsupportedTypeAction::Error,
                Vec::new(),
                None,
                VortexConfig::default(),
                None,
                Vec::new(),
                None,
                SessionContext::new().runtime_env(),
            )
            .with_direct_partition_writes(),
        );
        let partitioned = Arc::new(
            PartitionTableProvider::new(creator, partition_by, Arc::clone(&schema))
                .await
                .expect("partitioned provider"),
        );
        let partition = |key: &str| {
            let partitioned = Arc::clone(&partitioned);
            let key = key.to_string();
            async move {
                partitioned
                    .get_or_create_partition_provider(vec![ScalarValue::Utf8(Some(key))])
                    .await
                    .expect("partition")
                    .downcast_ref::<CayenneTableProvider>()
                    .expect("a Cayenne partition")
                    .clone_for_write_operations()
            }
        };

        let federated = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("federated table"),
        ) as Arc<dyn TableProvider>;
        let refresher = Arc::new(Refresher::new(
            runtime_status::RuntimeStatus::new(),
            TableReference::bare("events"),
            Arc::new(FederatedTable::new_unchecked(Arc::clone(&federated))),
            None,
            Arc::new(RwLock::new(Refresh::new(RefreshMode::Full))),
            Arc::clone(&partitioned) as Arc<dyn TableProvider>,
            None,
            None,
            tokio::runtime::Handle::current(),
            Arc::new(Mutex::new(())),
        ));

        // Another multi-partition writer holds the coordinator and has staged `a`.
        let coordinator = partitioned.write_coordinator().lock_owned().await;
        let staged_a = partition("a")
            .await
            .begin_overwrite(nothing(), 1)
            .await
            .expect("stage a");

        // The dual write reaches `b` first, then `a`.
        let (input, input_rx) = mpsc::channel(4);
        let dual_write = tokio::spawn(super::write_all_with_partitioned_cayenne(
            refresher,
            Arc::clone(&partitioned) as Arc<dyn TableProvider>,
            Arc::clone(&federated),
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                ReceiverStream::new(input_rx),
            )),
            1,
        ));
        input
            .send(Ok(rows(vec![1, 2], "b")))
            .await
            .expect("the dual write accepts input");
        // Give a dual write that ignores the coordinator every chance to take `b`
        // before it sees `a`: poll until something holds `b`, briefly releasing
        // each probe that gets it.
        let b = partition("b").await;
        for _ in 0..40 {
            match tokio::time::timeout(Duration::from_millis(50), b.begin_overwrite(nothing(), 1))
                .await
            {
                Ok(probe) => {
                    probe
                        .expect("probe b")
                        .rollback()
                        .await
                        .expect("release the probe");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(_) => break,
            }
        }
        input
            .send(Ok(rows(vec![3], "a")))
            .await
            .expect("the dual write accepts input");

        let staged_b =
            tokio::time::timeout(Duration::from_secs(10), b.begin_overwrite(nothing(), 1))
                .await
                .expect("staging `b` must not wait on a dual write that is waiting for `a`")
                .expect("stage b");
        staged_b.rollback().await.expect("roll back b");
        staged_a.rollback().await.expect("roll back a");
        drop(coordinator);
        drop(input);

        let written = tokio::time::timeout(Duration::from_secs(30), dual_write)
            .await
            .expect("the dual write finishes once the coordinator is free")
            .expect("the dual write task joins")
            .expect("the dual write succeeds");
        assert_eq!(written, 3);
    }
}
