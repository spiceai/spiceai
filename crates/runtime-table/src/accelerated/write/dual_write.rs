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
//! append) and the federated source. On success the source commits, then the
//! accelerator; a write that fails or stops before the source commits commits
//! neither, and the error is surfaced synchronously. Supports both
//! non-partitioned and partitioned Cayenne accelerators.
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
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::StreamExt;
use tokio::sync::{OwnedMutexGuard, mpsc};
use tokio::task::JoinHandle;

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

        let mut input_end = InputEnd::Complete;
        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    if source_tx
                        .send(PathMessage::Batch(Ok(batch.clone())))
                        .await
                        .is_err()
                    {
                        input_end = InputEnd::SourceClosed;
                        break;
                    }
                    if accelerator_tx
                        .send(PathMessage::Batch(Ok(batch)))
                        .await
                        .is_err()
                    {
                        input_end = InputEnd::AcceleratorClosed;
                        break;
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    let _ = source_tx
                        .send(PathMessage::Batch(Err(DataFusionError::Execution(
                            message.clone(),
                        ))))
                        .await;
                    let _ = accelerator_tx
                        .send(PathMessage::Batch(Err(DataFusionError::Execution(
                            message.clone(),
                        ))))
                        .await;
                    input_end = InputEnd::Failed(DataFusionError::Execution(message));
                    break;
                }
            }
        }

        if !matches!(input_end, InputEnd::Failed(_)) {
            let _ = accelerator_tx.send(PathMessage::End).await;
        }
        drop(accelerator_tx);
        let staged = join_staged_task(staged_task).await.map(StagedAppends::from);

        complete_dual_write(
            input_end,
            staged,
            source_tx,
            source_task,
            None,
            Arc::clone(&self.refresher),
        )
        .await
    }
}

/// What a dual write sends to each of its write paths.
enum PathMessage {
    Batch(datafusion::common::Result<RecordBatch>),
    /// Ends the path's input. A path whose sender is dropped before this message fails instead
    /// of ending, so a dual write that stops early cannot commit what it already passed on.
    /// The source's input is ended only once the accelerator has staged the whole input; the
    /// accelerator's is ended unless the input failed, since a staged append commits only when
    /// told to.
    End,
}

/// The input of one write path: the batches sent to it, until [`PathMessage::End`].
fn path_input(
    schema: SchemaRef,
    receiver: mpsc::Receiver<PathMessage>,
) -> SendableRecordBatchStream {
    let batches = futures::stream::unfold(Some(receiver), |receiver| async move {
        let mut receiver = receiver?;
        match receiver.recv().await {
            Some(PathMessage::Batch(batch)) => {
                let next = batch.is_ok().then_some(receiver);
                Some((batch, next))
            }
            Some(PathMessage::End) => None,
            None => Some((
                Err(DataFusionError::Execution(
                    "The dual write stopped before its input was complete".to_string(),
                )),
                None,
            )),
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, batches))
}

/// How the input loop of a dual write ended.
enum InputEnd {
    /// Every batch reached both write paths.
    Complete,
    /// The input failed.
    Failed(DataFusionError),
    /// The source stopped taking batches; its error is the cause.
    SourceClosed,
    /// The accelerator stopped taking batches; its error is the cause.
    AcceleratorClosed,
}

/// Completes a dual write after its input loop.
///
/// The source commits when its input ends, so it is told the input is complete only after the
/// accelerator has staged all of it, and the accelerator commits after the source. From that
/// point both commits run in a task of their own, so a caller that stops waiting cannot separate
/// them. Otherwise neither path commits: the source's input fails and the staged rows are rolled
/// back.
async fn complete_dual_write(
    input_end: InputEnd,
    staged: datafusion::common::Result<StagedAppends>,
    source_tx: mpsc::Sender<PathMessage>,
    source_task: JoinHandle<datafusion::common::Result<()>>,
    write_coordinator: Option<OwnedMutexGuard<()>>,
    refresher: Arc<refresh::Refresher>,
) -> datafusion::common::Result<u64> {
    let staged = match (input_end, staged) {
        (InputEnd::Complete, Ok(staged)) => staged,
        (input_end, staged) => {
            drop(source_tx);
            let source_result = join_source_task(source_task).await;
            let staged_error = match staged {
                Ok(staged) => {
                    if let Err(error) = staged.rollback().await {
                        tracing::error!("Failed to roll back staged Cayenne write: {error}");
                    }
                    None
                }
                Err(error) => Some(error),
            };
            let cause = match input_end {
                InputEnd::Failed(error) => Some(error),
                InputEnd::SourceClosed => source_result.err().or(staged_error),
                InputEnd::Complete | InputEnd::AcceleratorClosed => {
                    staged_error.or(source_result.err())
                }
            };
            return Err(cause.unwrap_or_else(|| {
                DataFusionError::Execution(
                    "Dual-write insert stream terminated before both write paths completed"
                        .to_string(),
                )
            }));
        }
    };

    tokio::spawn(async move {
        let _write_coordinator = write_coordinator;
        let _ = source_tx.send(PathMessage::End).await;
        drop(source_tx);
        if let Err(error) = join_source_task(source_task).await {
            if let Err(rollback_error) = staged.rollback().await {
                tracing::error!("Failed to roll back staged Cayenne write: {rollback_error}");
            }
            return Err(error);
        }
        let committed = staged.commit().await;
        if committed.is_ok() {
            refresher.set_initial_load_completed(true);
        }
        committed
    })
    .await
    .map_err(|error| {
        DataFusionError::Execution(format!("Dual-write commit task failed: {error}"))
    })?
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
    let write_coordinator = partitioned.write_coordinator().lock_owned().await;

    let schema = data.schema();
    let physical_exprs = create_partition_physical_exprs(partitioned, Arc::clone(&schema))?;
    let (source_tx, source_rx) = mpsc::channel(8);
    let source_task =
        spawn_federated_insert(Arc::clone(&federated), Arc::clone(&schema), source_rx);

    let mut input_end = InputEnd::Complete;
    let mut partition_senders = HashMap::<String, mpsc::Sender<PathMessage>>::new();
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

                    if sender
                        .send(PathMessage::Batch(Ok(partition_batch)))
                        .await
                        .is_err()
                    {
                        input_end = InputEnd::AcceleratorClosed;
                        break;
                    }
                }

                if !matches!(input_end, InputEnd::Complete) {
                    break;
                }

                if source_tx.send(PathMessage::Batch(Ok(batch))).await.is_err() {
                    input_end = InputEnd::SourceClosed;
                    break;
                }
            }
            Err(error) => {
                let message = error.to_string();
                let _ = source_tx
                    .send(PathMessage::Batch(Err(DataFusionError::Execution(
                        message.clone(),
                    ))))
                    .await;
                for sender in partition_senders.values() {
                    let _ = sender
                        .send(PathMessage::Batch(Err(DataFusionError::Execution(
                            message.clone(),
                        ))))
                        .await;
                }
                input_end = InputEnd::Failed(DataFusionError::Execution(message));
                break;
            }
        }
    }

    if !matches!(input_end, InputEnd::Failed(_)) {
        for sender in partition_senders.values() {
            let _ = sender.send(PathMessage::End).await;
        }
    }
    drop(partition_senders);
    let staged = join_partitioned_staged_tasks(partition_handles).await;

    complete_dual_write(
        input_end,
        staged,
        source_tx,
        source_task,
        Some(write_coordinator),
        refresher,
    )
    .await
}

/// Attempts to downcast a partition provider to [`CayenneTableProvider`].
fn downcast_to_cayenne(provider: &Arc<dyn TableProvider>) -> Option<&CayenneTableProvider> {
    provider.downcast_ref::<CayenneTableProvider>()
}

fn spawn_federated_insert(
    federated: Arc<dyn TableProvider>,
    schema: SchemaRef,
    receiver: mpsc::Receiver<PathMessage>,
) -> JoinHandle<datafusion::common::Result<()>> {
    tokio::spawn(async move {
        let ctx = util::session_state::session_context();
        let input: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(
            Arc::new(StreamingDataUpdateExecutionPlan::new(path_input(
                schema, receiver,
            ))),
            federated.schema(),
        ));

        let insert_plan = federated
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await?;
        let _ = datafusion::physical_plan::collect(insert_plan, ctx.task_ctx()).await?;
        Ok(())
    })
}

/// The staged appends of one dual write: one for a table, one per partition for a partitioned
/// table.
struct StagedAppends {
    staged_appends: Vec<CayenneStagedAppend>,
    row_count: u64,
}

impl std::fmt::Debug for StagedAppends {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedAppends")
            .field("staged_appends", &self.staged_appends.len())
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl From<CayenneStagedAppend> for StagedAppends {
    fn from(staged_append: CayenneStagedAppend) -> Self {
        Self {
            row_count: staged_append.row_count(),
            staged_appends: vec![staged_append],
        }
    }
}

impl StagedAppends {
    async fn commit(self) -> datafusion::common::Result<u64> {
        for staged_append in self.staged_appends {
            staged_append
                .commit()
                .await
                .map_err(DataFusionError::from)?;
        }
        Ok(self.row_count)
    }

    /// Rolls back every staged append, even after one fails, and returns the first failure.
    async fn rollback(self) -> datafusion::common::Result<()> {
        let mut failure = None;
        for staged_append in self.staged_appends {
            if let Err(error) = staged_append.rollback().await
                && failure.is_none()
            {
                failure = Some(DataFusionError::from(error));
            }
        }
        failure.map_or(Ok(()), Err)
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
                &execution_props, &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )
        })
        .collect()
}

/// Joins every partition's staging task. If any failed, the rows the others staged are rolled
/// back and the first failure, in partition order, is returned.
async fn join_partitioned_staged_tasks(
    handles: Vec<JoinHandle<datafusion::common::Result<CayenneStagedAppend>>>,
) -> datafusion::common::Result<StagedAppends> {
    let mut staged_appends = Vec::with_capacity(handles.len());
    let mut row_count = 0_u64;
    let mut failure = None;

    for handle in handles {
        match join_staged_task(handle).await {
            Ok(staged_append) => {
                row_count += staged_append.row_count();
                staged_appends.push(staged_append);
            }
            Err(error) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
    }

    let staged = StagedAppends {
        staged_appends,
        row_count,
    };
    let Some(failure) = failure else {
        return Ok(staged);
    };
    if let Err(error) = staged.rollback().await {
        tracing::error!("Failed to roll back staged Cayenne write: {error}");
    }
    Err(failure)
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

/// Stop the background maintenance of every Cayenne instance `table_provider`
/// serves from — the provider itself, each partition of a partitioned table, and
/// what a poly or upsert-dedup wrapper writes to — and wait for maintenance they
/// already started.
///
/// Called when a dataset generation is drained for replacement or removal: the
/// replacement opens the same table on the same catalog, and nothing in memory
/// serializes the two instances' maintenance (#11581). Every instance is
/// quiesced at once, and the wait is not bounded: a drain publishes completion
/// only once the work it drains has ended, and a pass still running when the
/// replacement starts is exactly the overlap this exists to prevent.
pub async fn quiesce_cayenne_maintenance(table_provider: &Arc<dyn TableProvider>) {
    let mut instances = Vec::new();
    let mut pending = vec![Arc::clone(table_provider)];
    while let Some(provider) = pending.pop() {
        if let Some(cayenne) = spice_table::find_concrete::<CayenneTableProvider>(
            provider.as_ref(),
            spice_table::LayerWalk::Write,
        ) {
            instances.push(cayenne.clone_for_write_operations());
        } else if let Some(partitioned) = spice_table::find_concrete::<PartitionTableProvider>(
            provider.as_ref(),
            spice_table::LayerWalk::Write,
        ) {
            pending.extend(partitioned.partition_table_providers().await);
        } else if let Some(poly) = spice_table::find_layer::<PolyTableProvider>(
            provider.as_ref(),
            spice_table::LayerWalk::Write,
        ) {
            pending.push(poly.writer());
        } else if let Some(upsert_dedup) = spice_table::find_concrete::<UpsertDedupTableProvider>(
            provider.as_ref(),
            spice_table::LayerWalk::Write,
        ) {
            pending.push(Arc::clone(upsert_dedup.inner()));
        }
    }
    futures::future::join_all(instances.iter().map(CayenneTableProvider::quiesce)).await;
}

fn spawn_staged_append(
    accelerator: CayenneTableProvider,
    schema: SchemaRef,
    receiver: mpsc::Receiver<PathMessage>,
    target_partitions: usize,
) -> JoinHandle<datafusion::common::Result<CayenneStagedAppend>> {
    tokio::spawn(async move {
        accelerator
            .begin_staged_append(path_input(schema, receiver), target_partitions)
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

        use datafusion::catalog::TableProvider;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let events = fixture::Events::new(Vec::new()).await;
        let partitioned = &events.partitioned;
        let nothing = || -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&events.schema),
                futures::stream::empty(),
            ))
        };

        let federated = Arc::new(
            MemTable::try_new(Arc::clone(&events.schema), vec![vec![]]).expect("federated table"),
        ) as Arc<dyn TableProvider>;

        // Another multi-partition writer holds the coordinator and has staged `a`.
        let coordinator = partitioned.write_coordinator().lock_owned().await;
        let staged_a = events
            .partition("a")
            .await
            .begin_overwrite(nothing(), 1)
            .await
            .expect("stage a");

        // Every clone of the coordinator is a holder or a waiter. With the lock
        // held here, a dual write that adds one is parked on it: a positive,
        // deterministic signal that it serializes with this writer, rather than a
        // window in which a writer ignoring the coordinator might take `b`.
        let coordinator_handle = partitioned.write_coordinator();
        let idle_holders = Arc::strong_count(&coordinator_handle);

        // The dual write would reach `b` first, then `a`.
        let (input, data) = events.input(4);
        let dual_write = events
            .spawn_dual_write(fixture::Shape::Partitioned, federated, data)
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&coordinator_handle) == idle_holders {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the dual write never queued on the table's write coordinator \
                 ({idle_holders} holder(s) throughout)"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Parked on the coordinator it reads none of its input, so it cannot take
        // `b` whatever order the input reaches the partitions in.
        input
            .send(Ok(events.rows(vec![1, 2], "b")))
            .await
            .expect("the dual write accepts input");
        input
            .send(Ok(events.rows(vec![3], "a")))
            .await
            .expect("the dual write accepts input");
        assert_eq!(
            input.capacity(),
            2,
            "a dual write parked on the coordinator must not have read its input"
        );

        let b = events.partition("b").await;
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

    /// A dual write that is dropped partway, as when its client disconnects or a `DoPut` times
    /// out, commits neither path: the source's input fails instead of ending, so the source
    /// cannot commit the batches it already received.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dual_write_dropped_partway_commits_neither_path() {
        use std::time::Duration;

        for shape in [fixture::Shape::Table, fixture::Shape::Partitioned] {
            let events = fixture::Events::new(Vec::new()).await;
            let (federated, mut inserts) = fixture::RecordingTable::create(&events.schema);
            let (input, data) = events.input(1);
            let dual_write = events.spawn_dual_write(shape, federated, data).await;

            // The input holds one batch, so the third send returns only after the dual write
            // has passed the first batch to both paths.
            for id in 1..=3 {
                input
                    .send(Ok(events.rows(vec![id], "a")))
                    .await
                    .expect("the dual write takes its input");
            }
            dual_write.abort();
            let cancelled = matches!(dual_write.await, Err(error) if error.is_cancelled());
            assert!(cancelled, "{shape:?}: the dual write must be cancelled");

            let insert = tokio::time::timeout(Duration::from_secs(10), inserts.recv())
                .await
                .expect("the source insert ends")
                .expect("the source insert reports how it ended");
            assert!(
                insert.is_err(),
                "{shape:?}: the source committed a dual write that was dropped: {insert:?}"
            );

            // The next dual write on the partition stages only once the dropped one's staging
            // has released the partition, so after it commits the partition holds its row alone.
            let (federated, _inserts) = fixture::RecordingTable::create(&events.schema);
            let (input, data) = events.input(1);
            let next = events.spawn_dual_write(shape, federated, data).await;
            input
                .send(Ok(events.rows(vec![100], "a")))
                .await
                .expect("the next dual write takes its input");
            drop(input);
            tokio::time::timeout(Duration::from_secs(10), next)
                .await
                .expect("the next dual write ends")
                .expect("the next dual write task joins")
                .expect("the next dual write succeeds");
            assert_eq!(
                events.cayenne_rows("a").await,
                1,
                "{shape:?}: the dropped dual write published rows to the accelerator"
            );
        }
    }

    /// Once the source has been told the input is complete, the dual write runs to its end even
    /// if its caller stops waiting: the source commits, then the accelerator, and a partitioned
    /// table's write coordinator stays held until then.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dual_write_dropped_while_the_source_commits_still_commits_the_accelerator() {
        use std::time::Duration;

        for shape in [fixture::Shape::Table, fixture::Shape::Partitioned] {
            let events = fixture::Events::new(Vec::new()).await;
            let (federated, mut inserts, gate) = fixture::RecordingTable::gated(&events.schema);
            let (input, data) = events.input(1);
            let dual_write = events.spawn_dual_write(shape, federated, data).await;
            input
                .send(Ok(events.rows(vec![1, 2], "a")))
                .await
                .expect("the dual write takes its input");
            drop(input);

            tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
                .await
                .expect("the source reads its whole input");
            dual_write.abort();
            let cancelled = matches!(dual_write.await, Err(error) if error.is_cancelled());
            assert!(cancelled, "{shape:?}: the dual write must be cancelled");
            if matches!(shape, fixture::Shape::Partitioned) {
                let held = events.partitioned.write_coordinator().try_lock().is_err();
                assert!(
                    held,
                    "the write coordinator must stay held until the accelerator commits"
                );
            }
            gate.release.notify_one();

            let insert = tokio::time::timeout(Duration::from_secs(10), inserts.recv())
                .await
                .expect("the source insert ends")
                .expect("the source insert reports how it ended");
            assert_eq!(insert, Ok(2), "{shape:?}");
            let committed = tokio::time::timeout(Duration::from_secs(10), async {
                while events.cayenne_rows("a").await != 2 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .is_ok();
            assert!(
                committed,
                "{shape:?}: the accelerator did not commit after the caller stopped waiting"
            );
        }
    }

    /// When the accelerator cannot stage a write, the source commits none of it either, and the
    /// dual write fails with the accelerator's error.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_accelerator_that_cannot_stage_keeps_the_source_from_committing() {
        use std::time::Duration;

        for shape in [fixture::Shape::Table, fixture::Shape::Partitioned] {
            // A Cayenne table with a primary key refuses a staged append.
            let events = fixture::Events::new(vec!["id".to_string()]).await;
            let (federated, mut inserts) = fixture::RecordingTable::create(&events.schema);
            let (input, data) = events.input(1);
            let dual_write = events.spawn_dual_write(shape, federated, data).await;

            input
                .send(Ok(events.rows(vec![1, 2], "a")))
                .await
                .expect("the dual write takes its input");
            drop(input);

            let error = tokio::time::timeout(Duration::from_secs(10), dual_write)
                .await
                .expect("the dual write ends")
                .expect("the dual write task joins")
                .expect_err("the dual write fails");
            assert!(
                error.to_string().contains("primary-key deletion handling"),
                "{shape:?}: the dual write must fail with the accelerator's error: {error}"
            );
            let insert = tokio::time::timeout(Duration::from_secs(10), inserts.recv())
                .await
                .expect("the source insert ends")
                .expect("the source insert reports how it ended");
            assert!(
                insert.is_err(),
                "{shape:?}: the source committed a write the accelerator refused: {insert:?}"
            );
        }
    }

    #[cfg(not(windows))]
    mod fixture {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use arrow::record_batch::RecordBatch;
        use async_trait::async_trait;
        use cayenne::metadata::{CreateTableOptions, VortexConfig};
        use cayenne::{
            CayenneCatalog, CayennePartitionCreator, CayenneTableProvider, MetadataCatalog,
        };
        use datafusion::catalog::{Session, TableProvider};
        use datafusion::common::TableReference;
        use datafusion::datasource::TableType;
        use datafusion::error::{DataFusionError, Result as DataFusionResult};
        use datafusion::execution::{SendableRecordBatchStream, TaskContext};
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::logical_expr::{Expr, col};
        use datafusion::physical_plan::metrics::MetricsSet;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
        use datafusion::prelude::{SessionConfig, SessionContext};
        use datafusion::scalar::ScalarValue;
        use datafusion_datasource::sink::{DataSink, DataSinkExec};
        use datafusion_table_providers::UnsupportedTypeAction;
        use futures::StreamExt;
        use runtime_component::dataset::acceleration::RefreshMode;
        use runtime_table_partition::expression::PartitionedBy;
        use runtime_table_partition::provider::PartitionTableProvider;
        use tokio::sync::{Mutex, RwLock, mpsc};
        use tokio::task::JoinHandle;
        use tokio_stream::wrappers::ReceiverStream;

        use crate::accelerated::refresh::{Refresh, Refresher};
        use crate::federated::FederatedTable;

        /// Which of the two dual-write paths a test drives.
        #[derive(Clone, Copy, Debug)]
        pub(super) enum Shape {
            /// One Cayenne table: partition `a` of [`Events`], written as a table of its own.
            Table,
            /// The whole partitioned [`Events`] table.
            Partitioned,
        }

        /// A Cayenne table `events (id BIGINT, k VARCHAR)` partitioned by `k`, in a temporary
        /// directory.
        pub(super) struct Events {
            _dir: tempfile::TempDir,
            pub(super) schema: SchemaRef,
            pub(super) partitioned: Arc<PartitionTableProvider>,
        }

        impl Events {
            pub(super) async fn new(primary_key: Vec<String>) -> Self {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("k", DataType::Utf8, false),
                ]));
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
                        primary_key: primary_key.clone(),
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
                        primary_key,
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
                Self {
                    _dir: dir,
                    schema,
                    partitioned,
                }
            }

            pub(super) fn rows(&self, ids: Vec<i64>, key: &str) -> RecordBatch {
                let keys = vec![key; ids.len()];
                RecordBatch::try_new(
                    Arc::clone(&self.schema),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(keys)),
                    ],
                )
                .expect("rows match the schema")
            }

            pub(super) async fn partition(&self, key: &str) -> CayenneTableProvider {
                self.partitioned
                    .get_or_create_partition_provider(vec![ScalarValue::Utf8(Some(
                        key.to_string(),
                    ))])
                    .await
                    .expect("partition")
                    .downcast_ref::<CayenneTableProvider>()
                    .expect("a Cayenne partition")
                    .clone_for_write_operations()
            }

            /// The rows committed to the Cayenne partition `key`.
            pub(super) async fn cayenne_rows(&self, key: &str) -> usize {
                SessionContext::new()
                    .read_table(Arc::new(self.partition(key).await))
                    .expect("the partition reads")
                    .count()
                    .await
                    .expect("the partition counts")
            }

            /// A dual write's input, fed through the returned sender, which holds `capacity`
            /// batches.
            pub(super) fn input(
                &self,
                capacity: usize,
            ) -> (
                mpsc::Sender<DataFusionResult<RecordBatch>>,
                SendableRecordBatchStream,
            ) {
                let (input, receiver) = mpsc::channel(capacity);
                let data = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    ReceiverStream::new(receiver),
                ));
                (input, data)
            }

            pub(super) async fn spawn_dual_write(
                &self,
                shape: Shape,
                federated: Arc<dyn TableProvider>,
                data: SendableRecordBatchStream,
            ) -> JoinHandle<DataFusionResult<u64>> {
                let refresher = Arc::new(Refresher::new(
                    runtime_status::RuntimeStatus::new(),
                    TableReference::bare("events"),
                    Arc::new(FederatedTable::new_unchecked(Arc::clone(&federated))),
                    None,
                    Arc::new(RwLock::new(Refresh::new(RefreshMode::Full))),
                    Arc::clone(&self.partitioned) as Arc<dyn TableProvider>,
                    None,
                    None,
                    tokio::runtime::Handle::current(),
                    Arc::new(Mutex::new(())),
                ));
                match shape {
                    Shape::Partitioned => {
                        tokio::spawn(super::super::write_all_with_partitioned_cayenne(
                            refresher,
                            Arc::clone(&self.partitioned) as Arc<dyn TableProvider>,
                            federated,
                            data,
                            1,
                        ))
                    }
                    Shape::Table => {
                        let sink = super::super::DualWriteDataSink::new(
                            super::super::CayenneWriteTarget::Staged(Box::new(
                                self.partition("a").await,
                            )),
                            federated,
                            refresher,
                            Arc::clone(&self.schema),
                        );
                        let context =
                            Arc::new(TaskContext::default().with_session_config(
                                SessionConfig::new().with_target_partitions(1),
                            ));
                        tokio::spawn(async move { sink.write_all(data, &context).await })
                    }
                }
            }
        }

        /// How each insert into a [`RecordingTable`] ended: the rows it committed, or its error.
        pub(super) type Inserts = mpsc::UnboundedReceiver<Result<usize, String>>;

        /// A federated table whose inserts, like an Iceberg table's, commit the rows they read
        /// when their input ends and nothing when it fails. Each insert reports how it ended:
        /// the rows it committed, or its error.
        #[derive(Debug)]
        pub(super) struct RecordingTable {
            schema: SchemaRef,
            inserts: mpsc::UnboundedSender<Result<usize, String>>,
            gate: Option<Arc<SourceGate>>,
        }

        /// Holds a recording table's insert after it has read its whole input and before it
        /// commits, until the test releases it.
        #[derive(Debug, Default)]
        pub(super) struct SourceGate {
            pub(super) reached: tokio::sync::Notify,
            pub(super) release: tokio::sync::Notify,
        }

        impl RecordingTable {
            pub(super) fn create(schema: &SchemaRef) -> (Arc<dyn TableProvider>, Inserts) {
                Self::with_gate(schema, None)
            }

            /// A recording table whose inserts wait at the returned gate before they commit.
            pub(super) fn gated(
                schema: &SchemaRef,
            ) -> (Arc<dyn TableProvider>, Inserts, Arc<SourceGate>) {
                let gate = Arc::new(SourceGate::default());
                let (table, outcomes) = Self::with_gate(schema, Some(Arc::clone(&gate)));
                (table, outcomes, gate)
            }

            fn with_gate(
                schema: &SchemaRef,
                gate: Option<Arc<SourceGate>>,
            ) -> (Arc<dyn TableProvider>, Inserts) {
                let (inserts, outcomes) = mpsc::unbounded_channel();
                let table = Arc::new(Self {
                    schema: Arc::clone(schema),
                    inserts,
                    gate,
                });
                (table, outcomes)
            }
        }

        #[async_trait]
        impl TableProvider for RecordingTable {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }

            fn table_type(&self) -> TableType {
                TableType::Base
            }

            async fn scan(
                &self,
                _state: &dyn Session,
                _projection: Option<&Vec<usize>>,
                _filters: &[Expr],
                _limit: Option<usize>,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                Err(DataFusionError::NotImplemented(
                    "the recording table is written, not read".to_string(),
                ))
            }

            async fn insert_into(
                &self,
                _state: &dyn Session,
                input: Arc<dyn ExecutionPlan>,
                _insert_op: InsertOp,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(DataSinkExec::new(
                    input,
                    Arc::new(RecordingSink {
                        schema: Arc::clone(&self.schema),
                        inserts: self.inserts.clone(),
                        gate: self.gate.clone(),
                    }),
                    None,
                )))
            }
        }

        #[derive(Debug)]
        struct RecordingSink {
            schema: SchemaRef,
            inserts: mpsc::UnboundedSender<Result<usize, String>>,
            gate: Option<Arc<SourceGate>>,
        }

        impl DisplayAs for RecordingSink {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                write!(f, "RecordingSink")
            }
        }

        #[async_trait]
        impl DataSink for RecordingSink {
            fn metrics(&self) -> Option<MetricsSet> {
                None
            }

            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            async fn write_all(
                &self,
                mut data: SendableRecordBatchStream,
                _context: &Arc<TaskContext>,
            ) -> DataFusionResult<u64> {
                let mut rows = 0;
                while let Some(batch) = data.next().await {
                    match batch {
                        Ok(batch) => rows += batch.num_rows(),
                        Err(error) => {
                            let _ = self.inserts.send(Err(error.to_string()));
                            return Err(error);
                        }
                    }
                }
                if let Some(gate) = &self.gate {
                    gate.reached.notify_one();
                    gate.release.notified().await;
                }
                let _ = self.inserts.send(Ok(rows));
                Ok(rows as u64)
            }
        }
    }
}
