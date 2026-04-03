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

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::{CayenneStagedAppend, CayenneTableProvider};
use data_components::poly::PolyTableProvider;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::{Constraints, DFSchema, DataFusionError, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
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

use crate::accelerated_table::AcceleratedTable;
use crate::dataaccelerator::cayenne::CayennePartitionCreator;
use crate::dataaccelerator::upsert_dedup::UpsertDedupTableProvider;
use crate::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::insert::partition_batch_composite;
use runtime_table_partition::provider::PartitionTableProvider;

enum CayenneWriteTarget {
    Staged(CayenneTableProvider),
    Partitioned(Arc<dyn TableProvider>),
}

impl Clone for CayenneWriteTarget {
    fn clone(&self) -> Self {
        match self {
            Self::Staged(provider) => Self::Staged(provider.clone_for_write_operations()),
            Self::Partitioned(provider) => Self::Partitioned(Arc::clone(provider)),
        }
    }
}

impl std::fmt::Debug for CayenneWriteTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Staged(_) => f.write_str("CayenneWriteTarget::Staged"),
            Self::Partitioned(_) => f.write_str("CayenneWriteTarget::Partitioned"),
        }
    }
}

#[derive(Debug)]
pub struct WriteThroughAcceleratedTableProvider {
    inner: Arc<AcceleratedTable>,
    accelerator: CayenneWriteTarget,
    federated: Arc<dyn TableProvider>,
}

impl WriteThroughAcceleratedTableProvider {
    pub fn try_new(inner: Arc<AcceleratedTable>) -> Result<Self, DataFusionError> {
        let accelerator_provider = inner.get_accelerator();
        let accelerator = extract_cayenne_write_target(&accelerator_provider).ok_or_else(|| {
            DataFusionError::Execution(
                "Write-through acceleration currently requires the Cayenne accelerator".to_string(),
            )
        })?;

        let federated = inner
            .get_federated_table_ref()
            .try_table_provider_sync()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Write-through acceleration requires an immediately available federated table provider"
                        .to_string(),
                )
            })?;

        Ok(Self {
            inner,
            accelerator,
            federated,
        })
    }

    #[must_use]
    pub fn inner(&self) -> &Arc<AcceleratedTable> {
        &self.inner
    }
}

#[async_trait]
impl TableProvider for WriteThroughAcceleratedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>>
    {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> datafusion::common::Result<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match overwrite {
            InsertOp::Append => Ok(Arc::new(DataSinkExec::new(
                input,
                Arc::new(WriteThroughDataSink::new(
                    Arc::clone(&self.inner),
                    self.accelerator.clone(),
                    Arc::clone(&self.federated),
                )),
                None,
            ))),
            InsertOp::Overwrite | InsertOp::Replace => Err(DataFusionError::Plan(
                "Write-through accelerated catalog tables currently support append writes only"
                    .to_string(),
            )),
        }
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }
}

struct WriteThroughDataSink {
    inner: Arc<AcceleratedTable>,
    accelerator: CayenneWriteTarget,
    federated: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl WriteThroughDataSink {
    fn new(
        inner: Arc<AcceleratedTable>,
        accelerator: CayenneWriteTarget,
        federated: Arc<dyn TableProvider>,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            accelerator,
            federated,
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
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        if let CayenneWriteTarget::Partitioned(accelerator) = &self.accelerator {
            return write_all_with_partitioned_cayenne(
                Arc::clone(&self.inner),
                Arc::clone(accelerator),
                Arc::clone(&self.federated),
                data,
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
                self.inner.refresher().set_initial_load_completed(true);
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
    inner: Arc<AcceleratedTable>,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<dyn TableProvider>,
    mut data: SendableRecordBatchStream,
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
                        let cayenne = partition_provider
                            .as_any()
                            .downcast_ref::<CayenneTableProvider>()
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
            inner.refresher().set_initial_load_completed(true);
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

fn extract_cayenne_write_target(
    table_provider: &Arc<dyn TableProvider>,
) -> Option<CayenneWriteTarget> {
    if let Some(cayenne) = table_provider
        .as_any()
        .downcast_ref::<CayenneTableProvider>()
    {
        return Some(CayenneWriteTarget::Staged(
            cayenne.clone_for_write_operations(),
        ));
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
) -> JoinHandle<datafusion::common::Result<CayenneStagedAppend>> {
    tokio::spawn(async move {
        let stream = RecordBatchStreamAdapter::new(schema, ReceiverStream::new(receiver));
        accelerator
            .begin_staged_append(Box::pin(stream))
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
