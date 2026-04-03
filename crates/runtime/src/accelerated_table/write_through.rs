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
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use cayenne::{CayenneStagedAppend, CayenneTableProvider};
use data_components::poly::PolyTableProvider;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::{Constraints, DataFusionError, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
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
use crate::dataupdate::{
    DataUpdate, StreamingDataUpdate, StreamingDataUpdateExecutionPlan, UpdateType,
};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
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
    let schema = data.schema();
    let (source_tx, source_rx) = mpsc::channel(8);
    let source_task =
        spawn_federated_insert(Arc::clone(&federated), Arc::clone(&schema), source_rx);

    let mut upstream_error: Option<DataFusionError> = None;
    let mut buffered_batches: Vec<RecordBatch> = Vec::new();
    let mut row_count = 0_u64;

    while let Some(batch_result) = data.next().await {
        match batch_result {
            Ok(batch) => {
                row_count += batch.num_rows() as u64;
                buffered_batches.push(batch.clone());
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
                upstream_error = Some(DataFusionError::Execution(message));
                break;
            }
        }
    }

    drop(source_tx);

    let source_result = join_source_task(source_task).await;
    if let Some(error) = upstream_error {
        return Err(error);
    }
    source_result?;

    append_to_accelerator(accelerator, schema, buffered_batches).await?;
    inner.refresher().set_initial_load_completed(true);
    Ok(row_count)
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

async fn append_to_accelerator(
    accelerator: Arc<dyn TableProvider>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> datafusion::common::Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let ctx = SessionContext::new();
    let streaming_update = StreamingDataUpdate::try_from(DataUpdate {
        schema,
        data: batches,
        update_type: UpdateType::Append,
    })?;
    let input: Arc<dyn ExecutionPlan> = Arc::new(SchemaCastScanExec::new(
        Arc::new(StreamingDataUpdateExecutionPlan::new(streaming_update.data)),
        accelerator.schema(),
    ));

    let insert_plan = accelerator
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await?;
    let _ = datafusion::physical_plan::collect(insert_plan, ctx.task_ctx()).await?;
    Ok(())
}

fn extract_cayenne_write_target(
    table_provider: &Arc<dyn TableProvider>,
) -> Option<CayenneWriteTarget> {
    let type_id = table_provider.as_any().type_id();
    tracing::debug!(
        ?type_id,
        "extract_cayenne_write_target: inspecting provider"
    );

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
