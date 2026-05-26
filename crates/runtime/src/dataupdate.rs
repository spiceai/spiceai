/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{any::Any, collections::HashMap, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::TryStreamExt;
pub use runtime_datafusion::query_engine::{DataUpdate, UpdateType};
use tokio::sync::{Mutex, RwLock, broadcast};

use datafusion::sql::TableReference;

const DATA_UPDATE_BROADCAST_CAPACITY: usize = 100;

#[derive(Clone, Debug, Default)]
pub struct DataUpdateBroadcaster {
    channels: Arc<RwLock<HashMap<TableReference, Arc<broadcast::Sender<DataUpdate>>>>>,
}

pub struct DataUpdateReceiver {
    broadcaster: DataUpdateBroadcaster,
    table_reference: TableReference,
    receiver: Option<broadcast::Receiver<DataUpdate>>,
}

impl DataUpdateReceiver {
    pub async fn recv(&mut self) -> Result<DataUpdate, broadcast::error::RecvError> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Err(broadcast::error::RecvError::Closed);
        };
        receiver.recv().await
    }
}

impl Drop for DataUpdateReceiver {
    fn drop(&mut self) {
        self.receiver.take();
        let broadcaster = self.broadcaster.clone();
        let table_reference = self.table_reference.clone();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let cleanup_task = handle.spawn(async move {
                broadcaster.prune_unused(&table_reference).await;
            });
            drop(cleanup_task);
        }
    }
}

impl DataUpdateBroadcaster {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe(&self, table_reference: &TableReference) -> DataUpdateReceiver {
        let receiver = if let Some(channel) = self.channels.read().await.get(table_reference) {
            channel.subscribe()
        } else {
            let mut channels = self.channels.write().await;
            channels
                .entry(table_reference.clone())
                .or_insert_with(|| {
                    let (sender, _) = broadcast::channel(DATA_UPDATE_BROADCAST_CAPACITY);
                    Arc::new(sender)
                })
                .subscribe()
        };

        DataUpdateReceiver {
            broadcaster: self.clone(),
            table_reference: table_reference.clone(),
            receiver: Some(receiver),
        }
    }

    pub async fn has_subscribers(&self, table_reference: &TableReference) -> bool {
        let Some(channel) = self.channels.read().await.get(table_reference).cloned() else {
            return false;
        };

        if channel.receiver_count() > 0 {
            return true;
        }

        self.remove_if_unused(table_reference, &channel).await;
        false
    }

    pub async fn publish(&self, table_reference: &TableReference, update: DataUpdate) {
        let Some(channel) = self.channels.read().await.get(table_reference).cloned() else {
            return;
        };

        if channel.receiver_count() == 0 {
            self.remove_if_unused(table_reference, &channel).await;
            return;
        }

        if let Err(err) = channel.send(update) {
            tracing::debug!(
                dataset = %table_reference,
                "No active DoExchange subscribers received data update: {err}"
            );
            self.remove_if_unused(table_reference, &channel).await;
        }
    }

    pub async fn close_subscribers(&self, table_reference: &TableReference) -> bool {
        self.channels
            .write()
            .await
            .remove(table_reference)
            .is_some_and(|sender| sender.receiver_count() > 0)
    }

    pub async fn prune_unused(&self, table_reference: &TableReference) -> bool {
        let Some(channel) = self.channels.read().await.get(table_reference).cloned() else {
            return false;
        };

        if channel.receiver_count() > 0 {
            return false;
        }

        self.remove_if_unused(table_reference, &channel).await;
        true
    }

    async fn remove_if_unused(
        &self,
        table_reference: &TableReference,
        channel: &Arc<broadcast::Sender<DataUpdate>>,
    ) {
        if channel.receiver_count() > 0 {
            return;
        }

        let mut channels = self.channels.write().await;
        if channels
            .get(table_reference)
            .is_some_and(|current| Arc::ptr_eq(current, channel) && current.receiver_count() == 0)
        {
            channels.remove(table_reference);
        }
    }
}

use crate::datafusion::error::find_datafusion_root;

pub struct StreamingDataUpdate {
    pub data: SendableRecordBatchStream,
    pub update_type: UpdateType,
}

impl StreamingDataUpdate {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, update_type: UpdateType) -> Self {
        Self { data, update_type }
    }

    pub async fn collect_data(self) -> Result<DataUpdate, DataFusionError> {
        let schema = self.data.schema();
        let data = self
            .data
            .try_collect::<Vec<_>>()
            .await
            .map_err(find_datafusion_root)?;
        Ok(DataUpdate {
            schema,
            data,
            update_type: self.update_type,
        })
    }
}

impl TryFrom<DataUpdate> for StreamingDataUpdate {
    type Error = DataFusionError;

    fn try_from(data_update: DataUpdate) -> std::result::Result<Self, Self::Error> {
        let data = Box::pin(
            MemoryStream::try_new(data_update.data, data_update.schema, None)
                .map_err(find_datafusion_root)?,
        ) as SendableRecordBatchStream;
        Ok(Self {
            data,
            update_type: data_update.update_type,
        })
    }
}

pub struct StreamingDataUpdateExecutionPlan {
    record_batch_stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl StreamingDataUpdateExecutionPlan {
    #[must_use]
    pub fn new(record_batch_stream: SendableRecordBatchStream) -> Self {
        let schema = record_batch_stream.schema();
        Self::new_with_stream(schema, Some(record_batch_stream))
    }

    #[must_use]
    pub fn new_empty(schema: SchemaRef) -> Self {
        Self::new_with_stream(schema, None)
    }

    fn new_with_stream(
        schema: SchemaRef,
        record_batch_stream: Option<SendableRecordBatchStream>,
    ) -> Self {
        Self {
            record_batch_stream: Arc::new(Mutex::new(record_batch_stream)),
            schema: Arc::clone(&schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }

    pub fn set_stream(
        &self,
        record_batch_stream: SendableRecordBatchStream,
    ) -> DataFusionResult<()> {
        let stream_schema = record_batch_stream.schema();
        if stream_schema.as_ref() != self.schema.as_ref() {
            return Err(DataFusionError::Execution(format!(
                "StreamingDataUpdateExecutionPlan stream schema mismatch: expected {:?}, got {:?}",
                self.schema, stream_schema
            )));
        }

        let mut stream = self.record_batch_stream.try_lock().map_err(|e| {
            DataFusionError::Execution(format!(
                "StreamingDataUpdateExecutionPlan is already executing: {e}"
            ))
        })?;

        if stream.is_some() {
            return Err(DataFusionError::Execution(
                "StreamingDataUpdateExecutionPlan stream has not been consumed".to_string(),
            ));
        }

        *stream = Some(record_batch_stream);
        Ok(())
    }

    pub fn clear_stream(&self) -> DataFusionResult<()> {
        let mut stream = self.record_batch_stream.try_lock().map_err(|e| {
            DataFusionError::Execution(format!(
                "StreamingDataUpdateExecutionPlan is already executing: {e}"
            ))
        })?;
        stream.take();
        Ok(())
    }
}

impl std::fmt::Debug for StreamingDataUpdateExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingDataUpdateExecutionPlan")
    }
}

impl DisplayAs for StreamingDataUpdateExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingDataUpdateExecutionPlan")
    }
}

impl ExecutionPlan for StreamingDataUpdateExecutionPlan {
    fn name(&self) -> &'static str {
        "StreamingDataUpdateExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        let mut stream = self.record_batch_stream.try_lock().map_err(|e| {
            DataFusionError::Execution(format!(
                "StreamingDataUpdateExecutionPlan is already executing: {e}"
            ))
        })?;

        stream.take().ok_or_else(|| {
            DataFusionError::Execution(
                "StreamingDataUpdateExecutionPlan stream was already consumed".to_string(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::sql::TableReference;

    fn one_column_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int32Array::from(values))])
            .expect("batch should be valid")
    }

    fn one_batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
        ))
    }

    async fn collect_i32_values(plan: Arc<StreamingDataUpdateExecutionPlan>) -> Vec<i32> {
        let exec: Arc<dyn ExecutionPlan> = plan;
        let batches = collect(exec, Arc::new(TaskContext::default()))
            .await
            .expect("collect should succeed");
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 column");
                (0..values.len())
                    .map(|idx| values.value(idx))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn streaming_execution_plan_can_be_refilled_after_consumption() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let plan = Arc::new(StreamingDataUpdateExecutionPlan::new_empty(Arc::clone(
            &schema,
        )));

        plan.set_stream(one_batch_stream(one_column_batch(&schema, vec![1, 2])))
            .expect("first stream should be accepted");
        assert_eq!(collect_i32_values(Arc::clone(&plan)).await, vec![1, 2]);
        plan.clear_stream()
            .expect("consumed stream slot should clear");

        plan.set_stream(one_batch_stream(one_column_batch(&schema, vec![3, 4, 5])))
            .expect("second stream should be accepted");
        assert_eq!(collect_i32_values(Arc::clone(&plan)).await, vec![3, 4, 5]);
        plan.clear_stream()
            .expect("second consumed stream slot should clear");
    }

    #[tokio::test]
    async fn data_update_broadcaster_delivers_published_updates() {
        let broadcaster = DataUpdateBroadcaster::new();
        let table_reference = TableReference::bare("cdc_table");
        let mut receiver = broadcaster.subscribe(&table_reference).await;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        broadcaster
            .publish(
                &table_reference,
                DataUpdate {
                    schema: Arc::clone(&schema),
                    data: vec![],
                    update_type: UpdateType::Append,
                },
            )
            .await;

        let update = receiver
            .recv()
            .await
            .expect("published update should be received");
        assert_eq!(update.schema, schema);
        assert!(matches!(update.update_type, UpdateType::Append));
    }

    #[tokio::test]
    async fn data_update_broadcaster_prunes_channels_without_subscribers() {
        let broadcaster = DataUpdateBroadcaster::new();
        let table_reference = TableReference::bare("cdc_table");
        let receiver = broadcaster.subscribe(&table_reference).await;
        drop(receiver);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if broadcaster.channels.read().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped receiver should prune the channel");
    }

    #[tokio::test]
    async fn data_update_broadcaster_close_subscribers_closes_receivers() {
        let broadcaster = DataUpdateBroadcaster::new();
        let table_reference = TableReference::bare("cdc_table");
        let mut receiver = broadcaster.subscribe(&table_reference).await;

        assert!(broadcaster.close_subscribers(&table_reference).await);
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(broadcaster.channels.read().await.is_empty());
    }
}
