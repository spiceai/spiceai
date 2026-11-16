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

use std::{collections::HashMap, fmt, sync::Arc};

use arrow::array::RecordBatch;
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
pub use runtime_query_engine::query_engine::{DataUpdate, UpdateType};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, broadcast};

use datafusion::common::TableReference;

const DATA_UPDATE_BROADCAST_CAPACITY: usize = 100;

/// A published update and the commit sequence number it was published under.
///
/// Sequence numbers count the commits published for one table by one
/// broadcaster, which lives as long as the runtime process. Together with the
/// broadcaster's [`epoch`](DataUpdateBroadcaster::epoch) they let an append-stream
/// subscriber tell which updates its snapshot already holds.
#[derive(Clone, Debug)]
pub struct SequencedDataUpdate {
    pub sequence: u64,
    pub update: DataUpdate,
}

type CommitFence = Arc<RwLock<u64>>;

#[derive(Clone, Debug)]
pub struct DataUpdateBroadcaster {
    channels: Arc<RwLock<HashMap<TableReference, Arc<broadcast::Sender<SequencedDataUpdate>>>>>,
    /// Per table, the sequence number of the last published commit, behind a
    /// lock that orders commits against subscriber snapshots: a fenced commit
    /// holds it for writing from before its write until after its publish, and
    /// a subscriber holds it for reading while it subscribes and opens its
    /// snapshot. So a subscriber's snapshot holds exactly the fenced commits up
    /// to the sequence number it read, and it receives every later one.
    fences: Arc<parking_lot::Mutex<HashMap<TableReference, CommitFence>>>,
    epoch: Arc<str>,
}

impl Default for DataUpdateBroadcaster {
    fn default() -> Self {
        Self {
            channels: Arc::default(),
            fences: Arc::default(),
            epoch: Arc::from(uuid::Uuid::now_v7().simple().to_string()),
        }
    }
}

/// Held by a writer from before a commit until it has published it; see
/// [`DataUpdateBroadcaster::begin_commit`].
pub struct CommitGuard {
    sequence: OwnedRwLockWriteGuard<u64>,
}

/// A subscription opened with [`DataUpdateBroadcaster::subscribe_at_snapshot`].
/// Fenced commits wait until it is dropped, so open the snapshot before
/// dropping it.
pub struct SnapshotFence {
    _sequence: OwnedRwLockReadGuard<u64>,
    /// The last commit published before the snapshot.
    pub sequence: u64,
}

pub struct DataUpdateReceiver {
    broadcaster: DataUpdateBroadcaster,
    table_reference: TableReference,
    receiver: Option<broadcast::Receiver<SequencedDataUpdate>>,
}

impl DataUpdateReceiver {
    /// Waits for the next update on the channel.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] if no sender remains, or if this receiver was
    /// built without one; [`RecvError::Lagged`] if updates were dropped because
    /// this receiver fell behind.
    ///
    /// [`RecvError::Closed`]: broadcast::error::RecvError::Closed
    /// [`RecvError::Lagged`]: broadcast::error::RecvError::Lagged
    pub async fn recv(&mut self) -> Result<DataUpdate, broadcast::error::RecvError> {
        self.recv_sequenced()
            .await
            .map(|sequenced| sequenced.update)
    }

    /// [`Self::recv`], with the commit sequence number of the update.
    ///
    /// # Errors
    ///
    /// The same as [`Self::recv`].
    pub async fn recv_sequenced(
        &mut self,
    ) -> Result<SequencedDataUpdate, broadcast::error::RecvError> {
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

    /// Identifies this broadcaster, so this runtime process: sequence numbers
    /// from different epochs are not comparable.
    #[must_use]
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    fn fence(&self, table_reference: &TableReference) -> CommitFence {
        Arc::clone(
            self.fences
                .lock()
                .entry(table_reference.clone())
                .or_default(),
        )
    }

    /// Starts a fenced commit to `table_reference`: hold the guard while the
    /// write runs and pass it to [`Self::publish_committed`]. A subscriber that
    /// arrives meanwhile waits, then reads a snapshot that holds the commit.
    pub async fn begin_commit(&self, table_reference: &TableReference) -> CommitGuard {
        CommitGuard {
            sequence: self.fence(table_reference).write_owned().await,
        }
    }

    /// Subscribes to `table_reference` and reads the sequence number of its
    /// last published commit, both while no fenced commit is running.
    pub async fn subscribe_at_snapshot(
        &self,
        table_reference: &TableReference,
    ) -> (DataUpdateReceiver, SnapshotFence) {
        let sequence = self.fence(table_reference).read_owned().await;
        let receiver = self.subscribe(table_reference).await;
        let fence = SnapshotFence {
            sequence: *sequence,
            _sequence: sequence,
        };
        (receiver, fence)
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

    /// Publishes an update whose write was not fenced; it takes the next
    /// sequence number once the write is done.
    pub async fn publish(&self, table_reference: &TableReference, update: DataUpdate) {
        let guard = self.begin_commit(table_reference).await;
        self.publish_committed(table_reference, guard, update).await;
    }

    /// Publishes the update a fenced commit wrote, under the commit's next
    /// sequence number, and ends the commit.
    pub async fn publish_committed(
        &self,
        table_reference: &TableReference,
        mut guard: CommitGuard,
        update: DataUpdate,
    ) {
        let Some(channel) = self.channels.read().await.get(table_reference).cloned() else {
            return;
        };

        if channel.receiver_count() == 0 {
            self.remove_if_unused(table_reference, &channel).await;
            return;
        }

        *guard.sequence += 1;
        let update = SequencedDataUpdate {
            sequence: *guard.sequence,
            update,
        };
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
        channel: &Arc<broadcast::Sender<SequencedDataUpdate>>,
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

/// Most bytes of appended rows a refresh holds to forward to `DoExchange`
/// subscribers after its write commits. The same bound a subscriber applies to
/// updates it buffers while streaming its initial snapshot.
pub const MAX_PUBLISHED_REFRESH_APPEND_BYTES: usize = 128 * 1024 * 1024;

/// Forwards the rows an accelerated refresh appends to the `DoExchange`
/// subscribers of one dataset, so an append stream delivers refresh-driven
/// appends as well as `DoPut` writes.
#[derive(Clone, Debug)]
pub struct DataUpdatePublisher {
    broadcaster: DataUpdateBroadcaster,
    table_reference: TableReference,
}

impl DataUpdatePublisher {
    #[must_use]
    pub fn new(broadcaster: DataUpdateBroadcaster, table_reference: TableReference) -> Self {
        Self {
            broadcaster,
            table_reference,
        }
    }

    #[must_use]
    pub fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }

    pub async fn has_subscribers(&self) -> bool {
        self.broadcaster
            .has_subscribers(&self.table_reference)
            .await
    }

    /// Starts a fenced commit to the dataset; see
    /// [`DataUpdateBroadcaster::begin_commit`].
    pub async fn begin_commit(&self) -> CommitGuard {
        self.broadcaster.begin_commit(&self.table_reference).await
    }

    /// Sends appended rows to every current subscriber under the commit's
    /// sequence number, and ends the commit. Call it only once the rows are
    /// committed, so a subscriber never receives rows a reader of the dataset
    /// cannot see.
    pub async fn publish_append(
        &self,
        guard: CommitGuard,
        schema: SchemaRef,
        data: Vec<RecordBatch>,
    ) {
        let data: Vec<RecordBatch> = data
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        if data.is_empty() {
            return;
        }
        self.broadcaster
            .publish_committed(
                &self.table_reference,
                guard,
                DataUpdate {
                    schema,
                    data,
                    update_type: UpdateType::Append,
                },
            )
            .await;
    }

    /// Ends every current subscription with a data-loss error, for an append
    /// too large to forward. A subscriber then resubscribes and takes a fresh
    /// snapshot instead of silently missing the rows.
    pub async fn close_subscribers(&self) -> bool {
        self.broadcaster
            .close_subscribers(&self.table_reference)
            .await
    }
}

use runtime_datafusion::error::find_datafusion_root;

pub struct StreamingDataUpdate {
    pub data: SendableRecordBatchStream,
    pub update_type: UpdateType,
    /// How the accelerator should order the copies of a key this update repeats,
    /// when it resolves them after writing; see
    /// [`util::session_state::RowVersions`]. A wrapper that rebuilds the update
    /// must carry it forward.
    pub row_versions: Option<Arc<dyn util::session_state::RowVersions>>,
    /// The rows the refresh already counted as superseded before the write, when it
    /// selects them itself; the accelerator then counts none of its own.
    pub superseded: Option<Arc<util::session_state::SupersededRows>>,
}

impl StreamingDataUpdate {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, update_type: UpdateType) -> Self {
        Self {
            data,
            update_type,
            row_versions: None,
            superseded: None,
        }
    }

    /// This update with the rows the refresh counted as superseded before the write,
    /// so the accelerator does not count them again.
    #[must_use]
    pub fn superseded_counted_before_write(
        mut self,
        superseded: Arc<util::session_state::SupersededRows>,
    ) -> Self {
        self.superseded = Some(superseded);
        self
    }

    /// This update with the row versions its writer supplies.
    #[must_use]
    pub fn with_row_versions(
        mut self,
        row_versions: Option<Arc<dyn util::session_state::RowVersions>>,
    ) -> Self {
        self.row_versions = row_versions;
        self
    }

    /// Drains the stream into an in-memory [`DataUpdate`].
    ///
    /// # Errors
    ///
    /// Returns the stream's own error if producing a batch fails.
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
        Ok(Self::new(data, data_update.update_type))
    }
}

pub struct StreamingDataUpdateExecutionPlan {
    record_batch_stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
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
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    /// Installs the stream this plan will emit on execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream's schema differs from the plan's, or if the
    /// plan is already executing and the stream slot is locked.
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

    /// Drops any installed stream, so the plan can be reused.
    ///
    /// # Errors
    ///
    /// Returns an error if the plan is already executing and the stream slot is
    /// locked.
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

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
    use arrow::record_batch::RecordBatch;
    use datafusion::common::TableReference;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

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

        // The subscription registered exactly this table's channel, so the
        // pruning asserted below has a registered channel to remove.
        {
            let channels = broadcaster.channels.read().await;
            assert_eq!(channels.len(), 1);
            let sender = channels
                .get(&table_reference)
                .expect("subscribe registers the table's channel");
            assert_eq!(sender.receiver_count(), 1);
        }
        drop(receiver);

        let pruned = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if broadcaster.channels.read().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let remaining: Vec<TableReference> =
            broadcaster.channels.read().await.keys().cloned().collect();
        assert!(
            remaining.is_empty(),
            "dropped receiver should prune the channel within 1s; still registered: {remaining:?}"
        );
        pruned.expect("the prune poll ends as soon as no channel is registered");
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
