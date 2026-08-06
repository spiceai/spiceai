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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne::{CayenneCatalog, PreparedOverwrite};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::sink::DataSink;
use futures::StreamExt;
use runtime_table_partition::Partition;
use runtime_table_partition::insert::partition_batch_composite;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use super::{
    CompositePartitionKey, PARTITION_WRITER_CHANNEL_DEPTH, WriteFanoutFailure, downcast_to_cayenne,
    get_or_create_partition_provider, join_writer_handles, poison_open_writer_channels,
    resolve_write_error,
};

/// `DataSink` that fans the input stream out by partition key, stages every
/// partition's overwrite, and commits all of them in one shared
/// `MetastoreTransaction`.
///
/// Streams batches to per-partition writer tasks via `mpsc` channels so
/// steady-state memory is bounded by channel capacity × partition count,
/// not by total input size.
///
/// ## Coordination flow
///
/// 1. **Stage** (parallel-safe per partition): partition the input stream by
///    partition key. For each unique key seen, spawn a writer task that
///    streams batches into `CayenneTableProvider::begin_overwrite`, which
///    writes data into a fresh `<table_id>/<new_snapshot>/` directory and
///    returns a [`PreparedOverwrite`] receipt.
/// 2. **Apply** (single shared transaction): open one transaction on the
///    shared [`CayenneCatalog`]. For every receipt, call
///    `PreparedOverwrite::apply_in_txn` inside that transaction. Commit
///    once. Retries on transient `SQLITE_BUSY` / Turso `BEGIN CONCURRENT`
///    conflicts with bounded backoff.
/// 3. **Finish** (parallel-safe per partition): for every receipt, run
///    `PreparedOverwrite::finish` to publish the new snapshot in-memory and
///    trigger old-snapshot GC.
///
/// Failure at step 2 rolls back every prepared receipt (best-effort cleanup
/// of the staged snapshot directories). Failure at step 3 is logged but does
/// not roll back — the catalog has already committed, so readers see the new
/// state via the next scan.
pub(super) struct CayennePartitionedOverwriteSink {
    catalog: Arc<CayenneCatalog>,
    /// See [`super::CayennePartitionedInsertStrategy::coordinator_lock`].
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for CayennePartitionedOverwriteSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedOverwriteSink")
            .field("partition_exprs", &self.physical_exprs.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayennePartitionedOverwriteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayennePartitionedOverwriteSink")
    }
}

#[async_trait]
impl DataSink for CayennePartitionedOverwriteSink {
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
        // Serialize cross-partition coordinators on this table so concurrent
        // coordinators on overlapping partition sets can't deadlock on
        // per-partition lock-acquisition order. Within this coordinator,
        // per-partition writer tasks run in parallel.
        let _coordinator_guard = self.coordinator_lock.lock().await;

        // Each per-partition writer fan-outs across `target_partitions` Vortex
        // file writers; the session config drives that count to match the
        // rest of the query (see PR #10822).
        let target_partitions = context.session_config().target_partitions();

        // Step 1: route each input batch to its partition's writer task.
        // On first-seen partition, spawn a `tokio::task` that calls
        // `begin_overwrite` on a `RecordBatchStreamAdapter` around an
        // mpsc::Receiver. Subsequent batches for that partition are pushed
        // through the sender, providing natural backpressure (capacity
        // PARTITION_WRITER_CHANNEL_DEPTH). Steady-state memory is
        // O(channel_depth × partition_count), not O(input_size).
        let mut senders: HashMap<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>> =
            HashMap::new();
        let mut handles: Vec<JoinHandle<cayenne::provider::Result<PreparedOverwrite>>> = Vec::new();
        let mut fanout_failure: Option<WriteFanoutFailure> = None;

        'outer: while let Some(batch_result) = data.next().await {
            let batch = match batch_result {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => batch,
                Err(e) => {
                    fanout_failure = Some(WriteFanoutFailure::Upstream(e));
                    break 'outer;
                }
            };

            for (partition_key, (partition_values, partition_rb)) in
                partition_batch_composite(&batch, &self.physical_exprs)?
            {
                let sender = if let Some(s) = senders.get(&partition_key) {
                    s.clone()
                } else {
                    let (handle, tx) = self
                        .prepare_new_provider_for_partition(partition_values, target_partitions)
                        .await?;
                    senders.insert(partition_key.clone(), tx.clone());
                    handles.push(handle);
                    tx
                };

                if sender.send(Ok(partition_rb)).await.is_err() {
                    // This partition's writer task already returned before
                    // the coordinator finished feeding it — its own
                    // `Err`/panic (surfaced when we join below) is the real
                    // cause, not a placeholder.
                    fanout_failure = Some(WriteFanoutFailure::WriterChannelClosed);
                    break 'outer;
                }
            }
        }

        // If the fan-out stopped early, propagate a poison pill down every
        // still-open channel so each writer task observes the failure and
        // returns Err (rather than committing a truncated overwrite).
        if let Some(ref failure) = fanout_failure {
            poison_open_writer_channels(&senders, failure).await;
        }

        // Close all senders so writer streams terminate naturally.
        drop(senders);

        // Step 2: join every writer task, collecting prepared overwrites and
        // any task-side errors. We always await every task even if one fails,
        // so that no writer task leaks past `write_all`'s return.
        let (prepared, genuine_task_err, poisoned_task_err) = join_writer_handles(handles).await;

        if let Some(err) = resolve_write_error(fanout_failure, genuine_task_err, poisoned_task_err)
        {
            for prep in prepared {
                let table_id = prep.table_id().to_string();
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        table_id,
                        %rollback_err,
                        "Failed to roll back a partition's write after a write error elsewhere in the batch"
                    );
                }
            }
            return Err(err);
        }

        if prepared.is_empty() {
            return Ok(0);
        }

        // Step 3: catalog transaction. Open once, apply every partition's
        // mutation, commit once. If any apply fails, roll back the prepared
        // overwrites (cleanup of the staged snapshot directories) and return
        // the error; the txn is auto-rolled-back when its handle drops.
        if let Err(err) = self.commit_in_one_txn(&prepared).await {
            for prep in prepared {
                let table_id = prep.table_id().to_string();
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        table_id,
                        %rollback_err,
                        "Failed to roll back a partition's write after the multi-partition commit failed"
                    );
                }
            }
            return Err(err);
        }

        // Step 4: per-partition in-memory finish (snapshot id, listing
        // table, deletion caches, GC trigger). Failures here are logged but
        // do not roll back — the catalog has already committed, so readers
        // see the new state via the next scan.
        let mut total_rows: u64 = 0;
        for prep in prepared {
            let table_id = prep.table_id().to_string();
            match prep.finish().await {
                Ok(rows) => total_rows = total_rows.saturating_add(rows),
                Err(error) => {
                    tracing::warn!(
                        table_id,
                        %error,
                        "Failed to update a partition's in-memory state after its write was \
                         committed; it will catch up automatically the next time this table is queried"
                    );
                }
            }
        }
        Ok(total_rows)
    }
}

impl CayennePartitionedOverwriteSink {
    pub(super) fn new(
        catalog: Arc<CayenneCatalog>,
        coordinator_lock: Arc<tokio::sync::Mutex<()>>,
        creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
        partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
        schema: SchemaRef,
        physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            catalog,
            coordinator_lock,
            creator,
            partitions,
            schema,
            physical_exprs,
        }
    }

    /// Open one transaction, apply every partition's `commit_compaction_in_txn`,
    /// commit. Mirrors the retry-on-conflict shape of
    /// [`cayenne::MetadataCatalog::commit_compaction`] but for the batched
    /// cross-partition case.
    ///
    /// Retries on `SQLITE_BUSY` / `SQLITE_LOCKED` (and the equivalent Turso
    /// `BEGIN CONCURRENT` write-conflict at commit time). Each retry opens a
    /// fresh transaction and re-runs every `PreparedOverwrite::apply_in_txn`
    /// — the prepared receipts are immutable (data already on disk in their
    /// new snapshot directories), so re-applying their catalog mutations is
    /// safe and idempotent.
    async fn commit_in_one_txn(
        &self,
        prepared: &[PreparedOverwrite],
    ) -> datafusion::common::Result<()> {
        let max_attempts = turso_shared::DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        for attempt in 1..=max_attempts {
            let mut txn = self
                .catalog
                .begin_transaction()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut apply_err: Option<cayenne::CatalogError> = None;
            for prep in prepared {
                if let Err(e) = prep.apply_in_txn(&self.catalog, &mut *txn).await {
                    apply_err = Some(e);
                    break;
                }
            }
            if let Some(e) = apply_err {
                // Drop the transaction (auto-rollback). Retry if the failure
                // looks transient.
                drop(txn);
                if attempt < max_attempts && cayenne::is_retryable_write_conflict(&e) {
                    let delay = turso_shared::retry_backoff_delay(attempt);
                    tracing::debug!(
                        attempt,
                        max_attempts,
                        ?delay,
                        "Retrying the multi-partition commit after a conflicting write"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(DataFusionError::External(Box::new(e)));
            }

            match txn.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < max_attempts && cayenne::is_retryable_write_conflict(&e) => {
                    let delay = turso_shared::retry_backoff_delay(attempt);
                    tracing::debug!(
                        attempt,
                        max_attempts,
                        ?delay,
                        "Retrying the multi-partition commit after a conflicting commit"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }
        }

        Err(DataFusionError::Execution(format!(
            "Failed to commit this write across all its partitions after {max_attempts} attempts \
             due to repeated conflicting writes; retry the write"
        )))
    }

    /// Resolve the per-partition [`cayenne::CayenneTableProvider`] and spawn
    /// its writer task.
    async fn prepare_new_provider_for_partition(
        &self,
        partition_values: Vec<ScalarValue>,
        target_partitions: usize,
    ) -> Result<
        (
            JoinHandle<cayenne::provider::Result<PreparedOverwrite>>,
            mpsc::Sender<datafusion::common::Result<RecordBatch>>,
        ),
        DataFusionError,
    > {
        let provider =
            get_or_create_partition_provider(&self.creator, &self.partitions, partition_values)
                .await?;
        let cayenne = downcast_to_cayenne(&provider).ok_or_else(|| {
            DataFusionError::Execution(
                "CayennePartitionedInsertStrategy expects every partition's table \
                 provider to be a CayenneTableProvider"
                    .to_string(),
            )
        })?;

        let cayenne_owned = cayenne.clone_for_write_operations();
        let (tx, rx) = mpsc::channel::<datafusion::common::Result<RecordBatch>>(
            PARTITION_WRITER_CHANNEL_DEPTH,
        );
        let schema_clone = Arc::clone(&self.schema);
        let handle = tokio::spawn(async move {
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema_clone,
                ReceiverStream::new(rx),
            ));
            cayenne_owned
                .begin_overwrite(stream, target_partitions)
                .await
        });
        Ok((handle, tx))
    }
}
