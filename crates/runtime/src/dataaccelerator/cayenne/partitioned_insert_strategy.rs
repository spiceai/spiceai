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

//! Cross-partition atomic commit coordinator for Cayenne.
//!
//! Implements [`InsertStrategy`] for partitioned Cayenne tables (issue
//! #10125). For overwrite-mode inserts, every participating partition's
//! catalog mutation is batched into a single [`MetastoreTransaction`] so the
//! `current_snapshot_id` pointer flips happen atomically — either every
//! partition advances or none do. Append-mode inserts fall through to
//! [`DefaultInsertStrategy`] (handled by the per-partition write path).
//!
//! ## Overwrite coordination flow
//!
//! 1. **Stage** (parallel-safe per partition): partition the input stream by
//!    partition key. For each unique key seen, call
//!    [`CayenneTableProvider::begin_overwrite`] which writes data into a
//!    fresh `<table_id>/<new_snapshot>/` directory and returns a
//!    [`PreparedOverwrite`] receipt.
//! 2. **Apply** (single shared transaction): open one transaction on the
//!    shared `CayenneCatalog`. For every receipt, call
//!    [`PreparedOverwrite::apply_in_txn`] inside that transaction.
//!    Commit the transaction once.
//! 3. **Finish** (parallel-safe per partition): for every receipt, run
//!    [`PreparedOverwrite::finish`] to publish the new snapshot in-memory and
//!    trigger old-snapshot GC.
//!
//! Failure at step 2 rolls back every prepared receipt (best-effort cleanup
//! of the staged snapshot directories). Failure at step 3 is logged but does
//! not roll back — the catalog has already committed, so readers see the new
//! state via the next scan.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne::{CayenneCatalog, CayenneTableProvider, PreparedOverwrite};
use datafusion::common::{Column, DFSchema};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, dml::InsertOp};
use futures::StreamExt;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::encode_composite_key;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::insert::{
    DefaultInsertStrategy, InsertStrategy, PartitionContext, partition_batch_composite,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

/// Per-partition key type used by `PartitionTableProvider`. The
/// runtime-table-partition crate keeps the concrete alias `pub(crate)`; for
/// our local map we just use the underlying `String`.
type CompositePartitionKey = String;

/// Cross-partition atomic-commit strategy for Cayenne. See module-level
/// documentation for the overwrite coordination flow.
pub struct CayennePartitionedInsertStrategy {
    catalog: Arc<CayenneCatalog>,
    /// Serializes cross-partition overwrite coordinators on this table.
    ///
    /// One coordinator may have many concurrent per-partition writer tasks
    /// in flight (each holding its own partition's write lock via
    /// `begin_overwrite`), but two coordinators on the same table acquire
    /// per-partition locks in input-stream order — that order is not
    /// guaranteed consistent across coordinators, so concurrent coordinators
    /// on overlapping partition sets could deadlock under lock-ordering.
    /// Serializing the coordinators here eliminates that hazard. Within one
    /// coordinator, every partition's writer task runs in parallel, so the
    /// outer serialization does not bottleneck a single refresh.
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for CayennePartitionedInsertStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedInsertStrategy")
            .finish_non_exhaustive()
    }
}

impl CayennePartitionedInsertStrategy {
    /// Construct a new strategy. `catalog` must be the same
    /// `CayenneCatalog` that the partition creator was built against.
    #[must_use]
    pub fn new(catalog: Arc<CayenneCatalog>) -> Self {
        Self {
            catalog,
            coordinator_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

#[async_trait]
impl InsertStrategy for CayennePartitionedInsertStrategy {
    async fn execute_insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
        context: &PartitionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match insert_op {
            InsertOp::Overwrite => {
                let physical_exprs =
                    create_partition_physical_exprs(&context.partition_by, Arc::clone(&context.schema))?;
                let sink = Arc::new(CayennePartitionedOverwriteSink {
                    catalog: Arc::clone(&self.catalog),
                    coordinator_lock: Arc::clone(&self.coordinator_lock),
                    creator: Arc::clone(&context.creator),
                    partitions: Arc::clone(&context.partitions),
                    schema: Arc::clone(&context.schema),
                    physical_exprs,
                });
                Ok(Arc::new(DataSinkExec::new(input, sink, None)))
            }
            // Append + Replace continue through the default per-partition
            // path; cross-partition append atomicity is a separate roadmap
            // step (#10125 step 6).
            _ => {
                DefaultInsertStrategy
                    .execute_insert(input, insert_op, context)
                    .await
            }
        }
    }
}

/// DataSink that fans the input stream out by partition key, stages every
/// partition's overwrite, and commits all of them in one shared
/// `MetastoreTransaction`.
///
/// Streams batches to per-partition writer tasks via `mpsc` channels so
/// steady-state memory is bounded by channel capacity × partition count,
/// not by total input size.
struct CayennePartitionedOverwriteSink {
    catalog: Arc<CayenneCatalog>,
    /// See [`CayennePartitionedInsertStrategy::coordinator_lock`].
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions:
        Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

/// Per-partition writer-task channel depth. Bounds in-flight backpressure on
/// the slowest partition; larger values trade memory for throughput tolerance
/// when partitions write at different speeds. Matches the value used by
/// `accelerated_table::write::write_through.rs` for the same shape.
const PARTITION_WRITER_CHANNEL_DEPTH: usize = 8;

impl std::fmt::Debug for CayennePartitionedOverwriteSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedOverwriteSink")
            .field("partition_exprs", &self.physical_exprs.len())
            .finish()
    }
}

impl DisplayAs for CayennePartitionedOverwriteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayennePartitionedOverwriteSink")
    }
}

#[async_trait]
impl DataSink for CayennePartitionedOverwriteSink {
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
        // Serialize cross-partition coordinators on this table so concurrent
        // coordinators on overlapping partition sets can't deadlock on
        // per-partition lock-acquisition order. Within this coordinator,
        // per-partition writer tasks run in parallel.
        let _coordinator_guard = self.coordinator_lock.lock().await;

        // Step 1: route each input batch to its partition's writer task.
        // On first-seen partition, spawn a `tokio::task` that calls
        // `begin_overwrite` on a `RecordBatchStreamAdapter` around an
        // mpsc::Receiver. Subsequent batches for that partition are pushed
        // through the sender, providing natural backpressure (capacity
        // PARTITION_WRITER_CHANNEL_DEPTH). Steady-state memory is
        // O(channel_depth × partition_count), not O(input_size).
        let mut senders: HashMap<
            String,
            mpsc::Sender<datafusion::common::Result<RecordBatch>>,
        > = HashMap::new();
        let mut handles: Vec<JoinHandle<cayenne::provider::Result<PreparedOverwrite>>> =
            Vec::new();
        let mut upstream_err: Option<DataFusionError> = None;

        'outer: while let Some(batch_result) = data.next().await {
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) => {
                    upstream_err = Some(e);
                    break 'outer;
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let partitioned = partition_batch_composite(&batch, &self.physical_exprs)?;
            for (key, (values, batch_part)) in partitioned {
                let sender = if let Some(s) = senders.get(&key) {
                    s.clone()
                } else {
                    // First batch for this partition: resolve the per-partition
                    // CayenneTableProvider and spawn its writer task.
                    let provider = self.get_or_create_partition_provider(values).await?;
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
                        let stream: SendableRecordBatchStream = Box::pin(
                            RecordBatchStreamAdapter::new(
                                schema_clone,
                                ReceiverStream::new(rx),
                            ),
                        );
                        cayenne_owned.begin_overwrite(stream).await
                    });
                    senders.insert(key.clone(), tx.clone());
                    handles.push(handle);
                    tx
                };

                if sender.send(Ok(batch_part)).await.is_err() {
                    // Writer task terminated unexpectedly (channel closed).
                    // Stop sending; we'll surface the real error when we
                    // join the handle below.
                    upstream_err = Some(DataFusionError::Execution(
                        "partition writer task terminated before stream end"
                            .to_string(),
                    ));
                    break 'outer;
                }
            }
        }

        // If upstream errored mid-stream, propagate the error down every
        // open channel so each writer task observes the failure and returns
        // Err (rather than committing a truncated overwrite).
        if let Some(ref err) = upstream_err {
            let err_msg = err.to_string();
            for sender in senders.values() {
                let _ = sender
                    .send(Err(DataFusionError::Execution(format!(
                        "upstream stream terminated with error: {err_msg}"
                    ))))
                    .await;
            }
        }

        // Close all senders so writer streams terminate naturally.
        drop(senders);

        // Step 2: join every writer task, collecting prepared overwrites and
        // any task-side errors. We always await every task even if one fails,
        // so that no writer task leaks past `write_all`'s return.
        let mut prepared: Vec<PreparedOverwrite> = Vec::with_capacity(handles.len());
        let mut task_err: Option<DataFusionError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(prep)) => prepared.push(prep),
                Ok(Err(e)) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::from(e));
                    }
                }
                Err(panic_err) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::Execution(format!(
                            "partition writer task panicked: {panic_err}"
                        )));
                    }
                }
            }
        }

        if let Some(err) = upstream_err.or(task_err) {
            for prep in prepared {
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        "rollback after stream/writer error failed: {rollback_err}"
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
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        "rollback of prepared overwrite failed after txn error: {rollback_err}"
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
            match prep.finish().await {
                Ok(rows) => total_rows = total_rows.saturating_add(rows),
                Err(e) => {
                    tracing::warn!(
                        "finish() for prepared overwrite failed after txn commit: {e}; \
                         in-memory state will reconcile on next scan"
                    );
                }
            }
        }
        Ok(total_rows)
    }
}

impl CayennePartitionedOverwriteSink {
    /// Open one transaction, apply every partition's commit_compaction_in_txn,
    /// commit. Mirrors the retry-on-conflict shape of
    /// [`crate::cayenne::CayenneCatalog::commit_compaction`] but for the
    /// batched cross-partition case.
    async fn commit_in_one_txn(
        &self,
        prepared: &[PreparedOverwrite],
    ) -> datafusion::common::Result<()> {
        let mut txn = self
            .catalog
            .begin_transaction()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for prep in prepared {
            prep.apply_in_txn(&self.catalog, &mut *txn)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        txn.commit()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Resolve or create the partition provider for the given partition
    /// values. Mirrors `PartitionTableProvider::get_or_create_partition_provider`
    /// but operates on the `PartitionContext` directly so the sink doesn't
    /// need a back-reference to the provider.
    async fn get_or_create_partition_provider(
        &self,
        partition_values: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, DataFusionError> {
        let partition_key = encode_composite_key(&partition_values).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode partition key: {e}"))
        })?;

        let mut partitions_lock = self.partitions.write().await;
        if let Some(partition) = partitions_lock.get(&partition_key) {
            return Ok(Arc::clone(&partition.table_provider));
        }
        let partition = self
            .creator
            .create_partition(partition_values)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let provider = Arc::clone(&partition.table_provider);
        partitions_lock.insert(partition_key, partition);
        Ok(provider)
    }
}

/// Compile every partition-by expression into a physical expression that can
/// be evaluated against incoming `RecordBatch`es.
fn create_partition_physical_exprs(
    partition_by: &[PartitionedBy],
    schema: SchemaRef,
) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    let input_dfschema = DFSchema::try_from(schema)?;
    let execution_props = ExecutionProps::new();
    partition_by
        .iter()
        .map(|partitioned_by| {
            create_physical_expr(
                &Expr::Column(Column::new_unqualified(
                    match &partitioned_by.expression {
                        Expr::Column(c) => c.name.clone(),
                        other => other.to_string(),
                    },
                )),
                &input_dfschema,
                &execution_props,
            )
            // Fall through with the actual expression if the simple Column
            // shortcut fails (e.g. a derived partition expression).
            .or_else(|_| {
                create_physical_expr(
                    &partitioned_by.expression,
                    &input_dfschema,
                    &execution_props,
                )
            })
        })
        .collect()
}

/// Try to downcast a partition's table provider to a `CayenneTableProvider`.
///
/// The provider returned by `CayennePartitionCreator` is always a
/// `CayenneTableProvider`, so the downcast succeeds in practice; the
/// `Option` return covers the case where a non-Cayenne creator is
/// accidentally wired into this strategy (which should fail loudly).
fn downcast_to_cayenne(
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
) -> Option<&CayenneTableProvider> {
    provider.as_any().downcast_ref::<CayenneTableProvider>()
}
