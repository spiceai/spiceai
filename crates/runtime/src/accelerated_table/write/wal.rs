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

//! Write-Ahead Log (WAL) for durable `write_back` acceleration.
//!
//! Once a write is ACK'd to the client it will eventually reach the federated source
//! even across Spice process restarts. The WAL entry and the data change are committed
//! atomically in a single accelerator transaction; a background worker delivers
//! undelivered entries to the federated source with retry. On startup, any entries
//! that were written but not yet delivered are replayed automatically.
//!
//! Engine support is declared by implementing [`WalBackend`]. Currently `DuckDB`
//! (file mode) is supported. `SQLite` and Cayenne can be added by implementing
//! the trait and overriding `DataAccelerator::wal_backend`.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::ipc;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink};
use data_components::update::{UpdateExec, UpdateSink};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::{SessionContext, lit};
use datafusion::scalar::ScalarValue;

use arrow::compute::concat_batches;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use util::stream_utils::StreamingExec;

use crate::accelerated_table::refresh::Refresher;
use crate::federated_table::FederatedTable;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ── WAL entry ────────────────────────────────────────────────────────────────

/// The operation kind stored in a WAL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOp {
    Insert,
    Update,
    Delete,
}

impl WalOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// A single WAL entry read from the backend's pending queue.
pub struct WalEntry {
    pub seq: i64,
    /// Groups all chunks of one client DML operation — used for atomic delivery.
    pub txn_id: i64,
    pub op: WalOp,
    pub pks_ipc: Vec<u8>,
    pub new_values: Option<Vec<u8>>,
}

// ── WalBackend trait ─────────────────────────────────────────────────────────

/// Engine-specific implementation of WAL atomicity and durability.
///
/// Each accelerator engine that supports durable write-back implements this
/// trait. The trait encapsulates all engine-specific concerns — transaction
/// management, conflict detection, retry, and WAL schema management — so that
/// the rest of the WAL machinery (sinks, worker, delivery) is engine-agnostic.
///
/// To add WAL support for a new engine:
/// 1. Create a struct implementing this trait (e.g. `CayenneWalBackend`).
/// 2. Override `DataAccelerator::wal_backend` to return it.
#[async_trait]
pub trait WalBackend: Send + Sync {
    /// Create WAL tables / sequences if they do not already exist.
    async fn initialize(&self) -> Result<(), BoxError>;

    /// Count of WAL entries not yet delivered to the federated source.
    async fn pending_count(&self) -> Result<i64, BoxError>;

    /// Atomically: append a WAL INSERT entry + write `batches` to the data table.
    ///
    /// Implementations must handle engine-specific conflict resolution (e.g.
    /// `DuckDB` optimistic concurrency retry) internally.
    async fn atomic_insert(&self, batches: Vec<RecordBatch>) -> Result<(), BoxError>;

    /// Atomically: resolve PKs matching `filters`, append WAL DELETE entry, delete rows.
    ///
    /// Returns the number of deleted rows.
    async fn atomic_delete(&self, filters: &[Expr]) -> Result<u64, BoxError>;

    /// Atomically: resolve PKs matching `filters`, apply UPDATE, capture new
    /// row state, append WAL UPDATE entry.
    ///
    /// Returns the number of updated rows.
    async fn atomic_update(
        &self,
        assignments: &[(String, Expr)],
        filters: &[Expr],
    ) -> Result<u64, BoxError>;

    /// Stream all WAL entries belonging to the next undelivered `txn_id` group, ordered
    /// by `seq` ascending.  The group is the one with the smallest `min(seq)` among
    /// all entries that have `seq > last_delivered_checkpoint`.
    ///
    /// Returns an empty stream when there are no undelivered entries.
    fn next_pending_group(&self) -> BoxStream<'static, Result<WalEntry, BoxError>>;

    /// Advance the checkpoint to `seq` and delete delivered entries with seq ≤ seq.
    async fn advance_checkpoint(&self, seq: i64) -> Result<(), BoxError>;

    /// Columns that form the primary key (needed for PK-based delivery filters).
    fn primary_keys(&self) -> &[String];

    /// Logical table name (used in log messages and WAL table name derivation).
    fn table_name(&self) -> &str;
}

// ── WalContext ────────────────────────────────────────────────────────────────

/// Engine-agnostic shared context for WAL operations on a single accelerated table.
#[derive(Clone)]
pub(crate) struct WalContext {
    pub backend: Arc<dyn WalBackend>,
    pub dataset_name: String,
    pub notify_tx: mpsc::Sender<()>,
}

// ── Shared helpers ────────────────────────────────────────────────────────────

pub(crate) fn sanitize_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Serialize `batches` to Arrow IPC stream bytes (schema is embedded).
pub(crate) fn batches_to_arrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, BoxError> {
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Deserialize Arrow IPC stream bytes back to `RecordBatch`es.
pub(crate) fn arrow_ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, BoxError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Project PK columns from `batches` and serialize to Arrow IPC bytes.
pub(crate) fn extract_pks_ipc(
    batches: &[RecordBatch],
    primary_keys: &[String],
) -> Result<Vec<u8>, BoxError> {
    let pk_batches: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| -> Result<RecordBatch, BoxError> {
            let indices: Vec<usize> = primary_keys
                .iter()
                .map(|pk| {
                    batch
                        .schema()
                        .index_of(pk)
                        .map_err(|e| Box::new(e) as BoxError)
                })
                .collect::<Result<_, _>>()?;
            batch.project(&indices).map_err(|e| Box::new(e) as BoxError)
        })
        .collect::<Result<_, _>>()?;
    batches_to_arrow_ipc(&pk_batches)
}

/// Build `DataFusion` `IN`-list filter expressions from Arrow IPC PK bytes.
fn build_pk_filters_from_ipc(ipc: &[u8], primary_keys: &[String]) -> DataFusionResult<Vec<Expr>> {
    let batches = arrow_ipc_to_batches(ipc)
        .map_err(|e| DataFusionError::Execution(format!("WAL pk filter IPC decode: {e}")))?;

    if batches.is_empty() {
        return Ok(vec![lit(false)]);
    }

    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    if batch.num_rows() == 0 {
        return Ok(vec![lit(false)]);
    }

    if primary_keys.len() == 1 {
        let pk_col = &primary_keys[0];
        let col_idx = batch
            .schema()
            .index_of(pk_col)
            .map_err(|e| DataFusionError::Execution(format!("WAL pk filter: {e}")))?;
        let arr = batch.column(col_idx);
        let values: Vec<Expr> = (0..batch.num_rows())
            .filter_map(|row| ScalarValue::try_from_array(arr, row).ok().map(lit))
            .collect();
        if values.is_empty() {
            return Ok(vec![lit(false)]);
        }
        return Ok(vec![col(pk_col).in_list(values, false)]);
    }

    let row_filters: Vec<Expr> = (0..batch.num_rows())
        .filter_map(|row| {
            primary_keys
                .iter()
                .filter_map(|pk_col| {
                    let col_idx = batch.schema().index_of(pk_col).ok()?;
                    let scalar = ScalarValue::try_from_array(batch.column(col_idx), row).ok()?;
                    Some(col(pk_col).eq(lit(scalar)))
                })
                .reduce(Expr::and)
        })
        .collect();

    if row_filters.is_empty() {
        return Ok(vec![lit(false)]);
    }

    Ok(vec![
        row_filters
            .into_iter()
            .reduce(Expr::or)
            .unwrap_or(lit(false)),
    ])
}

// ── Write path ────────────────────────────────────────────────────────────────

/// Returns a `DataSinkExec` plan that atomically writes a WAL entry and the data rows.
pub(crate) fn insert_wal_write_back(
    input: Arc<dyn ExecutionPlan>,
    wal: Arc<WalContext>,
    refresher: Arc<Refresher>,
    schema: SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    let sink = Arc::new(WalInsertSink {
        wal,
        refresher,
        schema,
    });
    Arc::new(DataSinkExec::new(input, sink, None))
}

struct WalInsertSink {
    wal: Arc<WalContext>,
    refresher: Arc<Refresher>,
    schema: SchemaRef,
}

impl std::fmt::Debug for WalInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalInsertSink").finish_non_exhaustive()
    }
}

impl DisplayAs for WalInsertSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WalInsertSink")
    }
}

#[async_trait]
impl DataSink for WalInsertSink {
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
    ) -> DataFusionResult<u64> {
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut row_count: u64 = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            row_count = row_count.saturating_add(batch.num_rows() as u64);
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(0);
        }

        self.wal
            .backend
            .atomic_insert(batches)
            .await
            .map_err(|e| DataFusionError::Execution(format!("WAL insert failed: {e}")))?;

        // TODO: What is it?
        self.refresher.set_initial_load_completed(true);
        let _ = self.wal.notify_tx.try_send(());
        Ok(row_count)
    }
}

// ── WAL delete ────────────────────────────────────────────────────────────────

/// Returns a `DeletionExec` plan that atomically records a WAL DELETE entry and
/// deletes the matching rows from the accelerator.
pub(crate) fn delete_wal_write_back(
    filters: Vec<Expr>,
    wal: Arc<WalContext>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(DeletionExec::new(Arc::new(WalDeletionSink {
        wal,
        filters,
    })))
}

struct WalDeletionSink {
    wal: Arc<WalContext>,
    filters: Vec<Expr>,
}

#[async_trait]
impl DeletionSink for WalDeletionSink {
    async fn delete_from(&self) -> Result<u64, BoxError> {
        let count = self.wal.backend.atomic_delete(&self.filters).await?;
        let _ = self.wal.notify_tx.try_send(());
        Ok(count)
    }
}

// ── WAL update ────────────────────────────────────────────────────────────────

/// Returns a `DeletionExec` plan that atomically records a WAL UPDATE entry and
/// applies the update to the accelerator.
pub(crate) fn update_wal_write_back(
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    wal: Arc<WalContext>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(UpdateExec::new(Arc::new(WalUpdateSink {
        wal,
        assignments,
        filters,
    })))
}

struct WalUpdateSink {
    wal: Arc<WalContext>,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
}

#[async_trait]
impl UpdateSink for WalUpdateSink {
    async fn update_from(&self) -> Result<u64, BoxError> {
        let count = self
            .wal
            .backend
            .atomic_update(&self.assignments, &self.filters)
            .await?;
        let _ = self.wal.notify_tx.try_send(());
        Ok(count)
    }
}

// ── WAL worker ────────────────────────────────────────────────────────────────

/// Spawn the background WAL worker that delivers undelivered entries to the federated source.
pub(crate) fn start_wal_worker(
    wal: Arc<WalContext>,
    federated: Arc<FederatedTable>,
    mut notify_rx: mpsc::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = notify_rx.recv() => {
                    if msg.is_none() {
                        break;
                    }
                }
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }

            if let Err(e) = deliver_pending(&wal, &federated).await {
                tracing::warn!(
                    table = %wal.backend.table_name(),
                    "WAL delivery failed, will retry: {e}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    })
}

// ── WAL delivery ──────────────────────────────────────────────────────────────

/// Deliver all undelivered WAL entries to the federated source, one `txn_id` group at a time.
///
/// INSERT and UPDATE are streamed directly into `insert_into` — no full-group buffering.
/// DELETE accumulates only PK bytes (much smaller than full rows) to build the filter,
/// then issues a single `delete_from` call.
/// The checkpoint advances only after the entire group is delivered.
pub(crate) async fn deliver_pending(
    wal: &Arc<WalContext>,
    federated: &Arc<FederatedTable>,
) -> Result<(), BoxError> {
    let session_state = SessionContext::new().state();
    let primary_keys = wal.backend.primary_keys().to_vec();

    loop {
        let mut stream = wal.backend.next_pending_group();

        let first = match stream.next().await {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok(entry)) => entry,
        };

        let op = first.op;
        let mut max_seq = first.seq;
        let federated_provider = federated.table_provider().await;

        match op {
            WalOp::Insert => {
                // max_seq is updated inside the stream closure; share it via an atomic.
                let max_seq_atom = Arc::new(AtomicI64::new(first.seq));
                let max_seq_clone = Arc::clone(&max_seq_atom);

                let first_ipc = first.new_values.unwrap_or_default();
                let first_batches = arrow_ipc_to_batches(&first_ipc)?;
                let schema = federated_provider.schema();

                // Build a lazy RecordBatch stream from the WAL entry stream.
                // Each entry's IPC bytes are decoded on demand; the full group is never buffered.
                let batch_stream: BoxStream<'static, DataFusionResult<RecordBatch>> = Box::pin(
                    futures::stream::iter(first_batches.into_iter().map(Ok::<_, DataFusionError>))
                        .chain(stream.flat_map(
                            move |result| -> BoxStream<'static, DataFusionResult<RecordBatch>> {
                                match result {
                                    Err(e) => Box::pin(futures::stream::once(
                                        futures::future::ready(Err(DataFusionError::External(e))),
                                    )),
                                    Ok(entry) => {
                                        max_seq_clone.fetch_max(entry.seq, Ordering::SeqCst);
                                        match arrow_ipc_to_batches(
                                            &entry.new_values.unwrap_or_default(),
                                        ) {
                                            Err(e) => Box::pin(futures::stream::once(
                                                futures::future::ready(Err(
                                                    DataFusionError::External(e),
                                                )),
                                            )),
                                            Ok(batches) => Box::pin(futures::stream::iter(
                                                batches.into_iter().map(Ok::<_, DataFusionError>),
                                            )),
                                        }
                                    }
                                }
                            },
                        )),
                );

                let adapted = RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);
                let plan_input = Arc::new(StreamingExec::new(&schema, Box::pin(adapted)));
                let insert_plan = federated_provider
                    .insert_into(&session_state, plan_input, InsertOp::Append)
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                datafusion::physical_plan::collect(insert_plan, session_state.task_ctx())
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;

                max_seq = max_seq_atom.load(Ordering::SeqCst);
            }
            WalOp::Update => {
                // UPDATE delivery: delete the old rows by PK, then insert the new row state.
                // This avoids requiring upsert support on the target connector and is idempotent:
                // if delivery fails after delete but before insert, the retry deletes 0 rows
                // (already gone) and inserts the new values successfully.
                let mut new_value_batches =
                    arrow_ipc_to_batches(&first.new_values.unwrap_or_default())?;
                let mut pk_batches = arrow_ipc_to_batches(&first.pks_ipc)?;
                while let Some(result) = stream.next().await {
                    let entry = result?;
                    max_seq = max_seq.max(entry.seq);
                    new_value_batches
                        .extend(arrow_ipc_to_batches(&entry.new_values.unwrap_or_default())?);
                    pk_batches.extend(arrow_ipc_to_batches(&entry.pks_ipc)?);
                }

                if !pk_batches.is_empty() {
                    let combined_pks = concat_batches(&pk_batches[0].schema(), &pk_batches)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    let all_pks_ipc = batches_to_arrow_ipc(&[combined_pks])?;
                    let filters = build_pk_filters_from_ipc(&all_pks_ipc, &primary_keys)
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                    let delete_plan = federated_provider
                        .delete_from(&session_state, filters)
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                    datafusion::physical_plan::collect(delete_plan, session_state.task_ctx())
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                }

                if !new_value_batches.is_empty() {
                    let schema = federated_provider.schema();
                    let combined =
                        concat_batches(&new_value_batches[0].schema(), &new_value_batches)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    let batch_stream: BoxStream<'static, DataFusionResult<RecordBatch>> =
                        Box::pin(futures::stream::once(futures::future::ready(Ok(combined))));
                    let adapted = RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);
                    let plan_input = Arc::new(StreamingExec::new(&schema, Box::pin(adapted)));
                    let insert_plan = federated_provider
                        .insert_into(&session_state, plan_input, InsertOp::Append)
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                    datafusion::physical_plan::collect(insert_plan, session_state.task_ctx())
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                }
            }
            WalOp::Delete => {
                // Accumulate only PK bytes from all entries, then issue one delete_from call.
                // PK data is orders of magnitude smaller than full row data so buffering is OK.
                let mut pk_batches = arrow_ipc_to_batches(&first.pks_ipc)?;
                while let Some(result) = stream.next().await {
                    let entry = result?;
                    max_seq = max_seq.max(entry.seq);
                    pk_batches.extend(arrow_ipc_to_batches(&entry.pks_ipc)?);
                }
                if !pk_batches.is_empty() {
                    let combined = concat_batches(&pk_batches[0].schema(), &pk_batches)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    let all_pks_ipc = batches_to_arrow_ipc(&[combined])?;
                    let filters = build_pk_filters_from_ipc(&all_pks_ipc, &primary_keys)
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                    let plan = federated_provider
                        .delete_from(&session_state, filters)
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                    datafusion::physical_plan::collect(plan, session_state.task_ctx())
                        .await
                        .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                }
            }
        }

        wal.backend.advance_checkpoint(max_seq).await?;
    }

    Ok(())
}

// ── Startup logging ────────────────────────────────────────────────────────────

/// Log the number of undelivered WAL entries without blocking startup.
pub(crate) async fn log_pending_on_startup(wal: &Arc<WalContext>) {
    let dataset_name = wal.dataset_name.clone();
    let table_name = wal.backend.table_name().to_string();

    match wal.backend.pending_count().await {
        Ok(0) => {
            tracing::info!(
                dataset = %dataset_name,
                table = %table_name,
                "WAL worker started: no undelivered entries pending"
            );
        }
        Ok(n) => {
            tracing::info!(
                dataset = %dataset_name,
                table = %table_name,
                "WAL worker started: {n} undelivered entries pending delivery"
            );
        }
        Err(e) => {
            tracing::debug!(
                dataset = %dataset_name,
                "WAL worker started (could not read pending count: {e})"
            );
        }
    }
}
