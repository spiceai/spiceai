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
use super::RefreshTask;
use crate::accelerated_table::refresh::Refresh;
use crate::accelerated_table::refresh_task::deletion::build_batch_delete_expr_from_change_batch;
use crate::datafusion::error::{find_datafusion_root, format_datafusion_error};
use crate::{dataupdate::StreamingDataUpdateExecutionPlan, status};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::datatypes::DataType;
use cache::Caching;
use data_components::arrow::{IndexedMemTable, write::MemTable};
use data_components::cdc::{self, ChangeBatch, ChangeOperation, ChangesStream};
#[cfg(feature = "dynamodb")]
use data_components::dynamodb::stream::StreamError as DynamoDBStreamError;
use data_components::index_maintenance::perform_index_maintenance;
#[cfg(any(feature = "debezium", feature = "kafka"))]
use data_components::kafka::{
    Error as KafkaError, rdkafka::error::KafkaError as RdKafkaError,
    rdkafka::types::RDKafkaErrorCode,
};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::lit;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::{execution::context::SessionContext, physical_plan::collect};
use futures::{StreamExt, stream};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::provider::PartitionTableProvider;
use snafu::{OptionExt, ResultExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{Notify, RwLock};

/// Extracts the primary key value from the data, as a tuple of (String, Expr).
///
/// # Example
///
/// ```ignore
/// let data: RecordBatch = get_record_batch();
/// let key = "id";
/// let key_col = data.column(0);
/// let result = extract_primary_key!(key_col, key, data_schema, Int32Array, "Int32");
/// if let Ok((str_value, expr_value)) = result {
///    println!("Primary key value as String: {}", str_value);
///    println!("Primary key value as DataFusion expression: {}", expr_value);
/// }
/// ```
macro_rules! extract_primary_key {
    ($key_col:expr, $key:expr, $data_schema:expr, $array_type:ty, $data_type_str:expr) => {{
        let key_col = $key_col.as_any().downcast_ref::<$array_type>().context(
            crate::accelerated_table::PrimaryKeyArrayDataTypeMismatchSnafu {
                field_name: $key.to_string(),
                expected_data_type: $data_type_str.to_string(),
                schema: Arc::clone(&$data_schema),
            },
        )?;
        if key_col.is_null(0) {
            return crate::accelerated_table::PrimaryKeyNullValueSnafu {
                field_name: $key.to_string(),
            }
            .fail();
        }
        Ok((key_col.value(0).to_string(), lit(key_col.value(0))))
    }};
}

/// Channel depth between the CDC source-stream reader and the apply loop.
/// Each slot can hold one decoded `ChangeEnvelope`, so peak prefetch memory
/// is `CDC_PREFETCH_BUFFER * max_batch_bytes`. Tuned small to keep memory
/// bounded for sources that emit large per-transaction batches (e.g.,
/// Postgres logical replication during bulk inserts).
const CDC_PREFETCH_BUFFER: usize = 4;

impl RefreshTask {
    pub async fn start_changes_stream(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        changes_stream: ChangesStream,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let sql = refresh.read().await.display_sql();

        self.set_refresh_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        // Pipeline source-stream reads with apply+commit by running the source
        // in its own task on the refresh runtime and feeding a bounded channel.
        // While the apply loop writes batch N to the accelerator and commits
        // its source-side offset, the reader task can already be pulling and
        // decoding batch N+1 (network/CPU work that would otherwise be idle).
        // The bounded channel provides natural backpressure: when the apply
        // loop is the bottleneck, the reader parks on `send` and stops
        // pulling, so we never accumulate unbounded memory.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<
            Result<cdc::ChangeEnvelope, cdc::StreamError>,
        >(CDC_PREFETCH_BUFFER);

        let reader_dataset = dataset_name.clone();
        let reader_handle = tokio::spawn(async move {
            let mut stream = changes_stream;
            // `select!` on `tx.closed()` lets the reader exit promptly even
            // when it is parked in `stream.next()`. This matters at shutdown:
            // when the parent task is aborted, its locals (including `rx`)
            // are dropped, which closes `tx`. Without this select, a reader
            // blocked on the source (e.g., a Postgres replication recv) would
            // remain alive holding the source connection until the next item
            // happens to arrive. With it, the reader notices the consumer is
            // gone and tears down its source connection immediately.
            loop {
                tokio::select! {
                    biased;
                    () = tx.closed() => {
                        tracing::debug!(
                            "CDC consumer for {reader_dataset} dropped; reader exiting"
                        );
                        return;
                    }
                    item = stream.next() => {
                        let Some(item) = item else { return; };
                        if tx.send(item).await.is_err() {
                            tracing::debug!(
                                "CDC consumer for {reader_dataset} dropped; reader exiting"
                            );
                            return;
                        }
                    }
                }
            }
        });

        while let Some(update) = rx.recv().await {
            match update {
                Ok(change_envelope) => {
                    match self
                        .write_change(change_envelope.change_batch.clone())
                        .await
                    {
                        Ok(write_result) => {
                            // Mark the dataset as ready if possible
                            if change_envelope.is_dataset_ready() {
                                initial_load_completed.store(true, Ordering::Relaxed);
                                if let Some(ready_sender) = ready_sender.as_ref() {
                                    ready_sender.notify_waiters();
                                }
                                self.update_component_status(status::ComponentStatus::Ready)
                                    .await;
                            }

                            if let Err(e) = change_envelope.commit().await
                                && !self.runtime_status.is_shutdown()
                            {
                                tracing::error!("Failed to commit CDC change envelope: {e}");
                            }

                            if write_result == WriteChangeResult::DataWritten
                                && let Some(cache_provider_ref) = caching.as_ref()
                                && let Some(cache_provider) = cache_provider_ref.upgrade()
                                && let Err(e) =
                                    cache_provider.invalidate_for_table(dataset_name.clone())
                                && !self.runtime_status.is_shutdown()
                            {
                                // No cache provider means runtime is shutting down and cache is already cleaned up
                                tracing::error!(
                                    "Failed to invalidate cached results for dataset {}: {e}",
                                    &dataset_name.to_string()
                                );
                            }
                        }
                        Err(e) => {
                            let error_message = format_datafusion_error(&e);
                            self.set_refresh_status(
                                refresh.read().await.display_sql().as_deref(),
                                status::ComponentStatus::error_with_message(error_message),
                            )
                            .await;
                            if !self.runtime_status.is_shutdown() {
                                tracing::error!("Error writing change for {dataset_name}: {e}");
                            }
                        }
                    }
                }
                Err(e) => {
                    // If the error is transient (e.g., Kafka poll timeout), continue without changing the refresh status to Error
                    if handle_stream_error(&e, &self.dataset_name) == StreamErrorType::Transient {
                        continue;
                    }

                    let error_message = format_datafusion_error(&e);
                    self.set_refresh_status(
                        refresh.read().await.display_sql().as_deref(),
                        status::ComponentStatus::error_with_message(error_message),
                    )
                    .await;
                }
            }
        }

        // rx returned None: the reader dropped its sender. Three causes:
        //   1) source stream returned None (clean end-of-stream),
        //   2) reader saw `tx.closed()` and exited (consumer was dropped),
        //   3) reader panicked.
        // (1) and (2) join Ok; (3) joins Err with `is_panic()` true. We must
        // surface (3) loudly — silently swallowing it would leave the dataset
        // appearing healthy/ready while CDC ingestion has stopped. Cancelled
        // joins are expected during shutdown and do not need to escalate.
        match reader_handle.await {
            Ok(()) => {
                if !self.runtime_status.is_shutdown() {
                    tracing::warn!("Changes stream ended for dataset {dataset_name}");
                }
            }
            Err(e) if e.is_cancelled() => {
                tracing::debug!(
                    "CDC reader task for {dataset_name} was cancelled (likely shutdown)"
                );
            }
            Err(e) if !self.runtime_status.is_shutdown() => {
                let err_msg = format!("CDC reader task ended unexpectedly: {e}");
                tracing::error!("{err_msg} (dataset={dataset_name})");
                self.set_refresh_status(
                    refresh.read().await.display_sql().as_deref(),
                    status::ComponentStatus::error_with_message(err_msg),
                )
                .await;
            }
            Err(_) => {
                // Shutdown in progress and reader did not exit cleanly —
                // expected during teardown; nothing to escalate.
            }
        }

        Ok(())
    }

    async fn write_change(
        &self,
        change_batch: ChangeBatch,
    ) -> crate::accelerated_table::Result<WriteChangeResult> {
        let dataset_name = self.dataset_name.clone();

        let sub_batches = group_into_sub_batches(&change_batch);

        tracing::trace!(
            "Processing append/change stream batch: dataset={}, rows={}, sub-batches={}",
            self.dataset_name,
            change_batch.record.num_rows(),
            sub_batches.len()
        );

        let mut had_change = false;
        for (op_type, row_indices) in sub_batches {
            match op_type {
                ChangeOperationType::Delete => {
                    self.process_delete_batch(&change_batch, &row_indices)
                        .await?;
                    had_change = true;
                }
                ChangeOperationType::Upsert => {
                    self.process_upsert_batch(&change_batch, &row_indices)
                        .await?;
                    had_change = true;
                }
                ChangeOperationType::Truncate => {
                    self.process_truncate().await?;
                    had_change = true;
                }
                ChangeOperationType::Unknown => {
                    tracing::error!("Unknown change operation type for {dataset_name}");
                }
            }
        }

        if let Some(ref callback) = self.on_stream_batch_process_callback {
            let mut callback_guard = callback.lock().await;
            let future = callback_guard();
            future.await;
        }

        if had_change {
            Ok(WriteChangeResult::DataWritten)
        } else {
            Ok(WriteChangeResult::NoChange)
        }
    }

    async fn process_upsert_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = &self.dataset_name;

        let data_batch = change_batch.data_batch();

        if !row_indices.is_empty() {
            tracing::trace!(
                "Processing upsert batch for {dataset_name} with {} rows",
                row_indices.len()
            );
        }

        let selected_batch = select_data_rows(&data_batch, row_indices)?;

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let record_batch_stream = Box::pin(RecordBatchStreamAdapter::new(
            selected_batch.schema(),
            Box::pin(stream::once(async move { Ok(selected_batch) })),
        ));

        let _lock_guard = self.accelerator_write_mutex.lock().await;

        // Wrap with SchemaCastScanExec to ensure data types match the accelerator schema
        // (e.g., timestamp precision conversion from Millisecond to Microsecond for Cayenne)
        let target_schema = self.accelerator.schema();
        let streaming_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(record_batch_stream));
        let cast_plan: Arc<dyn ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(streaming_plan, target_schema));

        let insert_plan = self
            .accelerator
            .insert_into(&session_state, cast_plan, InsertOp::Append)
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
        collect(insert_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
        perform_change_write_maintenance(&self.accelerator).await?;

        self.update_last_updated_at();

        Ok(())
    }

    async fn process_truncate(&self) -> crate::accelerated_table::Result<()> {
        let dataset_name = &self.dataset_name;
        tracing::info!("Processing TRUNCATE for {dataset_name}");

        let ctx = SessionContext::new();
        let session_state = ctx.state();
        let _lock_guard = self.accelerator_write_mutex.lock().await;
        // Some accelerator impls (notably DuckDB) treat an empty filter list as
        // a no-op to guard against accidental full-table deletes. To get
        // uniform "wipe the whole table" semantics we pass an always-true
        // literal, which is emitted as `DELETE FROM <table> WHERE TRUE` and
        // applied consistently across engines.
        let delete_plan = self
            .accelerator
            .delete_from(&session_state, vec![lit(true)])
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
        collect(delete_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
        perform_change_write_maintenance(&self.accelerator).await?;

        self.update_last_updated_at();
        Ok(())
    }

    async fn process_delete_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = &self.dataset_name;

        tracing::trace!(
            "Processing delete batch for {dataset_name} with {} rows",
            row_indices.len()
        );

        if row_indices
            .first()
            .is_some_and(|row| change_batch.primary_keys(*row).is_empty())
        {
            let selected_batch = select_data_rows(&change_batch.data_batch(), row_indices)?;
            if delete_matching_rows_from_arrow_provider(&self.accelerator, &selected_batch)
                .await?
                .is_some()
            {
                perform_change_write_maintenance(&self.accelerator).await?;
                self.update_last_updated_at();
                return Ok(());
            }
        }

        let combined = build_batch_delete_expr_from_change_batch(
            change_batch,
            row_indices,
            dataset_name.to_string().as_str(),
        )?;

        if let Some(combined) = combined {
            let ctx = SessionContext::new();
            let session_state = ctx.state();

            let _lock_guard = self.accelerator_write_mutex.lock().await;
            let delete_plan = self
                .accelerator
                .delete_from(&session_state, vec![combined])
                .await
                .map_err(find_datafusion_root)
                .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
            collect(delete_plan, ctx.task_ctx())
                .await
                .map_err(find_datafusion_root)
                .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
            perform_change_write_maintenance(&self.accelerator).await?;
        }

        self.update_last_updated_at();

        Ok(())
    }
}

fn select_data_rows(
    data_batch: &RecordBatch,
    row_indices: &[usize],
) -> crate::accelerated_table::Result<RecordBatch> {
    let indices = row_indices
        .iter()
        .map(|&idx| {
            u32::try_from(idx).map_err(|_| {
                arrow::error::ArrowError::InvalidArgumentError(format!(
                    "CDC row index {idx} exceeds u32::MAX"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .context(crate::accelerated_table::FailedToBuildRecordBatchSnafu)?;

    let indices_array = UInt32Array::from(
        indices,
    );

    let selected_columns: Vec<ArrayRef> = data_batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
        .collect::<Result<Vec<_>, _>>()
        .context(crate::accelerated_table::FailedToBuildRecordBatchSnafu)?;

    RecordBatch::try_new(data_batch.schema(), selected_columns)
        .context(crate::accelerated_table::FailedToBuildRecordBatchSnafu)
}

async fn delete_matching_rows_from_arrow_provider(
    provider: &Arc<dyn TableProvider>,
    rows: &RecordBatch,
) -> crate::accelerated_table::Result<Option<u64>> {
    if let Some(table) = provider.as_any().downcast_ref::<MemTable>() {
        return table
            .delete_matching_rows(rows)
            .await
            .map(Some)
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu);
    }

    if let Some(table) = provider.as_any().downcast_ref::<IndexedMemTable>() {
        return table
            .delete_matching_rows(rows)
            .await
            .map(Some)
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu);
    }

    if let Some(partitioned) = provider.as_any().downcast_ref::<PartitionTableProvider>() {
        let mut deleted = 0_u64;
        let mut matched_arrow_provider = false;
        for partition_provider in partitioned.partition_table_providers().await {
            if let Some(partition_deleted) = Box::pin(delete_matching_rows_from_arrow_provider(
                &partition_provider,
                rows,
            ))
            .await?
            {
                deleted += partition_deleted;
                matched_arrow_provider = true;
            }
        }

        return Ok(matched_arrow_provider.then_some(deleted));
    }

    Ok(None)
}

async fn perform_change_write_maintenance(
    provider: &Arc<dyn TableProvider>,
) -> crate::accelerated_table::Result<()> {
    if let Some(partitioned) = provider.as_any().downcast_ref::<PartitionTableProvider>() {
        for partition_provider in partitioned.partition_table_providers().await {
            Box::pin(perform_change_write_maintenance(&partition_provider)).await?;
        }
        return Ok(());
    }

    perform_index_maintenance(provider.as_ref())
        .await
        .map(|_| ())
        .map_err(find_datafusion_root)
        .context(crate::accelerated_table::FailedToWriteDataSnafu)
}

pub(crate) fn get_primary_key_value(
    data: &RecordBatch,
    key: &str,
) -> crate::accelerated_table::Result<(String, Expr)> {
    let data_schema = data.schema();
    let (primary_key_idx, field) = data_schema.column_with_name(key).ok_or_else(|| {
        crate::accelerated_table::PrimaryKeyExpectedSchemaToHaveFieldSnafu {
            field_name: key.to_string(),
            schema: Arc::clone(&data_schema),
        }
        .build()
    })?;

    let key_col = data.column(primary_key_idx);
    match field.data_type() {
        DataType::Int32 => {
            extract_primary_key!(key_col, key, data_schema, Int32Array, "Int32")
        }
        DataType::Int64 => {
            extract_primary_key!(key_col, key, data_schema, Int64Array, "Int64")
        }
        DataType::Utf8 => {
            extract_primary_key!(key_col, key, data_schema, StringArray, "String")
        }
        _ => crate::accelerated_table::PrimaryKeyTypeNotYetSupportedSnafu {
            data_type: field.data_type().to_string(),
        }
        .fail(),
    }
}

/// Groups rows into sub-batches based on operation type and primary key uniqueness
/// Returns a vector of (`operation_type`, `row_indices`) tuples
#[must_use]
fn group_into_sub_batches(change_batch: &ChangeBatch) -> Vec<(ChangeOperationType, Vec<usize>)> {
    let num_rows = change_batch.record.num_rows();
    if num_rows == 0 {
        return vec![];
    }

    // Extract data batch and PK column indices once, instead of per-row.
    let data_batch = change_batch.data_batch();
    let pk_column_names = change_batch.primary_keys(0);
    let pk_col_indices: Vec<usize> = pk_column_names
        .iter()
        .filter_map(|name| data_batch.schema().index_of(name).ok())
        .collect();

    let mut sub_batches = Vec::new();
    let mut current_batch_indices = Vec::new();
    let mut current_op_type: Option<ChangeOperationType> = None;
    let mut seen_primary_keys: HashSet<String> = HashSet::new();

    for row_id in 0..num_rows {
        let op = change_batch.op(row_id);
        let op_type = ChangeOperationType::from_operation(&op);

        // Build PK string directly from column arrays — no per-row RecordBatch allocation.
        // If there are no PK columns (e.g., Kafka append-only), skip dedup tracking entirely.
        let has_pks = !pk_col_indices.is_empty();
        let primary_keys = if has_pks {
            pk_col_indices
                .iter()
                .filter_map(|&col_idx| {
                    arrow::util::display::array_value_to_string(data_batch.column(col_idx), row_id)
                        .ok()
                })
                .collect::<Vec<_>>()
                .join(",")
        } else {
            String::new()
        };

        let should_split = if let Some(current_type) = current_op_type {
            current_type != op_type || (has_pks && seen_primary_keys.contains(&primary_keys))
        } else {
            false
        };

        if should_split {
            if !current_batch_indices.is_empty()
                && let Some(op_type) = current_op_type
            {
                sub_batches.push((op_type, current_batch_indices.clone()));
            }

            current_batch_indices.clear();
            seen_primary_keys.clear();
            current_op_type = Some(op_type);
        } else if current_op_type.is_none() {
            current_op_type = Some(op_type);
        }

        current_batch_indices.push(row_id);
        if has_pks {
            seen_primary_keys.insert(primary_keys);
        }
    }

    if !current_batch_indices.is_empty()
        && let Some(op_type) = current_op_type
    {
        sub_batches.push((op_type, current_batch_indices));
    }

    sub_batches
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteChangeResult {
    DataWritten,
    NoChange,
}

// Used to group batch changes into sub-batches
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperationType {
    Upsert, // Create, Update, or Read
    Delete,
    Truncate,
    Unknown,
}

impl ChangeOperationType {
    fn from_operation(op: &ChangeOperation) -> Self {
        match op {
            ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => {
                Self::Upsert
            }
            ChangeOperation::Delete => Self::Delete,
            ChangeOperation::Truncate => Self::Truncate,
            ChangeOperation::Unknown(_) => Self::Unknown,
        }
    }
}

#[derive(PartialEq)]
enum StreamErrorType {
    Transient,
    Fatal,
}

/// Logs and classifies [`StreamError`] errors for a dataset.
/// Returns `true` if the error is transient and the stream can continue normally.
/// These errors are generally nonfatal and often indicate that the consumer should retry or continue polling.
fn handle_stream_error(err: &cdc::StreamError, dataset_name: &TableReference) -> StreamErrorType {
    #[cfg(any(feature = "debezium", feature = "kafka"))]
    if matches!(err, cdc::StreamError::Kafka(KafkaError::EmptyBatch)) {
        return StreamErrorType::Transient;
    }

    #[cfg(any(feature = "debezium", feature = "kafka"))]
    if let cdc::StreamError::Kafka(KafkaError::UnableToReceiveMessage { source }) = err {
        match source {
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) => {
                tracing::warn!(
                    "Kafka poll interval exceeded for dataset '{dataset_name}': connection lost or consumer too slow. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure) => {
                tracing::warn!(
                    "Connection to Kafka broker for dataset '{dataset_name}' was lost or is invalid. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut) => {
                tracing::error!(
                    "Kafka operation timed out while retrieving message for dataset '{dataset_name}'. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown) => {
                tracing::warn!(
                    "All Kafka brokers are down for dataset '{dataset_name}'. Check broker status and network connectivity. Retrying."
                );
                return StreamErrorType::Transient;
            }
            RdKafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition) => {
                tracing::error!(
                    "Kafka topic not found for dataset '{dataset_name}': check if the topic exists and is spelled correctly."
                );
            }
            _ => {
                tracing::error!(
                    "A Kafka error occurred for dataset '{dataset_name}': {source}. Check your Kafka broker and network connectivity."
                );
            }
        }
        return StreamErrorType::Fatal;
    }

    #[cfg(feature = "dynamodb")]
    if matches!(
        err,
        cdc::StreamError::DynamoDB(DynamoDBStreamError::FailedToReceiveMessage {
            source: dynamodb_streams::Error::StreamBeyondRetention,
        })
    ) {
        tracing::error!(
            "DynamoDB Stream for dataset '{dataset_name}' is beyond 24 hour retention policy. Delete acceleration to initiate table bootstrapping"
        );
        return StreamErrorType::Fatal;
    }

    tracing::error!("Changes stream error for {dataset_name}: {err}");
    StreamErrorType::Fatal
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use data_components::arrow::write::MemTable;
    use data_components::cdc::changes_schema;
    use datafusion::datasource::TableProvider;

    use std::sync::Arc;

    fn create_test_data_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn create_test_change_batch(
        ops: Vec<&str>,
        primary_keys: &[Vec<&str>],
        ids: Vec<i32>,
        names: Vec<Option<&str>>,
    ) -> ChangeBatch {
        assert_eq!(
            ops.len(),
            primary_keys.len(),
            "ops and primary_keys must have same length"
        );
        assert_eq!(ops.len(), ids.len(), "ops and ids must have same length");
        assert_eq!(
            ops.len(),
            names.len(),
            "ops and names must have same length"
        );

        let data_schema = create_test_data_schema();
        let schema = changes_schema(&data_schema);

        // Create op column
        let op_array: ArrayRef = Arc::new(StringArray::from(ops));

        // Create primary_keys column (List of Strings)
        let mut pk_offsets = vec![0i32];
        let mut pk_values = Vec::new();

        for pk_vec in primary_keys {
            for &pk in pk_vec {
                pk_values.push(pk);
            }
            pk_offsets.push(
                pk_offsets.last().expect("offsets should not be empty")
                    + i32::try_from(pk_vec.len()).expect("pk_vec.len() fits in i32"),
            );
        }

        let pk_values_array = StringArray::from(pk_values);
        let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let pk_array: ArrayRef = Arc::new(
            ListArray::try_new(
                pk_field,
                arrow::buffer::OffsetBuffer::new(pk_offsets.into()),
                Arc::new(pk_values_array),
                None,
            )
            .expect("Failed to create ListArray"),
        );

        // Create data column (Struct)
        let id_array: ArrayRef = Arc::new(Int32Array::from(ids));
        let name_array: ArrayRef = Arc::new(StringArray::from(names));

        let data_fields = vec![
            (Arc::new(Field::new("id", DataType::Int32, false)), id_array),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                name_array,
            ),
        ];
        let data_array: ArrayRef = Arc::new(StructArray::from(data_fields));

        let record = RecordBatch::try_new(Arc::new(schema), vec![op_array, pk_array, data_array])
            .expect("Failed to create RecordBatch");

        ChangeBatch::try_new(record).expect("Failed to create ChangeBatch")
    }

    #[test]
    fn test_empty_batch() {
        let change_batch = create_test_change_batch(vec![], &[], vec![], vec![]);

        let result = group_into_sub_batches(&change_batch);

        assert!(result.is_empty(), "Empty batch should return empty vector");
    }

    #[test]
    fn test_single_row() {
        let change_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("Alice")]);

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(result.len(), 1, "Should have one sub-batch");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);
    }

    #[test]
    fn test_same_operation_different_primary_keys() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("Alice"), Some("Bob"), Some("Charlie")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            1,
            "Should have one sub-batch for same operation type with different keys"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_different_operation_types_split() {
        let change_batch = create_test_change_batch(
            vec!["c", "d", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("Alice"), Some("Bob"), Some("Charlie")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            3,
            "Should split into three sub-batches for different operations"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);

        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1, vec![2]);
    }

    #[test]
    fn test_duplicate_primary_key_causes_split() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 2], // First two rows have same id value
            vec![Some("Alice"), Some("Alice_v2"), Some("Bob")],
        );

        let result = group_into_sub_batches(&change_batch);

        // Should split when duplicate primary key is encountered within same operation type
        assert_eq!(
            result.len(),
            2,
            "Should split into two sub-batches when duplicate key is found"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);

        assert_eq!(result[1].0, ChangeOperationType::Upsert);
        assert_eq!(result[1].1, vec![1, 2]);
    }

    #[test]
    fn test_upsert_operations_grouped_together() {
        // create, update, and read should all map to Upsert
        let change_batch = create_test_change_batch(
            vec!["c", "u", "r"],
            &[vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3],
            vec![Some("A"), Some("B"), Some("C")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            1,
            "Create, update, and read should be grouped as Upsert"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_all_operation_types() {
        let change_batch = create_test_change_batch(
            vec!["c", "u", "r", "d", "t"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3, 4, 5],
            vec![Some("A"), Some("B"), Some("C"), Some("D"), Some("E")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            3,
            "Should have 3 sub-batches: Upsert, Delete, Truncate"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![3]);

        assert_eq!(result[2].0, ChangeOperationType::Truncate);
        assert_eq!(result[2].1, vec![4]);
    }

    #[test]
    fn test_multiple_duplicate_keys_in_sequence() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c", "c"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 1, 2, 1],
            vec![Some("A"), Some("A2"), Some("B"), Some("A3")],
        );

        let result = group_into_sub_batches(&change_batch);

        // First batch: id=1 (row 0)
        // Second batch: id=1 (row 1, duplicate), id=2 (row 2, new)
        // Third batch: id=1 (row 3, duplicate again)
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);

        assert_eq!(result[1].0, ChangeOperationType::Upsert);
        assert_eq!(result[1].1, vec![1, 2]);

        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1, vec![3]);
    }

    #[test]
    fn test_composite_primary_keys() {
        let change_batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec!["id", "name"], vec!["id", "name"], vec!["id", "name"]],
            vec![1, 2, 1],
            vec![Some("Alice"), Some("Bob"), Some("Alice")],
        );

        let result = group_into_sub_batches(&change_batch);

        // Composite keys are formatted differently, so these should be distinct
        assert_eq!(
            result.len(),
            2,
            "Different composite keys should not cause split"
        );
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1]);
        assert_eq!(result[1].0, ChangeOperationType::Upsert);
        assert_eq!(result[1].1, vec![2]);
    }

    #[test]
    fn test_alternating_operations() {
        let change_batch = create_test_change_batch(
            vec!["c", "d", "c", "d"],
            &[vec!["id"], vec!["id"], vec!["id"], vec!["id"]],
            vec![1, 2, 3, 4],
            vec![Some("A"), Some("B"), Some("C"), Some("D")],
        );

        let result = group_into_sub_batches(&change_batch);

        assert_eq!(
            result.len(),
            4,
            "Alternating operations should create 4 sub-batches"
        );

        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0]);

        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1, vec![1]);

        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1, vec![2]);

        assert_eq!(result[3].0, ChangeOperationType::Delete);
        assert_eq!(result[3].1, vec![3]);
    }

    fn make_mem_table() -> Arc<MemTable> {
        let schema = Arc::new(create_test_data_schema());
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
    }

    fn make_refresh_task(accelerator: Arc<dyn TableProvider>) -> RefreshTask {
        use crate::accelerated_table::refresh_task::RefreshTaskBuilder;
        use crate::federated_table::FederatedTable;
        use tokio::runtime::Handle;
        use tokio::sync::Mutex;

        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        RefreshTaskBuilder::new(
            crate::status::RuntimeStatus::new(),
            datafusion::sql::TableReference::bare("test"),
            federated,
            None,
            accelerator,
            Handle::current(),
            Arc::new(Mutex::new(())),
        )
        .build()
    }

    #[tokio::test]
    async fn test_write_change_upsert_returns_data_written() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let change_batch =
            create_test_change_batch(vec!["c"], &[vec!["id"]], vec![1], vec![Some("Alice")]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::DataWritten
        );
    }

    #[tokio::test]
    async fn test_write_change_delete_returns_data_written() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let change_batch =
            create_test_change_batch(vec!["d"], &[vec!["id"]], vec![1], vec![Some("Alice")]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::DataWritten
        );
    }

    #[tokio::test]
    async fn test_empty_returns_no_change() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        // Any unrecognized op string maps to ChangeOperation::Unknown
        let change_batch = create_test_change_batch(vec![], &[], vec![], vec![]);
        assert_eq!(
            task.write_change(change_batch)
                .await
                .expect("write_change should succeed"),
            WriteChangeResult::NoChange
        );
    }

    #[test]
    fn test_group_into_sub_batches_no_pks_single_batch() {
        let batch = create_test_change_batch(
            vec!["c", "c", "c"],
            &[vec![], vec![], vec![]],
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let result = group_into_sub_batches(&batch);

        // No PKs + all same op → 1 sub-batch with all rows
        assert_eq!(result.len(), 1, "Should produce 1 sub-batch when no PKs");
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn test_group_into_sub_batches_no_pks_mixed_ops() {
        // Mixed ops with no PKs: should split only on op type boundaries
        let ops = vec!["c", "c", "c", "d", "d", "c", "c"];
        let primary_keys: Vec<Vec<&str>> = vec![vec![]; 7];
        let ids = vec![1, 2, 3, 4, 5, 6, 7];
        let names = vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ];
        let batch = create_test_change_batch(ops, &primary_keys, ids, names);

        let result = group_into_sub_batches(&batch);

        // Should split into 3 groups: [c,c,c], [d,d], [c,c]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, ChangeOperationType::Upsert);
        assert_eq!(result[0].1.len(), 3);
        assert_eq!(result[1].0, ChangeOperationType::Delete);
        assert_eq!(result[1].1.len(), 2);
        assert_eq!(result[2].0, ChangeOperationType::Upsert);
        assert_eq!(result[2].1.len(), 2);
    }

    // ---------------------------------------------------------------------
    // Tests for `start_changes_stream` (the CDC source-stream → apply
    // pipeline). These exercise correctness of the prefetch-channel design:
    // ordering, commit-after-write, error continuation, clean termination,
    // dataset-ready signaling, actual pipelining behavior under a slow
    // accelerator, and prompt reader cancellation when the consumer goes
    // away. Together they nail down the invariants the broader CDC stack
    // relies on (PG WAL, Kafka/Debezium, DynamoDB Streams).
    // ---------------------------------------------------------------------

    use async_trait::async_trait;
    use data_components::cdc::{
        ChangeEnvelope, CommitChange, CommitError, StreamError as CdcStreamError,
    };
    use datafusion::catalog::Session;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::Expr;
    use futures::stream::{self as fstream};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;
    use tokio::sync::Notify;

    /// Records when each envelope is committed and in what order.
    /// Used to assert the apply→commit ordering invariant.
    #[derive(Default)]
    struct CommitLog {
        // (envelope_id, commit_outcome)
        events: TokioMutex<Vec<(i32, Result<(), String>)>>,
    }

    impl CommitLog {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        async fn ids(&self) -> Vec<i32> {
            self.events.lock().await.iter().map(|(id, _)| *id).collect()
        }
    }

    struct TrackingCommitter {
        id: i32,
        log: Arc<CommitLog>,
        outcome: Result<(), String>,
    }

    #[async_trait]
    impl CommitChange for TrackingCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log
                .events
                .lock()
                .await
                .push((self.id, self.outcome.clone()));
            match &self.outcome {
                Ok(()) => Ok(()),
                Err(msg) => Err(CommitError::UnableToCommitChange {
                    source: msg.clone().into(),
                }),
            }
        }
    }

    fn make_tracked_envelope(id: i32, log: Arc<CommitLog>, is_ready: bool) -> ChangeEnvelope {
        let batch = create_test_change_batch(vec!["c"], &[vec!["id"]], vec![id], vec![Some("row")]);
        ChangeEnvelope::new(
            Box::new(TrackingCommitter {
                id,
                log,
                outcome: Ok(()),
            }),
            batch,
            is_ready,
        )
    }

    /// Stream wrapper that signals on Drop. Used to verify the reader task
    /// is torn down when the consumer goes away.
    struct DropSignalStream<S> {
        inner: S,
        notify_on_drop: Arc<Notify>,
    }

    impl<S> Drop for DropSignalStream<S> {
        fn drop(&mut self) {
            self.notify_on_drop.notify_waiters();
        }
    }

    impl<S: futures::Stream + Unpin> futures::Stream for DropSignalStream<S> {
        type Item = S::Item;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    /// Builds a `ChangesStream` from a vector of pre-built items. Items are
    /// yielded in order; the stream then ends.
    fn make_changes_stream(items: Vec<Result<ChangeEnvelope, CdcStreamError>>) -> ChangesStream {
        fstream::iter(items).boxed()
    }

    /// Counts every poll on the inner stream, and lets us pull on demand via
    /// an inner channel. This makes pipeline overlap directly observable.
    async fn run_changes_stream(
        task: &RefreshTask,
        stream: ChangesStream,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated_table::Result<()> {
        let refresh = Arc::new(RwLock::new(
            crate::accelerated_table::refresh::Refresh::default(),
        ));
        task.start_changes_stream(refresh, stream, None, ready_sender, initial_load_completed)
            .await
    }

    // -- Correctness: ordering ------------------------------------------------

    #[tokio::test]
    async fn test_start_changes_stream_processes_envelopes_in_order() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(4, Arc::clone(&log), false)),
        ]);

        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should succeed");

        assert_eq!(
            log.ids().await,
            vec![1, 2, 3, 4],
            "envelopes must be committed in arrival order"
        );
    }

    // -- Correctness: commit-after-write ordering -----------------------------

    /// Wraps a `TableProvider` and records each `insert_into` call.
    /// Together with `CommitLog`, this lets us assert that for every
    /// envelope `id`, the write event happens strictly before the commit.
    #[derive(Debug)]
    struct WriteOrderRecordingProvider {
        inner: Arc<dyn TableProvider>,
        write_log: Arc<TokioMutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl TableProvider for WriteOrderRecordingProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.write_log.lock().await.push("write");
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    /// Records "commit" into a shared log when its `commit()` runs, so we
    /// can assert the interleaved write/commit sequence in
    /// `test_start_changes_stream_commits_after_write`.
    struct SequencedCommitter {
        log: Arc<TokioMutex<Vec<&'static str>>>,
    }
    #[async_trait]
    impl CommitChange for SequencedCommitter {
        async fn commit(&self) -> Result<(), CommitError> {
            self.log.lock().await.push("commit");
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_start_changes_stream_commits_after_write() {
        let write_log: Arc<TokioMutex<Vec<&'static str>>> = Arc::new(TokioMutex::new(Vec::new()));
        let provider = Arc::new(WriteOrderRecordingProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            write_log: Arc::clone(&write_log),
        });
        let task = make_refresh_task(provider as Arc<dyn TableProvider>);

        // Use a single shared log; both `insert_into` and `commit()` push
        // markers, so we can read off the interleaved write/commit sequence.
        let combined: Arc<TokioMutex<Vec<&'static str>>> = Arc::clone(&write_log);

        let mk = |id: i32| -> ChangeEnvelope {
            let batch =
                create_test_change_batch(vec!["c"], &[vec!["id"]], vec![id], vec![Some("row")]);
            ChangeEnvelope::new(
                Box::new(SequencedCommitter {
                    log: Arc::clone(&combined),
                }),
                batch,
                false,
            )
        };

        let stream = make_changes_stream(vec![Ok(mk(1)), Ok(mk(2)), Ok(mk(3))]);
        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should succeed");

        let observed = combined.lock().await.clone();
        // For each envelope: write must precede the next commit. With three
        // envelopes the strict expected sequence is W,C,W,C,W,C.
        assert_eq!(
            observed,
            vec!["write", "commit", "write", "commit", "write", "commit"],
            "each envelope must be written, then committed, in order"
        );
    }

    // -- Correctness: error path continues the loop ---------------------------

    #[tokio::test]
    async fn test_start_changes_stream_continues_after_stream_error() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // Sandwich a fatal stream error between two healthy envelopes; both
        // valid envelopes must still be committed (the loop logs the error
        // and continues — it does not abort).
        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Err(CdcStreamError::Arrow("synthetic test failure".into())),
            Ok(make_tracked_envelope(2, Arc::clone(&log), false)),
        ]);

        run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false)))
            .await
            .expect("start_changes_stream should not propagate stream errors");

        assert_eq!(
            log.ids().await,
            vec![1, 2],
            "both pre- and post-error envelopes must be committed"
        );
    }

    // -- Correctness: clean termination on stream end -------------------------

    #[tokio::test]
    async fn test_start_changes_stream_terminates_on_stream_end() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();

        // Empty stream: returns None immediately. start_changes_stream must
        // exit cleanly (does not hang).
        let stream = make_changes_stream(vec![]);

        let res = tokio::time::timeout(
            Duration::from_secs(5),
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))),
        )
        .await
        .expect("must not hang on empty stream");
        res.expect("must return Ok on empty stream");
        assert!(log.ids().await.is_empty());
    }

    // -- Correctness: dataset-ready signaling ---------------------------------

    #[tokio::test]
    async fn test_start_changes_stream_signals_dataset_ready() {
        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let initial_load = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());

        // Subscribe BEFORE running so we don't miss the notify_waiters signal.
        let notified = {
            let n = Arc::clone(&notify);
            tokio::spawn(async move {
                let waiter = n.notified();
                tokio::pin!(waiter);
                tokio::time::timeout(Duration::from_secs(5), &mut waiter)
                    .await
                    .is_ok()
            })
        };
        // Yield so the subscriber registers before we proceed.
        tokio::task::yield_now().await;

        let stream = make_changes_stream(vec![
            Ok(make_tracked_envelope(1, Arc::clone(&log), false)),
            Ok(make_tracked_envelope(2, Arc::clone(&log), true)), // ready=true
            Ok(make_tracked_envelope(3, Arc::clone(&log), false)),
        ]);
        run_changes_stream(
            &task,
            stream,
            Some(Arc::clone(&notify)),
            Arc::clone(&initial_load),
        )
        .await
        .expect("start_changes_stream should succeed");

        assert!(
            initial_load.load(Ordering::Relaxed),
            "initial_load_completed must flip to true once a ready envelope is processed"
        );
        assert!(
            notified.await.expect("ready notifier task must finish"),
            "ready_sender.notify_waiters() must fire when a ready envelope is processed"
        );
    }

    // -- Pipelining: verify reader prefetches under a slow apply --------------

    /// `TableProvider` that delays each `insert_into` to simulate a slow
    /// accelerator. Used to expose pipeline overlap: while the apply task
    /// is sleeping inside `insert_into`, the reader task should be free to
    /// drain ahead and fill the prefetch channel.
    #[derive(Debug)]
    struct SlowProvider {
        inner: Arc<dyn TableProvider>,
        delay: Duration,
        writes_started: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TableProvider for SlowProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            insert_op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.writes_started.fetch_add(1, AtomicOrdering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.inner.insert_into(state, input, insert_op).await
        }
    }

    /// A stream wrapper that increments a counter every time `poll_next`
    /// produces a new item. This makes "items pulled from source" directly
    /// observable.
    struct CountingStream<S> {
        inner: S,
        pulled: Arc<AtomicUsize>,
    }

    impl<S: futures::Stream + Unpin> futures::Stream for CountingStream<S> {
        type Item = S::Item;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    self.pulled.fetch_add(1, AtomicOrdering::SeqCst);
                    Poll::Ready(Some(item))
                }
                other => other,
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_start_changes_stream_pipelines_reads_with_writes() {
        // 6 envelopes, accelerator delays 80ms per write. With pipelining,
        // the reader should pull all 6 items into the prefetch channel
        // within the first apply window, well before the writes complete.
        // Without pipelining (serial), pulls and writes would alternate and
        // we'd see at most ~1 pull worth of headroom.
        let writes_started = Arc::new(AtomicUsize::new(0));
        let pulled = Arc::new(AtomicUsize::new(0));

        let slow = Arc::new(SlowProvider {
            inner: make_mem_table() as Arc<dyn TableProvider>,
            delay: Duration::from_millis(80),
            writes_started: Arc::clone(&writes_started),
        });
        let task = make_refresh_task(slow as Arc<dyn TableProvider>);

        let log = CommitLog::new();
        let envelopes: Vec<Result<ChangeEnvelope, CdcStreamError>> = (1..=6)
            .map(|id| Ok(make_tracked_envelope(id, Arc::clone(&log), false)))
            .collect();

        let inner = fstream::iter(envelopes);
        let counting = CountingStream {
            inner: Box::pin(inner),
            pulled: Arc::clone(&pulled),
        };
        let stream: ChangesStream = counting.boxed();

        let task_handle = tokio::spawn(async move {
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))).await
        });

        // Wait until the first write has started — that means the apply task
        // has consumed one envelope from the channel and is now in the slow
        // insert. Give it a generous window so this isn't flaky on loaded
        // CI; the assertion below still requires real pipelining.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while writes_started.load(AtomicOrdering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() <= deadline,
                "apply task never started writing",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Poll until the reader has prefetched at least 2 items ahead of the
        // applier, or time out. The invariant we care about — reader ahead of
        // applier under a slow accelerator — must hold during the 80ms apply
        // window; we just don't want to depend on hitting any specific
        // moment in that window. Polling avoids fixed-sleep flakiness under
        // CI scheduling variance.
        let prefetch_deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let p = pulled.load(AtomicOrdering::SeqCst);
            let w = writes_started.load(AtomicOrdering::SeqCst);
            if p >= w + 2 {
                break;
            }
            assert!(
                std::time::Instant::now() <= prefetch_deadline,
                "expected reader to prefetch ahead of applier; pulled={p}, writes_started={w}",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        task_handle
            .await
            .expect("task join")
            .expect("changes stream should succeed");
        // Final invariant: every envelope was committed exactly once, in order.
        assert_eq!(log.ids().await, vec![1, 2, 3, 4, 5, 6]);
    }

    // -- Reliability: reader exits when consumer is dropped -------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_start_changes_stream_reader_exits_on_consumer_drop() {
        // Build a stream that yields one item, then PARKS forever (returns
        // Pending and never wakes). If the reader were not racing on
        // tx.closed(), aborting the parent task would leave the reader
        // stuck in stream.next() and the source would never be dropped.
        struct ParkingForeverStream {
            yielded: bool,
            log: Arc<CommitLog>,
        }
        impl futures::Stream for ParkingForeverStream {
            type Item = Result<ChangeEnvelope, CdcStreamError>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if self.yielded {
                    // Pending forever — never registers a waker.
                    Poll::Pending
                } else {
                    self.yielded = true;
                    let env = make_tracked_envelope(1, Arc::clone(&self.log), false);
                    Poll::Ready(Some(Ok(env)))
                }
            }
        }

        let task = make_refresh_task(make_mem_table() as Arc<dyn TableProvider>);
        let log = CommitLog::new();
        let drop_signal = Arc::new(Notify::new());

        let parking = ParkingForeverStream {
            yielded: false,
            log: Arc::clone(&log),
        };
        let drop_signaling = DropSignalStream {
            inner: Box::pin(parking),
            notify_on_drop: Arc::clone(&drop_signal),
        };
        let stream: ChangesStream = drop_signaling.boxed();

        let join = tokio::spawn(async move {
            run_changes_stream(&task, stream, None, Arc::new(AtomicBool::new(false))).await
        });

        // Wait for the first envelope to commit so we know the apply loop is
        // active and the reader is now parked in stream.next().
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !log.ids().await.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "first envelope never committed",
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Register the drop notifier BEFORE aborting. `Notify::notify_waiters`
        // does not buffer — if we created the `notified()` future after
        // `abort()` returned, the reader could already have torn down the
        // stream and called `notify_waiters` with no waiters registered,
        // which would lose the signal and make this test wait the full
        // timeout for nothing.
        let dropped_fut = drop_signal.notified();
        tokio::pin!(dropped_fut);

        // Abort the parent task. This drops `rx`, which closes `tx`, which
        // must wake the reader's `tokio::select!` and cause it to exit —
        // dropping the source stream as it goes. Without the select-on-
        // tx.closed() guard, the reader would remain alive forever holding
        // the source.
        join.abort();

        let dropped = tokio::time::timeout(Duration::from_secs(2), &mut dropped_fut)
            .await
            .is_ok();
        assert!(
            dropped,
            "reader task did not drop its source stream within 2s after parent abort — \
             this regression would leak source connections at shutdown"
        );
    }

    #[test]
    fn test_get_primary_key_value_null_int32_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let batch =
            RecordBatch::try_new(schema, vec![id_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "id");
        let err =
            result.expect_err("NULL primary key should return an error, not silently produce 0");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("NULL"),
            "Error should mention NULL: {err_msg}"
        );
    }

    #[test]
    fn test_get_primary_key_value_null_utf8_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec![Option::<&str>::None]));
        let batch =
            RecordBatch::try_new(schema, vec![name_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "name");
        assert!(
            result.is_err(),
            "NULL primary key should return an error, not silently produce empty string"
        );
    }

    #[test]
    fn test_get_primary_key_value_non_null_succeeds() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let batch =
            RecordBatch::try_new(schema, vec![id_array]).expect("Failed to create RecordBatch");

        let result = get_primary_key_value(&batch, "id");
        assert!(result.is_ok(), "Non-null PK should succeed");
        let (str_val, _expr) = result.expect("already asserted Ok");
        assert_eq!(str_val, "42");
    }
}
