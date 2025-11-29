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
use std::collections::HashSet;
use super::RefreshTask;
use crate::accelerated_table::refresh::Refresh;
use crate::datafusion::error::find_datafusion_root;
use crate::{dataupdate::StreamingDataUpdateExecutionPlan, status};
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use cache::Caching;
use data_components::cdc::{self, ChangeBatch, ChangeOperation, ChangesStream};
use data_components::delete::{get_deletion_provider, DeletionTableProvider};
#[cfg(any(feature = "debezium", feature = "kafka"))]
use data_components::kafka::{
    Error as KafkaError, rdkafka::error::KafkaError as RdKafkaError,
    rdkafka::types::RDKafkaErrorCode,
};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{Expr, col};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::{execution::context::SessionContext, physical_plan::collect};
use futures::{StreamExt, stream};
use snafu::{OptionExt, ResultExt};
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
        Ok((key_col.value(0).to_string(), lit(key_col.value(0))))
    }};
}

impl RefreshTask {
    pub async fn start_changes_stream(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        mut changes_stream: ChangesStream,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let sql = refresh.read().await.sql.clone();

        self.set_refresh_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        while let Some(update) = changes_stream.next().await {
            match update {
                Ok(change_envelope) => {
                    match self
                        .write_change(change_envelope.change_batch.clone())
                        .await
                    {
                        Ok(()) => {
                            if let Some(ready_sender) = ready_sender.as_ref() {
                                ready_sender.notify_waiters();
                            }
                            initial_load_completed.store(true, Ordering::Relaxed);

                            // Mark the dataset as ready after the first message is received. This covers both streaming append and CDC modes.
                            self.update_component_status(status::ComponentStatus::Ready)
                                .await;

                            if let Err(e) = change_envelope.commit() {
                                if !self.runtime_status.is_shutdown() {
                                    tracing::error!("Failed to commit CDC change envelope: {e}");
                                }
                            }

                            if let Some(cache_provider_ref) = caching.as_ref() {
                                // No cache provider means runtime is shutting down and cache is already cleaned up
                                if let Some(cache_provider) = cache_provider_ref.upgrade()
                                    && let Err(e) =
                                        cache_provider.invalidate_for_table(dataset_name.clone())
                                {
                                    if !self.runtime_status.is_shutdown() {
                                        tracing::error!(
                                            "Failed to invalidate cached results for dataset {}: {e}",
                                            &dataset_name.to_string()
                                        );
                                    }

                                }
                            }
                        }
                        Err(e) => {
                            self.set_refresh_status(
                                refresh.read().await.sql.clone().as_deref(),
                                status::ComponentStatus::Error,
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

                    self.set_refresh_status(
                        refresh.read().await.sql.clone().as_deref(),
                        status::ComponentStatus::Error,
                    )
                    .await;
                }
            }
        }

        tracing::warn!("Changes stream ended for dataset {dataset_name}");

        Ok(())
    }

    async fn write_change(
        &self,
        change_batch: ChangeBatch,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let deletion_provider = get_deletion_provider(Arc::clone(&self.accelerator))
            .context(crate::accelerated_table::AcceleratedTableDoesntSupportDeleteSnafu)?;

        let sub_batches = Self::group_into_sub_batches(&change_batch);

        tracing::trace!(
            "Processing append/change stream batch: dataset={}, rows={}, sub-batches={}",
            self.dataset_name, change_batch.record.num_rows(), sub_batches.len()
        );

        for (op_type, row_indices) in sub_batches {
            match op_type {
                ChangeOperationType::Delete => {
                    self.process_delete_batch(&change_batch, &row_indices, &deletion_provider)
                        .await?;
                }
                ChangeOperationType::Upsert => {
                    self.process_upsert_batch(&change_batch, &row_indices)
                        .await?;
                }
                ChangeOperationType::Truncate => {
                    tracing::warn!("Truncate operation not yet implemented for {dataset_name}");
                }
                ChangeOperationType::Unknown => {
                    tracing::error!("Unknown change operation type for {dataset_name}");
                }
            }
        }

        Ok(())
    }

    async fn process_upsert_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = &self.dataset_name;

        tracing::trace!(
            "Processing upsert batch for {dataset_name} with {} rows",
            row_indices.len()
        );

        let data_batch = change_batch.data_batch();

        let indices_array = UInt32Array::from(
            row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );

        let selected_columns: Vec<ArrayRef> = data_batch
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
            .collect::<Result<Vec<_>, _>>()
            .context(crate::accelerated_table::FailedToBuildRecordBatchSnafu)?;

        let selected_batch = RecordBatch::try_new(data_batch.schema(), selected_columns)
            .context(crate::accelerated_table::FailedToBuildRecordBatchSnafu)?;

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let record_batch_stream = Box::pin(RecordBatchStreamAdapter::new(
            selected_batch.schema(),
            Box::pin(stream::once(async move { Ok(selected_batch) })),
        ));

        let insert_plan = self
            .accelerator
            .insert_into(
                &session_state,
                Arc::new(StreamingDataUpdateExecutionPlan::new(record_batch_stream)),
                InsertOp::Append,
            )
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        collect(insert_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        Ok(())
    }

    async fn process_delete_batch(
        &self,
        change_batch: &ChangeBatch,
        row_indices: &[usize],
        deletion_provider: &Arc<dyn DeletionTableProvider>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = &self.dataset_name;

        tracing::trace!(
            "Processing delete batch for {dataset_name} with {} rows",
            row_indices.len()
        );

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let mut all_where_exprs = Vec::new();

        for &row in row_indices {
            let inner_data = change_batch.data(row);
            let primary_keys = change_batch.primary_keys(row);
            let primary_key_log_fmt = Self::get_primary_key_log_fmt(&inner_data, &primary_keys).unwrap();
            let delete_where_exprs = Self::get_delete_where_expr(&inner_data, primary_keys)?;

            tracing::trace!("Deleting data for {dataset_name} where {primary_key_log_fmt}");
            all_where_exprs.extend(delete_where_exprs);
        }

        let delete_plan = deletion_provider
            .delete_from(&session_state, &all_where_exprs)
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        collect(delete_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        Ok(())
    }

    fn get_primary_key_log_fmt(
        data: &RecordBatch,
        primary_keys: &[String],
    ) -> crate::accelerated_table::Result<String> {
        primary_keys
            .iter()
            .map(|key| {
                let (value, _) = Self::get_primary_key_value(data, key)?;
                Ok(format!("{key}={value}"))
            })
            .collect::<crate::accelerated_table::Result<Vec<String>>>()
            .map(|keys| keys.join(", "))
    }

    fn get_delete_where_expr(
        data: &RecordBatch,
        primary_keys: Vec<String>,
    ) -> crate::accelerated_table::Result<Vec<Expr>> {
        let mut delete_where_exprs: Vec<Expr> = vec![];

        for primary_key in primary_keys {
            let (_, expr_val) = Self::get_primary_key_value(data, &primary_key)?;
            delete_where_exprs.push(col(primary_key).eq(expr_val));
        }

        Ok(delete_where_exprs)
    }

    fn get_primary_key_value(
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
    /// Returns a vector of (operation_type, row_indices) tuples
    #[must_use]
    pub fn group_into_sub_batches(change_batch: &ChangeBatch) -> Vec<(ChangeOperationType, Vec<usize>)> {
        if change_batch.record.num_rows() == 0 {
            return vec![];
        }

        let mut sub_batches = Vec::new();
        let mut current_batch_indices = Vec::new();
        let mut current_op_type: Option<ChangeOperationType> = None;
        let mut seen_primary_keys: HashSet<String> = HashSet::new();

        for row_id in 0..change_batch.record.num_rows() {
            let row = change_batch.data(row_id);
            let op = change_batch.op(row_id);
            let op_type = ChangeOperationType::from_operation(&op);
            let primary_keys_columns = change_batch.primary_keys(row_id);
            let primary_keys = Self::get_primary_key_log_fmt(&row, &primary_keys_columns).unwrap();

            let should_split = if let Some(current_type) = current_op_type {
                current_type != op_type
                    || (seen_primary_keys.contains(&primary_keys))
            } else {
                false
            };

            if should_split {
                if !current_batch_indices.is_empty() {
                    sub_batches.push((current_op_type.unwrap(), current_batch_indices.clone()));
                }

                current_batch_indices.clear();
                seen_primary_keys.clear();
                current_op_type = Some(op_type);
            } else if current_op_type.is_none() {
                current_op_type = Some(op_type);
            }

            current_batch_indices.push(row_id);
            seen_primary_keys.insert(primary_keys);
        }

        if !current_batch_indices.is_empty() {
            sub_batches.push((current_op_type.unwrap(), current_batch_indices));
        }

        sub_batches
    }
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

    tracing::error!("Changes stream error for {dataset_name}: {err}");
    StreamErrorType::Fatal
}
