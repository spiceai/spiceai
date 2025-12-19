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
use crate::datafusion::error::find_datafusion_root;
use crate::{dataupdate::StreamingDataUpdateExecutionPlan, status};
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use cache::Caching;
use data_components::cdc::{self, ChangeBatch, ChangeOperation, ChangesStream};
use data_components::delete::{DeletionTableProvider, get_deletion_provider};
#[cfg(feature = "dynamodb")]
use data_components::dynamodb::stream::StreamError as DynamoDBStreamError;
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
                            // Mark the dataset as ready if possible
                            if change_envelope.is_dataset_ready() {
                                initial_load_completed.store(true, Ordering::Relaxed);
                                if let Some(ready_sender) = ready_sender.as_ref() {
                                    ready_sender.notify_waiters();
                                }
                                self.update_component_status(status::ComponentStatus::Ready)
                                    .await;
                            }

                            if let Err(e) = change_envelope.commit()
                                && !self.runtime_status.is_shutdown()
                            {
                                tracing::error!("Failed to commit CDC change envelope: {e}");
                            }

                            if let Some(cache_provider_ref) = caching.as_ref()
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

        if !self.runtime_status.is_shutdown() {
            tracing::warn!("Changes stream ended for dataset {dataset_name}");
        }

        Ok(())
    }

    async fn write_change(
        &self,
        change_batch: ChangeBatch,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let deletion_provider = get_deletion_provider(Arc::clone(&self.accelerator))
            .context(crate::accelerated_table::AcceleratedTableDoesntSupportDeleteSnafu)?;

        let sub_batches = group_into_sub_batches(&change_batch);

        tracing::trace!(
            "Processing append/change stream batch: dataset={}, rows={}, sub-batches={}",
            self.dataset_name,
            change_batch.record.num_rows(),
            sub_batches.len()
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

        if let Some(ref callback) = self.on_stream_batch_process_callback {
            let mut callback_guard = callback.lock().await;
            let future = callback_guard();
            future.await;
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
            row_indices
                .iter()
                .filter_map(|&i| u32::try_from(i).ok())
                .collect::<Vec<_>>(),
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
            let primary_key_log_fmt = get_primary_key_log_fmt(&inner_data, &primary_keys)?;
            let delete_where_exprs = get_delete_where_expr(&inner_data, primary_keys)?;

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
}

fn get_primary_key_log_fmt(
    data: &RecordBatch,
    primary_keys: &[String],
) -> crate::accelerated_table::Result<String> {
    primary_keys
        .iter()
        .map(|key| {
            let (value, _) = get_primary_key_value(data, key)?;
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
        let (_, expr_val) = get_primary_key_value(data, &primary_key)?;
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
/// Returns a vector of (`operation_type`, `row_indices`) tuples
#[must_use]
fn group_into_sub_batches(change_batch: &ChangeBatch) -> Vec<(ChangeOperationType, Vec<usize>)> {
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
        let primary_keys = match get_primary_key_log_fmt(&row, &primary_keys_columns) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::error!("Failed to get primary key log format for row {row_id}: {e}");
                continue;
            }
        };

        let should_split = if let Some(current_type) = current_op_type {
            current_type != op_type || (seen_primary_keys.contains(&primary_keys))
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
        seen_primary_keys.insert(primary_keys);
    }

    if !current_batch_indices.is_empty()
        && let Some(op_type) = current_op_type
    {
        sub_batches.push((op_type, current_batch_indices));
    }

    sub_batches
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
    use data_components::cdc::changes_schema;
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
}
