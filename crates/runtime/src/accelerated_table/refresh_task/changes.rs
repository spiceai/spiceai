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
use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;
use cache::Caching;
use data_components::cdc::readiness::Readiness;
use data_components::cdc::{ChangeBatch, ChangeOperation, ChangesStream};
use data_components::delete::get_deletion_provider;
use datafusion::common::{Constraint, Constraints};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{Expr, col};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{execution::context::SessionContext, physical_plan::collect};
use datafusion_table_providers::util::constraints::{
    UpsertOptions, validate_batch_with_constraints,
};
use futures::stream;
use snafu::{OptionExt, ResultExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio_stream::StreamExt;

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
        changes_stream: ChangesStream,
        readiness: Readiness,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let sql = refresh.read().await.sql.clone();

        self.set_refresh_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        let runtime_status = Arc::clone(&self.runtime_status);
        let dataset_name_readiness = dataset_name.clone();
        tokio::spawn(async move {
            readiness.wait_until_ready().await;
            tracing::info!("Dataset {dataset_name_readiness} is ready");
            if let Some(ready_sender) = ready_sender.as_ref() {
                ready_sender.notify_waiters();
            }
            initial_load_completed.store(true, Ordering::Relaxed);
            runtime_status.update_dataset(&dataset_name_readiness, status::ComponentStatus::Ready);
        });

        let mut chunked_stream =
            Box::pin(changes_stream.chunks_timeout(10000, Duration::from_secs(1)));

        while let Some(update) = chunked_stream.next().await {
            match update.into_iter().collect::<Result<Vec<_>, _>>() {
                Ok(change_envelopes) => {
                    let change_batches: Vec<ChangeBatch> = change_envelopes
                        .iter()
                        .map(|envelope| envelope.change_batch.clone())
                        .collect();

                    match self.write_changes(change_batches).await {
                        Ok(()) => {
                            for change_envelope in change_envelopes {
                                if let Err(e) = change_envelope.commit() {
                                    tracing::debug!("Failed to commit CDC change envelope: {e}");
                                }
                            }

                            if let Some(cache_provider_ref) = caching.as_ref() {
                                // No cache provider means runtime is shutting down and cache is already cleaned up
                                if let Some(cache_provider) = cache_provider_ref.upgrade() {
                                    if let Err(e) =
                                        cache_provider.invalidate_for_table(dataset_name.clone())
                                    {
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
                            tracing::error!("Error writing changes for {dataset_name}: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Changes stream error for {dataset_name}: {e}");
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

    async fn write_changes(
        &self,
        change_batches: Vec<ChangeBatch>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();

        // Separate deletes from upserts
        let mut delete_operations = Vec::new();
        let mut upsert_operations = Vec::new();

        for change_batch in change_batches {
            for row in 0..change_batch.record.num_rows() {
                let op = change_batch.op(row);
                match op {
                    ChangeOperation::Delete => {
                        let inner_data: RecordBatch = change_batch.data(row);
                        let primary_keys = change_batch.primary_keys(row);
                        delete_operations.push((inner_data, primary_keys));
                    }
                    ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => {
                        let inner_data: RecordBatch = change_batch.data(row);
                        let primary_keys = change_batch.primary_keys(row);
                        upsert_operations.push((inner_data, primary_keys));
                    }
                    _ => {
                        tracing::error!("Unknown change operation {op} for {dataset_name}");
                    }
                }
            }
        }

        // Process all delete operations first
        if !delete_operations.is_empty() {
            self.execute_delete_operations(delete_operations).await?;
        }

        // Process upsert operations with batching to handle conflicts
        if !upsert_operations.is_empty() {
            self.write_upserts_with_batching(upsert_operations).await?;
        }

        Ok(())
    }

    async fn execute_delete_operations(
        &self,
        delete_operations: Vec<(RecordBatch, Vec<String>)>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let deletion_provider = get_deletion_provider(Arc::clone(&self.accelerator))
            .context(crate::accelerated_table::AcceleratedTableDoesntSupportDeleteSnafu)?;

        // Build OR'ed delete conditions for all operations
        let mut all_delete_conditions = Vec::new();
        let mut log_messages = Vec::new();

        for (inner_data, primary_keys) in delete_operations {
            let primary_key_log_fmt = Self::get_primary_key_log_fmt(&inner_data, &primary_keys)?;
            let delete_where_exprs = Self::get_delete_where_expr(&inner_data, primary_keys)?;

            log_messages.push(primary_key_log_fmt);

            // Create an AND condition for this row's primary key constraints
            let row_condition = if delete_where_exprs.len() == 1 {
                delete_where_exprs.into_iter().next().unwrap()
            } else {
                delete_where_exprs
                    .into_iter()
                    .reduce(|acc, expr| acc.and(expr))
                    .unwrap()
            };

            all_delete_conditions.push(row_condition);
        }

        if all_delete_conditions.is_empty() {
            return Ok(());
        }

        // Combine all row conditions with OR
        let final_condition = all_delete_conditions
            .into_iter()
            .reduce(|acc, expr| acc.or(expr))
            .unwrap();

        tracing::info!(
            "Deleting data for {dataset_name} where: {}",
            log_messages.join(" OR ")
        );

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let delete_plan = deletion_provider
            .delete_from(&session_state, &[final_condition])
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        collect(delete_plan, ctx.task_ctx())
            .await
            .map_err(find_datafusion_root)
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        Ok(())
    }

    async fn write_upserts_with_batching(
        &self,
        upsert_operations: Vec<(RecordBatch, Vec<String>)>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();

        // Start with the full batch of upsert operations
        let mut operation_batches = vec![upsert_operations];

        loop {
            let Some(next_batch) = operation_batches.pop() else {
                break;
            };

            let batch_len = next_batch.len();

            match self.execute_upsert_batch(&next_batch).await {
                Ok(()) => {
                    // Batch succeeded, continue to next batch
                }
                Err(e) => {
                    // Check if error indicates a conflict and we can split the batch
                    let error_string = e.to_string();
                    if (error_string.contains("duplicate")
                        || error_string.contains("conflict")
                        || error_string.contains("unique constraint"))
                        || error_string
                            .contains("Incoming data violates uniqueness constraint on column")
                        || error_string.contains("Constraint Violation") && batch_len > 1
                    {
                        // Split the batch in half and retry
                        let mid = batch_len / 2;
                        operation_batches.push(next_batch[mid..].to_vec());
                        operation_batches.push(next_batch[..mid].to_vec());

                        tracing::warn!(
                            "Write to dataset '{}' failed due to conflicting writes. Splitting batch of {} operations and retrying.",
                            dataset_name,
                            batch_len
                        );
                    } else {
                        // Error is not recoverable by splitting, or batch size is 1
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_upsert_batch(
        &self,
        operations: &[(RecordBatch, Vec<String>)],
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();

        if operations.is_empty() {
            return Ok(());
        }

        // Extract record batches and combine them
        let record_batches: Vec<&RecordBatch> = operations.iter().map(|(batch, _)| batch).collect();
        let schema = record_batches[0].schema();

        let total_primary_keys: HashSet<String> = operations
            .iter()
            .flat_map(|(_, primary_keys)| primary_keys.iter().cloned())
            .collect();

        let constraint = Constraint::PrimaryKey(
            total_primary_keys
                .iter()
                .map(|key| schema.index_of(key).unwrap())
                .collect::<Vec<usize>>(),
        );
        let test_constraints = Constraints::new_unverified(vec![constraint]);

        let combined_batch = compute::concat_batches(&schema, record_batches)
            .map_err(|e| DataFusionError::ArrowError(e, None))
            .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

        debug_assert!(
            test_constraints
                == *self
                    .accelerator
                    .constraints()
                    .unwrap_or(&Constraints::empty())
        );

        let Some(combined_batch) = validate_batch_with_constraints(
            vec![combined_batch],
            self.accelerator
                .constraints()
                .unwrap_or(&Constraints::empty()),
            &UpsertOptions::new()
                .with_remove_duplicates(true)
                .with_last_write_wins(true),
        )
        .await
        .unwrap()
        .pop() else {
            panic!("uhoh!");
        };

        if total_primary_keys.is_empty() {
            tracing::debug!(
                "Inserting {} data rows for {dataset_name}",
                operations.len()
            );
        } else {
            tracing::debug!(
                "Upserting {} data rows for {dataset_name}",
                operations.len()
            );
        }

        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let record_batch_stream = Box::pin(RecordBatchStreamAdapter::new(
            combined_batch.schema(),
            Box::pin(stream::once(async { Ok(combined_batch) })),
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
}
