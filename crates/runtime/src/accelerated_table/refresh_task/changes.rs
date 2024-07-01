/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::{
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
};
use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use cache::QueryResultsCacheProvider;
use data_components::cdc::{ChangeBatch, ChangesStream};
use data_components::delete::get_deletion_provider;
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{col, Expr};
use datafusion::{execution::context::SessionContext, physical_plan::collect};
use futures::StreamExt;
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;
use tokio::sync::oneshot;

macro_rules! extract_primary_key {
    ($key_col:expr, $key:expr, $data_schema:expr, $array_type:ty, $data_type_str:expr) => {{
        let key_col = $key_col.as_any().downcast_ref::<$array_type>().context(
            crate::accelerated_table::ArrayDataTypeMismatchSnafu {
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
        mut changes_stream: ChangesStream,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
        ready_sender: Option<oneshot::Sender<()>>,
    ) -> crate::accelerated_table::Result<()> {
        self.mark_dataset_status(status::ComponentStatus::Refreshing)
            .await;

        let dataset_name = self.dataset_name.clone();

        let mut ready_sender = ready_sender;

        while let Some(update) = changes_stream.next().await {
            match update {
                Ok(change_envelope) => {
                    match self
                        .write_change(change_envelope.change_batch.clone())
                        .await
                    {
                        Ok(()) => {
                            if let Some(ready_sender) = ready_sender.take() {
                                ready_sender.send(()).ok();
                            }

                            if let Err(e) = change_envelope.commit() {
                                tracing::debug!("Failed to commit CDC change envelope: {e}");
                            }

                            if let Some(cache_provider) = &cache_provider {
                                if let Err(e) = cache_provider
                                    .invalidate_for_table(&dataset_name.to_string())
                                    .await
                                {
                                    tracing::error!(
                                        "Failed to invalidate cached results for dataset {}: {e}",
                                        &dataset_name.to_string()
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            self.mark_dataset_status(status::ComponentStatus::Error)
                                .await;
                            tracing::error!("Error writing change for {dataset_name}: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Changes stream error for {dataset_name}: {e}");
                    self.mark_dataset_status(status::ComponentStatus::Error)
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

        for row in 0..change_batch.record.num_rows() {
            let op = change_batch.op(row);
            match op {
                "d" => {
                    let inner_data: RecordBatch = change_batch.data(row);
                    let primary_keys = change_batch.primary_keys(row);
                    let primary_key_log_fmt =
                        Self::get_primary_key_log_fmt(&inner_data, &primary_keys)?;
                    let delete_where_exprs =
                        Self::get_delete_where_expr(&inner_data, primary_keys)?;

                    tracing::info!("Deleting data for {dataset_name} where {primary_key_log_fmt}");

                    let ctx = SessionContext::new();
                    let session_state = ctx.state();

                    let delete_plan = deletion_provider
                        .delete_from(&session_state, &delete_where_exprs)
                        .await
                        .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

                    collect(delete_plan, ctx.task_ctx())
                        .await
                        .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
                }
                "c" | "u" | "r" => {
                    let inner_data: RecordBatch = change_batch.data(row);
                    let primary_keys = change_batch.primary_keys(row);
                    let ctx = SessionContext::new();
                    let session_state = ctx.state();

                    tracing::info!(
                        "Upserting data row for {dataset_name} with {}",
                        Self::get_primary_key_log_fmt(&inner_data, &primary_keys)?
                    );

                    let insert_plan = self
                        .accelerator
                        .insert_into(
                            &session_state,
                            Arc::new(DataUpdateExecutionPlan::new(DataUpdate {
                                schema: inner_data.schema(),
                                data: vec![inner_data],
                                update_type: UpdateType::Append,
                            })),
                            false,
                        )
                        .await
                        .context(crate::accelerated_table::FailedToWriteDataSnafu)?;

                    collect(insert_plan, ctx.task_ctx())
                        .await
                        .context(crate::accelerated_table::FailedToWriteDataSnafu)?;
                }
                _ => {
                    tracing::error!("Unknown operation {op} for {dataset_name}");
                }
            }
        }

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
            crate::accelerated_table::ExpectedSchemaToHaveFieldSnafu {
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
