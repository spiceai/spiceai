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

use std::{pin::Pin, sync::Arc};

use datafusion::{
    catalog::TableProvider, execution::RecordBatchStream, logical_expr::dml::InsertOp,
    physical_plan::collect, prelude::SessionContext,
};
use util::RetryError;

use crate::{
    accelerated_table::refresh_task::retry_from_df_error, datafusion::error::find_datafusion_root,
    dataupdate::StreamingDataUpdateExecutionPlan,
};

#[derive(Debug)]
pub(crate) struct TableSink {
    pub(super) table_provider: Arc<dyn TableProvider>,
    pub(super) retention_sql: Option<String>,
}

impl TableSink {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self {
            table_provider,
            retention_sql: None,
        }
    }

    pub fn with_retention_sql(mut self, retention_sql: Option<String>) -> Self {
        self.retention_sql = retention_sql;
        self
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let ctx = SessionContext::new();

        let insertion_plan = match self
            .table_provider
            .insert_into(
                &ctx.state(),
                Arc::new(StreamingDataUpdateExecutionPlan::new(record_batch_stream)),
                overwrite,
            )
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                // Should not retry if we are unable to create execution plan to insert data
                return Err(RetryError::permanent(
                    crate::accelerated_table::Error::FailedToWriteData {
                        source: find_datafusion_root(e),
                    },
                ));
            }
        };

        if let Err(e) = collect(insertion_plan, ctx.task_ctx()).await {
            return Err(retry_from_df_error(e));
        }

        // If retention_sql is specified and this is an append operation with on_conflict, execute the retention SQL
        if let (Some(retention_sql), InsertOp::Append) = (&self.retention_sql, overwrite) {
            tracing::debug!("Executing retention SQL after insert: {}", retention_sql);

            // For DuckDB, we need to execute the retention SQL directly on the connection
            // Since we can't easily access the DuckDB connection from here, we'll need to
            // execute it via the table provider or use a different approach
            //
            // TODO: This is a placeholder for the actual retention SQL execution
            // The proper implementation would require access to the DuckDB connection
            // to execute the retention SQL within the same transaction as the insert
            tracing::warn!(
                "retention_sql execution after insert is not yet fully implemented for append mode with on_conflict"
            );
        }

        Ok(())
    }
}
