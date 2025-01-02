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

pub(crate) struct TableSink {
    pub(super) table_provider: Arc<dyn TableProvider>,
}

impl TableSink {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self { table_provider }
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

        Ok(())
    }
}
