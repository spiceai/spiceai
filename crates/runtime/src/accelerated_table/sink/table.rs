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

use data_components::index_maintenance::perform_index_maintenance;
use datafusion::{
    catalog::TableProvider, execution::RecordBatchStream, logical_expr::dml::InsertOp,
    physical_plan::collect, prelude::SessionContext,
};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion_index::{Index, IndexedTableProvider};
use runtime_table_partition::provider::PartitionTableProvider;
use util::RetryError;

use crate::{
    accelerated_table::refresh_task::retry_from_df_error, datafusion::error::find_datafusion_root,
    dataupdate::StreamingDataUpdateExecutionPlan,
};

#[derive(Debug)]
pub(crate) struct TableSink {
    pub(super) table_provider: Arc<dyn TableProvider>,
    /// Additional indexes that receive write lifecycle hooks (`on_write_start`,
    /// `on_write_failed`, `on_write_complete`) but are **not** stored in the
    /// accelerator-side [`IndexedTableProvider`].
    ///
    /// Used for external indexes (e.g. Elasticsearch) that must be maintained on
    /// the accelerator write path but must never be visible to the query optimizer.
    pub(super) sink_indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl TableSink {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self {
            table_provider,
            sink_indexes: vec![],
        }
    }

    pub fn with_sink_indexes(mut self, indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        self.sink_indexes = indexes;
        self
    }

    async fn providers_for_write_hooks(&self) -> Vec<Arc<dyn TableProvider>> {
        if let Some(p) = self
            .table_provider
            .as_any()
            .downcast_ref::<PartitionTableProvider>()
        {
            p.partition_table_providers().await
        } else {
            vec![Arc::clone(&self.table_provider)]
        }
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated_table::Error>> {
        let start = std::time::Instant::now();
        tracing::debug!(
            "TableSink::insert_into starting (overwrite: {:?})",
            overwrite
        );

        let ctx = SessionContext::new();
        let target_schema = self.table_provider.schema();
        let streaming_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(record_batch_stream));
        let cast_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(streaming_plan, target_schema));

        tracing::debug!("TableSink: calling table_provider.insert_into to create execution plan");
        let plan_start = std::time::Instant::now();
        let insertion_plan = match self
            .table_provider
            .insert_into(&ctx.state(), cast_plan, overwrite)
            .await
        {
            Ok(plan) => {
                tracing::debug!(
                    "TableSink: insert_into returned execution plan in {:.2}s",
                    plan_start.elapsed().as_secs_f64()
                );
                plan
            }
            Err(e) => {
                tracing::error!("TableSink: insert_into failed to create plan: {e}");
                // Should not retry if we are unable to create execution plan to insert data
                return Err(RetryError::permanent(
                    crate::accelerated_table::Error::FailedToWriteData {
                        source: find_datafusion_root(e),
                    },
                ));
            }
        };

        tracing::debug!(
            "TableSink: executing insertion plan with collect() - this will read source and write to accelerator"
        );

        let providers_before_write = self.providers_for_write_hooks().await;

        // Collect all indexes that need write-lifecycle hooks: those embedded in
        // IndexedTableProvider on the accelerator and any extra sink_indexes (e.g.
        // Elasticsearch indexes maintained externally to the accelerator storage).
        let provider_indexes_before: Vec<Arc<dyn Index + Send + Sync>> = providers_before_write
            .iter()
            .filter_map(|p| p.as_any().downcast_ref::<IndexedTableProvider>())
            .flat_map(IndexedTableProvider::get_all_indexes)
            .collect();

        for index in provider_indexes_before
            .iter()
            .chain(self.sink_indexes.iter())
        {
            tracing::debug!("Running on_write_start for index '{}'", index.name());
            if let Err(e) = index.on_write_start().await {
                tracing::warn!(
                    "TableSink: on_write_start failed for index '{}': {e}. Continuing with write.",
                    index.name()
                );
            }
        }

        let collect_start = std::time::Instant::now();
        if let Err(e) = collect(insertion_plan, ctx.task_ctx()).await {
            tracing::debug!(
                "TableSink: collect() failed after {:.2}s: {e}",
                collect_start.elapsed().as_secs_f64()
            );
            run_on_write_failed(&providers_before_write, &self.sink_indexes).await;
            return Err(retry_from_df_error(e));
        }

        // Perform post-write index maintenance (e.g., rebuild hash indexes) if the table supports it.
        // For partitioned tables each partition holds its own IndexedMemTable, so we iterate over
        // all partition providers after the insert has created any new partitions.
        let providers_after_write = self.providers_for_write_hooks().await;
        for provider in &providers_after_write {
            match perform_index_maintenance(provider.as_ref()).await {
                Ok(true) => {
                    tracing::debug!("TableSink: index maintenance completed successfully");
                }
                Ok(false) => {
                    // Table doesn't support index maintenance - this is expected for most tables
                }
                Err(e) => {
                    tracing::warn!(
                        "TableSink: index maintenance failed after write: {e}. Index may be stale until next refresh."
                    );
                    // Don't fail the write - data was successfully written, index rebuild is best-effort
                }
            }
        }

        // Call on_write_complete on every index in all providers and on sink_indexes.
        // Uses IF NOT EXISTS semantics: creates index after overwrite (new table),
        // no-op after append (index already exists). CDC skips this path entirely.
        let provider_indexes_after: Vec<Arc<dyn Index + Send + Sync>> = providers_after_write
            .iter()
            .filter_map(|p| p.as_any().downcast_ref::<IndexedTableProvider>())
            .flat_map(IndexedTableProvider::get_all_indexes)
            .collect();

        for index in provider_indexes_after
            .iter()
            .chain(self.sink_indexes.iter())
        {
            tracing::debug!("Running on_write_complete for index '{}'", index.name());
            if let Err(e) = index.on_write_complete().await {
                tracing::warn!(
                    "TableSink: on_write_complete failed for index '{}': {e}. Index may be stale until next refresh.",
                    index.name()
                );
            }
        }

        tracing::debug!(
            "TableSink::insert_into completed in {:.2}s (collect phase: {:.2}s)",
            start.elapsed().as_secs_f64(),
            collect_start.elapsed().as_secs_f64()
        );
        Ok(())
    }
}

async fn run_on_write_failed(
    providers: &[Arc<dyn TableProvider>],
    sink_indexes: &[Arc<dyn Index + Send + Sync>],
) {
    let provider_indexes: Vec<Arc<dyn Index + Send + Sync>> = providers
        .iter()
        .filter_map(|p| p.as_any().downcast_ref::<IndexedTableProvider>())
        .flat_map(IndexedTableProvider::get_all_indexes)
        .collect();

    for index in provider_indexes.iter().chain(sink_indexes.iter()) {
        if let Err(e) = index.on_write_failed().await {
            tracing::warn!(
                "TableSink: on_write_failed failed for index '{}': {e}. Index write state may need manual cleanup.",
                index.name()
            );
        }
    }
}
