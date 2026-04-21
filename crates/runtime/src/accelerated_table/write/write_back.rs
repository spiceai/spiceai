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

//! Write-back execution path for [`WriteMode::WriteBack`].
//!
//! Writes are applied to the local accelerator first (fast path, returning
//! to the caller once the accelerator commit completes), then asynchronously
//! forwarded to the federated source. The federated source may lag briefly;
//! failures to persist back to the source are logged but do not affect the
//! synchronous response.
//!
//! Implemented as a [`DataSink`] so that:
//!
//! 1. The write only occurs when the returned [`ExecutionPlan`] is executed,
//!    not merely planned. If the caller cancels before execution, neither
//!    the accelerator nor the federated source is modified.
//! 2. The input batches are consumed exactly once — the same batches written
//!    to the accelerator are forwarded to the federated source, so the two
//!    sides cannot diverge due to non-deterministic input plans.
//!
//! [`WriteMode::WriteBack`]: super::WriteMode::WriteBack

use std::any::Any;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::source::DataSourceExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::StreamExt;

use crate::accelerated_table::refresh::Refresher;
use crate::federated_table::FederatedTable;

/// Creates a `DataSinkExec` plan for write-back inserts.
pub(crate) fn insert_write_back(
    input: Arc<dyn ExecutionPlan>,
    overwrite: InsertOp,
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
    refresher: Arc<Refresher>,
    schema: SchemaRef,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let sink = Arc::new(WriteBackDataSink {
        accelerator,
        federated,
        refresher,
        overwrite,
        schema,
    });
    Ok(Arc::new(DataSinkExec::new(input, sink, None)))
}

struct WriteBackDataSink {
    accelerator: Arc<dyn TableProvider>,
    federated: Arc<FederatedTable>,
    refresher: Arc<Refresher>,
    overwrite: InsertOp,
    schema: SchemaRef,
}

impl std::fmt::Debug for WriteBackDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBackDataSink").finish_non_exhaustive()
    }
}

impl DisplayAs for WriteBackDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteBackDataSink")
    }
}

#[async_trait]
impl DataSink for WriteBackDataSink {
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
        // Consume the input stream exactly once and buffer the batches so they
        // can be replayed to both the accelerator (synchronously) and the
        // federated source (asynchronously). This guarantees both sides see
        // identical data and the input plan is never executed twice.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut row_count: u64 = 0;
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            row_count = row_count.saturating_add(batch.num_rows() as u64);
            batches.push(batch);
        }

        // Write to the accelerator synchronously. The caller blocks until this
        // completes, matching the "write reaches local storage before the
        // response is returned" contract of write-back caching.
        execute_insert(
            Arc::clone(&self.accelerator),
            Arc::clone(&self.schema),
            batches.clone(),
            self.overwrite,
        )
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Write-back: failed to persist write to accelerator: {e}"
            ))
        })?;

        self.refresher.set_initial_load_completed(true);

        // Forward the same buffered batches to the federated source in the
        // background. Failures are logged but do not affect the synchronous
        // response.
        let federated = Arc::clone(&self.federated);
        let schema = Arc::clone(&self.schema);
        let overwrite = self.overwrite;
        tokio::spawn(async move {
            let federated_provider = federated.table_provider().await;
            if let Err(e) = execute_insert(federated_provider, schema, batches, overwrite).await {
                tracing::error!("Write-back: failed to persist write to federated source: {e}");
            }
        });

        Ok(row_count)
    }
}

/// Builds an in-memory execution plan from buffered batches and executes
/// an `insert_into` against the supplied table provider.
async fn execute_insert(
    table: Arc<dyn TableProvider>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    overwrite: InsertOp,
) -> DataFusionResult<()> {
    let memory_source = MemorySourceConfig::try_new(&[batches], schema, None)?;
    let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(memory_source)));

    let ctx = SessionContext::new();
    let plan = table.insert_into(&ctx.state(), input, overwrite).await?;
    let _ = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}
