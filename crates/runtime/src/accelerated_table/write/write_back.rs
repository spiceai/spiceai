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
//! immediately to the caller), then asynchronously forwarded to the federated
//! source. The federated source may lag briefly; failures to persist back to
//! the source are logged but do not affect the synchronous response.
//!
//! [`WriteMode::WriteBack`]: super::WriteMode::WriteBack

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::accelerated_table::refresh::Refresher;
use crate::federated_table::FederatedTable;

/// Executes a write-back insert: writes synchronously to the accelerator,
/// then spawns a task to forward the write to the federated source.
pub(crate) async fn insert_write_back(
    state: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    overwrite: InsertOp,
    accelerator: &Arc<dyn TableProvider>,
    federated: &Arc<FederatedTable>,
    refresher: &Arc<Refresher>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let accelerated_plan = accelerator
        .insert_into(state, Arc::clone(&input), overwrite)
        .await?;
    refresher.set_initial_load_completed(true);

    let federated = Arc::clone(federated);
    tokio::spawn(async move {
        let federated_provider = federated.table_provider().await;
        let ctx = SessionContext::new();
        let plan = match federated_provider
            .insert_into(&ctx.state(), input, overwrite)
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                tracing::error!(
                    "Write-back: failed to create insert plan for federated source: {e}"
                );
                return;
            }
        };

        let task_ctx = Arc::new(TaskContext::default());
        if let Err(e) = datafusion::physical_plan::collect(plan, task_ctx).await {
            tracing::error!("Write-back: failed to persist write to federated source: {e}");
        }
    });

    Ok(accelerated_plan)
}
