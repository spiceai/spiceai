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
use crate::dataupdate::{DataUpdate, UpdateType};
use crate::status;
use async_stream::stream;
use cache::QueryResultsCacheProvider;
use datafusion::physical_plan::ExecutionPlanProperties;
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{oneshot, RwLock};

impl RefreshTask {
    pub async fn start_streaming_append(
        &self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
        ready_sender: Option<oneshot::Sender<()>>,
        refresh: Arc<RwLock<Refresh>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated_table::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let sql = refresh.read().await.sql.clone();

        self.mark_dataset_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;

        let mut stream = Box::pin(self.get_append_stream().await);

        let mut ready_sender = ready_sender;

        while let Some(update) = stream.next().await {
            match update {
                Ok((start_time, data_update)) => {
                    // write_data_update updates dataset status and logs errors so we don't do this here
                    let sql = refresh.read().await.sql.clone();
                    if self
                        .write_data_update(sql, start_time, data_update)
                        .await
                        .is_ok()
                    {
                        if let Some(ready_sender) = ready_sender.take() {
                            ready_sender.send(()).ok();
                        }
                        initial_load_completed.store(true, Ordering::Relaxed);

                        if let Some(cache_provider) = &cache_provider {
                            if let Err(e) = cache_provider
                                .invalidate_for_table(dataset_name.clone())
                                .await
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
                    tracing::error!("Error getting update for dataset {dataset_name}: {e}");
                    let sql = refresh.read().await.sql.clone();
                    self.mark_dataset_status(sql.as_deref(), status::ComponentStatus::Error)
                        .await;
                }
            }
        }

        Ok(())
    }

    async fn get_append_stream(
        &self,
    ) -> impl Stream<Item = crate::accelerated_table::Result<(Option<SystemTime>, DataUpdate)>>
    {
        let federated = self.federated.table_provider().await;
        let ctx = self.refresh_df_context(Arc::clone(&federated));
        let dataset_name = self.dataset_name.clone();

        stream! {
            let plan = federated
                .scan(&ctx.state(), None, &[], None)
                .await
            .map_err(find_datafusion_root)
                .context(crate::accelerated_table::UnableToScanTableProviderSnafu {})?;

            if plan.output_partitioning().partition_count() > 1 {
                tracing::error!(
                    "Append mode is not supported for datasets with multiple partitions: {dataset_name}"
                );
                return;
            }

            let schema = federated.schema();

            let mut stream = plan
                .execute(0, ctx.task_ctx())
            .map_err(find_datafusion_root)
                .context(crate::accelerated_table::UnableToScanTableProviderSnafu {})?;
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        yield Ok((None, DataUpdate {
                            schema: Arc::clone(&schema),
                            data: vec![batch],
                            update_type: UpdateType::Append,
                        }));
                    }
                    Some(Err(e)) => {
                        tracing::error!("Error reading data for dataset {dataset_name}: {e}");
                        yield Err(crate::accelerated_table::Error::UnableToScanTableProvider { source: find_datafusion_root(e) });
                    }
                    None => {
                        tracing::warn!("Append stream ended for dataset {dataset_name}");
                        break;
                    },
                }
            }
        }
    }
}
