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

use super::{RefreshTask, include_source_to_table_name, inner_err_from_retry_ref};
use crate::accelerated_table::refresh::Refresh;
use crate::accelerated_table::{
    ChangesAfterLoadCheckDatasetSnafu, Error, Result as AcceleratedTableResult,
};
use crate::dataconnector::changes_after_load::{ChangesAfterLoadCoordinator, DatasetStatus};
use cache::Caching;
use snafu::ResultExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::SystemTime;
use tokio::sync::{Notify, RwLock};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

impl RefreshTask {
    pub async fn coordinate_changes_after_load(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        coordinator: Arc<dyn ChangesAfterLoadCoordinator>,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> AcceleratedTableResult<()> {
        let federated_provider = self.federated.table_provider().await;

        let ctx = &Self::create_refresh_df_context(
            Arc::clone(&federated_provider),
            &self.dataset_name,
            &self.accelerator,
            self.disable_federation,
            self.io_runtime.clone(),
        )
        .await;

        let refresh_cloned = Arc::clone(&refresh);
        let refresh_guard = refresh_cloned.read().await;
        let max_retries = if refresh_guard.retry_enabled {
            refresh_guard.retry_max_attempts
        } else {
            Some(0)
        };

        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(max_retries)
            .build();

        match coordinator
            .check_dataset_status(ctx)
            .await
            .context(ChangesAfterLoadCheckDatasetSnafu)?
        {
            DatasetStatus::Bootstrap => {
                retry(retry_strategy, {
                    let coordinator = Arc::clone(&coordinator);
                    let refresh = Arc::clone(&refresh);
                    let caching = caching.clone();
                    let ready_sender = ready_sender.clone();
                    let initial_load_completed = Arc::clone(&initial_load_completed);

                    move || {
                        let coordinator = Arc::clone(&coordinator);
                        let refresh = Arc::clone(&refresh);
                        let caching = caching.clone();
                        let ready_sender = ready_sender.clone();
                        let initial_load_completed = Arc::clone(&initial_load_completed);

                        async move {
                            self.cold_start(
                                coordinator,
                                refresh,
                                caching,
                                ready_sender,
                                initial_load_completed,
                            )
                            .await
                        }
                    }
                });
            }
            DatasetStatus::ChangesStream => {
                let () = self
                    .changes_after_load_stream(
                        coordinator,
                        refresh,
                        caching,
                        ready_sender,
                        initial_load_completed,
                    )
                    .await;
            }
        }

        Ok(())
    }

    async fn cold_start(
        &self,
        coordinator: Arc<dyn ChangesAfterLoadCoordinator>,
        refresh: Arc<RwLock<Refresh>>,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> Result<(), RetryError<Error>> {
        // Step 1. Initialize changes stream
        let deferred_stream = coordinator.initialize_deferred_changes_stream().await;

        // Step 2. Ingest initial data
        self.cold_start_ingestion(
            Arc::clone(&refresh),
            ready_sender,
            Arc::clone(&initial_load_completed),
        )
        .await?;

        // Step 3. Start changes stream
        tracing::debug!(
            "Starting changes_stream for {} {}",
            self.component_type(),
            self.dataset_name,
        );

        self.start_changes_stream(
            refresh,
            deferred_stream.changes_stream(),
            caching,
            None,
            initial_load_completed,
        )
        .await
        .map_err(RetryError::permanent)
    }

    async fn cold_start_ingestion(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> Result<(), RetryError<Error>> {
        let start_time = SystemTime::now();

        let refresh = refresh.read().await;

        let streaming_data_update = self
            .get_full_or_incremental_append_update(&refresh, None)
            .await?;

        self.write_streaming_data_update(
            Some(start_time),
            streaming_data_update,
            refresh.sql.as_deref(),
        )
        .await
        .inspect_err(|e| {
            tracing::warn!(
                "Failed to write data for {} {}: {}",
                self.component_type(),
                include_source_to_table_name(&self.dataset_name, self.federated_source.as_deref()),
                inner_err_from_retry_ref(e)
            );
        })?;

        if let Some(ready_sender) = ready_sender.as_ref() {
            ready_sender.notify_waiters();
        }
        initial_load_completed.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn changes_after_load_stream(
        &self,
        coordinator: Arc<dyn ChangesAfterLoadCoordinator>,
        refresh: Arc<RwLock<Refresh>>,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) {
        let changes_stream = coordinator.changes_stream();

        tracing::debug!(
            "Starting changes_stream for {} {}",
            self.component_type(),
            self.dataset_name,
        );

        if let Err(err) = self
            .start_changes_stream(
                refresh,
                changes_stream,
                caching,
                ready_sender,
                initial_load_completed,
            )
            .await
        {
            tracing::error!("Changes stream failed with error: {err}");
        }
    }
}
