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

use super::{inner_err_from_retry_ref, RefreshTask};
use crate::accelerated_table::refresh::Refresh;
use crate::accelerated_table::{
    ChangesAfterLoadBootstrappingSnafu, ChangesAfterLoadCheckDatasetSnafu, Result,
};
use crate::dataconnector::changes_after_load::{ChangesAfterLoadCoordinator, DatasetStatus};
use crate::dataconnector::get_data;
use crate::dataupdate::{StreamingDataUpdate, UpdateType};
use cache::Caching;
use datafusion::catalog::TableProvider;
use snafu::ResultExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::SystemTime;
use tokio::sync::{Notify, RwLock};

impl RefreshTask {
    pub async fn coordinate_changes_after_load(
        &self,
        refresh: Arc<RwLock<Refresh>>,
        coordinator: Arc<dyn ChangesAfterLoadCoordinator>,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> Result<()> {
        let federated_provider = self.federated.table_provider().await;

        let ctx = &Self::create_refresh_df_context(
            Arc::clone(&federated_provider),
            &self.dataset_name,
            &self.accelerator,
            self.disable_federation,
            self.io_runtime.clone(),
        )
        .await;

        match coordinator
            .check_dataset_status(ctx)
            .await
            .context(ChangesAfterLoadCheckDatasetSnafu)?
        {
            DatasetStatus::Bootstrap => {
                self.cold_start(
                    coordinator,
                    federated_provider,
                    refresh,
                    caching,
                    ready_sender,
                    initial_load_completed,
                )
                .await
            }
            DatasetStatus::ChangesStream => {
                self.changes_after_load_stream(
                    coordinator,
                    refresh,
                    caching,
                    ready_sender,
                    initial_load_completed,
                )
                .await
            }
        }
    }

    async fn cold_start(
        &self,
        coordinator: Arc<dyn ChangesAfterLoadCoordinator>,
        federated_provider: Arc<dyn TableProvider>,
        refresh: Arc<RwLock<Refresh>>,
        caching: Option<Weak<Caching>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> Result<()> {
        // Step 1. Initialize changes stream
        let deferred_stream = coordinator.initialize_deferred_changes_stream().await;

        // Step 2. Ingest initial data
        self.cold_start_ingestion(
            federated_provider,
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
    }

    async fn cold_start_ingestion(
        &self,
        federated_provider: Arc<dyn TableProvider>,
        refresh: Arc<RwLock<Refresh>>,
        ready_sender: Option<Arc<Notify>>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> Result<()> {
        let start_time = SystemTime::now();

        let dataset_name = self.dataset_name.clone();

        let mut ctx = Self::create_refresh_df_context(
            Arc::clone(&federated_provider),
            &self.dataset_name,
            &self.accelerator,
            self.disable_federation,
            self.io_runtime.clone(),
        )
        .await;

        let refresh = refresh.read().await;

        let batch_stream = get_data(
            &mut ctx,
            dataset_name,
            federated_provider,
            refresh.sql.clone(),
            vec![],
        )
        .await
        .context(ChangesAfterLoadBootstrappingSnafu)?;

        let streaming_data_update = StreamingDataUpdate::new(batch_stream, UpdateType::Append);

        self.write_streaming_data_update(
            Some(start_time),
            streaming_data_update,
            refresh.sql.as_deref(),
        )
        .await
        .map_err(|e| inner_err_from_retry_ref(&e))
        .context(ChangesAfterLoadBootstrappingSnafu)?;

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
    ) -> Result<()> {
        let changes_stream = coordinator.changes_stream();

        tracing::debug!(
            "Starting changes_stream for {} {}",
            self.component_type(),
            self.dataset_name,
        );

        self.start_changes_stream(
            refresh,
            changes_stream,
            caching,
            ready_sender,
            initial_load_completed,
        )
        .await
    }
}
