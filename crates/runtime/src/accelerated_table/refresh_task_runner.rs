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

use crate::{federated_table::FederatedTable, status};

use super::{
    refresh::RefreshOverrides, refresh_task::RefreshTask, synchronized_table::SynchronizedTable,
};
use futures::future::BoxFuture;
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use std::sync::Arc;
use tokio::sync::RwLock;

use datafusion::{datasource::TableProvider, sql::TableReference};

use super::refresh::Refresh;

/// `RefreshTaskRunner` is responsible for running all refresh tasks for a dataset. It is expected
/// that only one [`RefreshTaskRunner`] is used per dataset, and that is is the only entity
/// refreshing an `accelerator`.
pub struct RefreshTaskRunner {
    dataset_name: TableReference,
    refresh: Arc<RwLock<Refresh>>,
    refresh_task: Arc<RefreshTask>,
    task: Option<JoinHandle<()>>,
}

impl RefreshTaskRunner {
    #[must_use]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
    ) -> Self {
        let refresh_task = Arc::new(RefreshTask::new(
            runtime_status,
            dataset_name.clone(),
            federated,
            federated_source,
            accelerator,
        ));

        Self {
            dataset_name,
            refresh,
            refresh_task,
            task: None,
        }
    }

    pub fn start(
        &mut self,
    ) -> (
        Sender<Option<RefreshOverrides>>,
        Receiver<super::Result<()>>,
    ) {
        assert!(self.task.is_none());

        let (start_refresh, mut on_start_refresh) = mpsc::channel::<Option<RefreshOverrides>>(1);

        let (notify_refresh_complete, on_refresh_complete) = mpsc::channel::<super::Result<()>>(1);

        let dataset_name = self.dataset_name.clone();
        let notify_refresh_complete = Arc::new(notify_refresh_complete);

        let base_refresh = Arc::clone(&self.refresh);

        let refresh_task = Arc::clone(&self.refresh_task);

        self.task = Some(tokio::spawn(async move {
            let mut task_completion: Option<BoxFuture<super::Result<()>>> = None;

            loop {
                if let Some(task) = task_completion.take() {
                    select! {
                        res = task => {
                            match res {
                                Ok(()) => {
                                    tracing::debug!("Refresh task successfully completed for dataset {dataset_name}");
                                    if let Err(err) = notify_refresh_complete.send(Ok(())).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                },
                                Err(err) => {
                                    tracing::debug!("Refresh task for dataset {dataset_name} failed with error: {err}");
                                    if let Err(err) = notify_refresh_complete.send(Err(err)).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                }
                            }
                        },
                        Some(overrides_opt) = on_start_refresh.recv() => {
                            let request = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt).await;
                            task_completion = Some(Box::pin(refresh_task.run(request)));
                        }
                    }
                } else {
                    select! {
                        Some(overrides_opt) = on_start_refresh.recv() => {
                            let request = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt).await;
                            task_completion = Some(Box::pin(refresh_task.run(request)));
                        }
                        else => {
                            // The parent refresher is shutting down, we should too
                            break;
                        }
                    }
                }
            }
        }));

        (start_refresh, on_refresh_complete)
    }

    /// Subscribes a new acceleration table provider to the existing `AccelerationSink` managed by this `RefreshTask`.
    pub async fn add_synchronized_table(&self, synchronized_table: SynchronizedTable) {
        self.refresh_task
            .add_synchronized_table(synchronized_table)
            .await;
    }

    /// Create a new [`Refresh`] based on defaults and overrides.
    async fn create_refresh_from_overrides(
        defaults: Arc<RwLock<Refresh>>,
        overrides_opt: Option<RefreshOverrides>,
    ) -> Refresh {
        let mut r = defaults.read().await.clone();
        if let Some(overrides) = overrides_opt {
            r = r.with_overrides(&overrides);
        }
        r
    }

    pub fn abort(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
            self.task = None;
        }
    }
}

impl Drop for RefreshTaskRunner {
    fn drop(&mut self) {
        self.abort();
    }
}
