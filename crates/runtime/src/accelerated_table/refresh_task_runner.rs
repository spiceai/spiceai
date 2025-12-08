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
    metrics, refresh::RefreshOverrides, refresh_task::RefreshTask,
    synchronized_table::SynchronizedTable,
};
use futures::{FutureExt, future::BoxFuture};
use tokio::{
    runtime::Handle,
    select,
    sync::{
        Semaphore,
        mpsc::{self, Receiver, Sender},
    },
    task::JoinHandle,
};

use std::{any::Any, panic::AssertUnwindSafe, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use super::refresh::Refresh;
use datafusion::{datasource::TableProvider, sql::TableReference};
use opentelemetry::KeyValue;
use spicepod::metric::Metrics;

pub struct RefreshTaskRunnerBuilder {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    disable_federation: bool,
    semaphore: Option<Arc<Semaphore>>,
    metrics: Option<Metrics>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    /// Mutex to protect concurrent cache operations (insert, upsert) to the accelerator.
    /// Shared with `CachingAccelerationScanExec`.
    cache_mutex: Arc<Mutex<()>>,
}

impl RefreshTaskRunnerBuilder {
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        cache_mutex: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            refresh,
            accelerator,
            disable_federation: false,
            semaphore: None,
            metrics: None,
            cpu_runtime: None,
            io_runtime,
            resource_monitor: None,
            cache_mutex,
        }
    }

    /// Sets the `disable_federation` flag
    #[must_use]
    pub fn with_disable_federation(mut self, disable: bool) -> Self {
        self.disable_federation = disable;
        self
    }

    #[must_use]
    pub fn with_semaphore(mut self, semaphore: Arc<Semaphore>) -> Self {
        self.semaphore = Some(semaphore);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Option<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn with_cpu_runtime(mut self, runtime: Option<Handle>) -> Self {
        self.cpu_runtime = runtime;
        self
    }

    #[must_use]
    pub fn with_resource_monitor(
        mut self,
        monitor: crate::resource_monitor::ResourceMonitor,
    ) -> Self {
        self.resource_monitor = Some(monitor);
        self
    }

    #[must_use]
    pub fn build(self) -> RefreshTaskRunner {
        let mut refresh_task_builder = RefreshTask::builder(
            self.runtime_status,
            self.dataset_name.clone(),
            self.federated,
            self.federated_source,
            self.accelerator,
            self.io_runtime,
            self.cache_mutex,
        )
        .with_disable_federation(self.disable_federation)
        .with_metrics(self.metrics);

        if let Some(semaphore) = self.semaphore {
            refresh_task_builder = refresh_task_builder.with_semaphore(semaphore);
        }

        refresh_task_builder = refresh_task_builder.with_cpu_runtime(self.cpu_runtime);

        if let Some(resource_monitor) = self.resource_monitor {
            refresh_task_builder = refresh_task_builder.with_resource_monitor(resource_monitor);
        }

        let refresh_task = Arc::new(refresh_task_builder.build());

        RefreshTaskRunner {
            dataset_name: self.dataset_name,
            refresh: self.refresh,
            refresh_task,
            task: None,
        }
    }
}

/// `RefreshTaskRunner` is responsible for running all refresh tasks for a dataset. It is expected
/// that only one [`RefreshTaskRunner`] is used per dataset, and that is is the only entity
/// refreshing an `accelerator`.
#[derive(Debug)]
pub struct RefreshTaskRunner {
    dataset_name: TableReference,
    refresh: Arc<RwLock<Refresh>>,
    refresh_task: Arc<RefreshTask>,
    task: Option<JoinHandle<()>>,
}

type RefreshRunFuture =
    BoxFuture<'static, std::result::Result<super::Result<()>, Box<dyn Any + Send>>>;

type RefreshTaskStartSender = Sender<Option<RefreshOverrides>>;
type RefreshTaskCompletionReceiver = Receiver<super::Result<()>>;

impl RefreshTaskRunner {
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn builder(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        cache_mutex: Arc<Mutex<()>>,
    ) -> RefreshTaskRunnerBuilder {
        RefreshTaskRunnerBuilder::new(
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            refresh,
            accelerator,
            io_runtime,
            cache_mutex,
        )
    }

    pub fn start(
        &mut self,
    ) -> super::Result<(RefreshTaskStartSender, RefreshTaskCompletionReceiver)> {
        if self.task.is_some() {
            return Err(super::Error::RefreshTaskAlreadyStarted {});
        }

        let (start_refresh, mut on_start_refresh) = mpsc::channel::<Option<RefreshOverrides>>(1);

        let (notify_refresh_complete, on_refresh_complete) = mpsc::channel::<super::Result<()>>(1);

        let dataset_name = self.dataset_name.clone();
        let notify_refresh_complete = Arc::new(notify_refresh_complete);

        let base_refresh = Arc::clone(&self.refresh);

        let refresh_task = Arc::clone(&self.refresh_task);

        self.task = Some(tokio::spawn(async move {
            let mut task_completion: Option<RefreshRunFuture> = None;

            loop {
                if let Some(task) = task_completion.take() {
                    select! {
                        res = task => {
                            match res {
                                Ok(Ok(())) => {
                                    tracing::debug!("Dataset {dataset_name} refreshed successfully");
                                    if let Err(err) = notify_refresh_complete.send(Ok(())).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                },
                                Ok(Err(err)) => {
                                    tracing::debug!("Dataset {dataset_name} failed to refresh with error: {err}");
                                    if let Err(err) = notify_refresh_complete.send(Err(err)).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                },
                                Err(panic_payload) => {
                                    let dataset_label = dataset_name.to_string();
                                    let panic_message = Self::panic_to_message(panic_payload);
                                    tracing::error!(
                                        dataset = %dataset_label,
                                        %panic_message,
                                        "Refresh worker panicked; continuing refresh loop"
                                    );
                                    metrics::REFRESH_WORKER_PANICS.add(1, &[KeyValue::new("dataset", dataset_label.clone())]);

                                    let panic_error = super::Error::RefreshWorkerPanicked {
                                        dataset_name: dataset_label,
                                        message: panic_message.clone(),
                                    };

                                    if let Err(err) = notify_refresh_complete.send(Err(panic_error)).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                }
                            }
                        },
                        Some(overrides_opt) = on_start_refresh.recv() => {
                            let request = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt).await;
                            task_completion = Some(Self::wrap_refresh_future(Arc::clone(&refresh_task), request));
                        }
                    }
                } else {
                    select! {
                        Some(overrides_opt) = on_start_refresh.recv() => {
                            let request = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt).await;
                            task_completion = Some(Self::wrap_refresh_future(Arc::clone(&refresh_task), request));
                        }
                        else => {
                            // The parent refresher is shutting down, we should too
                            break;
                        }
                    }
                }
            }
        }));

        Ok((start_refresh, on_refresh_complete))
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

    fn wrap_refresh_future(refresh_task: Arc<RefreshTask>, request: Refresh) -> RefreshRunFuture {
        Box::pin(AssertUnwindSafe(async move { refresh_task.run(request).await }).catch_unwind())
    }

    fn panic_to_message(panic: Box<dyn Any + Send>) -> String {
        match panic.downcast::<String>() {
            Ok(message) => *message,
            Err(panic) => match panic.downcast::<&'static str>() {
                Ok(message) => (*message).to_string(),
                Err(_) => "refresh worker panicked with a non-string payload".to_string(),
            },
        }
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
