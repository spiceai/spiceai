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

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use crate::{
    accelerated_table::AcceleratedTable,
    component::dataset::{self, acceleration::RefreshMode, Dataset},
    dataaccelerator,
    dataconnector::{
        self,
        localpod::{LocalPodConnector, LOCALPOD_DATACONNECTOR},
        ConnectorComponent, ConnectorParams, ConnectorParamsBuilder, DataConnector,
        DataConnectorError, ODBC_DATACONNECTOR,
    },
    embeddings::connector::EmbeddingConnector,
    error_spaced,
    federated_table::FederatedTable,
    metrics, status,
    tracing_util::dataset_registered_trace,
    warn_spaced, AcceleratedReadWriteTableWithoutReplicationSnafu,
    AcceleratedTableInvalidChangesSnafu, AcceleratorEngineNotAvailableSnafu,
    AcceleratorInitializationFailedSnafu, Error, LogErrors, OdbcNotInstalledSnafu, Result, Runtime,
    UnableToAttachDataConnectorSnafu, UnableToCreateAcceleratedTableSnafu,
    UnableToInitializeDataConnectorSnafu, UnableToLoadDatasetConnectorSnafu,
    UnableToReceiveAcceleratedTableStatusSnafu, UnknownDataConnectorSnafu,
};
use app::App;
use datafusion::sql::TableReference;
use futures::{future::join_all, StreamExt};
use opentelemetry::KeyValue;
use snafu::prelude::*;
use tokio::sync::Semaphore;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

impl Runtime {
    pub(crate) async fn load_datasets(&self) {
        let app_lock = self.app.read().await;
        let Some(app) = app_lock.as_ref() else {
            return;
        };

        // Control the number of parallel dataset loads
        let semaphore = if let Some(parallel_num) = app.runtime.dataset_load_parallelism {
            Arc::new(Semaphore::new(parallel_num))
        } else {
            Arc::new(Semaphore::new(Semaphore::MAX_PERMITS))
        };

        let valid_datasets = Self::get_valid_datasets(app, LogErrors(true));

        if valid_datasets.is_empty() {
            tracing::info!(
                "No datasets were configured. If this is unexpected, check the Spicepod configuration."
            );
            return;
        }

        let initialized_datasets = self.initialize_accelerators(&valid_datasets).await;

        // Create a map of dataset names to their futures
        let mut dataset_futures = HashMap::new();
        let mut localpod_datasets = Vec::new();

        // First create futures for non-localpod datasets
        for ds in initialized_datasets {
            if ds.source() == LOCALPOD_DATACONNECTOR {
                localpod_datasets.push(ds);
                continue;
            }

            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);
            let ds_clone = Arc::clone(&ds);
            let cloned_self = self.clone();
            let future: Pin<Box<dyn Future<Output = ()> + Send>> =
                Box::pin(async move { cloned_self.load_dataset(ds_clone).await })
                    as Pin<Box<dyn Future<Output = ()> + Send>>;
            dataset_futures.insert(ds.name.clone(), future);
        }

        // For each localpod dataset, chain it after its parent's future
        for ds in localpod_datasets {
            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);

            // Get the parent dataset path from the localpod dataset
            let path = ds.path();
            let path_table_ref = TableReference::parse_str(path);

            // Find and remove the parent dataset's future
            if let Some(parent_future) = dataset_futures.remove(&path_table_ref) {
                let ds_clone = Arc::clone(&ds);

                let cloned_self = self.clone();
                // Chain the localpod dataset load after its parent
                let chained_future = Box::pin(async move {
                    parent_future.await;
                    cloned_self.load_dataset(ds_clone).await;
                }) as Pin<Box<dyn Future<Output = ()> + Send>>;

                // Replace parent future with the chained future
                dataset_futures.insert(ds.name.clone(), chained_future);
            } else {
                // Parent doesn't exist, provide an error message to the user
                tracing::error!(
                    "Failed to load localpod dataset '{}': Parent dataset '{}' doesn't exist. \
                    Ensure the '{}' dataset is configured in the Spicepod.",
                    ds.name,
                    path_table_ref,
                    path_table_ref
                );
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
                continue;
            };
        }

        let mut spawned_tasks = vec![];

        for (ds, dataset_load_future) in dataset_futures {
            let semaphore = Arc::clone(&semaphore);
            let handle = tokio::spawn(async move {
                let Ok(_guard) = semaphore.acquire().await else {
                    unreachable!("Semaphore is never closed.");
                };
                tracing::info!("Initializing dataset {ds}");
                dataset_load_future.await;
            });
            spawned_tasks.push(handle);
        }

        let _ = join_all(spawned_tasks).await;

        // After all datasets have loaded, load the views.
        self.load_views(app);
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_datasets(app: &Arc<App>, log_errors: LogErrors) -> Vec<Arc<Dataset>> {
        Self::datasets_iter(app)
            .zip(&app.datasets)
            .filter_map(|(ds, spicepod_ds)| match ds {
                Ok(ds) => Some(Arc::new(ds)),
                Err(e) => {
                    if log_errors.0 {
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        tracing::error!(dataset = &spicepod_ds.name, "{e}");
                    }
                    None
                }
            })
            .collect()
    }

    fn datasets_iter(app: &Arc<App>) -> impl Iterator<Item = Result<Dataset>> + '_ {
        app.datasets
            .clone()
            .into_iter()
            .map(Dataset::try_from)
            .map(move |ds| ds.map(|ds| Dataset::with_app(ds, Arc::clone(app))))
    }

    async fn load_dataset_connector(&self, ds: Arc<Dataset>) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let source = ds.source();
        let params = ConnectorParamsBuilder::new(source.into(), (&ds).into())
            .build(self.secrets())
            .await
            .context(UnableToInitializeDataConnectorSnafu)?;

        let data_connector: Arc<dyn DataConnector> = match self
            .get_dataconnector_from_source(source, params)
            .await
        {
            Ok(data_connector) => data_connector,
            Err(err) => {
                let ds_name = &ds.name;
                self.status
                    .update_dataset(ds_name, status::ComponentStatus::Error);
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                warn_spaced!(
                    spaced_tracer,
                    "Error initializing dataset {}. {err}",
                    ds_name.table()
                );
                return Err(crate::Error::UnableToInitializeDataConnector { source: err.into() });
            }
        };

        Ok(data_connector)
    }

    /// Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.
    async fn load_dataset(&self, ds: Arc<Dataset>) {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let _ = retry(retry_strategy, || async {
            let connector = match self.load_dataset_connector(Arc::clone(&ds)).await {
                Ok(connector) => connector,
                Err(err) => {
                    let ds_name = &ds.name;
                    self.status
                        .update_dataset(ds_name, status::ComponentStatus::Error);
                    metrics::datasets::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                    return Err(RetryError::transient(err));
                }
            };

            if let Err(err) = self
                .register_loaded_dataset(Arc::clone(&ds), connector, None)
                .await
            {
                return Err(RetryError::transient(err));
            };

            Ok(())
        })
        .await;
    }

    #[allow(clippy::too_many_lines)]
    async fn register_loaded_dataset(
        &self,
        ds: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<AcceleratedTable>,
    ) -> Result<()> {
        let source = ds.source();
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        if let Some(acceleration) = &ds.acceleration {
            if data_connector.resolve_refresh_mode(acceleration.refresh_mode)
                == RefreshMode::Changes
                && !data_connector.supports_changes_stream()
            {
                let err = AcceleratedTableInvalidChangesSnafu {
                    dataset_name: ds.name.to_string(),
                }
                .build();
                warn_spaced!(spaced_tracer, "{}{err}", "");
                return Err(err);
            }
        }

        // Only wrap data connector when necessary.
        let connector = if ds.has_embeddings() {
            let connector = EmbeddingConnector::new(data_connector, Arc::clone(&self.embeds));
            Arc::new(connector) as Arc<dyn DataConnector>
        } else {
            data_connector
        };

        // Test dataset connectivity by attempting to get a read provider.
        let federated_table = match connector.read_provider(&ds).await {
            Ok(provider) => FederatedTable::new(provider),
            Err(err) => {
                // We couldn't connect to the federated table. If the dataset has an existing
                // accelerated table, we can defer the federated table creation.
                if let Some(federated_table) =
                    FederatedTable::new_deferred(Arc::clone(&ds), Arc::clone(&connector)).await
                {
                    tracing::warn!(
                        "Unable to connect to the remote source for {}. Data will be served from the pre-existing accelerated table for {} while attempting to establish the connection.\n\n{err}",
                        ds.name,
                        ds.name
                    );
                    federated_table
                } else {
                    self.status
                        .update_dataset(&ds.name, status::ComponentStatus::Error);
                    metrics::datasets::LOAD_ERROR.add(1, &[]);
                    if let DataConnectorError::UnsupportedDataType { .. } = err {
                        error_spaced!(spaced_tracer, "{}{err}", "");
                    } else {
                        warn_spaced!(spaced_tracer, "{}{err}", "");
                    }
                    return UnableToLoadDatasetConnectorSnafu {
                        dataset: ds.name.clone(),
                    }
                    .fail();
                }
            }
        };

        match self
            .register_dataset(
                Arc::clone(&ds),
                RegisterDatasetContext {
                    data_connector: Arc::clone(&connector),
                    federated_read_table: federated_table,
                    source: source.to_string(),
                    accelerated_table,
                },
            )
            .await
        {
            Ok(()) => {
                tracing::info!(
                    "{}",
                    dataset_registered_trace(&connector, &ds, self.df.cache_provider().is_some())
                );
                if let Some(datasets_health_monitor) = &self.datasets_health_monitor {
                    if let Err(err) = datasets_health_monitor.register_dataset(&ds).await {
                        tracing::warn!(
                            "Unable to add dataset {} for availability monitoring: {err}",
                            &ds.name
                        );
                    };
                }
                let engine = ds.acceleration.as_ref().map_or_else(
                    || "None".to_string(),
                    |acc| {
                        if acc.enabled {
                            acc.engine.to_string()
                        } else {
                            "None".to_string()
                        }
                    },
                );
                metrics::datasets::COUNT.add(1, &[KeyValue::new("engine", engine)]);

                Ok(())
            }
            Err(err) => {
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                if let Error::UnableToAttachDataConnector {
                    source: crate::datafusion::Error::RefreshSql { .. },
                    connector_component: _,
                    data_connector: _,
                } = &err
                {
                    error_spaced!(spaced_tracer, "{}{err}", "");
                } else {
                    warn_spaced!(spaced_tracer, "{}{err}", "");
                }

                Err(err)
            }
        }
    }

    async fn remove_dataset(&self, ds: &Dataset) {
        if self.df.table_exists(ds.name.clone()) {
            if let Some(datasets_health_monitor) = &self.datasets_health_monitor {
                datasets_health_monitor
                    .deregister_dataset(&ds.name.to_string())
                    .await;
            }

            if let Err(e) = self.df.remove_table(&ds.name).await {
                tracing::warn!("Unable to unload dataset {}: {}", &ds.name, e);
                return;
            }
        }

        tracing::info!("Unloaded dataset {}", &ds.name);
        let engine = ds.acceleration.as_ref().map_or_else(
            || "None".to_string(),
            |acc| {
                if acc.enabled {
                    acc.engine.to_string()
                } else {
                    "None".to_string()
                }
            },
        );
        metrics::datasets::COUNT.add(-1, &[KeyValue::new("engine", engine)]);
    }

    async fn update_dataset(&self, ds: Arc<Dataset>) {
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        match self.load_dataset_connector(Arc::clone(&ds)).await {
            Ok(connector) => {
                // File accelerated datasets don't support hot reload.
                if Self::accelerated_dataset_supports_hot_reload(&ds, &*connector) {
                    tracing::info!("Updating accelerated dataset {}...", &ds.name);
                    if let Ok(()) = &self
                        .reload_accelerated_dataset(Arc::clone(&ds), Arc::clone(&connector))
                        .await
                    {
                        self.status
                            .update_dataset(&ds.name, status::ComponentStatus::Ready);
                        return;
                    }
                    tracing::debug!("Failed to create accelerated table for dataset {}, falling back to full dataset reload", ds.name);
                }

                self.remove_dataset(&ds).await;

                if self
                    .register_loaded_dataset(Arc::clone(&ds), Arc::clone(&connector), None)
                    .await
                    .is_err()
                {
                    self.status
                        .update_dataset(&ds.name, status::ComponentStatus::Error);
                }
            }
            Err(e) => {
                tracing::error!("Unable to update dataset {}: {e}", ds.name);
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Error);
            }
        }
    }

    fn accelerated_dataset_supports_hot_reload(
        ds: &Dataset,
        connector: &dyn DataConnector,
    ) -> bool {
        let Some(acceleration) = &ds.acceleration else {
            return false;
        };

        if !acceleration.enabled {
            return false;
        }

        // Datasets that configure changes and are file-accelerated automatically keep track of changes that survive restarts.
        // Thus we don't need to "hot reload" them to try to keep their data intact.
        if connector.supports_changes_stream()
            && ds.is_file_accelerated()
            && connector.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes
        {
            return false;
        }

        // File accelerated datasets don't support hot reload.
        if ds.is_file_accelerated() {
            return false;
        }

        true
    }

    async fn reload_accelerated_dataset(
        &self,
        ds: Arc<Dataset>,
        connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        let read_table = connector.read_provider(&ds).await.map_err(|_| {
            UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .build()
        })?;
        let federated_table = FederatedTable::new(read_table);

        // create new accelerated table for updated data connector
        let (accelerated_table, is_ready) = self
            .df
            .create_accelerated_table(&ds, Arc::clone(&connector), federated_table, self.secrets())
            .await
            .context(UnableToCreateAcceleratedTableSnafu {
                dataset: ds.name.clone(),
            })?;

        // wait for accelerated table to be ready
        is_ready
            .await
            .context(UnableToReceiveAcceleratedTableStatusSnafu)?;

        tracing::debug!("Accelerated table for dataset {} is ready", ds.name);

        self.register_loaded_dataset(ds, Arc::clone(&connector), Some(accelerated_table))
            .await?;

        Ok(())
    }

    pub(crate) async fn get_dataconnector_from_source(
        &self,
        source: &str,
        params: ConnectorParams,
    ) -> Result<Arc<dyn DataConnector>> {
        // Unlike most other data connectors, the localpod connector needs a reference to the current DataFusion instance.
        if source == LOCALPOD_DATACONNECTOR {
            return Ok(Arc::new(LocalPodConnector::new(Arc::clone(&self.df))));
        }

        match dataconnector::create_new_connector(source, params).await {
            Some(dc) => dc.context(UnableToInitializeDataConnectorSnafu {}),
            None => {
                if source == ODBC_DATACONNECTOR {
                    OdbcNotInstalledSnafu.fail()
                } else {
                    UnknownDataConnectorSnafu {
                        data_connector: source,
                    }
                    .fail()
                }
            }
        }
    }

    async fn register_dataset(
        &self,
        ds: Arc<Dataset>,
        register_dataset_ctx: RegisterDatasetContext,
    ) -> Result<()> {
        let RegisterDatasetContext {
            data_connector,
            federated_read_table,
            source,
            accelerated_table,
        } = register_dataset_ctx;

        let replicate = ds.replication.as_ref().is_some_and(|r| r.enabled);

        // FEDERATED TABLE
        if !ds.is_accelerated() {
            let ds_name: TableReference = ds.name.clone();
            self.df
                .register_table(
                    Arc::clone(&ds),
                    crate::datafusion::Table::Federated {
                        data_connector,
                        federated_read_table,
                    },
                )
                .await
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source,
                    connector_component: ConnectorComponent::from(&ds),
                })?;

            self.status
                .update_dataset(&ds_name, status::ComponentStatus::Ready);
            return Ok(());
        }

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;
        let accelerator_engine = acceleration_settings.engine;

        if ds.mode() == dataset::Mode::ReadWrite && !replicate {
            AcceleratedReadWriteTableWithoutReplicationSnafu.fail()?;
        }

        dataaccelerator::get_accelerator_engine(accelerator_engine)
            .await
            .context(AcceleratorEngineNotAvailableSnafu {
                name: accelerator_engine.to_string(),
            })?;

        // The accelerated refresh task will set the dataset status to `Ready` once it finishes loading.
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        self.df
            .register_table(
                Arc::clone(&ds),
                crate::datafusion::Table::Accelerated {
                    source: data_connector,
                    federated_read_table,
                    accelerated_table,
                    secrets: self.secrets(),
                },
            )
            .await
            .context(UnableToAttachDataConnectorSnafu {
                data_connector: source,
                connector_component: ConnectorComponent::from(&ds),
            })
    }

    pub(crate) async fn apply_dataset_diff(&self, current_app: &Arc<App>, new_app: &Arc<App>) {
        let valid_datasets = Self::get_valid_datasets(new_app, LogErrors(true));
        let initialized_datasets = self.initialize_accelerators(&valid_datasets).await;
        let existing_datasets = Self::get_valid_datasets(current_app, LogErrors(false));

        for ds in initialized_datasets {
            if let Some(current_ds) = existing_datasets.iter().find(|d| d.name == ds.name) {
                if ds != *current_ds {
                    self.update_dataset(ds).await;
                }
            } else {
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Initializing);
                self.load_dataset(ds).await;
            }
        }

        // Remove datasets that are no longer in the app
        for ds in &current_app.datasets {
            if !new_app.datasets.iter().any(|d| d.name == ds.name) {
                let ds = match Dataset::try_from(ds.clone()) {
                    Ok(ds) => ds,
                    Err(e) => {
                        tracing::error!("Could not remove dataset {}: {e}", ds.name);
                        continue;
                    }
                };
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Disabled);
                self.remove_dataset(&ds).await;
            }
        }
    }

    /// Initialize datasets configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `SQLite`).
    async fn initialize_accelerators(&self, datasets: &[Arc<Dataset>]) -> Vec<Arc<Dataset>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let mut initialized_datasets = vec![];
        for ds in datasets {
            if let Some(acceleration) = &ds.acceleration {
                let accelerator = match dataaccelerator::get_accelerator_engine(acceleration.engine)
                    .await
                    .context(AcceleratorEngineNotAvailableSnafu {
                        name: acceleration.engine.to_string(),
                    }) {
                    Ok(accelerator) => accelerator,
                    Err(err) => {
                        let ds_name = &ds.name;
                        self.status
                            .update_dataset(ds_name, status::ComponentStatus::Error);
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                        continue;
                    }
                };

                match accelerator
                    .init(ds)
                    .await
                    .context(AcceleratorInitializationFailedSnafu {
                        name: acceleration.engine.to_string(),
                    }) {
                    Ok(()) => {
                        initialized_datasets.push(Arc::clone(ds));
                    }
                    Err(err) => {
                        let ds_name = &ds.name;
                        self.status
                            .update_dataset(ds_name, status::ComponentStatus::Error);
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                    }
                }
            } else {
                initialized_datasets.push(Arc::clone(ds)); // non-accelerated datasets are always successfully initialized
            }
        }

        initialized_datasets
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) async fn get_initialized_datasets(
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<Dataset>> {
        let valid_datasets = Self::get_valid_datasets(app, log_errors);
        futures::stream::iter(valid_datasets)
            .filter_map(|ds| async move {
                match (ds.is_accelerated(), ds.is_accelerator_initialized().await) {
                    (true, true) | (false, _) => Some(Arc::clone(&ds)),
                    (true, false) => {
                        if log_errors.0 {
                            metrics::datasets::LOAD_ERROR.add(1, &[]);
                            tracing::error!(
                                dataset = &ds.name.to_string(),
                                "Dataset is accelerated but the accelerator failed to initialize."
                            );
                        }
                        None
                    }
                }
            })
            .collect()
            .await
    }
}

pub struct RegisterDatasetContext {
    data_connector: Arc<dyn DataConnector>,
    federated_read_table: FederatedTable,
    source: String,
    accelerated_table: Option<AcceleratedTable>,
}
