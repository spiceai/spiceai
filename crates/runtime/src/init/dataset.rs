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

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Instant};

use crate::cluster::partition::get_partition_filter_exprs;
use crate::component::dataset::acceleration::Mode;
use crate::dataaccelerator::BootstrapStatus;
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::caching_engine::CachingEngineSys;
use crate::init::dataset_initialization::DatasetInitialization;
use crate::{
    AcceleratedTableInvalidChangesSnafu, AcceleratorEngineNotAvailableSnafu,
    AcceleratorInitializationFailedSnafu, Error, FullTextSearchRequiresAccelerationSnafu,
    LogErrors, OdbcNotInstalledSnafu, PermanentDatasetFailureSnafu, Result, Runtime,
    UnableToAttachDataConnectorSnafu, UnableToBuildDatasetSnafu,
    UnableToCreateAcceleratedTableSnafu, UnableToInitializeDataConnectorSnafu,
    UnableToLoadDatasetConnectorSnafu, UnknownDataConnectorSnafu,
    accelerated_table::AcceleratedTable,
    component::dataset::{
        Dataset,
        acceleration::{Acceleration, RefreshMode},
        builder::DatasetBuilder,
    },
    dataaccelerator::{
        AccelerationSource, validate_cayenne_snapshot_consistency, validate_snapshot_paths,
    },
    dataconnector::{
        self, ConnectorComponent, DataConnector, ODBC_DATACONNECTOR,
        deferred::DeferredConnector,
        localpod::{LOCALPOD_DATACONNECTOR, LocalPodConnector},
        parameters::ConnectorParamsBuilder,
    },
    embeddings::connector::EmbeddingConnector,
    error_spaced,
    federated_table::FederatedTable,
    metrics::{self, components::register_component_metric},
    search::full_text::connector::FullTextConnector,
    status,
    tracing_util::dataset_registered_trace,
    warn_spaced,
};
use app::App;
use datafusion::sql::TableReference;
#[cfg(any(feature = "duckdb", feature = "sqlite"))]
use futures::StreamExt;
use futures::future::join_all;
use opentelemetry::KeyValue;
use snafu::prelude::*;
use tokio::sync::Semaphore;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

impl Runtime {
    pub(crate) async fn load_datasets(self: Arc<Self>) {
        let Some(app) = self.read_app().await else {
            return;
        };

        // Use the shared semaphore so that startup and on-demand loads share
        // the same `runtime.dataset_load_parallelism` budget.
        let semaphore = Arc::clone(&self.dataset_load_semaphore);

        // Before loading datasets, we must initialize views accelerators (if any).
        // This is required for acceleration federation for some engines (e.g. `DuckDB`).
        let valid_views = Arc::clone(&self).get_valid_views(&app, LogErrors(true));
        self.initialize_views_accelerators(&valid_views).await;

        let valid_datasets = Arc::clone(&self).get_valid_datasets(&app, LogErrors(true));
        let startup_datasets = valid_datasets;

        // Validate Cayenne snapshot consistency before initializing accelerators.
        // All Cayenne datasets sharing the same metadata directory must have the same
        // snapshot configuration (either all enabled or all disabled).
        let acceleration_sources: Vec<Arc<dyn AccelerationSource>> =
            startup_datasets.iter().map(|ds| ds.clone_arc()).collect();
        if let Err(err) = validate_cayenne_snapshot_consistency(&acceleration_sources) {
            tracing::error!("{err}");
            return;
        }

        let init_results = self
            .initialize_datasets_accelerators(&startup_datasets)
            .await;

        // Validate that no datasets with snapshots share acceleration files
        let initialized_sources: Vec<Arc<dyn AccelerationSource>> = startup_datasets
            .iter()
            .filter(|ds| init_results.get(&ds.name).is_some_and(Result::is_ok))
            .map(|ds| ds.clone_arc())
            .collect();
        if let Err(err) = validate_snapshot_paths(initialized_sources).await {
            tracing::error!("{err}");
            return;
        }

        // Create a map of dataset names to their futures
        let mut dataset_futures = HashMap::new();
        let mut localpod_datasets = Vec::new();

        // First create futures for non-localpod datasets
        for ds in &startup_datasets {
            let bootstrap_status = match init_results.get(&ds.name) {
                Some(Ok(status)) => status.clone(),
                Some(Err(_)) => {
                    // Error already logged in initialize_datasets_accelerators
                    continue;
                }
                None => {
                    tracing::error!("Dataset {} missing from initialization results", ds.name);
                    continue;
                }
            };

            if ds.source() == LOCALPOD_DATACONNECTOR {
                localpod_datasets.push((Arc::clone(ds), bootstrap_status));
                continue;
            }

            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);
            let ds_clone = Arc::clone(ds);
            let cloned_self = Arc::clone(&self);
            let load_semaphore = Arc::clone(&semaphore);
            let future: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
                cloned_self
                    .load_dataset(ds_clone, bootstrap_status, load_semaphore)
                    .await;
            })
                as Pin<Box<dyn Future<Output = ()> + Send>>;
            dataset_futures.insert(ds.name.clone(), future);
        }

        // For each localpod dataset, chain it after its parent's future
        for (ds, bootstrap_status) in localpod_datasets {
            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);

            // Get the parent dataset path from the localpod dataset
            let path = ds.path();
            let path_table_ref = TableReference::parse_str(path);

            // Find and remove the parent dataset's future
            if let Some(parent_future) = dataset_futures.remove(&path_table_ref) {
                let ds_clone = Arc::clone(&ds);
                let cloned_self = Arc::clone(&self);
                let load_semaphore = Arc::clone(&semaphore);
                // Chain the localpod dataset load after its parent
                let chained_future = Box::pin(async move {
                    parent_future.await;
                    cloned_self
                        .load_dataset(ds_clone, bootstrap_status, load_semaphore)
                        .await;
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
                self.status.update_dataset(
                    &ds.name,
                    status::ComponentStatus::error_with_message(format!(
                        "Parent dataset '{path_table_ref}' doesn't exist"
                    )),
                );
            }
        }

        let mut spawned_tasks = vec![];

        for (ds, dataset_load_future) in dataset_futures {
            let handle = tokio::spawn(async move {
                tracing::info!("Dataset {ds} initializing...");
                dataset_load_future.await;
            });
            spawned_tasks.push(handle);
        }

        // Aggregate startup summary so users see "3/5 queued, 2 skipped at init" at a glance
        // instead of having to piece that together from per-dataset warnings. `dispatched`
        // is the number of spawned load tasks, which can be less than the number of
        // datasets that will load because localpod datasets are chained behind their
        // parent dataset's task. Wording avoids the words "failed" / "error" so it
        // doesn't trip quickstart CI checks that grep spice.log for those tokens as a
        // sentinel for real failures.
        let dispatched = spawned_tasks.len();
        let init_skipped = init_results.values().filter(|r| r.is_err()).count();
        let total = startup_datasets.len();
        if total > 0 {
            tracing::info!(
                "Loading datasets: {dispatched} tasks dispatched, {init_skipped} skipped at accelerator init (of {total} total; localpod datasets may be chained)."
            );
        }

        // Spawn a best-effort follow-up summary that samples the status registry every
        // 30s until all datasets have settled (reached Ready/Refreshing or Error), so
        // users see periodic progress on slow-loading pods without having to query
        // /v1/datasets. Uses the runtime's shutdown token so a ctrl-c stops the sampler
        // cleanly. Skipped when there are no datasets at all so we don't spawn a timer
        // that would just no-op.
        if total > 0 {
            let status_handle = Arc::clone(&self.status);
            let shutdown_token = self.status.shutdown_token();
            tokio::spawn(async move {
                let mut elapsed_secs = 0u64;
                loop {
                    tokio::select! {
                        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                        () = shutdown_token.cancelled() => return,
                    }
                    elapsed_secs += 30;
                    let statuses = status_handle.get_dataset_statuses();
                    let mut ready = 0usize;
                    let mut unhealthy = 0usize;
                    let mut initializing = 0usize;
                    for s in statuses.values() {
                        match s {
                            status::ComponentStatus::Ready
                            | status::ComponentStatus::Refreshing => {
                                ready += 1;
                            }
                            status::ComponentStatus::Error(_) => unhealthy += 1,
                            status::ComponentStatus::Initializing => initializing += 1,
                            _ => {}
                        }
                    }
                    let total = statuses.len();
                    if total == 0 {
                        return;
                    }
                    // Phrasing deliberately avoids "error"/"failed" so quickstart smoke
                    // tests that grep spice.log for those tokens don't get false positives
                    // on a healthy startup. Real per-dataset failure is already logged at
                    // WARN level inside `load_dataset`.
                    tracing::info!(
                        "Dataset load summary (after {elapsed_secs}s): {ready}/{total} ready, {unhealthy} unhealthy, {initializing} still initializing."
                    );
                    // Stop once every dataset has settled (Ready/Refreshing or Error).
                    // `initializing` only counts Initializing; other transient states
                    // (e.g. Disabled) are treated as settled for this summary.
                    if initializing == 0 {
                        return;
                    }
                }
            });
        }

        let _ = join_all(spawned_tasks).await;

        // After all datasets have loaded, load the views.
        Arc::clone(&self).load_views(&app);
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    pub(crate) fn get_valid_datasets(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<Dataset>> {
        self.datasets_iter(app)
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

    #[expect(clippy::result_large_err)]
    fn datasets_iter(self: Arc<Self>, app: &Arc<App>) -> impl Iterator<Item = Result<Dataset>> {
        app.datasets
            .clone()
            .into_iter()
            .map(DatasetBuilder::try_from)
            .map(move |ds_builder_result| {
                ds_builder_result.and_then(|ds_builder| {
                    let dataset_name = ds_builder.name.to_string();
                    ds_builder
                        .with_app(Arc::clone(app))
                        .with_runtime(Arc::clone(&self))
                        .build()
                        .context(UnableToBuildDatasetSnafu {
                            dataset: dataset_name,
                        })
                })
            })
    }

    async fn load_dataset_connector(&self, ds: Arc<Dataset>) -> Result<Arc<dyn DataConnector>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let source = ds.source();

        let data_connector: Arc<dyn DataConnector> = match self
            .get_dataconnector_from_dataset(Arc::clone(&ds))
            .await
        {
            Ok(data_connector) => data_connector,
            Err(err) => {
                let ds_name = &ds.name;
                self.status.update_dataset(
                    ds_name,
                    status::ComponentStatus::error_with_message(err.to_string()),
                );
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                warn_spaced!(
                    spaced_tracer,
                    "Error initializing dataset {}. {err}",
                    ds_name.table()
                );
                return Err(crate::Error::UnableToInitializeDataConnector { source: err.into() });
            }
        };

        // Register component metrics for this dataset.
        if let Some(metrics_provider) = data_connector.metrics_provider() {
            let enabled_metrics = ds.metrics.enabled_metrics();
            let instance_name = ds.name.to_string();

            for metric in metrics_provider.available_metrics() {
                let explicitly_disabled = ds.metrics.metrics.iter().any(|configured_metric| {
                    configured_metric.name == metric.name && !configured_metric.enabled
                });
                let user_enabled = enabled_metrics.iter().any(|m| m == metric.name);
                if explicitly_disabled || (!metric.auto_register && !user_enabled) {
                    continue;
                }
                if let Err(e) =
                    register_component_metric(&metrics_provider, *metric, &instance_name)
                {
                    tracing::error!("Unable to register component metric {}: {}", metric.name, e);
                }
            }

            // Warn about user-enabled metrics that don't exist on this connector.
            for name in &enabled_metrics {
                if metrics_provider.get_metric(name).is_none() {
                    tracing::warn!("Metric {name} not available in {source}");
                }
            }
        } else if ds.metrics.has_enabled_metrics() {
            let enabled_metrics = ds.metrics.enabled_metrics();
            tracing::warn!(
                "Dataset {} does not support metrics. Skipping metric registration for {}.",
                ds.name,
                enabled_metrics.join(", ")
            );
        }

        Ok(data_connector)
    }

    async fn try_load_dataset_once(
        &self,
        ds: Arc<Dataset>,
        bootstrap_status: BootstrapStatus,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<()> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        if let Err(err) = validate_dataset(&ds) {
            let ds_name = &ds.name;
            metrics::datasets::LOAD_ERROR.add(1, &[]);
            error_spaced!(spaced_tracer, "{}{err}", "");
            self.status.update_dataset(
                ds_name,
                status::ComponentStatus::error_with_message(err.to_string()),
            );
            return Err(err);
        }

        // Deferred path. Each connector factory decides via
        // `static_schema()` whether the dataset can be registered
        // without contacting the source. If the factory returns a
        // schema AND the runtime-side gate (read-only,
        // on_registration, no embeddings/FTS) passes, register a
        // placeholder and skip eager connector construction. The
        // resolver hook in `datafusion::create_logical_plan` will
        // trigger `ensure_ready` on first reference.
        if self.is_deferral_eligible(&ds)
            && let Some(deferred_schema) = self.try_static_schema_for_dataset(&ds).await
        {
            let runtime = ds.runtime();
            let runtime_for_lazy = Arc::clone(&runtime);
            let ds_for_lazy = Arc::clone(&ds);
            let connector_builder: crate::init::dataset_initialization::LazyConnectorBuilder =
                Box::new(move || {
                    let runtime = runtime_for_lazy;
                    let ds = ds_for_lazy;
                    Box::pin(async move {
                        runtime
                            .get_dataconnector_from_dataset(ds)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    })
                });

            let init = crate::init::dataset_initialization::DatasetInitialization::plan_deferred(
                Arc::clone(&ds),
                Arc::clone(&runtime),
                connector_builder,
                Arc::clone(&deferred_schema),
                bootstrap_status,
                load_semaphore,
            );

            tracing::info!(
                dataset = %ds.name,
                "Registering dataset as deferred (placeholder); source will be contacted on first query."
            );
            return runtime
                .df
                .register_deferred_dataset(Arc::clone(&ds), init, deferred_schema)
                .await
                .map_err(|source| crate::Error::UnableToAttachDataConnector {
                    source,
                    data_connector: ds.source().to_string(),
                    connector_component: crate::dataconnector::ConnectorComponent::from(
                        ds.as_ref(),
                    ),
                });
        }

        let connector_start = Instant::now();
        let connector = match self.load_dataset_connector(Arc::clone(&ds)).await {
            Ok(connector) => {
                tracing::debug!(dataset = %ds.name, duration_ms = connector_start.elapsed().as_millis(), "Dataset connector created");
                connector
            }
            Err(err) => {
                if !self.status.is_shutdown() {
                    let ds_name = &ds.name;
                    self.status.update_dataset(
                        ds_name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::datasets::LOAD_ERROR.add(1, &[]);
                    warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                }
                return Err(err);
            }
        };

        // Check shutdown between connector load and registration.
        if self.status.is_shutdown() {
            return Err(crate::Error::UnableToInitializeDataConnector {
                source: "Runtime is shutting down".into(),
            });
        }

        let runtime = ds.runtime();
        DatasetInitialization::plan_eager(
            ds,
            runtime,
            connector,
            bootstrap_status,
            load_semaphore,
            None,
        )
        .initialize()
        .await
        .map(|_ready| ())
    }

    /// Deferral eligibility check: the runtime-side gate that
    /// complements per-factory `static_schema()`. Centralized here so
    /// future extensions (write-back, replication) can extend the gate
    /// without touching the planning value type.
    #[expect(clippy::unused_self)]
    fn is_deferral_eligible(&self, ds: &Dataset) -> bool {
        use crate::component::dataset::ReadyState;
        if ds.access().allows_write() {
            return false;
        }
        if ds.ready_state != ReadyState::OnRegistration {
            return false;
        }
        if ds.has_embeddings() || ds.has_full_text_column() {
            // EmbeddingConnector and FullTextConnector wrap the
            // connector with extra state that the deferred path does
            // not yet support — keep these on the eager path even
            // when acceleration is enabled.
            return false;
        }
        if ds.acceleration.as_ref().is_some_and(|a| a.enabled) {
            // Accelerated deferred datasets are eligible. The
            // Lazy+Known initialize branch builds the connector
            // lazily and then hands off to the eager bring-up path
            // (register_loaded_dataset) which constructs the
            // AcceleratedTable, kicks off refresh, registers with the
            // health monitor, and so on. The placeholder is
            // overwritten by `register_loaded_dataset` and the pending
            // bookkeeping is cleared after the swap completes.
            return true;
        }
        true
    }

    /// Caller must set `status::update_dataset(...` before calling `load_dataset`. This function will set error/ready statuses appropriately.
    ///
    /// The `load_semaphore` limits concurrent schema inference via
    /// `read_provider` so that `dataset_load_parallelism` controls how many
    /// datasets query the source for schema at the same time. Connector
    /// creation and `DataFusion` registration run outside the permit.
    async fn load_dataset(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        bootstrap_status: BootstrapStatus,
        load_semaphore: Arc<Semaphore>,
    ) {
        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

        let runtime = Arc::clone(&self);
        let shutdown_token = runtime.status.shutdown_token();
        let retry_fut = retry(retry_strategy, || async {
            // Exit immediately if the runtime is shutting down (e.g. after a backoff sleep completes).
            if runtime.status.is_shutdown() {
                return Err(RetryError::permanent(
                    crate::Error::UnableToInitializeDataConnector {
                        source: "Runtime is shutting down".into(),
                    },
                ));
            }

            match runtime
                .try_load_dataset_once(
                    Arc::clone(&ds),
                    bootstrap_status.clone(),
                    Some(Arc::clone(&load_semaphore)),
                )
                .await
            {
                Ok(()) => Ok(()),
                Err(err) if runtime.status.is_shutdown() => Err(RetryError::permanent(err)),
                Err(err) if matches!(err, Error::PermanentDatasetFailure { .. }) => {
                    Err(RetryError::permanent(err))
                }
                Err(err) => Err(RetryError::transient(err)),
            }
        });

        // Use tokio::select! so that backoff sleeps inside `retry` are immediately
        // interrupted when the runtime begins shutting down (e.g. on ctrl-c).
        tokio::select! {
            _ = retry_fut => {},
            () = shutdown_token.cancelled() => {},
        }
    }

    /// Apply extended schema inference to a freshly-resolved dataset.
    ///
    /// When the dataset opts into `schema_inference: extended` and the source
    /// connector emitted inferred-schema metadata, this fills any acceleration
    /// settings the user left unset (primary key, indexes, sort columns) and
    /// returns a rebuilt `Dataset`. Applying it here — before the `FederatedTable`
    /// and registration are created — ensures every refresh mode, including CDC
    /// (`refresh_mode: changes`), observes the inferred values. Returns `ds`
    /// unchanged when inference is disabled, the dataset is not accelerated, or no
    /// usable metadata was emitted.
    fn apply_inferred_acceleration(
        ds: Arc<Dataset>,
        provider: &Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Arc<Dataset> {
        use crate::component::dataset::schema_inference::apply_inferred_schema;
        use data_components::inferred_schema::InferredSchema;

        if !ds.schema_inference.is_extended() || ds.acceleration.is_none() {
            return ds;
        }

        let source_schema = provider.schema();
        let inferred = InferredSchema::from_metadata(source_schema.metadata());
        if inferred.is_empty() {
            return ds;
        }

        // Resolve the schema the accelerator will actually store. When a refresh_sql
        // reshapes the schema, validate inferred columns against the projected
        // schema; if it can't be parsed, skip inference rather than risk injecting a
        // column the accelerator would later reject.
        let effective_schema = match ds.acceleration.as_ref().and_then(|a| a.refresh_sql.as_ref())
        {
            Some(sql) => match crate::datafusion::refresh_sql::parse_refresh_sql(
                ds.name.clone(),
                sql.as_str(),
                Arc::clone(&source_schema),
            ) {
                Ok((_, projected)) => projected,
                Err(error) => {
                    tracing::debug!(
                        dataset = %ds.name,
                        %error,
                        "Skipping extended schema inference; could not parse refresh_sql to validate inferred columns"
                    );
                    return ds;
                }
            },
            None => source_schema,
        };

        let mut new_ds = (*ds).clone();
        if let Some(acceleration) = new_ds.acceleration.as_mut() {
            apply_inferred_schema(
                acceleration,
                &inferred,
                &effective_schema,
                &ds.name.to_string(),
            );
        }
        Arc::new(new_ds)
    }

    pub(crate) async fn register_loaded_dataset(
        self: Arc<Self>,
        mut ds: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<Arc<AcceleratedTable>>,
        bootstrap_status: BootstrapStatus,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<()> {
        // Owned (not borrowed from `ds`) so the dataset can be rebuilt below by
        // extended schema inference without holding a borrow across the reassignment.
        let source = ds.source().to_string();
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        if let Some(acceleration) = &ds.acceleration
            && data_connector.resolve_refresh_mode(acceleration.refresh_mode)
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

        // In file_update mode, schema mismatches are handled by create_accelerated_table
        // which detects changes and recreates the acceleration with the new schema.
        let allow_schema_mismatch = ds
            .acceleration
            .as_ref()
            .is_some_and(|a| a.mode == Mode::FileUpdate);

        // Test dataset connectivity by attempting to get a read provider.
        // Acquire the load semaphore (if provided) to limit concurrent source queries.
        let load_guard = if let Some(sem) = &load_semaphore {
            let Ok(guard) = sem.acquire().await else {
                unreachable!("Semaphore is never closed.");
            };
            Some(guard)
        } else {
            None
        };
        let schema_start = Instant::now();
        let federated_table = match data_connector.read_provider(&ds).await {
            Ok(provider) => {
                // Gap-fill acceleration settings from extended schema inference (no-op
                // unless `schema_inference: extended` and the connector emitted metadata)
                // before the dataset flows into registration and any changes stream.
                ds = Self::apply_inferred_acceleration(ds, &provider);
                FederatedTable::new(
                    Arc::clone(&ds),
                    provider,
                    Arc::clone(&data_connector),
                    self.status.shutdown_token(),
                    allow_schema_mismatch,
                )
                .await
            }
            Err(err) => {
                // We couldn't connect to the federated table. If the dataset has an existing
                // accelerated table, we can defer the federated table creation.
                if let Some(federated_table) = FederatedTable::new_deferred(
                    Arc::clone(&ds),
                    Arc::clone(&data_connector),
                    self.status.shutdown_token(),
                )
                .await
                {
                    tracing::warn!(
                        "Failed to connect to the source for dataset {}. Serving data from the existing acceleration for {} while retrying the connection. {err}",
                        ds.name,
                        ds.name
                    );
                    federated_table
                } else {
                    self.status.update_dataset(
                        &ds.name,
                        status::ComponentStatus::error_with_message(err.to_string()),
                    );
                    metrics::datasets::LOAD_ERROR.add(1, &[]);
                    if !err.is_retriable() {
                        error_spaced!(spaced_tracer, "{}{err}", "");
                        return PermanentDatasetFailureSnafu {
                            dataset: ds.name.clone(),
                            reason: err.to_string(),
                        }
                        .fail();
                    }
                    warn_spaced!(spaced_tracer, "{}{err}", "");
                    return UnableToLoadDatasetConnectorSnafu {
                        dataset: ds.name.clone(),
                    }
                    .fail();
                }
            }
        };

        tracing::debug!(dataset = %ds.name, duration_ms = schema_start.elapsed().as_millis(), "Dataset schema inference complete");

        // Release the load permit before registration so other datasets can
        // begin their source-facing work while this one registers.
        drop(load_guard);

        let register_start = Instant::now();
        match Arc::clone(&self)
            .register_dataset(
                Arc::clone(&ds),
                RegisterDatasetContext {
                    data_connector: Arc::clone(&data_connector),
                    federated_read_table: federated_table,
                    source,
                    accelerated_table,
                    bootstrap_status,
                },
            )
            .await
        {
            Ok(()) => {
                // Log experimental hash_index warning once per dataset at registration
                if matches!(
                    ds.acceleration.as_ref(),
                    Some(acceleration) if acceleration.is_hash_index_enabled()
                ) {
                    tracing::warn!(
                        dataset = %ds.name,
                        "hash_index is automatically enabled for Arrow acceleration because primary_key or indexes are configured. Note: hash_index is experimental and may have breaking changes in future releases."
                    );
                }
                tracing::info!(
                    duration_ms = register_start.elapsed().as_millis(),
                    "{}",
                    dataset_registered_trace(
                        data_connector.as_ref(),
                        &ds,
                        self.df.results_cache_provider().is_some()
                    )
                );
                if data_connector
                    .initialization_for_dataset(&ds)
                    .is_dataset_health_monitor_enabled()
                    && let Some(datasets_health_monitor) = &self.datasets_health_monitor
                    && let Err(err) = datasets_health_monitor.register_dataset(&ds).await
                {
                    tracing::warn!(
                        "Unable to add dataset {} for availability monitoring: {err}",
                        &ds.name
                    );
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
                self.status.update_dataset(
                    &ds.name,
                    status::ComponentStatus::error_with_message(err.to_string()),
                );
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

    async fn remove_dataset(
        self: Arc<Self>,
        ds_name: TableReference,
        ds_acceleration: Option<&Acceleration>,
    ) {
        if self.df.table_exists(&ds_name) {
            if let Some(datasets_health_monitor) = &self.datasets_health_monitor {
                datasets_health_monitor
                    .deregister_dataset(&ds_name.to_string())
                    .await;
            }

            if let Err(e) = self.df.remove_table(&ds_name).await {
                tracing::warn!("Unable to unload dataset {}: {}", &ds_name, e);
                return;
            }
        }

        tracing::info!("Unloaded dataset {}", &ds_name);
        let engine = ds_acceleration.map_or_else(
            || "None".to_string(),
            |acc| {
                if acc.enabled {
                    acc.engine.to_string()
                } else {
                    "None".to_string()
                }
            },
        );

        if ds_acceleration.is_some()
            && let Err(e) = Arc::clone(&self)
                .remove_dataset_or_view_schedule(&ds_name)
                .await
        {
            tracing::warn!("Unable to remove dataset schedule for {}: {e}", &ds_name);
        }

        metrics::datasets::COUNT.add(-1, &[KeyValue::new("engine", engine)]);
    }

    async fn update_dataset(self: Arc<Self>, ds: Arc<Dataset>) {
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);

        // Updating a dataset may cause the cached LogicalPlans to be
        // obsolete, so we remove them
        self.df.clear_cached_plans().await;

        match Arc::clone(&self)
            .load_dataset_connector(Arc::clone(&ds))
            .await
        {
            Ok(connector) => {
                // File accelerated datasets don't support hot reload.
                if Self::accelerated_dataset_supports_hot_reload(&ds, &*connector) {
                    tracing::info!("Accelerated Dataset {} updating...", &ds.name);
                    if matches!(
                        Arc::clone(&self)
                            .reload_accelerated_dataset(Arc::clone(&ds), Arc::clone(&connector))
                            .await,
                        Ok(())
                    ) {
                        self.status
                            .update_dataset(&ds.name, status::ComponentStatus::Ready);
                        return;
                    }
                    tracing::debug!(
                        "Failed to create accelerated table for dataset {}, falling back to full dataset reload",
                        ds.name
                    );
                }

                Arc::clone(&self)
                    .remove_dataset(ds.name.clone(), ds.acceleration.as_ref())
                    .await;

                if let Err(e) = DatasetInitialization::plan_eager(
                    Arc::clone(&ds),
                    Arc::clone(&self),
                    Arc::clone(&connector),
                    BootstrapStatus::None,
                    None,
                    None,
                )
                .initialize()
                .await
                .map(|_ready| ())
                {
                    self.status.update_dataset(
                        &ds.name,
                        status::ComponentStatus::error_with_message(e.to_string()),
                    );
                }
            }
            Err(e) => {
                tracing::error!("Unable to update dataset {}: {e}", ds.name);
                self.status.update_dataset(
                    &ds.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
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
        self: Arc<Self>,
        ds: Arc<Dataset>,
        connector: Arc<dyn DataConnector>,
    ) -> Result<()> {
        let read_table = connector.read_provider(&ds).await.map_err(|_| {
            UnableToLoadDatasetConnectorSnafu {
                dataset: ds.name.clone(),
            }
            .build()
        })?;
        let allow_schema_mismatch = ds
            .acceleration
            .as_ref()
            .is_some_and(|a| a.mode == Mode::FileUpdate);
        let federated_table = FederatedTable::new(
            Arc::clone(&ds),
            read_table,
            Arc::clone(&connector),
            self.status.shutdown_token(),
            allow_schema_mismatch,
        )
        .await;

        // Remove the schedule if the dataset has one, to prevent scheduling while the dataset is being updated.
        Arc::clone(&self)
            .remove_dataset_or_view_schedule(&ds.name)
            .await?;

        // create new accelerated table for updated data connector
        let accelerated_table = Arc::new(
            self.df
                .create_accelerated_table(
                    &ds,
                    Arc::clone(&connector),
                    federated_table,
                    self.secrets(),
                    BootstrapStatus::None,
                    vec![],
                )
                .await
                .context(UnableToCreateAcceleratedTableSnafu {
                    dataset: ds.name.clone(),
                })?,
        );

        let notifier = accelerated_table.refresher().on_complete_notification();

        // wait for accelerated table to be ready
        if let Some(notifier) = notifier {
            notifier.notified().await;
        }

        // recreate the scheduler, which also recreates with any updated parameters
        Arc::clone(&self)
            .create_dataset_or_view_schedule(Arc::clone(&ds))
            .await?;

        tracing::debug!("Accelerated table for dataset {} is ready", ds.name);

        // Hot reload doesn't bootstrap from snapshot
        DatasetInitialization::plan_eager(
            ds,
            Arc::clone(&self),
            Arc::clone(&connector),
            BootstrapStatus::None,
            None,
            Some(accelerated_table),
        )
        .initialize()
        .await?;

        Ok(())
    }

    /// Resolve a deferral schema for `ds` without contacting the
    /// source. Priority:
    /// 1. Connector factory's `static_schema()` — for connectors that
    ///    intrinsically know their schema from configuration alone.
    /// 2. User-declared `columns:` in the spicepod, when the factory
    ///    does not provide a static schema.
    ///
    /// Returns `None` if neither source yields a schema, in which
    /// case the dataset must take the eager path.
    pub(crate) async fn try_static_schema_for_dataset(
        &self,
        ds: &Dataset,
    ) -> Option<arrow_schema::SchemaRef> {
        // We must NOT construct the connector here — deferred bring-up
        // exists precisely to skip that work at startup. We only
        // resolve `ConnectorParams` (no I/O) so the factory can decide
        // based on configuration.
        let source = ds.source();
        let factory = dataconnector::get_connector_factory(source).await?;

        let params = ConnectorParamsBuilder::new(source.into(), ds.into())
            .build(self.secrets(), self.tokio_io_runtime())
            .await
            .ok()?;

        if let Some(schema) = factory.static_schema(&params, ds) {
            return Some(schema);
        }

        // Fallback: honor the user-declared `columns:` schema. The
        // first-query swap validates it against the live source
        // schema and fails fast on mismatch.
        match crate::component::dataset::declared_schema::declared_schema_for(ds) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::warn!(
                    dataset = %ds.name,
                    error = %err,
                    "Declared `columns:` schema is invalid; falling back to eager registration."
                );
                None
            }
        }
    }

    pub(crate) async fn get_dataconnector_from_dataset(
        &self,
        ds: Arc<Dataset>,
    ) -> Result<Arc<dyn DataConnector>> {
        let source = ds.source();

        let params = ConnectorParamsBuilder::new(source.into(), (&ds).into())
            .build(self.secrets(), self.tokio_io_runtime())
            .await
            .context(UnableToInitializeDataConnectorSnafu)?;

        // Unlike most other data connectors, the localpod connector needs a reference to the current DataFusion instance.
        if source == LOCALPOD_DATACONNECTOR {
            return Ok(Arc::new(LocalPodConnector::new(Arc::clone(&self.df))));
        }

        let mut data_connector =
            if let Some(dc) = dataconnector::create_new_connector(source, params).await {
                dc.context(UnableToInitializeDataConnectorSnafu {})?
            } else {
                if source == ODBC_DATACONNECTOR {
                    return Err(OdbcNotInstalledSnafu.build());
                }

                let suggestion = dataconnector::suggest_connector(source).await;
                let available = dataconnector::registered_connector_names().await;
                return Err(UnknownDataConnectorSnafu {
                    data_connector: source,
                    suggestion,
                    available,
                }
                .build());
            };

        if ds.has_embeddings() {
            data_connector = Arc::new(EmbeddingConnector::new(
                data_connector,
                Arc::clone(&self.embeds),
                self.secrets(),
            ));
        }

        if ds.has_full_text_column() {
            #[cfg(feature = "elasticsearch")]
            if ds.fts_engine() == Some("elasticsearch") {
                use crate::search::full_text::elasticsearch::ElasticsearchFullTextConnector;
                data_connector = Arc::new(
                    ElasticsearchFullTextConnector::try_new(data_connector, &ds, self.secrets())
                        .await
                        .context(UnableToInitializeDataConnectorSnafu)?,
                );
            } else {
                data_connector = Arc::new(FullTextConnector::new(data_connector));
            }
            #[cfg(not(feature = "elasticsearch"))]
            {
                data_connector = Arc::new(FullTextConnector::new(data_connector));
            }
        }

        if data_connector.initialization().is_on_trigger() {
            data_connector = Arc::new(DeferredConnector::new(data_connector));
        }

        Ok(data_connector)
    }

    async fn register_dataset(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        register_dataset_ctx: RegisterDatasetContext,
    ) -> Result<()> {
        let RegisterDatasetContext {
            data_connector,
            federated_read_table,
            source,
            accelerated_table,
            bootstrap_status,
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
                    data_connector: source.clone(),
                    connector_component: ConnectorComponent::from(&ds),
                })?;

            self.status
                .update_dataset(&ds_name, status::ComponentStatus::Ready);

            return Ok(());
        }

        // Apply partition filters if assigned (Executor mode)
        let mut ds = ds;
        let mut initial_partition_filters = vec![];
        // Only apply partition logic if the dataset is configured for partitioning
        if ds
            .acceleration
            .as_ref()
            .is_some_and(|acc| !acc.partition_by.is_empty())
            && let Some(assignments) = self.partition_assignments()
        {
            let assignments = assignments.read().await;
            let resolved = ds.name.clone().resolve(
                crate::datafusion::SPICE_DEFAULT_CATALOG,
                crate::datafusion::SPICE_DEFAULT_SCHEMA,
            );
            let partition_filters = get_partition_filter_exprs(&resolved, &assignments);
            if !partition_filters.is_empty() {
                tracing::debug!(
                    "For table={}, extracted {} partition filter(s) for assigned partitions.",
                    ds.name,
                    partition_filters.len(),
                );
                initial_partition_filters = partition_filters;
            }

            // Clear partition_by and convert engine to unpartitioned
            let mut ds_mod = (*ds).clone();
            if let Some(acc) = ds_mod.acceleration.as_mut() {
                acc.partition_by = vec![];
                acc.engine = acc.engine.to_unpartitioned();
            }
            ds = Arc::new(ds_mod);
        }

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;
        let accelerator_engine = acceleration_settings.engine;

        let has_on_conflict = !acceleration_settings.on_conflict.is_empty();
        let has_changes_refresh = acceleration_settings
            .refresh_mode
            .is_some_and(|mode| matches!(mode, RefreshMode::Changes));
        let has_write_back =
            acceleration_settings.write_mode == spicepod::acceleration::WriteMode::WriteBack;

        // `on_conflict` forces writes to the accelerator only. When combined with
        // `write_mode: write_back` and `refresh_mode: changes` (CDC), on_conflict acts
        // as WAL UPDATE upsert routing only and write_back can coexist with it.
        // Reject the combination of on_conflict + write_back without CDC, since there
        // would be no path to sync the accelerator writes back to the federated source.
        if has_on_conflict && has_write_back && !has_changes_refresh {
            crate::AcceleratedWriteBackWithOnConflictSnafu {
                dataset_name: ds.name.to_string(),
            }
            .fail()?;
        }

        // `write_mode: write_back` commits to the local accelerator first and
        // asynchronously forwards the same mutation to the federated source.
        // Because the source commit is not part of the synchronous response,
        // require `replication.enabled` as the user's explicit opt-in to those
        // asynchronous source durability semantics.
        if acceleration_settings.write_mode == spicepod::acceleration::WriteMode::WriteBack
            && !replicate
        {
            crate::AcceleratedWriteBackWithoutReplicationSnafu {
                dataset_name: ds.name.to_string(),
            }
            .fail()?;
        }

        self.accelerator_engine_registry
            .get_accelerator_engine(acceleration_settings.engine)
            .await
            .context(AcceleratorEngineNotAvailableSnafu {
                name: accelerator_engine.to_string(),
            })?;

        // Warn if Turso engine is being used
        if accelerator_engine == crate::component::dataset::acceleration::Engine::Turso {
            tracing::warn!(
                "Turso data accelerator (Alpha) is in preview and should not be used in production."
            );
        }

        // The accelerated refresh task will set the dataset status to `Ready` once it finishes loading.
        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);
        let notifier = self
            .df
            .register_table(
                Arc::clone(&ds),
                crate::datafusion::Table::Accelerated {
                    source: data_connector,
                    federated_read_table,
                    accelerated_table,
                    secrets: self.secrets(),
                    bootstrap_status,
                    initial_partition_filters,
                },
            )
            .await
            .context(UnableToAttachDataConnectorSnafu {
                data_connector: source.clone(),
                connector_component: ConnectorComponent::from(&ds),
            })?;

        if notifier.is_some() {
            // spawn a background task to wait for the accelerated table to be ready before creating schedules
            let runtime = ds.runtime();
            let runtime_status = Arc::clone(&self.status);
            let ds = Arc::clone(&ds);
            let dataset_name = ds.name.to_string();
            let dataset_table_ref = ds.name.clone();
            let broadcaster = runtime.executor_outbound_broadcaster();
            let resolved_name = ds.name.clone().resolve(
                crate::datafusion::SPICE_DEFAULT_CATALOG,
                crate::datafusion::SPICE_DEFAULT_SCHEMA,
            );
            tokio::task::spawn(async move {
                // Wait for the dataset's status to reach `Ready` rather than
                // relying on the `Notify`-based completion handle (which is
                // edge-triggered and can race with this spawn for fast
                // initial refreshes).
                runtime_status
                    .wait_for_dataset_ready(&dataset_table_ref)
                    .await;
                // After the executor's initial load for this dataset finishes,
                // ack the scheduler with the partition expressions we currently
                // hold. This is the executor → scheduler readiness signal that
                // lets the scheduler flip the dataset to `Ready` once every
                // assigned partition has at least one executor ack.
                //
                // Send the ack even when the assignment is empty or absent —
                // empty-source / zero-partition datasets still need an ack to
                // trip the scheduler-side `updated_at > 0` shortcut in
                // `PartitionLoadTracker::is_table_loaded`. Always send the
                // canonical (resolved) table name so the scheduler can match
                // the ack against the registered dataset regardless of how
                // the user spelled the table in their spicepod.
                if let Some(b) = broadcaster {
                    let bytes: Vec<Vec<u8>> =
                        if let Some(assignments_lock) = runtime.partition_assignments() {
                            let assignments = assignments_lock.read().await;
                            assignments
                                .get(&resolved_name)
                                .map(|exprs| {
                                    runtime_cluster::encode_partition_exprs(exprs, &dataset_name)
                                })
                                .unwrap_or_default()
                        } else {
                            Vec::new()
                        };
                    let table_name = resolved_name.to_string();
                    // Statistics flow via the periodic ExecutorStatistics reporter,
                    // not this readiness ack.
                    let sent = b
                        .broadcast_partitions_loaded(table_name.clone(), bytes)
                        .await;
                    tracing::info!(
                        "Broadcast initial PartitionsLoaded for {table_name} to {sent} scheduler(s)"
                    );
                }
                if let Err(e) = runtime.create_dataset_or_view_schedule(ds).await {
                    tracing::error!("Failed to create dataset schedule for '{dataset_name}': {e}");
                }
            });
        }

        Ok(())
    }

    pub(crate) async fn apply_dataset_diff(
        self: Arc<Self>,
        current_app: &Arc<App>,
        new_app: &Arc<App>,
    ) {
        let valid_datasets = Arc::clone(&self).get_valid_datasets(new_app, LogErrors(true));
        let startup_datasets = valid_datasets;

        // Validate Cayenne snapshot consistency before initializing accelerators.
        let acceleration_sources: Vec<Arc<dyn AccelerationSource>> =
            startup_datasets.iter().map(|ds| ds.clone_arc()).collect();
        if let Err(err) = validate_cayenne_snapshot_consistency(&acceleration_sources) {
            tracing::error!("{err}");
            return;
        }

        let init_results = self
            .initialize_datasets_accelerators(&startup_datasets)
            .await;
        let existing_datasets = Arc::clone(&self).get_valid_datasets(current_app, LogErrors(false));

        for ds in &startup_datasets {
            let bootstrap_status = match init_results.get(&ds.name) {
                Some(Ok(status)) => status.clone(),
                Some(Err(_)) => {
                    // Error already logged in initialize_datasets_accelerators
                    continue;
                }
                None => {
                    tracing::error!("Dataset {} missing from initialization results", ds.name);
                    continue;
                }
            };

            if let Some(current_ds) = existing_datasets.iter().find(|d| d.name == ds.name) {
                if ds != current_ds {
                    Arc::clone(&self).update_dataset(Arc::clone(ds)).await;
                }
            } else {
                self.status
                    .update_dataset(&ds.name, status::ComponentStatus::Initializing);
                Arc::clone(&self)
                    .load_dataset(
                        Arc::clone(ds),
                        bootstrap_status,
                        Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
                    )
                    .await;
            }
        }

        // Remove datasets that are no longer in the app
        for ds in &current_app.datasets {
            if !new_app.datasets.iter().any(|d| d.name == ds.name) {
                let ds_name = match Dataset::parse_table_reference(&ds.name) {
                    Ok(ds_name) => ds_name,
                    Err(err) => {
                        tracing::error!(
                            "Unable to unload dataset {}: {err}\nReport a bug to request support: https://github.com/spiceai/spiceai/issues ",
                            ds.name
                        );
                        continue;
                    }
                };
                let ds_acceleration = match ds
                    .acceleration
                    .clone()
                    .map(crate::component::dataset::acceleration::Acceleration::try_from)
                    .transpose()
                {
                    Ok(ds_acceleration) => ds_acceleration,
                    Err(err) => {
                        tracing::error!(
                            "Unable to unload dataset {ds_name}: {err}\nReport a bug to request support: https://github.com/spiceai/spiceai/issues"
                        );
                        continue;
                    }
                };

                self.status
                    .update_dataset(&ds_name, status::ComponentStatus::Disabled);
                Arc::clone(&self)
                    .remove_dataset(ds_name, ds_acceleration.as_ref())
                    .await;
            }
        }
    }

    /// Initialize datasets configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `SQLite`).
    /// Returns a `HashMap` mapping each dataset name to its initialization result, which contains
    /// the `BootstrapStatus` on success or an error on failure.
    async fn initialize_datasets_accelerators(
        &self,
        datasets: &[Arc<Dataset>],
    ) -> HashMap<TableReference, Result<BootstrapStatus>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let init_futures = datasets.iter().map(|ds| {
            let ds = Arc::clone(ds);
            let spaced_tracer = Arc::clone(&spaced_tracer);
            let status = Arc::clone(&self.status);
            let accelerator_engine_registry = Arc::clone(&self.accelerator_engine_registry);

            async move {
                // Non-accelerated datasets or disabled acceleration are always successfully initialized
                if ds.acceleration.as_ref().is_none_or(|acc| !acc.enabled) {
                    return (ds.name.clone(), Ok(BootstrapStatus::None));
                }

                let Some(acceleration_settings) = &ds.acceleration else {
                    unreachable!("acceleration is Some and enabled");
                };

                let accelerator = match accelerator_engine_registry
                    .get_accelerator_engine(acceleration_settings.engine)
                    .await
                    .context(AcceleratorEngineNotAvailableSnafu {
                        name: acceleration_settings.engine.to_string(),
                    }) {
                    Ok(accelerator) => accelerator,
                    Err(err) => {
                        let ds_name = &ds.name;
                        status.update_dataset(
                            ds_name,
                            status::ComponentStatus::error_with_message(err.to_string()),
                        );
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                        return (ds.name.clone(), Err(err));
                    }
                };

                match accelerator.init(ds.as_ref()).await.context(
                    AcceleratorInitializationFailedSnafu {
                        name: acceleration_settings.engine.to_string(),
                    },
                ) {
                    Ok(bootstrap_status) => {
                        if bootstrap_status.is_bootstrapped() {
                            update_cached_dataset_timestamps(ds.as_ref()).await;
                        }
                        (ds.name.clone(), Ok(bootstrap_status))
                    }
                    Err(err) => {
                        let ds_name = &ds.name;
                        status.update_dataset(
                            ds_name,
                            status::ComponentStatus::error_with_message(err.to_string()),
                        );
                        metrics::datasets::LOAD_ERROR.add(1, &[]);
                        warn_spaced!(spaced_tracer, "{} {err}", ds_name.table());
                        (ds.name.clone(), Err(err))
                    }
                }
            }
        });

        let results = join_all(init_futures).await;
        let init_results: HashMap<TableReference, Result<BootstrapStatus>> =
            results.into_iter().collect();

        init_results
    }

    /// Returns a list of valid datasets from the given App, skipping any that fail to parse and logging an error for them.
    #[cfg(any(feature = "duckdb", feature = "sqlite"))]
    pub(crate) async fn get_initialized_datasets(
        self: Arc<Self>,
        app: &Arc<App>,
        log_errors: LogErrors,
    ) -> Vec<Arc<Dataset>> {
        let valid_datasets = Arc::clone(&self).get_valid_datasets(app, log_errors);
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
    accelerated_table: Option<Arc<AcceleratedTable>>,
    bootstrap_status: BootstrapStatus,
}

#[expect(clippy::result_large_err)]
fn validate_dataset(ds: &Arc<Dataset>) -> Result<()> {
    if ds.has_full_text_column() && !ds.is_accelerated() {
        return Err(FullTextSearchRequiresAccelerationSnafu {
            dataset_name: ds.name.to_string(),
        }
        .build());
    }
    Ok(())
}

/// Updates the `fetched_at` column for all records in a cached dataset that was bootstrapped.
/// This is necessary for caching mode to ensure all bootstrapped records have a valid timestamp.
async fn update_cached_dataset_timestamps(dataset: &Dataset) {
    let is_caching_mode = dataset
        .acceleration
        .as_ref()
        .and_then(|acc| acc.refresh_mode)
        .is_some_and(|mode| matches!(mode, RefreshMode::Caching));

    if !is_caching_mode {
        return;
    }

    let is_reset_expiry_on_load_enabled = dataset
        .acceleration
        .as_ref()
        .is_some_and(|acc| acc.snapshots_reset_expiry_on_load_enabled);

    if !is_reset_expiry_on_load_enabled {
        return;
    }

    match CachingEngineSys::try_new(dataset, OpenOption::OpenExisting).await {
        Ok(caching_sys) => {
            if let Err(e) = caching_sys.update_fetched_at() {
                tracing::warn!(
                    "Failed to update _fetched_at for cached dataset {}: {e}",
                    dataset.name
                );
            } else {
                tracing::info!(
                    "Updated _fetched_at for all records in cached dataset {}",
                    dataset.name
                );
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to initialize caching engine for {}: {e}",
                dataset.name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataconnector::{
        ConnectorParams, DataConnectorFactory, DataConnectorResult, NewDataConnectorResult,
        register_connector_factory,
    };
    use crate::parameters::ParameterSpec;
    use async_trait::async_trait;
    use datafusion::datasource::TableProvider;
    use std::any::Any;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingConnectorFactory {
        creates: Arc<AtomicUsize>,
    }

    impl DataConnectorFactory for CountingConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create(
            &self,
            _params: ConnectorParams,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
            let creates = Arc::clone(&self.creates);
            Box::pin(async move {
                creates.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(CountingConnector) as Arc<dyn DataConnector>)
            })
        }

        fn prefix(&self) -> &'static str {
            "counting_on_demand"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }

        fn static_schema(
            &self,
            _params: &ConnectorParams,
            dataset: &crate::component::dataset::Dataset,
        ) -> Option<arrow_schema::SchemaRef> {
            crate::component::dataset::declared_schema::declared_schema_for(dataset)
                .ok()
                .flatten()
        }
    }

    #[derive(Debug)]
    struct CountingConnector;

    #[async_trait]
    impl DataConnector for CountingConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _dataset: &Dataset,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            unimplemented!("on-demand startup should not create or read from this connector")
        }
    }

    #[tokio::test]
    async fn deferred_dataset_with_declared_columns_does_not_create_connector_at_startup() {
        use spicepod::semantic::Column;
        let creates = Arc::new(AtomicUsize::new(0));
        register_connector_factory(
            "counting_on_demand",
            Arc::new(CountingConnectorFactory {
                creates: Arc::clone(&creates),
            }),
        )
        .await;

        let mut dataset =
            spicepod::component::dataset::Dataset::new("counting_on_demand:any", "lazy_dataset");
        dataset.ready_state = spicepod::component::dataset::ReadyState::OnRegistration;
        dataset.columns = vec![Column::new("id").with_type("bigint")];

        let app = app::AppBuilder::new("on_demand_test")
            .with_dataset(dataset)
            .build();
        let runtime = Arc::new(crate::Runtime::builder().with_app(app).build().await);

        Arc::clone(&runtime).set_components_initializing().await;
        Arc::clone(&runtime).load_datasets().await;

        assert_eq!(creates.load(Ordering::SeqCst), 0);
        let dataset_ref = TableReference::parse_str("lazy_dataset");
        assert_eq!(
            runtime.status().get_dataset_statuses().get(&dataset_ref),
            Some(&status::ComponentStatus::Ready)
        );
        assert!(runtime.df.has_pending_initializations());
    }

    #[tokio::test]
    async fn elasticsearch_full_text_requires_acceleration() {
        let mut dataset = spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        dataset.columns = vec![
            spicepod::semantic::Column::new("body").with_full_text_search(
                spicepod::semantic::FullTextSearchConfig::enabled().with_row_id("id"),
            ),
        ];
        dataset.full_text_search = Some(spicepod::fts::FtsStore {
            enabled: true,
            engine: Some("elasticsearch".to_string()),
            params: None,
        });

        let app = app::AppBuilder::new("fts_validation")
            .with_dataset(dataset.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let dataset = DatasetBuilder::try_from(dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(runtime)
            .build()
            .expect("valid runtime dataset");

        let err = validate_dataset(&Arc::new(dataset))
            .expect_err("elasticsearch fts should require acceleration");
        assert!(
            err.to_string()
                .contains("acceleration is required for full text search"),
            "unexpected error: {err}"
        );
    }
}
