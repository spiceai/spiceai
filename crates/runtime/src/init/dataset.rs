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

use crate::dataconnector::parameters::RuntimeConnectorContext;
use crate::datafusion::resolve_table_reference;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::accelerated::refresh_completion::{RefreshCompletionOutcome, RefreshCompletionWaiter};
use crate::cluster::partition::get_partition_filter_exprs;
use crate::dataaccelerator::BootstrapStatus;
use crate::dataconnector::reconnecting::{
    ConnectorBuilder, ReconnectingConnector, SourceUnavailable,
};
use crate::dataconnector::refresh_source::ConnectorRefreshSource;
use crate::datafusion::AcceleratorBootstrap;
use crate::init::dataset_initialization::DatasetInitialization;
use crate::init::dataset_loads::DatasetLoad;
use crate::{
    AcceleratedTableInvalidChangesSnafu, AcceleratorEngineNotAvailableSnafu,
    DataConnectorNotInBuildSnafu, DrasiWithoutChangeStreamSnafu,
    DurableWriteBackCompositePrimaryKeySnafu, DurableWriteBackPrerequisitesUnmetSnafu,
    DurableWriteBackRecreatingModeSnafu, DurableWriteBackUndeclaredPrimaryKeySnafu,
    DurableWriteBackUnsupportedBySourceSnafu, DurableWriteBackWithRetentionSnafu, Error,
    FullTextSearchRequiresAccelerationSnafu, HotReloadRefreshFailedSnafu,
    HotReloadRefreshTimedOutSnafu, LogErrors, OdbcNotInstalledSnafu, PermanentDatasetFailureSnafu,
    Result, Runtime, UnableToAttachDataConnectorSnafu, UnableToBuildDatasetSnafu,
    UnableToCreateAcceleratedTableSnafu, UnableToInitializeDataConnectorSnafu,
    UnableToLoadDatasetConnectorSnafu, UnknownDataConnectorSnafu,
    component::dataset::{
        Dataset,
        acceleration::{Acceleration, DurableWriteBackKey, Engine, Mode, RefreshMode},
        builder::DatasetBuilder,
    },
    component::{
        AcceleratedComponent, deprecated_ready_state_warning, disabled_acceleration_warning,
    },
    dataaccelerator::{AccelerationSource, validate_snapshot_paths},
    dataconnector::{
        self, ConnectorComponent, DataConnector, ODBC_DATACONNECTOR, SCYLLADB_DATACONNECTOR,
        SCYLLADB_FEATURE,
        deferred::DeferredConnector,
        localpod::{LOCALPOD_DATACONNECTOR, LocalPodConnector},
        parameters::ConnectorParamsBuilder,
    },
    embeddings::connector::EmbeddingConnector,
    federated::FederatedTable,
    search::full_text::connector::FullTextConnector,
    status,
    tracing_util::dataset_registered_trace,
};
use app::App;
use datafusion::common::{ResolvedTableReference, TableReference};
use futures::StreamExt;
use futures::future::join_all;
use opentelemetry::KeyValue;
use runtime_async::is_shutdown_cancellation;
use runtime_metrics::{self as metrics, components::register_component_metric};
use runtime_table::accelerated::checkpoint_primary_key::records_acceleration_primary_key;
use snafu::prelude::*;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};
use util::{error_spaced, warn_spaced};

/// How long a hot reload waits for the accelerated table it just recreated to
/// complete its first refresh before abandoning the in-place swap and reloading
/// the dataset from scratch.
///
/// Sized to cover an ordinary reload of a non-file acceleration — file
/// accelerations do not take this path at all
/// (`accelerated_dataset_supports_hot_reload`) — while still recovering without a
/// process restart when the refresh never completes.
///
/// This bounds one dataset, not one apply: `apply_dataset_diff` updates changed
/// datasets sequentially, so an apply changing several of them can spend this
/// bound once per dataset.
const HOT_RELOAD_INITIAL_REFRESH_TIMEOUT: Duration = Duration::from_mins(5);

/// What a caller of [`Runtime::invalidate_cached_results_because`] is doing to the
/// dataset, which decides what the operator is told they will observe when the
/// invalidation itself fails.
#[derive(Clone, Copy, Debug)]
enum CacheInvalidation {
    /// The dataset stays configured and will be queryable again, though its contents
    /// may change. It may be unregistered while its replacement is registered, so this
    /// says nothing about whether the table stays in place.
    Reload,
    /// The dataset is being unloaded and stops being queryable.
    Unload,
}

/// The warning for a cached-results invalidation that failed — a degrade-and-continue
/// path, so this line is the only account an operator gets of why a removed or reloaded
/// dataset keeps answering.
///
/// A pure function so the wording is asserted rather than assumed: see
/// `a_failed_unload_invalidation_says_the_dataset_keeps_answering`.
fn cache_invalidation_warning(
    dataset: &TableReference,
    cause: CacheInvalidation,
    source: &dyn std::fmt::Display,
) -> String {
    match cause {
        CacheInvalidation::Reload => format!(
            "Dataset '{dataset}' is updating, but the results cached from its previous contents could not be invalidated, so queries may be answered from them until they expire. Cause: {source}"
        ),
        CacheInvalidation::Unload => format!(
            "Dataset '{dataset}' was unloaded, but the results cached from it could not be invalidated, so queries may keep being answered from the dataset that is no longer there until they expire. Cause: {source}"
        ),
    }
}

/// Warn an operator about what their dataset's or view's acceleration block asks for and
/// the runtime will not do as written: settings that `enabled: false` discards (#13514), and
/// the deprecated `acceleration.ready_state`, honoured but superseded by the component's own
/// `ready_state` (#13749).
///
/// Deliberately **not** in `DatasetBuilder`/`ViewBuilder`'s `TryFrom`, where both started.
/// Those conversions are not the load path: `datasets_iter` and `get_valid_views` run them on
/// every call to `get_valid_datasets`/`get_valid_views`, and `GET /v1/datasets`, every
/// accelerated component's `initialized_sources()` and the hot-reload comparison are among
/// those callers — each passing `LogErrors(false)` precisely to say "do not log from here".
/// A warning emitted inside the conversion therefore printed once per *call*, not once per
/// component. Emitting here puts both behind the same `log_errors` gate as the load errors
/// beside them, so they are tied to a load rather than to a read.
pub(crate) fn warn_about_acceleration_block(
    component: AcceleratedComponent,
    name: &str,
    acceleration: Option<&spicepod::acceleration::Acceleration>,
    log_errors: LogErrors,
) {
    if !log_errors.0 {
        return;
    }
    let Some(acceleration) = acceleration else {
        return;
    };

    // Both formatters escape the name: a *quoted* Spicepod identifier passes validation
    // carrying a newline, and would otherwise forge a second log line.
    let ignored = acceleration.fields_ignored_when_disabled();
    if !ignored.is_empty() {
        tracing::warn!(
            "{}",
            disabled_acceleration_warning(component, name, &ignored)
        );
    }

    // Reading the deprecated key is the point.
    #[expect(deprecated)]
    let sets_deprecated_ready_state = acceleration.ready_state.is_some();
    if sets_deprecated_ready_state {
        tracing::warn!("{}", deprecated_ready_state_warning(component, name));
    }
}

/// Publish `dataset_acceleration_rows_superseded` at `0` for each reason a
/// refresh of `ds` can report, so the series exist before the first one. A
/// dataset whose refreshes report none gets no series.
fn seed_superseded_rows(ds: &Dataset, data_connector: &dyn DataConnector) {
    use util::session_state::SupersededReason;

    let Some(acceleration) = ds.acceleration.as_ref().filter(|a| a.enabled) else {
        return;
    };
    let refresh_mode = data_connector.resolve_refresh_mode(acceleration.refresh_mode);
    let reasons: &[SupersededReason] = match cayenne_key_rule(ds, acceleration, refresh_mode) {
        Some(KeyRule::NewestByTime(_)) => &[SupersededReason::Older, SupersededReason::Arrival],
        Some(KeyRule::LastArrival) => &[SupersededReason::Arrival],
        Some(KeyRule::ChangeOrder) | None => return,
    };
    for reason in reasons {
        metrics::acceleration::ROWS_SUPERSEDED.add(
            0,
            &[
                KeyValue::new("dataset", ds.name.to_string()),
                KeyValue::new("reason", reason.label()),
            ],
        );
    }
}

/// One sample of the startup `Dataset load summary` line: how many datasets have
/// finished their first load, how many failed it, and how many are still loading.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DatasetLoadSummary {
    pub(crate) ready: usize,
    pub(crate) unhealthy: usize,
    pub(crate) loading: usize,
    pub(crate) total: usize,
}

impl DatasetLoadSummary {
    /// Buckets each dataset by whether its first load has finished.
    ///
    /// `Ready` counts as ready, and so does `Refreshing` when `has_ever_been_ready`
    /// says the dataset was loaded before (see
    /// [`status::RuntimeStatus::has_dataset_ever_been_ready`]); a `Refreshing`
    /// dataset that never was is on its first load and counts as loading, alongside
    /// `Initializing`. `Error` counts as unhealthy. `Disabled`, `NotLoaded` and
    /// `ShuttingDown` fall into no bucket: the summary reports the progress of
    /// loads that are under way, and a dataset in one of those states has no load
    /// in flight to report, so it neither inflates the ready count nor keeps the
    /// sampler alive.
    pub(crate) fn from_statuses(
        statuses: &HashMap<TableReference, status::ComponentStatus>,
        has_ever_been_ready: impl Fn(&TableReference) -> bool,
    ) -> Self {
        let mut summary = Self {
            total: statuses.len(),
            ..Self::default()
        };
        for (dataset, current) in statuses {
            match current {
                status::ComponentStatus::Ready => summary.ready += 1,
                status::ComponentStatus::Refreshing if has_ever_been_ready(dataset) => {
                    summary.ready += 1;
                }
                status::ComponentStatus::Refreshing | status::ComponentStatus::Initializing => {
                    summary.loading += 1;
                }
                status::ComponentStatus::Error(_) => summary.unhealthy += 1,
                status::ComponentStatus::Disabled
                | status::ComponentStatus::NotLoaded
                | status::ComponentStatus::ShuttingDown => {}
            }
        }
        summary
    }

    /// The sampler stops once no dataset is still loading.
    pub(crate) fn is_settled(&self) -> bool {
        self.loading == 0
    }

    /// The line users watch for progress. Phrasing deliberately avoids "error"/"failed"
    /// so quickstart smoke tests that grep `spice.log` for those tokens don't get false
    /// positives on a healthy startup; real per-dataset failure is already logged at
    /// WARN level inside `load_dataset`.
    pub(crate) fn log_line(&self, elapsed_secs: u64) -> String {
        format!(
            "Dataset load summary (after {elapsed_secs}s): {}/{} ready, {} unhealthy, {} still initializing.",
            self.ready, self.total, self.unhealthy, self.loading
        )
    }
}

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
        //
        // `LogErrors(true)` here, and `LogErrors(false)` in `load_views` below, because
        // the two validate the same views and only one of them may report: this pre-pass
        // is the one that always runs. The snapshot validation between here and
        // `load_views` returns early on failure, so reporting from `load_views` instead
        // loses every view's load error, its status update, and its
        // discarded-acceleration warning on exactly the startups that already went wrong.
        let valid_views = Arc::clone(&self).get_valid_views(&app, LogErrors(true));
        self.initialize_views_accelerators(&valid_views).await;

        let valid_datasets = Arc::clone(&self).get_valid_datasets(&app, LogErrors(true));
        let startup_datasets = valid_datasets;

        if let Some(snapshots) = app.snapshots.as_deref() {
            let any_snapshot_mode_dataset = startup_datasets.iter().any(|ds| {
                ds.acceleration.as_ref().is_some_and(|acceleration| {
                    acceleration.enabled
                        && acceleration
                            .refresh_mode
                            .is_some_and(|mode| mode.is_snapshot_only())
                })
            });
            if let Some(warning) =
                runtime_acceleration::snapshot::notifications::unread_queue_warning(
                    snapshots,
                    any_snapshot_mode_dataset,
                )
            {
                tracing::warn!("{warning}");
            }
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
        if let Err(err) =
            validate_snapshot_paths(initialized_sources, &self.accelerator_engine_registry).await
        {
            tracing::error!("{err}");
            return;
        }

        // Keyed by resolved name, so a `localpod` path of `source`, `public.source`, or
        // `spice.public.source` finds the same parent. The value carries the dataset's own name
        // for the log line its task writes.
        let mut dataset_futures: HashMap<
            ResolvedTableReference,
            (TableReference, PendingDatasetLoad),
        > = HashMap::new();
        // Loads waiting on a snapshot's first publication, which component startup does not
        // wait for. Their tasks stay registered for cancellation on reload or shutdown.
        let mut background_roots = HashSet::new();
        let mut background_chains: Vec<(TableReference, PendingDatasetLoad)> = Vec::new();
        // Keyed by parent so several `localpod` datasets reading from one dataset all chain
        // behind the same load, rather than the first one consuming it.
        let mut localpod_by_parent: HashMap<
            ResolvedTableReference,
            Vec<(Arc<Dataset>, AcceleratorBootstrap)>,
        > = HashMap::new();

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

            self.status
                .update_dataset(&ds.name, status::ComponentStatus::Initializing);

            if let Some(parent) = localpod_parent(ds) {
                localpod_by_parent
                    .entry(parent)
                    .or_default()
                    .push((Arc::clone(ds), bootstrap_status));
                continue;
            }

            let pending = bootstrap_status.is_pending();
            let ds_clone = Arc::clone(ds);
            let cloned_self = Arc::clone(&self);
            let load_semaphore = Arc::clone(&semaphore);
            let load = self.dataset_loads.begin(&ds.name);
            let future: PendingDatasetLoad = Box::pin(async move {
                cloned_self
                    .load_dataset(ds_clone, bootstrap_status, load_semaphore, load)
                    .await;
            });
            let resolved = resolve_table_reference(ds.name.clone());
            let future = if pending {
                background_roots.insert(resolved.clone());
                self.track_snapshot_bootstrap(
                    &ds.name,
                    future,
                    Some(self.initial_load.cancel.clone()),
                )
                .await
            } else {
                future
            };
            dataset_futures.insert(resolved, (ds.name.clone(), future));
        }

        // Each `localpod` dataset loads after the dataset it reads from, and a `localpod`
        // dataset reading from another `localpod` dataset loads after that one, so every chain
        // hangs off the load of a non-`localpod` dataset.
        // Startup datasets chained behind a parent's load, with the parent they
        // finish with, and the done-token of each such parent.
        let mut localpod_children: Vec<(ResolvedTableReference, ResolvedTableReference)> =
            Vec::new();
        let mut parent_tokens: HashMap<ResolvedTableReference, CancellationToken> = HashMap::new();
        let roots: Vec<_> = localpod_by_parent
            .extract_if(|parent, _| dataset_futures.contains_key(parent))
            .collect();
        for (parent, children) in roots {
            let parent_background = background_roots.contains(&parent);
            if !parent_background {
                parent_tokens.entry(parent.clone()).or_default();
                for (ds, _) in &children {
                    localpod_children
                        .push((resolve_table_reference(ds.name.clone()), parent.clone()));
                }
            }
            // Signalled once the parent's load ends, however it ends, so a chain running in
            // the background starts only after its parent.
            let parent_done = tokio_util::sync::CancellationToken::new();
            let mut chains = Vec::new();
            for (ds, bootstrap_status) in children {
                let background = parent_background
                    || localpod_chain_pending(&ds, &bootstrap_status, &localpod_by_parent);
                let name = ds.name.clone();
                let chain = Arc::clone(&self).localpod_load_chain(
                    ds,
                    bootstrap_status,
                    &mut localpod_by_parent,
                    ReplacesRegistration::No,
                );
                if background {
                    let parent_done = parent_done.clone();
                    let chain: PendingDatasetLoad = Box::pin(async move {
                        parent_done.cancelled().await;
                        chain.await;
                    });
                    let chain = self
                        .track_snapshot_bootstrap(
                            &name,
                            chain,
                            Some(self.initial_load.cancel.clone()),
                        )
                        .await;
                    background_chains.push((name, chain));
                } else {
                    chains.push(chain);
                }
            }
            if let Some((_, parent_future)) = dataset_futures.get_mut(&parent) {
                let parent_load = std::mem::replace(parent_future, Box::pin(async {}));
                *parent_future = Box::pin(async move {
                    {
                        let _parent_done = parent_done.drop_guard();
                        parent_load.await;
                    }
                    join_all(chains).await;
                });
            }
        }

        // Whatever is still queued has no chain to a dataset that is loading: its parent is
        // not configured, failed to initialize, or is part of a `localpod` cycle.
        for (ds, _) in localpod_by_parent.into_values().flatten() {
            let path_table_ref = TableReference::parse_str(ds.path());
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

        let mut spawned_tasks = vec![];
        let dispatched = dataset_futures.len() + background_chains.len();

        // Signalled when a startup dataset's load task ends, however it ends, so
        // each view waits only on the datasets it reads from rather than on every
        // dataset in the Spicepod: one dataset still retrying must not keep views
        // over healthy datasets from registering.
        let mut dataset_done: HashMap<ResolvedTableReference, CancellationToken> = HashMap::new();
        for (child, parent) in &localpod_children {
            if let Some(token) = parent_tokens.get(parent) {
                dataset_done.insert(child.clone(), token.clone());
            }
        }
        for (resolved, (ds, dataset_load_future)) in dataset_futures {
            let background = background_roots.contains(&resolved);
            let done = if background {
                None
            } else {
                let token = parent_tokens.get(&resolved).cloned().unwrap_or_default();
                dataset_done.insert(resolved.clone(), token.clone());
                Some(token)
            };
            let handle = tokio::spawn(async move {
                let _done = done.map(CancellationToken::drop_guard);
                tracing::info!("Dataset {ds} initializing...");
                dataset_load_future.await;
            });
            // A reader's first publication is independent of component startup.
            if !background {
                spawned_tasks.push(handle);
            }
        }
        for (ds, chain) in background_chains {
            tokio::spawn(async move {
                tracing::info!("Dataset {ds} initializing...");
                chain.await;
            });
        }

        // Aggregate startup summary so users see "3/5 queued, 2 skipped at init" at a glance
        // instead of having to piece that together from per-dataset warnings. `dispatched`
        // is the number of spawned load tasks, which can be less than the number of
        // datasets that will load because localpod datasets are chained behind their
        // parent dataset's task. Wording avoids the words "failed" / "error" so it
        // doesn't trip quickstart CI checks that grep spice.log for those tokens as a
        // sentinel for real failures.
        let init_skipped = init_results.values().filter(|r| r.is_err()).count();
        let total = startup_datasets.len();
        if total > 0 {
            tracing::info!(
                "Loading datasets: {dispatched} tasks dispatched, {init_skipped} skipped at accelerator init (of {total} total; localpod datasets may be chained)."
            );
        }

        // Spawn a best-effort follow-up summary that samples the status registry every
        // 30s until every dataset has finished its first load or failed it, so users
        // see periodic progress on slow-loading pods without having to query
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
                    if statuses.is_empty() {
                        return;
                    }
                    let summary = DatasetLoadSummary::from_statuses(&statuses, |dataset| {
                        status_handle.has_dataset_ever_been_ready(dataset)
                    });
                    tracing::info!("{}", summary.log_line(elapsed_secs));
                    if summary.is_settled() {
                        return;
                    }
                }
            });
        }

        // Views are loaded as soon as the datasets each one reads from have loaded,
        // not after every dataset has: a dataset that keeps failing must only hold
        // back the views that depend on it.
        let view_tasks = Arc::clone(&self).load_views(&app, &dataset_done);

        let _ = join_all(spawned_tasks).await;
        let _ = join_all(view_tasks).await;
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
                Ok(ds) => {
                    warn_about_acceleration_block(
                        AcceleratedComponent::Dataset,
                        &spicepod_ds.name,
                        spicepod_ds.acceleration.as_ref(),
                        log_errors,
                    );
                    Some(Arc::new(ds))
                }
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

    /// Resolve the accelerated dataset named `table_ref` and, if a write carrying new
    /// columns (`target_schema`) arrives, evolve its accelerator schema in place per the
    /// dataset's `on_schema_change` policy. This is the entrypoint the OpenTelemetry
    /// metrics ingest path uses to admit new metric dimensions.
    ///
    /// Returns `Ok(Some(schema))` when the caller must rebuild its batch against `schema`
    /// before writing — either because an evolution was just applied, or because the
    /// accelerator schema is already a superset (e.g. a concurrent writer evolved it, or
    /// the change was a no-op) and the batch must still match its canonical field order.
    /// Returns `Ok(None)` when nothing was evolved — unknown dataset, no acceleration, a
    /// `block`/`fail` policy, or an unsupported/incompatible change. In every `Ok(None)`
    /// case the caller's write proceeds unchanged.
    pub async fn evolve_accelerated_schema_for_write(
        self: &Arc<Self>,
        table_ref: &TableReference,
        target_schema: &arrow_schema::SchemaRef,
    ) -> std::result::Result<Option<arrow_schema::SchemaRef>, crate::datafusion::Error> {
        let Some(app) = self.read_app().await else {
            return Ok(None);
        };
        let Some(dataset) = Arc::clone(self)
            .get_valid_datasets(&app, LogErrors(false))
            .into_iter()
            .find(|ds| &ds.name == table_ref)
        else {
            return Ok(None);
        };

        self.df
            .evolve_and_rebind_accelerated_schema(&dataset, self.secrets(), target_schema)
            .await
    }

    /// The acceleration checkpoint schema for the dataset named `table_ref`, or `None` when
    /// there is no such dataset or no persisted checkpoint. The OpenTelemetry ingest uses this
    /// to build a metric batch against the stored (wide) schema when the dataset is not yet
    /// registered — e.g. a `sink` dataset parked until its first write after a restart — so a
    /// data point that omits a NULL dimension still materializes every stored column instead
    /// of a narrower batch the write would reject.
    pub async fn accelerated_checkpoint_schema(
        self: &Arc<Self>,
        table_ref: &TableReference,
    ) -> Option<arrow_schema::SchemaRef> {
        let app = self.read_app().await?;
        let dataset = Arc::clone(self)
            .get_valid_datasets(&app, LogErrors(false))
            .into_iter()
            .find(|ds| &ds.name == table_ref)?;
        crate::dataconnector::sink::accelerated_checkpoint_schema(&dataset).await
    }

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
        self.build_dataset_connector(Arc::clone(&ds))
            .await
            .map_err(|err| self.report_connector_failure(&ds, err))
    }

    /// Builds `ds`'s connector and registers the component metrics it offers.
    async fn build_dataset_connector(&self, ds: Arc<Dataset>) -> Result<Arc<dyn DataConnector>> {
        let data_connector = self.get_dataconnector_from_dataset(Arc::clone(&ds)).await?;
        Self::register_connector_metrics(&ds, &data_connector);
        Ok(data_connector)
    }

    /// Reports a dataset's connector construction failure and returns the error to
    /// propagate.
    ///
    /// This is the only failure connector construction raises, and reporting it is
    /// owned here: the component status, the `LOAD_ERROR` count, and one log line
    /// at the level the failure's permanence warrants. Callers -- both
    /// `try_load_dataset_once` and the hot-reload path in `update_dataset` --
    /// propagate it without reporting it again, so one failure is counted once and
    /// writes one status. See #12365.
    fn report_connector_failure(&self, ds: &Dataset, err: Error) -> Error {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);
        let ds_name = &ds.name;
        self.status.update_dataset(
            ds_name,
            status::ComponentStatus::error_with_message(load_failure_status(ds, &err.to_string())),
        );
        metrics::datasets::LOAD_ERROR.add(1, &[]);
        if is_permanent_dataset_failure(&err) {
            error_spaced!(
                spaced_tracer,
                "Error initializing dataset {}. {err}",
                ds_name.table()
            );
            return PermanentDatasetFailureSnafu {
                dataset: ds_name.clone(),
                reason: err.to_string(),
            }
            .build();
        }
        warn_spaced!(
            spaced_tracer,
            "Error initializing dataset {}. {err}",
            ds_name.table()
        );
        crate::Error::UnableToInitializeDataConnector { source: err.into() }
    }

    /// Registers the component metrics `data_connector` offers for `ds`.
    fn register_connector_metrics(ds: &Dataset, data_connector: &Arc<dyn DataConnector>) {
        let source = ds.source();
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
    }

    async fn try_load_dataset_once(
        &self,
        ds: Arc<Dataset>,
        bootstrap_status: impl Into<AcceleratorBootstrap>,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<()> {
        let bootstrap_status = bootstrap_status.into();
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        preflight_dataset(&ds, &self.status, &spaced_tracer)?;

        // Deferred path. Each connector factory decides via
        // `static_schema()` whether the dataset can be registered
        // without contacting the source. If the factory returns a
        // schema AND the runtime-side gate (read-only,
        // on_registration, no embeddings/FTS) passes, register a
        // placeholder and skip eager connector construction. The
        // resolver hook in `datafusion::create_logical_plan` will
        // trigger `ensure_ready` on first reference.
        if bootstrap_status.is_none()
            && self.is_deferral_eligible(&ds)
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
                    source: Box::new(source),
                    data_connector: ds.source().to_string(),
                    connector_component: crate::dataconnector::ConnectorComponent::from(
                        ds.as_ref(),
                    ),
                });
        }

        let connector_start = Instant::now();
        let connector =
            if !bootstrap_status.is_pending() && Self::serves_existing_acceleration(&ds).await {
                // Served from its existing acceleration at once; the real connector is
                // built in the background, so the source's state never holds the dataset.
                Self::reconnecting_connector(&ds)
            } else {
                // `load_dataset_connector` owns reporting for this failure -- the
                // status, the `LOAD_ERROR` count, and a log line at the level its
                // permanence warrants -- and raises no other error, so propagating it
                // unreported leaves nothing unreported (#12365). Its reporting is
                // unconditional, including during teardown, so this path needs no
                // `is_shutdown()` guard of its own to keep the count at one.
                self.load_dataset_connector(Arc::clone(&ds)).await?
            };
        tracing::debug!(dataset = %ds.name, duration_ms = connector_start.elapsed().as_millis(), "Dataset connector created");

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
    ///
    /// `load` is taken from `dataset_loads` when the load is queued, not when it
    /// starts, so a load queued behind another (a `localpod` dataset behind its
    /// parent) can be superseded while it waits.
    async fn load_dataset(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        bootstrap_status: AcceleratorBootstrap,
        load_semaphore: Arc<Semaphore>,
        load: DatasetLoad,
    ) {
        let shutdown_token = self.status.shutdown_token();
        // A dataset that reads snapshots has no acceleration to load them into until
        // the engine that created them is known, which only their metadata says.
        // Outside the supersede scope below: a supersede waits for a restore that has
        // started rather than dropping it (see `dataset_loads`), and the resolution
        // stops on its own while it is only waiting.
        let (ds, bootstrap_status) = if ds.is_pending_snapshot_source() {
            let resolved = tokio::select! {
                resolved = self.resolve_snapshot_source(&ds, &load_semaphore, &load) => resolved,
                () = shutdown_token.cancelled() => None,
            };
            let Some(resolved) = resolved else {
                return;
            };
            resolved
        } else {
            (ds, bootstrap_status)
        };
        let load_fut = async {
            let pending = bootstrap_status.is_pending();
            let bootstrap_status = if pending
                && ds.ready_state == crate::component::dataset::ReadyState::OnRegistration
                && !self.df.table_exists(&ds.name)
            {
                // Source fallback must not open the acceleration file. Race it with
                // the restore so an unavailable source cannot delay a publication.
                // It holds no storage generation, so it cannot reinitialize the one
                // the restore writes under.
                let fallback = self.load_dataset_with_retry(
                    Arc::clone(&ds),
                    bootstrap_status.source_fallback(),
                    Arc::clone(&load_semaphore),
                    &load,
                );
                let restore = bootstrap_status.complete();
                tokio::pin!(restore);
                tokio::select! {
                    status = &mut restore => status,
                    () = fallback => restore.await,
                }
            } else {
                bootstrap_status.complete().await
            };
            // A restore completed at initialization already updated the cached
            // timestamps there; only one completed here still needs it.
            let restored = pending && bootstrap_status.is_bootstrapped();
            if restored && ds.is_snapshot_source() && self.refuses_projected_publisher(&ds).await {
                return;
            }
            if restored {
                update_cached_dataset_timestamps(ds.as_ref()).await;
            }
            self.load_dataset_with_retry(Arc::clone(&ds), bootstrap_status, load_semaphore, &load)
                .await;
            if restored {
                self.df.clear_cached_plans().await;
                self.invalidate_cached_results_for(&ds.name).await;
            }
        };

        // Use tokio::select! so that backoff sleeps inside `retry` are immediately
        // interrupted when the runtime begins shutting down (e.g. on ctrl-c), or
        // when a Spicepod change supersedes this load. Dropping `load_fut` there
        // drops any attempt in progress, which releases the attempt lock that
        // `DatasetLoads::supersede` waits for before the change applies.
        tokio::select! {
            () = load_fut => {},
            () = shutdown_token.cancelled() => {},
            () = load.superseded() => {},
        }
    }

    async fn load_dataset_with_retry(
        &self,
        ds: Arc<Dataset>,
        bootstrap_status: AcceleratorBootstrap,
        load_semaphore: Arc<Semaphore>,
        load: &DatasetLoad,
    ) {
        // Why a dataset with an acceleration on disk is not served from it, logged
        // once when its first attempt fails. Not before: a source that is reached only
        // when it is read (HTTP, S3) can fail after the dataset is already served
        // from its acceleration, and then it does not wait.
        let waits_for_source = parking_lot::Mutex::new(match Self::waits_for_source_reason(&ds) {
            Some(reason)
                if !bootstrap_status.is_pending()
                    && crate::dataconnector::sink::recorded_checkpoint_schema(&ds)
                        .await
                        .is_some() =>
            {
                Some(waits_for_source_message(&ds.name, &reason))
            }
            _ => None,
        });

        let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();
        let bootstrap_status = tokio::sync::Mutex::new(bootstrap_status);
        let _ = retry(retry_strategy, || async {
            if self.status.is_shutdown() {
                return Err(RetryError::permanent(
                    crate::Error::UnableToInitializeDataConnector {
                        source: "Runtime is shutting down".into(),
                    },
                ));
            }

            // A Spicepod change replaced or removed this configuration while the
            // attempt waited to start, so it must not register.
            let Some(_attempt) = load.start_attempt().await else {
                return Err(RetryError::permanent(
                    crate::Error::UnableToInitializeDataConnector {
                        source: "Dataset configuration was replaced".into(),
                    },
                ));
            };

            let mut bootstrap = bootstrap_status.lock().await;
            if bootstrap.needs_reinitialization() {
                let Some(result) = self
                    .initialize_datasets_accelerators(std::slice::from_ref(&ds))
                    .await
                    .remove(&ds.name)
                else {
                    return Err(RetryError::permanent(
                        crate::Error::UnableToInitializeDataConnector {
                            source: "Missing accelerator bootstrap result".into(),
                        },
                    ));
                };
                // Initialization leaves a reader's restore pending, as at startup.
                *bootstrap = result.map_err(RetryError::transient)?.restore_once().await;
            }
            match self
                .try_load_dataset_once(
                    Arc::clone(&ds),
                    bootstrap.clone(),
                    Some(Arc::clone(&load_semaphore)),
                )
                .await
            {
                Ok(()) => Ok(()),
                Err(err) if self.status.is_shutdown() => Err(RetryError::permanent(err)),
                Err(err) => {
                    if let Some(message) = waits_for_source.lock().take() {
                        tracing::info!("{message}");
                    }
                    if matches!(err, Error::PermanentDatasetFailure { .. }) {
                        Err(RetryError::permanent(err))
                    } else {
                        Err(RetryError::transient(err))
                    }
                }
            }
        })
        .await;
    }

    fn snapshot_bootstrap_task_name(name: &TableReference) -> String {
        format!(
            "snapshot_bootstrap:{}",
            resolve_table_reference(name.clone())
        )
    }

    /// Keeps background bootstrap in the runtime's task registry until it can
    /// register the reader, or its configuration is replaced or removed.
    async fn track_snapshot_bootstrap(
        self: &Arc<Self>,
        name: &TableReference,
        load: Pin<Box<dyn Future<Output = ()> + Send>>,
        initial_load_cancel: Option<tokio_util::sync::CancellationToken>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let cancel = self.status.shutdown_token().child_token();
        let task_cancel = cancel.clone();
        let completion = self
            .start_runtime_task(
                &Self::snapshot_bootstrap_task_name(name),
                Some(cancel),
                async move {
                    let initial_load_cancel = async move {
                        match initial_load_cancel {
                            Some(token) => token.cancelled().await,
                            None => std::future::pending().await,
                        }
                    };
                    tokio::select! {
                        biased;
                        () = task_cancel.cancelled() => {},
                        () = initial_load_cancel => {},
                        () = load => {},
                    }
                    Ok(())
                },
            )
            .await;
        let name = name.clone();
        Box::pin(async move {
            if let Err(err) = completion.await {
                tracing::error!("Failed to initialize snapshot reader '{name}': {err}");
            }
        })
    }

    async fn cancel_snapshot_bootstrap(&self, name: &TableReference) {
        let task = self
            .tasks
            .write()
            .await
            .remove(&Self::snapshot_bootstrap_task_name(name));
        if let Some(task) = task {
            task.cancel(Duration::from_secs(5)).await;
        }
    }

    /// Bootstraps the accelerator (if any) for a single dataset and loads it
    /// through the normal dataset lifecycle (connector creation,
    /// `AcceleratedTable` construction, `DataFusion` registration, retry with
    /// backoff on transient failure) — identical to how every spicepod-declared
    /// dataset is loaded via [`Runtime::load_dataset`].
    ///
    /// Used for datasets synthesized at runtime (e.g. by catalog-level
    /// acceleration) rather than declared in the Spicepod `datasets:` list.
    // Only the PostgreSQL catalog connector synthesizes datasets today.
    #[cfg(feature = "postgres")]
    pub(crate) async fn load_synthesized_dataset(self: Arc<Self>, ds: Arc<Dataset>) {
        // Throttle accelerator init to the same `dataset_load_parallelism` budget
        // `load_dataset` enforces. Catalog-level acceleration spawns one
        // `load_synthesized_dataset` task per table (potentially hundreds), and
        // `initialize_datasets_accelerators` does real init work/IO; without a
        // permit here every table would initialize its accelerator at once, ahead
        // of any throttling. The permit is held ONLY around init and dropped before
        // `load_dataset` (which acquires its own permit) -- holding both at once
        // would deadlock once `dataset_load_parallelism` tasks each await a second.
        let bootstrap_status = {
            let Ok(_init_permit) = self.dataset_load_semaphore.acquire().await else {
                unreachable!("Semaphore is never closed.");
            };
            match self
                .initialize_datasets_accelerators(std::slice::from_ref(&ds))
                .await
                .remove(&ds.name)
            {
                Some(Ok(status)) => status,
                Some(Err(_)) => return, // error already logged in initialize_datasets_accelerators
                None => {
                    let message = format!(
                        "Dataset {} missing from accelerator initialization results",
                        ds.name
                    );
                    tracing::error!("{message}");
                    self.status.update_dataset(
                        &ds.name,
                        status::ComponentStatus::error_with_message(message),
                    );
                    return;
                }
            }
        };

        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Initializing);

        let semaphore = Arc::clone(&self.dataset_load_semaphore);
        let load = self.dataset_loads.begin(&ds.name);
        self.load_dataset(ds, bootstrap_status, semaphore, load)
            .await;
    }

    /// A [`ReconnectingConnector`] for `ds` that builds its real connector — and
    /// registers its component metrics — on first use.
    pub(crate) fn reconnecting_connector(ds: &Arc<Dataset>) -> Arc<dyn DataConnector> {
        let ds_for_build = Arc::clone(ds);
        let build: ConnectorBuilder = Arc::new(move || {
            let ds = Arc::clone(&ds_for_build);
            Box::pin(async move { ds.runtime().build_dataset_connector(Arc::clone(&ds)).await })
        });
        Arc::new(ReconnectingConnector::new(ds.source(), build))
    }

    /// Whether `ds` is served from its existing acceleration as soon as it loads,
    /// connecting to its source in the background: it
    /// [may be](Self::may_serve_existing_acceleration), and its acceleration has a
    /// checkpointed schema to serve.
    pub(crate) async fn serves_existing_acceleration(ds: &Dataset) -> bool {
        Self::may_serve_existing_acceleration(ds) && Self::has_existing_acceleration(ds).await
    }

    /// Whether `ds`'s acceleration has a checkpointed schema to serve from.
    async fn has_existing_acceleration(ds: &Dataset) -> bool {
        Self::existing_acceleration_schema(ds).await.is_some()
    }

    /// Whether `ds`'s configuration allows serving it from an existing acceleration
    /// before its source answers.
    ///
    /// Only datasets whose startup the source does not otherwise shape qualify:
    /// read-only (the write path binds the source when the table is built), not
    /// `changes` (CDC keeps no dataset checkpoint to serve from; see #14611) or
    /// `caching` (which is ready immediately and serves its cache on its own), not
    /// `append` without a `time_column` (which takes the connector's append stream
    /// when the table is built), no embedding or full-text search columns (whose
    /// search indexes wrap the source's provider, so search would fail until the
    /// source answers), no Drasi forwarding (which rides the change stream), not `mode: file_create`, which starts from an empty acceleration by
    /// design, and not the local file connector, which attaches its file watcher
    /// when the table is registered and has no outage to ride out.
    ///
    /// The schema policy must be one the deferred provider reproduces: under
    /// `on_schema_change: block` without recreate-on-mismatch, `FederatedTable::new`
    /// itself defers on the checkpoint schema when the source differs, so resolving
    /// the source later reaches the same outcome as resolving it now; datasets that
    /// recreate or evolve on a schema change need the live schema to decide.
    ///
    /// This runs before the connector exists, so the refresh mode is resolved the way
    /// `DataConnector::resolve_refresh_mode` does by default.
    fn may_serve_existing_acceleration(ds: &Dataset) -> bool {
        ds.acceleration.as_ref().is_some_and(|a| a.enabled)
            && Self::waits_for_source_reason(ds).is_none()
    }

    /// The configuration that makes an accelerated dataset wait for its source
    /// instead of being served from an existing acceleration (see
    /// [`Self::may_serve_existing_acceleration`]), named the way the user wrote it, or
    /// `None` when it does not wait or is not accelerated.
    fn waits_for_source_reason(ds: &Dataset) -> Option<String> {
        use crate::component::dataset::OnSchemaChange;
        use crate::component::dataset::acceleration::Mode;

        let acceleration = ds.acceleration.as_ref().filter(|a| a.enabled)?;
        // An unset mode resolves the way the connector resolves it (`changes` for
        // `debezium` and `cdc`), which this check runs before the connector exists to ask.
        let refresh_mode = acceleration.refresh_mode.unwrap_or_else(|| {
            runtime_acceleration::acceleration::unset_refresh_mode_for_connector(ds.source())
        });
        let reason = if ds.access().allows_write() {
            "`access: read_write`".to_string()
        } else if ds.has_embeddings() {
            "embedding columns".to_string()
        } else if ds.has_full_text_column() {
            "full-text search columns".to_string()
        } else if ds.drasi.as_ref().is_some_and(is_drasi_forwarding) {
            "Drasi forwarding".to_string()
        } else if acceleration.mode == Mode::FileCreate {
            "`mode: file_create`".to_string()
        } else if ds.source() == crate::dataconnector::file::FILE_DATACONNECTOR {
            "the `file` connector".to_string()
        } else if refresh_mode == RefreshMode::Changes {
            "`refresh_mode: changes`".to_string()
        } else if refresh_mode == RefreshMode::Caching {
            "`refresh_mode: caching`".to_string()
        } else if refresh_mode == RefreshMode::Append && ds.time_column.is_none() {
            "`refresh_mode: append` without a `time_column`".to_string()
        } else if ds.on_schema_change != OnSchemaChange::Block {
            format!("`on_schema_change: {}`", ds.on_schema_change)
        } else if crate::schema_evolution::recreates_on_schema_mismatch(
            acceleration,
            ds.on_schema_change,
            refresh_mode,
        ) {
            format!("`mode: {}`", acceleration.mode)
        } else {
            return None;
        };
        Some(reason)
    }

    /// The reason a file-accelerated dataset that failed to load is not served from
    /// its acceleration, for its status message.
    fn not_served_reason(ds: &Dataset) -> Option<String> {
        if !ds.is_file_accelerated() {
            return None;
        }
        Self::waits_for_source_reason(ds)
    }

    /// Registers `ds` against its existing acceleration without waiting for the
    /// source. The federated side resolves in the background, retrying the source,
    /// while queries are served from the acceleration. The acceleration settings the
    /// source would have inferred are recovered from the checkpoint schema, which
    /// records them, so the dataset registers with the same primary key, indexes and
    /// sort columns it would get from the source. Logs `reason`.
    fn defer_to_existing_acceleration(
        &self,
        ds: &Arc<Dataset>,
        data_connector: &Arc<dyn DataConnector>,
        resolved_refresh_mode: RefreshMode,
        checkpoint_schema: arrow_schema::SchemaRef,
        reason: &SourceUnavailable,
    ) -> (Arc<Dataset>, FederatedTable) {
        let ds = Self::apply_inferred_acceleration(
            Arc::clone(ds),
            &checkpoint_schema,
            resolved_refresh_mode,
        );
        let federated_table = FederatedTable::new_deferred(
            Arc::new(ds.spec.clone()),
            crate::dataconnector::refresh_source::ReportingRefreshSource::new_arc(
                ConnectorRefreshSource::new_arc(Arc::clone(data_connector), Arc::clone(&ds)),
                Arc::clone(&ds),
                Arc::clone(&self.status),
                match reason {
                    SourceUnavailable::Failed {
                        configuration_error,
                        ..
                    } => Some(*configuration_error),
                    SourceUnavailable::NotContacted => None,
                },
            ),
            checkpoint_schema,
            self.status.shutdown_token(),
        );
        reason.log_serving_from_acceleration(&ds.name);
        (ds, federated_table)
    }

    /// The schema `ds`'s existing acceleration was checkpointed with, when it has one
    /// to serve from.
    /// A checkpoint written before the acceleration's primary key was recorded does
    /// not count: registering without the source could build the accelerator without
    /// a key its table has, so that dataset waits for its source once, and its next
    /// checkpoint records the key.
    async fn existing_acceleration_schema(ds: &Dataset) -> Option<arrow_schema::SchemaRef> {
        let schema = crate::dataconnector::sink::recorded_checkpoint_schema(ds).await?;
        if records_acceleration_primary_key(&schema) {
            return Some(schema);
        }
        tracing::debug!(
            dataset = %ds.name,
            "The acceleration checkpoint for dataset {} does not record its primary key (written by an earlier version), so it waits for its source before serving.",
            ds.name
        );
        None
    }

    /// Resolves `ds`'s source provider, or registers `ds` against its existing
    /// acceleration instead: at once for a dataset
    /// [served from its acceleration](Self::serves_existing_acceleration), or when the
    /// source cannot be read and an acceleration exists. Returns the dataset with any
    /// inferred acceleration settings applied.
    async fn federated_table_or_existing_acceleration(
        &self,
        ds: Arc<Dataset>,
        data_connector: &Arc<dyn DataConnector>,
        resolved_refresh_mode: RefreshMode,
        allow_schema_mismatch: bool,
        snapshot_fallback: bool,
    ) -> Result<(Arc<Dataset>, FederatedTable)> {
        // A dataset served from its existing acceleration registers before its real
        // connector is built (see `try_load_dataset_once`); the source is contacted in
        // the background.
        if !snapshot_fallback
            && data_connector
                .as_any()
                .downcast_ref::<ReconnectingConnector>()
                .is_some_and(|connector| !connector.is_connected())
            && let Some(checkpoint_schema) = Self::existing_acceleration_schema(&ds).await
        {
            return Ok(self.defer_to_existing_acceleration(
                &ds,
                data_connector,
                resolved_refresh_mode,
                checkpoint_schema,
                &SourceUnavailable::NotContacted,
            ));
        }

        let read_result = {
            let context = RuntimeConnectorContext::for_dataset(&ds);
            data_connector.read_provider(&context, &ds).await
        };

        match read_result {
            Ok(provider) => {
                // Gap-fill acceleration settings from schema inference (a no-op when
                // the connector emitted no inferred metadata) before the dataset
                // flows into registration and any changes stream.
                let ds = Self::apply_inferred_acceleration(
                    ds,
                    &provider.schema(),
                    resolved_refresh_mode,
                );
                let federated_table = if snapshot_fallback {
                    FederatedTable::new_unchecked(provider)
                } else {
                    FederatedTable::new(
                        Arc::new(ds.spec.clone()),
                        provider,
                        ConnectorRefreshSource::new_arc(
                            Arc::clone(data_connector),
                            Arc::clone(&ds),
                        ),
                        self.status.shutdown_token(),
                        allow_schema_mismatch,
                    )
                    .await
                };
                Ok((ds, federated_table))
            }
            Err(err) => {
                // We couldn't connect to the federated table. If the dataset has an existing
                // accelerated table, we can defer the federated table creation.
                if !snapshot_fallback
                    && let Some(checkpoint_schema) = Self::existing_acceleration_schema(&ds).await
                {
                    return Ok(self.defer_to_existing_acceleration(
                        &ds,
                        data_connector,
                        resolved_refresh_mode,
                        checkpoint_schema,
                        &SourceUnavailable::failed(&err),
                    ));
                }
                self.status.update_dataset(
                    &ds.name,
                    status::ComponentStatus::error_with_message(load_failure_status(
                        &ds,
                        &err.to_string(),
                    )),
                );
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                let spaced_tracer = Arc::clone(&self.spaced_tracer);
                if !err.is_retriable() {
                    error_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");
                    return PermanentDatasetFailureSnafu {
                        dataset: ds.name.clone(),
                        reason: err.to_string(),
                    }
                    .fail();
                }
                warn_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");
                UnableToLoadDatasetConnectorSnafu {
                    dataset: ds.name.clone(),
                }
                .fail()
            }
        }
    }

    /// Apply schema inference to a freshly-resolved dataset.
    ///
    /// When the source connector emitted inferred-schema metadata, this fills any
    /// acceleration settings the user left unset (primary key, indexes, sort
    /// columns) and returns a rebuilt `Dataset`. Applying it here — before the
    /// `FederatedTable` and registration are created — ensures every refresh mode,
    /// including CDC (`refresh_mode: changes`), observes the inferred values.
    /// `resolved_refresh_mode` (the connector-resolved mode, not the raw Spicepod
    /// value) gates which inferred settings are safe to apply — see
    /// `apply_inferred_schema`. Schema inference is always attempted; this returns
    /// `ds` unchanged when the dataset is not accelerated or the source emitted no
    /// usable metadata.
    ///
    /// `source_schema` is the source provider's schema, or — when the dataset is
    /// served from an existing acceleration before the source answers — the
    /// acceleration checkpoint's schema, which preserves the inferred metadata
    /// recorded when the acceleration was built.
    fn apply_inferred_acceleration(
        ds: Arc<Dataset>,
        source_schema: &arrow_schema::SchemaRef,
        resolved_refresh_mode: RefreshMode,
    ) -> Arc<Dataset> {
        use crate::component::dataset::schema_inference::apply_inferred_schema;
        use data_components::inferred_schema::InferredSchema;

        // Skip when the dataset is not accelerated — including an `acceleration`
        // block that is present but `enabled: false`, which the rest of the runtime
        // treats as non-accelerated. Schema inference is always attempted, so a
        // source that exposed no inferred metadata simply yields an empty set below.
        if !ds.acceleration.as_ref().is_some_and(|a| a.enabled) {
            return ds;
        }

        let inferred = InferredSchema::from_metadata(source_schema.metadata());
        // Only acceleration settings (primary key / indexes / sort / shard key) are
        // applied here; inferred sizing and column statistics ride on the provider
        // schema metadata and are surfaced as table statistics / tuning inputs
        // elsewhere. Skip the refresh_sql parse and dataset rebuild when nothing
        // acceleration-relevant was inferred (e.g. sizing only).
        // `shard_key` is consumed only by Cayenne (see `apply_inferred_shard_key`);
        // for other engines it is not acceleration-relevant and must not, on its
        // own, force a refresh_sql parse + dataset rebuild below.
        let shard_key_relevant = !inferred.shard_key.is_empty()
            && ds.acceleration.as_ref().is_some_and(|a| {
                a.engine.to_unpartitioned()
                    == crate::component::dataset::acceleration::Engine::Cayenne
            });
        if inferred.primary_key.is_empty()
            && inferred.indexes.is_empty()
            && inferred.sort_columns.is_empty()
            && !shard_key_relevant
        {
            return ds;
        }

        // Resolve the schema the accelerator will actually store. When a refresh_sql
        // reshapes the schema, validate inferred columns against the projected
        // schema; if it can't be parsed, skip inference rather than risk injecting a
        // column the accelerator would later reject.
        let effective_schema = match ds
            .acceleration
            .as_ref()
            .and_then(|a| a.refresh_sql.as_ref())
        {
            Some(sql) => match crate::datafusion::refresh_sql::parse_refresh_sql(
                ds.name.clone(),
                sql.as_str(),
                Arc::clone(source_schema),
            ) {
                Ok((_, projected)) => projected,
                Err(error) => {
                    tracing::debug!(
                        dataset = %ds.name,
                        %error,
                        "Skipping schema inference; could not parse refresh_sql to validate inferred columns"
                    );
                    return ds;
                }
            },
            None => Arc::clone(source_schema),
        };

        let mut new_ds = (*ds).clone();
        if let Some(acceleration) = new_ds.acceleration.as_mut() {
            apply_inferred_schema(
                acceleration,
                &inferred,
                &effective_schema,
                ds.name.table(),
                resolved_refresh_mode,
            );
        }
        Arc::new(new_ds)
    }

    pub(crate) async fn register_loaded_dataset(
        self: Arc<Self>,
        mut ds: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        accelerated_table: Option<crate::datafusion::PreparedAcceleratedTable>,
        bootstrap_status: AcceleratorBootstrap,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<()> {
        // Owned (not borrowed from `ds`) so the dataset can be rebuilt below by
        // schema inference without holding a borrow across the reassignment.
        let source = ds.source().to_string();
        let snapshot_fallback = bootstrap_status.is_pending();
        let replaces_snapshot_reader =
            bootstrap_status.is_bootstrapped() && self.df.table_exists(&ds.name);
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
            warn_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");
            return Err(err);
        }

        if let Some(acceleration) = ds.acceleration.as_ref().filter(|a| a.enabled) {
            let refresh_mode = data_connector.resolve_refresh_mode(acceleration.refresh_mode);
            let dataset_name = ds.name.to_string();
            let rule = cayenne_key_rule(&ds, acceleration, refresh_mode);
            if let (Some(rule), Some(key)) = (rule, acceleration.primary_key.as_ref()) {
                tracing::info!("{}", key_rule_line(&dataset_name, key, rule));
            }
            if !acceleration.on_conflict.is_empty() {
                let warning = if acceleration.engine == Engine::Cayenne {
                    cayenne_on_conflict_warning(&dataset_name, acceleration, rule)
                } else {
                    deprecated_on_conflict_warning(&dataset_name, acceleration, refresh_mode)
                };
                tracing::warn!("{warning}");
            }
            if let Some(KeyRule::NewestByTime(time_column)) = rule
                && refresh_mode == RefreshMode::Append
                && acceleration.refresh_append_overlap.is_none()
            {
                tracing::warn!(
                    "{}",
                    newest_by_time_without_overlap_warning(&dataset_name, time_column)
                );
            }
        }

        // A `drasi` block only takes effect through the change stream, so a
        // dataset without one forwards nothing. Silently publishing no changes
        // to a configured Drasi source is worse than refusing the dataset: the
        // continuous queries downstream would simply never fire, with nothing to
        // point at.
        if ds.drasi.as_ref().is_some_and(is_drasi_forwarding) {
            let refresh_mode = ds
                .acceleration
                .as_ref()
                .map(|a| data_connector.resolve_refresh_mode(a.refresh_mode));

            let reason = match refresh_mode {
                None => Some("not accelerated".to_string()),
                // Lowercased to match the value as it is spelled in the
                // Spicepod, which is what the operator has to change.
                Some(mode) if mode != RefreshMode::Changes => Some(format!(
                    "accelerated with 'refresh_mode: {}'",
                    format!("{mode:?}").to_lowercase()
                )),
                Some(_) if !data_connector.supports_changes_stream() => Some(format!(
                    "backed by the {source} connector, which does not support change data capture"
                )),
                Some(_) => None,
            };

            if let Some(reason) = reason {
                let err = DrasiWithoutChangeStreamSnafu {
                    dataset_name: ds.name.to_string(),
                    reason,
                }
                .build();
                warn_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");
                return Err(err);
            }
        }

        // Durable write-back delivers each committed row to the source. Unless
        // the connector can do that atomically, delivery has to emulate an
        // upsert as a standalone delete plus a separate insert — and because the
        // accelerator is CDC-fed from that same source, the delete echoes back
        // and erases the committed row. A failure between the two legs then
        // leaves the write gone from both sides with nothing reported. Refuse
        // the dataset instead of accepting a config that can lose data.
        if let Some(acceleration) = &ds.acceleration
            && acceleration.enabled
            && acceleration.resolves_to_durable_write_back()
            && !data_connector.supports_durable_write_back_delivery()
        {
            // `supports_durable_write_back_delivery` is a synchronous capability
            // flag, so this answer is the same on every attempt: refuse
            // permanently rather than rebuilding the connector forever.
            let err = DurableWriteBackUnsupportedBySourceSnafu {
                dataset_name: ds.name.to_string(),
                connector: source.clone(),
            }
            .build();
            return Err(refuse_permanently(&ds, &self.status, &spaced_tracer, &err));
        }

        // Bypass the deferred-mismatch gate when the dataset recreates on a schema change, so
        // create_accelerated_table drops + recreates the table with the new schema instead of
        // deferring. `recreates_on_schema_mismatch` is the single source of truth for the exact
        // conditions (`file_update` with refreshes enabled, or `on_schema_change:
        // drop_and_recreate` + `refresh_mode: full` on a recreate-capable engine); sharing it
        // keeps this gate and the recreate decision in create_accelerated_table aligned.
        let allow_schema_mismatch = ds.acceleration.as_ref().is_some_and(|a| {
            crate::schema_evolution::recreates_on_schema_mismatch(
                a,
                ds.on_schema_change,
                data_connector.resolve_refresh_mode(a.refresh_mode),
            )
        });

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
        let resolved_refresh_mode = data_connector
            .resolve_refresh_mode(ds.acceleration.as_ref().and_then(|a| a.refresh_mode));

        let (resolved_ds, federated_table) = self
            .federated_table_or_existing_acceleration(
                Arc::clone(&ds),
                &data_connector,
                resolved_refresh_mode,
                allow_schema_mismatch,
                snapshot_fallback,
            )
            .await?;
        ds = resolved_ds;

        tracing::debug!(dataset = %ds.name, duration_ms = schema_start.elapsed().as_millis(), "Dataset schema inference complete");

        // Release the load permit before registration so other datasets can
        // begin their source-facing work while this one registers.
        drop(load_guard);

        // `on_schema_change: fail` records an actionable message when a schema change
        // deferred the provider. Capture it now (the table is moved into registration)
        // and surface it as the dataset status AFTER registration completes —
        // registration marks checkpointed datasets Ready, which the fail policy
        // must override. The deferred retry keeps serving the existing acceleration
        // and self-heals (a later refresh restores Ready) if the source reverts.
        let schema_change_failure = federated_table.schema_change_failure().map(str::to_string);

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
                if !replaces_snapshot_reader {
                    metrics::datasets::COUNT.add(1, &[KeyValue::new("engine", engine)]);
                }
                seed_superseded_rows(&ds, data_connector.as_ref());

                if let Some(message) = schema_change_failure {
                    self.status.update_dataset(
                        &ds.name,
                        status::ComponentStatus::error_with_message(message),
                    );
                }

                Ok(())
            }
            Err(err) => {
                self.status.update_dataset(
                    &ds.name,
                    status::ComponentStatus::error_with_message(err.to_string()),
                );
                metrics::datasets::LOAD_ERROR.add(1, &[]);
                if is_permanent_dataset_failure(&err) {
                    error_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");
                    return PermanentDatasetFailureSnafu {
                        dataset: ds.name.clone(),
                        reason: err.to_string(),
                    }
                    .fail();
                }
                warn_spaced!(spaced_tracer, key = load_failure_log_key(&ds.name), "{err}");

                Err(err)
            }
        }
    }

    /// Unregisters `ds_name` and discards what is cached over it.
    ///
    /// `cause` is the caller's intent, which this cannot infer: most callers
    /// unregister the dataset only to register a replacement, and telling the
    /// results cache those were unloads would deny them the stale-serving window
    /// their reload is exactly what revalidates.
    async fn remove_dataset(
        self: Arc<Self>,
        ds_name: TableReference,
        ds_acceleration: Option<&Acceleration>,
        cause: CacheInvalidation,
    ) -> bool {
        self.remove_dataset_with_bootstrap(
            ds_name,
            ds_acceleration,
            cause,
            &BootstrapStatus::None.into(),
        )
        .await
    }

    async fn remove_dataset_with_bootstrap(
        self: Arc<Self>,
        ds_name: TableReference,
        ds_acceleration: Option<&Acceleration>,
        cause: CacheInvalidation,
        bootstrap: &AcceleratorBootstrap,
    ) -> bool {
        let was_registered = self.df.table_exists(&ds_name);
        if was_registered && let Some(datasets_health_monitor) = &self.datasets_health_monitor {
            datasets_health_monitor
                .deregister_dataset(&ds_name.to_string())
                .await;
        }
        // Pending construction also owns storage without a catalog entry.
        if let Err(e) = self
            .df
            .remove_table_with_bootstrap(&ds_name, bootstrap)
            .await
        {
            tracing::warn!("Unable to unload dataset {}: {}", &ds_name, e);
            return false;
        }

        // Drop the dataset's CDC schema-evolution settings; a reload re-installs
        // them at registration before the changes stream starts.
        crate::accelerated::refresh_task::changes::remove_cdc_schema_evolution(&ds_name);
        self.status.remove_dataset_freshness(&ds_name);

        // Deregistering the table is not enough to stop it being read: a cached
        // logical plan holds the `TableSource` it was planned against, so a query
        // executed before this point keeps executing against the retired provider,
        // answering rows from a dataset the catalog no longer lists (#14251). A
        // cached result reads the same way. Both discards happen *here*, after the
        // deregistration, rather than in the callers that used to do them before it:
        // a query planning in the window between a caller's discard and the
        // deregistration would otherwise cache a plan over the provider being retired.
        //
        // The plan discard is deliberately the blanket one, as `update_dataset` and
        // `remove_view` both use. `invalidate_for_table` would reach this dataset's own
        // plans, but a dataset leaves the app at human timescale and the cost of being
        // wrong about which plans reach it is the defect above, so the price paid is a
        // replan of everything else.
        self.df.clear_cached_plans().await;
        self.invalidate_cached_results_because(&ds_name, cause)
            .await;

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

        if was_registered {
            metrics::datasets::COUNT.add(-1, &[KeyValue::new("engine", engine)]);
        }
        true
    }

    #[cfg(test)]
    async fn update_dataset(self: Arc<Self>, ds: Arc<Dataset>) {
        self.update_dataset_with_bootstrap(ds, BootstrapStatus::None.into())
            .await;
    }

    async fn update_dataset_with_bootstrap(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        bootstrap: AcceleratorBootstrap,
    ) {
        // Defense in depth. Today the only caller is `apply_dataset_diff`, which
        // preflights through `initialize_datasets_accelerators` and skips a
        // refused dataset before it gets here. But both branches below mutate
        // accelerator state — `reload_accelerated_dataset` swaps it, the fallback
        // removes the dataset outright — so a second caller that forgot the
        // preflight would destroy the state of a dataset whose configuration
        // cannot deliver what it has already acknowledged. Cheap to check here,
        // and the running dataset is left as it was.
        if preflight_dataset(&ds, &self.status, &self.spaced_tracer).is_err() {
            // `preflight_dataset` reported it.
            return;
        }

        self.status
            .update_dataset(&ds.name, status::ComponentStatus::Refreshing);

        // Updating a dataset may cause the cached LogicalPlans to be
        // obsolete, so we remove them
        self.df.clear_cached_plans().await;

        // A reload can change what the dataset reads, so results read from its
        // previous contents must stop being served as fresh, and a query that
        // planned against the previous registration must not store its result.
        // Both of those read the table-change clock this marks. The replacement
        // below marks it again: this mark cannot reject a result whose read
        // starts after it and still lands on the old registration.
        self.invalidate_cached_results_for(&ds.name).await;

        match Arc::clone(&self)
            .load_dataset_connector(Arc::clone(&ds))
            .await
        {
            Ok(connector) => {
                // File accelerated datasets don't support hot reload.
                if Self::accelerated_dataset_supports_hot_reload(&ds, &*connector) {
                    tracing::info!("Accelerated Dataset {} updating...", &ds.name);
                    match Arc::clone(&self)
                        .reload_accelerated_dataset(
                            Arc::clone(&ds),
                            Arc::clone(&connector),
                            bootstrap.clone(),
                        )
                        .await
                    {
                        Ok(()) => {
                            // Mark again now the swap has happened. The mark above
                            // stops results read before the reload from being served
                            // as fresh, but a query that started after it and read the
                            // previous registration finishes with a `read_started_at`
                            // the clock would accept, so its result must be rejected by
                            // a mark at the replacement itself.
                            self.invalidate_cached_results_for(&ds.name).await;
                            self.status
                                .update_dataset(&ds.name, status::ComponentStatus::Ready);
                            return;
                        }
                        // The reason is the only thing that distinguishes a swap
                        // that could not be built from one whose acceleration
                        // never finished loading, and the fallback hides both.
                        Err(err) => tracing::warn!(
                            "Falling back to a full reload of dataset {}: {err}",
                            ds.name
                        ),
                    }
                }

                // A consumed bootstrap cannot describe storage after a failed replacement.
                // Drain that generation and initialize a fresh one rather than reuse its status.
                let bootstrap = if bootstrap.is_consumed() {
                    if !Arc::clone(&self)
                        .remove_dataset(
                            ds.name.clone(),
                            ds.acceleration.as_ref(),
                            CacheInvalidation::Reload,
                        )
                        .await
                    {
                        return;
                    }
                    let Some(Ok(bootstrap)) = self
                        .initialize_datasets_accelerators(std::slice::from_ref(&ds))
                        .await
                        .remove(&ds.name)
                    else {
                        return;
                    };
                    bootstrap
                } else {
                    bootstrap
                };
                if !Arc::clone(&self)
                    .remove_dataset_with_bootstrap(
                        ds.name.clone(),
                        ds.acceleration.as_ref(),
                        CacheInvalidation::Reload,
                        &bootstrap,
                    )
                    .await
                {
                    return;
                }

                let initialized = DatasetInitialization::plan_eager(
                    Arc::clone(&ds),
                    Arc::clone(&self),
                    Arc::clone(&connector),
                    bootstrap,
                    None,
                    None,
                )
                .initialize()
                .await
                .map(|_ready| ());

                // The registration this dataset reads through has just been
                // replaced, so mark the table again: a query that began after
                // the mark at the top of this reload, and read the registration
                // being replaced, would otherwise store a result the clock
                // accepts as fresh.
                self.invalidate_cached_results_for(&ds.name).await;

                if let Err(e) = initialized {
                    self.status.update_dataset(
                        &ds.name,
                        status::ComponentStatus::error_with_message(e.to_string()),
                    );
                }
            }
            Err(e) => {
                // `load_dataset_connector` set the error status for this failure.
                // Only the hot-reload context it cannot know is added here (#12365).
                tracing::error!("Unable to update dataset {}: {e}", ds.name);
            }
        }
    }

    /// Marks the results-cache table clock for `dataset`, so results read from
    /// what it held before this point stop being served as fresh and a result
    /// read before it cannot be stored as fresh afterwards.
    ///
    /// Degrade and continue, as the write paths that mark the same clock do: a
    /// reload that could not mark it still has to finish, and the warning is how
    /// an operator learns that queries may keep being answered from the previous
    /// contents until `item_ttl` expires.
    async fn invalidate_cached_results_for(&self, dataset: &TableReference) {
        self.invalidate_cached_results_because(dataset, CacheInvalidation::Reload)
            .await;
    }

    /// [`Self::invalidate_cached_results_for`], for a caller that is unloading the
    /// dataset rather than reloading it — which is what the warning on the degrade
    /// path has to say, because the two leave the operator observing different
    /// things: a reload keeps answering from the previous contents, an unload keeps
    /// answering at all.
    async fn invalidate_cached_results_because(
        &self,
        dataset: &TableReference,
        cause: CacheInvalidation,
    ) {
        let caching = self.df.caching();
        // An unload takes the evicting path, which a `cache_key_type: sql` hit needs
        // because such a hit is answered before planning and the plan cache therefore
        // never reaches it. See [`cache::QueryResultsCacheProvider::evict_for_table`]
        // for why the stale-serving window must not absorb an unload.
        let outcome = match cause {
            CacheInvalidation::Reload => caching.invalidate_for_table(dataset.clone()).await,
            CacheInvalidation::Unload => caching.evict_for_table(dataset.clone()).await,
        };

        if let Err(e) = outcome {
            tracing::warn!("{}", cache_invalidation_warning(dataset, cause, &e));
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

    /// Resolve executor partition scoping for `ds` before creating its accelerated table.
    ///
    /// On an executor node (partition assignments present) with a `partition_by`
    /// configured dataset, returns the dataset with `partition_by` cleared and its
    /// engine converted to unpartitioned, plus `Some` partition filters for the
    /// partitions assigned to this executor. `Some(empty)` — no partition assigned —
    /// resolves downstream to a `false` predicate (load no rows) rather than an
    /// unfiltered full-table load. Otherwise returns `ds` unchanged with `None`
    /// (not partition-scoped; retrieve everything).
    async fn resolve_executor_partition_scoping(
        &self,
        ds: Arc<Dataset>,
    ) -> (Arc<Dataset>, Option<Vec<datafusion_expr::Expr>>) {
        if ds
            .acceleration
            .as_ref()
            .is_none_or(|acc| acc.partition_by.is_empty())
        {
            return (ds, None);
        }
        let Some(assignments) = self.partition_assignments() else {
            return (ds, None);
        };

        let assignments = assignments.read().await;
        let resolved = ds.name.clone().resolve(
            crate::datafusion::SPICE_DEFAULT_CATALOG,
            crate::datafusion::SPICE_DEFAULT_SCHEMA,
        );
        let partition_filters = get_partition_filter_exprs(&resolved, &assignments);
        tracing::debug!(
            "For table={}, extracted {} partition filter(s) for assigned partitions.",
            ds.name,
            partition_filters.len(),
        );

        // Clear partition_by and convert engine to unpartitioned.
        let mut ds_mod = (*ds).clone();
        if let Some(acc) = ds_mod.acceleration.as_mut() {
            acc.partition_by = vec![];
            acc.engine = acc.engine.to_unpartitioned();
        }
        (Arc::new(ds_mod), Some(partition_filters))
    }

    async fn reload_accelerated_dataset(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        connector: Arc<dyn DataConnector>,
        bootstrap: AcceleratorBootstrap,
    ) -> Result<()> {
        let read_table = connector
            .read_provider(&RuntimeConnectorContext::for_dataset(&ds), &ds)
            .await
            .map_err(|_| {
                UnableToLoadDatasetConnectorSnafu {
                    dataset: ds.name.clone(),
                }
                .build()
            })?;
        // Same recreate-bypass as the initial-load gate. Previously this honored only
        // `file_update`, so a reloaded `on_schema_change: drop_and_recreate` dataset would not
        // recreate on an incompatible source change; the shared helper fixes that.
        let allow_schema_mismatch = ds.acceleration.as_ref().is_some_and(|a| {
            crate::schema_evolution::recreates_on_schema_mismatch(
                a,
                ds.on_schema_change,
                connector.resolve_refresh_mode(a.refresh_mode),
            )
        });
        let federated_table = FederatedTable::new(
            Arc::new(ds.spec.clone()),
            read_table,
            ConnectorRefreshSource::new_arc(Arc::clone(&connector), Arc::clone(&ds)),
            self.status.shutdown_token(),
            allow_schema_mismatch,
        )
        .await;

        // Remove the schedule if the dataset has one, to prevent scheduling while the dataset is being updated.
        Arc::clone(&self)
            .remove_dataset_or_view_schedule(&ds.name)
            .await?;

        // Mirror the initial-load path: on an executor, scope the recreated table
        // to this node's assigned partitions so a hot reload doesn't load the full
        // source table (or duplicate it across executors).
        let (ds, initial_partition_filters) = self.resolve_executor_partition_scoping(ds).await;

        // create new accelerated table for updated data connector
        let accelerated_table = self
            .df
            .create_accelerated_table(
                &ds,
                Arc::clone(&connector),
                federated_table,
                self.secrets(),
                bootstrap,
                initial_partition_filters,
            )
            .await
            .context(UnableToCreateAcceleratedTableSnafu {
                dataset: ds.name.clone(),
            })?;

        let refresher = accelerated_table.table().refresher();

        // wait for accelerated table to be ready
        if let Some(completion) = refresher.refresh_completion() {
            await_hot_reload_initial_refresh(
                &ds.name,
                &|| refresher.initial_load_completed(),
                completion.any(),
                &self.status.shutdown_token(),
                HOT_RELOAD_INITIAL_REFRESH_TIMEOUT,
            )
            .await?;
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

        let params = ConnectorParamsBuilder::for_dataset(source.into(), ds)
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
        // A dataset that reads acceleration snapshots is served only from its
        // acceleration; its source supplies nothing but the snapshot's schema.
        if ds.is_snapshot_source() {
            return Ok(Arc::new(
                dataconnector::snapshot_source::SnapshotSourceConnector::new(ds),
            ));
        }

        let source = ds.source();

        // Resolve the connector before building parameters. The builder resolves it too — it
        // reads the factory's prefix and parameter list — and fails with
        // `InvalidConnectorType`, which names no alternative, so it used to answer every
        // typo'd `from:` before `UnknownDataConnector` could. See #12415.
        if dataconnector::get_connector_factory(source).await.is_none() {
            return Err(unknown_data_connector(source).await);
        }

        let params = ConnectorParamsBuilder::for_dataset(source.into(), &ds)
            .build(self.secrets(), self.tokio_io_runtime())
            .await
            .context(UnableToInitializeDataConnectorSnafu)?;

        // Unlike most other data connectors, the localpod connector needs a reference to the current DataFusion instance.
        if source == LOCALPOD_DATACONNECTOR {
            return Ok(Arc::new(LocalPodConnector::new(Arc::clone(&self.df))));
        }

        let mut data_connector = if let Some(dc) = dataconnector::create_new_connector(
            source,
            params,
            &RuntimeConnectorContext::for_dataset(&ds),
        )
        .await
        {
            dc.context(UnableToInitializeDataConnectorSnafu {})?
        } else {
            // Only reachable if the connector is deregistered between the check above and
            // this lookup; report the same error rather than a second, blunter one.
            return Err(unknown_data_connector(source).await);
        };

        // Innermost of the stream decorators, so the properties Drasi receives
        // are the source table's own columns. Wrapping outside the embedding
        // decorator would instead publish every computed embedding vector as a
        // node property.
        if let Some(drasi) = ds.drasi.clone().filter(is_drasi_forwarding) {
            tracing::warn!(
                "Drasi change forwarding (Alpha) is in preview and should not be used in production."
            );

            let delivery = crate::drasi::sink_for_dataset(&ds, &drasi)
                .await
                .map_err(|e| crate::Error::UnableToInitializeDataConnector {
                    source: Box::new(e),
                })?;

            data_connector = Arc::new(crate::drasi::connector::DrasiConnector::new(
                data_connector,
                delivery,
            ));
        }

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
        if !ds.is_accelerated() || bootstrap_status.is_pending() {
            // `on_schema_change` only governs accelerated datasets in v1: federated
            // queries always reflect the live source schema, so the policy is inert.
            if !ds.is_accelerated()
                && ds.on_schema_change != crate::component::dataset::OnSchemaChange::Block
            {
                tracing::warn!(
                    dataset = %ds.name,
                    "`on_schema_change: {policy}` has no effect on non-accelerated datasets; it applies to accelerated datasets only",
                    policy = ds.on_schema_change,
                );
            }

            let ds_name: TableReference = ds.name.clone();
            self.df
                .register_table(
                    Arc::clone(&ds),
                    crate::datafusion::Table::Federated {
                        data_connector,
                        federated_read_table,
                        generation: if bootstrap_status.is_pending() {
                            crate::datafusion::FederatedGeneration::SnapshotRestore
                        } else {
                            crate::datafusion::FederatedGeneration::Drain
                        },
                    },
                )
                .await
                .context(UnableToAttachDataConnectorSnafu {
                    data_connector: source.clone(),
                    connector_component: ConnectorComponent::from(ds.as_ref()),
                })?;

            self.status
                .update_dataset(&ds_name, status::ComponentStatus::Ready);

            return Ok(());
        }

        // Apply partition filters if assigned (Executor mode). `None` means the
        // dataset is not partition-scoped (retrieve everything); in executor
        // partitioned mode this is `Some`, so an executor with no assigned
        // partition gets `Some(empty)` — a `false` predicate that loads no rows —
        // rather than an unfiltered full load.
        let (ds, initial_partition_filters) = self.resolve_executor_partition_scoping(ds).await;

        // ACCELERATED TABLE
        let acceleration_settings =
            ds.acceleration
                .as_ref()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: ds.name.to_string(),
                })?;
        let accelerator_engine = acceleration_settings.engine;

        // `write_mode: write_back` commits to the local accelerator and a delivery
        // worker carries the write to the federated source afterwards, so the source
        // lags. Require `replication.enabled` as the user's explicit opt-in to that,
        // and to the source's own changes arriving over the change stream.
        if acceleration_settings.write_mode == spicepod::acceleration::WriteMode::WriteBack
            && !replicate
        {
            crate::AcceleratedWriteBackWithoutReplicationSnafu {
                dataset_name: ds.name.to_string(),
            }
            .fail()?;
        }
        // Writes kept only in the acceleration would be overwritten by the changes a
        // change stream applies for the same keys. The connector's default counts:
        // a CDC source refreshes by changes when `refresh_mode` is omitted.
        if acceleration_settings.write_mode == spicepod::acceleration::WriteMode::Acceleration
            && data_connector.resolve_refresh_mode(acceleration_settings.refresh_mode)
                == RefreshMode::Changes
        {
            crate::AccelerationWriteModeWithChangesSnafu {
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
                    accelerated_table: accelerated_table.map(Box::new),
                    secrets: self.secrets(),
                    bootstrap_status,
                    initial_partition_filters,
                },
            )
            .await
            .context(UnableToAttachDataConnectorSnafu {
                data_connector: source.clone(),
                connector_component: ConnectorComponent::from(ds.as_ref()),
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
                // Gate on the dataset's status reaching `Ready` rather than on
                // the refresh completion: the ack reports the partitions this
                // executor serves, and the dataset is only servable once its
                // status has been published.
                // A shutdown before the dataset became ready means the initial
                // load never finished: there is no partition state worth acking.
                if runtime_status
                    .wait_for_dataset_ready(&dataset_table_ref)
                    .await
                    == crate::status::WaitOutcome::ShuttingDown
                {
                    return;
                }
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
                    if sent == 0 {
                        // Fast initial loads can finish before any scheduler
                        // control stream is connected; the broadcaster caches
                        // the ack and replays it on scheduler connect.
                        tracing::info!(
                            "Initial PartitionsLoaded for {table_name} cached; no scheduler connected yet, will replay on connect"
                        );
                    } else {
                        tracing::info!(
                            "Broadcast initial PartitionsLoaded for {table_name} to {sent} scheduler(s)"
                        );
                    }
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

        let existing_datasets = Arc::clone(&self).get_valid_datasets(current_app, LogErrors(false));

        // Only the datasets this diff loads or updates are initialized: `mode: file_create`
        // deletes the acceleration state on init, and an unchanged dataset keeps serving from
        // the `AcceleratedTable` it already has. The one exception is a `localpod` dataset
        // whose parent this diff reloads: it reads through the table the parent is about to
        // replace, so it reloads too, after its parent.
        let changed_datasets: Vec<Arc<Dataset>> = valid_datasets
            .iter()
            .filter(|ds| {
                existing_datasets
                    .iter()
                    .find(|current| current.name == ds.name)
                    .is_none_or(|current| current != *ds)
            })
            .map(Arc::clone)
            .collect();
        let datasets_to_apply = with_localpod_dependents(changed_datasets, &valid_datasets);

        // A load of a configuration this diff replaces or removes may still be
        // retrying, and its first successful attempt would register that
        // configuration over the one the Spicepod now declares (#1458). Stop it
        // before anything below initializes the new configuration's accelerator
        // or registers it. A dataset whose load was still retrying never
        // registered, so its new configuration is loaded below like an added
        // dataset's, retrying until its source answers, rather than updated once.
        //
        // A snapshot reader still waiting for its first publication is such a
        // load: superseding it drops its bootstrap, so the source table it may
        // have registered meanwhile is replaced below rather than kept serving
        // the replaced configuration. Its runtime task has then ended, and only
        // its registry entry is left to remove.
        let removed_datasets = current_app
            .datasets
            .iter()
            .filter(|ds| !new_app.datasets.iter().any(|d| d.name == ds.name))
            .filter_map(|ds| Dataset::parse_table_reference(&ds.name).ok());
        let mut still_loading = HashSet::new();
        for name in datasets_to_apply
            .iter()
            .map(|ds| ds.name.clone())
            .chain(removed_datasets)
        {
            if self.dataset_loads.supersede(&name).await {
                still_loading.insert(name.clone());
            }
            self.cancel_snapshot_bootstrap(&name).await;
        }

        let init_results = self
            .initialize_datasets_accelerators(&datasets_to_apply)
            .await;

        // Added datasets are loaded on spawned tasks rather than awaited inline:
        // `load_dataset` retries a transient failure with unbounded Fibonacci
        // backoff, and `apply_app` holds `apply_app_lock` across this whole
        // function, so awaiting one unreachable source parks this apply and every
        // apply queued behind it until the process restarts. A dataset that cannot
        // load lands in an error state reported through `status`; a transient
        // failure keeps retrying inside its own spawned task, while a permanent one
        // is re-attempted only when the dataset's configuration changes — an
        // identically-configured dataset is filtered out of `datasets_to_apply`
        // above, so a later apply schedules no fresh load for it. Tracked in #13098.
        //
        // Built here and spawned below so a localpod dataset can be chained behind
        // the dataset it reads from, exactly as `load_datasets` does at startup:
        // `LocalPodConnector::read_provider` raises `InvalidTableName` when its
        // parent is not registered yet, and that is classified permanent, so a
        // child racing its parent would fail for good rather than retry.
        let mut added_futures: HashMap<
            ResolvedTableReference,
            Pin<Box<dyn Future<Output = ()> + Send>>,
        > = HashMap::new();
        // Keyed by parent so several localpod datasets reading from one newly added
        // dataset all chain behind the same load, rather than the first one
        // consuming it and the rest racing it.
        let mut localpod_by_parent: HashMap<
            ResolvedTableReference,
            Vec<(Arc<Dataset>, AcceleratorBootstrap)>,
        > = HashMap::new();

        for ds in &datasets_to_apply {
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

            if existing_datasets.iter().any(|d| d.name == ds.name)
                && !still_loading.contains(&ds.name)
            {
                // A `localpod` dataset whose parent this same diff adds — or queues, deeper in
                // a chain — cannot bind to it until that parent is registered, so it is
                // unloaded here and queued behind the parent's load below, exactly like a
                // newly added child.
                if let Some(parent) = localpod_parent(ds)
                    && (added_futures.contains_key(&parent)
                        || is_queued_localpod(&localpod_by_parent, &parent))
                {
                    // A plan or result cached over the table being unloaded must not
                    // answer once the chain below registers its replacement;
                    // `remove_dataset` discards both, after the deregistration. An
                    // accelerated dataset's initial refresh invalidates again on
                    // completion; a pass-through one has only this.
                    if !Arc::clone(&self)
                        .remove_dataset_with_bootstrap(
                            ds.name.clone(),
                            ds.acceleration.as_ref(),
                            CacheInvalidation::Reload,
                            &bootstrap_status,
                        )
                        .await
                    {
                        continue;
                    }
                    self.status
                        .update_dataset(&ds.name, status::ComponentStatus::Initializing);
                    localpod_by_parent
                        .entry(parent)
                        .or_default()
                        .push((Arc::clone(ds), bootstrap_status));
                    continue;
                }

                // The dataset now reads snapshots whose engine is not known yet — it moved
                // to `file_format: snapshot`, or to another snapshot location — so there is
                // no acceleration to swap in. Unload it, and load it again once its load
                // has read the snapshots' metadata.
                if ds.is_pending_snapshot_source() {
                    self.df.clear_cached_plans().await;
                    self.invalidate_cached_results_for(&ds.name).await;
                    let current_acceleration = existing_datasets
                        .iter()
                        .find(|current| current.name == ds.name)
                        .and_then(|current| current.acceleration.clone());
                    if !Arc::clone(&self)
                        .remove_dataset_with_bootstrap(
                            ds.name.clone(),
                            current_acceleration.as_ref(),
                            CacheInvalidation::Reload,
                            &bootstrap_status,
                        )
                        .await
                    {
                        continue;
                    }
                    self.status
                        .update_dataset(&ds.name, status::ComponentStatus::Initializing);
                    let runtime = Arc::clone(&self);
                    let ds_clone = Arc::clone(ds);
                    let load_semaphore = Arc::clone(&self.dataset_load_semaphore);
                    let load = self.dataset_loads.begin(&ds.name);
                    added_futures.insert(
                        resolve_table_reference(ds.name.clone()),
                        Box::pin(async move {
                            runtime
                                .load_dataset(ds_clone, bootstrap_status, load_semaphore, load)
                                .await;
                        }),
                    );
                    continue;
                }

                if !bootstrap_status.is_pending() {
                    Arc::clone(&self)
                        .update_dataset_with_bootstrap(Arc::clone(ds), bootstrap_status)
                        .await;
                    continue;
                }
                // Keep the serving table until the replacement snapshot has been
                // restored and its provider can be registered in place.
            }

            // A snapshot reader keeps serving its table until the replacement snapshot
            // has been restored, including when an earlier replacement was still
            // waiting for its own: the superseded wait never registered anything, so
            // the table is still the one that was serving before either change.
            let pending = bootstrap_status.is_pending();
            let serving = if still_loading.contains(&ds.name) && !pending {
                // A superseded attempt can be dropped after it registered the table
                // and before its load completed. Pending construction also owns
                // storage without a catalog entry.
                if !Arc::clone(&self)
                    .remove_dataset_with_bootstrap(
                        ds.name.clone(),
                        ds.acceleration.as_ref(),
                        CacheInvalidation::Reload,
                        &bootstrap_status,
                    )
                    .await
                {
                    continue;
                }
                false
            } else {
                // A snapshot reader keeps serving its table until the replacement
                // snapshot has been restored (see above).
                self.df.table_exists(&ds.name)
            };
            self.status.update_dataset(
                &ds.name,
                if serving {
                    status::ComponentStatus::Refreshing
                } else {
                    status::ComponentStatus::Initializing
                },
            );

            if let Some(parent) = localpod_parent(ds) {
                localpod_by_parent
                    .entry(parent)
                    .or_default()
                    .push((Arc::clone(ds), bootstrap_status));
                continue;
            }

            // The runtime's shared semaphore is what keeps these loads inside the
            // `runtime.dataset_load_parallelism` budget.
            let runtime = Arc::clone(&self);
            let ds_clone = Arc::clone(ds);
            let load_semaphore = Arc::clone(&self.dataset_load_semaphore);
            let load = self.dataset_loads.begin(&ds.name);
            let load_future: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
                runtime
                    .load_dataset(ds_clone, bootstrap_status, load_semaphore, load)
                    .await;
            });
            let load_future = if pending {
                self.track_snapshot_bootstrap(&ds.name, load_future, None)
                    .await
            } else {
                load_future
            };
            added_futures.insert(resolve_table_reference(ds.name.clone()), load_future);
        }

        // Every queued `localpod` dataset loads behind its parent: a load this diff spawns
        // (`added_futures`) or, deeper in a chain, another queued `localpod` dataset. A parent
        // that is unchanged, or that was updated in the loop above, is already registered, so
        // its children start at once. Chains are built from their roots, so a key that is
        // itself queued is reached through its own parent's chain rather than scheduled on
        // its own — ordering the apply set cannot sequence loads that are spawned.
        let mut parents: Vec<ResolvedTableReference> = localpod_by_parent.keys().cloned().collect();
        parents.sort_by_key(|parent| is_queued_localpod(&localpod_by_parent, parent));
        for parent in parents {
            let Some(children) = localpod_by_parent.remove(&parent) else {
                // Already part of a root's chain.
                continue;
            };
            let parent_future = added_futures.remove(&parent);
            let chains: Vec<_> = children
                .into_iter()
                .map(|(ds, bootstrap_status)| {
                    Arc::clone(&self).localpod_load_chain(
                        ds,
                        bootstrap_status,
                        &mut localpod_by_parent,
                        ReplacesRegistration::Yes,
                    )
                })
                .collect();
            tokio::spawn(async move {
                if let Some(parent_future) = parent_future {
                    parent_future.await;
                }
                join_all(chains).await;
            });
        }

        for load in added_futures.into_values() {
            tokio::spawn(load);
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
                // A dataset reading snapshots may still be resolving, and must not load
                // after it is removed; one added again reads its snapshots' metadata afresh.
                self.snapshot_sources().forget(&ds_name);
                Arc::clone(&self)
                    .remove_dataset(ds_name, ds_acceleration.as_ref(), CacheInvalidation::Unload)
                    .await;
            }
        }
    }

    /// The load of a queued `localpod` dataset, followed by the loads of the `localpod` datasets
    /// queued behind it, recursively, so a child never binds before its parent registers. Each
    /// dataset's children are taken out of `localpod_by_parent` as its chain is built, so a
    /// `localpod` cycle — which can never load — is built once and ends.
    fn localpod_load_chain(
        self: Arc<Self>,
        ds: Arc<Dataset>,
        bootstrap_status: AcceleratorBootstrap,
        localpod_by_parent: &mut HashMap<
            ResolvedTableReference,
            Vec<(Arc<Dataset>, AcceleratorBootstrap)>,
        >,
        replaces_registration: ReplacesRegistration,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let children: Vec<_> = localpod_by_parent
            .remove(&resolve_table_reference(ds.name.clone()))
            .unwrap_or_default()
            .into_iter()
            .map(|(child, child_status)| {
                Arc::clone(&self).localpod_load_chain(
                    child,
                    child_status,
                    localpod_by_parent,
                    replaces_registration,
                )
            })
            .collect();
        let load_semaphore = Arc::clone(&self.dataset_load_semaphore);
        // Registered now rather than once the parent has loaded, so a Spicepod
        // change can supersede the load while it waits.
        let load = self.dataset_loads.begin(&ds.name);
        Box::pin(async move {
            let name = ds.name.clone();
            Arc::clone(&self)
                .load_dataset(ds, bootstrap_status, load_semaphore, load)
                .await;
            // The registration this dataset reads through has just been replaced, so mark its
            // results-cache clock again, as `update_dataset` does after its swap: a result read
            // from the previous registration after the mark above must not be stored as fresh.
            // For a dataset new to this apply the mark is a no-op; at startup there is no previous
            // registration, so it is skipped.
            if replaces_registration == ReplacesRegistration::Yes {
                self.invalidate_cached_results_for(&name).await;
            }
            join_all(children).await;
        })
    }

    /// Initialize datasets configured with accelerators before registering the datasets.
    /// This ensures that the required resources for acceleration are available before registration,
    /// which is important for acceleration federation for some acceleration engines (e.g. `SQLite`).
    /// Returns a `HashMap` mapping each dataset name to its initialization result, which contains
    /// the `BootstrapStatus` on success or an error on failure.
    pub(super) async fn initialize_datasets_accelerators(
        &self,
        datasets: &[Arc<Dataset>],
    ) -> HashMap<TableReference, Result<AcceleratorBootstrap>> {
        let spaced_tracer = Arc::clone(&self.spaced_tracer);

        let init_futures = datasets.iter().map(|ds| {
            let ds = Arc::clone(ds);
            let spaced_tracer = Arc::clone(&spaced_tracer);
            let status = Arc::clone(&self.status);
            let accelerator_engine_registry = Arc::clone(&self.accelerator_engine_registry);
            let df = Arc::clone(&self.df);

            async move {
                // Before anything is initialized, dropped, or replaced: `init`
                // below is where a `mode: file_create` accelerator is dropped,
                // and the refusals this checks exist to stop exactly that
                // happening to a dataset that cannot afford it.
                if let Err(err) = preflight_dataset(&ds, &status, &spaced_tracer) {
                    return (ds.name.clone(), Err(err));
                }

                // Non-accelerated datasets or disabled acceleration are always successfully initialized
                if ds.acceleration.as_ref().is_none_or(|acc| !acc.enabled) {
                    return (ds.name.clone(), Ok(BootstrapStatus::None.into()));
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

                match df
                    .initialize_accelerator(Arc::clone(&ds), accelerator)
                    .await
                    .map_err(|error| match error {
                        crate::datafusion::Error::AcceleratorInitialization { source } => {
                            Error::AcceleratorInitializationFailed {
                                name: acceleration_settings.engine.to_string(),
                                source,
                            }
                        }
                        error => Error::UnableToCreateAcceleratedTable {
                            dataset: ds.name.clone(),
                            source: Box::new(error),
                        },
                    }) {
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
        let init_results: HashMap<TableReference, Result<AcceleratorBootstrap>> =
            results.into_iter().collect();

        init_results
    }

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
    accelerated_table: Option<crate::datafusion::PreparedAcceleratedTable>,
    bootstrap_status: AcceleratorBootstrap,
}

/// Wait for the accelerated table a hot reload just recreated to complete its
/// first refresh, so the in-place swap does not register a table that has not
/// loaded yet.
///
/// The wait is bounded because `apply_app` holds `apply_app_lock` across it, and
/// one shape never delivers a completion at all: a `refresh_mode: changes` stream
/// that never produces a ready envelope, since the completion is recorded only
/// when one is applied.
///
/// A refresh that finished before this call is not that shape. The waiter is
/// level-triggered and satisfied by a completion recorded before it was taken, and
/// `initial_load_completed` — stored before the completion is recorded — is read
/// both before the bound and after it, so a load that lands either side of the
/// wait resolves as success instead of discarding a table that is loaded.
///
/// On a cluster scheduler no refresh runs locally, so the table's completion
/// signal is closed when it is built and the waiter resolves at once rather than
/// spending the bound.
///
/// Returns `Ok(())` when the table loaded (or the runtime is shutting down).
///
/// When the table is still unloaded once there is nothing left to wait for,
/// drops the in-place swap in favour of a full reload:
/// - [`Error::HotReloadRefreshFailed`] for a terminal refresh failure (or the
///   new table being dropped before its first refresh), so an immediate load
///   failure is not reported as a timeout.
/// - [`Error::HotReloadRefreshTimedOut`] when the bound expires with no
///   completion.
///
/// Waiting out the rest of the bound on a table nobody can refresh only delays
/// the same verdict.
async fn await_hot_reload_initial_refresh(
    dataset_name: &TableReference,
    initial_load_completed: &(dyn Fn() -> bool + Sync),
    completion: RefreshCompletionWaiter,
    shutdown_token: &tokio_util::sync::CancellationToken,
    timeout: Duration,
) -> Result<()> {
    if initial_load_completed() {
        return Ok(());
    }

    let mut wait_outcome = None;
    tokio::select! {
        // A `RefreshCompletionWaiter` for any completion is satisfied by a
        // *successful* refresh that finished before this wait began, so the
        // load cannot be missed by arriving here late. An abandoned wait or a
        // terminal failure falls through to the flag re-check below rather
        // than returning: no successful load is coming, but a load that
        // landed before the recorders went still counts.
        outcome = completion.wait() => {
            if outcome.is_answered() {
                return Ok(());
            }
            wait_outcome = Some(outcome);
        },
        () = shutdown_token.cancelled() => return Ok(()),
        () = tokio::time::sleep(timeout) => {}
    }

    // The bound is a backstop, not the verdict: the flag is stored before the
    // completion is recorded, so a load that finished as the bound expired
    // leaves a table that must not be discarded.
    if initial_load_completed() {
        return Ok(());
    }

    // A terminal failure (or abandoned wait) ended immediately — do not claim
    // the bound expired.
    if matches!(
        wait_outcome,
        Some(RefreshCompletionOutcome::TerminalFailure | RefreshCompletionOutcome::Abandoned)
    ) {
        return HotReloadRefreshFailedSnafu {
            dataset: dataset_name.clone(),
        }
        .fail();
    }

    HotReloadRefreshTimedOutSnafu {
        dataset: dataset_name.clone(),
        timeout_secs: timeout.as_secs(),
    }
    .fail()
}

/// What a Cayenne dataset with a primary key keeps of a key it sees again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyRule<'a> {
    /// The newest version by this `time_column`, the later arrival on a tie.
    NewestByTime(&'a str),
    /// The version that arrived last.
    LastArrival,
    /// Each source change, applied in order.
    ChangeOrder,
}

/// The rule a Cayenne dataset with a declared primary key applies to a repeated
/// key; `None` for another engine or a dataset without one.
fn cayenne_key_rule<'a>(
    ds: &'a Dataset,
    acceleration: &Acceleration,
    refresh_mode: RefreshMode,
) -> Option<KeyRule<'a>> {
    let key = acceleration
        .primary_key
        .as_ref()
        .filter(|_| acceleration.engine == Engine::Cayenne)?;
    Some(key_rule(
        acceleration,
        key,
        ds.time_column.as_deref(),
        refresh_mode,
    ))
}

/// The rule `acceleration`, keyed on `key`, applies to a repeated key; see
/// `Acceleration::orders_versions_by_time`.
fn key_rule<'a>(
    acceleration: &Acceleration,
    key: &datafusion_table_providers::util::column_reference::ColumnReference,
    time_column: Option<&'a str>,
    refresh_mode: RefreshMode,
) -> KeyRule<'a> {
    if refresh_mode == RefreshMode::Changes {
        return KeyRule::ChangeOrder;
    }
    match time_column {
        Some(time_column)
            if acceleration.orders_versions_by_time(Some(time_column), refresh_mode)
                && !key.iter().any(|column| column == time_column) =>
        {
            KeyRule::NewestByTime(time_column)
        }
        _ => KeyRule::LastArrival,
    }
}

/// The line a Cayenne dataset with a primary key logs at load, stating the rule it
/// keeps one row per key by (#14576).
fn key_rule_line(
    dataset_name: &str,
    key: &datafusion_table_providers::util::column_reference::ColumnReference,
    rule: KeyRule<'_>,
) -> String {
    match rule {
        KeyRule::NewestByTime(time_column) => format!(
            "Dataset '{dataset_name}' keeps one row per '{key}': the newest by '{time_column}', or the version that arrived last when times are equal."
        ),
        KeyRule::LastArrival => format!(
            "Dataset '{dataset_name}' keeps one row per '{key}': the version that arrived last, which can differ between refreshes; set `time_column` for a reproducible result."
        ),
        KeyRule::ChangeOrder => format!(
            "Dataset '{dataset_name}' keeps one row per '{key}', applying each source change in order."
        ),
    }
}

/// The `on_conflict` upsert value `options` stand for, as a Spicepod spells it.
fn upsert_name(
    options: &datafusion_table_providers::util::constraints::UpsertOptions,
) -> &'static str {
    if options.last_write_wins {
        "upsert_dedup_by_row_id"
    } else if options.remove_duplicates {
        "upsert_dedup"
    } else {
        "upsert"
    }
}

/// The warning for a Cayenne dataset that sets `on_conflict`, which it no longer
/// reads, naming any change in which version of a key it keeps; `rule` is `None`
/// when the dataset declares no primary key.
fn cayenne_on_conflict_warning(
    dataset_name: &str,
    acceleration: &Acceleration,
    rule: Option<KeyRule<'_>>,
) -> String {
    use crate::component::dataset::acceleration::OnConflictBehavior;
    let kept = match rule {
        Some(KeyRule::NewestByTime(time_column)) => format!("the newest by '{time_column}'"),
        _ => "the last to arrive".to_string(),
    };
    let change = match (acceleration.on_conflict.values().next(), rule) {
        (Some(OnConflictBehavior::Drop), Some(KeyRule::NewestByTime(_) | KeyRule::LastArrival)) => {
            format!("; `drop` kept the first version of a key, and now {kept} is kept.")
        }
        (Some(OnConflictBehavior::Upsert(options)), Some(KeyRule::NewestByTime(_))) => format!(
            "; `{}` did not order a key's versions by time, and now {kept} is kept.",
            upsert_name(options)
        ),
        _ => ".".to_string(),
    };
    format!(
        "Dataset '{dataset_name}' sets `acceleration.on_conflict`, which Cayenne no longer uses{change} Remove `on_conflict`. See: https://spiceai.org/docs/features/data-acceleration/constraints"
    )
}

/// The warning for a dataset on another accelerator that sets `on_conflict`.
fn deprecated_on_conflict_warning(
    dataset_name: &str,
    acceleration: &Acceleration,
    refresh_mode: RefreshMode,
) -> String {
    let persistence = if matches!(acceleration.mode, Mode::Memory | Mode::FileCreate)
        && refresh_mode == RefreshMode::Changes
    {
        " Set `mode: file` to preserve CDC data across restarts and resume replication."
    } else {
        ""
    };
    format!(
        "Dataset '{dataset_name}' sets `acceleration.on_conflict`, which is deprecated and removed in 3.0. Use `engine: cayenne` to keep one row per primary key without it.{persistence}"
    )
}

/// Warning for an append that keeps the newest version of each key by `time_column`
/// but has no `refresh_append_overlap`, so it never re-reads a late row.
fn newest_by_time_without_overlap_warning(dataset_name: &str, time_column: &str) -> String {
    format!(
        "Dataset '{dataset_name}' keeps the newest version of each key by '{time_column}', but without `refresh_append_overlap` an append never re-reads late rows, so a late update is not loaded. Set `refresh_append_overlap` to how late rows can arrive. See: https://spiceai.org/docs/features/data-acceleration/constraints"
    )
}

/// Returns `true` when a dataset load failure cannot be cleared by retrying it.
///
/// `load_dataset` retries with unbounded backoff and only short-circuits on
/// [`Error::PermanentDatasetFailure`], so a failure that is a pure function of
/// the Spicepod configuration would otherwise be retried for the life of the
/// process — rebuilding the table provider, and re-running its side effects,
/// on every attempt. Reading the source already classifies its failures this
/// way through `DataConnectorError::is_retriable`; this covers the
/// configuration errors raised on the rest of the load path.
///
/// Everything else stays retriable, so a source that is merely unreachable or
/// an accelerator that is momentarily unavailable still recovers on its own.
pub(crate) fn is_permanent_dataset_failure(err: &Error) -> bool {
    match err {
        // The Spicepod names a connector this build cannot provide.
        Error::UnknownDataConnector { .. }
        | Error::OdbcNotInstalled
        | Error::DataConnectorNotInBuild { .. }
        // Dataset-level settings that contradict each other.
        | Error::FullTextSearchRequiresAcceleration { .. }
        | Error::AcceleratedWriteBackWithoutReplication { .. }
        | Error::AccelerationWriteModeWithChanges { .. }
        // Durable write-back configurations that would acknowledge a write the
        // dataset cannot then deliver.
        | Error::DurableWriteBackWithRetention { .. }
        | Error::DurableWriteBackRecreatingMode { .. }
        | Error::DurableWriteBackCompositePrimaryKey { .. }
        | Error::DurableWriteBackUndeclaredPrimaryKey { .. }
        | Error::DurableWriteBackPrerequisitesUnmet { .. }
        | Error::DurableWriteBackUnsupportedBySource { .. } => true,
        // Connector creation boxes its error, so recover the type the way the
        // catalog load path does before asking it to classify itself.
        Error::UnableToInitializeDataConnector { source } => {
            is_permanent_dataset_source(source.as_ref())
        }
        // Registration carries the accelerated-table configuration errors.
        Error::UnableToAttachDataConnector { source, .. } => !source.is_retriable(),
        _ => false,
    }
}

/// Returns `true` when a boxed connector-construction error is a configuration
/// error that no retry can clear.
///
/// Construction has two failure sources that box into the same variant, and
/// only one of them is a [`dataconnector::DataConnectorError`]. Parameter
/// validation runs *before* the connector is created — `ConnectorParamsBuilder`
/// rejects an out-of-vocabulary `one_of` value or a missing required parameter
/// — so it raises [`runtime_parameters::Error`] instead. Classifying on the
/// `DataConnectorError` downcast alone therefore reads a plain Spicepod typo as
/// transient and retries it for the life of the process. See #12416.
///
/// The `runtime_parameters` variant is matched by name rather than accepting
/// any error of that type, so a future retriable variant does not silently
/// inherit "permanent" from this arm.
fn is_permanent_dataset_source(source: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    if let Some(err) = source.downcast_ref::<dataconnector::DataConnectorError>() {
        return !err.is_retriable();
    }
    matches!(
        source.downcast_ref::<runtime_parameters::Error>(),
        Some(runtime_parameters::Error::InvalidConfigurationNoSource { .. })
    )
}

/// Every retention setting a dataset can carry, for the durable write-back gate.
///
/// Deliberately wider than what would actually prune: the retention worker needs
/// more than any one of these (see `RetentionBuilder`), but a dataset that asks
/// for retention at all is refused rather than one that happened to ask
/// completely enough for the worker to start. `retention_check_interval` counts
/// too — on its own it prunes nothing, but it is a retention setting on a
/// dataset that must not have one, and silently ignoring it would leave the user
/// believing retention is configured.
fn configured_retention_setting(acceleration: &Acceleration) -> Option<String> {
    if acceleration.retention_period.is_some() {
        Some("acceleration.retention_period".to_string())
    } else if acceleration.retention_sql.is_some() {
        Some("acceleration.retention_sql".to_string())
    } else if acceleration.retention_check_interval.is_some() {
        Some("acceleration.retention_check_interval".to_string())
    } else if acceleration.retention_check_enabled {
        Some("acceleration.retention_check_enabled".to_string())
    } else {
        None
    }
}

/// Refuse a dataset whose configuration cannot be made to work, reporting the
/// refusal once.
///
/// Every lifecycle entry calls this BEFORE it initializes, drops, replaces, or
/// mutates accelerator state, because some of these refusals exist precisely to
/// stop that state being destroyed: a durable-write-back dataset on
/// `mode: file_create` has its accelerator — and the markers recording what the
/// source still owes — dropped by `DataAccelerator::init`, so a refusal that
/// arrives afterwards reports a loss it was supposed to prevent.
///
/// The decision itself is [`validate_dataset`], which touches nothing.
fn preflight_dataset(
    ds: &Arc<Dataset>,
    status: &status::RuntimeStatus,
    spaced_tracer: &Arc<util::tracers::SpacedTracer>,
) -> Result<()> {
    let Err(err) = validate_dataset(ds) else {
        return Ok(());
    };
    // Everything `validate_dataset` raises is a pure function of the Spicepod, so
    // retrying cannot change the answer.
    Err(refuse_permanently(ds, status, spaced_tracer, &err))
}

/// The key that rate-limits `dataset`'s load-failure lines on the runtime's
/// shared `SpacedTracer`.
///
/// It is per dataset, so one dataset's failure cannot hide another's. It also
/// differs from the dataset-name key of the connector and accelerator failure
/// lines, so a load failure does not hide those lines for the same dataset.
fn load_failure_log_key(dataset: &TableReference) -> String {
    format!("load failure: {dataset}")
}

/// Report a refusal and return it as permanent.
///
/// A refusal a retry cannot change must not go back through `load_dataset`'s retry
/// loop: that rebuilds the connector on every attempt for the life of the process
/// and leaves the dataset stuck in `Initializing`, with only a periodic log line to
/// show for it.
///
/// The log is keyed on the dataset rather than one shared slot, because a caller
/// runs this once per dataset inside `initialize_datasets_accelerators`' fan-out —
/// a shared key would let the first refusal of a startup suppress every other
/// dataset's for the tracer's whole interval, leaving them refused with no line
/// naming them.
fn refuse_permanently(
    ds: &Arc<Dataset>,
    status: &status::RuntimeStatus,
    spaced_tracer: &Arc<util::tracers::SpacedTracer>,
    err: &Error,
) -> Error {
    let ds_name = &ds.name;
    status.update_dataset(
        ds_name,
        status::ComponentStatus::error_with_message(err.to_string()),
    );
    metrics::datasets::LOAD_ERROR.add(1, &[]);
    error_spaced!(
        spaced_tracer,
        "Refusing to load dataset {}. {err}",
        ds_name.table()
    );

    PermanentDatasetFailureSnafu {
        dataset: ds_name.clone(),
        reason: err.to_string(),
    }
    .build()
}

fn validate_dataset(ds: &Arc<Dataset>) -> Result<()> {
    if ds.has_full_text_column() && !ds.is_accelerated() {
        return Err(FullTextSearchRequiresAccelerationSnafu {
            dataset_name: ds.name.to_string(),
        }
        .build());
    }

    // Write-back settings on a disabled acceleration are inert: nothing is
    // accelerated, so no write is acknowledged for delivery.
    let Some(acceleration) = ds.acceleration.as_ref().filter(|a| a.enabled) else {
        return Ok(());
    };

    // `write_mode: write_back` selects the write-back path on its own, but only a
    // configuration that resolves to DURABLE write-back records markers and runs a
    // delivery worker. One that asks for write-back without them has no path to the
    // source: it would load and then refuse every write.
    if let Some(missing) = acceleration.unmet_durable_write_back_prerequisites() {
        return Err(DurableWriteBackPrerequisitesUnmetSnafu {
            dataset_name: ds.name.to_string(),
            connector: ds.source().to_string(),
            missing: missing.join(", "),
        }
        .build());
    }

    // Durable write-back holds each committed row in the accelerator until it
    // reaches the source, so it cannot tolerate a configuration that removes a row
    // or discards the accelerator: the marker recording what the source still owes
    // goes with it, and no later pass can deliver a value nothing holds.
    if acceleration.resolves_to_durable_write_back() {
        let dataset_name = ds.name.to_string();
        let connector = ds.source().to_string();

        // Retention deletes accelerator rows on a schedule and marks nothing, so
        // it can prune a row that was acknowledged to the writer and not yet
        // delivered.
        if let Some(retention_setting) = configured_retention_setting(acceleration) {
            return Err(DurableWriteBackWithRetentionSnafu {
                dataset_name,
                connector,
                retention_setting,
            }
            .build());
        }

        // The same loss in bulk. `mode: file` is the only mode that keeps the
        // accelerator across a restart without recreating it: `memory` holds
        // nothing across one, `file_create` recreates on every load, and
        // `file_update` recreates whenever the source schema changes incompatibly
        // — durable write-back always refreshes, so that recreate is live. A
        // recreate drops the table, and `drop_table` is the one place
        // `cayenne_pending_write_back` rows are deleted.
        if acceleration.mode != Mode::File {
            return Err(DurableWriteBackRecreatingModeSnafu {
                dataset_name,
                connector,
                mode: acceleration.mode.to_string(),
            }
            .build());
        }

        // The delivery worker keys each committed row on a SINGLE primary-key
        // column: it builds a `pk IN (...)` filter, which a composite key has no
        // shape for and an absent one has nothing to fill. Either way the dataset
        // would accept writes, mark them, and never deliver one — so both are
        // refused here, over the key the Spicepod DECLARES. Schema inference can
        // supply or widen a key, but it runs after this point, so a key it
        // produced could only be judged once the dataset had already been
        // accepted; requiring the declaration is what makes the answer knowable
        // before anything is acknowledged.
        match acceleration.durable_write_back_primary_key() {
            DurableWriteBackKey::Single(_) => {}
            DurableWriteBackKey::Undeclared => {
                return Err(DurableWriteBackUndeclaredPrimaryKeySnafu {
                    dataset_name,
                    connector,
                }
                .build());
            }
            DurableWriteBackKey::Composite(columns) => {
                return Err(DurableWriteBackCompositePrimaryKeySnafu {
                    dataset_name,
                    connector,
                    primary_key: columns.join(", "),
                    pk_columns: columns.len(),
                }
                .build());
            }
        }
    }

    Ok(())
}

/// The error for a `from:` naming a connector this build does not register: the closest
/// registered name plus the full list, so the message names a fix.
///
/// ODBC and `ScyllaDB` are the exceptions. They are real connectors that this build may simply
/// not have been compiled with, so they get the build instruction for their feature instead of
/// a "did you mean" over the connectors that happen to be present.
async fn unknown_data_connector(source: &str) -> Error {
    if source == ODBC_DATACONNECTOR {
        return OdbcNotInstalledSnafu.build();
    }

    if source == SCYLLADB_DATACONNECTOR {
        return DataConnectorNotInBuildSnafu {
            data_connector: source,
            feature: SCYLLADB_FEATURE,
        }
        .build();
    }

    UnknownDataConnectorSnafu {
        data_connector: source,
        suggestion: dataconnector::suggest_connector(source).await,
        available: dataconnector::registered_connector_names().await,
    }
    .build()
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

    match crate::dataaccelerator::spice_sys::update_caching_engine_fetched_at(dataset).await {
        Ok(()) => {
            tracing::info!(
                "Updated _fetched_at for all records in cached dataset {}",
                dataset.name
            );
        }
        Err(e) if is_shutdown_cancellation(&e) => {
            tracing::debug!(
                "Did not update _fetched_at for cached dataset {}: the runtime is shutting down ({e})",
                dataset.name
            );
        }
        Err(e) => {
            tracing::warn!(
                "Failed to update _fetched_at for cached dataset {}: {e}",
                dataset.name
            );
        }
    }
}

/// The message logged when an accelerated dataset with data on disk waits for its
/// source before serving, because of `reason` (see
/// `Runtime::waits_for_source_reason`).
fn waits_for_source_message(dataset: &TableReference, reason: &str) -> String {
    format!(
        "Dataset '{dataset}' waits for its source before serving because it uses {reason}. See: https://spiceai.org/docs/features/data-acceleration/data-refresh"
    )
}

/// The status message of a dataset that failed to load: for a file-accelerated
/// dataset that waits for its source, it says the dataset is not served and why.
fn load_failure_status(ds: &Dataset, cause: &str) -> String {
    match Runtime::not_served_reason(ds) {
        Some(reason) => not_served_status(&reason, cause),
        None => cause.to_string(),
    }
}

fn not_served_status(reason: &str, cause: &str) -> String {
    format!("Not served: waits for its source because it uses {reason}. Cause: {cause}")
}

/// Whether a dataset's `drasi:` block is live.
fn is_drasi_forwarding(drasi: &spicepod::drasi::Drasi) -> bool {
    drasi.forwarding == spicepod::drasi::DrasiForwarding::Enabled
}

/// A dataset's pending load, as startup queues it before spawning.
type PendingDatasetLoad = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Whether a `localpod` load chain runs over registrations a spicepod apply is replacing, whose
/// cached results must be invalidated once each dataset reloads, or at startup, where nothing was
/// registered before.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplacesRegistration {
    Yes,
    No,
}

/// Whether `ds`, or a `localpod` dataset queued anywhere below it, is still waiting for a
/// snapshot's first publication, so its chain must not hold up component startup.
fn localpod_chain_pending(
    ds: &Dataset,
    bootstrap_status: &AcceleratorBootstrap,
    localpod_by_parent: &HashMap<ResolvedTableReference, Vec<(Arc<Dataset>, AcceleratorBootstrap)>>,
) -> bool {
    bootstrap_status.is_pending()
        || localpod_by_parent
            .get(&resolve_table_reference(ds.name.clone()))
            .is_some_and(|children| {
                children.iter().any(|(child, child_status)| {
                    localpod_chain_pending(child, child_status, localpod_by_parent)
                })
            })
}

/// The dataset a `localpod` dataset reads through, resolved as the query engine resolves it (so
/// `parent`, `public.parent`, and `spice.public.parent` name one dataset), or `None` for any
/// other connector.
fn localpod_parent(ds: &Dataset) -> Option<ResolvedTableReference> {
    (ds.source() == LOCALPOD_DATACONNECTOR)
        .then(|| resolve_table_reference(TableReference::parse_str(ds.path())))
}

/// Whether `name` is a `localpod` dataset queued in `localpod_by_parent`. A queued dataset
/// registers nothing until its chain runs, so a `localpod` dataset reading through it must queue
/// behind it too.
fn is_queued_localpod(
    localpod_by_parent: &HashMap<ResolvedTableReference, Vec<(Arc<Dataset>, AcceleratorBootstrap)>>,
    name: &ResolvedTableReference,
) -> bool {
    localpod_by_parent
        .values()
        .flatten()
        .any(|(ds, _)| resolve_table_reference(ds.name.clone()) == *name)
}

/// Extends the datasets a spicepod apply reloads with every `localpod` dataset that reads
/// through one of them, transitively, and orders the result so each `localpod` dataset follows
/// its parent.
///
/// A `localpod` dataset binds to the table its parent has registered at the moment it loads:
/// it reads through that provider and hands its refreshes to that table's refresh task. A parent
/// reloaded onto a new table therefore leaves an unchanged child on the retired one, answering
/// rows the parent no longer has and keeping the retired refresh task alive (#3288). Reloading
/// the child after its parent binds it to the parent's new table.
fn with_localpod_dependents(
    mut reloading: Vec<Arc<Dataset>>,
    all: &[Arc<Dataset>],
) -> Vec<Arc<Dataset>> {
    let mut reloading_names: HashSet<ResolvedTableReference> = reloading
        .iter()
        .map(|ds| resolve_table_reference(ds.name.clone()))
        .collect();

    // A child of a reloading child reloads too, so iterate to a fixpoint. Each pass adds at
    // least one dataset or stops, so it runs at most `all.len()` times.
    let mut added = true;
    while added {
        added = false;
        for ds in all {
            if !reloading_names.contains(&resolve_table_reference(ds.name.clone()))
                && localpod_parent(ds).is_some_and(|parent| reloading_names.contains(&parent))
            {
                reloading_names.insert(resolve_table_reference(ds.name.clone()));
                reloading.push(Arc::clone(ds));
                added = true;
            }
        }
    }

    // Parents first: a child binds to whatever its parent has registered when it reloads. The
    // walk up is bounded so a `localpod` cycle, which can never load, cannot spin here.
    let by_name: HashMap<ResolvedTableReference, &Arc<Dataset>> = all
        .iter()
        .map(|ds| (resolve_table_reference(ds.name.clone()), ds))
        .collect();
    let depth = |ds: &Arc<Dataset>| {
        let mut depth = 0;
        let mut current = ds;
        while let Some(parent) = localpod_parent(current)
            && reloading_names.contains(&parent)
            && depth < all.len()
            && let Some(parent_dataset) = by_name.get(&parent)
        {
            depth += 1;
            current = parent_dataset;
        }
        depth
    };
    reloading.sort_by_cached_key(depth);
    reloading
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_waits_for_source_message_names_the_dataset_and_its_configuration() {
        assert_eq!(
            super::waits_for_source_message(
                &datafusion::common::TableReference::bare("orders"),
                "`refresh_mode: changes`"
            ),
            "Dataset 'orders' waits for its source before serving because it uses `refresh_mode: changes`. See: https://spiceai.org/docs/features/data-acceleration/data-refresh"
        );
    }

    #[test]
    fn the_not_served_status_says_why_and_keeps_the_cause() {
        assert_eq!(
            super::not_served_status("`refresh_mode: changes`", "connection refused"),
            "Not served: waits for its source because it uses `refresh_mode: changes`. Cause: connection refused"
        );
    }

    use super::*;

    mod key_rule {
        use super::*;
        use datafusion_table_providers::util::column_reference::ColumnReference;

        fn acceleration(engine: &str, on_conflict: Option<&str>) -> Acceleration {
            let mut spicepod = spicepod::acceleration::Acceleration {
                engine: Some(engine.to_string()),
                primary_key: Some("id".to_string()),
                ..Default::default()
            };
            if let Some(value) = on_conflict {
                spicepod.on_conflict.insert(
                    "id".to_string(),
                    serde_json::from_value(serde_json::Value::String(value.to_string()))
                        .expect("an on_conflict value"),
                );
            }
            Acceleration::try_from(spicepod).expect("valid acceleration")
        }

        fn id() -> ColumnReference {
            ColumnReference::new(vec!["id".to_string()])
        }

        #[test]
        fn the_rule_follows_the_time_column_and_refresh_mode() {
            let cayenne = acceleration("cayenne", None);
            for mode in [RefreshMode::Full, RefreshMode::Append] {
                assert_eq!(
                    key_rule(&cayenne, &id(), Some("updated_at"), mode),
                    KeyRule::NewestByTime("updated_at")
                );
                assert_eq!(key_rule(&cayenne, &id(), None, mode), KeyRule::LastArrival);
            }
            assert_eq!(
                key_rule(&cayenne, &id(), Some("updated_at"), RefreshMode::Changes),
                KeyRule::ChangeOrder
            );
            // A key that holds the time column gives every version a key of its own.
            let keyed_on_time = ColumnReference::new(vec!["id".to_string(), "at".to_string()]);
            assert_eq!(
                key_rule(&cayenne, &keyed_on_time, Some("at"), RefreshMode::Full),
                KeyRule::LastArrival
            );
        }

        #[test]
        fn the_load_line_states_the_rule() {
            assert_eq!(
                key_rule_line("events", &id(), KeyRule::NewestByTime("updated_at")),
                "Dataset 'events' keeps one row per 'id': the newest by 'updated_at', or the version that arrived last when times are equal."
            );
            assert_eq!(
                key_rule_line("events", &id(), KeyRule::LastArrival),
                "Dataset 'events' keeps one row per 'id': the version that arrived last, which can differ between refreshes; set `time_column` for a reproducible result."
            );
            assert_eq!(
                key_rule_line("orders", &id(), KeyRule::ChangeOrder),
                "Dataset 'orders' keeps one row per 'id', applying each source change in order."
            );
            let composite = ColumnReference::new(vec!["region".to_string(), "id".to_string()]);
            assert_eq!(
                key_rule_line("orders", &composite, KeyRule::LastArrival),
                "Dataset 'orders' keeps one row per '(id, region)': the version that arrived last, which can differ between refreshes; set `time_column` for a reproducible result."
            );
        }

        #[test]
        fn the_cayenne_warning_names_what_changed() {
            assert_eq!(
                cayenne_on_conflict_warning(
                    "events",
                    &acceleration("cayenne", Some("drop")),
                    Some(KeyRule::NewestByTime("updated_at")),
                ),
                "Dataset 'events' sets `acceleration.on_conflict`, which Cayenne no longer uses; `drop` kept the first version of a key, and now the newest by 'updated_at' is kept. Remove `on_conflict`. See: https://spiceai.org/docs/features/data-acceleration/constraints"
            );
            assert_eq!(
                cayenne_on_conflict_warning(
                    "events",
                    &acceleration("cayenne", Some("drop")),
                    Some(KeyRule::LastArrival),
                ),
                "Dataset 'events' sets `acceleration.on_conflict`, which Cayenne no longer uses; `drop` kept the first version of a key, and now the last to arrive is kept. Remove `on_conflict`. See: https://spiceai.org/docs/features/data-acceleration/constraints"
            );
            assert_eq!(
                cayenne_on_conflict_warning(
                    "events",
                    &acceleration("cayenne", Some("upsert")),
                    Some(KeyRule::NewestByTime("updated_at")),
                ),
                "Dataset 'events' sets `acceleration.on_conflict`, which Cayenne no longer uses; `upsert` did not order a key's versions by time, and now the newest by 'updated_at' is kept. Remove `on_conflict`. See: https://spiceai.org/docs/features/data-acceleration/constraints"
            );
            assert_eq!(
                cayenne_on_conflict_warning(
                    "events",
                    &acceleration("cayenne", Some("upsert")),
                    Some(KeyRule::LastArrival),
                ),
                "Dataset 'events' sets `acceleration.on_conflict`, which Cayenne no longer uses. Remove `on_conflict`. See: https://spiceai.org/docs/features/data-acceleration/constraints"
            );
        }

        #[test]
        fn other_engines_are_told_on_conflict_is_removed_in_3_0() {
            for engine in ["arrow", "duckdb"] {
                let mut acceleration = acceleration(engine, Some("upsert"));
                for (mode, refresh_mode) in [
                    (Mode::File, None),
                    (Mode::File, Some(RefreshMode::Changes)),
                    (Mode::FileUpdate, None),
                    (Mode::FileUpdate, Some(RefreshMode::Changes)),
                    (Mode::Memory, None),
                    (Mode::Memory, Some(RefreshMode::Full)),
                    (Mode::Memory, Some(RefreshMode::Append)),
                ] {
                    acceleration.mode = mode;
                    acceleration.refresh_mode = refresh_mode;
                    assert_eq!(
                        deprecated_on_conflict_warning(
                            "orders",
                            &acceleration,
                            refresh_mode.unwrap_or(RefreshMode::Full),
                        ),
                        "Dataset 'orders' sets `acceleration.on_conflict`, which is deprecated and removed in 3.0. Use `engine: cayenne` to keep one row per primary key without it."
                    );
                }
            }
        }

        #[test]
        fn changes_recommend_persistent_cayenne() {
            let mut acceleration = acceleration("duckdb", Some("upsert"));
            assert_eq!(acceleration.mode, Mode::Memory);
            for (mode, refresh_mode) in [
                (Mode::Memory, None),
                (Mode::Memory, Some(RefreshMode::Changes)),
                (Mode::FileCreate, None),
                (Mode::FileCreate, Some(RefreshMode::Changes)),
            ] {
                acceleration.mode = mode;
                acceleration.refresh_mode = refresh_mode;
                assert_eq!(
                    deprecated_on_conflict_warning("orders", &acceleration, RefreshMode::Changes),
                    "Dataset 'orders' sets `acceleration.on_conflict`, which is deprecated and removed in 3.0. Use `engine: cayenne` to keep one row per primary key without it. Set `mode: file` to preserve CDC data across restarts and resume replication."
                );
            }
        }
    }

    #[test]
    fn the_no_overlap_warning_explains_what_is_never_fetched() {
        assert_eq!(
            newest_by_time_without_overlap_warning("events", "updated_at"),
            "Dataset 'events' keeps the newest version of each key by 'updated_at', but without `refresh_append_overlap` an append never re-reads late rows, so a late update is not loaded. Set `refresh_append_overlap` to how late rows can arrive. See: https://spiceai.org/docs/features/data-acceleration/constraints"
        );
    }

    /// Every retention setting has to be recognised, whichever one the dataset
    /// carries: a prune can remove a row that was acknowledged to the writer and
    /// not yet delivered, and nothing else holds that value.
    #[test]
    fn every_retention_setting_is_recognised_as_configured() {
        assert_eq!(configured_retention_setting(&Acceleration::default()), None);

        for (acceleration, expected) in [
            (
                Acceleration {
                    retention_period: Some("1d".to_string()),
                    ..Acceleration::default()
                },
                "acceleration.retention_period",
            ),
            (
                Acceleration {
                    retention_sql: Some("where ts < now()".to_string()),
                    ..Acceleration::default()
                },
                "acceleration.retention_sql",
            ),
            (
                Acceleration {
                    retention_check_interval: Some("1h".to_string()),
                    ..Acceleration::default()
                },
                "acceleration.retention_check_interval",
            ),
            (
                Acceleration {
                    retention_check_enabled: true,
                    ..Acceleration::default()
                },
                "acceleration.retention_check_enabled",
            ),
        ] {
            assert_eq!(
                configured_retention_setting(&acceleration).as_deref(),
                Some(expected),
                "a dataset carrying only {expected} still asks for retention"
            );
        }
    }

    /// A spicepod acceleration that `resolves_to_durable_write_back` accepts, on
    /// the one mode that can hold an undelivered write.
    fn durable_write_back_acceleration() -> spicepod::acceleration::Acceleration {
        spicepod::acceleration::Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: spicepod::acceleration::Mode::File,
            write_mode: spicepod::acceleration::WriteMode::WriteBack,
            refresh_mode: Some(spicepod::acceleration::RefreshMode::Changes),
            on_conflict: HashMap::from([(
                "id".to_string(),
                spicepod::acceleration::OnConflictBehavior::Upsert,
            )]),
            // Declared, single-column: durable write-back has nothing to key a
            // delivery on otherwise, so this is part of the valid shape.
            primary_key: Some("id".to_string()),
            ..Default::default()
        }
    }

    fn dataset_with_acceleration(
        runtime: &Arc<crate::Runtime>,
        acceleration: spicepod::acceleration::Acceleration,
    ) -> Arc<Dataset> {
        let mut spec = spicepod::component::dataset::Dataset::new("postgres:orders", "orders");
        spec.acceleration = Some(acceleration);
        let app = app::AppBuilder::new("validate_dataset")
            .with_dataset(spec.clone())
            .build();
        Arc::new(
            DatasetBuilder::try_from(spec)
                .expect("valid dataset builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::clone(runtime))
                .build()
                .expect("valid runtime dataset"),
        )
    }

    /// Every configuration `validate_dataset` refuses, asserted at the decision
    /// itself rather than at a predicate below it — including the composite-key
    /// branch, whose `> 1` bound has no other cover.
    #[tokio::test]
    async fn validate_dataset_refuses_every_durable_write_back_configuration_that_cannot_deliver() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        assert!(
            validate_dataset(&dataset_with_acceleration(
                &runtime,
                durable_write_back_acceleration()
            ))
            .is_ok(),
            "the supported configuration must load"
        );

        let retention = spicepod::acceleration::Acceleration {
            retention_period: Some("1d".to_string()),
            ..durable_write_back_acceleration()
        };
        assert!(
            matches!(
                validate_dataset(&dataset_with_acceleration(&runtime, retention)),
                Err(Error::DurableWriteBackWithRetention { .. })
            ),
            "retention can prune a row that was acknowledged and not yet delivered"
        );

        for mode in [
            spicepod::acceleration::Mode::Memory,
            spicepod::acceleration::Mode::FileCreate,
            spicepod::acceleration::Mode::FileUpdate,
        ] {
            let named = mode.to_string();
            let acceleration = spicepod::acceleration::Acceleration {
                mode,
                ..durable_write_back_acceleration()
            };
            assert!(
                matches!(
                    validate_dataset(&dataset_with_acceleration(&runtime, acceleration)),
                    Err(Error::DurableWriteBackRecreatingMode { .. })
                ),
                "{named} cannot hold an acknowledged write until it is delivered"
            );
        }

        // Parenthesised: that is the compound-key syntax `ColumnReference` parses.
        // Without them this is one column whose name contains a comma.
        let composite = spicepod::acceleration::Acceleration {
            primary_key: Some("(id, region)".to_string()),
            ..durable_write_back_acceleration()
        };
        assert!(
            matches!(
                validate_dataset(&dataset_with_acceleration(&runtime, composite)),
                Err(Error::DurableWriteBackCompositePrimaryKey { .. })
            ),
            "a composite key cannot be delivered, and would accumulate markers silently"
        );

        // Unset is refused for the same reason, and refused HERE rather than left
        // to inference: inference runs after the dataset is accepted, so a key it
        // supplied could only be judged once writes could already be acknowledged.
        let undeclared = spicepod::acceleration::Acceleration {
            primary_key: None,
            ..durable_write_back_acceleration()
        };
        assert!(
            matches!(
                validate_dataset(&dataset_with_acceleration(&runtime, undeclared)),
                Err(Error::DurableWriteBackUndeclaredPrimaryKey { .. })
            ),
            "an undeclared key leaves the worker nothing to deliver on"
        );

        // A different single column than the fixture's, so this asserts the rule
        // is about arity rather than the name `id`.
        let single = spicepod::acceleration::Acceleration {
            primary_key: Some("region".to_string()),
            ..durable_write_back_acceleration()
        };
        assert!(
            validate_dataset(&dataset_with_acceleration(&runtime, single)).is_ok(),
            "any single-column key is what the worker delivers with"
        );
    }

    /// Every one of these settings is ordinary on an acceleration that does not ask
    /// for write-back at all — retention, a recreating mode and a composite key are
    /// unremarkable there, because nothing depends on the accelerator holding a row
    /// until a delivery carries it away.
    ///
    /// Leaving the regime any *other* way — the engine, `on_conflict` or
    /// `refresh_mode` — still requests `write_mode: write_back`, which cannot be
    /// delivered and is refused by the prerequisites gate instead. Those doors are
    /// covered by `validate_dataset_refuses_write_back_without_the_durable_prerequisites`.
    #[tokio::test]
    async fn validate_dataset_allows_all_of_them_when_write_back_is_not_requested() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        let acceleration = spicepod::acceleration::Acceleration {
            retention_period: Some("1d".to_string()),
            mode: spicepod::acceleration::Mode::FileCreate,
            primary_key: Some("(id, region)".to_string()),
            write_mode: spicepod::acceleration::WriteMode::default(),
            ..durable_write_back_acceleration()
        };

        assert!(
            validate_dataset(&dataset_with_acceleration(&runtime, acceleration)).is_ok(),
            "none of these settings is refusable without write-back"
        );
    }

    /// `write_mode: write_back` without the settings that make it durable used to
    /// load and then refuse every write: the write-back path is selected from
    /// `write_mode` alone, but only a resolving configuration records markers and
    /// runs a delivery worker.
    #[tokio::test]
    async fn validate_dataset_refuses_write_back_without_the_durable_prerequisites() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        for (acceleration, named) in [
            (
                spicepod::acceleration::Acceleration {
                    engine: Some("duckdb".to_string()),
                    ..durable_write_back_acceleration()
                },
                "acceleration.engine: cayenne",
            ),
            (
                spicepod::acceleration::Acceleration {
                    on_conflict: HashMap::new(),
                    ..durable_write_back_acceleration()
                },
                "acceleration.on_conflict",
            ),
            (
                spicepod::acceleration::Acceleration {
                    refresh_mode: Some(spicepod::acceleration::RefreshMode::Full),
                    ..durable_write_back_acceleration()
                },
                "acceleration.refresh_mode: changes",
            ),
        ] {
            let err = validate_dataset(&dataset_with_acceleration(&runtime, acceleration))
                .expect_err("write-back without its prerequisites must be refused");
            assert!(
                matches!(err, Error::DurableWriteBackPrerequisitesUnmet { .. }),
                "expected a prerequisites refusal, got: {err}"
            );
            // The message is the user's only account of what to add, so it names
            // the setting rather than saying something is missing.
            assert!(
                err.to_string().contains(named),
                "the refusal must name {named:?}: {err}"
            );
        }

        assert!(
            validate_dataset(&dataset_with_acceleration(
                &runtime,
                durable_write_back_acceleration()
            ))
            .is_ok(),
            "a configuration meeting them all must still load"
        );
    }

    /// A disabled acceleration accelerates nothing, so its write-back settings
    /// acknowledge nothing for delivery and are not judged — a configuration
    /// write-back could not deliver is unremarkable there.
    #[tokio::test]
    async fn validate_dataset_ignores_write_back_settings_on_a_disabled_acceleration() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        let acceleration = spicepod::acceleration::Acceleration {
            enabled: false,
            retention_period: Some("1d".to_string()),
            mode: spicepod::acceleration::Mode::FileCreate,
            primary_key: Some("(id, region)".to_string()),
            ..durable_write_back_acceleration()
        };

        assert!(
            validate_dataset(&dataset_with_acceleration(&runtime, acceleration)).is_ok(),
            "write-back settings on a disabled acceleration are inert, not refused"
        );
    }

    /// The rejection is the only account a user gets of why the dataset will not
    /// load, so it names the dataset, the setting to remove, what would go wrong,
    /// and a way out.
    #[test]
    fn the_retention_rejection_names_the_setting_and_a_way_out() {
        let message = DurableWriteBackWithRetentionSnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
            retention_setting: "acceleration.retention_period".to_string(),
        }
        .build()
        .to_string();
        for expected in [
            "orders",
            "postgres",
            "acceleration.retention_period",
            "the write would be lost",
            "acceleration.write_mode",
            "https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
        ] {
            assert!(
                message.contains(expected),
                "the rejection must contain {expected:?}: {message}"
            );
        }
    }
    use crate::component::dataset::DatasetSpec;
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

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
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
            dataset: &DatasetSpec,
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
            _context: &dyn crate::dataconnector::ConnectorContext,
            _dataset: &DatasetSpec,
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

    /// The #12415 regression: `ConnectorParamsBuilder::build` resolves the factory first and
    /// fails with `InvalidConnectorType`, which names no alternative, so the
    /// suggestion-bearing `UnknownDataConnector` written for this case was unreachable.
    #[tokio::test]
    async fn a_misspelled_dataset_connector_suggests_the_closest_connector() {
        register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory)).await;

        let app = Arc::new(app::AppBuilder::new("connector_typo").build());
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let spec = spicepod::component::dataset::Dataset::new("schema_onl:any", "typo_dataset");
        let dataset = DatasetBuilder::try_from(spec)
            .expect("valid dataset builder")
            .with_app(app)
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid runtime dataset");

        let err = runtime
            .get_dataconnector_from_dataset(Arc::new(dataset))
            .await
            .expect_err("a `from:` naming an unregistered connector must fail");

        assert!(
            matches!(err, Error::UnknownDataConnector { .. }),
            "expected UnknownDataConnector, got: {err}"
        );
        assert!(
            err.to_string().contains("Did you mean 'schema_only'?"),
            "the error should name the closest registered connector: {err}"
        );
    }

    /// ODBC is the one unregistered name that is not a typo: it is a real connector this build
    /// may simply lack, so it gets the build instruction instead of a lookalike suggestion.
    #[tokio::test]
    async fn an_unregistered_odbc_connector_reports_the_missing_build() {
        let err = unknown_data_connector(ODBC_DATACONNECTOR).await;

        assert!(
            matches!(err, Error::OdbcNotInstalled),
            "expected OdbcNotInstalled, got: {err}"
        );
    }

    /// `ScyllaDB` is not in the default build either, so a `from: scylladb:` earns the same
    /// build-or-Enterprise instruction rather than a "did you mean" over what is registered.
    #[tokio::test]
    async fn an_unregistered_scylladb_connector_reports_the_missing_build() {
        let err = unknown_data_connector(SCYLLADB_DATACONNECTOR).await;

        assert!(
            matches!(err, Error::DataConnectorNotInBuild { .. }),
            "expected DataConnectorNotInBuild, got: {err}"
        );

        let message = err.to_string();
        assert_eq!(
            message,
            "This build of Spice.ai does not include the scylladb data connector. \
Build Spice.ai OSS with the `scylladb` feature enabled, or use the Enterprise distribution of \
Spice.ai. Learn more at https://docs.spice.ai/docs/enterprise",
            "the message must keep the connector, the feature to build, and the Enterprise link"
        );
    }

    /// `ConnectorParamsBuilder` resolves the factory itself, so it carries the same message for
    /// the callers that reach it without first checking the registry.
    #[tokio::test]
    async fn scylladb_params_report_the_missing_build() {
        let app = app::AppBuilder::new("scylladb_not_built").build();
        let runtime = crate::Runtime::builder().build().await;

        let dataset = DatasetBuilder::try_new("scylladb:orders".to_string(), "orders")
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("valid runtime dataset");

        let secrets = Arc::new(tokio::sync::RwLock::new(runtime_secrets::Secrets::default()));
        let Err(err) = ConnectorParamsBuilder::for_dataset(SCYLLADB_DATACONNECTOR.into(), &dataset)
            .build(secrets, tokio::runtime::Handle::current())
            .await
        else {
            panic!("a scylladb dataset must fail on a build without the connector")
        };

        assert_eq!(
            err.to_string(),
            "Failed to initialize the dataset orders (scylladb). This build of Spice.ai does not \
include the scylladb data connector. Build Spice.ai OSS with the `scylladb` feature enabled, or \
use the Enterprise distribution of Spice.ai. Learn more at https://docs.spice.ai/docs/enterprise",
            "the message must name the dataset, the feature to build, and the Enterprise link"
        );
    }

    struct SchemaOnlyConnectorFactory;

    impl DataConnectorFactory for SchemaOnlyConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
            Box::pin(async { Ok(Arc::new(SchemaOnlyConnector) as Arc<dyn DataConnector>) })
        }

        fn prefix(&self) -> &'static str {
            "schema_only"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    #[derive(Debug)]
    struct SchemaOnlyConnector;

    #[async_trait]
    impl DataConnector for SchemaOnlyConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn crate::dataconnector::ConnectorContext,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            Ok(empty_table())
        }
    }

    /// A connector whose construction never completes — a source that accepts
    /// the connection attempt and never answers.
    ///
    /// The other shape of the same hazard is a construction that fails
    /// *transiently*, which `load_dataset` retries with unbounded backoff. Both
    /// leave an inline await with nothing to come back from, and the fix is one
    /// `tokio::spawn` that does not care which it was, so one fixture covers it.
    /// This one is preferred because it writes no metrics: a failing load
    /// increments the process-wide `dataset_load_errors` counter that
    /// `a_dataset_connector_failure_counts_one_load_error` reads as a delta.
    struct UnreachableConnectorFactory;

    impl DataConnectorFactory for UnreachableConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
            Box::pin(std::future::pending())
        }

        fn prefix(&self) -> &'static str {
            "never_reachable"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    fn spicepod_dataset(from: &str, name: &str) -> spicepod::component::dataset::Dataset {
        spicepod::component::dataset::Dataset::new(from, name)
    }

    /// An empty single-column table: a schema for a connector to answer with, and a real
    /// registration for the removal path to deregister rather than passing over a name
    /// that was never there.
    fn empty_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        Arc::new(
            datafusion::datasource::MemTable::try_new(schema, vec![vec![]])
                .expect("empty MemTable with a single column"),
        )
    }

    /// Regression test for #12862: `apply_dataset_diff` awaited each added
    /// dataset's `load_dataset` inline, and a load that does not complete — a
    /// source that never answers, or a transient failure retried with unbounded
    /// Fibonacci backoff — therefore parked that apply. `apply_app` holds
    /// `apply_app_lock` across the whole diff, so every apply queued behind it
    /// was parked too, until the process restarted.
    ///
    /// The bound here is wall-clock on purpose: the failure it guards against is
    /// an apply that never returns, so the assertion has to be "returns at all".
    #[tokio::test]
    async fn an_unreachable_added_dataset_does_not_block_the_apply() {
        register_connector_factory("never_reachable", Arc::new(UnreachableConnectorFactory)).await;
        register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory)).await;

        let runtime = Arc::new(
            crate::Runtime::builder()
                .with_app(app::AppBuilder::new("bounded_apply").build())
                .build()
                .await,
        );

        let reloaded = Arc::new(
            app::AppBuilder::new("bounded_apply")
                .with_dataset(spicepod_dataset("never_reachable:any", "unreachable"))
                .with_dataset(spicepod_dataset("schema_only:any", "healthy"))
                .build(),
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                Arc::clone(&runtime).apply_app(reloaded)
            )
            .await
            .expect("an added dataset that cannot load must not hold the apply lock"),
            "the reloaded spicepod differs from the booted one, so it must apply"
        );

        let healthy = TableReference::parse_str("healthy");
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(30), || async {
                matches!(
                    runtime.status().get_dataset_statuses().get(&healthy),
                    Some(status::ComponentStatus::Ready | status::ComponentStatus::Refreshing)
                )
            })
            .await,
            "the dataset alongside the unreachable one must still become queryable"
        );

        // The lock is only proven released by a second apply completing while the
        // first apply's dataset is still stuck in the background.
        let third = Arc::new(
            app::AppBuilder::new("bounded_apply")
                .with_dataset(spicepod_dataset("never_reachable:any", "unreachable"))
                .with_dataset(spicepod_dataset("schema_only:any", "healthy"))
                .with_dataset(spicepod_dataset("schema_only:any", "healthy_too"))
                .build(),
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                Arc::clone(&runtime).apply_app(third)
            )
            .await
            .expect("a later apply must not inherit the earlier apply's wait"),
            "the third spicepod adds a dataset, so it must apply"
        );

        // The stuck load's task outlives the test holding its own `Arc<Runtime>`;
        // marking shutdown will not unstick a connector already inside `create`,
        // but it stops the runtime the remaining assertions no longer need.
        runtime.status.mark_shutdown();
    }

    /// A connector whose construction waits until its test opens `gate`, and then
    /// returns a table with a `stale` column. Each test uses its own prefix and
    /// gate, because the connector registry is process-wide.
    struct GatedStaleConnectorFactory {
        prefix: &'static str,
        gate: &'static Semaphore,
    }

    impl DataConnectorFactory for GatedStaleConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
            Box::pin(async {
                let _open = self.gate.acquire().await;
                Ok(Arc::new(StaleConnector) as Arc<dyn DataConnector>)
            })
        }

        fn prefix(&self) -> &'static str {
            self.prefix
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    #[derive(Debug)]
    struct StaleConnector;

    #[async_trait]
    impl DataConnector for StaleConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn crate::dataconnector::ConnectorContext,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "stale",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]])
                .expect("empty MemTable with a single column");
            Ok(Arc::new(table) as Arc<dyn TableProvider>)
        }
    }

    /// Regression test for #1458: a dataset whose load had not succeeded yet was
    /// corrected in the Spicepod, and the load of the earlier configuration kept
    /// running. When that source answered, its load registered the earlier
    /// configuration over the corrected one, so queries read the wrong source
    /// while `/v1/datasets` reported the corrected one.
    ///
    /// The last wait is for something that must not happen, so it is bounded by
    /// wall-clock: on the unfixed code the stale registration lands within
    /// milliseconds of the gate opening.
    #[tokio::test]
    async fn a_corrected_dataset_is_not_overwritten_by_its_earlier_load() {
        static GATE: Semaphore = Semaphore::const_new(0);
        register_connector_factory(
            "gated",
            Arc::new(GatedStaleConnectorFactory {
                prefix: "gated",
                gate: &GATE,
            }),
        )
        .await;
        register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory)).await;

        let runtime = Arc::new(
            crate::Runtime::builder()
                .with_app(app::AppBuilder::new("corrected").build())
                .build()
                .await,
        );
        let first = Arc::new(
            app::AppBuilder::new("corrected")
                .with_dataset(spicepod_dataset("gated:earlier", "t"))
                .build(),
        );
        assert!(Arc::clone(&runtime).apply_app(first).await);

        let corrected = Arc::new(
            app::AppBuilder::new("corrected")
                .with_dataset(spicepod_dataset("schema_only:any", "t"))
                .build(),
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                Arc::clone(&runtime).apply_app(corrected)
            )
            .await
            .expect("correcting a dataset must not wait for its earlier load's source"),
            "the corrected spicepod differs from the first one, so it must apply"
        );

        let t = TableReference::parse_str("t");
        let columns = || async {
            runtime.df.get_table(&t).await.map(|table| {
                table
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            })
        };
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(30), || async {
                columns().await.is_some()
            })
            .await,
            "the corrected dataset must register"
        );
        assert_eq!(columns().await, Some(vec!["id".to_string()]));

        // The earlier configuration's source answers now.
        GATE.add_permits(Semaphore::MAX_PERMITS);

        let overwritten =
            test_framework::utils::wait_until_true(Duration::from_secs(3), || async {
                columns().await != Some(vec!["id".to_string()])
            })
            .await;
        assert!(
            !overwritten,
            "the earlier configuration's load registered over the corrected dataset: {:?}",
            columns().await
        );

        runtime.status.mark_shutdown();
    }

    /// A connector whose first construction fails with a retriable error, and
    /// every later one returns a table with an `id` column.
    struct FailsOnceConnectorFactory {
        prefix: &'static str,
        failed: &'static std::sync::atomic::AtomicBool,
    }

    impl DataConnectorFactory for FailsOnceConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
            Box::pin(async {
                if self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    Ok(Arc::new(SchemaOnlyConnector) as Arc<dyn DataConnector>)
                } else {
                    Err(Box::new(std::io::Error::other("source not available yet"))
                        as Box<dyn std::error::Error + Send + Sync>)
                }
            })
        }

        fn prefix(&self) -> &'static str {
            self.prefix
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    /// #1458, when the corrected configuration's source is not available yet
    /// either: the dataset never registered, so the correction must keep
    /// retrying its own source, as a newly added dataset does, rather than try
    /// it once and leave the dataset in error until the next Spicepod change.
    #[tokio::test]
    async fn a_corrected_dataset_retries_until_its_source_answers() {
        static FAILED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        register_connector_factory("never_reachable", Arc::new(UnreachableConnectorFactory)).await;
        register_connector_factory(
            "fails_once",
            Arc::new(FailsOnceConnectorFactory {
                prefix: "fails_once",
                failed: &FAILED,
            }),
        )
        .await;

        let runtime = Arc::new(
            crate::Runtime::builder()
                .with_app(app::AppBuilder::new("corrected_retry").build())
                .build()
                .await,
        );
        let first = Arc::new(
            app::AppBuilder::new("corrected_retry")
                .with_dataset(spicepod_dataset("never_reachable:earlier", "t"))
                .build(),
        );
        assert!(Arc::clone(&runtime).apply_app(first).await);

        let corrected = Arc::new(
            app::AppBuilder::new("corrected_retry")
                .with_dataset(spicepod_dataset("fails_once:any", "t"))
                .build(),
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                Arc::clone(&runtime).apply_app(corrected)
            )
            .await
            .expect("correcting a dataset must not wait for its earlier load's source"),
            "the corrected spicepod differs from the first one, so it must apply"
        );

        let t = TableReference::parse_str("t");
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(30), || async {
                runtime.df.table_exists(&t)
            })
            .await,
            "the corrected dataset must register once its source answers"
        );
        assert!(
            FAILED.load(std::sync::atomic::Ordering::SeqCst),
            "the corrected source's first attempt must have failed, or this test proves nothing"
        );

        runtime.status.mark_shutdown();
    }

    /// #1458, for a load queued behind another: a `localpod` dataset waits for its
    /// parent's load before its own starts, and one removed from the Spicepod
    /// while it waits must not register once the parent loads.
    #[tokio::test]
    async fn a_removed_localpod_dataset_waiting_for_its_parent_never_registers() {
        static GATE: Semaphore = Semaphore::const_new(0);
        register_connector_factory(
            "gated_parent",
            Arc::new(GatedStaleConnectorFactory {
                prefix: "gated_parent",
                gate: &GATE,
            }),
        )
        .await;

        let runtime = Arc::new(
            crate::Runtime::builder()
                .with_app(app::AppBuilder::new("queued_child").build())
                .build()
                .await,
        );
        let parent = || spicepod_dataset("gated_parent:any", "parent");
        let with_child = Arc::new(
            app::AppBuilder::new("queued_child")
                .with_dataset(parent())
                .with_dataset(spicepod_dataset("localpod:parent", "child"))
                .build(),
        );
        assert!(Arc::clone(&runtime).apply_app(with_child).await);

        let without_child = Arc::new(
            app::AppBuilder::new("queued_child")
                .with_dataset(parent())
                .build(),
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(30),
                Arc::clone(&runtime).apply_app(without_child)
            )
            .await
            .expect("removing a queued dataset must not wait for its parent"),
            "the spicepod without the child differs, so it must apply"
        );

        GATE.add_permits(Semaphore::MAX_PERMITS);

        let parent_ref = TableReference::parse_str("parent");
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(30), || async {
                runtime.df.table_exists(&parent_ref)
            })
            .await,
            "the parent must load once its source answers"
        );
        let child_ref = TableReference::parse_str("child");
        let registered = test_framework::utils::wait_until_true(Duration::from_secs(3), || async {
            runtime.df.table_exists(&child_ref)
        })
        .await;
        assert!(
            !registered,
            "a localpod dataset removed from the Spicepod registered once its parent loaded"
        );

        runtime.status.mark_shutdown();
    }

    /// The wait a hot reload performs on the recreated table's first refresh.
    /// #12862: it was untimed, and `apply_app_lock` is held across it.
    mod hot_reload_initial_refresh {
        use super::*;
        use crate::accelerated::refresh_completion::RefreshCompletion;
        use tokio_util::sync::CancellationToken;

        /// A completion signal no refresh ever reports on. The recorder comes
        /// back with the waiter and has to be held for the length of the wait:
        /// dropping it releases the waiter, which is the opposite of what these
        /// arms are asking for.
        fn silent() -> (RefreshCompletion, RefreshCompletionWaiter) {
            let completion = RefreshCompletion::new();
            let waiter = completion.any();
            (completion, waiter)
        }

        /// The production bound, so these arms cannot drift from it. A paused
        /// clock makes its size irrelevant to how long they take.
        const TIMEOUT: Duration = HOT_RELOAD_INITIAL_REFRESH_TIMEOUT;

        fn reloading() -> TableReference {
            TableReference::bare("reloading")
        }

        /// A table already loaded when the wait begins must not wait at all —
        /// the refresh finished before the reload got here.
        #[tokio::test(start_paused = true)]
        async fn an_already_loaded_table_does_not_wait() {
            let (_recorder, waiter) = silent();
            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &|| true,
                waiter,
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a table that has already loaded needs no completion");

            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "a loaded table must be recognised before the wait, not after it"
            );
        }

        /// The ordinary case: the refresh completes and reports it.
        #[tokio::test(start_paused = true)]
        async fn a_reported_completion_ends_the_wait() {
            let completion = RefreshCompletion::new();
            let waiter = completion.any();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                completion.record_untriggered();
            });

            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &|| false,
                waiter,
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a reported refresh completes the wait");

            assert!(
                started.elapsed() < TIMEOUT,
                "the wait must end on the completion, not on the bound"
            );
        }

        /// Regression test for #13086. A refresh that finished before the reload
        /// reached this wait must end it at once. The edge-triggered signal this
        /// replaced had nothing left to report by then, so the reload spent the
        /// whole bound and then discarded a table that was loaded.
        #[tokio::test(start_paused = true)]
        async fn a_completion_that_predates_the_wait_ends_it_immediately() {
            let completion = RefreshCompletion::new();
            completion.record_untriggered();

            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &|| false,
                completion.any(),
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a refresh that already completed must end the wait");

            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "a completion recorded before the wait must be observed, not waited out"
            );
        }

        /// Regression test for #13086. A cluster scheduler runs no refresh
        /// locally and closes the table's completion signal instead, which must
        /// end the wait rather than spend the bound on every hot reload.
        #[tokio::test(start_paused = true)]
        async fn a_closed_completion_signal_ends_the_wait() {
            let completion = RefreshCompletion::new();
            completion.close();

            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &|| false,
                completion.any(),
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a signal that will never report again must end the wait");

            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "a closed signal must be recognised immediately, not at the bound"
            );
        }

        /// A `refresh_mode: changes` stream that never produces a ready envelope
        /// never reports a completion. Before #12862 this held the apply lock
        /// for the life of the process.
        #[tokio::test(start_paused = true)]
        async fn a_refresh_that_never_completes_gives_up_at_the_bound() {
            let (_recorder, waiter) = silent();
            let started = tokio::time::Instant::now();
            let err = await_hot_reload_initial_refresh(
                &reloading(),
                &|| false,
                waiter,
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect_err("a refresh that never completes must not wait forever");

            assert!(
                matches!(err, Error::HotReloadRefreshTimedOut { .. }),
                "expected the hot-reload bound to be reported, got: {err}"
            );
            assert_eq!(
                started.elapsed(),
                TIMEOUT,
                "the wait must last exactly the bound it was given"
            );
        }

        /// A completion landing between the loaded-check and the `select!` must
        /// still end the wait. The closure is the seam: it runs in exactly that
        /// window.
        #[tokio::test(start_paused = true)]
        async fn a_completion_racing_the_loaded_check_is_not_missed() {
            let completion = RefreshCompletion::new();
            let waiter = completion.any();
            let records_then_reports_unloaded = move || {
                completion.record_untriggered();
                false
            };

            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &records_then_reports_unloaded,
                waiter,
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a completion racing the wait setup must end it, not be missed");

            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "a waiter must be released immediately, not at the bound"
            );
        }

        /// The bound and the completion can become ready together, and
        /// `select!` picks between ready branches at random. The table is loaded
        /// either way, so the backstop check must not let the bound discard it.
        #[tokio::test(start_paused = true)]
        async fn a_load_landing_at_the_bound_is_not_discarded() {
            // False for the pre-wait check, true for the backstop check: the
            // refresh completed while the wait was outstanding.
            let checks = AtomicUsize::new(0);
            let loaded = || checks.fetch_add(1, Ordering::SeqCst) >= 1;

            let (_recorder, waiter) = silent();
            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(
                &reloading(),
                &loaded,
                waiter,
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect("a load that lands at the bound must not discard the table");

            // The wait ran to the bound, and the table was accepted by the
            // backstop re-check after it: exactly the pre-wait check and one more.
            assert_eq!(started.elapsed(), TIMEOUT, "the wait must end at the bound");
            assert_eq!(
                checks.load(Ordering::SeqCst),
                2,
                "the pre-wait check and the backstop check after the bound"
            );
        }

        /// A one-shot load failure must not accept the unloaded table. Recording
        /// that failure as an ordinary completion would return `Ok` here.
        #[tokio::test(start_paused = true)]
        async fn a_terminal_failure_does_not_accept_an_unloaded_table() {
            let completion = RefreshCompletion::new();
            completion.record_terminal_failure(completion.issue());

            let started = tokio::time::Instant::now();
            let err = await_hot_reload_initial_refresh(
                &reloading(),
                &|| false,
                completion.any(),
                &CancellationToken::new(),
                TIMEOUT,
            )
            .await
            .expect_err("a failed one-shot load is not a loaded table");

            assert!(
                matches!(err, Error::HotReloadRefreshFailed { .. }),
                "expected a terminal refresh failure, got: {err}"
            );
            assert_eq!(
                started.elapsed(),
                Duration::ZERO,
                "a terminal failure must end the wait immediately, not spend the bound"
            );
        }

        /// Shutdown ends the wait without reporting a reload failure.
        #[tokio::test(start_paused = true)]
        async fn shutdown_ends_the_wait() {
            let token = CancellationToken::new();
            let cancel = token.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                cancel.cancel();
            });

            let (_recorder, waiter) = silent();
            let started = tokio::time::Instant::now();
            await_hot_reload_initial_refresh(&reloading(), &|| false, waiter, &token, TIMEOUT)
                .await
                .expect("a runtime shutting down is not a failed reload");

            assert!(
                started.elapsed() < TIMEOUT,
                "shutdown must not wait out the bound"
            );
        }
    }

    /// Regression test for #12339: a `time_column` the source schema does not
    /// have is a configuration error no retry can clear, so registration must
    /// report it as a permanent failure rather than letting `load_dataset`
    /// retry it for the life of the process.
    #[tokio::test]
    async fn a_dataset_configuration_error_fails_permanently() {
        register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory)).await;

        let mut dataset =
            spicepod::component::dataset::Dataset::new("schema_only:any", "missing_time_column");
        dataset.acceleration = Some(spicepod::acceleration::Acceleration {
            enabled: true,
            ..spicepod::acceleration::Acceleration::default()
        });
        dataset.time_column = Some("not_in_the_source_schema".to_string());

        let app = app::AppBuilder::new("permanent_configuration_failure")
            .with_dataset(dataset.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let ds = DatasetBuilder::try_from(dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid runtime dataset");

        let err = runtime
            .try_load_dataset_once(Arc::new(ds), BootstrapStatus::None, None)
            .await
            .expect_err("a missing time column should fail the load");

        assert!(
            matches!(err, Error::PermanentDatasetFailure { .. }),
            "expected a permanent failure, got: {err}"
        );
    }

    /// Regression test for #14918: every dataset's load-failure line goes through
    /// the runtime's one rate limiter, so the limiter has to be keyed on the
    /// dataset. A shared key would let the first dataset to fail suppress every
    /// other dataset's failure for the whole interval, leaving them in `Error`
    /// with no line naming them.
    #[test]
    fn each_failing_dataset_logs_its_own_load_failure() {
        let lines = crate::tracing_util::warn_lines_emitted_by(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("to build a test runtime")
                .block_on(async {
                    register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory))
                        .await;
                    let runtime = Arc::new(crate::Runtime::builder().build().await);
                    // The `_a` dataset fails a second time inside the interval in
                    // each loop, and must still log only once.
                    //
                    // A missing `time_column` is a permanent failure (`error_spaced!`).
                    for name in ["time_column_a", "time_column_b", "time_column_a"] {
                        let ds = accelerated_schema_only_dataset(&runtime, name, |dataset| {
                            dataset.time_column = Some("not_in_the_source_schema".to_string());
                        });
                        runtime
                            .try_load_dataset_once(ds, BootstrapStatus::None, None)
                            .await
                            .expect_err("a missing time column should fail the load");
                    }
                    // `refresh_mode: changes` over a source without a change stream
                    // is a retriable failure (`warn_spaced!`).
                    for name in ["changes_a", "changes_b", "changes_a"] {
                        let ds = accelerated_schema_only_dataset(&runtime, name, |dataset| {
                            if let Some(acceleration) = dataset.acceleration.as_mut() {
                                acceleration.refresh_mode =
                                    Some(spicepod::acceleration::RefreshMode::Changes);
                            }
                        });
                        runtime
                            .try_load_dataset_once(ds, BootstrapStatus::None, None)
                            .await
                            .expect_err(
                                "a changes load over a source without a change stream should fail",
                            );
                    }
                });
        });

        let failures_of = |name: &str| {
            lines
                .iter()
                .filter(|line| line.contains(&format!(" {name} ")))
                .count()
        };
        assert_eq!(
            ["time_column_a", "time_column_b", "changes_a", "changes_b"].map(failures_of),
            [1, 1, 1, 1],
            "each failing dataset must log its own failure, once per interval: {lines:#?}"
        );
    }

    /// An accelerated `schema_only` dataset named `name`, adjusted by `configure`.
    fn accelerated_schema_only_dataset(
        runtime: &Arc<crate::Runtime>,
        name: &str,
        configure: impl FnOnce(&mut spicepod::component::dataset::Dataset),
    ) -> Arc<Dataset> {
        let mut dataset = spicepod::component::dataset::Dataset::new("schema_only:any", name);
        dataset.acceleration = Some(spicepod::acceleration::Acceleration {
            enabled: true,
            ..spicepod::acceleration::Acceleration::default()
        });
        configure(&mut dataset);

        let app = app::AppBuilder::new(name)
            .with_dataset(dataset.clone())
            .build();
        Arc::new(
            DatasetBuilder::try_from(dataset)
                .expect("valid dataset builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::clone(runtime))
                .build()
                .expect("valid runtime dataset"),
        )
    }

    /// `access: read_write` over a source that only supports reads is a Spicepod
    /// mistake no retry clears: it fails the load once, naming
    /// `write_mode: acceleration`, instead of retrying for the life of the process.
    #[tokio::test]
    async fn a_read_write_dataset_over_a_read_only_source_fails_permanently() {
        register_connector_factory("schema_only", Arc::new(SchemaOnlyConnectorFactory)).await;

        let mut dataset =
            spicepod::component::dataset::Dataset::new("schema_only:any", "read_only_source");
        dataset.access = spicepod::component::access::AccessMode::ReadWrite;
        dataset.acceleration = Some(spicepod::acceleration::Acceleration {
            enabled: true,
            ..spicepod::acceleration::Acceleration::default()
        });

        let app = app::AppBuilder::new("read_only_source")
            .with_dataset(dataset.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let ds = DatasetBuilder::try_from(dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid runtime dataset");

        let err = runtime
            .try_load_dataset_once(Arc::new(ds), BootstrapStatus::None, None)
            .await
            .expect_err("a read-only source should fail a read_write load");

        assert!(
            matches!(err, Error::PermanentDatasetFailure { .. }),
            "expected a permanent failure, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("Set `acceleration.write_mode: acceleration`"),
            "{err}"
        );
    }

    /// A `from:` no build of the runtime can resolve is settled at parse time,
    /// so it must not be retried either.
    #[tokio::test]
    async fn an_unknown_connector_fails_permanently() {
        let dataset = spicepod::component::dataset::Dataset::new(
            "not_a_real_connector:any",
            "unknown_connector",
        );

        let app = app::AppBuilder::new("unknown_connector")
            .with_dataset(dataset.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let ds = DatasetBuilder::try_from(dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid runtime dataset");

        let err = runtime
            .try_load_dataset_once(Arc::new(ds), BootstrapStatus::None, None)
            .await
            .expect_err("an unknown connector should fail the load");

        assert!(
            matches!(err, Error::PermanentDatasetFailure { .. }),
            "expected a permanent failure, got: {err}"
        );
    }

    /// These refusals are pure functions of the Spicepod, so retrying cannot
    /// change the answer. Left retriable they are retried for the life of the
    /// process — rebuilding the connector on every attempt — and the dataset
    /// never leaves `Initializing`.
    #[test]
    fn a_durable_write_back_configuration_refusal_is_permanent() {
        let with_retention = DurableWriteBackWithRetentionSnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
            retention_setting: "acceleration.retention_period".to_string(),
        }
        .build();
        let recreating_mode = DurableWriteBackRecreatingModeSnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
            mode: "file_create".to_string(),
        }
        .build();
        let undeclared_key = DurableWriteBackUndeclaredPrimaryKeySnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
        }
        .build();
        let unsupported_source = DurableWriteBackUnsupportedBySourceSnafu {
            dataset_name: "orders".to_string(),
            connector: "duckdb".to_string(),
        }
        .build();
        let prerequisites = DurableWriteBackPrerequisitesUnmetSnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
            missing: "acceleration.refresh_mode: changes".to_string(),
        }
        .build();
        let composite_key = DurableWriteBackCompositePrimaryKeySnafu {
            dataset_name: "orders".to_string(),
            connector: "postgres".to_string(),
            primary_key: "id, region".to_string(),
            pk_columns: 2_usize,
        }
        .build();

        for err in [
            &with_retention,
            &recreating_mode,
            &composite_key,
            &undeclared_key,
            &prerequisites,
            &unsupported_source,
        ] {
            assert!(
                is_permanent_dataset_failure(err),
                "a configuration that cannot deliver an acknowledged write must not be retried: {err}"
            );
        }
    }

    #[test]
    fn a_contradictory_dataset_configuration_is_permanent() {
        let err = FullTextSearchRequiresAccelerationSnafu {
            dataset_name: "docs".to_string(),
        }
        .build();
        assert!(
            is_permanent_dataset_failure(&err),
            "full-text search without acceleration cannot resolve itself"
        );
        let err = crate::AccelerationWriteModeWithChangesSnafu {
            dataset_name: "orders".to_string(),
        }
        .build();
        assert!(
            is_permanent_dataset_failure(&err),
            "write_mode: acceleration with a change stream cannot resolve itself"
        );
    }

    /// The #12416 regression: `ConnectorParamsBuilder` validates parameters
    /// before the connector is built, so it raises `runtime_parameters::Error`
    /// rather than a `DataConnectorError`. The original downcast recognised only
    /// the latter, so a typo'd `one_of` value or a missing required parameter was
    /// classified transient and retried for the life of the process.
    #[test]
    fn a_dataset_parameter_validation_failure_is_permanent() {
        let source = runtime_parameters::Error::InvalidConfigurationNoSource {
            component: "dataset taxi_trips".to_string(),
            message: "'s3_auth' must be one of: public, key, iam_role. Found 'keys'.".to_string(),
        };
        let err = Error::UnableToInitializeDataConnector {
            source: Box::new(source),
        };
        assert!(
            is_permanent_dataset_failure(&err),
            "an out-of-vocabulary parameter value is a pure function of the Spicepod"
        );
    }

    /// Regression test for #14609: a source that is down when the dataset loads
    /// reports `UnableToConnectInvalidHostOrPort`, which must not be a permanent
    /// failure, so the dataset keeps retrying and recovers once the source is
    /// reachable. Rejected credentials and TLS failures are configuration errors and
    /// stay permanent.
    #[test]
    fn an_unreachable_source_stays_retriable_but_rejected_credentials_do_not() {
        let component = crate::dataconnector::ConnectorComponent::Dataset(Arc::new(
            crate::component::dataset::DatasetSpec::new(
                "postgres:public.orders",
                TableReference::bare("orders"),
            ),
        ));
        let boxed =
            |source: dataconnector::DataConnectorError| Error::UnableToInitializeDataConnector {
                source: Box::new(source),
            };

        let unreachable = boxed(
            dataconnector::DataConnectorError::UnableToConnectInvalidHostOrPort {
                dataconnector: "postgres".to_string(),
                connector_component: component.clone(),
                host: "db".to_string(),
                port: "5432".to_string(),
            },
        );
        assert!(
            !is_permanent_dataset_failure(&unreachable),
            "a source that cannot be reached right now must be retried: {unreachable}"
        );

        for configuration_error in [
            dataconnector::DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                dataconnector: "postgres".to_string(),
                connector_component: component.clone(),
            },
            dataconnector::DataConnectorError::UnableToConnectTlsError {
                dataconnector: "postgres".to_string(),
                connector_component: component,
            },
        ] {
            let err = boxed(configuration_error);
            assert!(
                is_permanent_dataset_failure(&err),
                "a credential or TLS misconfiguration must not be retried: {err}"
            );
        }
    }

    /// An error type neither downcast recognises must not be assumed permanent —
    /// failing open here would strand a dataset that would have recovered.
    #[test]
    fn an_unclassified_boxed_connector_error_stays_retriable() {
        let err = Error::UnableToInitializeDataConnector {
            source: "connection reset by peer".into(),
        };
        assert!(
            !is_permanent_dataset_failure(&err),
            "an unrecognised error is not evidence the configuration is wrong"
        );
    }

    #[test]
    fn only_configuration_errors_are_classified_permanent() {
        use crate::datafusion::Error as DfError;

        assert!(
            !DfError::UnsupportedRefreshCompleteForStream.is_retriable(),
            "a refresh setting the source cannot serve needs an operator to change it"
        );
        assert!(
            !DfError::SnapshotCreationBatchesShouldBePositive.is_retriable(),
            "an out-of-range Spicepod value needs an operator to change it"
        );
        assert!(
            DfError::TableAlreadyExists {}.is_retriable(),
            "an unclassified registration failure must keep retrying"
        );
        assert!(
            DfError::UnableToLockDataWriters {}.is_retriable(),
            "contention on an internal lock is transient"
        );
    }

    /// Installs a `MeterProvider` backed by a scrapable Prometheus registry, so the
    /// `datasets::LOAD_ERROR` counter this module writes can be read back.
    ///
    /// The metric statics are `LazyLock`s that bind to whichever provider is global
    /// when they are first touched, and that binding survives a later
    /// `set_meter_provider`. So this rewires the meter for the whole process and only
    /// the first caller in it wins -- keep it to a single test, and run that test in
    /// a process of its own (see `run_in_own_process`).
    fn install_prometheus_meter_provider() -> prometheus::Registry {
        let registry = prometheus::Registry::new();

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_resource(opentelemetry_sdk::Resource::builder().build())
            .with_reader(
                crate::prometheus_reader(registry.clone()).expect("to build the prometheus reader"),
            )
            .build();
        opentelemetry::global::set_meter_provider(provider);

        registry
    }

    /// Reads a counter's current value, treating "never incremented" as zero -- a
    /// counter that was never written does not appear among the gathered families.
    fn counter_value(registry: &prometheus::Registry, name: &str) -> f64 {
        registry
            .gather()
            .iter()
            .find(|family| {
                family.name() == name
                    && family.get_field_type() == prometheus::proto::MetricType::COUNTER
            })
            .and_then(|family| family.get_metric().first())
            .map_or(0.0, |metric| metric.get_counter().value())
    }

    /// A `DataAccelerator` that records whether the runtime ever asked it to
    /// initialize. `init` is where a `mode: file_create` accelerator is dropped,
    /// so "was `init` called" is the same question as "was the accelerator, and
    /// the markers beside it, destroyed".
    #[derive(Debug, Default)]
    struct RecordingAccelerator {
        initialized: std::sync::atomic::AtomicBool,
    }

    impl RecordingAccelerator {
        fn was_initialized(&self) -> bool {
            self.initialized.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl data_accelerator_api::DataAccelerator for RecordingAccelerator {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn create_external_table(
            &self,
            _cmd: datafusion::logical_expr::CreateExternalTable,
            _source: Option<&dyn AccelerationSource>,
            _partition_by: Vec<runtime_table_partition::expression::PartitionedBy>,
            _runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
        ) -> std::result::Result<
            Arc<dyn datafusion::datasource::TableProvider>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            Err("the test accelerator creates no table".into())
        }

        fn name(&self) -> &'static str {
            "recording"
        }

        fn prefix(&self) -> &'static str {
            "recording"
        }

        fn parameters(&self) -> &'static [runtime_parameters::ParameterSpec] {
            &[]
        }

        async fn init(
            &self,
            _source: &dyn AccelerationSource,
        ) -> std::result::Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>>
        {
            self.initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(BootstrapStatus::none())
        }

        async fn sidecar(
            &self,
            _source: &dyn AccelerationSource,
            _registry: Arc<data_accelerator_api::AcceleratorEngineRegistry>,
            _open_option: runtime_acceleration::sidecar::OpenOption,
        ) -> std::result::Result<
            Arc<dyn runtime_acceleration::sidecar::AcceleratorSidecar>,
            runtime_checkpoint_api::CheckpointError,
        > {
            Err(runtime_acceleration::sidecar::unsupported_sidecar(
                "recording",
                "checkpoint",
            ))
        }
    }

    /// A runtime whose Cayenne engine is the recording accelerator, so a test can
    /// ask whether the runtime tried to initialize it.
    async fn runtime_with_recording_accelerator() -> (Arc<crate::Runtime>, Arc<RecordingAccelerator>)
    {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let accelerator = Arc::new(RecordingAccelerator::default());
        runtime
            .accelerator_engine_registry
            .register_accelerator_engine(
                runtime_acceleration::Engine::Cayenne,
                Arc::clone(&accelerator) as Arc<dyn data_accelerator_api::DataAccelerator>,
            )
            .await;
        (runtime, accelerator)
    }

    /// A durable-write-back dataset on `mode`. On `file_create` this is the
    /// configuration whose accelerator `init` would drop, taking the undelivered
    /// markers with it; on `file` it is the supported one, and the control that
    /// proves `init` is reachable at all.
    fn write_back_dataset_on_mode(
        runtime: &Arc<crate::Runtime>,
        mode: spicepod::acceleration::Mode,
    ) -> Arc<Dataset> {
        dataset_with_acceleration(
            runtime,
            spicepod::acceleration::Acceleration {
                mode,
                ..durable_write_back_acceleration()
            },
        )
    }

    /// The refusal has to land before `DataAccelerator::init`, not after it.
    /// `init` is where `mode: file_create` drops the Cayenne table, and dropping
    /// it deletes `cayenne_pending_write_back` — so a gate that runs afterwards
    /// reports a loss it existed to prevent. This is the startup path.
    #[tokio::test]
    async fn a_refused_write_back_dataset_never_initializes_its_accelerator() {
        let (runtime, accelerator) = runtime_with_recording_accelerator().await;

        let refused =
            write_back_dataset_on_mode(&runtime, spicepod::acceleration::Mode::FileCreate);
        let results = runtime
            .initialize_datasets_accelerators(std::slice::from_ref(&refused))
            .await;

        assert!(
            results
                .get(&refused.name)
                .is_some_and(std::result::Result::is_err),
            "the dataset must be refused rather than initialized"
        );
        assert!(
            !accelerator.was_initialized(),
            "init would have dropped the accelerator and every marker beside it"
        );

        // The control: the same dataset on the one mode that can hold an
        // undelivered write does reach `init`. Without this the assertion above
        // would also pass if nothing ever called `init`.
        let allowed = write_back_dataset_on_mode(&runtime, spicepod::acceleration::Mode::File);
        let results = runtime
            .initialize_datasets_accelerators(std::slice::from_ref(&allowed))
            .await;
        assert!(
            results
                .get(&allowed.name)
                .is_some_and(std::result::Result::is_ok),
            "a supported configuration must still initialize"
        );
        assert!(
            accelerator.was_initialized(),
            "so `init` is reachable, and the refusal above is what stopped it"
        );
    }

    /// The same guard on the reload entry, exercised directly. `apply_dataset_diff`
    /// preflights before it reaches `update_dataset`, so this pins the
    /// defense-in-depth check rather than a live hole: both of `update_dataset`'s
    /// branches mutate accelerator state, so a future caller that arrives without
    /// having preflighted must still be stopped here.
    #[tokio::test]
    async fn a_refused_write_back_dataset_is_not_reloaded() {
        let (runtime, accelerator) = runtime_with_recording_accelerator().await;

        let ds = write_back_dataset_on_mode(&runtime, spicepod::acceleration::Mode::FileCreate);
        let registered_before = runtime.df.get_table(&ds.name).await.is_some();

        Arc::clone(&runtime).update_dataset(Arc::clone(&ds)).await;

        // The reload's own refusal, not a later failure: `update_dataset` sets
        // `Refreshing` and starts building a connector as its first act, so the
        // mode named in the status is what proves it stopped before that.
        let status = runtime
            .status
            .get_dataset_status(&ds.name)
            .expect("the refused reload reports a status");
        let crate::status::ComponentStatus::Error(Some(message)) = status else {
            panic!("a refused reload must report an error status, got {status:?}");
        };
        assert!(
            message.contains("file_create"),
            "the status must name the refused mode rather than a downstream failure: {message}"
        );
        assert!(
            !accelerator.was_initialized(),
            "and nothing initialized the accelerator"
        );
        assert_eq!(
            runtime.df.get_table(&ds.name).await.is_some(),
            registered_before,
            "leaving whatever was registered exactly as it was"
        );
    }

    /// A reload marks the results-cache table clock for the dataset it reloads.
    ///
    /// The reload replaces what the dataset reads, so a result read from its previous
    /// contents must stop being served as fresh, and a query that planned against the
    /// previous registration must not store the result it reads. Both of those are
    /// decided by that mark: `entry_validity` reads it on every hit, and
    /// `tables_changed_since` reads it before a result is stored. Clearing the cached
    /// plans, which is all the reload used to do, changes neither.
    #[tokio::test]
    async fn updating_a_dataset_invalidates_the_results_cached_from_it() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let ds = unloadable_dataset(&runtime);
        let provider = runtime
            .df
            .results_cache_provider()
            .expect("the results cache is enabled by default");

        let tables = std::collections::HashSet::from([ds.name.clone()]);
        let read_started_at = std::time::Instant::now();
        assert!(
            !provider.tables_changed_since(&tables, read_started_at),
            "nothing has changed this dataset yet"
        );

        // This dataset's connector cannot be built, so the reload fails after the
        // point that must invalidate: what the assertion below pins is that the
        // invalidation happens before the reload touches the registration at all.
        Arc::clone(&runtime).update_dataset(Arc::clone(&ds)).await;

        assert!(
            provider.tables_changed_since(&tables, read_started_at),
            "a reload must mark the table, or results read from the dataset's previous contents \
             stay servable as fresh until item_ttl expires"
        );
    }

    /// A connector whose construction blocks until the test releases it, so a
    /// reload can be held open between the mark at its start and the replacement
    /// at its end.
    struct GatedConnectorFactory {
        gate: Arc<tokio::sync::Semaphore>,
    }

    impl DataConnectorFactory for GatedConnectorFactory {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            _params: ConnectorParams,
            _context: &'a dyn crate::dataconnector::ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
            let gate = Arc::clone(&self.gate);
            Box::pin(async move {
                let _permit = gate
                    .acquire()
                    .await
                    .expect("the test releases the gate before awaiting the reload");
                Ok(Arc::new(SchemaOnlyConnector) as Arc<dyn DataConnector>)
            })
        }

        fn prefix(&self) -> &'static str {
            "gated_reload"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    /// The reload marks the table again once the registration has been replaced.
    ///
    /// The mark at the start of `update_dataset` cannot cover a query that begins
    /// *after* it: that query reads the registration still being replaced and
    /// finishes with a `read_started_at` later than the mark, so
    /// `tables_changed_since` accepts its result and the cache serves the
    /// dataset's previous contents as fresh until `item_ttl`.
    ///
    /// The instant this asserts from is therefore taken while the reload is
    /// parked inside connector construction, after the first mark has already
    /// landed — which is what makes it fail when only that first mark exists.
    #[tokio::test]
    async fn a_dataset_reload_marks_the_table_again_once_it_has_been_replaced() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        register_connector_factory(
            "gated_reload",
            Arc::new(GatedConnectorFactory {
                gate: Arc::clone(&gate),
            }),
        )
        .await;

        let spec = spicepod::component::dataset::Dataset::new("gated_reload:any", "replaced");
        let app = app::AppBuilder::new("reload_marks_at_replacement")
            .with_dataset(spec.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let ds = Arc::new(
            DatasetBuilder::try_from(spec)
                .expect("valid dataset builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::clone(&runtime))
                .build()
                .expect("valid runtime dataset"),
        );
        let provider = runtime
            .df
            .results_cache_provider()
            .expect("the results cache is enabled by default");
        let tables = std::collections::HashSet::from([ds.name.clone()]);

        let before_the_reload = std::time::Instant::now();
        let reload = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let ds = Arc::clone(&ds);
            async move { runtime.update_dataset(ds).await }
        });

        // The reload is now parked in connector construction, with its first mark
        // already recorded.
        assert!(
            test_framework::utils::wait_until_true(Duration::from_secs(30), || {
                let provider = Arc::clone(&provider);
                let tables = tables.clone();
                async move { provider.tables_changed_since(&tables, before_the_reload) }
            })
            .await,
            "the reload must mark the table before it builds the connector"
        );

        // Stands in for a query that starts here, reads the registration being
        // replaced, and stores its result: only a mark from the replacement is
        // later than this instant.
        let read_started_mid_reload = std::time::Instant::now();
        gate.add_permits(1);
        reload.await.expect("the reload task should not panic");

        assert!(
            provider.tables_changed_since(&tables, read_started_mid_reload),
            "the replacement must mark the table too, or a result read from the previous \
             registration after the reload started is stored and served as fresh"
        );
    }

    /// A dataset whose `from:` names no registered connector, so building its
    /// connector always fails.
    fn unloadable_dataset(runtime: &Arc<crate::Runtime>) -> Arc<Dataset> {
        let spec =
            spicepod::component::dataset::Dataset::new("not_a_real_connector:any", "reported_once");
        let app = app::AppBuilder::new("single_load_error_report")
            .with_dataset(spec.clone())
            .build();

        Arc::new(
            DatasetBuilder::try_from(spec)
                .expect("valid dataset builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::clone(runtime))
                .build()
                .expect("valid runtime dataset"),
        )
    }

    /// Regression test for #12365: `load_dataset_connector` reports a connector
    /// failure -- component status, `LOAD_ERROR`, and a log line -- and its caller
    /// then reported the very same error again, so one unloadable dataset advanced
    /// `dataset_load_errors` by 2 per attempt instead of 1.
    ///
    /// The teardown half is asserted in the same test on purpose: installing the
    /// meter provider rewires the process, so only one test per binary can do it.
    /// Deleting the caller's block also deleted the `is_shutdown()` guard around it,
    /// and that guard only ever suppressed the duplicate -- the callee counted
    /// regardless -- so a failure during teardown counted exactly one before this
    /// change and must still count exactly one.
    ///
    /// It runs in a process of its own (regression test for #13085): under
    /// `cargo test` the sibling tests share this process, so `LOAD_ERROR` could be
    /// bound to the no-op provider before this test installed its own (reading 0),
    /// or, once bound, siblings that fail a load on purpose add to the same
    /// unlabeled counter inside this test's window (reading 3 or 4).
    #[test]
    fn a_dataset_connector_failure_counts_one_load_error() {
        if run_in_own_process("a_dataset_connector_failure_counts_one_load_error") {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("to build a test runtime")
                .block_on(assert_a_dataset_connector_failure_counts_one_load_error());
        }
    }

    /// Set on the child process `run_in_own_process` spawns.
    const OWN_PROCESS_ENV: &str = "SPICE_RUNTIME_TEST_OWN_PROCESS";

    /// Re-runs the test `name` (in this module) alone in a fresh process of this
    /// test binary and asserts it passed there. Returns `true` only inside that
    /// child, where the caller runs the test body; returns `false` in the parent
    /// once the child has passed.
    fn run_in_own_process(name: &str) -> bool {
        if std::env::var_os(OWN_PROCESS_ENV).is_some() {
            return true;
        }

        // libtest names tests by module path without the crate name.
        let module = module_path!()
            .split_once("::")
            .map_or(module_path!(), |(_, module)| module);
        let test = format!("{module}::{name}");
        let output =
            std::process::Command::new(std::env::current_exe().expect("to locate the test binary"))
                .args([test.as_str(), "--exact"])
                .env(OWN_PROCESS_ENV, "1")
                .output()
                .expect("to run the test binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "{test} failed in its own process ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
        false
    }

    async fn assert_a_dataset_connector_failure_counts_one_load_error() {
        let registry = install_prometheus_meter_provider();
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        let before = counter_value(&registry, "dataset_load_errors");
        runtime
            .try_load_dataset_once(unloadable_dataset(&runtime), BootstrapStatus::None, None)
            .await
            .expect_err("a connector that cannot be created must fail the load");
        let counted = counter_value(&registry, "dataset_load_errors") - before;

        assert!(
            (counted - 1.0).abs() < f64::EPSILON,
            "one failure must be counted once, not once per reporting site; counted {counted}"
        );

        runtime.status.mark_shutdown();

        let before = counter_value(&registry, "dataset_load_errors");
        runtime
            .try_load_dataset_once(unloadable_dataset(&runtime), BootstrapStatus::None, None)
            .await
            .expect_err("a connector that cannot be created must fail the load");
        let counted = counter_value(&registry, "dataset_load_errors") - before;

        assert!(
            (counted - 1.0).abs() < f64::EPSILON,
            "teardown counted one load error before this change; counted {counted}"
        );
    }

    /// A dataset performing its first load reports `Refreshing`, exactly like one
    /// refreshing data it already holds; the registry's ever-ready record is what
    /// tells them apart, so the summary counts the first as loading and the second
    /// as ready, and stays unsettled while the first load is in flight.
    /// Regression test for #13974.
    #[test]
    fn a_first_load_counts_as_loading_and_a_refresh_of_loaded_data_as_ready() {
        let registry = status::RuntimeStatus::new();
        let first_load = TableReference::bare("first_load");
        let refreshing = TableReference::bare("refreshing");
        registry.update_dataset(&first_load, status::ComponentStatus::Initializing);
        registry.update_dataset(&first_load, status::ComponentStatus::Refreshing);
        registry.update_dataset(&refreshing, status::ComponentStatus::Ready);
        registry.update_dataset(&refreshing, status::ComponentStatus::Refreshing);

        let summarize = || {
            DatasetLoadSummary::from_statuses(&registry.get_dataset_statuses(), |dataset| {
                registry.has_dataset_ever_been_ready(dataset)
            })
        };

        let during_first_load = summarize();
        assert_eq!(
            during_first_load,
            DatasetLoadSummary {
                ready: 1,
                unhealthy: 0,
                loading: 1,
                total: 2,
            }
        );
        assert!(
            !during_first_load.is_settled(),
            "a first load in flight keeps the sampler alive"
        );
        assert_eq!(
            during_first_load.log_line(30),
            "Dataset load summary (after 30s): 1/2 ready, 0 unhealthy, 1 still initializing."
        );

        registry.update_dataset(&first_load, status::ComponentStatus::Ready);
        let after_first_load = summarize();
        assert_eq!((after_first_load.ready, after_first_load.loading), (2, 0));
        assert!(after_first_load.is_settled());
    }

    #[test]
    fn the_summary_settles_once_nothing_is_loading() {
        let mut statuses = HashMap::from([
            (
                TableReference::bare("ready"),
                status::ComponentStatus::Ready,
            ),
            (
                TableReference::bare("failed"),
                status::ComponentStatus::error_with_message("connection refused"),
            ),
            (
                TableReference::bare("disabled"),
                status::ComponentStatus::Disabled,
            ),
            (
                TableReference::bare("not_loaded"),
                status::ComponentStatus::NotLoaded,
            ),
            (
                TableReference::bare("shutting_down"),
                status::ComponentStatus::ShuttingDown,
            ),
        ]);

        let summary = DatasetLoadSummary::from_statuses(&statuses, |_| false);
        assert_eq!(
            summary,
            DatasetLoadSummary {
                ready: 1,
                unhealthy: 1,
                loading: 0,
                total: 5,
            }
        );
        assert!(summary.is_settled());

        statuses.insert(
            TableReference::bare("waiting"),
            status::ComponentStatus::Initializing,
        );
        let summary = DatasetLoadSummary::from_statuses(&statuses, |_| false);
        assert_eq!(summary.loading, 1);
        assert!(
            !summary.is_settled(),
            "an Initializing dataset keeps the sampler alive"
        );
    }

    /// Every `acceleration.ready_state` deprecation line emitted while `f` runs. Synchronous
    /// callers only — `get_valid_datasets` and `get_valid_views` log on the caller's thread.
    fn ready_state_deprecation_lines(f: impl FnOnce()) -> Vec<String> {
        crate::tracing_util::warn_lines_emitted_by(f)
            .into_iter()
            .filter(|line| line.contains("sets `acceleration.ready_state`"))
            .collect()
    }

    /// One dataset and one view, both setting the deprecated key, plus a dataset that does not.
    fn app_with_deprecated_ready_state() -> Arc<app::App> {
        #[expect(deprecated)]
        let acceleration = spicepod::acceleration::Acceleration {
            ready_state: Some(spicepod::component::dataset::ReadyState::OnRegistration),
            ..spicepod::acceleration::Acceleration::default()
        };

        let mut trips = spicepod::component::dataset::Dataset::new("test:source", "trips");
        trips.acceleration = Some(acceleration.clone());

        let mut trips_vw = spicepod::component::view::View::new("trips_vw".to_string());
        trips_vw.sql = Some("SELECT 1".to_string());
        trips_vw.acceleration = Some(acceleration);

        let mut current = spicepod::component::dataset::Dataset::new("test:source", "current");
        current.acceleration = Some(spicepod::acceleration::Acceleration::default());

        Arc::new(
            app::AppBuilder::new("deprecated_ready_state")
                .with_dataset(trips)
                .with_dataset(current)
                .with_view(trips_vw)
                .build(),
        )
    }

    /// Regression test for #13749: the deprecation notice prints exactly once per component,
    /// from the load path, and never from a read — not once per `get_valid_*` call.
    #[tokio::test]
    async fn the_ready_state_deprecation_is_reported_once_per_component_and_only_on_load() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let app = app_with_deprecated_ready_state();

        let on_dataset_load = ready_state_deprecation_lines(|| {
            let loaded = Arc::clone(&runtime).get_valid_datasets(&app, LogErrors(true));
            assert_eq!(loaded.len(), 2, "both datasets must build");
        });
        assert_eq!(
            on_dataset_load.len(),
            1,
            "one dataset sets the key, so one line — not one per conversion, and none for the \
             dataset that does not set it: {on_dataset_load:?}"
        );
        assert!(
            on_dataset_load[0].contains("Dataset 'trips'"),
            "the line names the component that set the key: {on_dataset_load:?}"
        );

        // `get_valid_views` also rebuilds every dataset (with `LogErrors(false)`) to check for
        // name collisions, so this is where the dataset's line used to reappear.
        let on_view_load = ready_state_deprecation_lines(|| {
            let loaded = Arc::clone(&runtime).get_valid_views(&app, LogErrors(true));
            assert_eq!(loaded.len(), 1, "the view must build");
        });
        assert_eq!(
            on_view_load.len(),
            1,
            "loading the views reports the view's key once and the datasets' not at all: \
             {on_view_load:?}"
        );
        assert!(
            on_view_load[0].contains("View 'trips_vw'"),
            "the line names the view: {on_view_load:?}"
        );

        // A read — `GET /v1/datasets`, `initialized_sources()`, the hot-reload comparison —
        // says so with `LogErrors(false)`, and must not warn: these are the callers that
        // multiplied the line.
        let on_read = ready_state_deprecation_lines(|| {
            let datasets = Arc::clone(&runtime).get_valid_datasets(&app, LogErrors(false));
            let views = Arc::clone(&runtime).get_valid_views(&app, LogErrors(false));
            assert_eq!((datasets.len(), views.len()), (2, 1));
        });
        assert!(
            on_read.is_empty(),
            "a read must not emit the deprecation notice: {on_read:?}"
        );
    }

    /// Build the runtime datasets of `specs` the way `get_valid_datasets` does.
    fn datasets_of(
        runtime: &Arc<crate::Runtime>,
        specs: &[spicepod::component::dataset::Dataset],
    ) -> Vec<Arc<Dataset>> {
        let app = Arc::new(
            specs
                .iter()
                .cloned()
                .fold(app::AppBuilder::new("localpod_dependents"), |b, ds| {
                    b.with_dataset(ds)
                })
                .build(),
        );
        specs
            .iter()
            .map(|spec| {
                Arc::new(
                    DatasetBuilder::try_from(spec.clone())
                        .expect("valid dataset builder")
                        .with_app(Arc::clone(&app))
                        .with_runtime(Arc::clone(runtime))
                        .build()
                        .expect("valid runtime dataset"),
                )
            })
            .collect()
    }

    fn names(datasets: &[Arc<Dataset>]) -> Vec<String> {
        datasets.iter().map(|ds| ds.name.to_string()).collect()
    }

    /// A `localpod` dataset reloads whenever the dataset it reads through does — through any
    /// depth of `localpod` chaining, and however its parent is spelled — and after it, whatever
    /// order the spicepod lists them in.
    /// Regression test for <https://github.com/spiceai/spiceai/issues/3288>.
    #[tokio::test]
    async fn localpod_dependents_reload_after_their_parent() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let all = datasets_of(
            &runtime,
            &[
                // The grandchild is listed first, so the order below is the function's. The
                // parents are spelled with the default schema and catalog: a `localpod` path
                // is resolved the way the query engine resolves it, not compared as text.
                spicepod::component::dataset::Dataset::new("localpod:public.child", "grandchild"),
                spicepod::component::dataset::Dataset::new("localpod:spice.public.parent", "child"),
                spicepod::component::dataset::Dataset::new("file:data.csv", "parent"),
                spicepod::component::dataset::Dataset::new("localpod:other", "other_child"),
                spicepod::component::dataset::Dataset::new("file:other.csv", "other"),
            ],
        );
        let parent = Arc::clone(&all[2]);

        let reloading = with_localpod_dependents(vec![parent], &all);
        assert_eq!(
            names(&reloading),
            ["parent", "child", "grandchild"],
            "the parent's whole localpod chain reloads, parents first; unrelated datasets do not"
        );

        // A changed child reloads alone: its parent is untouched.
        let reloading = with_localpod_dependents(vec![Arc::clone(&all[1])], &all);
        assert_eq!(names(&reloading), ["child", "grandchild"]);

        // Nothing changed, nothing reloads.
        assert!(with_localpod_dependents(vec![], &all).is_empty());
    }

    /// Two `localpod` datasets reading through each other can never load; the ordering walk
    /// must still terminate.
    #[tokio::test]
    async fn localpod_dependents_ordering_tolerates_a_cycle() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let all = datasets_of(
            &runtime,
            &[
                spicepod::component::dataset::Dataset::new("localpod:b", "a"),
                spicepod::component::dataset::Dataset::new("localpod:a", "b"),
            ],
        );
        let reloading = with_localpod_dependents(vec![Arc::clone(&all[0])], &all);
        assert_eq!(
            reloading.len(),
            2,
            "both datasets of the cycle are selected"
        );
    }

    /// #14251: deregistering the table is not what stops it being read. A cached logical
    /// plan holds the `TableSource` it was planned against, so a query executed before the
    /// removal keeps answering from the retired provider, while a *new* query correctly
    /// fails to plan and `information_schema.tables` no longer lists the dataset.
    ///
    /// Scope: this asserts the cached plan, and nothing about memory. The reproduction on
    /// #14251 shows a query-pool reservation still held after both caches are cleared, so
    /// some other holder keeps it; this test's empty `MemTable` reserves nothing and could
    /// not tell either way.
    ///
    /// Before the fix this read `Some(1)`: `update_dataset` and `remove_view` both discard
    /// cached plans, and the removal arm was the one that did not.
    #[tokio::test]
    async fn removing_a_dataset_discards_cached_plans() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let name = TableReference::bare("removed_ds");
        runtime
            .df
            .ctx
            .register_table(name.clone(), empty_table())
            .expect("the table registers");

        runtime
            .df
            .cache_one_plan("SELECT id FROM removed_ds")
            .await
            .expect("the query plans while the dataset is registered");
        assert_eq!(runtime.df.cached_plan_count().await, Some(1));

        Arc::clone(&runtime)
            .apply_dataset_diff(&app_with_datasets(&["removed_ds"]), &app_with_datasets(&[]))
            .await;

        assert!(
            !runtime.df.table_exists(&name),
            "the premise: the dataset really was unloaded"
        );
        assert_eq!(
            runtime.df.cached_plan_count().await,
            Some(0),
            "a plan cached over the removed dataset still resolves through its retired provider, so an already-executed query keeps answering from a dataset the catalog no longer lists"
        );

        // What the operator actually observes. Going back through the same cache key is the
        // whole defect: on `trunk` this returns the stale plan and the query answers rows,
        // which is why asserting only on the count above would be a weaker claim than the
        // issue makes.
        let replanned = runtime.df.cache_one_plan("SELECT id FROM removed_ds").await;
        let err = replanned.expect_err(
            "the previously executed SQL must be replanned against the catalog, which no longer has the dataset",
        );
        assert!(
            err.to_string().contains("removed_ds"),
            "and it must fail for the reason a new query fails — table not found: {err}"
        );
    }

    /// The premise the test above rests on: the discard belongs to the *removal*, not to
    /// applying a diff. Were `apply_dataset_diff` to clear unconditionally, the assertion
    /// above would hold for a reason that has nothing to do with `remove_dataset`.
    #[tokio::test]
    async fn an_apply_that_removes_no_dataset_keeps_cached_plans() {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        runtime
            .df
            .cache_one_plan("SELECT 1")
            .await
            .expect("SELECT 1 should plan");

        Arc::clone(&runtime)
            .apply_dataset_diff(&app_with_datasets(&[]), &app_with_datasets(&[]))
            .await;

        assert_eq!(runtime.df.cached_plan_count().await, Some(1));
    }

    /// The warning is the only account an operator gets of why a dataset that is gone keeps
    /// answering, so it has to say *that*, not that the dataset "is updating" — which is what
    /// the one shared message said before the unload path had its own.
    #[test]
    fn a_failed_unload_invalidation_says_the_dataset_keeps_answering() {
        let dataset = TableReference::partial("public", "orders");
        let unload = cache_invalidation_warning(
            &dataset,
            CacheInvalidation::Unload,
            &"cache backend unavailable",
        );
        assert!(
            unload.contains("'public.orders'"),
            "the dataset is named, and quoted so an empty or word-like name survives: {unload}"
        );
        assert!(
            unload.contains("unloaded"),
            "an operator reading this must not be told the dataset is updating: {unload}"
        );
        assert!(
            unload.contains("cache backend unavailable"),
            "the cause is carried: {unload}"
        );
        assert!(!unload.contains('\n'), "one log line: {unload}");

        let reload = cache_invalidation_warning(
            &dataset,
            CacheInvalidation::Reload,
            &"cache backend unavailable",
        );
        assert!(
            reload.contains("is updating") && reload.contains("previous contents"),
            "the reload wording is unchanged, so an operator's existing alert keeps matching: {reload}"
        );
    }

    /// An app declaring `names` as `file:` datasets. The `from` never has to resolve: the
    /// removal arm reads only which names left the app.
    fn app_with_datasets(names: &[&str]) -> Arc<App> {
        let mut builder = app::AppBuilder::new("dataset_removal");
        for name in names {
            builder = builder.with_dataset(spicepod_dataset("file:data.csv", name));
        }
        Arc::new(builder.build())
    }
}

#[cfg(all(test, feature = "snapshots", feature = "duckdb"))]
#[path = "dataset_snapshot_bootstrap_tests.rs"]
mod snapshot_bootstrap_tests;
