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

//! A representation of a federated table in Spice.
//!
//! A federated table is mainly just a wrapper around an `Arc<dyn TableProvider>`. However,
//! in the event that we cannot connect to the table provider, we can create a task
//! to keep trying to connect to the table provider until it is available.
//!
//! Combined with the ability to retrieve the schema of the table from an existing acceleration,
//! this allows us to register accelerated tables and serve data from them while waiting
//! for the table provider to become available.
//!
//! Unlike the `AcceleratedTable` struct, this struct does not implement the `TableProvider` trait itself.
//! It only provides a way to get the underlying table provider and schema.

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};

use crate::datafusion::table_provider_with_spicepod_metadata;
use arrow::datatypes::SchemaRef;
use arrow_tools::schema::schema_difference;
use arrow_tools::schema_evolution::{self, EvolutionContext, SchemaEvolution};
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use tokio::sync::{RwLock, oneshot};
use tokio_util::sync::CancellationToken;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::{
    component::dataset::{Dataset, OnSchemaChange, acceleration::RefreshMode},
    dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint},
    dataconnector::{DataConnector, DataConnectorError},
    schema_evolution::{
        SCHEMA_EVOLUTION_DETECTED, SCHEMA_EVOLUTION_FAILED, dataset_constraint_columns,
        engine_supports_in_place_evolution, evolution_allowed, schema_evolution_labels,
        widening_plan_kind,
    },
    tracers::OnceTracer,
    warn_once,
};

/// A [`TableProvider`] that always returns an error when scanned.
///
/// Used as a fallback when the deferred provider task exits without producing
/// a real table (e.g. during shutdown or after a task panic), so queries fail
/// explicitly rather than silently returning zero rows.
struct UnavailableTableProvider {
    schema: SchemaRef,
    dataset_name: String,
}

impl UnavailableTableProvider {
    fn new(schema: SchemaRef, dataset_name: String) -> Self {
        Self {
            schema,
            dataset_name,
        }
    }
}

impl std::fmt::Debug for UnavailableTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnavailableTableProvider").finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for UnavailableTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(DataFusionError::Execution(format!(
            "Data source unavailable for '{}': the connection to the federated source could not be established. The runtime may be shutting down or the source is unreachable.",
            self.dataset_name
        )))
    }
}

#[derive(Debug)]
pub enum FederatedTable {
    // To optimize the common case where the table provider is available immediately.
    Immediate(Arc<dyn TableProvider>),

    // If the table provider is not available immediately, we wait for it to become
    // available and store it here.
    Deferred(DeferredTableProvider),
}

#[derive(Debug)]
enum DeferredState {
    Waiting(oneshot::Receiver<Arc<dyn TableProvider>>),
    InProgress,
    Done,
}

#[derive(Debug)]
pub struct DeferredTableProvider {
    state: RwLock<DeferredState>,
    table: OnceLock<Arc<dyn TableProvider>>,
    /// True when the deferred task failed to produce a real provider (e.g. was
    /// cancelled during shutdown or panicked) and [`FederatedTable::table_provider`]
    /// returned an [`UnavailableTableProvider`] as a fallback. Readiness paths can
    /// use [`FederatedTable::try_wait_table_provider`] to distinguish this case
    /// from a successful schema resolution without downcasting to the private
    /// fallback provider type.
    resolved_unavailable: AtomicBool,
    schema: SchemaRef,
    dataset_name: String,
    /// Set when `on_schema_change: fail` deferred this provider because of a
    /// detected source schema change; holds the actionable message that the
    /// registration path surfaces as the dataset's error status.
    schema_change_failure: Option<String>,
}

/// Indicates why [`FederatedTable::try_wait_table_provider`] could not return a
/// successfully resolved federated [`TableProvider`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FederatedResolutionError {
    /// The deferred task finished without producing a real provider (cancelled
    /// during shutdown or panicked). Callers should treat this as "resolution
    /// did not succeed" rather than a live schema-resolved state.
    Unavailable,
}

impl DeferredTableProvider {
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl FederatedTable {
    /// Creates a federated table without checking if the schema matches the existing acceleration checkpoint.
    pub fn new_unchecked(table_provider: Arc<dyn TableProvider>) -> Self {
        Self::Immediate(table_provider)
    }

    /// Creates a federated table, and first checks if the schema matches the existing acceleration checkpoint.
    pub async fn new(
        dataset: Arc<Dataset>,
        table_provider: Arc<dyn TableProvider>,
        data_connector: Arc<dyn DataConnector>,
        shutdown_token: CancellationToken,
        allow_schema_mismatch: bool,
    ) -> Self {
        let table_provider = table_provider_with_spicepod_metadata(
            table_provider,
            &dataset.metadata,
            &dataset.columns,
        );

        // When `allow_schema_mismatch` is `true`, schema differences are ignored and the
        // table provider is returned immediately. The caller is responsible for handling
        // schema evolution (e.g. `file_update` mode detects changes and recreates the acceleration).
        if allow_schema_mismatch {
            return Self::new_unchecked(table_provider);
        }

        let Some(checkpoint) = Self::get_checkpoint(Arc::clone(&dataset)).await else {
            // Either this is not an accelerated table or the checkpoint does not exist.
            return Self::new_unchecked(table_provider);
        };
        let Ok(Some(accelerated_schema)) = checkpoint.get_schema().await else {
            // The checkpoint exists but the schema is not available.
            return Self::new_unchecked(table_provider);
        };

        let federated_schema = table_provider.schema();

        if schema_difference(&accelerated_schema, &federated_schema).is_none() {
            return Self::new_unchecked(table_provider);
        }

        if dataset.on_schema_change == OnSchemaChange::Block {
            // `on_schema_change: block` (default): today's behavior verbatim — defer
            // with the checkpoint schema, retrying the source until it matches.
            return Self::Deferred(Self::new_deferred_with_schema(
                Arc::clone(&dataset),
                data_connector,
                accelerated_schema,
                shutdown_token,
            ));
        }

        Self::new_with_schema_change_policy(
            dataset,
            table_provider,
            data_connector,
            shutdown_token,
            accelerated_schema,
        )
    }

    /// Applies the dataset's non-`block` `on_schema_change` policy to a detected
    /// difference between the acceleration checkpoint schema and the source schema.
    ///
    /// A widening change inside the policy's evolution set registers Immediately
    /// with the new source provider — a Deferred provider would report the OLD
    /// schema and hide the change from the registration path that applies the
    /// engine evolution. Everything else is block-equivalent (Deferred + loud
    /// warn), and `fail` additionally records the actionable message for the
    /// dataset's error status while keeping the deferred retry so a source
    /// revert self-heals.
    fn new_with_schema_change_policy(
        dataset: Arc<Dataset>,
        table_provider: Arc<dyn TableProvider>,
        data_connector: Arc<dyn DataConnector>,
        shutdown_token: CancellationToken,
        accelerated_schema: SchemaRef,
    ) -> Self {
        let policy = dataset.on_schema_change;
        let federated_schema = table_provider.schema();
        let constraint_columns =
            dataset_constraint_columns(&dataset, table_provider.constraints(), &federated_schema);
        let ctx = EvolutionContext {
            constraint_columns: &constraint_columns,
        };
        let dataset_name = dataset.name.to_string();
        let acceleration = dataset.acceleration.as_ref();
        let refresh_mode =
            data_connector.resolve_refresh_mode(acceleration.and_then(|a| a.refresh_mode));
        let engine = acceleration.map(|a| a.engine);

        match schema_evolution::classify(&accelerated_schema, &federated_schema, &ctx) {
            SchemaEvolution::Identical => Self::new_unchecked(table_provider),
            SchemaEvolution::Widening(plan) => {
                let kind = widening_plan_kind(&plan);
                let change = plan.describe();
                if !evolution_allowed(&policy, &plan) {
                    if policy == OnSchemaChange::Fail {
                        return Self::deferred_schema_change_failure(
                            dataset,
                            data_connector,
                            accelerated_schema,
                            shutdown_token,
                            kind,
                            &change,
                        );
                    }
                    SCHEMA_EVOLUTION_DETECTED
                        .add(1, &schema_evolution_labels(&dataset_name, kind, "startup"));
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset_name, kind, "blocked_by_policy"),
                    );
                    tracing::warn!(
                        dataset = %dataset.name,
                        "Schema change detected ({change}), but `on_schema_change: {policy}` only evolves added columns. Serving the existing acceleration and retrying the source; revert the change or set `on_schema_change: sync_all_columns` to evolve it",
                    );
                    return Self::Deferred(Self::new_deferred_with_schema(
                        Arc::clone(&dataset),
                        data_connector,
                        accelerated_schema,
                        shutdown_token,
                    ));
                }
                if refresh_mode == RefreshMode::Caching {
                    SCHEMA_EVOLUTION_DETECTED
                        .add(1, &schema_evolution_labels(&dataset_name, kind, "startup"));
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset_name, kind, "caching_mode"),
                    );
                    tracing::warn!(
                        dataset = %dataset.name,
                        "Schema change detected ({change}), but `refresh_mode: caching` does not support in-place schema evolution. Serving the existing acceleration; delete the acceleration data to adopt the new schema",
                    );
                    return Self::Deferred(Self::new_deferred_with_schema(
                        Arc::clone(&dataset),
                        data_connector,
                        accelerated_schema,
                        shutdown_token,
                    ));
                }
                if !engine.is_some_and(engine_supports_in_place_evolution) {
                    SCHEMA_EVOLUTION_DETECTED
                        .add(1, &schema_evolution_labels(&dataset_name, kind, "startup"));
                    SCHEMA_EVOLUTION_FAILED.add(
                        1,
                        &schema_evolution_labels(&dataset_name, kind, "engine_unsupported"),
                    );
                    tracing::warn!(
                        dataset = %dataset.name,
                        "Schema change detected ({change}), but the '{engine}' acceleration engine does not support in-place schema evolution. Serving the existing acceleration and retrying the source until its schema matches",
                        engine = engine.map(|e| e.to_string()).unwrap_or_default(),
                    );
                    return Self::Deferred(Self::new_deferred_with_schema(
                        Arc::clone(&dataset),
                        data_connector,
                        accelerated_schema,
                        shutdown_token,
                    ));
                }
                // Detection + applied metrics for this path are emitted by
                // `handle_schema_difference`, which re-classifies with the same
                // classifier and applies the engine evolution at registration.
                tracing::info!(
                    dataset = %dataset.name,
                    "Widening schema change detected ({change}); registering with the new source schema for in-place evolution",
                );
                Self::new_unchecked(table_provider)
            }
            SchemaEvolution::Incompatible { reason } => {
                if policy == OnSchemaChange::Fail {
                    return Self::deferred_schema_change_failure(
                        dataset,
                        data_connector,
                        accelerated_schema,
                        shutdown_token,
                        "incompatible",
                        &reason,
                    );
                }
                SCHEMA_EVOLUTION_DETECTED.add(
                    1,
                    &schema_evolution_labels(&dataset_name, "incompatible", "startup"),
                );
                SCHEMA_EVOLUTION_FAILED.add(
                    1,
                    &schema_evolution_labels(&dataset_name, "incompatible", "incompatible"),
                );
                tracing::warn!(
                    dataset = %dataset.name,
                    "Schema change detected that cannot be evolved under `on_schema_change: {policy}`: {reason}. Serving the existing acceleration and retrying the source; revert the source schema change to recover",
                );
                Self::Deferred(Self::new_deferred_with_schema(
                    Arc::clone(&dataset),
                    data_connector,
                    accelerated_schema,
                    shutdown_token,
                ))
            }
        }
    }

    /// `on_schema_change: fail`: defer with the checkpoint schema (so a source
    /// revert self-heals through the retry loop) and record the actionable
    /// message for the registration path to surface as the dataset status.
    fn deferred_schema_change_failure(
        dataset: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        accelerated_schema: SchemaRef,
        shutdown_token: CancellationToken,
        kind: &'static str,
        change: &str,
    ) -> Self {
        let dataset_name = dataset.name.to_string();
        SCHEMA_EVOLUTION_DETECTED.add(1, &schema_evolution_labels(&dataset_name, kind, "startup"));
        SCHEMA_EVOLUTION_FAILED.add(
            1,
            &schema_evolution_labels(&dataset_name, kind, "fail_policy"),
        );
        let message = format!(
            "A schema change was detected for {dataset_name} ({change}), and `on_schema_change: fail` is set. The existing acceleration continues to serve the previous schema. Revert the source schema change to recover, or set `on_schema_change` to `append_new_columns` or `sync_all_columns` to evolve the schema."
        );
        tracing::error!(dataset = %dataset.name, "{message}");
        let mut deferred = Self::new_deferred_with_schema(
            dataset,
            data_connector,
            accelerated_schema,
            shutdown_token,
        );
        deferred.schema_change_failure = Some(message);
        Self::Deferred(deferred)
    }

    /// When `on_schema_change: fail` deferred the provider because of a detected
    /// source schema change, returns the actionable message so the registration
    /// path can surface it as the dataset's error status.
    #[must_use]
    pub fn schema_change_failure(&self) -> Option<&str> {
        match self {
            Self::Immediate(_) => None,
            Self::Deferred(deferred) => deferred.schema_change_failure.as_deref(),
        }
    }

    /// If the table provider is not available immediately and this is an accelerated table with a previous acceleration checkpoint,
    /// we can create a deferred task to keep trying to connect to the table provider until it is available.
    ///
    /// Returns `None` if the dataset isn't a valid file-accelerated dataset.
    pub async fn new_deferred(
        dataset: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        shutdown_token: CancellationToken,
    ) -> Option<Self> {
        let checkpoint = Self::get_checkpoint(Arc::clone(&dataset)).await?;
        let accelerated_schema = checkpoint.get_schema().await.ok()??;

        Some(Self::Deferred(Self::new_deferred_with_schema(
            dataset,
            data_connector,
            accelerated_schema,
            shutdown_token,
        )))
    }

    /// Attempts to return the [`TableProvider`] without waiting for a deferred [`TableProvider`] that is not done (i.e. not in `DeferredState::Done`).
    ///
    /// Returns None if
    ///   1. Active write on the [`DeferredTableProvider`]'s state.
    ///   2. The [`DeferredTableProvider`] is not Ready.
    pub fn try_table_provider_sync(&self) -> Option<Arc<dyn TableProvider>> {
        Some(Arc::clone(self.try_table_provider_sync_ref()?))
    }

    /// Attempts to return the [`TableProvider`] without waiting for a deferred [`TableProvider`] that is not done (i.e. not in `DeferredState::Done`).
    ///
    /// Returns None if
    ///   1. Active write on the [`DeferredTableProvider`]'s state.
    ///   2. The [`DeferredTableProvider`] is not Ready.
    pub fn try_table_provider_sync_ref(&self) -> Option<&Arc<dyn TableProvider>> {
        let deferred_table_provider = match self {
            Self::Immediate(table_provider) => return Some(table_provider),
            Self::Deferred(deferred_table_provider) => deferred_table_provider,
        };

        deferred_table_provider.table.get()
    }

    pub async fn table_provider(&self) -> Arc<dyn TableProvider> {
        match self.try_wait_table_provider().await {
            Ok(table_provider) | Err((FederatedResolutionError::Unavailable, table_provider)) => {
                table_provider
            }
        }
    }

    /// Resolves the federated [`TableProvider`], distinguishing successful
    /// resolution from the fallback case where the deferred task did not
    /// produce a real provider.
    ///
    /// For [`FederatedTable::Immediate`] this always returns `Ok`. For
    /// [`FederatedTable::Deferred`] it awaits the deferred task and returns
    /// `Ok` only if a real [`TableProvider`] was produced; otherwise it
    /// returns `Err((FederatedResolutionError::Unavailable, provider))`,
    /// where `provider` is the fallback that errors on scan. The fallback
    /// provider is still returned in the `Err` variant so callers that just
    /// want a provider (e.g. query scan paths) can fall through to it.
    pub async fn try_wait_table_provider(
        &self,
    ) -> Result<Arc<dyn TableProvider>, (FederatedResolutionError, Arc<dyn TableProvider>)> {
        let deferred_table_provider = match self {
            Self::Immediate(table_provider) => return Ok(Arc::clone(table_provider)),
            Self::Deferred(deferred_table_provider) => deferred_table_provider,
        };

        // If the table provider is available now, return it (respecting any prior fallback).
        if let Some(table_provider) = deferred_table_provider.table.get() {
            let provider = Arc::clone(table_provider);
            return if deferred_table_provider
                .resolved_unavailable
                .load(Ordering::Acquire)
            {
                Err((FederatedResolutionError::Unavailable, provider))
            } else {
                Ok(provider)
            };
        }

        // If the table provider is not available immediately, see if we already have it from the deferred task.
        let mut deferred_state_guard = deferred_table_provider.state.write().await;

        // We need to own the deferred state to be able to wait on the receiver. Temporarily replace it with InProgress.
        let deferred_state_owned =
            std::mem::replace(&mut *deferred_state_guard, DeferredState::InProgress);

        // The only valid state at this point is Waiting, we've already checked Done above and we always set the state back to Done before exiting.
        match deferred_state_owned {
            DeferredState::Waiting(rx) => {
                if let Ok(table_provider) = rx.await {
                    let _ = deferred_table_provider
                        .table
                        .set(Arc::clone(&table_provider));
                    *deferred_state_guard = DeferredState::Done;
                    Ok(table_provider)
                } else {
                    // The deferred task was cancelled (e.g. during shutdown) or panicked
                    // without sending a provider. Return a provider that errors on scan
                    // so queries fail explicitly instead of silently returning zero rows.
                    let unavailable: Arc<dyn TableProvider> =
                        Arc::new(UnavailableTableProvider::new(
                            deferred_table_provider.schema(),
                            deferred_table_provider.dataset_name.clone(),
                        ));
                    let _ = deferred_table_provider.table.set(Arc::clone(&unavailable));
                    deferred_table_provider
                        .resolved_unavailable
                        .store(true, Ordering::Release);
                    *deferred_state_guard = DeferredState::Done;
                    Err((FederatedResolutionError::Unavailable, unavailable))
                }
            }
            DeferredState::InProgress | DeferredState::Done => {
                unreachable!("deferred state should only be Waiting at this point");
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Immediate(table_provider) => table_provider.schema(),
            Self::Deferred(deferred_table_provider) => Arc::clone(&deferred_table_provider.schema),
        }
    }

    fn new_deferred_with_schema(
        dataset: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
        schema: SchemaRef,
        shutdown_token: CancellationToken,
    ) -> DeferredTableProvider {
        let dataset_name = dataset.name.clone();
        let dataset_name_str = dataset_name.to_string();
        let accelerated_schema = Arc::clone(&schema);

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

            let tracer = OnceTracer::new();
            let data_connector = Arc::clone(&data_connector);
            let retry_fut = retry(retry_strategy, || async {
                match data_connector.read_provider(&dataset).await {
                    Ok(table_provider) => {
                        let federated_schema = table_provider.schema();

                        if let Some(differences) =
                            schema_difference(&accelerated_schema, &federated_schema)
                        {
                            let error = DataConnectorError::SchemaMismatch {
                                dataset_name: dataset_name.to_string(),
                                differences,
                            };
                            warn_once!(tracer, "{}", error);
                            return Err(RetryError::transient(error));
                        }

                        Ok(table_provider)
                    }
                    Err(e) => Err(RetryError::transient(e)),
                }
            });

            // Use tokio::select! so that the retry loop is interrupted immediately
            // when the runtime begins shutting down (e.g. on Ctrl-C).
            let table_provider_result = tokio::select! {
                result = retry_fut => result,
                () = shutdown_token.cancelled() => {
                    tracing::debug!("Deferred table provider for '{}' cancelled due to shutdown.", dataset.name);
                    return;
                }
            };

            match table_provider_result {
                Ok(table_provider) => {
                    let table_provider = table_provider_with_spicepod_metadata(
                        table_provider,
                        &dataset.metadata,
                        &dataset.columns,
                    );
                    if tx.send(table_provider).is_err() {
                        tracing::error!(
                            "Failed to send deferred table provider for dataset '{}': Channel closed.",
                            dataset.name,
                        );
                    }
                    tracing::info!("Connection to source re-established for {dataset_name}.");
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to connect to table provider for dataset '{}': {e}",
                        dataset.name,
                    );
                }
            }
        });

        DeferredTableProvider {
            state: RwLock::new(DeferredState::Waiting(rx)),
            schema,
            table: OnceLock::new(),
            resolved_unavailable: AtomicBool::new(false),
            dataset_name: dataset_name_str,
            schema_change_failure: None,
        }
    }

    async fn get_checkpoint(dataset: Arc<Dataset>) -> Option<Arc<dyn DatasetCheckpointer>> {
        if !dataset.is_file_accelerated() {
            return None;
        }

        let checkpoint = DatasetCheckpoint::try_new(dataset.as_ref(), OpenOption::OpenExisting)
            .await
            .ok()?;
        Some(checkpoint.to_arc())
    }
}
