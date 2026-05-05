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

//! Dataset initialization value type.
//!
//! `DatasetInitialization` is a pure-data planning value that captures
//! everything needed to materialize one dataset, with no I/O at
//! construction. Calling [`DatasetInitialization::initialize`] runs the
//! full bring-up.
//!
//! ## Connector / schema sources
//!
//! * [`ConnectorSource::Eager`] — connector already constructed at
//!   plan time. Used for the startup path, hot-reload, and any case
//!   where the trigger condition for deferral is not met.
//! * [`ConnectorSource::Lazy`] — connector built on first reference,
//!   inside `initialize`. Captures the factory + resolved params via a
//!   parked closure so plan construction stays I/O-free.
//! * [`SchemaSource::FromProvider`] — schema is inferred from the
//!   constructed connector via `read_provider`.
//! * [`SchemaSource::Known`] — schema is determined entirely from
//!   configuration (user-declared `columns[]` or
//!   `DataConnectorFactory::static_schema`). `initialize` skips the
//!   `read_provider` schema-inference step.
//!
//! `should_initialize_at_startup` returns `false` only when both
//! `ConnectorSource::Lazy` and `SchemaSource::Known` are picked, which
//! is the deferred path: a placeholder is registered up-front and the
//! real provider is materialized on first reference.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use futures::future::BoxFuture;
use tokio::sync::Semaphore;

use crate::Error as RuntimeError;
use crate::component::dataset::Dataset;
use crate::dataaccelerator::BootstrapStatus;
use crate::dataconnector::{DataConnector, NewDataConnectorResult};
use crate::{Result, Runtime, accelerated_table::AcceleratedTable};

/// Where the `DataConnector` for this dataset comes from.
pub enum ConnectorSource {
    /// Connector is already constructed. Used for the eager startup
    /// path, the hot-reload path, and any case where the trigger
    /// condition for deferral is not met.
    Eager(Arc<dyn DataConnector>),

    /// Connector built lazily by `initialize`. The closure captures the
    /// factory plus resolved `ConnectorParams` so plan construction
    /// stays I/O-free; it must construct the connector and any
    /// post-construction wrappers (e.g. `EmbeddingConnector`,
    /// `FullTextConnector`).
    Lazy(LazyConnectorBuilder),
}

pub type LazyConnectorBuilder =
    Box<dyn FnOnce() -> BoxFuture<'static, NewDataConnectorResult> + Send + Sync>;

/// How the dataset's schema is resolved.
pub enum SchemaSource {
    /// Schema is unknown at planning time; `initialize` will call
    /// `DataConnector::read_provider` and use the provider's schema.
    FromProvider,

    /// Schema is known from configuration. `initialize` will not call
    /// `read_provider` for schema purposes; it will validate the real
    /// provider's schema matches on first scan.
    Known { schema: SchemaRef },
}

/// Outcome of a successful [`DatasetInitialization::initialize`] call.
///
/// `table_provider` is `Some` for the lazy-known path (which builds
/// the provider but does not register it; the caller — typically
/// `DatasetTableProvider::ensure_ready` — performs the catalog swap).
/// For the eager path, `initialize` still delegates to
/// `Runtime::register_loaded_dataset` which registers the provider as
/// a side effect, and `table_provider` is left `None`.
#[derive(Default)]
pub struct DatasetReady {
    pub table_provider: Option<Arc<dyn TableProvider>>,
    pub accelerated_table: Option<Arc<AcceleratedTable>>,
    pub data_connector: Option<Arc<dyn DataConnector>>,
}

impl std::fmt::Debug for DatasetReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetReady")
            .field("has_table_provider", &self.table_provider.is_some())
            .field("has_accelerated_table", &self.accelerated_table.is_some())
            .field("has_data_connector", &self.data_connector.is_some())
            .finish()
    }
}

/// Pure-data plan for materializing one dataset.
///
/// Construction is I/O-free. All side effects (connector construction,
/// `read_provider`, catalog registration, refresh-task spawning, health
/// monitor registration) happen inside [`DatasetInitialization::initialize`].
pub struct DatasetInitialization {
    dataset: Arc<Dataset>,
    runtime: Arc<Runtime>,
    connector_source: ConnectorSource,
    schema_source: SchemaSource,
    bootstrap_status: BootstrapStatus,
    load_semaphore: Option<Arc<Semaphore>>,
    preloaded_accelerated_table: Option<Arc<AcceleratedTable>>,
}

impl DatasetInitialization {
    /// Build an eager initialization plan for a dataset whose connector
    /// has already been constructed. Used by the hot-reload path and as
    /// a fallback when deferral preconditions are not met.
    pub fn plan_eager(
        dataset: Arc<Dataset>,
        runtime: Arc<Runtime>,
        connector: Arc<dyn DataConnector>,
        bootstrap_status: BootstrapStatus,
        load_semaphore: Option<Arc<Semaphore>>,
        preloaded_accelerated_table: Option<Arc<AcceleratedTable>>,
    ) -> Self {
        Self {
            dataset,
            runtime,
            connector_source: ConnectorSource::Eager(connector),
            schema_source: SchemaSource::FromProvider,
            bootstrap_status,
            load_semaphore,
            preloaded_accelerated_table,
        }
    }

    /// Build a deferred initialization plan with a known schema and a
    /// lazily-constructed connector.
    ///
    /// I/O-free; the connector is built only inside `initialize`. The
    /// caller is responsible for ensuring `connector_builder` performs
    /// the same factory wrapping (`EmbeddingConnector`,
    /// `FullTextConnector`) that the eager path applies in
    /// `Runtime::load_dataset_connector`.
    pub fn plan_deferred(
        dataset: Arc<Dataset>,
        runtime: Arc<Runtime>,
        connector_builder: LazyConnectorBuilder,
        schema: SchemaRef,
        bootstrap_status: BootstrapStatus,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        Self {
            dataset,
            runtime,
            connector_source: ConnectorSource::Lazy(connector_builder),
            schema_source: SchemaSource::Known { schema },
            bootstrap_status,
            load_semaphore,
            preloaded_accelerated_table: None,
        }
    }

    /// Synchronous accessor: the dataset this plan will initialize.
    #[expect(dead_code)] // Reserved for upcoming deferred-initialization callers.
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// True iff this plan must run at startup. False iff it can be
    /// deferred to first reference.
    #[must_use]
    pub fn should_initialize_at_startup(&self) -> bool {
        !matches!(
            (&self.connector_source, &self.schema_source),
            (ConnectorSource::Lazy(_), SchemaSource::Known { .. })
        )
    }

    /// Consume the plan and run the dataset bring-up to completion.
    pub async fn initialize(self) -> Result<DatasetReady> {
        let Self {
            dataset,
            runtime,
            connector_source,
            schema_source,
            bootstrap_status,
            load_semaphore,
            preloaded_accelerated_table,
        } = self;

        match (connector_source, schema_source) {
            (ConnectorSource::Eager(connector), SchemaSource::FromProvider) => {
                Box::pin(runtime.register_loaded_dataset(
                    dataset,
                    connector,
                    preloaded_accelerated_table,
                    bootstrap_status,
                    load_semaphore,
                ))
                .await?;
                Ok(DatasetReady::default())
            }

            (ConnectorSource::Lazy(builder), SchemaSource::Known { schema }) => {
                let connector = builder()
                    .await
                    .map_err(|e| RuntimeError::UnableToInitializeDataConnector { source: e })?;

                // Accelerated deferred datasets: hand off to
                // the eager bring-up path; it constructs the
                // AcceleratedTable, registers metrics + health
                // monitor, and starts refresh. The placeholder must be
                // deregistered first so the eager path's
                // `ctx.register_table` slot is free.
                if dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
                    // Register the real (accelerated) provider first.
                    // `register_loaded_dataset` calls `ctx.register_table`
                    // which overwrites the placeholder in place. Only
                    // after the real provider is registered do we drop
                    // the pending bookkeeping, so the placeholder —
                    // which the resolver hook can find via the pending
                    // registry — stays valid right up until the swap
                    // succeeds. If `register_loaded_dataset` fails, the
                    // placeholder remains in place and a retry can run.
                    Box::pin(Arc::clone(&runtime).register_loaded_dataset(
                        Arc::clone(&dataset),
                        Arc::clone(&connector),
                        None,
                        bootstrap_status,
                        load_semaphore,
                    ))
                    .await?;

                    runtime
                        .df
                        .complete_pending_initialization(&dataset.name)
                        .await;

                    return Ok(DatasetReady {
                        table_provider: None, // registration happened as side-effect
                        accelerated_table: None,
                        data_connector: Some(connector),
                    });
                }

                let federated_provider = connector.read_provider(&dataset).await.map_err(|e| {
                    RuntimeError::UnableToInitializeDataConnector {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }
                })?;

                // Validate that the resolved provider's schema matches
                // the declared/static schema. Mismatch is a config bug
                // and we fail fast rather than silently using the
                // provider's schema (which would diverge from what's
                // already in the catalog).
                if !arrow_schemas_equal(&schema, &federated_provider.schema()) {
                    return Err(RuntimeError::PermanentDatasetFailure {
                        dataset: dataset.name.clone(),
                        reason: format!(
                            "Declared schema does not match source schema for dataset `{}`. \
                             Declared: {:?}; source: {:?}",
                            dataset.name,
                            schema.fields(),
                            federated_provider.schema().fields(),
                        ),
                    });
                }

                Ok(DatasetReady {
                    table_provider: Some(federated_provider),
                    accelerated_table: None,
                    data_connector: Some(connector),
                })
            }

            // Other combinations (Eager + Known, Lazy + FromProvider)
            // are not reachable because plan_eager and plan_deferred
            // are the only constructors and they fix the pairing.
            _ => Err(RuntimeError::PermanentDatasetFailure {
                dataset: dataset.name.clone(),
                reason: "DatasetInitialization built with an unsupported (ConnectorSource, \
                         SchemaSource) pairing"
                    .to_string(),
            }),
        }
    }
}

fn arrow_schemas_equal(a: &SchemaRef, b: &SchemaRef) -> bool {
    a.fields() == b.fields()
}
