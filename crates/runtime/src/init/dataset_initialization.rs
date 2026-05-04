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
//! full bring-up: building the connector (if needed), resolving the
//! schema, registering the dataset with `DataFusion`, registering with
//! the health monitor, and starting any accelerated table refresh.
//!
//! Today only the eager flow is supported:
//!
//!   * [`ConnectorSource::Eager`] — connector is already built before
//!     `DatasetInitialization::plan_eager` is called. This matches the
//!     existing dataset bring-up path one-to-one.
//!   * [`SchemaSource::FromProvider`] — `initialize` will call
//!     `DataConnector::read_provider` to obtain the schema.
//!   * `should_initialize_at_startup` always returns `true`.
//!
//! `initialize` currently delegates to `Runtime::register_loaded_dataset`
//! so behavior matches the prior call sites exactly.

use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::component::dataset::Dataset;
use crate::dataaccelerator::BootstrapStatus;
use crate::dataconnector::DataConnector;
use crate::{Result, Runtime, accelerated_table::AcceleratedTable};

/// Where the `DataConnector` for this dataset comes from.
pub(crate) enum ConnectorSource {
    /// Connector is already constructed. This matches today's bring-up
    /// path (`Runtime::load_dataset_connector` runs first, then
    /// `register_loaded_dataset` is called with the resolved connector)
    /// and the hot-reload path (which also has the connector in hand by
    /// the time `register_loaded_dataset` runs).
    Eager(Arc<dyn DataConnector>),
}

/// How the dataset's schema is resolved.
pub(crate) enum SchemaSource {
    /// Schema is unknown at planning time; `initialize` will call
    /// `DataConnector::read_provider` and use the provider's schema.
    FromProvider,
}

/// Pure-data plan for materializing one dataset.
///
/// Construction is I/O-free. All side effects (connector construction,
/// `read_provider`, catalog registration, refresh-task spawning, health
/// monitor registration) happen inside [`DatasetInitialization::initialize`].
pub(crate) struct DatasetInitialization {
    dataset: Arc<Dataset>,
    runtime: Arc<Runtime>,
    connector_source: ConnectorSource,
    schema_source: SchemaSource,
    bootstrap_status: BootstrapStatus,
    load_semaphore: Option<Arc<Semaphore>>,
    /// Set on the hot-reload path where an `AcceleratedTable` instance
    /// has already been constructed and should be reused instead of
    /// being rebuilt by `register_loaded_dataset`.
    preloaded_accelerated_table: Option<Arc<AcceleratedTable>>,
}

impl DatasetInitialization {
    /// Build an eager initialization plan for a dataset whose connector
    /// has already been constructed.
    ///
    /// I/O-free.
    pub(crate) fn plan_eager(
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

    /// Synchronous accessor: the dataset this plan will initialize.
    #[expect(dead_code)] // Reserved for upcoming deferred-initialization callers.
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// True iff this plan must run at startup. False iff it can be
    /// deferred to first reference.
    ///
    /// Currently always `true`; will return `false` for deferred
    /// initialization once additional `ConnectorSource` / `SchemaSource`
    /// variants are introduced.
    #[expect(dead_code)] // Reserved for upcoming deferred-initialization callers.
    pub(crate) fn should_initialize_at_startup(&self) -> bool {
        match (&self.connector_source, &self.schema_source) {
            (ConnectorSource::Eager(_), SchemaSource::FromProvider) => true,
        }
    }

    /// Consume the plan and run the dataset bring-up to completion.
    ///
    /// Currently delegates to `Runtime::register_loaded_dataset` so
    /// behavior matches the prior bring-up path exactly.
    pub(crate) async fn initialize(self) -> Result<()> {
        let Self {
            dataset,
            runtime,
            connector_source,
            schema_source,
            bootstrap_status,
            load_semaphore,
            preloaded_accelerated_table,
        } = self;

        let ConnectorSource::Eager(connector) = connector_source;
        let SchemaSource::FromProvider = schema_source;

        runtime
            .register_loaded_dataset(
                dataset,
                connector,
                preloaded_accelerated_table,
                bootstrap_status,
                load_semaphore,
            )
            .await
    }
}
