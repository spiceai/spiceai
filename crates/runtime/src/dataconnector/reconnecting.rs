/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A connector for a dataset whose source could not be reached when the dataset
//! loaded, but whose existing acceleration can serve queries in the meantime.
//!
//! Many connectors contact their source while being constructed (to validate
//! credentials, or to detect the server's variant), so an unreachable source
//! leaves no connector to register the dataset with, even though the
//! acceleration already holds its data. [`ReconnectingConnector`] stands in for
//! that connector: it builds the real one on first use — which, for a dataset
//! served from its acceleration, is the deferred federated provider retrying
//! [`DataConnector::read_provider`] in the background — and forwards every call
//! to it from then on.
//!
//! Before the real connector exists, each synchronous capability method answers
//! with the trait's default, and the registration hooks do nothing. That is only
//! correct for the datasets `Runtime::serves_existing_acceleration` admits.

use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider, execution::runtime_env::RuntimeEnv, sql::TableReference,
};
use futures::future::BoxFuture;
use tokio::sync::OnceCell;

use super::{ConnectorComponent, ConnectorContext, DataConnector, DataConnectorError};
use crate::component::ComponentInitialization;
use crate::component::dataset::DatasetSpec;
use crate::component::dataset::acceleration::RefreshMode;

/// Builds the real connector. Called again after each failure, until one succeeds.
pub type ConnectorBuilder = Arc<dyn Fn() -> ConnectorAttempt + Send + Sync>;

/// One attempt to build the real connector.
pub type ConnectorAttempt = BoxFuture<'static, crate::Result<Arc<dyn DataConnector>>>;

/// How long loading a dataset waits for its source before serving an existing
/// acceleration instead. A responsive source answers well within this, and keeps
/// the dataset on the path that registers with the live source provider; a slow or
/// unresponsive one no longer holds a dataset that already has its data locally
/// unregistered.
pub const SOURCE_WAIT_BEFORE_SERVING_ACCELERATION: Duration = Duration::from_secs(2);

/// Why a dataset is registered before its real connector exists.
#[derive(Debug, Clone)]
pub enum SourceUnavailable {
    /// Building the connector failed with a retriable error.
    Failed(String),
    /// The source did not respond within [`SOURCE_WAIT_BEFORE_SERVING_ACCELERATION`].
    Slow,
    /// The source was not contacted: a deferred dataset (`ready_state:
    /// on_registration` with typed `columns:`) serves its first query from its
    /// existing acceleration and connects in the background.
    NotContacted,
}

impl SourceUnavailable {
    /// Logs why `dataset` is served from its existing acceleration: a source that
    /// could not be reached is a warning; a slow source, or one not contacted yet,
    /// resolves on its own and is informational.
    pub fn log_serving_from_acceleration(&self, dataset: &TableReference) {
        match self {
            Self::Failed(cause) => {
                tracing::warn!("{}", unreachable_source_warning(dataset, cause));
            }
            Self::Slow => tracing::info!("{}", slow_source_message(dataset)),
            Self::NotContacted => tracing::info!("{}", not_contacted_message(dataset)),
        }
    }
}

/// The warning logged when a dataset's source could not be reached, so queries are
/// served from its existing acceleration while the connection is retried.
#[must_use]
fn unreachable_source_warning(dataset: &TableReference, cause: &str) -> String {
    format!(
        "Failed to connect to the source for dataset {dataset}. Serving data from the existing acceleration for {dataset} while retrying the connection. {cause}"
    )
}

/// The message logged when a dataset's source did not respond within
/// [`SOURCE_WAIT_BEFORE_SERVING_ACCELERATION`], so queries are served from its
/// existing acceleration until it does.
#[must_use]
fn slow_source_message(dataset: &TableReference) -> String {
    format!(
        "The source for dataset '{dataset}' did not respond within {}s, so queries are served from the existing acceleration for '{dataset}' until the source responds and the next refresh completes.",
        SOURCE_WAIT_BEFORE_SERVING_ACCELERATION.as_secs()
    )
}

/// The message logged when a deferred dataset serves its first query from its
/// existing acceleration while it connects to the source in the background.
#[must_use]
fn not_contacted_message(dataset: &TableReference) -> String {
    format!(
        "Dataset '{dataset}' is served from its existing acceleration while it connects to its source in the background."
    )
}

pub struct ReconnectingConnector {
    /// The connector name from the dataset's `from:`, for errors.
    source_name: String,
    build: ConnectorBuilder,
    /// A build already in progress when the dataset loaded, awaited before `build`
    /// is called, so a slow source is not connected to twice.
    first_attempt: parking_lot::Mutex<Option<ConnectorAttempt>>,
    unavailable: SourceUnavailable,
    inner: OnceCell<Arc<dyn DataConnector>>,
    /// Object stores the runtime asked to register before the real connector existed,
    /// replayed once it is built.
    pending_object_stores: parking_lot::Mutex<Vec<(DatasetSpec, Arc<RuntimeEnv>)>>,
}

impl ReconnectingConnector {
    #[must_use]
    pub fn new(
        source_name: impl Into<String>,
        build: ConnectorBuilder,
        first_attempt: Option<ConnectorAttempt>,
        unavailable: SourceUnavailable,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            build,
            first_attempt: parking_lot::Mutex::new(first_attempt),
            unavailable,
            inner: OnceCell::new(),
            pending_object_stores: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Why the real connector was not built when the dataset loaded, until it has
    /// been built.
    #[must_use]
    pub fn pending_reason(&self) -> Option<&SourceUnavailable> {
        (!self.inner.initialized()).then_some(&self.unavailable)
    }

    /// The real connector, once it has been built.
    fn built(&self) -> Option<&Arc<dyn DataConnector>> {
        self.inner.get()
    }

    /// The real connector, building it if it does not exist yet.
    async fn connector(
        &self,
        dataset: &DatasetSpec,
    ) -> Result<&Arc<dyn DataConnector>, DataConnectorError> {
        let connector = self
            .inner
            .get_or_try_init(|| {
                // Taken by whichever initialization runs first; if that one fails or
                // is dropped, the next builds afresh.
                let first_attempt = self.first_attempt.lock().take();
                first_attempt.unwrap_or_else(|| (self.build)())
            })
            .await
            .map_err(|err| self.build_error(dataset, err))?;

        let pending = std::mem::take(&mut *self.pending_object_stores.lock());
        for (spec, runtime_env) in pending {
            if let Err(err) = connector.register_object_stores(&spec, &runtime_env).await {
                tracing::warn!(
                    "Failed to register the object store for dataset '{}' ({}) after reconnecting to its source, so reading it from the source may fail. {err}",
                    spec.name,
                    self.source_name,
                );
            }
        }

        Ok(connector)
    }

    /// The connector's own error when construction failed with one, so the message
    /// matches what a failed load reports; otherwise the failure wrapped as a read failure.
    fn build_error(&self, dataset: &DatasetSpec, err: crate::Error) -> DataConnectorError {
        let source: Box<dyn std::error::Error + Send + Sync> = match err {
            crate::Error::UnableToInitializeDataConnector { source } => {
                match source.downcast::<DataConnectorError>() {
                    Ok(connector_error) => return *connector_error,
                    Err(source) => source,
                }
            }
            err => Box::new(err),
        };
        DataConnectorError::UnableToGetReadProvider {
            dataconnector: self.source_name.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source,
        }
    }
}

impl std::fmt::Debug for ReconnectingConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectingConnector")
            .field("source_name", &self.source_name)
            .field("connected", &self.inner.initialized())
            .finish_non_exhaustive()
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl DataConnector for ReconnectingConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        match self.built() {
            Some(inner) => inner.resolve_refresh_mode(refresh_mode),
            None => refresh_mode.unwrap_or(RefreshMode::Full),
        }
    }

    async fn read_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        self.connector(dataset)
            .await?
            .read_provider(context, dataset)
            .await
    }

    async fn read_write_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.connector(dataset).await {
            Ok(inner) => inner.read_write_provider(context, dataset).await,
            Err(err) => Some(Err(err)),
        }
    }

    fn supports_changes_stream(&self) -> bool {
        self.built()
            .is_some_and(|inner| inner.supports_changes_stream())
    }

    async fn changes_stream(
        &self,
        context: &dyn ConnectorContext,
        federated_table: Arc<dyn data_connector_api::federated::FederatedTableProvider>,
        dataset: &DatasetSpec,
        acceleration: data_components::cdc::AccelerationContents,
    ) -> Option<data_components::cdc::ChangesStream> {
        match self.built() {
            Some(inner) => {
                inner
                    .changes_stream(context, federated_table, dataset, acceleration)
                    .await
            }
            None => None,
        }
    }

    fn supports_append_stream(&self) -> bool {
        self.built()
            .is_some_and(|inner| inner.supports_append_stream())
    }

    fn append_stream(
        &self,
        federated_table: Arc<dyn data_connector_api::federated::FederatedTableProvider>,
    ) -> Option<data_components::cdc::ChangesStream> {
        self.built()
            .and_then(|inner| inner.append_stream(federated_table))
    }

    fn supports_durable_write_back_delivery(&self) -> bool {
        self.built()
            .is_some_and(|inner| inner.supports_durable_write_back_delivery())
    }

    async fn write_back_deliverer(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<
        data_connector_api::DataConnectorResult<Arc<dyn data_connector_api::WriteBackDeliverer>>,
    > {
        match self.built() {
            Some(inner) => inner.write_back_deliverer(context, dataset).await,
            None => None,
        }
    }

    async fn metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.built() {
            Some(inner) => inner.metadata_provider(dataset).await,
            None => None,
        }
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> super::DataConnectorResult<()> {
        if let Some(inner) = self.built() {
            inner.register_object_stores(dataset, runtime_env).await
        } else {
            self.pending_object_stores
                .lock()
                .push((dataset.clone(), Arc::clone(runtime_env)));
            Ok(())
        }
    }

    async fn on_accelerator_setup(
        &self,
        dataset: &DatasetSpec,
        accelerator: &mut dyn data_connector_api::accelerated::AcceleratorSetup,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.built() {
            Some(inner) => inner.on_accelerator_setup(dataset, accelerator).await,
            None => Ok(()),
        }
    }

    async fn on_accelerated_table_registration(
        &self,
        dataset: &DatasetSpec,
        accelerated_table: &mut dyn data_connector_api::accelerated::RegisteredAcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.built() {
            Some(inner) => {
                inner
                    .on_accelerated_table_registration(dataset, accelerated_table)
                    .await
            }
            None => Ok(()),
        }
    }

    fn metrics_provider(&self) -> Option<Arc<dyn runtime_metrics::component::MetricsProvider>> {
        self.built().and_then(|inner| inner.metrics_provider())
    }

    fn initialization(&self) -> ComponentInitialization {
        self.built()
            .map_or_else(ComponentInitialization::default, |inner| {
                inner.initialization()
            })
    }

    fn initialization_for_dataset(&self, dataset: &DatasetSpec) -> ComponentInitialization {
        self.built()
            .map_or_else(ComponentInitialization::default, |inner| {
                inner.initialization_for_dataset(dataset)
            })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::TableReference;

    use super::{not_contacted_message, slow_source_message, unreachable_source_warning};

    #[test]
    fn unreachable_source_warning_names_the_dataset_and_the_cause() {
        let warning = unreachable_source_warning(
            &TableReference::bare("orders"),
            "Cannot connect to the dataset orders (postgres) on db:5432.",
        );
        assert_eq!(
            warning,
            "Failed to connect to the source for dataset orders. Serving data from the existing acceleration for orders while retrying the connection. Cannot connect to the dataset orders (postgres) on db:5432."
        );
    }

    #[test]
    fn slow_source_message_names_the_dataset_the_wait_and_what_is_served() {
        let message = slow_source_message(&TableReference::bare("orders"));
        assert_eq!(
            message,
            "The source for dataset 'orders' did not respond within 2s, so queries are served from the existing acceleration for 'orders' until the source responds and the next refresh completes."
        );
    }

    #[test]
    fn not_contacted_message_names_the_dataset_and_what_is_served() {
        assert_eq!(
            not_contacted_message(&TableReference::bare("orders")),
            "Dataset 'orders' is served from its existing acceleration while it connects to its source in the background."
        );
    }
}
