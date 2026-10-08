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

//! A connector for a dataset served from its existing acceleration before its
//! source has been contacted.
//!
//! Many connectors contact their source while being constructed (to validate
//! credentials, or to detect the server's variant), so building the connector
//! first would tie the dataset's startup to its source, even though the
//! acceleration already holds its data. [`ReconnectingConnector`] stands in for
//! that connector: it builds the real one on first use — which, for a dataset
//! served from its acceleration, is the deferred federated provider retrying
//! [`DataConnector::read_provider`] in the background — and forwards every call
//! to it from then on.
//!
//! Before the real connector exists, each synchronous capability method answers
//! with the trait's default, and the registration hooks do nothing. That is only
//! correct for the datasets `Runtime::serves_existing_acceleration` admits.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::runtime_env::RuntimeEnv,
};
use futures::future::BoxFuture;
use tokio::sync::{Mutex, OnceCell};

use super::{ConnectorComponent, ConnectorContext, DataConnector, DataConnectorError};
use crate::component::ComponentInitialization;
use crate::component::dataset::DatasetSpec;
use crate::component::dataset::acceleration::RefreshMode;

/// Builds the real connector. Called again after each failure, until one succeeds.
pub type ConnectorBuilder =
    Arc<dyn Fn() -> BoxFuture<'static, crate::Result<Arc<dyn DataConnector>>> + Send + Sync>;

/// Why a dataset is served from its existing acceleration before its source has
/// answered.
#[derive(Debug, Clone)]
pub enum SourceUnavailable {
    /// Reading the source failed, retaining whether its configuration must be fixed.
    Failed {
        cause: String,
        configuration_error: bool,
    },
    /// The source has not been contacted yet: the dataset is served from its
    /// acceleration and connects in the background.
    NotContacted,
}

impl SourceUnavailable {
    /// Retains the classification used by background source failure reporting.
    #[must_use]
    pub fn failed(error: &DataConnectorError) -> Self {
        Self::Failed {
            cause: error.to_string(),
            configuration_error: !error.is_retriable(),
        }
    }

    /// Logs why `dataset` is served from its existing acceleration: a transient
    /// failure is a warning, and a configuration failure is an error. A source not
    /// contacted yet logs nothing here; a failure is reported when it happens.
    pub fn log_serving_from_acceleration(&self, dataset: &TableReference) {
        if let Self::Failed {
            cause,
            configuration_error,
        } = self
        {
            if *configuration_error {
                tracing::error!(
                    "{}",
                    super::refresh_source::source_configuration_error(dataset, cause)
                );
            } else {
                tracing::warn!("{}", unreachable_source_warning(dataset, cause));
            }
        }
    }
}

/// The warning logged when a dataset's source could not be reached, so queries are
/// served from its existing acceleration while the connection is retried.
#[must_use]
pub(crate) fn unreachable_source_warning(dataset: &TableReference, cause: &str) -> String {
    format!(
        "Failed to connect to the source for dataset {dataset}. Serving data from the existing acceleration for {dataset} while retrying the connection. {cause}"
    )
}

pub struct ReconnectingConnector {
    /// The connector name from the dataset's `from:`, for errors.
    source_name: String,
    build: ConnectorBuilder,
    inner: OnceCell<Arc<dyn DataConnector>>,
    /// Object stores the runtime asked to register before the real connector existed,
    /// replayed after construction and retained until registration succeeds.
    pending_object_stores: Mutex<Vec<(DatasetSpec, Arc<RuntimeEnv>)>>,
}

impl ReconnectingConnector {
    #[must_use]
    pub fn new(source_name: impl Into<String>, build: ConnectorBuilder) -> Self {
        Self {
            source_name: source_name.into(),
            build,
            inner: OnceCell::new(),
            pending_object_stores: Mutex::new(Vec::new()),
        }
    }

    /// Whether the real connector has been built.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.initialized()
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
            .get_or_try_init(|| async {
                let connector = (self.build)().await?;
                // A source read triggers deferred initialization. Forward through
                // the connector behind the placeholder to verify source access.
                let connector = match connector
                    .as_any()
                    .downcast_ref::<super::deferred::DeferredConnector>()
                {
                    Some(deferred) => deferred.source(),
                    None => connector,
                };
                Ok::<_, crate::Error>(connector)
            })
            .await
            .map_err(|err| self.build_error(dataset, err))?;

        // Keep each registration queued while it is in flight, so an error or
        // cancellation leaves it available for the next source connection attempt.
        let mut pending = self.pending_object_stores.lock().await;
        while let Some((spec, runtime_env)) = pending.last() {
            connector.register_object_stores(spec, runtime_env).await?;
            pending.pop();
        }

        Ok(connector)
    }

    /// The connector's own error when construction failed with one, so the message
    /// matches what a failed load reports. Otherwise the failure is wrapped as a
    /// configuration error when no retry can clear it (an unknown connector, a
    /// parameter that fails validation), so it is reported as one, and as a read
    /// failure, which is retried, when it can.
    fn build_error(&self, dataset: &DatasetSpec, err: crate::Error) -> DataConnectorError {
        let permanent = crate::init::dataset::is_permanent_dataset_failure(&err);
        let source: Box<dyn std::error::Error + Send + Sync> = match err {
            crate::Error::UnableToInitializeDataConnector { source } => {
                match source.downcast::<DataConnectorError>() {
                    Ok(connector_error) => return *connector_error,
                    Err(source) => source,
                }
            }
            err => Box::new(err),
        };
        if permanent {
            return DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: self.source_name.clone(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            };
        }
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
            None => refresh_mode.unwrap_or_else(|| {
                runtime_acceleration::acceleration::unset_refresh_mode_for_connector(
                    &self.source_name,
                )
            }),
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
        if !dataset.has_metadata_table {
            return None;
        }

        // Registration asks for metadata once, so the connector must exist before
        // it can decide whether a metadata table is supported.
        match self.connector(dataset).await {
            Ok(inner) => inner.metadata_provider(dataset).await,
            Err(err) => Some(Err(err)),
        }
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> super::DataConnectorResult<()> {
        // Checked under the queue's lock, which `connector()` takes after publishing
        // the built connector: either the store is queued before that drain, or the
        // connector is already visible here and the store is registered directly.
        let built = {
            let mut pending = self.pending_object_stores.lock().await;
            let built = self.built().map(Arc::clone);
            if built.is_none() {
                pending.push((dataset.clone(), Arc::clone(runtime_env)));
            }
            built
        };
        match built {
            Some(inner) => inner.register_object_stores(dataset, runtime_env).await,
            None => Ok(()),
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
    use datafusion::common::TableReference;

    use super::{
        ConnectorComponent, DataConnectorError, SourceUnavailable, unreachable_source_warning,
    };

    #[test]
    fn initial_source_failure_retains_its_configuration_classification() {
        let component = ConnectorComponent::Dataset(std::sync::Arc::new(super::DatasetSpec::new(
            "http://127.0.0.1:28199/api",
            TableReference::bare("orders"),
        )));
        let configuration_error = DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "https".to_string(),
            connector_component: component.clone(),
            message: "Full refresh requires refresh_sql".to_string(),
        };
        assert!(matches!(
            SourceUnavailable::failed(&configuration_error),
            SourceUnavailable::Failed {
                configuration_error: true,
                ..
            }
        ));

        let transient_error = DataConnectorError::UnableToConnectInvalidHostOrPort {
            dataconnector: "https".to_string(),
            connector_component: component,
            host: "127.0.0.1".to_string(),
            port: "28199".to_string(),
        };
        assert!(matches!(
            SourceUnavailable::failed(&transient_error),
            SourceUnavailable::Failed {
                configuration_error: false,
                ..
            }
        ));
    }

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
}

#[cfg(test)]
#[path = "reconnecting/metadata_tests.rs"]
mod metadata_tests;
