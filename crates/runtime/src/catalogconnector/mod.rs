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

use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crate::{
    component::catalog::Catalog,
    dataconnector::{ConnectorComponent, ConnectorParams},
    parameters::{ParameterSpec, Parameters},
    Runtime,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use datafusion::catalog::CatalogProvider;
use snafu::prelude::*;
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to setup the {connector_component} ({connector}).\n{source}"))]
    UnableToGetCatalogProvider {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot setup the {connector_component} ({connector}) with an invalid configuration.\n{message}"))]
    InvalidConfiguration {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot setup the {connector_component} ({connector}) with an invalid configuration.\n{message}"))]
    InvalidConfigurationNoSource {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    #[snafu(display("Failed to load the {connector_component} ({connector}).\nAn unknown Catalog Connector Error occurred: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    InternalWithSource {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "databricks")]
pub mod databricks;
pub mod iceberg;
pub mod spice_cloud;
#[cfg(feature = "delta_lake")]
pub mod unity_catalog;

pub(crate) static CATALOG_CONNECTOR_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, CatalogConnectorFactory>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Create a new `CatalogConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `CatalogConnector`.
pub async fn create_new_connector(
    name: &str,
    params: ConnectorParams,
) -> Option<Arc<dyn CatalogConnector>> {
    let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    let factory = connector_factory?;

    Some(factory.connector(params))
}

pub async fn register_all() {
    let mut registry = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;

    #[cfg(feature = "delta_lake")]
    registry.insert(
        "unity_catalog".to_string(),
        CatalogConnectorFactory::new(
            unity_catalog::UnityCatalog::new_connector,
            "unity_catalog",
            unity_catalog::PARAMETERS,
        ),
    );

    #[cfg(feature = "databricks")]
    registry.insert(
        "databricks".to_string(),
        CatalogConnectorFactory::new(
            databricks::Databricks::new_connector,
            "databricks",
            databricks::PARAMETERS,
        ),
    );

    registry.insert(
        "iceberg".to_string(),
        CatalogConnectorFactory::new(
            iceberg::IcebergCatalog::new_connector,
            "iceberg",
            iceberg::PARAMETERS,
        ),
    );

    registry.insert(
        "spice.ai".to_string(),
        CatalogConnectorFactory::new(
            spice_cloud::SpiceCloudPlatformCatalog::new_connector,
            "spiceai",
            spice_cloud::PARAMETERS,
        ),
    );
}

pub(crate) struct CatalogConnectorFactory {
    connector_factory: fn(ConnectorParams) -> Arc<dyn CatalogConnector>,
    prefix: &'static str,
    parameters: &'static [ParameterSpec],
}

impl CatalogConnectorFactory {
    pub fn new(
        connector_factory: fn(ConnectorParams) -> Arc<dyn CatalogConnector>,
        prefix: &'static str,
        parameters: &'static [ParameterSpec],
    ) -> Self {
        Self {
            connector_factory,
            prefix,
            parameters,
        }
    }

    pub fn connector(&self, params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        (self.connector_factory)(params)
    }

    pub fn prefix(&self) -> &'static str {
        self.prefix
    }

    pub fn parameters(&self) -> &'static [ParameterSpec] {
        self.parameters
    }
}

/// A `CatalogConnector` knows how to connect to a remote catalog and create a DataFusion `CatalogProvider`.
#[async_trait]
pub trait CatalogConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Returns a DataFusion `CatalogProvider` which can automatically populate tables from a remote catalog.
    /// The returned provider must implement RefreshableCatalogProvider which will be used to refresh the catalog.
    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: &Runtime,
        _catalog: &Catalog,
    ) -> Result<Arc<dyn RefreshableCatalogProvider>>;
}

pub async fn get_catalog_provider(
    connector: Arc<dyn CatalogConnector>,
    runtime: &Runtime,
    catalog: &Catalog,
    refresh_interval: Option<Duration>,
) -> Result<Arc<dyn CatalogProvider>> {
    let provider = RefreshingCatalogProvider::new(
        connector
            .refreshable_catalog_provider(runtime, catalog)
            .await?,
    )
    .start_refresh(refresh_interval);
    Ok(Arc::new(provider))
}

/// A `CatalogProvider` that periodically refreshes its contents from a remote catalog.
#[derive(Debug)]
pub struct RefreshingCatalogProvider {
    inner: Arc<dyn RefreshableCatalogProvider>,
    refresh_task: Option<JoinHandle<Result<()>>>,
}

impl RefreshingCatalogProvider {
    pub fn new_with_refresh(
        inner: Arc<dyn RefreshableCatalogProvider>,
        refresh_interval: Duration,
    ) -> Self {
        Self::new(inner).start_refresh(Some(refresh_interval))
    }

    fn new(inner: Arc<dyn RefreshableCatalogProvider>) -> Self {
        Self {
            inner,
            refresh_task: None,
        }
    }

    fn start_refresh(mut self, interval: Option<Duration>) -> Self {
        assert!(self.refresh_task.is_none(), "Refresh task already running");
        let interval = interval.unwrap_or(Duration::from_secs(60));
        let inner = Arc::clone(&self.inner);
        self.refresh_task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match inner.refresh().await {
                    Ok(()) => (),
                    Err(e) => {
                        tracing::error!("Failed to refresh catalog: {}", e);
                    }
                }
            }
        }));
        self
    }
}

impl CatalogProvider for RefreshingCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        self.inner.schema(name)
    }
}

impl Drop for RefreshingCatalogProvider {
    fn drop(&mut self) {
        if let Some(task) = self.refresh_task.take() {
            task.abort();
        }
    }
}
