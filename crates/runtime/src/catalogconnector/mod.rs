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
    Runtime,
    component::{ComponentInitialization, catalog::Catalog},
    dataconnector::{ConnectorComponent, parameters::ConnectorParams},
    parameters::{ParameterSpec, Parameters},
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
pub use data_components::RefreshingCatalogProvider;
use datafusion::catalog::CatalogProvider;
use deferred::DeferredCatalogProvider;
use snafu::prelude::*;
use tokio::sync::Mutex;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to setup the {connector_component} ({connector}). {source}"))]
    UnableToGetCatalogProvider {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({connector}) with an invalid configuration. {message}"
    ))]
    InvalidConfiguration {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({connector}) with an invalid configuration. {message}"
    ))]
    InvalidConfigurationNoSource {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({connector}). An unknown Catalog Connector Error occurred: {source}"
    ))]
    InternalWithSource {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to initiate catalog, app reference cannot be obtained from the runtime."
    ))]
    FailedToGetAppFromRuntime {},

    #[snafu(transparent)]
    IcebergSnafu { source: iceberg::Error },

    #[snafu(display(
        "Failed to read partition metadata for table {schema_name}.{table_name}: {source}"
    ))]
    PartitionMetadataRead {
        schema_name: String,
        table_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    /// Returns `true` for configuration errors that will not resolve with retries.
    pub fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            Error::InvalidConfiguration { .. } | Error::InvalidConfigurationNoSource { .. }
        )
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "adbc")]
pub mod adbc;
#[cfg(not(windows))]
pub mod cayenne;
#[cfg(feature = "databricks")]
pub mod databricks;
pub mod deferred;
#[cfg(feature = "duckdb")]
pub mod ducklake;
pub mod glue;
pub mod iceberg;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "oracle")]
pub mod oracle;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "snowflake")]
pub mod snowflake;
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

/// Names of every registered catalog connector, for "did you mean?" suggestions.
pub async fn registered_catalog_names() -> Vec<String> {
    let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
    let mut names: Vec<String> = guard.keys().cloned().collect();
    names.sort();
    names
}

/// Closest-match suggestion for an unknown catalog connector name. Reuses the
/// same scoring/threshold helper as data connectors so behaviour is consistent.
pub async fn suggest_catalog_connector(name: &str) -> Option<String> {
    util::levenshtein::closest_match(name, &registered_catalog_names().await)
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
            &iceberg::PARAMETERS,
        ),
    );

    registry.insert(
        glue::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            glue::GlueCatalog::new_connector,
            glue::PREFIX,
            &super::dataconnector::glue::PARAMETERS,
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

    #[cfg(feature = "duckdb")]
    registry.insert(
        ducklake::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            ducklake::DuckLakeCatalog::new_connector,
            ducklake::PREFIX,
            ducklake::PARAMETERS,
        ),
    );

    #[cfg(feature = "snowflake")]
    registry.insert(
        snowflake::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            snowflake::SnowflakeCatalog::new_connector,
            snowflake::PREFIX,
            snowflake::PARAMETERS,
        ),
    );

    #[cfg(feature = "postgres")]
    registry.insert(
        postgres::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            postgres::PostgresCatalog::new_connector,
            postgres::PREFIX,
            postgres::PARAMETERS,
        ),
    );

    #[cfg(feature = "mysql")]
    registry.insert(
        mysql::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            mysql::MySQLCatalog::new_connector,
            mysql::PREFIX,
            mysql::PARAMETERS,
        ),
    );

    #[cfg(feature = "mssql")]
    registry.insert(
        mssql::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            mssql::MssqlCatalog::new_connector,
            mssql::PREFIX,
            mssql::PARAMETERS,
        ),
    );

    #[cfg(feature = "oracle")]
    registry.insert(
        oracle::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            oracle::OracleCatalog::new_connector,
            oracle::PREFIX,
            oracle::PARAMETERS,
        ),
    );

    #[cfg(all(not(windows), feature = "spicebench"))]
    registry.insert(
        cayenne::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            cayenne::CayenneCatalogConnector::new_connector,
            cayenne::PREFIX,
            cayenne::PARAMETERS,
        ),
    );

    #[cfg(feature = "adbc")]
    registry.insert(
        adbc::PREFIX.to_string(),
        CatalogConnectorFactory::new(
            adbc::AdbcCatalog::new_connector,
            adbc::PREFIX,
            adbc::PARAMETERS,
        ),
    );
}

pub async fn unregister_all() {
    let mut registry = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
    registry.clear();
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

/// A `CatalogConnector` knows how to connect to a remote catalog and create a `DataFusion` `CatalogProvider`.
#[async_trait]
pub trait CatalogConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Returns a `DataFusion` `CatalogProvider` which can automatically populate tables from a remote catalog.
    /// The returned provider must implement `RefreshableCatalogProvider` which will be used to refresh the catalog.
    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        _catalog: &Catalog,
    ) -> Result<Arc<dyn RefreshableCatalogProvider>>;

    /// Returns whether the catalog connector should be initialized on startup or on trigger.
    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::default()
    }
}

/// Trait for catalog providers that can report partition metadata labels for their tables.
///
/// Implementations read partition metadata from the catalog's own persistent storage,
/// ensuring partition information survives runtime restarts.
#[async_trait]
pub trait PartitionAwareCatalog: Send + Sync {
    /// Returns the partition metadata label for a table, if one was defined at creation time.
    ///
    /// The returned string is the partition column/label value as stored in catalog metadata
    /// (e.g. `"region"` for `PARTITION BY region`).
    ///
    /// Returns `Ok(None)` only when no partition metadata is present.
    async fn table_partition_expr(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<String>>;
}

pub async fn get_catalog_provider(
    connector: Arc<dyn CatalogConnector>,
    runtime: Arc<Runtime>,
    catalog: &Catalog,
    refresh_interval: Option<Duration>,
) -> Result<Arc<dyn CatalogProvider>> {
    if connector.initialization().is_on_trigger() {
        return Ok(Arc::new(DeferredCatalogProvider::new(
            runtime,
            connector,
            catalog.clone(),
        )));
    }

    let provider = RefreshingCatalogProvider::new(
        Arc::clone(&connector)
            .refreshable_catalog_provider(runtime, catalog)
            .await?,
    )
    .start_refresh(refresh_interval);
    Ok(Arc::new(provider))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static REGISTRY_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    #[tokio::test]
    async fn test_catalog_connector_registry_lifecycle() {
        let _test_guard = REGISTRY_TEST_LOCK.lock().await;

        // Start clean
        unregister_all().await;
        {
            let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
            assert!(
                guard.is_empty(),
                "registry should be empty after unregister_all"
            );
        }

        // Register all connectors
        register_all().await;
        {
            let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;

            // Always-registered connectors (no feature gates)
            assert!(
                guard.contains_key("iceberg"),
                "iceberg should be registered"
            );
            assert!(
                guard.contains_key(glue::PREFIX),
                "glue should be registered"
            );
            assert!(
                guard.contains_key("spice.ai"),
                "spice.ai should be registered"
            );

            // Feature-gated connectors
            #[cfg(feature = "delta_lake")]
            assert!(
                guard.contains_key("unity_catalog"),
                "unity_catalog should be registered"
            );
            #[cfg(feature = "databricks")]
            assert!(
                guard.contains_key("databricks"),
                "databricks should be registered"
            );
            #[cfg(feature = "duckdb")]
            assert!(
                guard.contains_key(ducklake::PREFIX),
                "ducklake should be registered"
            );
            #[cfg(feature = "snowflake")]
            assert!(
                guard.contains_key(snowflake::PREFIX),
                "snowflake should be registered"
            );
            #[cfg(feature = "postgres")]
            assert!(
                guard.contains_key(postgres::PREFIX),
                "postgres should be registered"
            );
            #[cfg(feature = "mysql")]
            assert!(
                guard.contains_key(mysql::PREFIX),
                "mysql should be registered"
            );
            #[cfg(feature = "mssql")]
            assert!(
                guard.contains_key(mssql::PREFIX),
                "mssql should be registered"
            );
            #[cfg(feature = "oracle")]
            assert!(
                guard.contains_key(oracle::PREFIX),
                "oracle should be registered"
            );
            #[cfg(all(not(windows), feature = "spicebench"))]
            assert!(
                guard.contains_key(cayenne::PREFIX),
                "cayenne should be registered"
            );
            // Guard against accidental re-registration: by default (without the
            // `spicebench` feature) the Cayenne catalog connector must NOT be registered.
            #[cfg(all(not(windows), not(feature = "spicebench")))]
            assert!(
                !guard.contains_key(cayenne::PREFIX),
                "cayenne should not be registered without the spicebench feature"
            );
        }

        // Verify factory metadata for a known connector
        {
            let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
            let iceberg_factory = guard.get("iceberg").expect("iceberg factory should exist");
            assert_eq!(iceberg_factory.prefix(), "iceberg");
            assert!(
                !iceberg_factory.parameters().is_empty(),
                "iceberg should have parameters"
            );
        }

        // Unregister and verify empty
        unregister_all().await;
        {
            let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
            assert!(
                guard.is_empty(),
                "registry should be empty after final unregister_all"
            );
        }
    }

    #[tokio::test]
    async fn test_create_new_connector_unknown_returns_none() {
        let _test_guard = REGISTRY_TEST_LOCK.lock().await;

        unregister_all().await;
        register_all().await;

        // Trying to look up the factory for a nonexistent connector
        let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
        assert!(
            guard.get("nonexistent_connector").is_none(),
            "unknown connector name should not be found"
        );

        drop(guard);
        unregister_all().await;
    }
}
