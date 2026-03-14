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

use crate::component::dataset::Dataset;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Driver as _, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: adbc_driver"))]
    MissingAdbcDriver,

    #[snafu(display("Failed to load ADBC driver: {source}"))]
    UnableToLoadDriver { source: adbc_core::error::Error },

    #[snafu(display("Failed to create ADBC database: {source}"))]
    UnableToCreateDatabase { source: adbc_core::error::Error },

    #[snafu(display("Failed to create ADBC connection pool: {source}"))]
    UnableToCreateConnectionPool {
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Adbc {
    adbc_factory: AdbcTableFactory<adbc_driver_manager::ManagedDatabase>,
}

impl std::fmt::Debug for Adbc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adbc").finish_non_exhaustive()
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct AdbcFactory {}

impl AdbcFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("adbc_driver")
        .description("The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres')"),
    ParameterSpec::component("adbc_driver_path")
        .description("Optional path to the ADBC driver library"),
    ParameterSpec::component("adbc_uri")
        .description("Database URI/connection string for the ADBC driver"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections in the connection pool.")
        .default("5"),
    ParameterSpec::component("connection_pool_min_idle")
        .description("The minimum number of idle connections to keep open in the pool.")
        .default("1"),
];

impl DataConnectorFactory for AdbcFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let driver_name = params
                .parameters
                .get("adbc_driver")
                .expose()
                .ok()
                .context(MissingAdbcDriverSnafu)
                .map_err(|e| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                })?;

            let driver_path = params.parameters.get("adbc_driver_path").expose().ok();
            let driver_location = driver_path.unwrap_or(driver_name).to_string();

            let mut db_options: Vec<(OptionDatabase, adbc_core::options::OptionValue)> = Vec::new();
            if let Some(uri) = params.parameters.get("adbc_uri").expose().ok() {
                db_options.push((OptionDatabase::Uri, uri.into()));
            }

            // Allow passing through any other parameters as database options
            for (key, value) in &params.parameters {
                if !key.starts_with("adbc_")
                    && !key.starts_with("conn_")
                    && key != "connection_pool_size"
                    && key != "connection_pool_min_idle"
                {
                    db_options.push((
                        OptionDatabase::Other(key.clone()),
                        value.expose_secret().into(),
                    ));
                }
            }

            let mut conn_options: HashMap<String, String> = HashMap::new();

            // Extract connection-specific options (strip "conn_" prefix)
            for (key, value) in &params.parameters {
                if let Some(conn_key) = key.strip_prefix("conn_") {
                    conn_options.insert(conn_key.to_string(), value.expose_secret().to_string());
                }
            }

            let pool_size: Option<u32> = params
                .parameters
                .get("connection_pool_size")
                .expose()
                .ok()
                .and_then(|v| v.parse().ok());
            let pool_min_idle: Option<u32> = params
                .parameters
                .get("connection_pool_min_idle")
                .expose()
                .ok()
                .and_then(|v| v.parse().ok());

            let component = params.component.clone();

            // Driver loading, database creation, and pool creation are all
            // synchronous FFI/IO operations — offload to a blocking thread.
            let pool = tokio::task::spawn_blocking(move || -> Result<Arc<ADBCPool<_>>> {
                let mut driver = ManagedDriver::load_from_name(
                    &driver_location,
                    None,
                    AdbcVersion::V110,
                    LOAD_FLAG_DEFAULT,
                    None,
                )
                .context(UnableToLoadDriverSnafu)?;

                let db = driver
                    .new_database_with_opts(db_options)
                    .context(UnableToCreateDatabaseSnafu)?;

                let pool = AdbcConnectionPoolBuilder::new(db)
                    .with_conn_options(conn_options)
                    .with_max_size(pool_size)
                    .with_min_idle(pool_min_idle)
                    .build()
                    .context(UnableToCreateConnectionPoolSnafu)?;

                Ok(Arc::new(pool))
            })
            .await
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "adbc".to_string(),
                connector_component: component.clone(),
                source: Box::new(e),
            })?
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "adbc".to_string(),
                connector_component: component,
                source: Box::new(e),
            })?;

            let adbc_factory = AdbcTableFactory::new(pool);

            Ok(Arc::new(Adbc { adbc_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "adbc"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

register_data_connector!("adbc", AdbcFactory);

#[async_trait]
impl DataConnector for Adbc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_reference = dataset.path().into();

        self.adbc_factory
            .table_provider(table_reference, None)
            .await
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "adbc".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let table_reference = dataset.path().into();

        Some(
            self.adbc_factory
                .read_write_table_provider(table_reference, None)
                .await
                .map_err(|e| DataConnectorError::UnableToGetReadWriteProvider {
                    dataconnector: "adbc".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_as_any() {
        let factory = AdbcFactory::new();
        assert!(factory.as_any().is::<AdbcFactory>());
    }

    #[test]
    fn test_factory_prefix() {
        let factory = AdbcFactory::new();
        assert_eq!(factory.prefix(), "adbc");
    }

    #[test]
    fn test_factory_parameters() {
        let factory = AdbcFactory::new();
        let params = factory.parameters();

        let param_names: Vec<&str> = params.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"adbc_driver"));
        assert!(param_names.contains(&"adbc_driver_path"));
        assert!(param_names.contains(&"adbc_uri"));
        assert!(param_names.contains(&"connection_pool_size"));
        assert!(param_names.contains(&"connection_pool_min_idle"));
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingAdbcDriver;
        assert_eq!(err.to_string(), "Missing required parameter: adbc_driver");

        let _boxed: Box<dyn std::error::Error> = Box::new(err);
    }

    #[test]
    fn test_factory_new_arc() {
        let factory = AdbcFactory::new_arc();
        assert_eq!(factory.prefix(), "adbc");
    }

    #[test]
    fn test_debug_impl() {
        let factory = AdbcFactory::new();
        let debug_str = format!("{factory:?}");
        assert!(debug_str.contains("AdbcFactory"));
    }
}
