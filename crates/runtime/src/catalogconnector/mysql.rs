/*
Copyright 2026 The Spice.ai OSS Authors

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

//! MySQL catalog connector.
//!
//! Connects to a MySQL database and provides schema/table
//! discovery via `information_schema` queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{
    Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::mysql::provider::MySQLCatalogProvider;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "mysql";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create MySQL connection pool: {source}"))]
    UnableToCreateConnectionPool {
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .secret()
        .description("The MySQL connection string."),
    ParameterSpec::component("user")
        .secret()
        .description("The MySQL username for authentication."),
    ParameterSpec::component("pass")
        .secret()
        .description("The MySQL password for authentication."),
    ParameterSpec::component("host")
        .description("The MySQL host address."),
    ParameterSpec::component("tcp_port")
        .description("The MySQL port number."),
    ParameterSpec::component("db")
        .description("The MySQL database name."),
    ParameterSpec::component("sslmode")
        .description("The SSL mode for the connection."),
    ParameterSpec::component("sslrootcert")
        .description("The path to the SSL root certificate."),
];

/// A catalog connector for MySQL, providing access to schemas and tables
/// within a MySQL database.
#[derive(Clone)]
pub struct MySQLCatalog {
    params: ConnectorParams,
}

impl MySQLCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for MySQLCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let pool = MySQLConnectionPool::new(self.params.parameters.to_secret_map())
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?;

        let pool = Arc::new(pool);
        let table_factory = Arc::new(MySQLTableFactory::new(Arc::clone(&pool)));

        // Create a separate mysql_async::Pool for metadata queries.
        let metadata_pool = Self::create_metadata_pool(&self.params)
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: e,
            })?;

        let catalog_provider = Arc::new(MySQLCatalogProvider::new(
            metadata_pool,
            table_factory,
            catalog.include.clone(),
        ));

        catalog_provider
            .refresh()
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component,
                source: e,
            })?;

        Ok(catalog_provider as Arc<dyn RefreshableCatalogProvider>)
    }
}

impl MySQLCatalog {
    /// Creates a `mysql_async::Pool` from the connector parameters for metadata queries.
    fn create_metadata_pool(
        params: &ConnectorParams,
    ) -> std::result::Result<mysql_async::Pool, Box<dyn std::error::Error + Send + Sync>> {
        use secrecy::ExposeSecret;

        let secret_map = params.parameters.to_secret_map();

        // Build connection URL from parameters, similar to how MySQLConnectionPool does it.
        if let Some(conn_string) = secret_map.get("connection_string") {
            let url = conn_string.expose_secret();
            let opts = mysql_async::Opts::from_url(url)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            return Ok(mysql_async::Pool::new(opts));
        }

        let user = secret_map
            .get("user")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_default();
        let pass = secret_map
            .get("pass")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_default();
        let host = secret_map
            .get("host")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| "localhost".to_string());
        let port = secret_map
            .get("tcp_port")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_else(|| "3306".to_string());
        let db = secret_map
            .get("db")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_default();

        let url = format!("mysql://{user}:{pass}@{host}:{port}/{db}");
        let opts = mysql_async::Opts::from_url(&url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(mysql_async::Pool::new(opts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RuntimeBuilder;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::ConnectorComponent;
    use app::AppBuilder;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;
    use tokio::runtime::Handle;

    async fn make_connector_params(params: HashMap<String, String>) -> ConnectorParams {
        let app = AppBuilder::new("test").build();
        let rt = RuntimeBuilder::new().build().await;

        let dataset = DatasetBuilder::try_new("mysql://localhost/db".to_string(), "test_ds")
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("valid dataset");

        ConnectorParams {
            parameters: crate::parameters::Parameters::new(
                to_secret_map(params).into_iter().collect(),
                PREFIX,
                PARAMETERS,
            ),
            unsupported_type_action: None,
            component: ConnectorComponent::from(&dataset),
            app: None,
            runtime: None,
            io_runtime: Handle::current(),
        }
    }

    #[tokio::test]
    async fn test_create_metadata_pool_from_connection_string() {
        let mut params = HashMap::new();
        params.insert(
            "connection_string".to_string(),
            "mysql://root:pass@127.0.0.1:3306/testdb".to_string(),
        );
        let connector_params = make_connector_params(params).await;
        MySQLCatalog::create_metadata_pool(&connector_params)
            .expect("should create pool from connection string");
    }

    #[tokio::test]
    async fn test_create_metadata_pool_from_individual_params() {
        let mut params = HashMap::new();
        params.insert("user".to_string(), "root".to_string());
        params.insert("pass".to_string(), "password".to_string());
        params.insert("host".to_string(), "127.0.0.1".to_string());
        params.insert("tcp_port".to_string(), "3306".to_string());
        params.insert("db".to_string(), "mydb".to_string());
        let connector_params = make_connector_params(params).await;
        MySQLCatalog::create_metadata_pool(&connector_params)
            .expect("should create pool from individual params");
    }

    #[tokio::test]
    async fn test_create_metadata_pool_defaults_host_and_port() {
        // With no host/port, should default to localhost:3306
        let params = HashMap::new();
        let connector_params = make_connector_params(params).await;
        MySQLCatalog::create_metadata_pool(&connector_params)
            .expect("should succeed with default host and port");
    }

    #[tokio::test]
    async fn test_create_metadata_pool_invalid_connection_string() {
        let mut params = HashMap::new();
        params.insert(
            "connection_string".to_string(),
            "not-a-valid-url".to_string(),
        );
        let connector_params = make_connector_params(params).await;
        assert!(
            MySQLCatalog::create_metadata_pool(&connector_params).is_err(),
            "should fail with invalid connection string"
        );
    }

    #[tokio::test]
    async fn test_create_metadata_pool_connection_string_takes_precedence() {
        // When both connection_string and individual params are present,
        // connection_string should be used.
        let mut params = HashMap::new();
        params.insert(
            "connection_string".to_string(),
            "mysql://conn_user:conn_pass@connhost:3307/conndb".to_string(),
        );
        params.insert("user".to_string(), "individual_user".to_string());
        params.insert("host".to_string(), "individual_host".to_string());
        let connector_params = make_connector_params(params).await;
        // Should succeed using connection_string, not individual params
        MySQLCatalog::create_metadata_pool(&connector_params)
            .expect("should create pool from connection_string when both are present");
    }
}
