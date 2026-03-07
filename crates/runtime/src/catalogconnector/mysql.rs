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

//! `MySQL` catalog connector.
//!
//! Connects to a `MySQL` database and provides schema/table
//! discovery via `information_schema` queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::mysql::provider::MySQLCatalogProvider;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "mysql";

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
    ParameterSpec::component("host").description("The MySQL host address."),
    ParameterSpec::component("tcp_port").description("The MySQL port number."),
    ParameterSpec::component("db").description("The MySQL database name."),
    ParameterSpec::component("sslmode").description("The SSL mode for the connection."),
    ParameterSpec::component("sslrootcert").description("The path to the SSL root certificate."),
];

/// A catalog connector for `MySQL`, providing access to schemas and tables
/// within a `MySQL` database.
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
        // `MySQLTableFactory` requires `MySQLConnectionPool` while metadata discovery uses
        // `mysql_async`; until these abstractions converge, constrain metadata connections to
        // avoid excessive combined connection usage.
        let metadata_pool = Self::create_metadata_pool(&self.params).map_err(|e| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: e,
            }
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
        use std::io;
        use std::path::PathBuf;

        let secret_map = params.parameters.to_secret_map();

        // Build connection URL from parameters, similar to how MySQLConnectionPool does it.
        if let Some(conn_string) = secret_map.get("connection_string") {
            let url = conn_string.expose_secret();
            let opts = mysql_async::Opts::from_url(url)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let opts = Self::with_metadata_pool_constraints(opts)?;
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
        let host = secret_map.get("host").map_or_else(
            || "localhost".to_string(),
            |s| s.expose_secret().to_string(),
        );
        let port = secret_map.get("tcp_port").map_or(Ok(3306_u16), |s| {
            s.expose_secret().parse::<u16>().map_err(|e| {
                Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid tcp_port '{}': {e}", s.expose_secret()),
                )) as Box<dyn std::error::Error + Send + Sync>
            })
        })?;
        let db = secret_map
            .get("db")
            .map(|s| s.expose_secret().to_string())
            .unwrap_or_default();

        let ssl_mode = secret_map.get("sslmode").map_or_else(
            || "required".to_string(),
            |s| s.expose_secret().to_lowercase(),
        );
        if ssl_mode != "disabled" && ssl_mode != "required" && ssl_mode != "preferred" {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid sslmode '{ssl_mode}'; expected disabled, required, or preferred"),
            )));
        }

        let ssl_rootcert_path = if let Some(root_cert) = secret_map.get("sslrootcert") {
            let root_cert_path = root_cert.expose_secret();
            if !std::path::Path::new(root_cert_path).exists() {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid sslrootcert path '{root_cert_path}'"),
                )));
            }
            Some(PathBuf::from(root_cert_path))
        } else {
            None
        };

        let mut builder = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::default());
        if !user.is_empty() {
            builder = builder.user(Some(user));
        }
        if !pass.is_empty() {
            builder = builder.pass(Some(pass));
        }
        if !host.is_empty() {
            builder = builder.ip_or_hostname(host);
        }
        if !db.is_empty() {
            builder = builder.db_name(Some(db));
        }
        builder = builder.tcp_port(port);
        builder = builder.ssl_opts(Self::metadata_pool_ssl_opts(&ssl_mode, ssl_rootcert_path));

        let opts = mysql_async::Opts::from(builder);
        let opts = Self::with_metadata_pool_constraints(opts)?;
        Ok(mysql_async::Pool::new(opts))
    }

    fn with_metadata_pool_constraints(
        opts: mysql_async::Opts,
    ) -> std::result::Result<mysql_async::Opts, Box<dyn std::error::Error + Send + Sync>> {
        use std::io;

        let constraints = mysql_async::PoolConstraints::new(0, 1).ok_or_else(|| {
            Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid MySQL metadata pool constraints",
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let pool_opts = mysql_async::PoolOpts::default().with_constraints(constraints);
        let builder = mysql_async::OptsBuilder::from_opts(opts).pool_opts(pool_opts);

        Ok(mysql_async::Opts::from(builder))
    }

    fn metadata_pool_ssl_opts(
        ssl_mode: &str,
        rootcert_path: Option<std::path::PathBuf>,
    ) -> Option<mysql_async::SslOpts> {
        if ssl_mode == "disabled" {
            return None;
        }

        let mut opts = mysql_async::SslOpts::default();

        if let Some(rootcert_path) = rootcert_path {
            opts = opts.with_root_certs(vec![rootcert_path.into()]);
        }

        if ssl_mode == "preferred" {
            opts = opts
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }

        Some(opts)
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
