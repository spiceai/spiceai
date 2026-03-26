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
use datafusion::sql::unparser::dialect::{BigQueryDialect, Dialect};
use datafusion_table_providers::adbc::AdbcTableFactory;
use datafusion_table_providers::sql::db_connection_pool::adbcpool::{
    ADBCPool, AdbcConnectionPoolBuilder,
};
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

    #[snafu(display("Missing required parameter: adbc_uri"))]
    MissingAdbcUri,

    #[snafu(display(
        "Invalid value for parameter '{name}': expected a positive integer, got '{value}'"
    ))]
    InvalidPoolParameter { name: String, value: String },

    #[snafu(display("Failed to load ADBC driver '{driver_location}': {source}"))]
    UnableToLoadDriver {
        driver_location: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC database (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateDatabase {
        driver_location: String,
        uri: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display(
        "Failed to create ADBC connection pool (driver='{driver_location}', uri='{uri}'): {source}"
    ))]
    UnableToCreateConnectionPool {
        driver_location: String,
        uri: String,
        source: datafusion_table_providers::sql::db_connection_pool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Adbc {
    adbc_factory: AdbcTableFactory<adbc_driver_manager::ManagedDatabase>,
    driver_name: String,
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
    ParameterSpec::component("driver")
        .description("The ADBC driver name (e.g., 'duckdb', 'sqlite', 'postgres')")
        .required(),
    ParameterSpec::component("driver_path").description("Optional path to the ADBC driver library"),
    ParameterSpec::component("uri")
        .description("Database URI/connection string for the ADBC driver")
        .required(),
    ParameterSpec::component("username")
        .description("Username for database authentication")
        .secret(),
    ParameterSpec::component("password")
        .description("Password for database authentication")
        .secret(),
    ParameterSpec::component("driver_options").description(
        "Semicolon-delimited driver-specific database options (e.g., 'key1=value1;key2=value2')",
    ),
    ParameterSpec::component("catalog").description("The catalog for the connection"),
    ParameterSpec::component("schema").description("The schema for the connection"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections in the connection pool.")
        .default("5"),
    ParameterSpec::runtime("connection_pool_min_idle")
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
                .get("driver")
                .expose()
                .ok()
                .context(MissingAdbcDriverSnafu)
                .map_err(|e| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                })?;

            let driver_name_owned = driver_name.to_string();
            let driver_path = params.parameters.get("driver_path").expose().ok();
            let driver_location = driver_path.unwrap_or(driver_name).to_string();

            let uri = params
                .parameters
                .get("uri")
                .expose()
                .ok()
                .context(MissingAdbcUriSnafu)
                .map_err(|e| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                })?;

            let uri_str = uri.to_string();

            let username = params.parameters.get("username").expose().ok();
            let password = params.parameters.get("password").expose().ok();
            let driver_options = params.parameters.get("driver_options").expose().ok();
            let db_options = build_db_options(&uri_str, username, password, driver_options);

            let catalog = params
                .parameters
                .get("catalog")
                .expose()
                .ok()
                .map(String::from);
            let schema = params
                .parameters
                .get("schema")
                .expose()
                .ok()
                .map(String::from);

            let conn_options = build_conn_options(catalog.as_deref(), schema.as_deref());

            let parse_pool_param = |name: &str| -> std::result::Result<Option<u32>, Error> {
                match params.parameters.get(name).expose().ok() {
                    Some(v) => {
                        let parsed = v.parse::<u32>().map_err(|_| Error::InvalidPoolParameter {
                            name: name.to_string(),
                            value: v.to_string(),
                        })?;
                        if parsed == 0 {
                            return Err(Error::InvalidPoolParameter {
                                name: name.to_string(),
                                value: v.to_string(),
                            });
                        }
                        Ok(Some(parsed))
                    }
                    None => Ok(None),
                }
            };

            let pool_size = parse_pool_param("connection_pool_size").map_err(|e| {
                DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                }
            })?;
            let pool_min_idle = parse_pool_param("connection_pool_min_idle").map_err(|e| {
                DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "adbc".to_string(),
                    connector_component: params.component.clone(),
                    source: Box::new(e),
                }
            })?;

            let component = params.component.clone();

            if uri_str == ":memory:" || uri_str.contains("mode=memory") {
                let err: Box<dyn std::error::Error + Send + Sync> =
                    Box::new(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "adbc".to_string(),
                        connector_component: component,
                        message: "In-memory database URIs (e.g., ':memory:') are not supported because each pooled connection creates an isolated database, leading to data inconsistency".to_string(),
                    });
                return Err(err);
            }

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
                .context(UnableToLoadDriverSnafu {
                    driver_location: driver_location.clone(),
                })?;

                let db = driver.new_database_with_opts(db_options).context(
                    UnableToCreateDatabaseSnafu {
                        driver_location: driver_location.clone(),
                        uri: uri_str.clone(),
                    },
                )?;

                let mut pool_builder = AdbcConnectionPoolBuilder::new(db)
                    .with_max_size(pool_size)
                    .with_min_idle(pool_min_idle);

                if let Some(conn_opts) = conn_options {
                    pool_builder = pool_builder.with_conn_options(conn_opts);
                }

                let pool = pool_builder
                    .build()
                    .context(UnableToCreateConnectionPoolSnafu {
                        driver_location,
                        uri: uri_str,
                    })?;

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

            Ok(Arc::new(Adbc {
                adbc_factory,
                driver_name: driver_name_owned,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "adbc"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Builds the list of ADBC database options from connector parameters.
pub(crate) fn build_db_options(
    uri: &str,
    username: Option<&str>,
    password: Option<&str>,
    driver_options: Option<&str>,
) -> Vec<(OptionDatabase, adbc_core::options::OptionValue)> {
    let mut opts = vec![(OptionDatabase::Uri, uri.into())];
    if let Some(u) = username {
        opts.push((OptionDatabase::Username, u.into()));
    }
    if let Some(p) = password {
        opts.push((OptionDatabase::Password, p.into()));
    }
    if let Some(options_str) = driver_options {
        for pair in options_str.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((key, value)) = pair.split_once('=') {
                let key = key.trim();
                if key.is_empty() {
                    tracing::warn!("Ignoring ADBC driver option with empty key");
                    continue;
                }
                let key = if key.starts_with("adbc.") {
                    key.to_string()
                } else {
                    format!("adbc.{key}")
                };
                opts.push((OptionDatabase::Other(key), value.trim().into()));
            } else {
                tracing::warn!("Ignoring malformed ADBC driver option (expected 'key=value')");
            }
        }
    }
    opts
}

/// Builds connection-level options from connector parameters.
fn build_conn_options(
    catalog: Option<&str>,
    schema: Option<&str>,
) -> Option<HashMap<String, String>> {
    let mut opts = HashMap::new();

    if let Some(catalog) = catalog {
        opts.insert(
            adbc_core::options::OptionConnection::CurrentCatalog
                .as_ref()
                .to_string(),
            catalog.to_string(),
        );
    }

    if let Some(schema) = schema {
        opts.insert(
            adbc_core::options::OptionConnection::CurrentSchema
                .as_ref()
                .to_string(),
            schema.to_string(),
        );
    }

    if opts.is_empty() { None } else { Some(opts) }
}
pub(crate) fn dialect_for_driver(driver_name: &str) -> Option<Arc<dyn Dialect + Send + Sync>> {
    match driver_name {
        "bigquery" => Some(Arc::new(BigQueryDialect::new())),
        _ => None,
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
        let dialect = dialect_for_driver(&self.driver_name);
        self.adbc_factory
            .table_provider(table_reference, dialect)
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
        let dialect = dialect_for_driver(&self.driver_name);

        Some(
            self.adbc_factory
                .read_write_table_provider(table_reference, dialect)
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
        assert!(param_names.contains(&"driver"));
        assert!(param_names.contains(&"driver_path"));
        assert!(param_names.contains(&"uri"));
        assert!(param_names.contains(&"username"));
        assert!(param_names.contains(&"password"));
        assert!(param_names.contains(&"driver_options"));
        assert!(param_names.contains(&"catalog"));
        assert!(param_names.contains(&"schema"));
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

    #[test]
    fn test_build_db_options_uri_only() {
        let opts = build_db_options("file:test.db", None, None, None);
        assert_eq!(opts.len(), 1);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "file:test.db")
        );
    }

    #[test]
    fn test_build_db_options_with_username_password() {
        let opts = build_db_options("postgres://host/db", Some("admin"), Some("secret"), None);
        assert_eq!(opts.len(), 3);

        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert!(
            matches!(&opts[0].1, adbc_core::options::OptionValue::String(s) if s == "postgres://host/db")
        );

        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "admin"));

        assert_eq!(opts[2].0, OptionDatabase::Password);
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "secret"));
    }

    #[test]
    fn test_build_db_options_username_only() {
        let opts = build_db_options("sqlite:test.db", Some("user"), None, None);
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(opts[1].0, OptionDatabase::Username);
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "user"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_unprefixed() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("snowflake.sql.db=MY_DB;snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(opts[0].0, OptionDatabase::Uri);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "MY_DB"));
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
        assert!(matches!(&opts[2].1, adbc_core::options::OptionValue::String(s) if s == "PUBLIC"));
    }

    #[test]
    fn test_build_db_options_with_driver_options_prefixed() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("adbc.snowflake.sql.db=MY_DB;adbc.snowflake.sql.schema=PUBLIC"),
        );
        assert_eq!(opts.len(), 3);
        assert_eq!(
            opts[1].0,
            OptionDatabase::Other("adbc.snowflake.sql.db".to_string())
        );
        assert_eq!(
            opts[2].0,
            OptionDatabase::Other("adbc.snowflake.sql.schema".to_string())
        );
    }

    #[test]
    fn test_build_db_options_driver_options_trailing_semicolon() {
        let opts = build_db_options("uri://db", None, None, Some("key=value;"));
        assert_eq!(opts.len(), 2);
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.key".to_string()));
        assert!(matches!(&opts[1].1, adbc_core::options::OptionValue::String(s) if s == "value"));
    }

    #[test]
    fn test_build_db_options_driver_options_malformed_ignored() {
        let opts = build_db_options(
            "uri://db",
            None,
            None,
            Some("good=val;bad_no_equals;another=ok"),
        );
        assert_eq!(opts.len(), 3); // uri + good + another (bad_no_equals skipped)
    }

    #[test]
    fn test_build_db_options_driver_options_empty_key_ignored() {
        let opts = build_db_options("uri://db", None, None, Some("=value;good=ok"));
        assert_eq!(opts.len(), 2); // uri + good (empty key skipped)
        assert_eq!(opts[1].0, OptionDatabase::Other("adbc.good".to_string()));
    }

    #[test]
    fn test_build_conn_options_none_when_empty() {
        let opts = build_conn_options(None, None);
        assert!(opts.is_none());
    }

    #[test]
    fn test_build_conn_options_both() {
        let opts =
            build_conn_options(Some("my_catalog"), Some("my_schema")).expect("should have options");
        assert_eq!(opts.len(), 2);
        assert_eq!(
            opts.get("adbc.connection.catalog"),
            Some(&"my_catalog".to_string())
        );
        assert_eq!(
            opts.get("adbc.connection.db_schema"),
            Some(&"my_schema".to_string())
        );
    }

    #[test]
    fn test_build_conn_options_catalog_only() {
        let opts = build_conn_options(Some("cat"), None).expect("should have options");
        assert_eq!(opts.len(), 1);
        assert_eq!(
            opts.get("adbc.connection.catalog"),
            Some(&"cat".to_string())
        );
    }
}
