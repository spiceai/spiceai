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

//! MSSQL catalog connector.
//!
//! Connects to a Microsoft SQL Server database and provides schema/table
//! discovery via `INFORMATION_SCHEMA` queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::parameters::Parameters;
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::mssql::connection_manager::SqlServerConnectionManager;
use data_components::mssql::provider::MssqlCatalogProvider;
use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;
use tiberius::{Config, EncryptionLevel};

pub const PREFIX: &str = "mssql";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: '{parameter}'. Specify a value."))]
    MissingParameter { parameter: String },

    #[snafu(display("Invalid connection string: {source}"))]
    InvalidConnectionString { source: tiberius::error::Error },

    #[snafu(display("Invalid port value: {port}"))]
    FailedToParsePort { port: String },

    #[snafu(display("Invalid parameter value for '{parameter}'"))]
    InvalidParameterValue { parameter: String },
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .secret()
        .description("The MSSQL connection string."),
    ParameterSpec::component("username")
        .secret()
        .description("The MSSQL username for authentication."),
    ParameterSpec::component("password")
        .secret()
        .description("The MSSQL password for authentication."),
    ParameterSpec::component("host").description("The MSSQL host address."),
    ParameterSpec::component("port").description("The MSSQL port number."),
    ParameterSpec::component("database").description("The MSSQL database name."),
    ParameterSpec::component("encrypt").description(
        "Encryption mode ('true', 'false', 'require', 'disable'). Defaults to 'true'.",
    ),
    ParameterSpec::component("trust_server_certificate")
        .description("Whether to trust the server certificate ('true' or 'false')."),
];

/// A catalog connector for MSSQL, providing access to schemas and tables
/// within a SQL Server database.
#[derive(Clone)]
pub struct MssqlCatalog {
    params: ConnectorParams,
}

impl MssqlCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }

    fn create_config(params: &Parameters) -> std::result::Result<Config, Error> {
        if let Some(conn_string) = params.get("connection_string").expose().ok() {
            return Config::from_ado_string(conn_string).context(InvalidConnectionStringSnafu);
        }

        let mut config = Config::default();

        config.authentication(tiberius::AuthMethod::sql_server(
            params
                .get("username")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
            params
                .get("password")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
        ));

        config.host(
            params
                .get("host")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
        );

        if let Some(port_str) = params.get("port").expose().ok() {
            let port = port_str.parse::<u16>().map_err(|_| {
                FailedToParsePortSnafu {
                    port: port_str.to_string(),
                }
                .build()
            })?;
            config.port(port);
        }

        if let Some(database) = params.get("database").expose().ok() {
            config.database(database);
        }

        if let Some(val) = params.get("encrypt").expose().ok() {
            match val.to_lowercase().as_str() {
                "true" | "require" => {
                    config.encryption(EncryptionLevel::Required);
                }
                "false" | "disable" => {
                    config.encryption(EncryptionLevel::Off);
                }
                _ => InvalidParameterValueSnafu {
                    parameter: "encrypt",
                }
                .fail()?,
            }
        } else {
            config.encryption(EncryptionLevel::Required);
        }

        if let Some(val) = params.get("trust_server_certificate").expose().ok() {
            match val.to_lowercase().as_str() {
                "true" => {
                    config.trust_cert();
                }
                "false" => (),
                _ => InvalidParameterValueSnafu {
                    parameter: "trust_server_certificate",
                }
                .fail()?,
            }
        }

        Ok(config)
    }
}

#[async_trait]
impl CatalogConnector for MssqlCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let config = Self::create_config(&self.params.parameters).map_err(|e| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            }
        })?;

        let pool = SqlServerConnectionManager::create(config)
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?;

        let pool = Arc::new(pool);

        let catalog_provider = Arc::new(MssqlCatalogProvider::new(pool, catalog.include.clone()));

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

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    fn make_params(pairs: Vec<(&str, &str)>) -> Parameters {
        Parameters::new(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
                .collect(),
            PREFIX,
            PARAMETERS,
        )
    }

    #[test]
    fn test_create_config_from_connection_string() {
        let params = make_params(vec![(
            "connection_string",
            "Server=localhost,1433;Database=mydb;User Id=sa;Password=pass;",
        )]);
        MssqlCatalog::create_config(&params).expect("should create config from connection string");
    }

    #[test]
    fn test_create_config_from_individual_params() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "my_password"),
            ("host", "localhost"),
            ("port", "1433"),
            ("database", "testdb"),
        ]);
        MssqlCatalog::create_config(&params).expect("should create config from individual params");
    }

    #[test]
    fn test_create_config_missing_username() {
        let params = make_params(vec![("password", "pass"), ("host", "localhost")]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail without username"
        );
    }

    #[test]
    fn test_create_config_missing_password() {
        let params = make_params(vec![("username", "sa"), ("host", "localhost")]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail without password"
        );
    }

    #[test]
    fn test_create_config_missing_host() {
        let params = make_params(vec![("username", "sa"), ("password", "pass")]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail without host"
        );
    }

    #[test]
    fn test_create_config_invalid_port() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("port", "not_a_number"),
        ]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail with invalid port"
        );
    }

    #[test]
    fn test_create_config_encrypt_true() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "true"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept encrypt=true");
    }

    #[test]
    fn test_create_config_encrypt_false() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "false"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept encrypt=false");
    }

    #[test]
    fn test_create_config_encrypt_require() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "require"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept encrypt=require");
    }

    #[test]
    fn test_create_config_encrypt_disable() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "disable"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept encrypt=disable");
    }

    #[test]
    fn test_create_config_invalid_encrypt() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "invalid"),
        ]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail with invalid encrypt value"
        );
    }

    #[test]
    fn test_create_config_trust_server_certificate_true() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("trust_server_certificate", "true"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept trust_server_certificate=true");
    }

    #[test]
    fn test_create_config_trust_server_certificate_false() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("trust_server_certificate", "false"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept trust_server_certificate=false");
    }

    #[test]
    fn test_create_config_invalid_trust_server_certificate() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("trust_server_certificate", "maybe"),
        ]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail with invalid trust_server_certificate"
        );
    }

    #[test]
    fn test_create_config_without_optional_params() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
        ]);
        MssqlCatalog::create_config(&params).expect("should succeed with only required params");
    }

    #[test]
    fn test_create_config_encrypt_case_insensitive() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("encrypt", "TRUE"),
        ]);
        MssqlCatalog::create_config(&params).expect("should accept case-insensitive encrypt value");
    }

    #[test]
    fn test_create_config_port_overflow() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            ("port", "99999"),
        ]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail with port exceeding u16 range"
        );
    }

    #[test]
    fn test_create_config_empty_params() {
        let params = make_params(vec![]);
        assert!(
            MssqlCatalog::create_config(&params).is_err(),
            "should fail with no params at all"
        );
    }
}
