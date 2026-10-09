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
use crate::{
    Runtime,
    component::catalog::{Catalog, table_selector},
    dataconnector::parameters::ConnectorParams,
};
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

        let catalog_provider = Arc::new(MssqlCatalogProvider::new(pool, table_selector(catalog)));

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

    /// The exact `Debug` rendering of a config built from individual params for
    /// user `sa` on host `localhost`. tiberius keeps its `Config` fields
    /// crate-private, so this is how a test reads back every mapped value.
    fn individual_config_debug(
        port: Option<u16>,
        database: Option<&str>,
        encryption: &str,
        trust: &str,
    ) -> String {
        format!(
            "Config {{ host: Some(\"localhost\"), port: {port:?}, database: {database:?}, \
             instance_name: None, application_name: None, encryption: {encryption}, \
             trust: {trust}, auth: SqlServer(SqlServerAuth {{ user: \"sa\", password: \"<HIDDEN>\" }}), \
             readonly: false }}"
        )
    }

    fn required_params_with(extra: (&'static str, &'static str)) -> Parameters {
        make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
            extra,
        ])
    }

    fn expect_missing_parameter(params: &Parameters, expected: &str) {
        let err = MssqlCatalog::create_config(params)
            .expect_err("create_config should fail when a required parameter is absent");
        assert!(
            matches!(&err, Error::MissingParameter { parameter } if parameter == expected),
            "expected MissingParameter for '{expected}', got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!("Missing required parameter: '{expected}'. Specify a value.")
        );
    }

    fn expect_invalid_parameter_value(params: &Parameters, expected: &str) {
        let err = MssqlCatalog::create_config(params)
            .expect_err("create_config should reject an unrecognized parameter value");
        assert!(
            matches!(&err, Error::InvalidParameterValue { parameter } if parameter == expected),
            "expected InvalidParameterValue for '{expected}', got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!("Invalid parameter value for '{expected}'")
        );
    }

    fn expect_failed_to_parse_port(params: &Parameters, expected: &str) {
        let err = MssqlCatalog::create_config(params)
            .expect_err("create_config should reject a port that is not a u16");
        assert!(
            matches!(&err, Error::FailedToParsePort { port } if port == expected),
            "expected FailedToParsePort for '{expected}', got {err:?}"
        );
        assert_eq!(err.to_string(), format!("Invalid port value: {expected}"));
    }

    #[test]
    fn test_create_config_from_connection_string() {
        let params = make_params(vec![(
            "connection_string",
            "Server=localhost,1433;Database=mydb;User Id=sa;Password=pass;",
        )]);
        let config = MssqlCatalog::create_config(&params)
            .expect("should create config from connection string");
        assert_eq!(config.get_addr(), "localhost:1433");
        let debug = format!("{config:?}");
        assert!(
            debug.starts_with(
                "Config { host: Some(\"localhost\"), port: Some(1433), database: Some(\"mydb\"), "
            ),
            "host, port and database should come from the connection string, got {debug}"
        );
        assert!(
            debug.contains(
                "auth: SqlServer(SqlServerAuth { user: \"sa\", password: \"<HIDDEN>\" })"
            ),
            "the connection string's 'User Id' should authenticate, got {debug}"
        );
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
        let config = MssqlCatalog::create_config(&params)
            .expect("should create config from individual params");
        assert_eq!(config.get_addr(), "localhost:1433");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(Some(1433), Some("testdb"), "Required", "Default")
        );
    }

    #[test]
    fn test_create_config_missing_username() {
        let params = make_params(vec![("password", "pass"), ("host", "localhost")]);
        expect_missing_parameter(&params, "mssql_username");
    }

    #[test]
    fn test_create_config_missing_password() {
        let params = make_params(vec![("username", "sa"), ("host", "localhost")]);
        expect_missing_parameter(&params, "mssql_password");
    }

    #[test]
    fn test_create_config_missing_host() {
        let params = make_params(vec![("username", "sa"), ("password", "pass")]);
        expect_missing_parameter(&params, "mssql_host");
    }

    #[test]
    fn test_create_config_invalid_port() {
        let params = required_params_with(("port", "not_a_number"));
        expect_failed_to_parse_port(&params, "not_a_number");
    }

    #[test]
    fn test_create_config_encrypt_true() {
        let params = required_params_with(("encrypt", "true"));
        let config = MssqlCatalog::create_config(&params).expect("should accept encrypt=true");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Required", "Default")
        );
    }

    #[test]
    fn test_create_config_encrypt_false() {
        let params = required_params_with(("encrypt", "false"));
        let config = MssqlCatalog::create_config(&params).expect("should accept encrypt=false");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Off", "Default")
        );
    }

    #[test]
    fn test_create_config_encrypt_require() {
        let params = required_params_with(("encrypt", "require"));
        let config = MssqlCatalog::create_config(&params).expect("should accept encrypt=require");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Required", "Default")
        );
    }

    #[test]
    fn test_create_config_encrypt_disable() {
        let params = required_params_with(("encrypt", "disable"));
        let config = MssqlCatalog::create_config(&params).expect("should accept encrypt=disable");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Off", "Default")
        );
    }

    #[test]
    fn test_create_config_invalid_encrypt() {
        let params = required_params_with(("encrypt", "invalid"));
        expect_invalid_parameter_value(&params, "encrypt");
    }

    #[test]
    fn test_create_config_trust_server_certificate_true() {
        let params = required_params_with(("trust_server_certificate", "true"));
        let config = MssqlCatalog::create_config(&params)
            .expect("should accept trust_server_certificate=true");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Required", "TrustAll")
        );
    }

    #[test]
    fn test_create_config_trust_server_certificate_false() {
        let params = required_params_with(("trust_server_certificate", "false"));
        let config = MssqlCatalog::create_config(&params)
            .expect("should accept trust_server_certificate=false");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Required", "Default")
        );
    }

    #[test]
    fn test_create_config_invalid_trust_server_certificate() {
        let params = required_params_with(("trust_server_certificate", "maybe"));
        expect_invalid_parameter_value(&params, "trust_server_certificate");
    }

    #[test]
    fn test_create_config_without_optional_params() {
        let params = make_params(vec![
            ("username", "sa"),
            ("password", "pass"),
            ("host", "localhost"),
        ]);
        let config =
            MssqlCatalog::create_config(&params).expect("should succeed with only required params");
        // The secure defaults: the SQL Server port, TLS required, and the
        // server certificate validated.
        assert_eq!(config.get_addr(), "localhost:1433");
        assert_eq!(
            format!("{config:?}"),
            individual_config_debug(None, None, "Required", "Default")
        );
    }

    #[test]
    fn test_create_config_encrypt_case_insensitive() {
        for (raw, expected_encryption) in [("TRUE", "Required"), ("FALSE", "Off")] {
            let params = required_params_with(("encrypt", raw));
            let config = MssqlCatalog::create_config(&params)
                .unwrap_or_else(|e| panic!("should accept encrypt={raw}: {e}"));
            assert_eq!(
                format!("{config:?}"),
                individual_config_debug(None, None, expected_encryption, "Default"),
                "encrypt={raw}"
            );
        }
    }

    #[test]
    fn test_create_config_port_overflow() {
        let params = required_params_with(("port", "99999"));
        expect_failed_to_parse_port(&params, "99999");

        // The largest u16 is the last accepted port.
        let params = required_params_with(("port", "65535"));
        let config = MssqlCatalog::create_config(&params).expect("port 65535 should be accepted");
        assert_eq!(config.get_addr(), "localhost:65535");
    }
}
