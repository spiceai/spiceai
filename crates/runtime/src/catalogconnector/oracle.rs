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

//! Oracle catalog connector.
//!
//! Connects to an Oracle database and provides schema/table
//! discovery via Oracle dictionary view queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::oracle::connection::{
    OracleConnectionParams, OracleConnectionPool, OracleDirectConnectionParamsBuilder,
};
use data_components::oracle::provider::OracleCatalogProvider;
use runtime_parameters::Parameters;
use snafu::prelude::*;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "oracle";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: '{parameter}'. Specify a value."))]
    MissingParameter { parameter: String },

    #[snafu(display("Failed to create Oracle connection pool: {source}"))]
    UnableToCreateConnectionPool {
        source: data_components::oracle::Error,
    },

    #[snafu(display("Invalid port value: {port}"))]
    FailedToParsePort { port: String },
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .secret()
        .description("The Oracle connection string."),
    ParameterSpec::component("username")
        .secret()
        .description("The Oracle username for authentication."),
    ParameterSpec::component("password")
        .secret()
        .description("The Oracle password for authentication."),
    ParameterSpec::component("host").description("The Oracle host address."),
    ParameterSpec::component("port").description("The Oracle port number."),
    ParameterSpec::component("service_name").description("The Oracle service name."),
];

/// A catalog connector for Oracle, providing access to schemas and tables
/// within an Oracle database.
#[derive(Clone)]
pub struct OracleCatalog {
    params: ConnectorParams,
}

impl OracleCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }

    fn build_connection_params(
        params: &Parameters,
    ) -> std::result::Result<OracleConnectionParams, Error> {
        let username = params
            .get("username")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let password = params
            .get("password")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        if let Some(connection_string) = params.get("connection_string").expose().ok() {
            return Ok(OracleConnectionParams::new(
                username,
                password,
                connection_string,
            ));
        }

        let host = params
            .get("host")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let mut builder = OracleDirectConnectionParamsBuilder::new(host, username, password);

        if let Some(port_str) = params.get("port").expose().ok() {
            let port = port_str.parse::<u16>().map_err(|_| {
                FailedToParsePortSnafu {
                    port: port_str.to_string(),
                }
                .build()
            })?;
            builder.port(port);
        }

        if let Some(service_name) = params.get("service_name").expose().ok() {
            builder.service_name(service_name);
        }

        Ok(builder.build())
    }
}

#[async_trait]
impl CatalogConnector for OracleCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let conn_params = Self::build_connection_params(&self.params.parameters).map_err(|e| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            }
        })?;

        let pool = data_components::oracle::connection::connect(&conn_params, None)
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?;

        let pool = Arc::new(pool);

        let catalog_provider = Arc::new(OracleCatalogProvider::new(pool, catalog.include.clone()));

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
    fn test_build_connection_params_from_connection_string() {
        let params = make_params(vec![
            ("username", "admin"),
            ("password", "pass"),
            ("connection_string", "//myhost:1521/ORCLPDB1"),
        ]);
        let conn =
            OracleCatalog::build_connection_params(&params).expect("should build from conn str");
        assert_eq!(conn.connect_string, "//myhost:1521/ORCLPDB1");
        assert_eq!(conn.username, "admin");
        assert_eq!(conn.password, "pass");
    }

    #[test]
    fn test_build_connection_params_from_individual_params() {
        let params = make_params(vec![
            ("username", "admin"),
            ("password", "pass"),
            ("host", "myhost"),
            ("port", "1521"),
            ("service_name", "ORCLPDB1"),
        ]);
        let conn = OracleCatalog::build_connection_params(&params)
            .expect("should build from individual params");
        assert_eq!(conn.connect_string, "//myhost:1521/ORCLPDB1");
    }

    #[test]
    fn test_build_connection_params_defaults() {
        let params = make_params(vec![
            ("username", "admin"),
            ("password", "pass"),
            ("host", "myhost"),
        ]);
        let conn =
            OracleCatalog::build_connection_params(&params).expect("should build with defaults");
        // Default port 1521, default service XEPDB1
        assert_eq!(conn.connect_string, "//myhost:1521/XEPDB1");
    }

    #[test]
    fn test_build_connection_params_missing_username() {
        let params = make_params(vec![("password", "pass"), ("host", "myhost")]);
        assert!(
            OracleCatalog::build_connection_params(&params).is_err(),
            "should fail without username"
        );
    }

    #[test]
    fn test_build_connection_params_missing_password() {
        let params = make_params(vec![("username", "admin"), ("host", "myhost")]);
        assert!(
            OracleCatalog::build_connection_params(&params).is_err(),
            "should fail without password"
        );
    }

    #[test]
    fn test_build_connection_params_missing_host() {
        let params = make_params(vec![("username", "admin"), ("password", "pass")]);
        assert!(
            OracleCatalog::build_connection_params(&params).is_err(),
            "should fail without host or connection_string"
        );
    }

    #[test]
    fn test_build_connection_params_invalid_port() {
        let params = make_params(vec![
            ("username", "admin"),
            ("password", "pass"),
            ("host", "myhost"),
            ("port", "not_a_number"),
        ]);
        assert!(
            OracleCatalog::build_connection_params(&params).is_err(),
            "should fail with invalid port"
        );
    }
}
