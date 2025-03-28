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

use crate::component::dataset::Dataset;
use crate::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult,
};
use crate::parameters::{ParameterSpec, Parameters};
use async_trait::async_trait;
use data_components::mongodb::MongoDBTableProvider;
use datafusion::catalog::TableProvider;
use mongodb::options::{ClientOptions, Credential, ServerAddress, Tls, TlsOptions};
use mongodb::Client;
use regex::Regex;
use snafu::Snafu;
use std::any::Any;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse the connection string.\nVerify the connection string is valid, and try again."))]
    InvalidConnectionString,

    #[snafu(display("Failed to parse database_name and collection_name. The format of `from` field is `mongodb:database_name.collection_name`"))]
    InvalidDatasetFormat,

    #[snafu(display("host is required"))]
    HostIsMissing,
}

const TLS_INVALID_HOSTNAMES_CONN_OPTION: &str = r"(?i:(tls|ssl)allowinvalidhostnames)=true";
static TLS_INVALID_HOSTNAMES_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Regex::new(TLS_INVALID_HOSTNAMES_CONN_OPTION).expect(
        "tlsAllowInvalidHostnames(sslAllowInvalidHostnames) connection option regex should build",
    )
});

const CA_FILE_PATH_CONN_OPTION: &str = r"(tls|ssl)CAFile=([^&]+)";
static CA_FILE_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Regex::new(CA_FILE_PATH_CONN_OPTION)
        .expect("tlsCAFile(sslCAFile) connection option regex should build")
});

const TLS_INVALID_HOSTNAMES_WITH_AMPERSAND_CONN_OPTION: &str =
    r"(&)?(?i:(tls|ssl)allowinvalidhostnames)=true";
static TLS_INVALID_HOSTNAMES_WITH_AMPERSAND_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Regex::new(TLS_INVALID_HOSTNAMES_WITH_AMPERSAND_CONN_OPTION)
        .expect("&tlsAllowInvalidHostnames(&sslAllowInvalidHostnames) should build")
});

pub struct MongoDB {
    params: Parameters,
}

impl MongoDB {
    async fn parse_connection_string(
        connection_string: String,
        dataset: &Dataset,
    ) -> DataConnectorResult<ClientOptions> {
        let mut client_options;

        let is_allowing_tls_invalid_hostnames =
            TLS_INVALID_HOSTNAMES_REGEX.is_match(connection_string.as_str());

        if is_allowing_tls_invalid_hostnames {
            // suppose user is trying to connect documentdb through ssh tunneling
            let parsable_connection_string =
                Self::remove_tls_allow_invalid_hostnames_option(connection_string.as_str());

            client_options = ClientOptions::parse(parsable_connection_string)
                .await
                .map_err(|_| DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "mongodb".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(Error::InvalidConnectionString {}),
                })?;

            // parsing tlsAllowInvalidHostnames(sslAllowInvalidHostnames) option in connection string
            // is not supported by ClientOptions::parse() function,
            // so injecting the option through TlsOptions is needed.
            if let Some(_tls_options) = client_options.tls.take() {
                let ca_file_path =
                    Self::get_ca_file_path(connection_string.as_str()).unwrap_or_default();
                let new_tls = TlsOptions::builder()
                    .ca_file_path(Some(PathBuf::from(ca_file_path)))
                    .allow_invalid_hostnames(true)
                    .build();

                client_options.tls = Some(Tls::Enabled(new_tls));
                client_options.direct_connection = Some(true); // to make connection directly to 'localhost', not amazon domain which leads to connection failure.
            }
        } else {
            client_options = ClientOptions::parse(connection_string).await.map_err(|_| {
                DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "mongodb".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(Error::InvalidConnectionString {}),
                }
            })?;
        }

        Ok(client_options)
    }

    fn get_ca_file_path(connection_string: &str) -> Option<String> {
        CA_FILE_PATH_REGEX
            .captures(connection_string)
            .and_then(|caps| caps.get(2).map(|m| m.as_str().to_string()))
    }

    fn remove_tls_allow_invalid_hostnames_option(connection_string: &str) -> String {
        TLS_INVALID_HOSTNAMES_WITH_AMPERSAND_REGEX
            .replace_all(connection_string, "")
            .to_string()
    }

    fn parse_params(params: &Parameters, dataset: &Dataset) -> DataConnectorResult<ClientOptions> {
        let host = params
            .get("host")
            .expose()
            .ok_or_else(|_| DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "mongodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(Error::HostIsMissing {}),
            })?
            .to_string();

        let port = params
            .get("port")
            .expose()
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(27017); // default port of mongodb

        let username = params
            .get("username")
            .expose()
            .ok()
            .map(ToString::to_string);

        let password = params
            .get("password")
            .expose()
            .ok()
            .map(ToString::to_string);

        let auth_source = params
            .get("auth_source")
            .expose()
            .ok()
            .map(ToString::to_string);

        Ok(ClientOptions::builder()
            .hosts(vec![ServerAddress::Tcp {
                host,
                port: Some(port),
            }])
            .credential(
                Credential::builder()
                    .username(username)
                    .password(password)
                    .source(auth_source) // `--authenticationDatabase` in mongodb shell, `authSource` in connection string
                    .build(),
            )
            .build())
    }
}

#[derive(Default, Copy, Clone)]
pub struct MongoDBFactory {}

impl MongoDBFactory {
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
    ParameterSpec::component("connection_string").secret(), // especially recommended when connecting to cloud environment (MongoDB Atlas, Amazon DocumentDB)
    ParameterSpec::component("username").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("auth_source"), // `authSource` in connection string
    ParameterSpec::component("query_body"),
];

impl DataConnectorFactory for MongoDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mongodb = MongoDB {
                params: params.parameters,
            };
            Ok(Arc::new(mongodb) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mongodb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for MongoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let mut db_and_collection = path.split('.');

        let (Some(database), Some(collection)) = (db_and_collection.next(), db_and_collection.next()) else {
                return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "mongodb".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(Error::InvalidDatasetFormat {}),
                })
            };

        let connection_string = self
            .params
            .get("connection_string")
            .expose()
            .ok()
            .map(ToString::to_string);

        let client_options = if let Some(connection_string) = connection_string {
            Self::parse_connection_string(connection_string, dataset).await?
        } else {
            Self::parse_params(&self.params, dataset)?
        };

        let client = Client::with_options(client_options).map_err(|e| {
            DataConnectorError::UnableToConnectInternal {
                dataconnector: "mongodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })?;

        let query_body = self.params.get("query_body").expose().ok().unwrap_or("{}"); // empty query body means selecting all fields in collection

        let provider = MongoDBTableProvider::try_new(
            Arc::new(client),
            Arc::from(database),
            Arc::from(collection),
            Arc::from(query_body),
        )
        .await
        .map_err(|e| DataConnectorError::UnableToGetReadProvider {
            dataconnector: "mongodb".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;

        Ok(Arc::new(provider))
    }
}
