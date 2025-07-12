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
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use data_components::oracle::OracleTableProvider;
use data_components::oracle::connection::{
    OracleConnectionParams, OracleConnectionPool, OracleDirectConnectionParamsBuilder,
};
use datafusion::datasource::TableProvider;
use once_cell::sync::OnceCell;
use snafu::{ResultExt, Snafu};
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, future::Future};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    ParameterSpec, Parameters, UnableToGetReadProviderSnafu,
};

const DEFAULT_WALLET_PATH: &str = ".oracle";

// Ensures that the wallet certificate is only saved once, even if multiple datasets
// attempt to initialize concurrently. This avoids race conditions when writing the
// cwallet.sso file and ensures the Oracle OCI client is initialized with a valid wallet.
static WALLET_INIT: OnceCell<()> = OnceCell::new();

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: '{parameter}'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "Failed to connect to the Oracle Server.\nVerify your connection configuration, and try again.\n{source}"
    ))]
    UnableToCreateConnectionPool {
        source: data_components::oracle::Error,
    },

    #[snafu(display(
        "Invalid value provided for the 'port' parameter: {port}.\nSpecify a valid port, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    FailedToParsePort { port: String },

    #[snafu(display("Failed to create wallet directory: {path}.\n{source}"))]
    FailedToCreateWalletDirectory {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to decode wallet certificate from base64.\n{source}"))]
    FailedToDecodeWalletCert { source: base64::DecodeError },

    #[snafu(display("Failed to write wallet certificate file: {path}.\n{source}"))]
    FailedToWriteWalletFile {
        path: String,
        source: std::io::Error,
    },
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("username").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("service_name"),
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("wallet_sso_cert").secret(),
    ParameterSpec::component("wallet"),
];

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Oracle {
    conn: Arc<OracleConnectionPool>,
}

impl Oracle {
    async fn new(params: &Parameters) -> Result<Self> {
        let username = params
            .get("username")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let password = params
            .get("password")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let connect_params: OracleConnectionParams = if let Some(connect_string) =
            params.get("connection_string").expose().ok()
        {
            // verify that no conflicting parameters are used
            for param in ["host", "port", "service_name"] {
                if params.get(param).expose().ok().is_some() {
                    tracing::warn!(
                        "'oracle_{}' parameter is not supported together with 'oracle_connection_string' and will be ignored.",
                        param
                    );
                }
            }

            OracleConnectionParams::new(username, password, connect_string)
        } else {
            let mut conn_params = OracleDirectConnectionParamsBuilder::new(
                params
                    .get("host")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
                username,
                password,
            );

            if let Some(port_str) = params.get("port").expose().ok() {
                let port = port_str.parse::<u16>().map_err(|_| {
                    FailedToParsePortSnafu {
                        port: port_str.to_string(),
                    }
                    .build()
                })?;
                conn_params.port(port);
            }

            if let Some(service_name) = params.get("service_name").expose().ok() {
                conn_params.service_name(service_name);
            }

            conn_params.build()
        };

        // Optional parameter to specify mTLS Wallet directory
        let mut wallet_path_opt = params.get("wallet").expose().ok();

        // If wallet certificate is provided, decode it and save it to the specified or default wallet path
        if let Some(wallet_sso_cert) = params.get("wallet_sso_cert").expose().ok() {
            let wallet_path = wallet_path_opt.unwrap_or(DEFAULT_WALLET_PATH);
            Self::save_wallet_cert_once(wallet_sso_cert, wallet_path)?;
            // Set the wallet path to the one provided or default
            wallet_path_opt = Some(wallet_path);
        }

        let conn = data_components::oracle::connection::connect(&connect_params, wallet_path_opt)
            .await
            .context(UnableToCreateConnectionPoolSnafu)?;

        Ok(Self {
            conn: Arc::new(conn),
        })
    }

    /// Writes the decoded `cwallet.sso` certificate to the specified wallet path.
    /// Ensures safe, single initialization across concurrent dataset connections by guarding
    /// against race conditions using `WALLET_INIT`. If multiple datasets attempt to initialize
    /// the wallet concurrently, only the first call will perform the write and initialization;
    /// subsequent calls will no-op.
    pub fn save_wallet_cert_once(cert_base64_str: &str, wallet_path: &str) -> Result<()> {
        WALLET_INIT.get_or_try_init(|| {
            Self::save_wallet_cert(cert_base64_str, wallet_path)?;
            Ok(())
        })?;
        Ok(())
    }

    /// Save base64-encoded wallet certificate as cwallet.sso file
    fn save_wallet_cert(cert_base64_str: &str, wallet_path: &str) -> Result<()> {
        let wallet_dir = Path::new(wallet_path);

        // Create wallet directory if it doesn't exist
        if !wallet_dir.exists() {
            fs::create_dir_all(wallet_dir).context(FailedToCreateWalletDirectorySnafu {
                path: wallet_path.to_string(),
            })?;
        }

        let cert_data = BASE64_STANDARD
            .decode(cert_base64_str)
            .context(FailedToDecodeWalletCertSnafu)?;

        let wallet_file_path = wallet_dir.join("cwallet.sso");
        fs::write(&wallet_file_path, cert_data).context(FailedToWriteWalletFileSnafu {
            path: wallet_file_path.to_string_lossy().to_string(),
        })?;

        tracing::debug!(
            "Wallet certificate saved at: {}",
            wallet_file_path.to_string_lossy()
        );

        Ok(())
    }
}

#[derive(Default, Copy, Clone)]
pub struct OracleFactory {}

impl OracleFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for OracleFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Oracle::new(&params.parameters).await?) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "oracle"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Oracle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let provider = OracleTableProvider::new(Arc::clone(&self.conn), &dataset.path().into())
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "oracle",
                connector_component: ConnectorComponent::from(dataset),
            })?;

        Ok(Arc::new(provider))
    }
}
