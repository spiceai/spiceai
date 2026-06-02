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

//! Oracle data connector for Spice.ai runtime.
//!
//! This crate provides the Oracle connector implementation, allowing
//! Spice.ai to connect to Oracle databases as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use data_components::oracle::OracleTableProvider;
use data_components::oracle::connection::{
    OracleConnectionParams, OracleConnectionPool, OracleDirectConnectionParamsBuilder,
};
use datafusion::datasource::TableProvider;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use runtime_parameters::Parameters;
use snafu::{ResultExt, Snafu};
use std::any::Any;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

const DEFAULT_WALLET_PATH: &str = ".oracle";

// Ensures that the wallet certificate is only saved once, even if multiple datasets
// attempt to initialize concurrently. This avoids race conditions when writing the
// cwallet.sso file and ensures the Oracle OCI client is initialized with a valid wallet.
// Stores the result of the first initialization attempt (success or error message) to
// prevent repeated retries on failure.
static WALLET_INIT: OnceLock<Mutex<Option<Result<(), String>>>> = OnceLock::new();

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: '{parameter}'. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "Failed to initialize Oracle wallet: A previous initialization failed and the lock is poisoned. Restart the application."
    ))]
    WalletInitializationLockPoisoned,

    #[snafu(display(
        "Failed to initialize Oracle wallet: A previous initialization attempt failed: {message}"
    ))]
    WalletInitializationPreviouslyFailed { message: String },

    #[snafu(display(
        "Failed to connect to the Oracle Server. Verify your connection configuration, and try again. {source}"
    ))]
    UnableToCreateConnectionPool {
        source: data_components::oracle::Error,
    },

    #[snafu(display(
        "Invalid value provided for the 'port' parameter: {port}. Specify a valid port, and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    FailedToParsePort { port: String },

    #[snafu(display("Failed to create wallet directory: {path}. {source}"))]
    FailedToCreateWalletDirectory {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to decode wallet certificate from base64. {source}"))]
    FailedToDecodeWalletCert { source: base64::DecodeError },

    #[snafu(display("Failed to write wallet certificate file: {path}. {source}"))]
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

/// Oracle data connector.
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
    /// subsequent calls will return the cached result (success or error).
    ///
    /// # Errors
    ///
    /// Returns an error if the wallet directory cannot be created, the certificate cannot be
    /// decoded from base64, or the certificate file cannot be written.
    pub fn save_wallet_cert_once(cert_base64_str: &str, wallet_path: &str) -> Result<()> {
        let mutex = WALLET_INIT.get_or_init(|| Mutex::new(None));
        let mut guard = mutex
            .lock()
            .map_err(|_| Error::WalletInitializationLockPoisoned)?;

        match &*guard {
            Some(Ok(())) => Ok(()),
            Some(Err(cached_error)) => Err(Error::WalletInitializationPreviouslyFailed {
                message: cached_error.clone(),
            }),
            None => {
                let result = Self::save_wallet_cert(cert_base64_str, wallet_path);
                match &result {
                    Ok(()) => *guard = Some(Ok(())),
                    Err(e) => *guard = Some(Err(e.to_string())),
                }
                result
            }
        }
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

/// Factory for creating Oracle connector instances.
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
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
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

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "oracle";

/// Returns a new instance of the `Oracle` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    OracleFactory::new_arc()
}

#[derive(Debug, Snafu)]
enum ReadProviderError {
    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: &'static str,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ReadProviderError> for runtime::dataconnector::DataConnectorError {
    fn from(err: ReadProviderError) -> Self {
        match err {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector,
                connector_component,
                source,
            } => runtime::dataconnector::DataConnectorError::UnableToGetReadProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
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
