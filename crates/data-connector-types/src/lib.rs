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

//! The vocabulary every connector-facing crate shares.
//!
//! [`ConnectorComponent`] names *which* component — a dataset or a catalog — a
//! connector is being built for, and [`DataConnectorError`] is what a connector
//! reports about one. They live here, below `data-connector-api`, because
//! crates that a connector *builds on* need the same vocabulary as the contract
//! itself: `data-http-rate-control` reports an invalid rate-control setting as a
//! `DataConnectorError` naming the component it came from, and it sits below the
//! contract, not above it.
//!
//! `data-connector-api` re-exports everything here, so a connector names one
//! crate rather than two.

use std::sync::Arc;

use runtime_component::catalog::CatalogSpec;
use runtime_component::dataset::DatasetSpec;
use snafu::prelude::*;

/// The component (dataset or catalog) a data connector is being built for,
/// carried as its **configuration only**: connectors read a component's
/// spicepod configuration, never the orchestrator it is attached to. Runtime
/// capabilities a connector needs while it is built are reached through the
/// runtime's separate `ConnectorContext` handle instead.
#[derive(Debug, Clone)]
pub enum ConnectorComponent {
    Catalog(Arc<CatalogSpec>),
    Dataset(Arc<DatasetSpec>),
}

impl From<&Arc<DatasetSpec>> for ConnectorComponent {
    fn from(dataset: &Arc<DatasetSpec>) -> Self {
        ConnectorComponent::Dataset(Arc::clone(dataset))
    }
}

impl From<&DatasetSpec> for ConnectorComponent {
    fn from(dataset: &DatasetSpec) -> Self {
        ConnectorComponent::Dataset(Arc::new(dataset.clone()))
    }
}

impl From<&Arc<CatalogSpec>> for ConnectorComponent {
    fn from(catalog: &Arc<CatalogSpec>) -> Self {
        ConnectorComponent::Catalog(Arc::clone(catalog))
    }
}

impl std::fmt::Display for ConnectorComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorComponent::Catalog(catalog) => write!(f, "catalog {}", catalog.name),
            ConnectorComponent::Dataset(dataset) => write!(f, "dataset {}", dataset.name),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DataConnectorError {
    #[snafu(display("Cannot connect to the {connector_component} ({dataconnector}). {source}"))]
    UnableToConnectInternal {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot connect to the {connector_component} ({dataconnector}) on {host}:{port}. Ensure that the host and port are correctly configured in the spicepod, and that the host is reachable."
    ))]
    UnableToConnectInvalidHostOrPort {
        dataconnector: String,
        connector_component: ConnectorComponent,
        host: String,
        port: String,
    },

    #[snafu(display(
        "Cannot connect to the {connector_component} ({dataconnector}). Authentication failed. Ensure that the username and password are correctly configured in the spicepod."
    ))]
    UnableToConnectInvalidUsernameOrPassword {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display(
        "Cannot connect to the {connector_component} ({dataconnector}). A TLS error occurred. Ensure that the corresponding TLS/secure option is configured to match the data connector's TLS security requirements."
    ))]
    UnableToConnectTlsError {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}). {source}"))]
    UnableToGetReadProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}). {source}"))]
    UnableToGetReadWriteProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to set up durable write-back delivery for the {connector_component} ({dataconnector}). {source}"
    ))]
    UnableToGetWriteBackDeliverer {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to setup the {connector_component} ({dataconnector}). {source}"))]
    UnableToGetCatalogProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The {connector_component} ({dataconnector}) has been rate limited. {source}"
    ))]
    RateLimited {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration. {message}"
    ))]
    InvalidConfiguration {
        dataconnector: String,
        connector_component: ConnectorComponent,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration. {source}"
    ))]
    InvalidConfigurationSourceOnly {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration. {message}"
    ))]
    InvalidConfigurationNoSource {
        dataconnector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    // Unlike the InvalidConfiguration* variants, this is a transient (retriable)
    // condition: an object-store source has no data files at the path yet. Object
    // stores are eventually consistent and data is frequently written after the
    // runtime starts, so the dataset load must keep retrying until the files
    // appear rather than failing permanently. See `is_retriable`.
    #[snafu(display(
        "No data files are yet available for the {connector_component} ({dataconnector}). {message} The runtime will keep retrying until the source data becomes available."
    ))]
    ObjectStoreNoFilesAvailable {
        dataconnector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({dataconnector}). The connector '{dataconnector}' is not a valid connector. For details, visit: https://spiceai.org/docs/components/data-connectors"
    ))]
    InvalidConnectorType {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). An invalid glob pattern was provided '{pattern}'. Ensure the glob pattern is valid. {source}"
    ))]
    InvalidGlobPattern {
        dataconnector: String,
        connector_component: ConnectorComponent,
        pattern: String,
        source: globset::Error,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). The table, '{table_name}', was not found. Verify the source table name in the Spicepod configuration."
    ))]
    InvalidTableName {
        dataconnector: String,
        connector_component: ConnectorComponent,
        table_name: String,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). Failed to detect a table schema. Ensure the table, '{table_name}', exists in the data source."
    ))]
    UnableToGetSchema {
        dataconnector: String,
        connector_component: ConnectorComponent,
        table_name: String,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). An unknown Data Connector Error occurred: {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InternalWithSource {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). An internal error occurred in the {dataconnector} Data Connector. Report a bug on GitHub (https://github.com/spiceai/spiceai/issues) and reference the code: {code}"
    ))]
    Internal {
        dataconnector: String,
        connector_component: ConnectorComponent,
        code: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). Failed to infer the table schema. Report a bug on GitHub (https://github.com/spiceai/spiceai/issues) and reference the error: {source}"
    ))]
    UnableToGetSchemaInternal {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). Unsupported type action is not enabled for the {dataconnector} Data Connector. Remove the parameter from your dataset configuration."
    ))]
    UnsupportedTypeAction {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}). The field '{field_name}' has an unsupported data type: {data_type}. Skip loading this field by setting the `unsupported_type_action` parameter to `ignore` or `warn` in the dataset configuration. For details, visit: https://spiceai.org/docs/reference/spicepod/datasets#unsupported_type_action"
    ))]
    UnsupportedDataType {
        dataconnector: String,
        connector_component: ConnectorComponent,
        data_type: String,
        field_name: String,
    },

    #[snafu(display(
        "Failed to initialize the {connector_component} (ODBC). The runtime is built without ODBC support. Build Spice.ai OSS with the `odbc` feature enabled or use the Docker image that includes ODBC support. For details, visit: https://spiceai.org/docs/components/data-connectors/odbc"
    ))]
    OdbcNotInstalled {
        connector_component: ConnectorComponent,
    },

    #[snafu(display(
        "Failed to initialize the {connector_component} ({dataconnector}). This build of Spice.ai does not include the {dataconnector} data connector. Build Spice.ai OSS with the `{feature}` feature enabled, or use the Enterprise distribution of Spice.ai. Learn more at https://docs.spice.ai/docs/enterprise"
    ))]
    ConnectorNotInBuild {
        dataconnector: String,
        feature: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display(
        "Schema mismatch between remote table and acceleration for {dataset_name}. {differences}. The existing accelerated data is available, but updates are disabled. Verify if the remote table schema update is expected and rebuild the acceleration if necessary."
    ))]
    SchemaMismatch {
        dataset_name: String,
        differences: String,
    },

    #[snafu(display(
        "The name '{keyword}' is reserved and cannot be used as a name for a dataset for the {dataconnector} data connector. Change the name in the Spicepod and try again."
    ))]
    UseOfProtectedKeyword {
        dataconnector: String,
        keyword: String,
    },

    #[snafu(display(
        "Insufficient permissions to access the {connector_component} ({dataconnector}). {source}"
    ))]
    InsufficientPermissions {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl DataConnectorError {
    /// Returns `true` if this error is transient and the operation may succeed
    /// on retry. Configuration errors, unsupported type/table errors, and
    /// permission errors are permanent and should not be retried.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        !matches!(
            self,
            Self::InvalidConfiguration { .. }
                | Self::InvalidConfigurationSourceOnly { .. }
                | Self::InvalidConfigurationNoSource { .. }
                | Self::InvalidConnectorType { .. }
                | Self::InvalidGlobPattern { .. }
                | Self::InvalidTableName { .. }
                | Self::InsufficientPermissions { .. }
                | Self::UnableToConnectInvalidHostOrPort { .. }
                | Self::UnableToConnectInvalidUsernameOrPassword { .. }
                | Self::UnableToConnectTlsError { .. }
                | Self::UnsupportedTypeAction { .. }
                | Self::UnsupportedDataType { .. }
                | Self::OdbcNotInstalled { .. }
                | Self::ConnectorNotInBuild { .. }
                | Self::UseOfProtectedKeyword { .. }
        )
    }
}

pub type Result<T, E = DataConnectorError> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type DataConnectorResult<T> = std::result::Result<T, DataConnectorError>;
