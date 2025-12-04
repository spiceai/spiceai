/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;

use crate::datasets::{AcceleratedTableInfo, DatasetInfo, FederatedTableInfo};
use crate::metrics::MetricsProvider;
use crate::{ParameterSpec, Parameters};

pub type NewDataConnectorResult =
    Result<Arc<dyn DataConnector>, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy)]
pub struct DataConnectorRegistration {
    pub name: &'static str,
    pub constructor: fn() -> Arc<dyn DataConnectorFactory>,
}

impl DataConnectorRegistration {
    pub const fn new(
        name: &'static str,
        constructor: fn() -> Arc<dyn DataConnectorFactory>,
    ) -> Self {
        Self { name, constructor }
    }
}

/// Distributed slice that automatically collects all data connector registrations at link time
/// via the `linkme` crate. Entries are added using the [`register_data_connector!`] macro.
#[linkme::distributed_slice]
pub static DATA_CONNECTOR_REGISTRATIONS: [DataConnectorRegistration] = [..];

/// Registers a data connector factory by name.
///
/// This macro creates a constructor function for the specified connector factory type and
/// registers it in the global distributed slice of data connectors. This allows
/// the runtime to discover and instantiate connectors without updating a central registry.
///
/// # Example (simple form)
///
/// ```
/// register_data_connector!("file", FileFactory);
/// ```
///
/// # Example (explicit form)
///
/// ```
/// register_data_connector!(
///     register_file_connector,
///     FILE_CONNECTOR_REGISTRATION,
///     "file",
///     FileFactory
/// );
/// ```
///
/// Using this macro automatically adds the connector to the distributed slice,
/// making it available for discovery by the runtime.
#[macro_export]
macro_rules! register_data_connector {
    ($fn_name:ident, $static_name:ident, $name:expr, $factory:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::dataconnector::DataConnectorFactory> {
            <$factory>::new_arc()
        }

        #[linkme::distributed_slice($crate::dataconnector::DATA_CONNECTOR_REGISTRATIONS)]
        pub static $static_name: $crate::dataconnector::DataConnectorRegistration =
            $crate::dataconnector::DataConnectorRegistration::new($name, $fn_name);
    };

    ($name:expr, $factory:ident) => {
        ::paste::paste! {
            $crate::register_data_connector!(
                [<__register_data_connector_fn_ $factory:snake>],
                [<__REGISTER_DATA_CONNECTOR_ $factory:upper>],
                $name,
                $factory
            );
        }
    };
}

#[async_trait]
pub trait DataConnectorFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;

    fn supports_unsupported_type_action(&self) -> bool {
        false
    }

    fn prefix(&self) -> &'static str;

    fn parameters(&self) -> &'static [ParameterSpec];

    fn reserved_keywords(&self) -> &'static [&'static str] {
        &[]
    }
}

#[async_trait]
pub trait DataConnector: std::fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }

    async fn read_provider(
        &self,
        dataset: &dyn DatasetInfo,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &dyn DatasetInfo,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(
        &self,
        _federated_table: Arc<dyn FederatedTableInfo>,
    ) -> Option<ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(
        &self,
        _federated_table: Arc<dyn FederatedTableInfo>,
    ) -> Option<ChangesStream> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &dyn DatasetInfo,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn on_accelerated_table_registration(
        &self,
        _dataset: &dyn DatasetInfo,
        _accelerated_table: &mut dyn AcceleratedTableInfo,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        None
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }

    fn prefix(&self) -> Option<&'static str> {
        None
    }
}

pub type DataConnectorResult<T, E = DataConnectorError> = Result<T, E>;

pub type ChangesStream = Pin<Box<dyn TableProviderChangesStream + Send>>;

#[async_trait]
pub trait TableProviderChangesStream: Send + Sync + 'static {
    async fn next_batch(
        self: Pin<&mut Self>,
    ) -> Option<Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    Full,
    Append,
    Cdc,
}

#[expect(dead_code)]
pub struct ConnectorParams {
    pub(crate) parameters: Parameters,
    pub(crate) unsupported_type_action: Option<UnsupportedTypeAction>,
}

#[derive(Clone, Copy, Debug)]
pub enum UnsupportedTypeAction {
    Fail,
    Coerce,
}

#[derive(Debug)]
pub enum DataConnectorError {
    InvalidConnectorType {
        dataconnector: String,
    },
    OdbcNotInstalled,
    UnsupportedTypeAction {
        dataconnector: String,
    },
    UseOfProtectedKeyword {
        dataconnector: String,
        keyword: String,
    },
    InvalidParameter {
        dataconnector: String,
        parameter: String,
        message: String,
    },
    UnableToConnect {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
