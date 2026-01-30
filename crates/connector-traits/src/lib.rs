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

use arrow_schema::Schema;
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::cdc::ChangesStream;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion_datasource::metadata::MetadataColumn;
use datafusion_table_providers::UnsupportedTypeAction;
use globset::GlobSet;
use linkme::distributed_slice;
use runtime_secrets::Secrets;
use snafu::prelude::*;
use spicepod::semantic::Column;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use token_provider::registry::TokenProviderRegistry;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub use runtime_parameters::{ExposedParamLookup, ParamLookup, ParameterSpec, Parameters};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentType {
    Dataset,
    DatasetAccelerator,
    Catalog,
    Model,
    Embedding,
    Tool,
    Eval,
    View,
}

impl Display for ComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentType::Dataset => write!(f, "dataset"),
            ComponentType::DatasetAccelerator => write!(f, "dataset_accelerator"),
            ComponentType::Catalog => write!(f, "catalog"),
            ComponentType::Model => write!(f, "model"),
            ComponentType::Embedding => write!(f, "embedding"),
            ComponentType::Tool => write!(f, "tool"),
            ComponentType::Eval => write!(f, "eval"),
            ComponentType::View => write!(f, "view"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    ObservableCounterU64,
    ObservableGaugeU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricSpec {
    pub name: &'static str,
    pub metric_type: MetricType,
    pub description: Option<&'static str>,
    pub unit: Option<&'static str>,
}

impl MetricSpec {
    #[must_use]
    pub const fn new(name: &'static str, metric_type: MetricType) -> Self {
        Self {
            name,
            metric_type,
            description: None,
            unit: None,
        }
    }

    #[must_use]
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    #[must_use]
    pub const fn unit(mut self, unit: &'static str) -> Self {
        self.unit = Some(unit);
        self
    }
}

pub trait MetricsProviderComponent: Debug + Send + Sync + 'static {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>>;
}

pub trait MetricsProvider: Debug + Send + Sync + 'static {
    fn component_type(&self) -> ComponentType;
    fn component_name(&self) -> &'static str;
    fn available_metrics(&self) -> &'static [MetricSpec];
    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<opentelemetry::KeyValue>,
    ) -> Option<ObserveMetricCallback>;
}

pub enum ObserveMetricCallback {
    U64(opentelemetry::metrics::Callback<u64>),
    I64(opentelemetry::metrics::Callback<i64>),
    F64(opentelemetry::metrics::Callback<f64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentInitialization {
    OnStartup(StartupOptions),
    OnTrigger,
}

impl Default for ComponentInitialization {
    fn default() -> Self {
        Self::OnStartup(StartupOptions::default())
    }
}

impl ComponentInitialization {
    #[must_use]
    pub fn is_on_trigger(&self) -> bool {
        matches!(self, ComponentInitialization::OnTrigger)
    }

    #[must_use]
    pub fn is_dataset_health_monitor_enabled(&self) -> bool {
        match self {
            ComponentInitialization::OnStartup(options) => {
                options.dataset_health_monitor == DatasetHealthMonitor::Enabled
            }
            ComponentInitialization::OnTrigger => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatasetHealthMonitor {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartupOptions {
    pub dataset_health_monitor: DatasetHealthMonitor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    Disabled,
    Full,
    Append,
    Changes,
    Caching,
}

pub trait ConnectorApp: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    /// Returns the max Flight message size override.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime parameters cannot be read.
    fn flight_max_message_size_bytes(&self) -> AnyErrorResult<Option<usize>>;
}

#[async_trait]
pub trait ConnectorRuntime: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn token_provider_registry(&self) -> Arc<TokenProviderRegistry>;

    fn secrets(&self) -> Arc<RwLock<Secrets>>;

    fn tokio_io_runtime(&self) -> Handle;

    async fn runtime_param(&self, key: &str) -> AnyErrorResult<Option<String>>;
}

pub trait ConnectorDataset: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn name(&self) -> &TableReference;

    fn table_name(&self) -> &str;

    fn from(&self) -> &str;

    fn path(&self) -> &str;

    fn params(&self) -> &HashMap<String, String>;

    fn get_param(&self, param: &str) -> Option<&str> {
        self.params().get(param).map(String::as_str)
    }

    fn columns(&self) -> &[Column];

    fn metadata(&self) -> &HashMap<String, String>;

    fn time_column(&self) -> Option<&str>;

    fn time_partition_column(&self) -> Option<&str>;

    fn has_metadata_table(&self) -> bool;

    fn is_accelerated(&self) -> bool;

    fn is_file_accelerated(&self) -> bool;

    fn acceleration_configured(&self) -> bool;

    fn acceleration_params(&self) -> Option<&HashMap<String, String>>;

    fn refresh_mode(&self) -> Option<RefreshMode>;

    fn refresh_sql(&self) -> Option<String>;

    fn listing_table_metadata_columns(
        &self,
        url_prefix: Arc<str>,
        schema: &Schema,
    ) -> Option<Vec<MetadataColumn>> {
        let needs_last_modified = self.needs_last_modified(schema);
        if !needs_last_modified && self.metadata().is_empty() {
            return None;
        }

        let mut columns = Vec::new();

        if self.metadata_column_enabled(MetadataColumn::LastModified.name(), schema)
            || needs_last_modified
        {
            columns.push(MetadataColumn::LastModified);
        }

        if self.metadata_column_enabled(MetadataColumn::Location(None).name(), schema) {
            columns.push(MetadataColumn::Location(Some(url_prefix)));
        }

        if self.metadata_column_enabled(MetadataColumn::Size.name(), schema) {
            columns.push(MetadataColumn::Size);
        }

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    /// Parses the dataset path into a [`TableReference`].
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset path cannot be parsed.
    fn parse_path(
        &self,
        case_sensitive: bool,
        dialect: Option<&dyn Dialect>,
    ) -> AnyErrorResult<TableReference>;

    fn needs_last_modified(&self, schema: &Schema) -> bool {
        let needs_last_modified_time_col = self
            .time_column()
            .is_some_and(|col| col == MetadataColumn::LastModified.name())
            || self
                .time_partition_column()
                .is_some_and(|col| col == MetadataColumn::LastModified.name());

        needs_last_modified_time_col
            && schema
                .fields()
                .find(MetadataColumn::LastModified.name())
                .is_none()
    }

    fn metadata_column_enabled(&self, column: &str, schema: &Schema) -> bool {
        self.metadata()
            .get(column)
            .is_some_and(|val| val == "enabled")
            && schema.fields().find(column).is_none()
    }
}

pub trait ConnectorCatalog: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn name(&self) -> &str;

    fn catalog_id(&self) -> Option<&str>;

    fn from(&self) -> &str;

    fn params(&self) -> &HashMap<String, String>;

    fn dataset_params(&self) -> &HashMap<String, String>;

    fn include(&self) -> Option<&GlobSet>;
}

pub trait ConnectorFederatedTable: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

pub trait ConnectorAcceleratedTable: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug, Clone)]
pub struct ConnectorComponentDataset {
    pub name: String,
    pub table_name: String,
    pub from: String,
}

#[derive(Debug, Clone)]
pub struct ConnectorComponentCatalog {
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum ConnectorComponent {
    Catalog(ConnectorComponentCatalog),
    Dataset(ConnectorComponentDataset),
}

impl ConnectorComponent {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            ConnectorComponent::Catalog(catalog) => catalog.name.as_str(),
            ConnectorComponent::Dataset(dataset) => dataset.name.as_str(),
        }
    }

    #[must_use]
    pub fn dataset_table_name(&self) -> Option<&str> {
        match self {
            ConnectorComponent::Dataset(dataset) => Some(dataset.table_name.as_str()),
            ConnectorComponent::Catalog(_) => None,
        }
    }
}

impl From<&dyn ConnectorDataset> for ConnectorComponent {
    fn from(dataset: &dyn ConnectorDataset) -> Self {
        ConnectorComponent::Dataset(ConnectorComponentDataset {
            name: dataset.name().to_string(),
            table_name: dataset.table_name().to_string(),
            from: dataset.from().to_string(),
        })
    }
}

impl From<&dyn ConnectorCatalog> for ConnectorComponent {
    fn from(catalog: &dyn ConnectorCatalog) -> Self {
        ConnectorComponent::Catalog(ConnectorComponentCatalog {
            name: catalog.name().to_string(),
        })
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
}

pub type Result<T, E = DataConnectorError> = std::result::Result<T, E>;
pub type DataConnectorResult<T> = std::result::Result<T, DataConnectorError>;
pub type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

#[derive(Clone)]
pub struct ConnectorParams {
    pub parameters: Parameters,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub component: ConnectorComponent,
    pub app: Option<Arc<dyn ConnectorApp>>,
    pub runtime: Option<Arc<dyn ConnectorRuntime>>,
    pub io_runtime: Handle,
}

#[async_trait]
pub trait DataConnector: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }

    async fn read_provider(
        &self,
        dataset: &dyn ConnectorDataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &dyn ConnectorDataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(
        &self,
        _federated_table: Arc<dyn ConnectorFederatedTable>,
        _dataset: &dyn ConnectorDataset,
    ) -> Option<ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(
        &self,
        _federated_table: Arc<dyn ConnectorFederatedTable>,
    ) -> Option<ChangesStream> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &dyn ConnectorDataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn on_accelerated_table_registration(
        &self,
        _dataset: &dyn ConnectorDataset,
        _accelerated_table: &mut dyn ConnectorAcceleratedTable,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        None
    }

    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::default()
    }

    fn initialization_for_dataset(
        &self,
        _dataset: &dyn ConnectorDataset,
    ) -> ComponentInitialization {
        self.initialization()
    }
}

impl<T: DataConnector + Debug + 'static> MetricsProviderComponent for T {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.metrics_provider()
    }
}

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

#[derive(Debug, Snafu)]
#[snafu(context(suffix(Catalog)))]
pub enum CatalogConnectorError {
    #[snafu(display("Failed to setup the {connector_component} ({connector}). {source}"))]
    UnableToGetCatalogProvider {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({connector}) with an invalid configuration. {message}"
    ))]
    InvalidConfiguration {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Cannot setup the {connector_component} ({connector}) with an invalid configuration. {message}"
    ))]
    InvalidConfigurationNoSource {
        connector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({connector}). An unknown Catalog Connector Error occurred: {source}"
    ))]
    InternalWithSource {
        connector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to initiate catalog, app reference cannot be obtained from the runtime."
    ))]
    FailedToGetAppFromRuntime {},

    #[snafu(display("Failed to start a catalog refresh task. The task is already running."))]
    RefreshTaskAlreadyStarted {},
}

pub type CatalogConnectorResult<T, E = CatalogConnectorError> = std::result::Result<T, E>;

#[async_trait]
pub trait CatalogConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<dyn ConnectorRuntime>,
        catalog: &dyn ConnectorCatalog,
    ) -> CatalogConnectorResult<Arc<dyn RefreshableCatalogProvider>>;

    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::default()
    }
}

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

#[distributed_slice]
pub static DATA_CONNECTOR_REGISTRATIONS: [DataConnectorRegistration] = [..];

#[macro_export]
macro_rules! register_data_connector {
    ($fn_name:ident, $static_name:ident, $name:expr, $factory:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::DataConnectorFactory> {
            <$factory>::new_arc()
        }

        #[linkme::distributed_slice($crate::DATA_CONNECTOR_REGISTRATIONS)]
        pub static $static_name: $crate::DataConnectorRegistration =
            $crate::DataConnectorRegistration::new($name, $fn_name);
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

#[derive(Clone, Copy)]
pub struct CatalogConnectorRegistration {
    pub name: &'static str,
    pub constructor: fn(ConnectorParams) -> Arc<dyn CatalogConnector>,
    pub prefix: &'static str,
    pub parameters: &'static [ParameterSpec],
}

impl CatalogConnectorRegistration {
    pub const fn new(
        name: &'static str,
        constructor: fn(ConnectorParams) -> Arc<dyn CatalogConnector>,
        prefix: &'static str,
        parameters: &'static [ParameterSpec],
    ) -> Self {
        Self {
            name,
            constructor,
            prefix,
            parameters,
        }
    }
}

#[distributed_slice]
pub static CATALOG_CONNECTOR_REGISTRATIONS: [CatalogConnectorRegistration] = [..];

#[macro_export]
macro_rules! register_catalog_connector {
    ($fn_name:ident, $static_name:ident, $name:expr, $prefix:expr, $parameters:expr, $factory:path) => {
        fn $fn_name(
            params: $crate::ConnectorParams,
        ) -> ::std::sync::Arc<dyn $crate::CatalogConnector> {
            $factory(params)
        }

        #[linkme::distributed_slice($crate::CATALOG_CONNECTOR_REGISTRATIONS)]
        pub static $static_name: $crate::CatalogConnectorRegistration =
            $crate::CatalogConnectorRegistration::new($name, $fn_name, $prefix, $parameters);
    };

    ($name:expr, $factory:path, $prefix:expr, $parameters:expr) => {
        ::paste::paste! {
            $crate::register_catalog_connector!(
                [<__register_catalog_connector_fn_ $factory:snake>],
                [<__REGISTER_CATALOG_CONNECTOR_ $factory:upper>],
                $name,
                $prefix,
                $parameters,
                $factory
            );
        }
    };
}

/// Creates a default reqwest client with standard Spice settings.
///
/// # Errors
///
/// Returns an error if the client cannot be built.
pub fn default_spice_client(content_type: &'static str) -> reqwest::Result<reqwest::Client> {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    let mut headers = HeaderMap::new();
    headers.append(CONTENT_TYPE, HeaderValue::from_static(content_type));

    reqwest::Client::builder()
        .user_agent("spice")
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .default_headers(headers)
        .build()
}
