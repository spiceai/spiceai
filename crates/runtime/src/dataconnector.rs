/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::component::catalog::Catalog;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::Dataset;
use crate::secrets::Secrets;
use crate::Runtime;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use data_components::object::metadata::ObjectStoreMetadataTable;
use data_components::object::text::ObjectStoreTextTable;
use datafusion::catalog::CatalogProvider;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::sql::TableReference;
use lazy_static::lazy_static;
use object_store::ObjectStore;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use url::Url;

use std::future::Future;

use crate::object_store_registry::default_runtime_env;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "debezium")]
pub mod debezium;
#[cfg(feature = "delta_lake")]
pub mod delta_lake;
#[cfg(feature = "dremio")]
pub mod dremio;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod file;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "ftp")]
pub mod ftp;
pub mod graphql;
pub mod https;
pub mod localhost;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod s3;
#[cfg(feature = "ftp")]
pub mod sftp;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "spark")]
pub mod spark;
pub mod spiceai;
#[cfg(feature = "delta_lake")]
pub mod unity_catalog;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to construct logical plan builder: {source}"))]
    UnableToConstructLogicalPlanBuilder {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to build logical plan: {source}"))]
    UnableToBuildLogicalPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to register table provider: {source}"))]
    UnableToRegisterTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to create data frame: {source}"))]
    UnableToCreateDataFrame {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to filter data frame: {source}"))]
    UnableToFilterDataFrame {
        source: datafusion::error::DataFusionError,
    },
}

#[derive(Debug, Snafu)]
pub enum DataConnectorError {
    #[snafu(display("Cannot connect to {dataconnector}. {source}"))]
    UnableToConnectInternal {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot connect to {dataconnector} on {host}:{port}. Ensure that the host and port are correctly configured in the spicepod, and that the host is reachable."))]
    UnableToConnectInvalidHostOrPort {
        dataconnector: String,
        host: String,
        port: String,
    },

    #[snafu(display("Cannot connect to {dataconnector}. Authentication failed. Ensure that the username and password are correctly configured in the spicepod."))]
    UnableToConnectInvalidUsernameOrPassword { dataconnector: String },

    #[snafu(display("Cannot connect to {dataconnector}. Ensure that the corresponding secure option is configured to match the data connector's TLS security requirements."))]
    UnableToConnectTlsError { dataconnector: String },

    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get read write provider for {dataconnector}: {source}"))]
    UnableToGetReadWriteProvider {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get catalog provider for {dataconnector}: {source}"))]
    UnableToGetCatalogProvider {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to read the secrets for {dataconnector}: {source}"))]
    UnableToReadSecrets {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid configuration for {dataconnector}. {message}"))]
    InvalidConfiguration {
        dataconnector: String,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid configuration for {dataconnector}. {message}"))]
    InvalidConfigurationNoSource {
        dataconnector: String,
        message: String,
    },

    #[snafu(display(
        "Failed to get {dataconnector} data connector for dataset {dataset_name}. Table {table_name} not found. Ensure the table name is correctly spelled in the spicepod."
    ))]
    InvalidTableName {
        dataconnector: String,
        dataset_name: String,
        table_name: String,
    },

    #[snafu(display(
        "Failed to get schema for {dataconnector} dataset {dataset_name}. Ensure the table '{table_name}' exists in the data source."
    ))]
    UnableToGetSchema {
        dataconnector: String,
        dataset_name: String,
        table_name: String,
    },

    #[snafu(display("{dataconnector} Data Connector Error: {source}"))]
    InternalWithSource {
        dataconnector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "An internal error occurred in the {dataconnector} Data Connector. Report a bug on GitHub (github.com/spiceai/spiceai) and reference the code: {code}"
    ))]
    Internal {
        dataconnector: String,
        code: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type DataConnectorResult<T> = std::result::Result<T, DataConnectorError>;

type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

lazy_static! {
    static ref DATA_CONNECTOR_FACTORY_REGISTRY: Mutex<HashMap<String, Arc<dyn DataConnectorFactory>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_connector_factory(
    name: &str,
    connector_factory: Arc<dyn DataConnectorFactory>,
) {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    registry.insert(name.to_string(), connector_factory);
}

/// Create a new `DataConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `DataConnector`.
#[allow(clippy::implicit_hasher)]
pub async fn create_new_connector(
    name: &str,
    params: HashMap<String, SecretString>,
    secrets: Arc<RwLock<Secrets>>,
) -> Option<AnyErrorResult<Arc<dyn DataConnector>>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    let Some(factory) = connector_factory else {
        return None;
    };

    let params = match Parameters::try_new(
        name,
        params.into_iter().collect(),
        factory.prefix(),
        secrets,
        factory.parameters(),
    )
    .await
    {
        Ok(params) => params,
        Err(e) => return Some(Err(e)),
    };

    let result = factory.create(params).await;
    Some(result)
}

pub async fn register_all() {
    register_connector_factory("localhost", localhost::LocalhostConnectorFactory::new_arc()).await;
    #[cfg(feature = "databricks")]
    register_connector_factory("databricks", databricks::DatabricksFactory::new_arc()).await;
    #[cfg(feature = "delta_lake")]
    register_connector_factory("delta_lake", delta_lake::DeltaLakeFactory::new_arc()).await;
    #[cfg(feature = "dremio")]
    register_connector_factory("dremio", dremio::DremioFactory::new_arc()).await;
    register_connector_factory("file", file::FileFactory::new_arc()).await;
    #[cfg(feature = "flightsql")]
    register_connector_factory("flightsql", flightsql::FlightSQLFactory::new_arc()).await;
    register_connector_factory("s3", s3::S3Factory::new_arc()).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("ftp", ftp::FTPFactory::new_arc()).await;
    register_connector_factory("http", https::HttpsFactory::new_arc()).await;
    register_connector_factory("https", https::HttpsFactory::new_arc()).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("sftp", sftp::SFTPFactory::new_arc()).await;
    register_connector_factory("spiceai", spiceai::SpiceAIFactory::new_arc()).await;
    #[cfg(feature = "mysql")]
    register_connector_factory("mysql", mysql::MySQLFactory::new_arc()).await;
    #[cfg(feature = "postgres")]
    register_connector_factory("postgres", postgres::PostgresFactory::new_arc()).await;
    #[cfg(feature = "duckdb")]
    register_connector_factory("duckdb", duckdb::DuckDBFactory::new_arc()).await;
    #[cfg(feature = "clickhouse")]
    register_connector_factory("clickhouse", clickhouse::ClickhouseFactory::new_arc()).await;
    register_connector_factory("graphql", graphql::GraphQLFactory::new_arc()).await;
    #[cfg(feature = "odbc")]
    register_connector_factory("odbc", odbc::ODBCFactory::new_arc()).await;
    #[cfg(feature = "spark")]
    register_connector_factory("spark", spark::SparkFactory::new_arc()).await;
    #[cfg(feature = "snowflake")]
    register_connector_factory("snowflake", snowflake::SnowflakeFactory::new_arc()).await;
    #[cfg(feature = "debezium")]
    register_connector_factory("debezium", debezium::DebeziumFactory::new_arc()).await;
    #[cfg(feature = "delta_lake")]
    register_connector_factory(
        "unity_catalog",
        unity_catalog::UnityCatalogFactory::new_arc(),
    )
    .await;
}

pub struct Parameters {
    params: Vec<(String, SecretString)>,
    prefix: &'static str,
    all_params: &'static [ParameterSpec],
}

#[derive(Debug, Clone)]
pub struct UserParam(String);

impl Display for UserParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum ParamLookup<'a> {
    Present(&'a SecretString),
    Absent(UserParam),
}

impl<'a> ParamLookup<'a> {
    pub fn ok(&self) -> Option<&'a SecretString> {
        match self {
            ParamLookup::Present(s) => Some(*s),
            ParamLookup::Absent(_) => None,
        }
    }

    pub fn expose(self) -> ExposedParamLookup<'a> {
        match self {
            ParamLookup::Present(s) => ExposedParamLookup::Present(s.expose_secret()),
            ParamLookup::Absent(s) => ExposedParamLookup::Absent(s),
        }
    }

    pub fn ok_or_else<E>(self, f: impl FnOnce(UserParam) -> E) -> Result<&'a SecretString, E> {
        match self {
            ParamLookup::Present(s) => Ok(s),
            ParamLookup::Absent(s) => Err(f(s)),
        }
    }
}

pub enum ExposedParamLookup<'a> {
    Present(&'a str),
    Absent(UserParam),
}

impl<'a> ExposedParamLookup<'a> {
    pub fn ok(self) -> Option<&'a str> {
        match self {
            ExposedParamLookup::Present(s) => Some(s),
            ExposedParamLookup::Absent(_) => None,
        }
    }

    pub fn ok_or_else<E>(self, f: impl FnOnce(UserParam) -> E) -> Result<&'a str, E> {
        match self {
            ExposedParamLookup::Present(s) => Ok(s),
            ExposedParamLookup::Absent(s) => Err(f(s)),
        }
    }
}

impl Parameters {
    pub async fn try_new(
        connector_name: &str,
        params: Vec<(String, SecretString)>,
        prefix: &'static str,
        secrets: Arc<RwLock<Secrets>>,
        all_params: &'static [ParameterSpec],
    ) -> AnyErrorResult<Self> {
        let full_prefix = format!("{prefix}_");

        // Convert the user-provided parameters into the format expected by the data connector
        let mut params: Vec<(String, SecretString)> = params
            .into_iter()
            .filter_map(|(key, value)| {
                let mut unprefixed_key = key.as_str();
                let mut has_prefix = false;
                if key.starts_with(&full_prefix) {
                    has_prefix = true;
                    unprefixed_key = &key[full_prefix.len()..];
                }

                let spec = all_params.iter().find(|p| p.name == unprefixed_key);

                let Some(spec) = spec else {
                    tracing::warn!("Ignoring parameter {key}: not supported for {connector_name}.");
                    return None;
                };

                if !has_prefix && spec.r#type.is_prefixed() {
                    tracing::warn!(
                    "Ignoring parameter {key}: must be prefixed with `{full_prefix}` for {connector_name}."
                );
                    return None;
                }

                Some((key[full_prefix.len()..].to_string(), value))
            })
            .collect();
        let secret_guard = secrets.read().await;

        // Try to autoload secrets that might be missing from params.
        for secret_key in all_params
            .iter()
            .filter_map(|p| if p.secret { Some(p) } else { None })
        {
            let secret_key_with_prefix = format!("{prefix}_{}", secret_key.name);
            tracing::debug!(
                "Attempting to autoload secret for {connector_name}: {secret_key_with_prefix}",
            );
            if params.iter().any(|p| p.0 == secret_key.name) {
                continue;
            }
            let secret = secret_guard.get_secret(&secret_key_with_prefix).await;
            if let Ok(Some(secret)) = secret {
                tracing::debug!(
                    "Autoloading secret for {connector_name}: {secret_key_with_prefix}",
                );
                // Insert without the prefix into the params
                params.push((secret_key.name.to_string(), secret));
            }
        }

        // Check if all required parameters are present
        for parameter in all_params {
            // If the parameter is missing and has a default value, add it to the params
            let missing = !params.iter().any(|p| p.0 == parameter.name);
            if missing {
                if let Some(default_value) = parameter.default {
                    params.push((parameter.name.to_string(), default_value.to_string().into()));
                    continue;
                }
            }

            if parameter.required && missing {
                return Err(Box::new(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: connector_name.to_string(),
                    message: format!("Missing required parameter: {}", parameter.name),
                }));
            }
        }

        Ok(Parameters::new(params, prefix, all_params))
    }

    pub fn new(
        params: Vec<(String, SecretString)>,
        prefix: &'static str,
        all_params: &'static [ParameterSpec],
    ) -> Self {
        Self {
            params,
            prefix,
            all_params,
        }
    }

    pub fn to_secret_map(&self) -> HashMap<String, SecretString> {
        self.params.iter().cloned().collect()
    }

    /// Returns the `SecretString` for the given parameter, or the user-facing parameter name of the missing parameter.
    ///
    /// # Panics
    ///
    /// Panics if the parameter is not found in the `all_params` list, as this is a programming error.
    pub fn get<'a>(&'a self, name: &str) -> ParamLookup<'a> {
        let spec = if let Some(spec) = self.all_params.iter().find(|p| p.name == name) {
            spec
        } else {
            panic!("Parameter `{name}` not found in parameters list. Add it to the parameters() list on the DataConnectorFactory.");
        };

        if let Some(param_value) = self.params.iter().find(|p| p.0 == name) {
            ParamLookup::Present(&param_value.1)
        } else {
            ParamLookup::Absent(if self.prefix.is_empty() {
                UserParam(spec.name.to_string())
            } else {
                UserParam(format!("{}_{}", self.prefix, spec.name))
            })
        }
    }
}

pub struct ParameterSpec {
    pub name: &'static str,
    pub required: bool,
    pub default: Option<&'static str>,
    pub secret: bool,
    pub description: &'static str,
    pub help_link: &'static str,
    pub examples: &'static [&'static str],
    pub r#type: ParameterType,
}

impl ParameterSpec {
    pub const fn connector(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            r#type: ParameterType::Connector,
        }
    }

    pub const fn runtime(name: &'static str) -> Self {
        Self {
            name,
            required: false,
            default: None,
            secret: false,
            description: "",
            help_link: "",
            examples: &[],
            r#type: ParameterType::Runtime,
        }
    }

    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub const fn default(mut self, default: &'static str) -> Self {
        self.default = Some(default);
        self
    }

    pub const fn secret(mut self) -> Self {
        self.secret = true;
        self
    }

    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = description;
        self
    }

    pub const fn help_link(mut self, help_link: &'static str) -> Self {
        self.help_link = help_link;
        self
    }

    pub const fn examples(mut self, examples: &'static [&'static str]) -> Self {
        self.examples = examples;
        self
    }
}

#[derive(Default, Clone, Copy, PartialEq)]
pub enum ParameterType {
    /// A parameter which tells Spice how to connect to the underlying data source.
    ///
    /// These parameters are automatically prefixed with the data connector's prefix.
    ///
    /// # Examples
    ///
    /// In Postgres, the `host` is a Connector parameter and would be auto-prefixed with `pg_`.
    #[default]
    Connector,
    /// Other parameters which control how the runtime interacts with the data source, but does
    /// not affect the actual connection.
    ///
    /// These parameters are not prefixed with the data connector's prefix.
    ///
    /// # Examples
    ///
    /// In Databricks, the `mode` parameter is used to select which connection to use, and thus is
    /// not a Connector parameter.
    Runtime,
}

impl ParameterType {
    pub const fn is_prefixed(&self) -> bool {
        matches!(self, Self::Connector)
    }
}

pub trait DataConnectorFactory: Send + Sync {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;

    /// The prefix to use for parameters and secrets for this `DataConnector`.
    ///
    /// This prefix is applied to any `ParameterType::Connector` parameters.
    ///
    /// ## Example
    ///
    /// If the prefix is `pg` then the following parameters are accepted:
    ///
    /// - `pg_host` -> `host`
    /// - `pg_port` -> `port`
    ///
    /// The prefix will be stripped from the parameter name before being passed to the data connector.
    fn prefix(&self) -> &'static str;

    /// Returns a list of parameters that the data connector requires to be able to connect to the data source.
    ///
    /// Any parameter provided by a user that isn't in this list will be filtered out and a warning logged.
    fn parameters(&self) -> &'static [ParameterSpec];
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Resolves the default refresh mode for the data connector.
    ///
    /// Most data connectors should keep this as `RefreshMode::Full`.
    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }

    async fn read_provider(&self, dataset: &Dataset)
        -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(&self, _table_provider: Arc<dyn TableProvider>) -> Option<ChangesStream> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    /// Returns a DataFusion `CatalogProvider` which can automatically populate tables from a remote catalog.
    async fn catalog_provider(
        self: Arc<Self>,
        _runtime: &Runtime,
        _catalog: &Catalog,
    ) -> Option<DataConnectorResult<Arc<dyn CatalogProvider>>> {
        None
    }
}

// Gets data from a table provider and returns it as a vector of RecordBatches.
pub async fn get_data(
    ctx: &mut SessionContext,
    table_name: TableReference,
    table_provider: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
) -> Result<(SchemaRef, SendableRecordBatchStream), DataFusionError> {
    let mut df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));
            let logical_plan =
                LogicalPlanBuilder::scan(table_name.clone(), table_source, None)?.build()?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => ctx.sql(&sql).await?,
    };

    for filter in filters {
        df = df.filter(filter)?;
    }

    let record_batch_stream = df.execute_stream().await?;
    Ok((table_provider.schema(), record_batch_stream))
}

pub trait ListingTableConnector: DataConnector {
    fn as_any(&self) -> &dyn Any;

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url>;

    fn get_params(&self) -> &Parameters;

    #[must_use]
    fn get_session_context() -> SessionContext {
        SessionContext::new_with_config_rt(
            SessionConfig::new().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(),
        )
    }

    fn get_object_store(&self, dataset: &Dataset) -> DataConnectorResult<Arc<dyn ObjectStore>>
    where
        Self: Display,
    {
        let store_url = self.get_object_store_url(dataset)?;
        let listing_store_url = ListingTableUrl::parse(store_url.clone()).boxed().context(
            UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
            },
        )?;
        Self::get_session_context()
            .runtime_env()
            .object_store(&listing_store_url)
            .boxed()
            .context(UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
            })
    }

    fn construct_metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let store_url: Url = self.get_object_store_url(dataset)?;
        let store = self.get_object_store(dataset)?;
        let (_, extension) = self.get_file_format_and_extension(dataset)?;

        let table = ObjectStoreMetadataTable::try_new(store, &store_url, Some(extension.clone()))
            .context(InvalidConfigurationSnafu {
            dataconnector: format!("{self}"),
            message: format!(
                "Invalid extension ({extension}) for source ({})",
                dataset.name
            ),
        })?;
        Ok(table as Arc<dyn TableProvider>)
    }

    /// Determines the file format and its corresponding extension for a given dataset.
    ///
    /// If not explicitly specified (via the [`Dataset`]'s `file_format` param key), it attempts
    /// to infer the format from the dataset's file extension. It supports both tabular and
    /// unstructured formats. It supports the following tabular formats:
    ///  - parquet
    ///  - csv
    /// For tabular formats, file options can also be specified in the [`Dataset`]'s `param`s.
    ///
    /// For unstructured text formats, the [`Dataset`]'s `file_format` param key must be set. `Ok`
    /// responses, are always of the format `Ok((None, String))`. The data must be UTF8 compatible.
    fn get_file_format_and_extension(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<(Option<Arc<dyn FileFormat>>, String)>
    where
        Self: Display,
    {
        let params = self.get_params();
        let extension = params
            .get("file_extension")
            .expose()
            .ok()
            .map(str::to_string);

        match params.get("file_format").expose().ok() {
            Some("csv") => Ok((
                Some(self.get_csv_format(params)?),
                extension.unwrap_or(".csv".to_string()),
            )),
            Some("parquet") => Ok((
                Some(Arc::new(ParquetFormat::default())),
                extension.unwrap_or(".parquet".to_string()),
            )),
            Some(format) => Ok((None, format!(".{format}"))),
            None => {
                if let Some(ext) = std::path::Path::new(dataset.path().as_str()).extension() {
                    if ext.eq_ignore_ascii_case("csv") {
                        return Ok((
                            Some(self.get_csv_format(params)?),
                            extension.unwrap_or(".csv".to_string()),
                        ));
                    }
                    if ext.eq_ignore_ascii_case("parquet") {
                        return Ok((
                            Some(Arc::new(ParquetFormat::default())),
                            extension.unwrap_or(".parquet".to_string()),
                        ));
                    }
                }

                Err(DataConnectorError::InvalidConfiguration {
                    dataconnector: format!("{self}"),
                    message: "Missing required file_format parameter.".to_string(),
                    source: "Missing file format".into(),
                })
            }
        }
    }

    fn get_csv_format(&self, params: &Parameters) -> DataConnectorResult<Arc<CsvFormat>>
    where
        Self: Display,
    {
        let has_header = params
            .get("csv_has_header")
            .expose()
            .ok()
            .map_or(true, |f| f.eq_ignore_ascii_case("true"));
        let quote = params
            .get("csv_quote")
            .expose()
            .ok()
            .map_or(b'"', |f| *f.as_bytes().first().unwrap_or(&b'"'));
        let escape = params
            .get("csv_escape")
            .expose()
            .ok()
            .and_then(|f| f.as_bytes().first().copied());
        let schema_infer_max_rec = params
            .get("csv_schema_infer_max_records")
            .expose()
            .ok()
            .map_or_else(|| 1000, |f| usize::from_str(f).map_or(1000, |f| f));
        let delimiter = params
            .get("csv_delimiter")
            .expose()
            .ok()
            .map_or(b',', |f| *f.as_bytes().first().unwrap_or(&b','));
        let compression_type = params
            .get("file_compression_type")
            .expose()
            .ok()
            .unwrap_or_default();

        Ok(Arc::new(
            CsvFormat::default()
                .with_has_header(has_header)
                .with_quote(quote)
                .with_escape(escape)
                .with_schema_infer_max_rec(schema_infer_max_rec)
                .with_delimiter(delimiter)
                .with_file_compression_type(
                    FileCompressionType::from_str(compression_type)
                        .boxed()
                        .context(InvalidConfigurationSnafu {
                            dataconnector: format!("{self}"),
                            message: format!("Invalid CSV compression_type: {compression_type}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
                        })?,
                ),
        ))
    }
}

#[async_trait]
impl<T: ListingTableConnector + Display> DataConnector for T {
    fn as_any(&self) -> &dyn Any {
        ListingTableConnector::as_any(self)
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }

        Some(
            self.construct_metadata_provider(dataset)
                .map_err(Into::into),
        )
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let ctx: SessionContext = Self::get_session_context();
        let url = self.get_object_store_url(dataset)?;

        // This shouldn't error because we've already validated the URL in `get_object_store_url`.
        let table_path = ListingTableUrl::parse(url.clone())
            .boxed()
            .context(InternalSnafu {
                dataconnector: format!("{self}"),
                code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
            })?;

        let (file_format_opt, extension) = self.get_file_format_and_extension(dataset)?;
        match file_format_opt {
            None => {
                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                Ok(ObjectStoreTextTable::try_new(
                    self.get_object_store(dataset)?,
                    &url.clone(),
                    Some(extension.clone()),
                )
                .context(InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid extension ({extension}) for source ({})",
                        dataset.name
                    ),
                })?)
            }
            Some(file_format) => {
                let options = ListingOptions::new(file_format).with_file_extension(&extension);

                let resolved_schema = options
                    .infer_schema(&ctx.state(), &table_path)
                    .await
                    .boxed()
                    .context(UnableToConnectInternalSnafu {
                        dataconnector: format!("{self}"),
                    })?;

                let config = ListingTableConfig::new(table_path)
                    .with_listing_options(options)
                    .with_schema(resolved_schema);

                // This shouldn't error because we're passing the schema and options correctly.
                let table = ListingTable::try_new(config)
                    .boxed()
                    .context(InternalSnafu {
                        dataconnector: format!("{self}"),
                        code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
                    })?;

                Ok(Arc::new(table))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_table_providers::util::secrets::to_secret_map;

    use super::*;

    struct TestConnector {
        params: HashMap<String, SecretString>,
    }

    impl std::fmt::Display for TestConnector {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestConnector")
        }
    }

    impl DataConnectorFactory for TestConnector {
        fn create(
            &self,
            params: HashMap<String, SecretString>,
        ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
            Box::pin(async move {
                let connector = Self { params };
                Ok(Arc::new(connector) as Arc<dyn DataConnector>)
            })
        }

        fn prefix(&self) -> &'static str {
            "test"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    impl ListingTableConnector for TestConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_params(&self) -> &HashMap<String, SecretString> {
            &self.params
        }

        fn get_object_store_url(&self, _dataset: &Dataset) -> DataConnectorResult<Url> {
            Url::parse("test")
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: "Invalid URL".to_string(),
                })
        }
    }

    fn setup_connector(path: String, params: HashMap<String, String>) -> (TestConnector, Dataset) {
        let connector = TestConnector {
            params: to_secret_map(params),
        };
        let dataset = Dataset::try_new(path, "test").expect("a valid dataset");

        (connector, dataset)
    }

    #[test]
    fn test_get_file_format_and_extension_require_file_format() {
        let (connector, dataset) = setup_connector("test:test/".to_string(), HashMap::new());

        match connector.get_file_format_and_extension(&dataset) {
            Ok(_) => panic!("Unexpected success"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Invalid configuration for TestConnector. Missing required file_format parameter."
            ),
        }
    }

    #[test]
    fn test_get_file_format_and_extension_detect_csv_extension() {
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), HashMap::new());

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[test]
    fn test_get_file_format_and_extension_detect_parquet_extension() {
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), HashMap::new());

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[test]
    fn test_get_file_format_and_extension_csv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[test]
    fn test_get_file_format_and_extension_parquet_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[test]
    fn test_remove_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_key1".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());
        hashmap.insert("key3".to_string(), "value3".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix");

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_no_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("key1".to_string(), "value1".to_string());
        hashmap.insert("key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix");

        let expected = HashMap::new();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_hashmap() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix");

        let expected: HashMap<String, String> = HashMap::new();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_full_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix");

        let mut expected = HashMap::new();
        expected.insert(String::new(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }
}
