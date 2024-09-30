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

use crate::accelerated_table::AcceleratedTable;
use crate::component::catalog::Catalog;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::Dataset;
use crate::parameters::ParameterSpec;
use crate::parameters::Parameters;
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
use datafusion::sql::unparser::Unparser;
use datafusion::sql::TableReference;
use object_store::ObjectStore;
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tokio::sync::{Mutex, RwLock};
use url::{form_urlencoded, Url};

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
pub mod github;
pub mod graphql;
pub mod https;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod s3;
#[cfg(feature = "ftp")]
pub mod sftp;
#[cfg(feature = "sharepoint")]
pub mod sharepoint;
pub mod sink;
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

    #[snafu(display(
        "Unable to load {dataconnector} dataset {dataset_name}. Table {table_name} not found. Verify the source table name in the Spicepod configuration."
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

    #[snafu(display("Invalid configuration for {dataconnector}. {message}"))]
    InvalidConfigurationNoSource {
        dataconnector: String,
        message: String,
    },

    #[snafu(display("Invalid glob pattern {pattern}: {source}"))]
    InvalidGlobPattern {
        pattern: String,
        source: globset::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type DataConnectorResult<T> = std::result::Result<T, DataConnectorError>;

type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

static DATA_CONNECTOR_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, Arc<dyn DataConnectorFactory>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

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

    let factory = connector_factory?;

    let params = match Parameters::try_new(
        &format!("connector {name}"),
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
    register_connector_factory("sink", sink::SinkConnectorFactory::new_arc()).await;
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
    register_connector_factory("github", github::GithubFactory::new_arc()).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("sftp", sftp::SFTPFactory::new_arc()).await;
    register_connector_factory("spiceai", spiceai::SpiceAIFactory::new_arc()).await;
    #[cfg(feature = "mssql")]
    register_connector_factory("mssql", mssql::SqlServerFactory::new_arc()).await;
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
    #[cfg(feature = "sharepoint")]
    register_connector_factory("sharepoint", sharepoint::SharepointFactory::new_arc()).await;
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

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(&self, _table_provider: Arc<dyn TableProvider>) -> Option<ChangesStream> {
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

    /// A hook that is called when an accelerated table is registered to the
    /// DataFusion context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &Dataset,
        _accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
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

    let sql = Unparser::default().plan_to_sql(df.logical_plan())?;
    tracing::info!(target: "task_history", sql = %sql, "labels");

    let record_batch_stream = df.execute_stream().await?;
    Ok((table_provider.schema(), record_batch_stream))
}

#[async_trait]
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
    ///
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

    /// A hook that is called when an accelerated table is registered to the
    /// DataFusion context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &Dataset,
        _accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
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
                let content_formatter = document_parse::get_parser_factory(extension.as_str())
                    .await
                    .map(|factory| {
                        // TODO: add opts.
                        factory.default()
                    });

                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                Ok(ObjectStoreTextTable::try_new(
                    self.get_object_store(dataset)?,
                    &url.clone(),
                    Some(extension.clone()),
                    content_formatter,
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

    /// A hook that is called when an accelerated table is registered to the
    /// DataFusion context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ListingTableConnector::on_accelerated_table_registration(self, dataset, accelerated_table)
            .await
    }
}

fn build_fragments(params: &Parameters, keys: Vec<&str>) -> String {
    let mut fragments = vec![];
    let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

    for key in keys {
        if let Some(value) = params.get(key).expose().ok() {
            fragment_builder.append_pair(key, value);
        }
    }
    fragments.push(fragment_builder.finish());
    fragments.join("&")
}

#[cfg(test)]
mod tests {
    use datafusion_table_providers::util::secrets::to_secret_map;

    use super::*;

    struct TestConnector {
        params: Parameters,
    }

    impl std::fmt::Display for TestConnector {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestConnector")
        }
    }

    impl DataConnectorFactory for TestConnector {
        fn create(
            &self,
            params: Parameters,
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

        fn get_params(&self) -> &Parameters {
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

    const TEST_PARAMETERS: &[ParameterSpec] = &[
        ParameterSpec::runtime("file_extension"),
        ParameterSpec::runtime("file_format"),
        ParameterSpec::runtime("csv_has_header"),
        ParameterSpec::runtime("csv_quote"),
        ParameterSpec::runtime("csv_escape"),
        ParameterSpec::runtime("csv_schema_infer_max_records"),
        ParameterSpec::runtime("csv_delimiter"),
        ParameterSpec::runtime("file_compression_type"),
    ];

    fn setup_connector(path: String, params: HashMap<String, String>) -> (TestConnector, Dataset) {
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "test",
                TEST_PARAMETERS,
            ),
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
    fn test_build_fragments() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        params.insert("csv_has_header".to_string(), "true".to_string());
        let params = Parameters::new(
            to_secret_map(params).into_iter().collect(),
            "test",
            TEST_PARAMETERS,
        );

        assert_eq!(
            build_fragments(&params, vec!["file_format", "csv_has_header"]),
            "file_format=csv&csv_has_header=true"
        );
    }
}
