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

use crate::component::dataset::Dataset;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::object::metadata::ObjectStoreMetadataTable;
use data_components::object::raw::ObjectStoreRawTable;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::sql::TableReference;
use lazy_static::lazy_static;
use object_store::ObjectStore;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use secrets::Secret;
use std::future::Future;

use crate::object_store_registry::default_runtime_env;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "dremio")]
pub mod dremio;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "ftp")]
pub mod ftp;
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
#[cfg(feature = "spark")]
pub mod spark;
pub mod spiceai;

#[cfg(feature = "snowflake")]
pub mod snowflake;

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

    #[snafu(display("Invalid configuration for {dataconnector}. {message}"))]
    InvalidConfiguration {
        dataconnector: String,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
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

type NewDataConnectorFn = dyn Fn(
        Option<Secret>,
        Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>
    + Send;

lazy_static! {
    static ref DATA_CONNECTOR_FACTORY_REGISTRY: Mutex<HashMap<String, Box<NewDataConnectorFn>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_connector_factory(
    name: &str,
    connector_factory: impl Fn(
            Option<Secret>,
            Arc<HashMap<String, String>>,
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>
        + Send
        + 'static,
) {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    registry.insert(name.to_string(), Box::new(connector_factory));
}

/// Create a new `DataConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `DataConnector`.
#[allow(clippy::implicit_hasher)]
pub async fn create_new_connector(
    name: &str,
    secret: Option<Secret>,
    params: Arc<HashMap<String, String>>,
) -> Option<AnyErrorResult<Arc<dyn DataConnector>>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    match connector_factory {
        Some(factory) => Some(factory(secret, params).await),
        None => None,
    }
}

pub async fn register_all() {
    register_connector_factory("localhost", localhost::LocalhostConnector::create).await;
    #[cfg(feature = "databricks")]
    register_connector_factory("databricks", databricks::Databricks::create).await;
    #[cfg(feature = "dremio")]
    register_connector_factory("dremio", dremio::Dremio::create).await;
    #[cfg(feature = "flightsql")]
    register_connector_factory("flightsql", flightsql::FlightSQL::create).await;
    register_connector_factory("s3", s3::S3::create).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("ftp", ftp::FTP::create).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("sftp", sftp::SFTP::create).await;
    register_connector_factory("spiceai", spiceai::SpiceAI::create).await;
    #[cfg(feature = "mysql")]
    register_connector_factory("mysql", mysql::MySQL::create).await;
    #[cfg(feature = "postgres")]
    register_connector_factory("postgres", postgres::Postgres::create).await;
    #[cfg(feature = "duckdb")]
    register_connector_factory("duckdb", duckdb::DuckDB::create).await;
    #[cfg(feature = "clickhouse")]
    register_connector_factory("clickhouse", clickhouse::Clickhouse::create).await;
    #[cfg(feature = "odbc")]
    register_connector_factory("odbc", odbc::ODBC::create).await;
    #[cfg(feature = "spark")]
    register_connector_factory("spark", spark::Spark::create).await;
    #[cfg(feature = "snowflake")]
    register_connector_factory("snowflake", snowflake::Snowflake::create).await;
}

pub trait DataConnectorFactory {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn read_provider(&self, dataset: &Dataset)
        -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn stream_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<AnyErrorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
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
) -> Result<(SchemaRef, Vec<arrow::record_batch::RecordBatch>)> {
    let mut df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));
            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .context(UnableToConstructLogicalPlanBuilderSnafu {})?
                .build()
                .context(UnableToBuildLogicalPlanSnafu {})?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => ctx
            .sql(&sql)
            .await
            .context(UnableToCreateDataFrameSnafu {})?,
    };

    for filter in filters {
        df = df.filter(filter).context(UnableToFilterDataFrameSnafu {})?;
    }

    let batches = df.collect().await.context(UnableToScanTableProviderSnafu)?;

    Ok((table_provider.schema(), batches))
}

pub trait ListingTableConnector: DataConnector {
    fn as_any(&self) -> &dyn Any;

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url>;

    fn get_params(&self) -> &HashMap<String, String>;

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

        let table = ObjectStoreMetadataTable::try_new(store, &store_url, Some(extension)).context(
            InternalSnafu {
                dataconnector: format!("{self}"),
                code: "LTC-MP-OSMT".to_string(), // ListingTableConnector-MetadataProvider-ObjectStoreMetadataTableTryNew
            },
        )?;
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
    /// For unstructured formats, the [`Dataset`]'s `file_format` param key must be set. `Ok`
    /// responses, are always of the format `Ok((None, String))`
    fn get_file_format_and_extension(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<(Option<Arc<dyn FileFormat>>, String)>
    where
        Self: Display,
    {
        let params = self.get_params();
        let extension = params.get("file_extension").cloned();

        match params.get("file_format").map(String::as_str) {
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

    fn get_csv_format(
        &self,
        params: &HashMap<String, String>,
    ) -> DataConnectorResult<Arc<CsvFormat>>
    where
        Self: Display,
    {
        let has_header = params.get("has_header").map_or(true, |f| f == "true");
        let quote = params
            .get("quote")
            .map_or(b'"', |f| *f.as_bytes().first().unwrap_or(&b'"'));
        let escape = params
            .get("escape")
            .and_then(|f| f.as_bytes().first().copied());
        let schema_infer_max_rec = params
            .get("schema_infer_max_records")
            .map_or_else(|| 1000, |f| usize::from_str(f).map_or(1000, |f| f));
        let delimiter = params
            .get("delimiter")
            .map_or(b',', |f| *f.as_bytes().first().unwrap_or(&b','));
        let compression_type = params.get("compression_type").map_or("", |f| f);

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
                // Assume its unstructured data. Use a [`ObjectStoreRawTable`].
                Ok(ObjectStoreRawTable::try_new(
                    self.get_object_store(dataset)?,
                    &url,
                    Some(extension),
                )
                .context(UnableToGetReadProviderSnafu {
                    dataconnector: format!("{self}"),
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
    use datafusion::common::FileType;

    use super::*;

    struct TestConnector {
        params: Arc<HashMap<String, String>>,
    }

    impl std::fmt::Display for TestConnector {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestConnector")
        }
    }

    impl DataConnectorFactory for TestConnector {
        fn create(
            _secret: Option<Secret>,
            params: Arc<HashMap<String, String>>,
        ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
            Box::pin(async move {
                let connector = Self { params };
                Ok(Arc::new(connector) as Arc<dyn DataConnector>)
            })
        }
    }

    impl ListingTableConnector for TestConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_params(&self) -> &HashMap<String, String> {
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
            params: params.into(),
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

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(file_format.file_type(), FileType::CSV);
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[test]
    fn test_get_file_format_and_extension_detect_parquet_extension() {
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), HashMap::new());

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(file_format.file_type(), FileType::PARQUET);
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

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(file_format.file_type(), FileType::CSV);
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

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(file_format.file_type(), FileType::PARQUET);
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }
}
