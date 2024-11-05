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
use crate::component::dataset::Dataset;
use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorResult;
use crate::parameters::Parameters;
use async_trait::async_trait;
use data_components::object::metadata::ObjectStoreMetadataTable;
use data_components::object::text::ObjectStoreTextTable;
use datafusion::config::ConfigField;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use object_store::ObjectStore;
use snafu::prelude::*;
use std::any::Any;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

use crate::object_store_registry::default_runtime_env;

use super::infer::infer_partitions_with_types;

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
            crate::dataconnector::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
            },
        )?;
        Self::get_session_context()
            .runtime_env()
            .object_store(&listing_store_url)
            .boxed()
            .context(crate::dataconnector::UnableToConnectInternalSnafu {
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
            .context(crate::dataconnector::InvalidConfigurationSnafu {
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
                Some(Arc::new(
                    ParquetFormat::default().with_options(self.get_table_parquet_options()?),
                )),
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
                            Some(Arc::new(
                                ParquetFormat::default()
                                    .with_options(self.get_table_parquet_options()?),
                            )),
                            extension.unwrap_or(".parquet".to_string()),
                        ));
                    }
                }

                Err(
                    crate::dataconnector::DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: "Missing required file_format parameter.".to_string(),
                        source: "Missing file format".into(),
                    },
                )
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
                        .context(crate::dataconnector::InvalidConfigurationSnafu {
                            dataconnector: format!("{self}"),
                            message: format!("Invalid CSV compression_type: {compression_type}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
                        })?,
                ),
        ))
    }

    fn get_table_parquet_options(&self) -> DataConnectorResult<TableParquetOptions>
    where
        Self: Display,
    {
        let mut table_parquet_options = TableParquetOptions::new();
        table_parquet_options
            .set("pushdown_filters", "true")
            .map_err(
                |e| crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                    dataconnector: format!("{self}"),
                    source: Box::new(e),
                },
            )?;
        Ok(table_parquet_options)
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
        let table_path = ListingTableUrl::parse(url.clone()).boxed().context(
            crate::dataconnector::InternalSnafu {
                dataconnector: format!("{self}"),
                code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
            },
        )?;

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
                .context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid extension ({extension}) for source ({})",
                        dataset.name
                    ),
                })?)
            }
            Some(file_format) => {
                let mut options = ListingOptions::new(file_format).with_file_extension(&extension);

                let resolved_schema = options
                    .infer_schema(&ctx.state(), &table_path)
                    .await
                    .boxed()
                    .context(crate::dataconnector::UnableToConnectInternalSnafu {
                        dataconnector: format!("{self}"),
                    })?;

                // If we should infer partitions and the path is a folder, infer the partitions from the folder structure.
                if dataset.get_param("hive_partitioning_enabled", false)
                    && table_path.is_collection()
                {
                    let inferred_partitions =
                        infer_partitions_with_types(&ctx.state(), &table_path, &extension).await;
                    match inferred_partitions {
                        Ok(partitions) => {
                            tracing::debug!(
                                "Inferred partitions for {:?}: {:?}",
                                table_path,
                                partitions
                                    .iter()
                                    .map(|(k, _)| k.as_str())
                                    .collect::<Vec<_>>()
                            );
                            options = options.with_table_partition_cols(partitions);
                        }
                        Err(e) => {
                            // This might not be an error, it could be that the table is not partitioned.
                            tracing::debug!("Failed to infer partitions for {:?}: {e}", table_path);
                        }
                    }
                }

                let config = ListingTableConfig::new(table_path)
                    .with_listing_options(options)
                    .with_schema(resolved_schema);

                // This shouldn't error because we're passing the schema and options correctly.
                let table = ListingTable::try_new(config).boxed().context(
                    crate::dataconnector::InternalSnafu {
                        dataconnector: format!("{self}"),
                        code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
                    },
                )?;

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

#[cfg(test)]
mod tests {
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use url::Url;

    use crate::dataconnector::DataConnectorFactory;
    use crate::parameters::ParameterSpec;

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
            _metadata: Option<HashMap<String, String>>,
        ) -> Pin<Box<dyn Future<Output = crate::dataconnector::NewDataConnectorResult> + Send>>
        {
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
                .context(crate::dataconnector::InvalidConfigurationSnafu {
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
}
