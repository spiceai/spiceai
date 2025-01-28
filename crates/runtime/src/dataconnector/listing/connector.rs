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

use crate::accelerated_table::AcceleratedTable;
use crate::component::dataset::Dataset;
use crate::dataconnector::ConnectorComponent;
use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorError;
use crate::dataconnector::DataConnectorResult;
use crate::parameters::ExposedParamLookup;
use crate::parameters::Parameters;
use arrow_tools::schema::expand_views_schema;
use async_trait::async_trait;
use data_components::object::metadata::ObjectStoreMetadataTable;
use data_components::object::text::ObjectStoreTextTable;
use datafusion::config::ConfigField;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

use crate::object_store_registry::default_runtime_env;

use super::infer::infer_partitions_with_types;
use super::DelimitedFormat;

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
                connector_component: ConnectorComponent::from(dataset),
            },
        )?;
        Self::get_session_context()
            .runtime_env()
            .object_store(&listing_store_url)
            .boxed()
            .context(crate::dataconnector::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
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
                "Invalid file extension ({extension}) for source ({})",
                dataset.name
            ),
            connector_component: ConnectorComponent::from(dataset),
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
        let file_extension = std::path::Path::new(dataset.path())
            .extension()
            .map(|ext| ext.to_ascii_lowercase().to_string_lossy().to_string());
        let file_format_param = params.get("file_format").expose().ok();

        match (file_format_param, file_extension.as_deref()) {
            (Some("csv"), _) | (None, Some("csv")) => Ok((
                Some(self.delimiter_separated_format(dataset, params, DelimitedFormat::Csv)?),
                extension.unwrap_or(".csv".to_string()),
            )),
            (Some("tsv"), _) | (None, Some("tsv")) => Ok((
                Some(self.delimiter_separated_format(dataset, params, DelimitedFormat::Tsv)?),
                extension.unwrap_or(".tsv".to_string()),
            )),
            (Some("jsonl"), _) | (None, Some("jsonl"))=> Ok((
                Some(self.get_jsonl_format(dataset, params)?),
                extension.unwrap_or(".jsonl".to_string()),
            )),
            (Some("parquet"), _) | (None, Some("parquet"))=> Ok((
                Some(Arc::new(
                    ParquetFormat::default().with_options(self.get_table_parquet_options(dataset)?),
                )),
                extension.unwrap_or(".parquet".to_string()),
            )),
            (Some(format), _) => Ok((None, format!(".{format}"))),
            (_, _) => Err(
                    crate::dataconnector::DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: "The required 'file_format' parameter is missing.\nEnsure the parameter is provided, and try again.".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: "Missing file format".into(),
                    },
                ),
        }
    }

    /// Returns a [`JsonFormat`] based on the provided [`Datasets`] parameters.
    ///
    /// If the [`Dataset`] has the relevant parameter, return an error if the value is invalid.
    fn get_jsonl_format(
        &self,
        dataset: &Dataset,
        params: &Parameters,
    ) -> DataConnectorResult<Arc<JsonFormat>>
    where
        Self: Display,
    {
        let mut format = JsonFormat::default();

        if let ExposedParamLookup::Present(comp_as_str) =
            params.get("file_compression_type").expose()
        {
            let compression = comp_as_str.parse::<FileCompressionType>().boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSONL compression_type: {comp_as_str}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_file_compression_type(compression);
        };

        if let ExposedParamLookup::Present(infer_max_rec_str) =
            params.get("schema_infer_max_records").expose()
        {
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSONL parameter 'schema_infer_max_records' must be an integer, not {infer_max_rec_str}"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_schema_infer_max_rec(schema_infer_max_rec);
        };

        Ok(Arc::new(format))
    }

    /// Returns a [`CsvFormat`] based on the provided [`Datasets`] parameters, and choice of delimiter.
    ///
    /// Uses the appropriate parameters based on the [`DelimitedFormat`] provided.
    fn delimiter_separated_format(
        &self,
        dataset: &Dataset,
        params: &Parameters,
        delimiter: DelimitedFormat,
    ) -> DataConnectorResult<Arc<CsvFormat>>
    where
        Self: Display,
    {
        let has_header = params
            .get(&format!("{delimiter}_has_header"))
            .expose()
            .ok()
            .map_or(true, |f| f.eq_ignore_ascii_case("true"));
        let quote = params
            .get(&format!("{delimiter}_quote"))
            .expose()
            .ok()
            .map_or(b'"', |f| *f.as_bytes().first().unwrap_or(&b'"'));
        let escape = params
            .get(&format!("{delimiter}_escape"))
            .expose()
            .ok()
            .and_then(|f| f.as_bytes().first().copied());
        let schema_infer_max_rec = params
            .get("schema_infer_max_records")
            .expose()
            .ok()
            .or(params
                .get(&format!("{delimiter}_schema_infer_max_records"))
                .expose()
                .ok()) // For backwards compatibility
            .map_or_else(|| 1000, |f| usize::from_str(f).unwrap_or(1000));
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
                .with_delimiter(delimiter.separator())
                .with_file_compression_type(
                    FileCompressionType::from_str(compression_type)
                        .boxed()
                        .context(crate::dataconnector::InvalidConfigurationSnafu {
                            dataconnector: format!("{self}"),
                            message: format!(
                                "Invalid {} compression_type: {compression_type}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED", delimiter.to_string().to_uppercase()
                            ),
                            connector_component: ConnectorComponent::from(dataset),
                        })?,
                ),
        ))
    }

    fn get_table_parquet_options(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<TableParquetOptions>
    where
        Self: Display,
    {
        let mut table_parquet_options = TableParquetOptions::new();
        table_parquet_options
            .set("pushdown_filters", "true")
            .map_err(
                |e| crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
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

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError
    where
        Self: Display,
    {
        crate::dataconnector::DataConnectorError::UnableToConnectInternal {
            dataconnector: format!("{self}"),
            connector_component: ConnectorComponent::from(dataset),
            source: error.into(),
        }
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
                connector_component: ConnectorComponent::from(dataset),
                code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
            },
        )?;

        let (file_format_opt, extension) = self.get_file_format_and_extension(dataset)?;
        match file_format_opt {
            None => {
                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                let content_formatter = document_parse::get_parser_factory(extension.as_str())
                    .await
                    .map(|factory| {
                        // TODO: add opts.
                        factory.default()
                    });

                Ok(ObjectStoreTextTable::try_new(
                    self.get_object_store(dataset)?,
                    &url.clone(),
                    Some(extension.clone()),
                    content_formatter,
                )
                .context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: format!(
                        "Invalid file extension ({extension}) for source ({})",
                        dataset.name
                    ),
                })?)
            }
            Some(file_format) => {
                let object_store = self.get_object_store(dataset)?;
                check_for_files_and_extensions(
                    format!("{self}"),
                    dataset,
                    &extension,
                    table_path.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;

                let mut options = ListingOptions::new(file_format).with_file_extension(&extension);

                let resolved_schema = options
                    .infer_schema(&ctx.state(), &table_path)
                    .await
                    .map_err(|e| match e {
                        DataFusionError::ObjectStore(object_store_error) => {
                            self.handle_object_store_error(dataset, object_store_error)
                        }
                        e => crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                            dataconnector: format!("{self}"),
                            connector_component: ConnectorComponent::from(dataset),
                            source: e.into(),
                        },
                    })?;

                let expanded_schema = Arc::new(expand_views_schema(&resolved_schema));

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
                    .with_schema(expanded_schema);

                // This shouldn't error because we're passing the schema and options correctly.
                let table = ListingTable::try_new(config).boxed().context(
                    crate::dataconnector::InternalSnafu {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
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

/// Lists the available files for a ListingTableConnector/ObjectStore
/// Infers if the `file_format` specified is valid, based on the existence of files with the required extension
///
/// # Errors
///
/// - If no files are found at the specified path
/// - If no files with the specified extension are found
async fn check_for_files_and_extensions(
    dataconnector: String,
    dataset: &Dataset,
    extension: &str,
    table_path: ListingTableUrl,
    ctx: &SessionContext,
    object_store: &Arc<dyn ObjectStore>,
) -> DataConnectorResult<()> {
    let files: Vec<_> = table_path
        .list_all_files(&ctx.state(), object_store, "")
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?
        .try_collect()
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?;

    if files.is_empty() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message:
                // Url could contain access keys from e.g. S3, so we don't want to log it.
                "Failed to find any files at the specified path. Check the path and try again."
                    .to_string(),
        });
    }

    let extensions = files
        .iter()
        .filter_map(|file| file.location.extension().map(|e| format!(".{e}")))
        .collect::<HashSet<_>>();

    if !extensions.contains(extension) {
        if extensions.is_empty() {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
                message: format!("Failed to find any files matching the extension '{extension}'.\nSpice could not find any files with extensions at the specified path. Check the path and try again."),
            });
        }

        let display_extensions = extensions
            .iter()
            .map(|e| format!("'{e}'"))
            .collect::<Vec<_>>()
            .join(", ");

        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!("Failed to find any files matching the extension '{extension}'.\nIs your `file_format` parameter correct? Spice found the following file extensions: {display_extensions}.\nFor details, visit: https://spiceai.org/docs/components/data-connectors#object-store-file-formats")
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use url::Url;

    use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
    use crate::dataconnector::{ConnectorParams, DataConnectorFactory};
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
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create(
            &self,
            params: ConnectorParams,
        ) -> Pin<Box<dyn Future<Output = crate::dataconnector::NewDataConnectorResult> + Send>>
        {
            Box::pin(async move {
                let connector = Self {
                    params: params.parameters,
                };
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

        fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
            Url::parse("test")
                .boxed()
                .context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "Invalid URL".to_string(),
                })
        }
    }

    const TEST_PARAMETERS: &[ParameterSpec] = LISTING_TABLE_PARAMETERS;

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
                "Cannot setup the dataset test (TestConnector) with an invalid configuration.\nThe required 'file_format' parameter is missing.\nEnsure the parameter is provided, and try again."
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
    fn test_get_file_format_and_extension_tsv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "tsv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".tsv");
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
