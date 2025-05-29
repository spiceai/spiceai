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

use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::Schema;
use arrow_tools::schema::expand_views_schema;
use async_trait::async_trait;
use datafusion::config::{ConfigField, TableParquetOptions};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::{
    FileFormat, csv::CsvFormat, file_compression_type::FileCompressionType, json::JsonFormat,
    parquet::ParquetFormat,
};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::{config::SessionConfig, context::SessionContext};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path};
use snafu::prelude::*;
use url::Url;

use crate::accelerated_table::AcceleratedTable;
use crate::component::dataset::Dataset;
use crate::dataconnector::{
    ConnectorComponent, DataConnector, DataConnectorError, DataConnectorResult,
    listing::infer::{infer_partitions_with_types_from_files, infer_partitions_with_types_prefix},
};
use crate::parameters::{ExposedParamLookup, Parameters};
use data_components::object::{metadata::ObjectStoreMetadataTable, text::ObjectStoreTextTable};

use crate::object_store_registry::default_runtime_env;

use super::DelimitedFormat;

/// Maximum number of files to scan when validating that the schema source path contains objects with the expected extension.
const SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT: usize = 10_000;

#[async_trait]
pub trait ListingTableConnector: DataConnector {
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the object store URL for a given dataset.
    ///
    /// Determines the URL of the object store associated with the dataset.
    /// If a specific URL is provided as an argument, it uses that; otherwise, it derives
    /// the URL based on the dataset's configuration.
    ///
    /// # Arguments
    ///
    /// * `dataset` - A reference to the [`Dataset`] for which the object store URL is being retrieved.
    /// * `url` - An optional reference to a string representing a specific Path or URL to use.
    ///
    /// # Returns
    ///
    /// A [`DataConnectorResult`] containing the resolved [`Url`] of the object store.
    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url>;

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
        let store_url = self.get_object_store_url(dataset, None)?;
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
        let store_url: Url = self.get_object_store_url(dataset, None)?;
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
        }

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
        }

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
            .is_none_or(|f| f.eq_ignore_ascii_case("true"));
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

    async fn create_text_table(
        &self,
        dataset: &Dataset,
        url: &Url,
        extension: &str,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let content_formatter =
            document_parse::get_parser_factory(extension)
                .await
                .map(|factory| {
                    // TODO: add opts.
                    factory.default()
                });

        Ok(ObjectStoreTextTable::try_new(
            self.get_object_store(dataset)?,
            &url.clone(),
            Some(extension.to_string()),
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

    #[allow(clippy::too_many_lines)]
    async fn create_listing_table(
        &self,
        dataset: &Dataset,
        url: &Url,
        extension: &str,
        file_format: Arc<dyn FileFormat>,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        // This shouldn't error because we've already validated the URL in `get_object_store_url`.
        let table_path = ListingTableUrl::parse(url.clone()).boxed().context(
            crate::dataconnector::InternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
            },
        )?;

        let object_store = self.get_object_store(dataset)?;

        let ctx: SessionContext = Self::get_session_context();

        let (schema_infer_url, schema_infer_meta) =
            if let Some(url) = dataset.params.get("schema_source_path") {
                let url = self.get_object_store_url(dataset, Some(url))?;
                let schema_infer_url = ListingTableUrl::parse(url).boxed().context(
                    crate::dataconnector::UnableToGetSchemaInternalSnafu {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                    },
                )?;
                let schema_infer_meta = verify_schema_source_path(
                    format!("{self}"),
                    dataset,
                    extension,
                    schema_infer_url.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;
                (schema_infer_url, schema_infer_meta)
            } else {
                // Get the last modified object for the provided ObjectStore to infer the schema.
                // Report an error if no files matching required extension are found.
                let last_modified_or_added = get_last_modified(
                    format!("{self}"),
                    dataset,
                    extension,
                    table_path.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;

                (
                    to_listing_table_url(
                        url,
                        &last_modified_or_added.location,
                        dataset,
                        &format!("{self}"),
                    )?,
                    None,
                )
            };

        tracing::debug!(
            "Dataset '{}' schema will be resolved based on {schema_infer_url}",
            dataset.name
        );

        let mut options = ListingOptions::new(file_format).with_file_extension(extension);

        let resolved_schema = options
            .infer_schema(&ctx.state(), &schema_infer_url)
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

        options = add_metadata_columns_if_required(options, url, &expanded_schema, dataset);

        // If we should infer partitions and the path is a folder, infer the partitions from the folder structure.
        if dataset.get_param("hive_partitioning_enabled", false) && table_path.is_collection() {
            let inferred_partitions = match schema_infer_meta {
                Some(meta) => infer_partitions_with_types_from_files(&table_path, &[meta]),
                None => {
                    infer_partitions_with_types_prefix(&ctx.state(), &table_path, extension).await
                }
            };
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
        let table =
            ListingTable::try_new(config)
                .boxed()
                .context(crate::dataconnector::InternalSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
                })?;

        Ok(Arc::new(table))
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

        Some(self.construct_metadata_provider(dataset))
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let url = self.get_object_store_url(dataset, None)?;

        let (file_format_opt, extension) = self.get_file_format_and_extension(dataset)?;
        match file_format_opt {
            None => {
                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                self.create_text_table(dataset, &url, &extension).await
            }
            Some(file_format) => {
                // Structured tabular data, use a [`ListingTable`].
                self.create_listing_table(dataset, &url, &extension, file_format)
                    .await
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

fn add_metadata_columns_if_required(
    mut options: ListingOptions,
    table_url: &Url,
    schema: &Schema,
    dataset: &Dataset,
) -> ListingOptions {
    let url_prefix = get_url_prefix(table_url);
    if let Some(columns) = dataset.listing_table_metadata_columns(url_prefix, schema) {
        tracing::debug!(
            "Enabling metadata columns for '{}': {:?}",
            dataset.name,
            columns
        );
        options = options.with_metadata_cols(columns);
    }

    options
}

// Returns the prefix of the table URL, e.g. for "s3://mybucket/myfolder" it returns "s3://mybucket/"
fn get_url_prefix(table_url: &Url) -> String {
    format!("{}/", &table_url[..url::Position::BeforePath])
}

// 1024³
const BYTES_PER_GIB: f64 = 1_073_741_824.0;

/// Identifies the last modified object for a provided ListingTableConnector/ObjectStore
/// Infers if the `file_format` specified is valid, based on the existence of files with the required extension
///
/// # Errors
///
/// - If no files are found at the specified path
/// - If no files with the specified extension are found
async fn get_last_modified(
    dataconnector: String,
    dataset: &Dataset,
    extension: &str,
    table_path: ListingTableUrl,
    ctx: &SessionContext,
    object_store: &Arc<dyn ObjectStore>,
) -> DataConnectorResult<ObjectMeta> {
    tracing::debug!("Detecting the most recently modified object for the path: {table_path}");

    let state = ctx.state();
    let mut file_stream = table_path
        .list_all_files(&state, object_store, "")
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?;

    let mut last_modified_file: Option<ObjectMeta> = None;
    let mut found_extensions = HashSet::new();

    let mut file_count = 0;
    let mut total_size = 0;

    while let Some(file) =
        file_stream
            .try_next()
            .await
            .map_err(|err| DataConnectorError::UnableToConnectInternal {
                dataconnector: dataconnector.clone(),
                connector_component: ConnectorComponent::from(dataset),
                source: err.into(),
            })?
    {
        file_count += 1;
        total_size += file.size;

        #[allow(clippy::cast_precision_loss)]
        if file_count % 1_000_000 == 0 {
            tracing::debug!(
                "Continuing to process {table_path} metadata... {} objects processed so far, representing a total size of: {:.2} GiB",
                file_count,
                total_size as f64 / BYTES_PER_GIB
            );
        }

        if let Some(ext) = file.location.extension() {
            let file_ext = format!(".{ext}");
            found_extensions.insert(file_ext.clone());
            if file_ext == extension {
                if let Some(ref current) = last_modified_file {
                    if current.last_modified < file.last_modified {
                        last_modified_file = Some(file);
                    }
                } else {
                    last_modified_file = Some(file);
                }
            }
        }
    }

    if found_extensions.is_empty() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Failed to find any files matching the extension '{extension}'.\nSpice could not find any files with extensions at the specified path. Check the path and try again."
            ),
        });
    }

    if let Some(best) = last_modified_file {
        Ok(best)
    } else {
        let display_extensions = found_extensions
            .iter()
            .map(|e| format!("'{e}'"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Failed to find any files matching the extension '{extension}'.\nIs your `file_format` parameter correct? Spice found the following file extensions: {display_extensions}.\nFor details, visit: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
        })
    }
}

async fn verify_schema_source_path(
    dataconnector: String,
    dataset: &Dataset,
    extension: &str,
    schema_source_path: ListingTableUrl,
    ctx: &SessionContext,
    object_store: &Arc<dyn ObjectStore>,
) -> DataConnectorResult<Option<ObjectMeta>> {
    tracing::debug!(
        "Verifying dataset {table_name} schema source path is valid: {schema_source_path}",
        table_name = dataset.name
    );

    let state = ctx.state();
    // Intentionally not passing the `file_extension` parameter to `list_all_files` because we want to
    // short-circuit the listing process if we need to iterate over too many files.
    let mut file_stream = schema_source_path
        .list_all_files(&state, object_store, "")
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?;

    let mut scanned_files = 0;

    while let Some(file) =
        file_stream
            .try_next()
            .await
            .map_err(|err| DataConnectorError::UnableToConnectInternal {
                dataconnector: dataconnector.clone(),
                connector_component: ConnectorComponent::from(dataset),
                source: err.into(),
            })?
    {
        if let Some(ext) = file.location.extension() {
            if format!(".{ext}") == extension {
                return Ok(Some(file));
            }
        }

        scanned_files += 1;
        if scanned_files > SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT {
            // We've reached the limit of files to scan, but have not found any with the expected extension.
            // We do warning, not an error, as the dataset might have a large number of files.
            tracing::warn!(
                "Failed to find any files matching the extension '{extension}' at the specified path `{schema_source_path}` after scanning {SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT} files.\nEnsure the `schema_source_path` is correct."
            );
            return Ok(None);
        }
    }

    Err(DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.clone(),
        connector_component: ConnectorComponent::from(dataset),
        message: format!(
            "Failed to find any files matching the extension '{extension}' at the specified path `{schema_source_path}`.\nVerify that `schema_source_path` is correct and try again."
        ),
    })
}

fn to_listing_table_url(
    original_url: &Url,
    path: &Path,
    dataset: &Dataset,
    dataconnector: &str,
) -> DataConnectorResult<ListingTableUrl> {
    let mut new_url = original_url.clone();
    new_url.set_path(&format!("/{path}"));

    ListingTableUrl::parse(new_url).boxed().context(
        crate::dataconnector::UnableToGetSchemaInternalSnafu {
            dataconnector: dataconnector.to_string(),
            connector_component: ConnectorComponent::from(dataset),
        },
    )
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use datafusion_table_providers::util::secrets::to_secret_map;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use url::Url;

    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
    use crate::dataconnector::{ConnectorParams, DataConnectorFactory};
    use crate::parameters::ParameterSpec;

    use super::*;

    #[derive(Debug)]
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

        fn get_object_store_url(
            &self,
            dataset: &Dataset,
            _url: Option<&str>,
        ) -> DataConnectorResult<Url> {
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

    async fn setup_connector(
        path: String,
        params: HashMap<String, String>,
    ) -> (TestConnector, Dataset) {
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "test",
                TEST_PARAMETERS,
            ),
        };
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let dataset = DatasetBuilder::try_new(path, "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        (connector, dataset)
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_require_file_format() {
        let (connector, dataset) = setup_connector("test:test/".to_string(), HashMap::new()).await;

        match connector.get_file_format_and_extension(&dataset) {
            Ok(_) => panic!("Unexpected success"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Cannot setup the dataset test (TestConnector) with an invalid configuration.\nThe required 'file_format' parameter is missing.\nEnsure the parameter is provided, and try again."
            ),
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_csv_extension() {
        let (connector, dataset) =
            setup_connector("test:test.csv".to_string(), HashMap::new()).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_parquet_extension() {
        let (connector, dataset) =
            setup_connector("test:test.parquet".to_string(), HashMap::new()).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_csv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_tsv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "tsv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".tsv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_parquet_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset)
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[derive(Debug)]
    struct TestObjectStore {
        meta: Vec<ObjectMeta>,
    }

    impl TestObjectStore {
        fn new(meta: Vec<ObjectMeta>) -> Self {
            Self { meta }
        }
    }

    impl std::fmt::Display for TestObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestObjectStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestObjectStore {
        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            stream::iter(self.meta.clone().into_iter().map(Ok)).boxed()
        }

        async fn put(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        async fn get(&self, _location: &Path) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }
        async fn get_opts(
            &self,
            _location: &Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }
        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        fn delete_stream<'a>(
            &'a self,
            _locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }
        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    fn create_meta(location: &str, last_modified_secs: i64, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: Utc
                .timestamp_opt(last_modified_secs, 0)
                .single()
                .expect("valid timestamp"),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[tokio::test]
    async fn test_get_last_modified_returns_latest() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let meta_files = vec![
            create_meta("file_old.parquet", 100, 100),
            create_meta("file_new.parquet", 200, 200),
            create_meta("file_other.csv", 300, 300),
            create_meta("file_other.parquet", 150, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let last_modified = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect("to get last modified");

        assert_eq!(last_modified.location.as_ref(), "file_new.parquet");
    }

    #[tokio::test]
    async fn test_get_last_modified_no_matching_extension() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let meta_files = vec![
            create_meta("file_old.parquet", 100, 100),
            create_meta("file_new.parquet", 200, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".csv",
            table_path,
            &ctx,
            &test_store,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_verify_schema_source_path_valid() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let meta_files = vec![
            create_meta("schema/file1.parquet", 100, 100),
            create_meta("schema/file2.csv", 200, 200),
            create_meta("schema/file3.parquet", 300, 300),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path,
            &ctx,
            &test_store,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_verify_schema_source_path_no_matching_files() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let meta_files = vec![
            create_meta("schema/file1.csv", 100, 100),
            create_meta("schema/file2.csv", 200, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path.clone(),
            &ctx,
            &test_store,
        )
        .await;

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string(),
                format!(
                    "Cannot setup the dataset test (TestListingConnector) with an invalid configuration.\nFailed to find any files matching the extension '.parquet' at the specified path `{schema_source_path}`.\nVerify that `schema_source_path` is correct and try again."
                )
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::cast_possible_wrap)]
    async fn test_verify_schema_source_path_file_limit() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        // Create more files than SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT
        let meta_files: Vec<ObjectMeta> = (0..SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT + 100)
            .map(|i| create_meta(&format!("schema/file{i}.csv"), 100 + i as i64, 100))
            .collect();

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path,
            &ctx,
            &test_store,
        )
        .await;

        // Should return Ok even though no matching files were found,
        // because we hit the scan limit
        assert!(result.is_ok(), "Expected Ok, got {result:?}");
    }

    #[test]
    fn test_get_url_prefix_basic() {
        let url = Url::parse("s3://mybucket/").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_path() {
        let url = Url::parse("s3://mybucket/folder/file.txt").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_query() {
        let url = Url::parse("s3://mybucket/file.txt?version=1").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_fragment() {
        let url = Url::parse("s3://mybucket/file.txt#section1").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_port() {
        let url = Url::parse("http://localhost:8080/path").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "http://localhost:8080/");
    }

    #[test]
    fn test_get_url_prefix_without_host() {
        let url = Url::parse("file:///absolute/path").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "file:///");
    }
}
