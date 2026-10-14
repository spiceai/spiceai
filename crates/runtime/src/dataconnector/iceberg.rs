/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The Iceberg Data Connector is a thin layer over the Iceberg Catalog Connector.
//! It takes the same parameters as the Catalog Connector.

use crate::dataconnector::ConnectorContext;
use std::{any::Any, collections::HashMap, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use aws_sdk_credential_bridge::S3CredentialProvider;
use data_components::iceberg::catalog::metadata_file::MetadataFileCatalog;
use datafusion::catalog::TableProvider;
use iceberg::{
    Catalog, NamespaceIdent, TableIdent,
    io::{FileIOBuilder, StorageFactory},
};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use secrecy::ExposeSecret;
use util::concat_arrays;

use super::DataConnectorFactory;
use super::iceberg_cluster::IcebergClusterTableProvider;
use crate::{
    catalogconnector::iceberg::{
        ICEBERG_PARAM_LEN, build_opendal_operator, get_rest_catalog, is_storage_url,
        map_param_name_to_iceberg_prop, parse_table_url, verify_s3_endpoint,
    },
    component::dataset::DatasetSpec,
    dataconnector::{
        ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError as Error,
        parameters::aws::initiate_config_with_credentials,
    },
    parameters::{ParameterSpec, Parameters},
};

#[derive(Default, Debug, Copy, Clone)]
pub struct IcebergDataConnectorFactory {}

impl IcebergDataConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

/// The parameter that pins the exact metadata file a table is read from.
const METADATA_PATH_PARAM: &str = "iceberg_metadata_path";
/// The earlier name of [`METADATA_PATH_PARAM`], still accepted.
const LEGACY_METADATA_PATH_PARAM: &str = "metadata_path";

const ICEBERG_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/iceberg";

const HADOOP_PARAM_LEN: usize = 2;
pub(crate) const HADOOP_PARAMETERS: [ParameterSpec; HADOOP_PARAM_LEN] = [
    // Registered under its full name rather than as a component param: the earlier,
    // unprefixed `metadata_path` shares its unprefixed name, and the two must
    // resolve to distinct keys for both spellings to be accepted.
    ParameterSpec::runtime(METADATA_PATH_PARAM)
        .description("The Iceberg metadata file to read the table from, as a path relative to the table root in `from` (for example, `/metadata/00003-8f2c.metadata.json`) or as a full URL. When set, the table is read from exactly this file, without a catalog. A version hint file (`/metadata/version-hint.text`) is read on every query instead, and names the metadata file to read."),
    ParameterSpec::runtime(LEGACY_METADATA_PATH_PARAM)
        .deprecated("Use `iceberg_metadata_path`, which accepts the same full URL and also a path relative to `from`, such as `/metadata/v1.metadata.json`."),
];

pub(crate) const PARAMETERS: &[ParameterSpec] = &concat_arrays::<
    ParameterSpec,
    HADOOP_PARAM_LEN,
    ICEBERG_PARAM_LEN,
    { HADOOP_PARAM_LEN + ICEBERG_PARAM_LEN },
>(
    HADOOP_PARAMETERS,
    crate::catalogconnector::iceberg::PARAMETERS,
);

impl DataConnectorFactory for IcebergDataConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            let iceberg = IcebergDataConnector {
                params: params.parameters,
            };
            Ok(Arc::new(iceberg) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "iceberg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// Holds the components needed for read, read-write, and distributed Iceberg
/// providers: the base provider plus the catalog and identity used to reload the
/// table on remote executors during distributed (Ballista) execution.
struct IcebergTableParts {
    provider: Arc<dyn TableProvider>,
    #[cfg(feature = "iceberg-write")]
    catalog: Arc<dyn Catalog>,
    #[cfg(feature = "iceberg-write")]
    table_identifier: TableIdent,
}

#[derive(Clone, Debug)]
pub struct IcebergDataConnector {
    params: Parameters,
}

impl IcebergDataConnector {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn DataConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    /// Creates the Iceberg table provider along with the catalog and table
    /// identity, used to build read, read-write, and distributed providers.
    async fn create_iceberg_table_parts(
        &self,
        dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let source = dataset.path();
        let table_source = self.table_source(source);

        if matches!(table_source, TableSource::MetadataFileInFrom) {
            return Err(Error::InvalidConfigurationNoSource {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                message: metadata_file_in_from_message(source),
            });
        }

        let mut props = HashMap::new();
        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        let storage_factory: Option<Arc<dyn StorageFactory>> =
            if let Some(endpoint) = props.get("s3.endpoint") {
                verify_s3_endpoint(endpoint)
                    .await
                    .map_err(|e| Error::InvalidConfiguration {
                        dataconnector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    })?;

                let aws_sdk_config = initiate_config_with_credentials(
                    "IcebergDataConnector",
                    "s3_region",
                    "s3_access_key_id",
                    "s3_secret_access_key",
                    "s3_session_token",
                    &self.params,
                    self.params.get("s3_iam_role_source").expose().ok(),
                )
                .await
                .map_err(|e| Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                })?
                .load()
                .await;

                let custom_loader = S3CredentialProvider::from_config(&aws_sdk_config)
                    .map_err(|e| Error::InvalidConfiguration {
                        dataconnector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    })?
                    .into_custom_loader();

                Some(Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: Some(custom_loader),
                }) as Arc<dyn StorageFactory>)
            } else {
                None
            };

        match table_source {
            TableSource::Pinned(pinned) => {
                return IcebergDataConnector::load_pinned_table(
                    &props,
                    storage_factory,
                    dataset,
                    source,
                    &pinned,
                )
                .await;
            }
            TableSource::StoragePath => {
                return IcebergDataConnector::load_table_root(
                    &props,
                    storage_factory,
                    dataset,
                    source,
                )
                .await;
            }
            TableSource::RestCatalog {
                ignored_param: Some(param),
            } => {
                tracing::warn!(
                    "{}",
                    ignored_pin_on_rest_catalog_message(&dataset.name, param)
                );
            }
            TableSource::RestCatalog {
                ignored_param: None,
            }
            | TableSource::MetadataFileInFrom => {}
        }

        let (base_uri, new_props, namespace, table_name) = match parse_table_url(source) {
            Ok(result) => result,
            Err(e) => {
                return Err(Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: format!(
                        "A Dataset Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>/tables/<table_name>.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/iceberg#from\n{e}"
                    ),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                });
            }
        };

        props.extend(new_props);

        let catalog_client = get_rest_catalog(base_uri, props, storage_factory.clone())
            .await
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        let catalog_client: Arc<dyn Catalog> = Arc::new(catalog_client);

        // Load the specific table
        let namespace_ident = namespace.name().clone();
        let table_identifier = TableIdent::new(namespace_ident, table_name.clone());

        table_parts(catalog_client, table_identifier, dataset).await
    }

    /// Where the dataset's table is read from, decided from its configuration
    /// alone. The read path and the write refusal both match on it, so they cannot
    /// disagree about which datasets are pinned.
    fn table_source(&self, source: &str) -> TableSource {
        if is_metadata_file(source) {
            TableSource::MetadataFileInFrom
        } else if is_storage_url(source) {
            self.pinned_metadata()
                .map_or(TableSource::StoragePath, TableSource::Pinned)
        } else {
            TableSource::RestCatalog {
                ignored_param: self.pinned_metadata().map(|pinned| pinned.param),
            }
        }
    }

    /// The metadata file the dataset pins, and the parameter that names it.
    fn pinned_metadata(&self) -> Option<PinnedMetadata> {
        [METADATA_PATH_PARAM, LEGACY_METADATA_PATH_PARAM]
            .into_iter()
            .find_map(|param| {
                self.params.get(param).ok().map(|value| PinnedMetadata {
                    param,
                    value: value.expose_secret().to_string(),
                })
            })
    }

    /// Loads the table from the file `iceberg_metadata_path` names, with no catalog
    /// and no listing of the table's `metadata/` directory. A metadata file is read
    /// once; a version hint file is read on every query.
    async fn load_pinned_table(
        props: &HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        dataset: &DatasetSpec,
        source: &str,
        pinned: &PinnedMetadata,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let location =
            pinned
                .location(source)
                .map_err(|message| Error::InvalidConfigurationNoSource {
                    dataconnector: "iceberg".into(),
                    connector_component: ConnectorComponent::from(dataset),
                    message,
                })?;

        let file_io = file_io(props, storage_factory, &location);
        let table_identifier = dataset_table_ident(dataset);
        let catalog = MetadataFileCatalog::try_new(&location, table_identifier.clone(), file_io)
            .await
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: unreadable_metadata_file_message(&location, &e).into(),
            })?;

        serve_table(catalog, table_identifier, dataset, &location).await
    }

    /// Loads the table rooted at `from`, with no catalog. Each query reads the
    /// metadata file `metadata/version-hint.text` names, or else the one with the
    /// highest version number in `metadata/`, so new commits are read as they land.
    async fn load_table_root(
        props: &HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        dataset: &DatasetSpec,
        source: &str,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let root = with_gcs_scheme_as_gs(source.trim_end_matches('/').to_string());
        let operator =
            build_opendal_operator(&root, props).map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;
        let file_io = file_io(props, storage_factory, &root);
        let table_identifier = dataset_table_ident(dataset);
        let catalog =
            MetadataFileCatalog::for_table_root(&root, operator, table_identifier.clone(), file_io);

        serve_table(catalog, table_identifier, dataset, &root).await
    }
}

#[async_trait]
impl DataConnector for IcebergDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let parts = self.create_iceberg_table_parts(dataset).await?;

        // Wrap so the scan can be serialized for distributed (Ballista) execution.
        // In a single-node session this is a transparent pass-through.
        let inner = parts.provider;
        Ok(Arc::new(IcebergClusterTableProvider::new(
            dataset.name.clone(),
            Arc::clone(&inner),
        ))
        .into_table() as Arc<dyn TableProvider>)
    }

    #[cfg(feature = "iceberg-write")]
    async fn read_write_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        // A table pinned to one metadata file has no catalog to commit to. Refused
        // before anything is built: an Iceberg insert writes its data files before
        // the commit that would be rejected, which would leave orphan files in a
        // table location Spice usually does not own. Writes kept in the acceleration
        // (`write_mode: acceleration`) never reach the source, so they never ask for
        // this provider and are unaffected.
        if matches!(self.table_source(dataset.path()), TableSource::Pinned(_)) {
            return Some(Err(Error::InvalidConfigurationNoSource {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                message: READ_ONLY_PINNED_TABLE_MESSAGE.to_string(),
            }));
        }

        // Create the table parts which include catalog + identity for delete support
        let parts = match self.create_iceberg_table_parts(dataset).await {
            Ok(parts) => parts,
            Err(e) => return Some(Err(e)),
        };

        // Stack a deletion layer for DELETE FROM support.
        let base = parts.provider;
        let deletion_provider = data_components::iceberg::delete::IcebergDeletionProvider::new(
            parts.catalog,
            parts.table_identifier.namespace().clone(),
            parts.table_identifier.name().to_string(),
            Arc::clone(&base),
        );
        let deletable = spice_table::SpiceTable::over(Arc::new(deletion_provider), base);

        // Then a cluster layer, so the scan can be serialized for distributed execution.
        Some(Ok(Arc::new(IcebergClusterTableProvider::new(
            dataset.name.clone(),
            deletable as Arc<dyn TableProvider>,
        ))
        .into_table() as Arc<dyn TableProvider>))
    }
}

data_connector_api::register_data_connector!("iceberg", IcebergDataConnectorFactory);

#[cfg(feature = "iceberg-write")]
const READ_ONLY_PINNED_TABLE_MESSAGE: &str = "An Iceberg table read from its metadata file (`iceberg_metadata_path`) has no catalog to commit to, so it is read-only. Remove `access: read_write`, keep writes in the acceleration with `acceleration.write_mode: acceleration`, or read the table through an Iceberg catalog to write to it. See: https://spiceai.org/docs/components/data-connectors/iceberg";

/// Where an Iceberg dataset's table is read from.
enum TableSource {
    /// `from` names a metadata file instead of the table root.
    MetadataFileInFrom,
    /// A table root on storage, read from the metadata file the dataset pins.
    Pinned(PinnedMetadata),
    /// A table root on storage whose current metadata file is found from its
    /// `metadata/` directory.
    StoragePath,
    /// A table served by an Iceberg REST catalog. `ignored_param` names a pinning
    /// parameter that was set but does not apply.
    RestCatalog { ignored_param: Option<&'static str> },
}

/// Builds the table provider for `table_identifier` in `catalog_client`, keeping
/// the catalog and identity the read-write and distributed providers need.
async fn table_parts(
    catalog_client: Arc<dyn Catalog>,
    table_identifier: TableIdent,
    dataset: &DatasetSpec,
) -> super::DataConnectorResult<IcebergTableParts> {
    let table_provider = IcebergTableProvider::try_new(
        Arc::clone(&catalog_client),
        table_identifier.namespace().clone(),
        table_identifier.name().to_string(),
    )
    .await
    .map_err(|e| Error::UnableToGetReadProvider {
        dataconnector: "iceberg".into(),
        connector_component: ConnectorComponent::from(dataset),
        source: Box::new(e),
    })?;

    Ok(IcebergTableParts {
        provider: Arc::new(table_provider),
        #[cfg(feature = "iceberg-write")]
        catalog: catalog_client,
        #[cfg(feature = "iceberg-write")]
        table_identifier,
    })
}

/// Serves the table `catalog` reads under `table_identifier`, after checking that
/// its current metadata file can be found, so a misconfigured dataset fails to
/// load rather than on its first query.
async fn serve_table(
    catalog: MetadataFileCatalog,
    table_identifier: TableIdent,
    dataset: &DatasetSpec,
    location: &str,
) -> super::DataConnectorResult<IcebergTableParts> {
    let current = catalog
        .current_metadata()
        .await
        .map_err(|e| Error::UnableToGetReadProvider {
            dataconnector: "iceberg".into(),
            connector_component: ConnectorComponent::from(dataset),
            source: unresolved_metadata_message(location, &e).into(),
        })?;
    if current.listed {
        tracing::warn!(
            "{}",
            found_by_listing_message(&dataset.name, location, &current.location)
        );
    } else {
        tracing::debug!(
            "Dataset '{}' reads Iceberg metadata file '{}'",
            dataset.name,
            current.location
        );
    }

    table_parts(Arc::new(catalog), table_identifier, dataset).await
}

/// The identity a catalog-less table is served under: the dataset's own name, so
/// two datasets reading tables from paths that end alike never share an identity.
fn dataset_table_ident(dataset: &DatasetSpec) -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(dataset.name.schema().unwrap_or("public").to_string()),
        dataset.name.table().to_string(),
    )
}

/// The `FileIO` that reads `location`, with the configured credentials.
fn file_io(
    props: &HashMap<String, String>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    location: &str,
) -> iceberg::io::FileIO {
    let factory = storage_factory.unwrap_or_else(|| default_storage_factory(location));
    FileIOBuilder::new(factory)
        .with_props(props.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .build()
}

/// The metadata file a dataset pins, with the parameter that named it.
struct PinnedMetadata {
    param: &'static str,
    value: String,
}

impl PinnedMetadata {
    /// The URL of the pinned file. `iceberg_metadata_path` resolves against the
    /// table root in `from`; the earlier `metadata_path` keeps its meaning and is
    /// used as given.
    fn location(&self, table_root: &str) -> Result<String, String> {
        let location = if self.param == METADATA_PATH_PARAM {
            resolve_metadata_location(table_root, &self.value)?
        } else {
            self.value.clone()
        };
        Ok(with_gcs_scheme_as_gs(location))
    }
}

/// Iceberg's GCS storage addresses objects only as `gs://`; `gcs://` names the
/// same location.
fn with_gcs_scheme_as_gs(location: String) -> String {
    match location.strip_prefix("gcs://") {
        Some(rest) => format!("gs://{rest}"),
        None => location,
    }
}

/// Whether `source` names a metadata file rather than a table: plain
/// (`.metadata.json`), gzip-compressed (`.gz.metadata.json`), or gzip-compressed
/// under the older Java naming (`.metadata.json.gz`).
/// Only a storage URL can name one: a REST catalog URL is a table address whose
/// last segment is the table name, whatever that name ends with.
fn is_metadata_file(source: &str) -> bool {
    is_storage_url(source)
        && (source.ends_with(".metadata.json") || source.ends_with(".metadata.json.gz"))
}

/// The storage backend for `url`'s scheme, used when no credentials-aware factory
/// was configured.
fn default_storage_factory(url: &str) -> Arc<dyn StorageFactory> {
    if url.starts_with("gs://") || url.starts_with("gcs://") {
        Arc::new(OpenDalStorageFactory::Gcs)
    } else if url.starts_with("s3://") || url.starts_with("s3a://") {
        Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        })
    } else {
        Arc::new(iceberg::io::LocalFsStorageFactory)
    }
}

/// Resolves an `iceberg_metadata_path` value to the URL of the metadata file.
///
/// A value with a URL scheme is used as given. Any other value is a path relative to
/// the table root `table_root`: a leading `/` means that root, not the bucket or
/// filesystem root, and is optional. A path that leaves the root (`..`) is refused.
fn resolve_metadata_location(table_root: &str, metadata_path: &str) -> Result<String, String> {
    if metadata_path.contains("://") {
        return Ok(metadata_path.to_string());
    }

    let relative = metadata_path.trim_start_matches('/');
    if relative.is_empty() {
        return Err(format!(
            "`iceberg_metadata_path` names no file. Set it to the metadata file to read, relative to `from` (for example, '/metadata/00003-8f2c.metadata.json'), or remove it. See: {ICEBERG_DOCS}"
        ));
    }
    if relative.split('/').any(|segment| segment == "..") {
        return Err(format!(
            "`iceberg_metadata_path` '{metadata_path}' points outside the table root in `from`. Give a path inside the table root, such as '/metadata/00003-8f2c.metadata.json', or a full URL. See: {ICEBERG_DOCS}"
        ));
    }

    Ok(format!("{}/{relative}", table_root.trim_end_matches('/')))
}

/// The configuration error for a `from` that names a metadata file instead of the
/// table root, spelling out the corrected `from` and `iceberg_metadata_path` when
/// the file sits in the conventional `metadata/` directory.
fn metadata_file_in_from_message(source: &str) -> String {
    match source.rsplit_once("/metadata/") {
        Some((root, file)) => format!(
            "`from` names an Iceberg metadata file, but it must name the table root. Set `from` to 'iceberg:{root}' and `iceberg_metadata_path` to '/metadata/{file}'. See: {ICEBERG_DOCS}"
        ),
        None => format!(
            "`from` names an Iceberg metadata file ('{source}'), but it must name the table root. Set `from` to the table's root directory and `iceberg_metadata_path` to the metadata file, relative to that root. See: {ICEBERG_DOCS}"
        ),
    }
}

/// The warning for a pinning parameter set on a dataset that reads its table
/// through a REST catalog, where it has no effect.
fn ignored_pin_on_rest_catalog_message(dataset: &impl std::fmt::Display, param: &str) -> String {
    format!(
        "Dataset '{dataset}' ignores `{param}`, so it reads the table's current metadata from its REST catalog: `{param}` applies only when `from` is the table root on object storage or a file path. Remove `{param}`, or set `from` to the table root to read the pinned file without a catalog. See: {ICEBERG_DOCS}"
    )
}

/// The cause reported when the current metadata file of the table at `location`
/// (a table root or a version hint) cannot be found.
fn unresolved_metadata_message(location: &str, error: &iceberg::Error) -> String {
    format!(
        "Failed to find the current Iceberg metadata file for '{location}': {error}. Verify `from` is the table root, the directory that contains 'metadata/', or set `iceberg_metadata_path` to the metadata file to read. See: {ICEBERG_DOCS}"
    )
}

/// The warning for a table whose current metadata file is found by listing, which
/// can pick up a commit that is still in progress or that failed.
fn found_by_listing_message(
    dataset: &impl std::fmt::Display,
    table_root: &str,
    metadata_file: &str,
) -> String {
    format!(
        "Dataset '{dataset}' has no 'metadata/version-hint.text', so each query reads the Iceberg table at '{table_root}' from the metadata file with the highest version number (now '{metadata_file}'). Without a catalog, a commit that is in progress or that failed can be read as the current table. To read a fixed snapshot, set `iceberg_metadata_path`. See: {ICEBERG_DOCS}"
    )
}

/// The cause reported when the pinned metadata file cannot be loaded.
fn unreadable_metadata_file_message(location: &str, error: &iceberg::Error) -> String {
    format!(
        "Failed to read the Iceberg metadata file '{location}' named by `iceberg_metadata_path`: {error}. Verify the file exists and the path is relative to `from` (for example, '/metadata/00003-8f2c.metadata.json') or a full URL. See: {ICEBERG_DOCS}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "s3://my-bucket/warehouse/db/taxis";
    const FILE: &str = "00003-8f2c.metadata.json";

    #[test]
    fn a_relative_metadata_path_resolves_under_the_table_root_with_or_without_a_leading_slash() {
        let expected = format!("{ROOT}/metadata/{FILE}");
        for value in [
            format!("/metadata/{FILE}"),
            format!("metadata/{FILE}"),
            format!("//metadata/{FILE}"),
        ] {
            assert_eq!(
                resolve_metadata_location(ROOT, &value),
                Ok(expected.clone()),
                "{value}"
            );
        }
        assert_eq!(
            resolve_metadata_location(&format!("{ROOT}/"), &format!("/metadata/{FILE}")),
            Ok(expected),
            "a trailing slash on `from` must not double the separator"
        );
    }

    #[test]
    fn a_relative_metadata_path_under_a_local_root_stays_an_absolute_file_url() {
        assert_eq!(
            resolve_metadata_location(
                "file:///srv/warehouse/db/taxis",
                "/metadata/v3.metadata.json"
            ),
            Ok("file:///srv/warehouse/db/taxis/metadata/v3.metadata.json".to_string())
        );
    }

    #[test]
    fn a_full_url_metadata_path_is_used_as_given_even_outside_the_table_root() {
        let elsewhere = "s3a://other-bucket/meta/db/taxis/v3.metadata.json";
        assert_eq!(
            resolve_metadata_location(ROOT, elsewhere),
            Ok(elsewhere.to_string())
        );
    }

    #[test]
    fn a_metadata_path_that_leaves_the_table_root_is_refused() {
        for value in [
            "../other/metadata/v1.metadata.json",
            "/metadata/../../v1.metadata.json",
        ] {
            assert_eq!(
                resolve_metadata_location(ROOT, value),
                Err(format!(
                    "`iceberg_metadata_path` '{value}' points outside the table root in `from`. Give a path inside the table root, such as '/metadata/00003-8f2c.metadata.json', or a full URL. See: https://spiceai.org/docs/components/data-connectors/iceberg"
                ))
            );
        }
        // Two dots inside a file name are not a parent reference.
        assert_eq!(
            resolve_metadata_location(ROOT, "/metadata/a..b.metadata.json"),
            Ok(format!("{ROOT}/metadata/a..b.metadata.json"))
        );
    }

    #[test]
    fn a_metadata_path_naming_no_file_is_refused() {
        for value in ["", "/", "///"] {
            assert_eq!(
                resolve_metadata_location(ROOT, value),
                Err("`iceberg_metadata_path` names no file. Set it to the metadata file to read, relative to `from` (for example, '/metadata/00003-8f2c.metadata.json'), or remove it. See: https://spiceai.org/docs/components/data-connectors/iceberg".to_string()),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_from_naming_a_metadata_file_is_told_the_exact_from_and_metadata_path_to_use() {
        let source = format!("{ROOT}/metadata/{FILE}");
        assert!(is_metadata_file(&source));
        assert_eq!(
            metadata_file_in_from_message(&source),
            "`from` names an Iceberg metadata file, but it must name the table root. Set `from` to 'iceberg:s3://my-bucket/warehouse/db/taxis' and `iceberg_metadata_path` to '/metadata/00003-8f2c.metadata.json'. See: https://spiceai.org/docs/components/data-connectors/iceberg"
        );
    }

    #[test]
    fn the_earlier_metadata_path_is_used_as_given_and_both_names_take_gcs_urls_as_gs() {
        let legacy = PinnedMetadata {
            param: LEGACY_METADATA_PATH_PARAM,
            value: "/srv/warehouse/db/taxis/metadata/v3.metadata.json".to_string(),
        };
        assert_eq!(
            legacy.location("file:///srv/warehouse/db/taxis"),
            Ok("/srv/warehouse/db/taxis/metadata/v3.metadata.json".to_string()),
            "`metadata_path` must keep its earlier meaning, not resolve against `from`"
        );

        let relative = PinnedMetadata {
            param: METADATA_PATH_PARAM,
            value: "/metadata/v3.metadata.json".to_string(),
        };
        assert_eq!(
            relative.location("gcs://bucket/db/taxis"),
            Ok("gs://bucket/db/taxis/metadata/v3.metadata.json".to_string())
        );
        let legacy_gcs = PinnedMetadata {
            param: LEGACY_METADATA_PATH_PARAM,
            value: "gcs://bucket/db/taxis/metadata/v3.metadata.json".to_string(),
        };
        assert_eq!(
            legacy_gcs.location("gcs://bucket/db/taxis"),
            Ok("gs://bucket/db/taxis/metadata/v3.metadata.json".to_string())
        );
    }

    #[test]
    fn a_rest_catalog_url_ending_in_metadata_json_is_not_a_metadata_file_in_from() {
        assert!(!is_metadata_file(
            "https://catalog.example/v1/namespaces/ns/tables/foo.metadata.json"
        ));
        assert!(is_metadata_file(&format!("{ROOT}/metadata/{FILE}")));
    }

    #[test]
    fn a_pin_on_a_rest_catalog_dataset_is_reported_with_the_fix() {
        assert_eq!(
            ignored_pin_on_rest_catalog_message(&"taxis", "iceberg_metadata_path"),
            "Dataset 'taxis' ignores `iceberg_metadata_path`, so it reads the table's current metadata from its REST catalog: `iceberg_metadata_path` applies only when `from` is the table root on object storage or a file path. Remove `iceberg_metadata_path`, or set `from` to the table root to read the pinned file without a catalog. See: https://spiceai.org/docs/components/data-connectors/iceberg"
        );
    }

    #[test]
    fn a_from_naming_a_compressed_metadata_file_is_recognised_but_a_table_root_is_not() {
        assert!(is_metadata_file(&format!(
            "{ROOT}/metadata/00003-8f2c.gz.metadata.json"
        )));
        assert!(is_metadata_file(&format!(
            "{ROOT}/metadata/v3.metadata.json.gz"
        )));
        assert!(!is_metadata_file(ROOT));
        assert!(!is_metadata_file(&format!("{ROOT}/metadata")));
    }

    #[test]
    fn a_table_found_by_listing_is_reported_with_how_to_pin_it() {
        assert_eq!(
            found_by_listing_message(&"taxis", ROOT, &format!("{ROOT}/metadata/{FILE}")),
            "Dataset 'taxis' has no 'metadata/version-hint.text', so each query reads the Iceberg table at 's3://my-bucket/warehouse/db/taxis' from the metadata file with the highest version number (now 's3://my-bucket/warehouse/db/taxis/metadata/00003-8f2c.metadata.json'). Without a catalog, a commit that is in progress or that failed can be read as the current table. To read a fixed snapshot, set `iceberg_metadata_path`. See: https://spiceai.org/docs/components/data-connectors/iceberg"
        );
    }
}
