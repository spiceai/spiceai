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
use data_components::iceberg::catalog::hadoop::HadoopCatalogBuilder;
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
        ICEBERG_PARAM_LEN, get_rest_catalog, map_param_name_to_iceberg_prop,
        parse_hadoop_table_url, parse_table_url, verify_s3_endpoint,
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
        .description("The Iceberg metadata file to read the table from, as a path relative to the table root in `from` (for example, `/metadata/00003-8f2c.metadata.json`) or as a full URL. When set, the table is read from exactly this file, without a catalog."),
    ParameterSpec::runtime(LEGACY_METADATA_PATH_PARAM)
        .deprecated("Use `iceberg_metadata_path`, which takes the same value and also accepts a path relative to `from`, such as `/metadata/v1.metadata.json`."),
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

        if is_metadata_file(source) {
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

        if is_storage_url(source) {
            if let Some(metadata_path) = self.metadata_path() {
                return IcebergDataConnector::load_pinned_table(
                    &props,
                    storage_factory,
                    dataset,
                    source,
                    &metadata_path,
                )
                .await;
            }

            return IcebergDataConnector::load_hadoop_catalog(
                props,
                storage_factory,
                dataset,
                source,
            )
            .await;
        }

        if self.metadata_path().is_some() {
            tracing::warn!(
                "Dataset {} ignores `iceberg_metadata_path`: it applies only when `from` is an object-store or file path, and this dataset reads the table through a REST catalog.",
                dataset.name
            );
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

    /// The metadata file the dataset pins, under either accepted spelling.
    fn metadata_path(&self) -> Option<String> {
        [METADATA_PATH_PARAM, LEGACY_METADATA_PATH_PARAM]
            .into_iter()
            .find_map(|name| {
                self.params
                    .get(name)
                    .ok()
                    .map(|value| value.expose_secret().to_string())
            })
    }

    /// Loads the table from exactly the metadata file `metadata_path` names, with no
    /// catalog and no listing of the table's `metadata/` directory, so the table
    /// root needs no particular layout.
    async fn load_pinned_table(
        props: &HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        dataset: &DatasetSpec,
        source: &str,
        metadata_path: &str,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let location = resolve_metadata_location(source, metadata_path).map_err(|message| {
            Error::InvalidConfigurationNoSource {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                message,
            }
        })?;

        let factory = storage_factory.unwrap_or_else(|| default_storage_factory(&location));
        let file_io = FileIOBuilder::new(factory)
            .with_props(props.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .build();

        // The table is served under the dataset's own name, so two datasets reading
        // tables from paths that end alike never share an identity.
        let table_identifier = TableIdent::new(
            NamespaceIdent::new(dataset.name.schema().unwrap_or("public").to_string()),
            dataset.name.table().to_string(),
        );

        tracing::debug!(
            "Dataset {} reads Iceberg metadata file '{location}'",
            dataset.name
        );
        let catalog = MetadataFileCatalog::try_new(&location, table_identifier.clone(), file_io)
            .await
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: unreadable_metadata_file_message(&location, &e).into(),
            })?;
        let catalog_client: Arc<dyn Catalog> = Arc::new(catalog);

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

    async fn load_hadoop_catalog(
        props: HashMap<String, String>,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        dataset: &DatasetSpec,
        source: &str,
    ) -> super::DataConnectorResult<IcebergTableParts> {
        let (base_uri, namespace, table_name) = parse_hadoop_table_url(source, None).map_err(|e| {
                Error::InvalidConfiguration {
                    dataconnector: "iceberg".into(),
                    message: format!("A Dataset Path is required for Iceberg in the format of: file:///tmp/hadoop_warehouse/<namespace>/<table_name>, s3://<bucket>/<namespace>/<table_name>, gs://<bucket>/<namespace>/<table_name>, or gcs://<bucket>/<namespace>/<table_name>. For details, visit: https://spiceai.org/docs/components/data-connectors/iceberg#from. {e}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?;

        // Load the specific table
        let table_identifier = TableIdent::new(namespace.name().clone(), table_name);

        // Determine storage factory from scheme if not explicitly provided
        let factory = storage_factory.unwrap_or_else(|| default_storage_factory(source));

        // Build an opendal operator for directory listing (scoped to the warehouse root, not the table URL)
        let operator = crate::catalogconnector::iceberg::build_opendal_operator(&base_uri, &props)
            .map_err(|e| Error::UnableToGetReadProvider {
                dataconnector: "iceberg".into(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;

        let catalog_builder = HadoopCatalogBuilder::default()
            .with_warehouse_root(base_uri)
            .with_storage_factory(factory)
            .with_operator(operator)
            .with_properties(props);

        let catalog_client: Arc<dyn Catalog> =
            Arc::new(catalog_builder.build().await.map_err(|e| {
                Error::UnableToGetReadProvider {
                    dataconnector: "iceberg".into(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }
            })?);

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
        // Refused before anything is built: an Iceberg insert writes its data files
        // before the commit the catalog-less table would reject, which would leave
        // orphan files in the table's location.
        if is_storage_url(dataset.path()) && self.metadata_path().is_some() {
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
const READ_ONLY_PINNED_TABLE_MESSAGE: &str = "An Iceberg table read from its metadata file (`iceberg_metadata_path`) has no catalog, so it is read-only. Remove `access: read_write` from the dataset, or read the table through an Iceberg catalog to write to it. See: https://spiceai.org/docs/components/data-connectors/iceberg";

/// Whether `source` addresses object storage or the local filesystem, rather than a
/// catalog service.
fn is_storage_url(source: &str) -> bool {
    ["file://", "s3://", "s3a://", "gs://", "gcs://"]
        .iter()
        .any(|scheme| source.starts_with(scheme))
}

/// Whether `source` names a metadata file rather than a table: plain
/// (`.metadata.json`), gzip-compressed (`.gz.metadata.json`), or gzip-compressed
/// under the older Java naming (`.metadata.json.gz`).
fn is_metadata_file(source: &str) -> bool {
    source.ends_with(".metadata.json") || source.ends_with(".metadata.json.gz")
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
}
