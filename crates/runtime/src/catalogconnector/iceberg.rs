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

use super::{CatalogConnector, ConnectorComponent, ParameterSpec, Parameters};
use crate::{
    Runtime,
    component::catalog::{Catalog, table_selector},
    dataconnector::parameters::{ConnectorParams, aws::initiate_config_with_credentials},
    http::v1::iceberg::namespace::Namespace as HttpNamespace,
};
use async_trait::async_trait;
use aws_sdk_credential_bridge::S3CredentialProvider;
use data_components::{
    RefreshableCatalogProvider,
    iceberg::{
        catalog::{hadoop::HadoopCatalogBuilder, rest::RestCatalog},
        provider::{CatalogTableWrapper, IcebergCatalogProvider},
    },
};
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;

use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;

/// Builds the hook that makes catalog-sourced Iceberg scans serializable for
/// distributed (Ballista) execution.
///
/// Each loaded table provider is wrapped in an [`IcebergClusterTableProvider`]
/// keyed by its fully-qualified `catalog.schema.table` reference — mirroring the
/// single-dataset Iceberg data connector, which wraps every dataset the same
/// way. All three parts are qualified because a remote executor resolves the
/// recipe's reference through `get_table_sync`, which needs the catalog and
/// schema to locate this provider. In a single-node session the wrapper is a
/// transparent pass-through, so non-distributed catalogs are unaffected.
fn cluster_table_wrapper(catalog_name: &str) -> CatalogTableWrapper {
    let catalog_name = catalog_name.to_string();
    Arc::new(move |schema: &str, table: &str, provider| {
        let table_ref =
            TableReference::full(catalog_name.clone(), schema.to_string(), table.to_string());
        Arc::new(IcebergClusterTableProvider::new(table_ref, provider)).into_table()
            as Arc<dyn TableProvider>
    })
}
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::{
    CatalogBuilder, Namespace, NamespaceIdent,
    io::{
        ADLS_AUTHORITY_HOST, FileRead, FileWrite, GCS_SERVICE_HOST, HF_ENDPOINT, OSS_ENDPOINT,
        S3_ENDPOINT, Storage, StorageConfig, StorageFactory,
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, RestCatalog as IcebergRestCatalog, RestCatalogBuilder,
};
use iceberg_storage_opendal::{CustomAwsCredentialLoader, OpenDalStorageFactory};
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use opendal::Operator;
use runtime_datafusion::url_table;

#[path = "vended_redirect.rs"]
mod vended_redirect;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::fmt::Write;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use std::{any::Any, collections::HashMap, sync::Arc};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid URL scheme '{}'. Must be 'http', 'https', 'file', 's3', or 's3a'.",
        scheme
    ))]
    InvalidScheme { scheme: String },

    #[snafu(display("URL is missing a host"))]
    MissingHost,

    #[snafu(display("Path must contain 'v1' segment"))]
    MissingV1Segment,

    #[snafu(display("Path must contain 'namespaces' segment"))]
    MissingNamespacesSegment,

    #[snafu(display("The 'namespaces' segment must come after 'v1'"))]
    InvalidSegmentOrder,

    #[snafu(display("Missing namespace name after 'namespaces'"))]
    MissingNamespace,

    #[snafu(display("Failed to parse URL: {}", source))]
    UrlParse { source: url::ParseError },

    #[snafu(display(
        "Failed to connect to the S3 endpoint at '{url}'. Verify the S3 endpoint is accessible and try again."
    ))]
    FailedToConnectS3Endpoint { url: String },

    #[snafu(display("Path must contain 'tables' segment followed by a table name"))]
    MissingTableSegment,

    #[snafu(display("Unexpected table segment in catalog path: {segment}"))]
    UnexpectedTableSegment { segment: String },

    #[snafu(display("Failed to build catalog: {source}"))]
    #[snafu(visibility(pub(crate)))]
    UnableToBuildCatalog { source: iceberg::Error },

    #[snafu(display("Failed to build catalog client: {source}"))]
    #[snafu(visibility(pub(crate)))]
    UnableToBuildCatalogClient { source: reqwest::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct IcebergCatalog {
    params: Parameters,
}

impl IcebergCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    async fn load_hadoop_catalog(
        props: HashMap<String, String>,
        s3_credential_loader: Option<CustomAwsCredentialLoader>,
        catalog: &Catalog,
        catalog_id: &str,
        table_wrapper: Option<CatalogTableWrapper>,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let operator = build_opendal_operator(catalog_id, &props).map_err(|e| {
            super::Error::InvalidConfiguration {
                connector: "iceberg".into(),
                message: format!("Failed to build opendal operator for Hadoop Catalog: {e}"),
                connector_component: ConnectorComponent::from(catalog),
                source: e,
            }
        })?;

        // Not much we can check with this path for Hadoop, because a namespace could be an empty folder, there could be no namespaces, etc.
        let mut catalog_builder = HadoopCatalogBuilder::default()
            .with_warehouse_root(catalog_id)
            .with_operator(operator)
            .with_properties(props);

        if catalog_id.starts_with("gs://") || catalog_id.starts_with("gcs://") {
            catalog_builder =
                catalog_builder.with_storage_factory(Arc::new(OpenDalStorageFactory::Gcs));
        } else if catalog_id.starts_with("s3://") || catalog_id.starts_with("s3a://") {
            catalog_builder = catalog_builder.with_storage_factory_builder(move |_scheme| {
                Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: s3_credential_loader.clone(),
                })
            });
        } else {
            catalog_builder =
                catalog_builder.with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory));
        }

        let hadoop_catalog =
            catalog_builder
                .build()
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: format!(
                        "Failed to create Hadoop Catalog for Iceberg with base URI: {catalog_id}",
                    ),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?;

        let catalog_provider = IcebergCatalogProvider::try_new(
            Arc::new(hadoop_catalog),
            None,
            &table_selector(catalog),
            table_wrapper,
        )
        .await
        .map_err(|e| super::Error::UnableToGetCatalogProvider {
            connector: "iceberg".into(),
            connector_component: ConnectorComponent::from(catalog),
            source: Box::new(e),
        })?;

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
}

pub const ICEBERG_PARAM_LEN: usize = 24;
pub const PARAMETERS: [ParameterSpec; ICEBERG_PARAM_LEN] = [
    ParameterSpec::component("token")
        .secret()
        .description("Bearer token value to use for Authorization header."),
    ParameterSpec::component("oauth2_credential")
        .secret()
        .description(
            "Credential to use for OAuth2 client credential flow when initializing the catalog. Separated by a colon as <client_id>:<client_secret>.",
        ),
    ParameterSpec::component("oauth2_token_url")
        .description("The URL to use for OAuth2 token endpoint."),
    ParameterSpec::component("oauth2_scope")
        .description(
            "The scope to use for OAuth2 token endpoint (default: catalog).",
        )
        .default("catalog"),
    ParameterSpec::component("oauth2_server_url")
        .description("URL of the OAuth2 server tokens endpoint."),

    // Catalog AWS Glue options
    ParameterSpec::component("sigv4_enabled")
        .description("Enable SigV4 authentication for the catalog (for connecting to AWS Glue)."),
    ParameterSpec::component("signing_region")
        .description("The region to use when signing the request for SigV4. Defaults to the region in the catalog URL if available."),
    ParameterSpec::component("signing_name")
        .description("The name to use when signing the request for SigV4.")
        .default("glue"),

    // Glue like catalog options. Eg: Lakekeeper
    ParameterSpec::component("warehouse")
        .description("Name of the Iceberg warehouse."),

    // S3 storage options
    ParameterSpec::component("s3_endpoint")
        .description(
            "Configure an alternative endpoint for the S3 service. This can be any s3-compatible object storage service. i.e. Minio, Cloudflare R2, etc.",
        )
        .secret(),
    ParameterSpec::component("s3_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::component("s3_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::component("s3_session_token")
        .description("Configure the static session token used for S3 storage.")
        .secret(),
    ParameterSpec::component("s3_iam_role_source")
        .description("IAM role credential source. 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.")
        .one_of(&["auto", "metadata", "env"]),
    ParameterSpec::component("s3_region")
        .description("The AWS S3 region to use.")
        .secret(),
    ParameterSpec::component("s3_role_session_name")
        .description("An optional identifier for the assumed role session for auditing purposes.")
        .secret(),
    ParameterSpec::component("s3_role_arn")
        .description("The Amazon Resource Name (ARN) of the role to assume. If provided instead of s3_access_key_id and s3_secret_access_key, temporary credentials will be fetched by assuming this role")
        .secret(),
    ParameterSpec::component("s3_connect_timeout")
        .description("Configure socket connection timeout, in seconds (default: 60)."),
    ParameterSpec::component("s3_path_style_access")
        .description("Controls S3 addressing style. Defaults to 'true' (path-style: endpoint/bucket), which is required for object stores such as MinIO. Set to 'false' to use virtual-hosted-style (bucket.endpoint).")
        .default("true"),

    // GCS storage options
    ParameterSpec::component("gcs_project_id")
        .description("The Google Cloud project ID for GCS storage."),
    ParameterSpec::component("gcs_credentials")
        .description("Base64-encoded Google Cloud service account credentials JSON for GCS storage.")
        .secret(),
    ParameterSpec::component("gcs_token")
        .description("OAuth2 token to use for GCS authentication.")
        .secret(),
    ParameterSpec::component("gcs_service_path")
        .description("Custom endpoint URL for GCS (for emulators or custom endpoints)."),
    ParameterSpec::component("gcs_no_auth")
        .description("Set to 'true' to allow anonymous access to GCS (for public buckets)."),
];

/// Maps a Spice parameter name to an Iceberg property name.
pub(crate) fn map_param_name_to_iceberg_prop(param_name: &str) -> Option<Vec<String>> {
    match param_name {
        "token" => Some(vec!["token".to_string()]),
        "oauth2_credential" => Some(vec!["credential".to_string()]),
        "oauth2_server_url" => Some(vec!["oauth2-server-uri".to_string()]),
        "oauth2_scope" => Some(vec!["scope".to_string()]),
        "s3_endpoint" => Some(vec!["s3.endpoint".to_string()]),
        "s3_access_key_id" => Some(vec![
            "s3.access-key-id".to_string(),
            "rest.access-key-id".to_string(),
        ]),
        "s3_secret_access_key" => Some(vec![
            "s3.secret-access-key".to_string(),
            "rest.secret-access-key".to_string(),
        ]),
        "s3_session_token" => Some(vec![
            "s3.session-token".to_string(),
            "rest.session-token".to_string(),
        ]),
        "s3_region" => Some(vec!["s3.region".to_string()]),
        "s3_path_style_access" => Some(vec!["s3.path-style-access".to_string()]),
        "s3_role_session_name" => Some(vec![
            "client.assume-role.session-name".to_string(),
            "rest.client.assume-role.session-name".to_string(),
        ]),
        "s3_role_arn" => Some(vec![
            "client.assume-role.arn".to_string(),
            "rest.client.assume-role.arn".to_string(),
        ]),
        "warehouse" => Some(vec!["warehouse".to_string()]),
        "sigv4_enabled" => Some(vec!["rest.sigv4-enabled".to_string()]),
        "signing_region" => Some(vec!["rest.signing-region".to_string()]),
        "signing_name" => Some(vec!["rest.signing-name".to_string()]),
        // GCS storage options
        "gcs_project_id" => Some(vec!["gcs.project-id".to_string()]),
        "gcs_credentials" => Some(vec!["gcs.credentials-json".to_string()]),
        "gcs_token" => Some(vec!["gcs.oauth2.token".to_string()]),
        "gcs_service_path" => Some(vec!["gcs.service.path".to_string()]),
        "gcs_no_auth" => Some(vec!["gcs.no-auth".to_string()]),
        _ => None,
    }
}

#[async_trait]
impl CatalogConnector for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "iceberg".into(),
                    message: "A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>. For details, visit: https://spiceai.org/docs/components/catalogs/iceberg#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        // Wrap every catalog table so its scans serialize for distributed
        // execution, mirroring the single-dataset Iceberg connector.
        let table_wrapper = Some(cluster_table_wrapper(&catalog.name));

        let mut props = HashMap::new();
        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        let s3_credential_loader: Option<CustomAwsCredentialLoader> =
            if let Some(endpoint) = props.get("s3.endpoint") {
                verify_s3_endpoint(endpoint).await.map_err(|e| {
                    super::Error::InvalidConfiguration {
                        connector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(catalog),
                        source: Box::new(e),
                    }
                })?;

                let aws_sdk_config = initiate_config_with_credentials(
                    "IcebergCatalogConnector",
                    "s3_region",
                    "s3_access_key_id",
                    "s3_secret_access_key",
                    "s3_session_token",
                    &self.params,
                    self.params.get("s3_iam_role_source").expose().ok(),
                )
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?
                .load()
                .await;

                let custom_loader = S3CredentialProvider::from_config(&aws_sdk_config)
                    .map_err(|e| super::Error::InvalidConfiguration {
                        connector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(catalog),
                        source: Box::new(e),
                    })?
                    .into_custom_loader();

                Some(custom_loader)
            } else {
                None
            };

        if is_storage_url(&catalog_id) {
            return IcebergCatalog::load_hadoop_catalog(
                props,
                s3_credential_loader,
                catalog,
                &catalog_id,
                table_wrapper,
            )
            .await;
        }

        let storage_factory: Option<Arc<dyn StorageFactory>> =
            s3_credential_loader.map(|custom_loader| {
                Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: Some(custom_loader),
                }) as Arc<dyn StorageFactory>
            });

        let (base_uri, new_props, namespace) = match parse_catalog_url(catalog_id.as_str()) {
            Ok(result) => result,
            Err(e) => {
                return Err(super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: format!(
                        "A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>. For details, visit: https://spiceai.org/docs/components/catalogs/iceberg#from {e}"
                    ),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                });
            }
        };

        props.extend(new_props);
        let catalog_config = get_rest_catalog(
            base_uri,
            props,
            storage_factory.clone(),
            format!("catalog '{}'", catalog.name),
        )
        .await?;
        let catalog_client = RestCatalog::new(catalog_config);

        let catalog_provider = IcebergCatalogProvider::try_new(
            Arc::new(catalog_client),
            namespace.map(|n| n.name().clone()),
            &table_selector(catalog),
            table_wrapper,
        )
        .await
        .map_err(|e| super::Error::UnableToGetCatalogProvider {
            connector: "iceberg".into(),
            connector_component: ConnectorComponent::from(catalog),
            source: Box::new(e),
        })?;

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
}

pub(crate) async fn verify_s3_endpoint(endpoint: &str) -> Result<()> {
    let url = Url::parse(endpoint).context(UrlParseSnafu)?;
    let host = url.host_str().context(MissingHostSnafu)?;
    let port = url.port().unwrap_or_else(|| {
        if url.scheme() == "http" {
            80
        } else if url.scheme() == "https" {
            443
        } else {
            0
        }
    });

    verify_ns_lookup_and_tcp_connect(host, port)
        .await
        .map_err(|_| Error::FailedToConnectS3Endpoint {
            url: endpoint.to_string(),
        })?;
    Ok(())
}

/// Parses a catalog URL into an Iceberg `RestCatalogConfig` (catalog URI + optional properties)
/// and the `Namespace` (namespace name + optional properties).
///
/// For example:
///
/// `https://my.iceberg.com/v1/namespaces/spiceai_sandbox`
///
/// Returns:
/// ```rust
/// (
///   "https://my.iceberg.com",
///   {},
///   Namespace { name: "spiceai_sandbox", properties: {} }
/// )
/// ```
///
/// Example with prefix:
///
/// `https://my.iceberg.com/v1/my_prefix/namespaces/spiceai_sandbox`
///
/// Returns:
/// ```rust
/// (
///   "https://my.iceberg.com",
///   {"prefix": "my_prefix"},
///   Namespace { name: "spiceai_sandbox", properties: {} }
/// )
/// ```
pub fn parse_catalog_url(
    url: &str,
) -> Result<(String, HashMap<String, String>, Option<Namespace>)> {
    let (base_uri, props, path_info) = parse_iceberg_url(url)?;

    match path_info {
        IcebergPathInfo::RootNamespace => Ok((base_uri, props, None)),
        IcebergPathInfo::Namespace(namespace) => Ok((base_uri, props, Some(namespace))),
        IcebergPathInfo::Table(_namespace, table_name) => Err(Error::UnexpectedTableSegment {
            segment: format!("/tables/{table_name}"),
        }),
    }
}

/// Represents the path information extracted from an Iceberg URL
#[derive(Debug, Clone)]
enum IcebergPathInfo {
    /// The root namespace (e.g., `/v1/namespaces`)
    RootNamespace,
    /// A namespace path (e.g., `/v1/namespaces/my_namespace`)
    Namespace(Namespace),
    /// A table path (e.g., `/v1/namespaces/my_namespace/tables/my_table`)
    Table(Namespace, String),
}

/// Parses an Iceberg URL into base URI, properties, and path information.
/// This function handles both namespace and table paths.
///
/// For example:
/// - `https://my.iceberg.com/v1/namespaces/spiceai_sandbox` (namespace path)
/// - `https://my.iceberg.com/v1/namespaces/spiceai_sandbox/tables/my_table` (table path)
fn parse_iceberg_url(url: &str) -> Result<(String, HashMap<String, String>, IcebergPathInfo)> {
    // Parse the URL
    let parsed = Url::parse(url).context(UrlParseSnafu)?;

    // Validate scheme
    match parsed.scheme() {
        "http" | "https" => {} // OK
        other => {
            return InvalidSchemeSnafu {
                scheme: other.to_string(),
            }
            .fail();
        }
    }

    // Build the base URI (scheme://host[:port])
    let host = parsed.host_str().context(MissingHostSnafu)?;

    let port_part = match parsed.port() {
        Some(port) => format!(":{port}"),
        None => String::new(),
    };
    let mut base_uri = format!("{}://{}{}", parsed.scheme(), host, port_part);

    // Extract path segments
    let segments: Vec<_> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();

    // Find the "v1" segment
    let v1_idx = segments
        .iter()
        .position(|seg| *seg == "v1")
        .context(MissingV1SegmentSnafu)?;

    // Add any path segments before v1 to the base URI
    if v1_idx > 0 {
        let prefix_path = segments[..v1_idx].join("/");
        let _ = write!(base_uri, "/{prefix_path}");
    }

    // Find the "namespaces" segment
    let namespaces_idx = segments
        .iter()
        .position(|seg| *seg == "namespaces")
        .context(MissingNamespacesSegmentSnafu)?;

    if namespaces_idx <= v1_idx {
        return InvalidSegmentOrderSnafu.fail();
    }

    // Everything between "v1" and "namespaces" is considered the prefix
    let prefix_segments = &segments[v1_idx + 1..namespaces_idx];
    let prefix = prefix_segments.join("/");

    // Build up the catalog properties
    let mut props = HashMap::new();
    if !prefix.is_empty() {
        props.insert("prefix".to_string(), prefix);
    }

    if let Some(warehouse) = get_warehouse(&parsed) {
        props.insert("warehouse".to_string(), warehouse);
    }

    // Auto-detect AWS Glue URLs and set signing region, name, and SigV4 enabled
    if let Some(host_str) = parsed.host_str()
        && host_str.starts_with("glue.")
        && host_str.ends_with(".amazonaws.com")
        && let Some(region) = host_str
            .strip_prefix("glue.")
            .and_then(|s| s.strip_suffix(".amazonaws.com"))
    {
        props.insert("rest.signing-region".to_string(), region.to_string());
        props.insert("rest.signing-name".to_string(), "glue".to_string());
        props.insert("rest.sigv4-enabled".to_string(), "true".to_string());
    }

    // The namespace name is the segment immediately after "namespaces"
    if namespaces_idx + 1 >= segments.len() {
        // This is the root namespace case (e.g., /v1/namespaces)
        return Ok((base_uri, props, IcebergPathInfo::RootNamespace));
    }

    let namespace_name = HttpNamespace::from_encoded(segments[namespaces_idx + 1]);
    let namespace_name =
        NamespaceIdent::from_vec(namespace_name.parts).map_err(|_| Error::MissingNamespace)?;
    let namespace = Namespace::new(namespace_name);

    // Check if this is a table path
    let path_info =
        if namespaces_idx + 3 < segments.len() && segments[namespaces_idx + 2] == "tables" {
            // This is a table path
            let table_name = segments[namespaces_idx + 3].to_string();
            IcebergPathInfo::Table(namespace, table_name)
        } else {
            // This is a namespace path
            IcebergPathInfo::Namespace(namespace)
        };

    // Return the Base URI + Properties + Path Info
    Ok((base_uri, props, path_info))
}

/// Parses a table URL into an Iceberg `RestCatalogConfig` (catalog URI + optional properties),
/// the `Namespace`, and the table name.
///
/// For example:
///
/// `https://my.iceberg.com/v1/namespaces/spiceai_sandbox/tables/my_table`
///
/// Returns:
/// ```rust
/// (
///   "https://my.iceberg.com",
///   {},
///   Namespace { name: "spiceai_sandbox", properties: {} },
///   "my_table"
/// )
/// ```
pub fn parse_table_url(url: &str) -> Result<(String, HashMap<String, String>, Namespace, String)> {
    let (base_uri, props, path_info) = parse_iceberg_url(url)?;

    match path_info {
        IcebergPathInfo::Table(namespace, table_name) => {
            Ok((base_uri, props, namespace, table_name))
        }
        IcebergPathInfo::Namespace(_) | IcebergPathInfo::RootNamespace => {
            Err(Error::MissingTableSegment)
        }
    }
}

/// Infers a default `StorageFactory` from the iceberg properties.
///
/// If any `gcs.*` property is present, returns a GCS factory; otherwise defaults to S3.
fn default_storage_factory_from_props(props: &HashMap<String, String>) -> Arc<dyn StorageFactory> {
    if props.keys().any(|k| k.starts_with("gcs.")) {
        Arc::new(OpenDalStorageFactory::Gcs)
    } else {
        Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        })
    }
}

/// Storage properties, as read by the Iceberg storage backends, whose value
/// selects the host that storage requests and their credentials are sent to.
const STORAGE_ENDPOINT_PROPS: [&str; 5] = [
    S3_ENDPOINT,
    GCS_SERVICE_HOST,
    ADLS_AUTHORITY_HOST,
    OSS_ENDPOINT,
    HF_ENDPOINT,
];

/// Region properties the Iceberg S3 parser copies onto the storage client.
/// `client.region` is applied after `s3.region` and wins when both are set.
const S3_REGION_PROPS: [&str; 2] = ["client.region", "s3.region"];

/// The only endpoint OpenDAL 0.58.2 rewrites with the region.
const S3_GLOBAL_ENDPOINT: &str = "https://s3.amazonaws.com";

/// A `StorageFactory` that refuses storage endpoints a REST catalog supplies in
/// its responses when they target a private, loopback, or link-local host
/// (including a hostname that resolves to one, an `s3.endpoint` that the S3
/// client rewrites onto such a host using the location's bucket, and an
/// `s3.region` or `client.region` that the client substitutes into the global
/// AWS endpoint), and refuses `file://` table and metadata locations the
/// catalog supplies.
///
/// A REST catalog's `LoadTableResult.config` (and `/v1/config` defaults and
/// overrides) is merged into the table's `FileIO` properties, and supplies any
/// key the Spicepod does not set. Endpoint properties the operator configured
/// are trusted as-is; any other endpoint value reaching [`StorageFactory::build`]
/// came from the catalog. The catalog also chooses every location the `FileIO`
/// reads (metadata, manifests, data files). A `file://` location reads the local
/// filesystem, so it is refused unless it is inside the `warehouse` the operator
/// configured. A redirect from a catalog-vended endpoint is checked again before
/// it is followed, and refused when the `Location` is internal. Vended
/// credentials are passed through unchanged.
#[derive(Debug, Serialize, Deserialize)]
struct VendedEndpointGuard {
    inner: Arc<dyn StorageFactory>,
    operator_endpoints: HashMap<String, String>,
    /// Operator `client.region` and `s3.region`, when set. A catalog value for
    /// the key the S3 backend actually uses is checked; the operator's own
    /// value is not.
    #[serde(default)]
    operator_regions: HashMap<String, String>,
    /// Operator-configured `warehouse`, when it is a `file://` URL.
    operator_file_root: Option<String>,
    component: String,
}

impl VendedEndpointGuard {
    fn new(
        inner: Arc<dyn StorageFactory>,
        operator_props: &HashMap<String, String>,
        component: String,
    ) -> Self {
        let operator_endpoints = STORAGE_ENDPOINT_PROPS
            .iter()
            .filter_map(|key| {
                operator_props
                    .get(*key)
                    .map(|value| ((*key).to_string(), value.clone()))
            })
            .collect();
        let operator_regions = S3_REGION_PROPS
            .iter()
            .filter_map(|key| {
                operator_props
                    .get(*key)
                    .map(|value| ((*key).to_string(), value.clone()))
            })
            .collect();
        let operator_file_root = operator_props.get("warehouse").and_then(|warehouse| {
            Url::parse(warehouse)
                .ok()
                .filter(|url| url.scheme() == "file")
                .map(|_| warehouse.clone())
        });
        Self {
            inner,
            operator_endpoints,
            operator_regions,
            operator_file_root,
            component,
        }
    }
}

#[typetag::serde(name = "SpiceVendedEndpointGuard")]
impl StorageFactory for VendedEndpointGuard {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        let allow_private_hosts = url_table::allow_private_hosts();
        check_vended_endpoints(
            config.props(),
            &self.operator_endpoints,
            &self.component,
            allow_private_hosts,
        )?;
        // Literal addresses and blocked names are refused above. The host the
        // storage client actually dials can still differ: OpenDAL's S3 builder
        // rewrites the endpoint with the bucket from the location, and a domain
        // still has to be resolved. Both happen on the async path, where the
        // location is known, before the inner client is used.
        let vended_endpoints = vended_endpoints(
            config.props(),
            &self.operator_endpoints,
            allow_private_hosts,
        );
        // A catalog region is substituted into the global AWS endpoint even
        // when `s3.endpoint` is absent. That host is classified on the async
        // path, once the location's bucket is known. An explicit catalog
        // endpoint is already in `vended_endpoints`.
        let vended_s3_region = if allow_private_hosts {
            None
        } else {
            catalog_controlled_s3_region(
                config.props(),
                &self.operator_regions,
                &self.operator_endpoints,
            )
        };
        let inner = self.inner.build(config)?;
        Ok(Arc::new(GuardedStorage {
            inner,
            operator_file_root: self.operator_file_root.clone(),
            component: self.component.clone(),
            vended_endpoints,
            vended_s3_region,
            s3_virtual_host_style: s3_virtual_host_style(config.props()),
            s3_region: s3_region(config.props()),
            effective_decisions: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }))
    }
}

/// `Storage` that refuses `file://` locations a catalog vended.
///
/// `InputFile` keeps the `Storage` it was built with and calls it on read, so
/// the check has to live here rather than only in [`Storage::new_input`].
#[derive(Clone, Debug, Serialize, Deserialize)]
struct GuardedStorage {
    inner: Arc<dyn Storage>,
    operator_file_root: Option<String>,
    component: String,
    /// Endpoint values the catalog supplied. The operator's own values are
    /// absent. Checked again on each storage call against the host the client
    /// will dial, which can differ from `value` after the S3 bucket rewrite.
    vended_endpoints: Vec<VendedEndpoint>,
    /// Catalog `client.region` or `s3.region` that the S3 backend substitutes
    /// into the global AWS endpoint. Absent when the region is the operator's,
    /// comes from the process environment, or an explicit catalog endpoint is
    /// already classified.
    #[serde(default)]
    vended_s3_region: Option<VendedS3Region>,
    /// `s3.path-style-access` is truthy → false. Matches the Iceberg S3
    /// backend, whose default is virtual-host style.
    s3_virtual_host_style: bool,
    /// Region the S3 backend will sign with: `client.region`, then `s3.region`,
    /// then `AWS_REGION`, then `AWS_DEFAULT_REGION`.
    s3_region: String,
    /// Refusal for an effective endpoint, keyed by that endpoint. `Some` is
    /// the error text; `None` means the effective host was public.
    #[serde(skip, default = "empty_effective_decisions")]
    effective_decisions: Arc<parking_lot::Mutex<HashMap<String, Option<String>>>>,
}

fn empty_effective_decisions() -> Arc<parking_lot::Mutex<HashMap<String, Option<String>>>> {
    Arc::new(parking_lot::Mutex::new(HashMap::new()))
}

/// A catalog-supplied endpoint that passed the synchronous literal check.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VendedEndpoint {
    key: String,
    value: String,
}

/// A catalog region the S3 backend inserts into the global AWS endpoint.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VendedS3Region {
    /// `client.region` or `s3.region`.
    key: String,
    value: String,
    /// Endpoint string the backend substitutes `value` into. It normalizes to
    /// `https://s3.amazonaws.com`.
    endpoint: String,
}

impl GuardedStorage {
    fn refuse_local(&self, location: &str) -> iceberg::Result<()> {
        check_vended_file_location(
            location,
            self.operator_file_root.as_deref(),
            &self.component,
            url_table::allow_private_hosts(),
        )
    }

    async fn before_io(&self, location: &str) -> iceberg::Result<()> {
        self.refuse_local(location)?;
        self.refuse_effective_endpoints(location).await
    }

    /// Catalog component when a redirect from this storage must be refused.
    ///
    /// Absent when every endpoint is the operator's, and when
    /// `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS` is set. Read on each call so the
    /// override applies without rebuilding the catalog.
    fn redirect_component(&self) -> Option<String> {
        if url_table::allow_private_hosts()
            || (self.vended_endpoints.is_empty() && self.vended_s3_region.is_none())
        {
            None
        } else {
            Some(self.component.clone())
        }
    }

    /// Refuses a catalog endpoint whose effective host is internal.
    ///
    /// For `s3.endpoint`, the effective host is the one OpenDAL's S3 builder
    /// dials after it rewrites the endpoint with the location's bucket. Other
    /// endpoints are classified as configured. A domain is resolved here; the
    /// storage client resolves it again when it connects, so this does not pin
    /// the socket to the address checked here.
    async fn refuse_effective_endpoints(&self, location: &str) -> iceberg::Result<()> {
        if url_table::allow_private_hosts()
            || (self.vended_endpoints.is_empty() && self.vended_s3_region.is_none())
        {
            return Ok(());
        }
        for endpoint in &self.vended_endpoints {
            let effective = effective_storage_endpoint(
                &endpoint.key,
                &endpoint.value,
                location,
                self.s3_virtual_host_style,
                &self.s3_region,
            );
            let cache_key = format!("{}|{effective}", endpoint.key);
            let cached = {
                let decisions = self.effective_decisions.lock();
                decisions.get(&cache_key).cloned()
            };
            let decision = if let Some(decision) = cached {
                decision
            } else {
                let decision = refusal_for_effective_endpoint(
                    &self.component,
                    &endpoint.key,
                    &endpoint.value,
                    &effective,
                )
                .await;
                self.effective_decisions
                    .lock()
                    .insert(cache_key, decision.clone());
                decision
            };
            if let Some(message) = decision {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    message,
                ));
            }
        }
        self.refuse_vended_s3_region(location).await
    }

    /// Refuses a catalog region that the S3 client substitutes into the global
    /// AWS endpoint, producing an internal host.
    ///
    /// OpenDAL uses `https://s3.amazonaws.com` when `s3.endpoint` is unset, then
    /// replaces that with `https://s3.{region}.amazonaws.com`. A region of
    /// `x@127.0.0.1:port/` makes the URL authority `s3.x@127.0.0.1:port`, so
    /// the client dials loopback. The operator's region, and a region that
    /// only comes from `AWS_REGION` or `AWS_DEFAULT_REGION`, is not checked
    /// here.
    async fn refuse_vended_s3_region(&self, location: &str) -> iceberg::Result<()> {
        let Some(region) = &self.vended_s3_region else {
            return Ok(());
        };
        let Some(bucket) = s3_location_bucket(location) else {
            return Ok(());
        };
        // Bucket removal can turn `https://{bucket}.s3.amazonaws.com` into the
        // global template. Until that happens the region does not change the
        // host, and an operator endpoint such as a private MinIO URL stays
        // trusted.
        if !s3_region_is_substituted(&region.endpoint, &bucket) {
            return Ok(());
        }
        let effective = s3_effective_endpoint(
            &region.endpoint,
            &bucket,
            self.s3_virtual_host_style,
            &region.value,
        );
        let cache_key = format!("{}|{effective}", region.key);
        let cached = {
            let decisions = self.effective_decisions.lock();
            decisions.get(&cache_key).cloned()
        };
        let decision = if let Some(decision) = cached {
            decision
        } else {
            let decision = refusal_for_vended_s3_region(
                &self.component,
                &region.key,
                &region.value,
                &effective,
            )
            .await;
            self.effective_decisions
                .lock()
                .insert(cache_key, decision.clone());
            decision
        };
        if let Some(message) = decision {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                message,
            ));
        }
        Ok(())
    }
}

#[typetag::serde(name = "SpiceGuardedStorage")]
#[async_trait]
impl Storage for GuardedStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.exists(path)).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<iceberg::io::FileMetadata> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.metadata(path)).await
    }

    async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.read(path)).await
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        self.before_io(path).await?;
        let component = self.redirect_component();
        let inner = vended_redirect::scope(component.clone(), self.inner.reader(path)).await?;
        Ok(Box::new(vended_redirect::ScopedFileRead {
            inner,
            component,
        }))
    }

    async fn write(&self, path: &str, bs: bytes::Bytes) -> iceberg::Result<()> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.write(path, bs)).await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        self.before_io(path).await?;
        let component = self.redirect_component();
        let inner = vended_redirect::scope(component.clone(), self.inner.writer(path)).await?;
        Ok(Box::new(vended_redirect::ScopedFileWrite {
            inner,
            component,
        }))
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.delete(path)).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        self.before_io(path).await?;
        vended_redirect::scope(self.redirect_component(), self.inner.delete_prefix(path)).await
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        let mut collected = Vec::new();
        while let Some(path) = paths.next().await {
            self.before_io(&path).await?;
            collected.push(path);
        }
        vended_redirect::scope(
            self.redirect_component(),
            self.inner
                .delete_stream(futures::stream::iter(collected).boxed()),
        )
        .await
    }

    fn new_input(&self, path: &str) -> iceberg::Result<iceberg::io::InputFile> {
        self.refuse_local(path)?;
        Ok(iceberg::io::InputFile::new(
            Arc::new(self.clone()),
            path.to_string(),
        ))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<iceberg::io::OutputFile> {
        self.refuse_local(path)?;
        Ok(iceberg::io::OutputFile::new(
            Arc::new(self.clone()),
            path.to_string(),
        ))
    }
}

/// Returns an error for the first storage endpoint in `props` that was not set
/// by the operator and targets a private, loopback, or link-local host.
fn check_vended_endpoints(
    props: &HashMap<String, String>,
    operator_endpoints: &HashMap<String, String>,
    component: &str,
    allow_private_hosts: bool,
) -> iceberg::Result<()> {
    if allow_private_hosts {
        return Ok(());
    }
    for key in STORAGE_ENDPOINT_PROPS {
        let Some(value) = props.get(key) else {
            continue;
        };
        if operator_endpoints.get(key) == Some(value) {
            continue;
        }
        if endpoint_targets_internal_host(value) {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                vended_endpoint_refused_message(component, key, value),
            ));
        }
    }
    Ok(())
}

/// How long a vended-endpoint lookup may block the caller. The lookup itself
/// runs on the blocking pool. This matches the bound the URL-table guard uses.
const VENDED_ENDPOINT_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Test stand-in for [`resolve_host_blocking`]. Production builds always use
/// the system resolver.
#[cfg(test)]
static VENDED_ENDPOINT_RESOLVER_OVERRIDE: parking_lot::Mutex<
    Option<HashMap<String, Result<Vec<IpAddr>, String>>>,
> = parking_lot::Mutex::new(None);

#[cfg(test)]
fn set_vended_endpoint_resolver_override(host: &str, result: Result<Vec<IpAddr>, String>) {
    VENDED_ENDPOINT_RESOLVER_OVERRIDE
        .lock()
        .get_or_insert_with(HashMap::new)
        .insert(host.to_ascii_lowercase(), result);
}

#[cfg(test)]
fn clear_vended_endpoint_resolver_override(host: &str) {
    let mut overrides = VENDED_ENDPOINT_RESOLVER_OVERRIDE.lock();
    if let Some(map) = overrides.as_mut() {
        map.remove(&host.to_ascii_lowercase());
    }
}

/// Whether `endpoint` (a URL, or a bare `host[:port]` that the storage backends
/// treat as `https://host[:port]`) has a literal internal IP, a well-known
/// internal hostname, or no host that can be classified.
///
/// A domain that is not a blocked name is classified later, after DNS, by
/// [`refusal_message_for_resolution`].
fn endpoint_targets_internal_host(endpoint: &str) -> bool {
    let parsed = Url::parse(endpoint)
        .ok()
        .filter(Url::has_host)
        .or_else(|| Url::parse(&format!("https://{endpoint}")).ok());
    match parsed.as_ref().and_then(Url::host) {
        Some(url::Host::Ipv4(ip)) => url_table::is_internal_ip(ip.into()),
        Some(url::Host::Ipv6(ip)) => url_table::is_internal_ip(ip.into()),
        Some(url::Host::Domain(domain)) => {
            domain.is_empty()
                || url_table::is_blocked_internal_hostname(domain.trim_end_matches('.'))
        }
        None => true,
    }
}

/// Catalog endpoint values that are not the operator's. The synchronous check
/// has already refused literal internal addresses; these still have to be
/// judged by the host the storage client will dial.
fn vended_endpoints(
    props: &HashMap<String, String>,
    operator_endpoints: &HashMap<String, String>,
    allow_private_hosts: bool,
) -> Vec<VendedEndpoint> {
    if allow_private_hosts {
        return Vec::new();
    }
    let mut endpoints = Vec::new();
    for key in STORAGE_ENDPOINT_PROPS {
        let Some(value) = props.get(key) else {
            continue;
        };
        if operator_endpoints.get(key) == Some(value) {
            continue;
        }
        endpoints.push(VendedEndpoint {
            key: key.to_string(),
            value: value.clone(),
        });
    }
    endpoints
}

/// `true` when the S3 backend will use virtual-host style. The Iceberg parser
/// defaults to virtual-host style and turns it off only for a truthy
/// `s3.path-style-access`.
fn s3_virtual_host_style(props: &HashMap<String, String>) -> bool {
    !props
        .get("s3.path-style-access")
        .is_some_and(|value| endpoint_flag_is_truthy(value))
}

/// The region the S3 backend will use.
///
/// Matches OpenDAL 0.58.2: a `client.region` or `s3.region` property wins,
/// including an empty one, and otherwise `AWS_REGION` then
/// `AWS_DEFAULT_REGION`. An empty `AWS_REGION` does not fall through.
fn s3_region(props: &HashMap<String, String>) -> String {
    let aws_region = std::env::var("AWS_REGION").ok();
    let aws_default_region = match aws_region {
        Some(_) => None,
        None => std::env::var("AWS_DEFAULT_REGION").ok(),
    };
    s3_region_with_fallback(props, aws_region.as_deref(), aws_default_region.as_deref())
}

fn s3_region_with_fallback(
    props: &HashMap<String, String>,
    aws_region: Option<&str>,
    aws_default_region: Option<&str>,
) -> String {
    if let Some((_, region)) = winning_s3_region(props) {
        return region.to_string();
    }
    aws_region.or(aws_default_region).unwrap_or("").to_string()
}

/// `client.region` when set, otherwise `s3.region`.
fn winning_s3_region(props: &HashMap<String, String>) -> Option<(&str, &str)> {
    if let Some(region) = props.get("client.region") {
        return Some(("client.region", region.as_str()));
    }
    props
        .get("s3.region")
        .map(|region| ("s3.region", region.as_str()))
}

/// The endpoint string OpenDAL starts from, before bucket and region rewrites.
///
/// A property wins. Otherwise, unless `s3.disable-config-load` is set,
/// `AWS_ENDPOINT_URL`, `AWS_ENDPOINT`, or `AWS_S3_ENDPOINT`. The default is
/// the global AWS endpoint.
fn s3_backend_endpoint(props: &HashMap<String, String>) -> String {
    let env_endpoint = if props
        .get("s3.disable-config-load")
        .is_some_and(|value| endpoint_flag_is_truthy(value))
    {
        None
    } else {
        std::env::var("AWS_ENDPOINT_URL")
            .or_else(|_| std::env::var("AWS_ENDPOINT"))
            .or_else(|_| std::env::var("AWS_S3_ENDPOINT"))
            .ok()
    };
    s3_backend_endpoint_with(props, env_endpoint.as_deref())
}

fn s3_backend_endpoint_with(props: &HashMap<String, String>, env_endpoint: Option<&str>) -> String {
    if let Some(endpoint) = props.get(S3_ENDPOINT) {
        return endpoint.clone();
    }
    if let Some(endpoint) = env_endpoint
        .map(|endpoint| endpoint.trim_end_matches('/'))
        .filter(|endpoint| !endpoint.is_empty())
    {
        return endpoint.to_string();
    }
    S3_GLOBAL_ENDPOINT.to_string()
}

/// A catalog region, and the endpoint the S3 backend starts from.
///
/// `None` when the region is the operator's or comes only from the
/// environment, or when an explicit catalog `s3.endpoint` is already
/// classified on its own. Whether the region changes the host is decided
/// later, after `//{bucket}.` is removed: an operator endpoint such as
/// `https://testbucket.s3.amazonaws.com` becomes the global template only
/// once the location's bucket is known.
fn catalog_controlled_s3_region(
    props: &HashMap<String, String>,
    operator_regions: &HashMap<String, String>,
    operator_endpoints: &HashMap<String, String>,
) -> Option<VendedS3Region> {
    if let Some(endpoint) = props.get(S3_ENDPOINT)
        && operator_endpoints.get(S3_ENDPOINT) != Some(endpoint)
    {
        return None;
    }
    let (key, value) = winning_s3_region(props)?;
    if operator_regions.get(key).map(String::as_str) == Some(value) {
        return None;
    }
    Some(VendedS3Region {
        key: key.to_string(),
        value: value.to_string(),
        endpoint: s3_backend_endpoint(props),
    })
}

/// The endpoint after OpenDAL removes `//{bucket}.` and normalizes the URL,
/// before the region template is applied.
fn s3_endpoint_after_bucket_removal(endpoint: &str, bucket: &str) -> String {
    let mut endpoint = if endpoint.starts_with("http") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    };
    endpoint = endpoint.replace(&format!("//{bucket}."), "//");
    if let Ok(url) = Url::parse(&endpoint) {
        endpoint = url.to_string().trim_end_matches('/').to_string();
    }
    endpoint
}

/// `true` when bucket removal leaves the global AWS endpoint, which is the
/// only endpoint OpenDAL rewrites with the region.
fn s3_region_is_substituted(endpoint: &str, bucket: &str) -> bool {
    s3_endpoint_after_bucket_removal(endpoint, bucket) == S3_GLOBAL_ENDPOINT
}

/// Truthy values recognized by the Iceberg S3 property parser.
fn endpoint_flag_is_truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "1" | "on"
    )
}

/// The endpoint string the storage client will use for `location`.
///
/// OpenDAL 0.58.2 `S3Builder::build_endpoint` deletes `//{bucket}.` from
/// `s3.endpoint`, then, for virtual-host style, inserts the bucket in front of
/// the host. Path style leaves the stripped host and prefixes the path with
/// the bucket. A catalog can set `s3.endpoint` to a public address such as
/// `http://8.127.0.1` and a location whose host is `8` (`s3://`, `s3a://`,
/// `s3n://`, or a custom scheme such as `minio://`). The S3 backend takes that
/// host as the bucket for any absolute URL. URL parsing then collapses the
/// stripped host `127.0.1` to `127.0.0.1`, which is loopback. Other endpoint
/// keys are not rewritten.
fn effective_storage_endpoint(
    key: &str,
    value: &str,
    location: &str,
    s3_virtual_host_style: bool,
    s3_region: &str,
) -> String {
    if key == S3_ENDPOINT
        && let Some(bucket) = s3_location_bucket(location)
    {
        return s3_effective_endpoint(value, &bucket, s3_virtual_host_style, s3_region);
    }
    value.to_string()
}

fn s3_location_bucket(location: &str) -> Option<String> {
    // `s3_config_build` uses the host of whatever absolute URL it is given.
    // The same S3 storage accepts `s3`, `s3a`, `s3n`, and custom schemes.
    let url = Url::parse(location).ok()?;
    let bucket = url.host_str()?;
    (!bucket.is_empty()).then(|| bucket.to_string())
}

/// Mirrors OpenDAL 0.58.2 `S3Builder::build_endpoint` for the parts that choose
/// the host. The global AWS endpoint template is the only rewrite that
/// substitutes the region.
fn s3_effective_endpoint(
    endpoint: &str,
    bucket: &str,
    virtual_host_style: bool,
    region: &str,
) -> String {
    let mut endpoint = s3_endpoint_after_bucket_removal(endpoint, bucket);
    if endpoint == S3_GLOBAL_ENDPOINT {
        endpoint = format!("https://s3.{region}.amazonaws.com");
    }
    if virtual_host_style {
        endpoint.replace("//", &format!("//{bucket}."))
    } else {
        endpoint.push('/');
        endpoint.push_str(bucket);
        endpoint
    }
}

/// The domain of `endpoint`, lowercased and without a trailing dot.
///
/// `None` when `endpoint` is not a URL with a domain host (a literal address,
/// or a value the synchronous check refuses as unclassifiable).
fn endpoint_domain(endpoint: &str) -> Option<String> {
    let parsed = Url::parse(endpoint)
        .ok()
        .filter(Url::has_host)
        .or_else(|| Url::parse(&format!("https://{endpoint}")).ok());
    match parsed.as_ref().and_then(Url::host) {
        Some(url::Host::Domain(domain)) => {
            let domain = domain.trim_end_matches('.').to_ascii_lowercase();
            (!domain.is_empty()).then_some(domain)
        }
        _ => None,
    }
}

fn refusal_message_for_resolution(
    component: &str,
    key: &str,
    value: &str,
    resolved: Result<Vec<IpAddr>, String>,
) -> Option<String> {
    match resolved {
        Ok(addrs) if addrs.iter().any(|ip| url_table::is_internal_ip(*ip)) => {
            Some(vended_endpoint_refused_message(component, key, value))
        }
        Ok(addrs) if !addrs.is_empty() => None,
        Ok(_) | Err(_) => Some(vended_endpoint_unresolved_message(component, key, value)),
    }
}

/// Classifies the endpoint the storage client will dial.
///
/// `configured` is the catalog value. `effective` is that value after the S3
/// bucket rewrite. When those hosts differ, the error names both.
async fn refusal_for_effective_endpoint(
    component: &str,
    key: &str,
    configured: &str,
    effective: &str,
) -> Option<String> {
    let rewritten = endpoint_host_label(configured) != endpoint_host_label(effective);
    if endpoint_targets_internal_host(effective) {
        return Some(if rewritten {
            vended_endpoint_rewritten_message(component, key, configured, effective)
        } else {
            vended_endpoint_refused_message(component, key, configured)
        });
    }
    let Some(domain) = endpoint_domain(effective) else {
        return None;
    };
    let resolved = resolve_vended_host(domain).await;
    let internal = match &resolved {
        Ok(addrs) if addrs.iter().any(|ip| url_table::is_internal_ip(*ip)) => true,
        Ok(addrs) if !addrs.is_empty() => return None,
        Ok(_) | Err(_) => false,
    };
    if !rewritten {
        return refusal_message_for_resolution(component, key, configured, resolved);
    }
    Some(if internal {
        vended_endpoint_rewritten_message(component, key, configured, effective)
    } else {
        vended_endpoint_rewritten_unresolved_message(component, key, configured, effective)
    })
}

fn endpoint_host_label(endpoint: &str) -> Option<String> {
    let parsed = Url::parse(endpoint)
        .ok()
        .filter(Url::has_host)
        .or_else(|| Url::parse(&format!("https://{endpoint}")).ok());
    parsed
        .as_ref()
        .and_then(Url::host_str)
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
}

async fn resolve_vended_host(host: String) -> Result<Vec<IpAddr>, String> {
    let lookup = tokio::task::spawn_blocking(move || resolve_host_blocking(&host));
    match tokio::time::timeout(VENDED_ENDPOINT_DNS_TIMEOUT, lookup).await {
        Err(_) => Err(format!(
            "timed out after {}s",
            VENDED_ENDPOINT_DNS_TIMEOUT.as_secs()
        )),
        Ok(Ok(result)) => result,
        Ok(Err(err)) => Err(err.to_string()),
    }
}

fn resolve_host_blocking(host: &str) -> Result<Vec<IpAddr>, String> {
    #[cfg(test)]
    {
        let overrides = VENDED_ENDPOINT_RESOLVER_OVERRIDE.lock();
        if let Some(result) = overrides
            .as_ref()
            .and_then(|map| map.get(&host.to_ascii_lowercase()))
        {
            return result.clone();
        }
    }
    (host, 0_u16)
        .to_socket_addrs()
        .map(|iter| iter.map(|addr| addr.ip()).collect())
        .map_err(|err| err.to_string())
}

/// Returns an error when `location` is a `file://` URL the catalog supplied.
///
/// A location inside the operator's `file://` warehouse is trusted. `..`
/// segments are collapsed before that comparison, so a location cannot escape
/// the warehouse by traversal. `allow_private_hosts` is the same override the
/// endpoint check uses.
fn check_vended_file_location(
    location: &str,
    operator_file_root: Option<&str>,
    component: &str,
    allow_private_hosts: bool,
) -> iceberg::Result<()> {
    if allow_private_hosts {
        return Ok(());
    }
    match local_file_path(location) {
        LocalFilePath::NotFile => Ok(()),
        LocalFilePath::OutsideLocalhost => Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            vended_file_location_refused_message(component, location),
        )),
        LocalFilePath::Path(path) => {
            let inside_operator_warehouse = operator_file_root
                .and_then(local_file_path_buf)
                .is_some_and(|root| path.starts_with(root));
            if inside_operator_warehouse {
                Ok(())
            } else {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    vended_file_location_refused_message(component, location),
                ))
            }
        }
    }
}

/// A `file://` location reduced to a local path, or a reason it is not one.
enum LocalFilePath {
    /// `location` is not a `file://` URL.
    NotFile,
    /// A `file://` URL that does not name a path on this machine.
    OutsideLocalhost,
    /// A local path with `.` and `..` collapsed, without reading the filesystem.
    Path(PathBuf),
}

fn local_file_path(location: &str) -> LocalFilePath {
    let Ok(url) = Url::parse(location) else {
        return LocalFilePath::NotFile;
    };
    if url.scheme() != "file" {
        return LocalFilePath::NotFile;
    }
    url.to_file_path()
        .map_or(LocalFilePath::OutsideLocalhost, |path| {
            LocalFilePath::Path(normalized_lexical_path(&path))
        })
}

fn local_file_path_buf(location: &str) -> Option<PathBuf> {
    match local_file_path(location) {
        LocalFilePath::Path(path) => Some(path),
        LocalFilePath::NotFile | LocalFilePath::OutsideLocalhost => None,
    }
}

fn normalized_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn vended_file_location_refused_message(component: &str, location: &str) -> String {
    format!(
        "The Iceberg catalog for {component} set a table location to {location:?}, which reads the local filesystem, so the file is not opened and the table cannot be read. If this location is trusted, set the `warehouse` parameter to the directory that contains it, or set `{}=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg",
        url_table::ALLOW_PRIVATE_HOSTS_ENV
    )
}

fn endpoint_param_hint(key: &str) -> &'static str {
    match key {
        S3_ENDPOINT => "set the `s3_endpoint` parameter to it",
        "s3.region" => "set the `s3_region` parameter to it",
        _ => "configure it in the Spicepod",
    }
}

/// Classifies the endpoint produced by substituting a catalog region into the
/// global AWS endpoint. The message always names the effective URL, because
/// the region string itself is not the host the client dials.
async fn refusal_for_vended_s3_region(
    component: &str,
    key: &str,
    region: &str,
    effective: &str,
) -> Option<String> {
    if endpoint_targets_internal_host(effective) {
        return Some(vended_endpoint_rewritten_message(
            component, key, region, effective,
        ));
    }
    let Some(domain) = endpoint_domain(effective) else {
        return None;
    };
    let resolved = resolve_vended_host(domain).await;
    let internal = match &resolved {
        Ok(addrs) if addrs.iter().any(|ip| url_table::is_internal_ip(*ip)) => true,
        Ok(addrs) if !addrs.is_empty() => return None,
        Ok(_) | Err(_) => false,
    };
    Some(if internal {
        vended_endpoint_rewritten_message(component, key, region, effective)
    } else {
        vended_endpoint_rewritten_unresolved_message(component, key, region, effective)
    })
}

fn vended_endpoint_refused_message(component: &str, key: &str, value: &str) -> String {
    let param_hint = endpoint_param_hint(key);
    format!(
        "The Iceberg catalog for {component} set `{key}` to {value:?}, a private, loopback, or link-local address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, {param_hint}, or set `{}=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg",
        url_table::ALLOW_PRIVATE_HOSTS_ENV
    )
}

fn vended_endpoint_unresolved_message(component: &str, key: &str, value: &str) -> String {
    let param_hint = endpoint_param_hint(key);
    format!(
        "The Iceberg catalog for {component} set `{key}` to {value:?}, and its host could not be resolved to a public address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, {param_hint}, or set `{}=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg",
        url_table::ALLOW_PRIVATE_HOSTS_ENV
    )
}

fn vended_endpoint_rewritten_message(
    component: &str,
    key: &str,
    value: &str,
    effective: &str,
) -> String {
    let param_hint = endpoint_param_hint(key);
    format!(
        "The Iceberg catalog for {component} set `{key}` to {value:?}. The storage client rewrites that to {effective:?}, a private, loopback, or link-local address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, {param_hint}, or set `{}=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg",
        url_table::ALLOW_PRIVATE_HOSTS_ENV
    )
}

fn vended_endpoint_rewritten_unresolved_message(
    component: &str,
    key: &str,
    value: &str,
    effective: &str,
) -> String {
    let param_hint = endpoint_param_hint(key);
    format!(
        "The Iceberg catalog for {component} set `{key}` to {value:?}. The storage client rewrites that to {effective:?}, and that host could not be resolved to a public address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, {param_hint}, or set `{}=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg",
        url_table::ALLOW_PRIVATE_HOSTS_ENV
    )
}

/// Builds an `IcebergRestCatalog` from a base URI and properties.
///
/// `component` names the dataset or catalog (e.g. `dataset 'orders'`) in errors.
pub async fn get_rest_catalog(
    base_uri: String,
    mut props: HashMap<String, String, std::hash::RandomState>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    component: String,
) -> Result<IcebergRestCatalog> {
    props.insert(REST_CATALOG_PROP_URI.to_string(), base_uri);
    let inner = storage_factory.unwrap_or_else(|| default_storage_factory_from_props(&props));
    let factory: Arc<dyn StorageFactory> =
        Arc::new(VendedEndpointGuard::new(inner, &props, component));
    RestCatalogBuilder::default()
        .with_storage_factory(factory)
        .load("rest", props)
        .await
        .context(UnableToBuildCatalogSnafu)
}

// Parse out the catalog id from the Glue URL if it exists, i.e.
// https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/211125479522/namespaces/big_datasets/tables/tpch_sf100_lineitem
// should return "211125479522"
fn get_warehouse(url: &Url) -> Option<String> {
    if let Some(host_str) = url.host_str()
        && host_str.starts_with("glue.")
        && host_str.ends_with(".amazonaws.com")
    {
        let path_segments: Vec<_> = url
            .path_segments()
            .map(Iterator::collect)
            .unwrap_or_default();

        if path_segments.len() >= 4
            && path_segments[0] == "iceberg"
            && path_segments[1] == "v1"
            && path_segments[2] == "catalogs"
            && path_segments[3].len() == 12
            && path_segments[3].chars().all(|c| c.is_ascii_digit())
        {
            return Some(path_segments[3].to_string());
        }
    }
    None
}

/// Whether `url` addresses object storage or the local filesystem, rather than an
/// Iceberg REST catalog.
pub(crate) fn is_storage_url(url: &str) -> bool {
    ["file://", "s3://", "s3a://", "gs://", "gcs://"]
        .iter()
        .any(|scheme| url.starts_with(scheme))
}

/// Builds an opendal `Operator` for directory listing operations from a warehouse URL and properties.
pub(crate) fn build_opendal_operator(
    warehouse_url: &str,
    props: &HashMap<String, String>,
) -> std::result::Result<Operator, Box<dyn std::error::Error + Send + Sync>> {
    use opendal::Configurator;

    if warehouse_url.starts_with("s3://") || warehouse_url.starts_with("s3a://") {
        let parsed = Url::parse(warehouse_url)?;
        let bucket = parsed
            .host_str()
            .ok_or("S3 URL must have a bucket (host)")?;

        let mut config = opendal::services::S3Config::default();
        config.bucket = bucket.to_string();
        config.root = Some(parsed.path().to_string());

        if let Some(endpoint) = props.get("s3.endpoint") {
            config.endpoint = Some(endpoint.clone());
        }
        config.region = Some(
            props
                .get("s3.region")
                .or_else(|| props.get("client.region"))
                .cloned()
                .unwrap_or_else(|| "us-east-1".to_string()),
        );
        if let Some(key_id) = props.get("s3.access-key-id") {
            config.access_key_id = Some(key_id.clone());
        }
        if let Some(secret) = props.get("s3.secret-access-key") {
            config.secret_access_key = Some(secret.clone());
        }
        if let Some(token) = props.get("s3.session-token") {
            config.session_token = Some(token.clone());
        }

        let builder = config.into_builder();
        Ok(Operator::new(builder)?)
    } else if warehouse_url.starts_with("gs://") || warehouse_url.starts_with("gcs://") {
        let mut config = opendal::services::GcsConfig::default();
        let parsed = Url::parse(warehouse_url)?;
        config.bucket = parsed
            .host_str()
            .ok_or("GCS URL must have a bucket (host)")?
            .to_string();
        config.root = Some(parsed.path().to_string());

        if let Some(cred) = props.get("gcs.credentials-json") {
            config.credential = Some(cred.clone());
        }

        let builder = config.into_builder();
        Ok(Operator::new(builder)?)
    } else if warehouse_url.starts_with("file://") || warehouse_url.starts_with('/') {
        let mut config = opendal::services::FsConfig::default();
        if let Ok(parsed) = Url::parse(warehouse_url) {
            config.root = Some(parsed.path().to_string());
        } else {
            // Bare path like /data/warehouse
            config.root = Some(warehouse_url.to_string());
        }
        let builder = config.into_builder();
        Ok(Operator::new(builder)?)
    } else {
        Err(format!("Unsupported scheme in warehouse URL: {warehouse_url}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_catalog_url_no_prefix() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let (base_uri, props, namespace) =
            parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(base_uri, "https://my.iceberg.com");
        assert!(props.is_empty());
        assert_eq!(
            namespace
                .clone()
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "spiceai_sandbox"
        );
        assert!(
            namespace
                .expect("Namespace is None")
                .properties()
                .is_empty()
        );
    }

    #[test]
    fn test_parse_catalog_url_with_prefix() {
        let url = "https://my.iceberg.com/v1/my_prefix/namespaces/spiceai_sandbox";
        let (base_uri, props, namespace) =
            parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(base_uri, "https://my.iceberg.com");
        assert_eq!(props.get("prefix"), Some(&"my_prefix".to_string()));
        assert_eq!(
            namespace
                .clone()
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "spiceai_sandbox"
        );
        assert!(
            namespace
                .expect("Namespace is None")
                .properties()
                .is_empty()
        );
    }

    #[test]
    fn test_invalid_scheme() {
        let url = "ftp://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let err = parse_catalog_url(url).expect_err("should error parsing url");
        assert!(
            matches!(&err, Error::InvalidScheme { scheme } if scheme == "ftp"),
            "expected InvalidScheme for 'ftp', got {err:?}"
        );
        // The connector routes file/s3/s3a/gs/gcs catalog paths to the Hadoop
        // catalog before this parser runs, so the list is the connector's.
        assert_eq!(
            err.to_string(),
            "Invalid URL scheme 'ftp'. Must be 'http', 'https', 'file', 's3', or 's3a'."
        );
    }

    #[test]
    fn test_no_host() {
        // An http(s) URL cannot have an empty host: the URL parser rejects it
        // before the host check runs.
        let err = parse_catalog_url("https://").expect_err("an empty host should not parse");
        assert!(
            matches!(&err, Error::UrlParse { source } if *source == url::ParseError::EmptyHost),
            "expected UrlParse(EmptyHost), got {err:?}"
        );
        assert_eq!(err.to_string(), "Failed to parse URL: empty host");

        // With a path and no host, the WHATWG URL parser skips the extra slash
        // and takes the first path segment as the host, so `v1` is consumed as
        // the host and the path is reported as missing it.
        let err = parse_catalog_url("https:///v1/namespaces/spiceai_sandbox")
            .expect_err("should error parsing url");
        assert!(
            matches!(err, Error::MissingV1Segment),
            "expected MissingV1Segment, got {err:?}"
        );
    }

    #[test]
    fn test_missing_namespace_segment() {
        let url = "https://my.iceberg.com/v1/";
        let err = parse_catalog_url(url).expect_err("should error parsing url");
        assert!(
            matches!(err, Error::MissingNamespacesSegment),
            "expected MissingNamespacesSegment, got {err:?}"
        );
        assert_eq!(err.to_string(), "Path must contain 'namespaces' segment");
    }

    #[test]
    fn test_empty_namespace_segment() {
        let url = "https://my.iceberg.com/v1/namespaces";
        let result = parse_catalog_url(url);
        assert!(result.expect("Failed to parse catalog URL").2.is_none());
    }

    #[test]
    fn test_path_before_v1() {
        let url = "https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/default";
        let (base_uri, props, namespace) =
            parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(
            base_uri,
            "https://glue.ap-northeast-2.amazonaws.com/iceberg"
        );
        assert_eq!(
            props.get("prefix"),
            Some(&"catalogs/123456789012".to_string())
        );
        assert_eq!(
            namespace
                .clone()
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "default"
        );
        assert!(
            namespace
                .expect("Namespace is None")
                .properties()
                .is_empty()
        );
    }

    #[test]
    fn test_aws_glue_url_sets_signing_region() {
        let url = "https://glue.ap-northeast-2.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/default";
        let (base_uri, props, namespace) =
            parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(
            base_uri,
            "https://glue.ap-northeast-2.amazonaws.com/iceberg"
        );
        assert_eq!(
            props.get("prefix"),
            Some(&"catalogs/123456789012".to_string())
        );
        assert_eq!(
            props.get("rest.signing-region"),
            Some(&"ap-northeast-2".to_string())
        );
        assert_eq!(
            namespace
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "default"
        );
    }

    #[test]
    fn test_non_aws_url_no_signing_region() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let (_, props, _) = parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert!(!props.contains_key("rest.signing-region"));
    }

    #[test]
    fn test_parse_table_url() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox/tables/my_table";
        let (base_uri, props, namespace, table_name) =
            parse_table_url(url).expect("Failed to parse table URL");
        assert_eq!(base_uri, "https://my.iceberg.com");
        assert_eq!(props.len(), 0);
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");
    }

    #[test]
    fn test_parse_table_url_with_prefix() {
        let url = "https://my.iceberg.com/v1/my_prefix/namespaces/spiceai_sandbox/tables/my_table";
        let (base_uri, props, namespace, table_name) =
            parse_table_url(url).expect("Failed to parse table URL");
        assert_eq!(base_uri, "https://my.iceberg.com");
        assert_eq!(props.len(), 1);
        assert_eq!(props.get("prefix").expect("Prefix is None"), "my_prefix");
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");
    }

    #[test]
    fn test_parse_table_url_missing_table() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let result = parse_table_url(url);
        assert!(result.is_err());
        assert!(matches!(
            result.expect_err("Failed to parse table URL"),
            Error::MissingTableSegment
        ));
    }

    #[test]
    fn test_get_warehouse_valid_glue_url() {
        let url = "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/211125479522/namespaces/big_datasets/tables/tpch_sf100_lineitem";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, Some("211125479522".to_string()));
    }

    #[test]
    fn test_get_warehouse_invalid_glue_url_missing_catalog() {
        let url = "https://glue.us-east-1.amazonaws.com/iceberg/v1/namespaces/big_datasets/tables/tpch_sf100_lineitem";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, None);
    }

    #[test]
    fn test_get_warehouse_invalid_glue_url_invalid_catalog_id() {
        let url = "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/not-a-number/namespaces/big_datasets";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, None);
    }

    #[test]
    fn test_get_warehouse_invalid_glue_url_catalog_id_too_short() {
        let url = "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/12345678901/namespaces/big_datasets";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, None);
    }

    #[test]
    fn test_get_warehouse_invalid_glue_url_catalog_id_too_long() {
        let url = "https://glue.us-east-1.amazonaws.com/iceberg/v1/catalogs/1234567890123/namespaces/big_datasets";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, None);
    }

    #[test]
    fn test_get_warehouse_non_glue_url() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let warehouse = get_warehouse(&parsed_url);
        assert_eq!(warehouse, None);
    }

    /// The `(scheme, name, root)` an operator lists under: `name` is the bucket
    /// for object stores, and `root` the prefix within it.
    fn operator_location(op: &Operator) -> (&'static str, String, String) {
        let info = op.info();
        (info.scheme(), info.name(), info.root())
    }

    /// The root the local filesystem backend reports for `dir`: canonicalized
    /// (so `/tmp` is `/private/tmp` on macOS), with no trailing slash.
    fn canonical_root(dir: &std::path::Path) -> String {
        std::fs::canonicalize(dir)
            .expect("canonicalize the warehouse directory")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn test_build_opendal_operator_s3() {
        let props = HashMap::new();
        let op = build_opendal_operator("s3://my-bucket/prefix/warehouse", &props)
            .expect("S3 operator should be created");
        assert_eq!(
            operator_location(&op),
            (
                "s3",
                "my-bucket".to_string(),
                "/prefix/warehouse/".to_string()
            )
        );
    }

    #[test]
    fn test_build_opendal_operator_s3a() {
        let props = HashMap::new();
        let op = build_opendal_operator("s3a://my-bucket/prefix/warehouse", &props)
            .expect("S3A operator should be created");
        assert_eq!(
            operator_location(&op),
            (
                "s3",
                "my-bucket".to_string(),
                "/prefix/warehouse/".to_string()
            )
        );
    }

    #[test]
    fn test_build_opendal_operator_gcs() {
        let props = HashMap::new();
        let op = build_opendal_operator("gs://my-bucket/prefix", &props)
            .expect("GCS operator should be created");
        assert_eq!(
            operator_location(&op),
            ("gcs", "my-bucket".to_string(), "/prefix/".to_string())
        );
    }

    #[test]
    fn test_build_opendal_operator_file_url() {
        let dir = tempfile::tempdir().expect("create a temporary warehouse directory");
        let path = dir
            .path()
            .to_str()
            .expect("temporary directory path is UTF-8");
        let op = build_opendal_operator(&format!("file://{path}"), &HashMap::new())
            .expect("File operator should be created");
        assert_eq!(
            operator_location(&op),
            ("fs", String::new(), canonical_root(dir.path()))
        );
    }

    #[test]
    fn test_build_opendal_operator_bare_path() {
        let dir = tempfile::tempdir().expect("create a temporary warehouse directory");
        let path = dir
            .path()
            .to_str()
            .expect("temporary directory path is UTF-8");
        let op = build_opendal_operator(path, &HashMap::new())
            .expect("Bare path operator should be created");
        assert_eq!(
            operator_location(&op),
            ("fs", String::new(), canonical_root(dir.path()))
        );
    }

    #[test]
    fn test_build_opendal_operator_unsupported_scheme() {
        let props = HashMap::new();
        let err = build_opendal_operator("ftp://my-host/path", &props)
            .expect_err("Unsupported scheme should fail");
        assert_eq!(
            err.to_string(),
            "Unsupported scheme in warehouse URL: ftp://my-host/path"
        );
    }

    mod vended_storage_endpoint {
        use super::super::{
            GCS_SERVICE_HOST, S3_ENDPOINT, STORAGE_ENDPOINT_PROPS, catalog_controlled_s3_region,
            check_vended_endpoints, check_vended_file_location,
            clear_vended_endpoint_resolver_override, effective_storage_endpoint,
            endpoint_targets_internal_host, get_rest_catalog, refusal_message_for_resolution,
            s3_effective_endpoint, s3_region_is_substituted, s3_region_with_fallback,
            set_vended_endpoint_resolver_override, vended_endpoint_refused_message,
            vended_endpoint_rewritten_message, vended_endpoint_unresolved_message,
            vended_file_location_refused_message,
            vended_redirect::{
                clear_vended_redirect_connect_override, set_vended_redirect_connect_override,
                vended_redirect_refused_message,
            },
        };
        use std::collections::HashMap;
        use std::sync::Arc;

        use axum::{Json, Router, routing::get};
        use iceberg::{Catalog, TableIdent};
        use parking_lot::Mutex;
        use serde_json::{Value, json};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use url::Url;

        const MANIFEST_LIST: &str = "s3://testbucket/warehouse/db/t/metadata/snap-1.avro";

        fn load_table_result_at(
            config: &Value,
            metadata_location: &str,
            manifest_list: &str,
        ) -> Value {
            json!({
                "metadata-location": metadata_location,
                "metadata": {
                    "format-version": 2,
                    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
                    "location": "s3://testbucket/warehouse/db/t",
                    "last-sequence-number": 1,
                    "last-updated-ms": 1_700_000_000_000_i64,
                    "last-column-id": 1,
                    "current-schema-id": 0,
                    "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                        {"id": 1, "name": "id", "required": false, "type": "long"}
                    ]}],
                    "default-spec-id": 0,
                    "partition-specs": [{"spec-id": 0, "fields": []}],
                    "last-partition-id": 999,
                    "default-sort-order-id": 0,
                    "sort-orders": [{"order-id": 0, "fields": []}],
                    "properties": {},
                    "current-snapshot-id": 1,
                    "snapshots": [{
                        "snapshot-id": 1,
                        "sequence-number": 1,
                        "timestamp-ms": 1_700_000_000_000_i64,
                        "manifest-list": manifest_list,
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    }],
                    "snapshot-log": [],
                    "metadata-log": []
                },
                "config": config,
            })
        }

        /// Serves an Iceberg REST catalog whose table `db.t` vends `config`.
        async fn start_rest_catalog(config: &Value) -> String {
            start_rest_catalog_at(
                config,
                "s3://testbucket/warehouse/db/t/metadata/v1.metadata.json",
                MANIFEST_LIST,
            )
            .await
        }

        /// An object store stand-in on a loopback address that records the
        /// request line of every connection it accepts and answers `404`.
        struct Sentinel {
            endpoint: String,
            requests: Arc<Mutex<Vec<String>>>,
        }

        async fn start_sentinel() -> Sentinel {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the sentinel listener");
            let endpoint = format!(
                "http://{}",
                listener.local_addr().expect("read the sentinel address")
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let recorded = Arc::clone(&recorded);
                    tokio::spawn(async move {
                        let mut buf = vec![0_u8; 8192];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let head = String::from_utf8_lossy(&buf[..n]);
                        recorded
                            .lock()
                            .push(head.lines().next().unwrap_or_default().to_string());
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                    });
                }
            });
            Sentinel { endpoint, requests }
        }

        /// Loads `db.t` through `get_rest_catalog`, as the Iceberg connectors
        /// do, and reads the table's manifest list through its `FileIO`.
        async fn read_manifest_list(
            catalog_uri: String,
            operator_props: HashMap<String, String>,
        ) -> iceberg::Result<usize> {
            let catalog =
                get_rest_catalog(catalog_uri, operator_props, None, "dataset 't'".to_string())
                    .await
                    .expect("build the REST catalog");
            let table = catalog
                .load_table(&TableIdent::from_strs(["db", "t"]).expect("valid table identifier"))
                .await
                .expect("load the table from the REST catalog");
            let bytes = table.file_io().new_input(MANIFEST_LIST)?.read().await?;
            Ok(bytes.len())
        }

        /// Like [`read_manifest_list`], reading `location` instead of the
        /// default manifest list.
        async fn read_location(
            catalog_uri: String,
            operator_props: HashMap<String, String>,
            location: &str,
        ) -> iceberg::Result<usize> {
            let catalog =
                get_rest_catalog(catalog_uri, operator_props, None, "dataset 't'".to_string())
                    .await
                    .expect("build the REST catalog");
            let table = catalog
                .load_table(&TableIdent::from_strs(["db", "t"]).expect("valid table identifier"))
                .await
                .expect("load the table from the REST catalog");
            let bytes = table.file_io().new_input(location)?.read().await?;
            Ok(bytes.len())
        }

        async fn start_rest_catalog_at(
            config: &Value,
            metadata_location: &str,
            manifest_list: &str,
        ) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the REST catalog listener");
            let addr = listener
                .local_addr()
                .expect("read the REST catalog address");
            let table = load_table_result_at(config, metadata_location, manifest_list);
            let app = Router::new()
                .route(
                    "/v1/config",
                    get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
                )
                .route(
                    "/v1/namespaces/db/tables/t",
                    get(move || {
                        let table = table.clone();
                        async move { Json(table) }
                    }),
                );
            tokio::spawn(async move { axum::serve(listener, app).await });
            format!("http://{addr}")
        }

        fn vended_config(endpoint: &str) -> Value {
            json!({
                "s3.endpoint": endpoint,
                "s3.access-key-id": "AKIATEST",
                "s3.secret-access-key": "testsecret",
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            })
        }

        #[tokio::test]
        async fn catalog_vended_internal_endpoint_is_refused_without_connecting() {
            let sentinel = start_sentinel().await;
            let port = sentinel
                .endpoint
                .rsplit(':')
                .next()
                .expect("sentinel endpoint has a port")
                .to_string();
            let endpoints = [
                sentinel.endpoint.clone(),
                format!("localhost:{port}"),
                "http://169.254.169.254".to_string(),
                "http://172.30.0.2:18077".to_string(),
                "http://[::ffff:169.254.169.254]".to_string(),
                "http://metadata.google.internal".to_string(),
            ];
            for endpoint in endpoints {
                let catalog_uri = start_rest_catalog(&vended_config(&endpoint)).await;
                let err = read_manifest_list(catalog_uri, HashMap::new())
                    .await
                    .expect_err("a vended internal endpoint must not be read");
                assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{endpoint}");
                assert_eq!(
                    err.message(),
                    format!(
                        "The Iceberg catalog for dataset 't' set `s3.endpoint` to {endpoint:?}, a private, loopback, or link-local address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, set the `s3_endpoint` parameter to it, or set `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg"
                    ),
                    "{endpoint}"
                );
            }
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        /// Removes this test's resolver override when the test ends.
        struct ResolverOverride {
            host: String,
        }

        impl ResolverOverride {
            fn set(host: &str, result: Result<Vec<std::net::IpAddr>, String>) -> Self {
                set_vended_endpoint_resolver_override(host, result);
                Self {
                    host: host.to_ascii_lowercase(),
                }
            }
        }

        impl Drop for ResolverOverride {
            fn drop(&mut self) {
                clear_vended_endpoint_resolver_override(&self.host);
            }
        }

        #[tokio::test]
        async fn catalog_vended_hostname_resolving_to_loopback_is_refused_without_connecting() {
            let sentinel = start_sentinel().await;
            let _override = ResolverOverride::set(
                "loopback.vended.test",
                Ok(vec![std::net::Ipv6Addr::LOCALHOST.into()]),
            );
            let endpoint = "http://loopback.vended.test:9000";
            let catalog_uri = start_rest_catalog(&vended_config(endpoint)).await;
            let err = read_manifest_list(catalog_uri, HashMap::new())
                .await
                .expect_err("a hostname that resolves to loopback must not be read");
            assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid);
            assert_eq!(
                err.message(),
                vended_endpoint_refused_message("dataset 't'", S3_ENDPOINT, endpoint)
            );
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        #[test]
        fn s3_endpoint_bucket_prefix_is_removed() {
            assert_eq!(
                s3_effective_endpoint("http://8.127.0.1:18075", "8", false, "us-east-1"),
                "http://127.0.0.1:18075/8",
                "path style strips the bucket prefix and URL parsing collapses 127.0.1"
            );
            assert_eq!(
                s3_effective_endpoint("http://8.127.0.0.1:18075", "8", false, "us-east-1"),
                "http://127.0.0.1:18075/8"
            );
            assert_eq!(
                s3_effective_endpoint("http://public.example.test:9000", "loop", true, "us-east-1"),
                "http://loop.public.example.test:9000",
                "virtual-host style puts the bucket in front of the host"
            );
            assert_eq!(
                s3_effective_endpoint("https://s3.amazonaws.com", "warehouse", false, "eu-west-1"),
                "https://s3.eu-west-1.amazonaws.com/warehouse"
            );
            assert_eq!(
                s3_effective_endpoint(
                    "https://s3.amazonaws.com",
                    "testbucket",
                    false,
                    "x@127.0.0.1:18074/",
                ),
                "https://s3.x@127.0.0.1:18074/.amazonaws.com/testbucket",
                "a region is substituted into the global endpoint before path-style"
            );
            assert!(endpoint_targets_internal_host(
                "https://s3.x@127.0.0.1:18074/.amazonaws.com/testbucket"
            ));
            assert_eq!(
                s3_effective_endpoint(
                    "https://s3.amazonaws.com",
                    "testbucket",
                    true,
                    "x@127.0.0.1:18074/",
                ),
                "https://testbucket.s3.x@127.0.0.1:18074/.amazonaws.com"
            );
            let endpoint = "http://8.127.0.1:18075";
            let effective = "http://127.0.0.1:18075/8";
            for scheme in ["s3", "s3a", "s3n", "minio"] {
                assert_eq!(
                    effective_storage_endpoint(
                        S3_ENDPOINT,
                        endpoint,
                        &format!("{scheme}://8/metadata/snap-1.avro"),
                        false,
                        "us-east-1",
                    ),
                    effective,
                    "{scheme}"
                );
            }
            assert_eq!(
                effective_storage_endpoint(
                    S3_ENDPOINT,
                    endpoint,
                    "file:///tmp/x",
                    false,
                    "us-east-1",
                ),
                endpoint,
                "a file URL with no host is not an S3 bucket"
            );
        }

        #[tokio::test]
        async fn catalog_vended_endpoint_rewritten_onto_loopback_is_refused_without_connecting() {
            let sentinel = start_sentinel().await;
            let port = sentinel
                .endpoint
                .rsplit(':')
                .next()
                .expect("sentinel endpoint has a port");
            let effective = format!("http://127.0.0.1:{port}/8");
            let cases = [
                format!("http://8.127.0.1:{port}"),
                format!("http://8.127.0.0.1:{port}"),
            ];
            for endpoint in cases {
                let catalog_uri = start_rest_catalog_at(
                    &vended_config(&endpoint),
                    "s3://8/metadata/v1.metadata.json",
                    "s3://8/metadata/snap-1.avro",
                )
                .await;
                let err = read_location(catalog_uri, HashMap::new(), "s3://8/metadata/snap-1.avro")
                    .await
                    .expect_err("a rewritten loopback endpoint must not be read");
                assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{endpoint}");
                assert_eq!(
                    err.message(),
                    vended_endpoint_rewritten_message(
                        "dataset 't'",
                        S3_ENDPOINT,
                        &endpoint,
                        &effective
                    ),
                    "{endpoint}"
                );
            }
            for scheme in ["s3a", "s3n", "minio"] {
                let location = format!("{scheme}://8/metadata/snap-1.avro");
                let endpoint = format!("http://8.127.0.1:{port}");
                let catalog_uri = start_rest_catalog_at(
                    &vended_config(&endpoint),
                    &format!("{scheme}://8/metadata/v1.metadata.json"),
                    &location,
                )
                .await;
                let err = read_location(catalog_uri, HashMap::new(), &location)
                    .await
                    .expect_err("a rewritten loopback endpoint must not be read");
                assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{scheme}");
                assert_eq!(
                    err.message(),
                    vended_endpoint_rewritten_message(
                        "dataset 't'",
                        S3_ENDPOINT,
                        &endpoint,
                        &effective
                    ),
                    "{scheme}"
                );
            }
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        #[test]
        fn s3_region_prefers_client_then_property_then_environment() {
            let both = HashMap::from([
                ("s3.region".to_string(), "from-s3".to_string()),
                ("client.region".to_string(), "from-client".to_string()),
            ]);
            assert_eq!(
                s3_region_with_fallback(&both, Some("from-env"), Some("from-default")),
                "from-client"
            );
            let property = HashMap::from([("s3.region".to_string(), "from-s3".to_string())]);
            assert_eq!(
                s3_region_with_fallback(&property, Some("from-env"), Some("from-default")),
                "from-s3"
            );
            let absent = HashMap::new();
            assert_eq!(
                s3_region_with_fallback(&absent, Some("from-env"), Some("from-default")),
                "from-env"
            );
            assert_eq!(
                s3_region_with_fallback(&absent, Some(""), Some("from-default")),
                "",
                "an empty AWS_REGION does not fall through"
            );
            assert_eq!(
                s3_region_with_fallback(&absent, None, Some("from-default")),
                "from-default"
            );
            assert_eq!(s3_region_with_fallback(&absent, None, None), "");
        }

        #[test]
        fn catalog_region_is_checked_only_when_it_changes_the_host() {
            let region = "x@127.0.0.1:18074/";
            let props = HashMap::from([("s3.region".to_string(), region.to_string())]);
            let vended = catalog_controlled_s3_region(&props, &HashMap::new(), &HashMap::new())
                .expect(
                    "a catalog region with no endpoint is substituted into the global endpoint",
                );
            assert_eq!(vended.key, "s3.region");
            assert_eq!(vended.value, region);
            assert_eq!(vended.endpoint, "https://s3.amazonaws.com");

            let operator_region = HashMap::from([("s3.region".to_string(), region.to_string())]);
            assert!(
                catalog_controlled_s3_region(&props, &operator_region, &HashMap::new()).is_none(),
                "the operator's own region is trusted"
            );

            let private_endpoint = HashMap::from([(
                S3_ENDPOINT.to_string(),
                "http://172.30.0.3:9000".to_string(),
            )]);
            let mut with_endpoint = props.clone();
            with_endpoint.insert(
                S3_ENDPOINT.to_string(),
                "http://172.30.0.3:9000".to_string(),
            );
            let recorded =
                catalog_controlled_s3_region(&with_endpoint, &HashMap::new(), &private_endpoint)
                    .expect("a catalog region is recorded with the operator endpoint");
            assert_eq!(recorded.endpoint, "http://172.30.0.3:9000");
            assert!(
                !s3_region_is_substituted(&recorded.endpoint, "warehouse"),
                "a private operator endpoint is not the global template after bucket removal"
            );
            assert!(s3_region_is_substituted(
                "https://testbucket.s3.amazonaws.com",
                "testbucket"
            ));
            assert_eq!(
                s3_effective_endpoint(
                    "https://testbucket.s3.amazonaws.com",
                    "testbucket",
                    false,
                    region,
                ),
                "https://s3.x@127.0.0.1:18074/.amazonaws.com/testbucket"
            );

            let client = HashMap::from([
                ("s3.region".to_string(), "us-east-1".to_string()),
                ("client.region".to_string(), region.to_string()),
            ]);
            let vended = catalog_controlled_s3_region(&client, &operator_region, &HashMap::new())
                .expect("client.region wins over the operator's s3.region");
            assert_eq!(vended.key, "client.region");
        }

        #[tokio::test]
        async fn catalog_vended_region_rewritten_onto_loopback_is_refused_without_connecting() {
            let sentinel = start_sentinel().await;
            let port = sentinel
                .endpoint
                .rsplit(':')
                .next()
                .expect("sentinel endpoint has a port");
            let region = format!("x@127.0.0.1:{port}/");
            let effective = format!("https://s3.x@127.0.0.1:{port}/.amazonaws.com/testbucket");
            let config = json!({
                "s3.access-key-id": "AKIATEST",
                "s3.secret-access-key": "testsecret",
                "s3.region": region,
                "s3.path-style-access": "true",
            });
            let catalog_uri = start_rest_catalog(&config).await;
            let err = read_manifest_list(catalog_uri, HashMap::new())
                .await
                .expect_err("a region that rewrites onto loopback must not be read");
            assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{err}");
            assert_eq!(
                err.message(),
                vended_endpoint_rewritten_message("dataset 't'", "s3.region", &region, &effective)
            );
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        #[tokio::test]
        async fn catalog_region_rewrites_a_bucket_qualified_operator_endpoint() {
            let sentinel = start_sentinel().await;
            let port = sentinel
                .endpoint
                .rsplit(':')
                .next()
                .expect("sentinel endpoint has a port");
            let region = format!("x@127.0.0.1:{port}/");
            let endpoint = "https://testbucket.s3.amazonaws.com";
            let effective = format!("https://s3.x@127.0.0.1:{port}/.amazonaws.com/testbucket");
            let config = json!({
                "client.region": region,
                "s3.access-key-id": "AKIATEST",
                "s3.secret-access-key": "testsecret",
                "s3.path-style-access": "true",
            });
            let catalog_uri = start_rest_catalog(&config).await;
            let operator_props = HashMap::from([
                (S3_ENDPOINT.to_string(), endpoint.to_string()),
                ("s3.access-key-id".to_string(), "AKIATEST".to_string()),
                ("s3.secret-access-key".to_string(), "testsecret".to_string()),
                ("s3.path-style-access".to_string(), "true".to_string()),
            ]);
            let err = read_manifest_list(catalog_uri, operator_props)
                .await
                .expect_err("a bucket-qualified global endpoint must not be read");
            assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{err}");
            assert_eq!(
                err.message(),
                vended_endpoint_rewritten_message(
                    "dataset 't'",
                    "client.region",
                    &region,
                    &effective
                )
            );
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        #[tokio::test]
        async fn operator_private_endpoint_ignores_a_catalog_region_that_does_not_change_its_host()
        {
            let sentinel = start_sentinel().await;
            let config = json!({
                "s3.region": "x@127.0.0.1:9/",
                "s3.access-key-id": "minio",
                "s3.secret-access-key": "minio123",
                "s3.path-style-access": "true",
            });
            let catalog_uri = start_rest_catalog(&config).await;
            let operator_props = HashMap::from([
                (S3_ENDPOINT.to_string(), sentinel.endpoint.clone()),
                ("s3.access-key-id".to_string(), "minio".to_string()),
                ("s3.secret-access-key".to_string(), "minio123".to_string()),
                ("s3.path-style-access".to_string(), "true".to_string()),
            ]);
            let err = read_manifest_list(catalog_uri, operator_props)
                .await
                .expect_err("the sentinel answers 404");
            assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected, "{err}");
            assert_eq!(
                *sentinel.requests.lock(),
                vec!["GET /testbucket/warehouse/db/t/metadata/snap-1.avro HTTP/1.1".to_string()]
            );
        }

        #[tokio::test]
        async fn operator_endpoint_rewritten_by_the_bucket_is_still_used() {
            let sentinel = start_sentinel().await;
            let port = sentinel
                .endpoint
                .rsplit(':')
                .next()
                .expect("sentinel endpoint has a port");
            let endpoint = format!("http://8.127.0.0.1:{port}");
            let catalog_uri = start_rest_catalog_at(
                &vended_config(&endpoint),
                "s3://8/metadata/v1.metadata.json",
                "s3://8/metadata/snap-1.avro",
            )
            .await;
            let operator_props = HashMap::from([
                (S3_ENDPOINT.to_string(), endpoint),
                ("s3.access-key-id".to_string(), "minio".to_string()),
                ("s3.secret-access-key".to_string(), "minio123".to_string()),
                ("s3.region".to_string(), "us-east-1".to_string()),
                ("s3.path-style-access".to_string(), "true".to_string()),
            ]);
            let err = read_location(catalog_uri, operator_props, "s3://8/metadata/snap-1.avro")
                .await
                .expect_err("the sentinel answers 404");
            assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected, "{err}");
            assert_eq!(
                *sentinel.requests.lock(),
                vec!["GET /8/metadata/snap-1.avro HTTP/1.1".to_string()]
            );
        }

        #[test]
        fn resolved_endpoint_classification() {
            let value = "http://minio.example.com:9000";
            assert!(
                refusal_message_for_resolution(
                    "dataset 't'",
                    S3_ENDPOINT,
                    value,
                    Ok(vec![std::net::IpAddr::from([1, 1, 1, 1])]),
                )
                .is_none(),
                "a public address is not a refusal"
            );
            assert_eq!(
                refusal_message_for_resolution(
                    "dataset 't'",
                    S3_ENDPOINT,
                    value,
                    Ok(vec![
                        std::net::IpAddr::from([1, 1, 1, 1]),
                        std::net::IpAddr::from([10, 1, 2, 3]),
                    ]),
                )
                .as_deref(),
                Some(vended_endpoint_refused_message("dataset 't'", S3_ENDPOINT, value).as_str()),
                "one internal address among public ones is refused"
            );
            assert_eq!(
                refusal_message_for_resolution(
                    "dataset 't'",
                    S3_ENDPOINT,
                    "http://loopback.vended.test:9000",
                    Err("no such host".to_string()),
                )
                .as_deref(),
                Some(
                    vended_endpoint_unresolved_message(
                        "dataset 't'",
                        S3_ENDPOINT,
                        "http://loopback.vended.test:9000",
                    )
                    .as_str()
                )
            );
        }

        #[tokio::test]
        async fn operator_configured_private_endpoint_is_used() {
            let sentinel = start_sentinel().await;
            let catalog_uri = start_rest_catalog(&vended_config("http://169.254.169.254")).await;
            let operator_props = HashMap::from([
                ("s3.endpoint".to_string(), sentinel.endpoint.clone()),
                ("s3.access-key-id".to_string(), "minio".to_string()),
                ("s3.secret-access-key".to_string(), "minio123".to_string()),
                ("s3.region".to_string(), "us-east-1".to_string()),
                ("s3.path-style-access".to_string(), "true".to_string()),
            ]);
            let err = read_manifest_list(catalog_uri, operator_props)
                .await
                .expect_err("the sentinel answers 404");
            assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected, "{err}");
            assert_eq!(
                *sentinel.requests.lock(),
                vec!["GET /testbucket/warehouse/db/t/metadata/snap-1.avro HTTP/1.1".to_string()]
            );
        }

        #[test]
        fn public_or_absent_vended_endpoint_passes() {
            let operator = HashMap::new();
            for endpoint in [
                "https://s3.us-west-2.amazonaws.com",
                "https://storage.googleapis.com",
                "s3.eu-central-1.amazonaws.com",
                "http://8.8.8.8:9000",
                "https://minio.example.com:9000",
            ] {
                let props = HashMap::from([(S3_ENDPOINT.to_string(), endpoint.to_string())]);
                check_vended_endpoints(&props, &operator, "dataset 't'", false)
                    .unwrap_or_else(|e| panic!("{endpoint} must pass: {e}"));
            }
            let credentials_only = HashMap::from([
                ("s3.access-key-id".to_string(), "AKIATEST".to_string()),
                ("s3.secret-access-key".to_string(), "testsecret".to_string()),
                ("s3.session-token".to_string(), "token".to_string()),
            ]);
            check_vended_endpoints(&credentials_only, &operator, "dataset 't'", false)
                .expect("vended credentials without an endpoint pass");
        }

        #[test]
        fn every_endpoint_property_is_checked() {
            for key in STORAGE_ENDPOINT_PROPS {
                let props = HashMap::from([(key.to_string(), "http://10.0.0.5".to_string())]);
                let err = check_vended_endpoints(&props, &HashMap::new(), "catalog 'ice'", false)
                    .expect_err("a vended private endpoint is refused");
                assert_eq!(
                    err.message(),
                    vended_endpoint_refused_message("catalog 'ice'", key, "http://10.0.0.5"),
                );
            }
            assert_eq!(
                vended_endpoint_refused_message(
                    "catalog 'ice'",
                    GCS_SERVICE_HOST,
                    "http://10.0.0.5"
                ),
                "The Iceberg catalog for catalog 'ice' set `gcs.service.host` to \"http://10.0.0.5\", a private, loopback, or link-local address, so no storage request is sent to it and the table cannot be read. If this endpoint is trusted, configure it in the Spicepod, or set `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true`. See: https://spiceai.org/docs/components/data-connectors/iceberg"
            );
        }

        #[test]
        fn operator_value_is_trusted_only_when_unchanged() {
            let operator =
                HashMap::from([(S3_ENDPOINT.to_string(), "http://10.0.0.5:9000".to_string())]);
            let same = operator.clone();
            check_vended_endpoints(&same, &operator, "dataset 't'", false)
                .expect("the operator's own endpoint passes");
            let replaced = HashMap::from([(
                S3_ENDPOINT.to_string(),
                "http://169.254.169.254".to_string(),
            )]);
            let err = check_vended_endpoints(&replaced, &operator, "dataset 't'", false)
                .expect_err("a catalog value replacing the operator's is checked");
            assert_eq!(
                err.message(),
                vended_endpoint_refused_message(
                    "dataset 't'",
                    S3_ENDPOINT,
                    "http://169.254.169.254"
                )
            );
        }

        #[test]
        fn allow_private_hosts_override_admits_internal_endpoints() {
            let props = HashMap::from([(
                S3_ENDPOINT.to_string(),
                "http://169.254.169.254".to_string(),
            )]);
            check_vended_endpoints(&props, &HashMap::new(), "dataset 't'", true)
                .expect("the override admits internal endpoints");
        }

        #[tokio::test]
        async fn catalog_vended_file_location_is_refused_without_opening() {
            let dir = tempfile::tempdir().expect("create a temporary directory");
            let secret = dir.path().join("secret.txt");
            std::fs::write(&secret, b"secret-bytes").expect("write the secret file");
            let secret_url = Url::from_file_path(&secret)
                .expect("secret file has a file url")
                .to_string();
            let missing = "file:///no/such/iceberg-vended-location";
            let credentials = json!({
                "s3.access-key-id": "AKIATEST",
                "s3.secret-access-key": "testsecret",
                "s3.region": "us-east-1",
            });
            for location in [secret_url.as_str(), missing] {
                let catalog_uri = start_rest_catalog_at(&credentials, location, location).await;
                let err = read_location(catalog_uri, HashMap::new(), location)
                    .await
                    .expect_err("a vended file location must not be opened");
                assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid, "{location}");
                assert_eq!(
                    err.message(),
                    vended_file_location_refused_message("dataset 't'", location),
                    "{location}"
                );
            }
            assert_eq!(
                std::fs::read(&secret).expect("the secret file is still on disk"),
                b"secret-bytes"
            );
        }

        #[tokio::test]
        async fn operator_file_warehouse_location_is_not_refused() {
            let dir = tempfile::tempdir().expect("create a temporary directory");
            let inside = dir.path().join("snap.avro");
            std::fs::write(&inside, b"not-avro").expect("write a file inside the warehouse");
            let inside_url = Url::from_file_path(&inside)
                .expect("warehouse file has a file url")
                .to_string();
            let root = Url::from_file_path(dir.path())
                .expect("warehouse directory has a file url")
                .to_string();
            let catalog_uri =
                start_rest_catalog_at(&json!({}), inside_url.as_str(), inside_url.as_str()).await;
            let err = read_location(
                catalog_uri,
                HashMap::from([("warehouse".to_string(), root.clone())]),
                &inside_url,
            )
            .await
            .expect_err("the s3 storage backend does not read file urls");
            assert!(
                err.message().contains("Invalid s3 url"),
                "the guard must let an operator warehouse file through to the storage backend, got: {err}"
            );
            let outside = "file:///etc/passwd";
            let catalog_uri = start_rest_catalog_at(&json!({}), outside, outside).await;
            let err = read_location(
                catalog_uri,
                HashMap::from([("warehouse".to_string(), root)]),
                outside,
            )
            .await
            .expect_err("a file outside the operator warehouse is refused");
            assert_eq!(
                err.message(),
                vended_file_location_refused_message("dataset 't'", outside)
            );
        }

        #[test]
        fn file_location_traversal_cannot_escape_the_operator_warehouse() {
            let root = "file:///var/warehouse";
            let inside = "file:///var/warehouse/db/t/metadata/snap.avro";
            check_vended_file_location(inside, Some(root), "dataset 't'", false)
                .expect("a location inside the operator warehouse passes");
            for location in [
                "file:///etc/passwd",
                "file:///var/warehouse/../../etc/passwd",
                "file:///var/warehouse/%2e%2e/%2e%2e/etc/passwd",
                "file://localhost/etc/passwd",
                "file:///var/warehouse-evil/secret",
            ] {
                let err = check_vended_file_location(location, Some(root), "dataset 't'", false)
                    .expect_err("a location outside the warehouse is refused");
                assert_eq!(
                    err.message(),
                    vended_file_location_refused_message("dataset 't'", location),
                    "{location}"
                );
            }
            check_vended_file_location(inside, None, "dataset 't'", false)
                .expect_err("without an operator warehouse every file location is refused");
            check_vended_file_location("file:///etc/passwd", None, "dataset 't'", true)
                .expect("the private-host override admits file locations");
            check_vended_file_location("s3://bucket/key", None, "dataset 't'", false)
                .expect("an object-store location is not a local file");
        }

        /// Removes this test's connect override when the test ends.
        struct ConnectOverride {
            host: String,
        }

        impl ConnectOverride {
            fn set(host: &str, authority: &str) -> Self {
                set_vended_redirect_connect_override(host, authority);
                Self {
                    host: host.to_ascii_lowercase(),
                }
            }
        }

        impl Drop for ConnectOverride {
            fn drop(&mut self) {
                clear_vended_redirect_connect_override(&self.host);
            }
        }

        /// Answers `302` to `location` and records the request line.
        async fn start_redirector(location: &str) -> Sentinel {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the redirector");
            let endpoint = format!(
                "http://{}",
                listener.local_addr().expect("read the redirector address")
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);
            let location = location.to_string();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let recorded = Arc::clone(&recorded);
                    let location = location.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0_u8; 8192];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let head = String::from_utf8_lossy(&buf[..n]);
                        recorded
                            .lock()
                            .push(head.lines().next().unwrap_or_default().to_string());
                        let response = format!(
                            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
            });
            Sentinel { endpoint, requests }
        }

        #[tokio::test]
        async fn catalog_vended_endpoint_redirect_to_loopback_is_refused_without_connecting() {
            let sentinel = start_sentinel().await;
            let redirector = start_redirector(&format!("{}/secret", sentinel.endpoint)).await;
            let authority = redirector
                .endpoint
                .trim_start_matches("http://")
                .to_string();
            let _connect = ConnectOverride::set("redirect.vended.test", &authority);
            let _dns = ResolverOverride::set(
                "redirect.vended.test",
                Ok(vec![std::net::Ipv4Addr::new(8, 8, 8, 8).into()]),
            );
            let endpoint = format!(
                "http://redirect.vended.test:{}",
                authority.rsplit(':').next().expect("port")
            );
            let catalog_uri = start_rest_catalog(&vended_config(&endpoint)).await;
            let err = read_manifest_list(catalog_uri, HashMap::new())
                .await
                .expect_err("a redirect to loopback must not be followed");
            let expected = vended_redirect_refused_message(
                "dataset 't'",
                &format!("{}/secret", sentinel.endpoint),
            );
            assert!(
                err.to_string().contains(&expected),
                "expected the redirect refusal in {err}"
            );
            assert!(
                !redirector.requests.lock().is_empty(),
                "the public endpoint was never contacted, so the redirect was not exercised"
            );
            assert_eq!(*sentinel.requests.lock(), Vec::<String>::new());
        }

        #[tokio::test]
        async fn operator_endpoint_redirect_to_loopback_is_followed() {
            let sentinel = start_sentinel().await;
            let redirector = start_redirector(&format!("{}/secret", sentinel.endpoint)).await;
            let mut operator = vended_config(&redirector.endpoint);
            operator
                .as_object_mut()
                .expect("config object")
                .remove("s3.endpoint");
            let catalog_uri = start_rest_catalog(&operator).await;
            let mut props = HashMap::new();
            props.insert("s3.endpoint".to_string(), redirector.endpoint.clone());
            props.insert("s3.path-style-access".to_string(), "true".to_string());
            props.insert("s3.region".to_string(), "us-east-1".to_string());
            props.insert("s3.access-key-id".to_string(), "AKIATEST".to_string());
            props.insert("s3.secret-access-key".to_string(), "testsecret".to_string());
            let err = read_manifest_list(catalog_uri, props)
                .await
                .expect_err("the sentinel answers 404");
            assert!(
                !err.to_string().contains("redirected storage"),
                "an operator endpoint redirect must be followed, got: {err}"
            );
            assert!(
                !redirector.requests.lock().is_empty(),
                "the operator endpoint was not contacted"
            );
            assert!(
                !sentinel.requests.lock().is_empty(),
                "the operator redirect was not followed: {err}"
            );
        }
    }
}
