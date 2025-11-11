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
    component::catalog::Catalog,
    dataconnector::parameters::{ConnectorParams, aws::initiate_config_with_credentials},
    http::v1::iceberg::namespace::Namespace as HttpNamespace,
};
use async_trait::async_trait;
use aws_sdk_credential_bridge::S3CredentialProvider;
use data_components::{
    RefreshableCatalogProvider,
    iceberg::{
        catalog::{
            hadoop::{HadoopCatalogBuilder, MetadataMode},
            rest::RestCatalog,
        },
        provider::IcebergCatalogProvider,
    },
};
use iceberg::{CatalogBuilder, Namespace, NamespaceIdent, io::CustomAwsCredentialLoader};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, RestCatalog as IcebergRestCatalog, RestCatalogBuilder,
};
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::fmt::Write;
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

    #[snafu(display("Failed to parse catalog URL"))]
    UrlParseNoSource,

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
        custom_credential_loader: Option<CustomAwsCredentialLoader>,
        catalog: &Catalog,
        catalog_id: &str,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        // Not much we can check with this path for Hadoop, because a namespace could be an empty folder, there could be no namespaces, etc.
        let mut catalog_builder = HadoopCatalogBuilder::default()
            .with_warehouse_root(catalog_id)
            .with_metadata_mode(MetadataMode::Infer)
            .with_properties(props);

        if let Some(loader) = custom_credential_loader {
            catalog_builder = catalog_builder.with_file_io_extension(loader);
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
            catalog.include.as_ref(),
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

pub(crate) const ICEBERG_PARAM_LEN: usize = 17;
pub(crate) const PARAMETERS: [ParameterSpec; ICEBERG_PARAM_LEN] = [
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
        .description("Configure socket connection timeout, in seconds (default: 60).")
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
        _ => None,
    }
}

#[async_trait]
impl CatalogConnector for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::too_many_lines)]
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

        let mut props = HashMap::new();
        for (key, value) in &self.params {
            if let Some(prop_vec) = map_param_name_to_iceberg_prop(key.as_str()) {
                for prop in prop_vec {
                    props.insert(prop.clone(), value.expose_secret().to_string());
                }
            }
        }

        let custom_credential_loader = if let Some(endpoint) = props.get("s3.endpoint") {
            verify_s3_endpoint(endpoint)
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?;

            let aws_sdk_config = initiate_config_with_credentials(
                "IcebergCatalogConnector",
                "s3_region",
                "s3_access_key_id",
                "s3_secret_access_key",
                "s3_session_token",
                &self.params,
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

            Some(
                S3CredentialProvider::from_config(&aws_sdk_config)
                    .map_err(|e| super::Error::InvalidConfiguration {
                        connector: "iceberg".into(),
                        message: e.to_string(),
                        connector_component: ConnectorComponent::from(catalog),
                        source: Box::new(e),
                    })?
                    .into_custom_loader(),
            )
        } else {
            None
        };

        if catalog_id.starts_with("file://")
            || catalog_id.starts_with("s3://")
            || catalog_id.starts_with("s3a://")
        {
            return IcebergCatalog::load_hadoop_catalog(
                props,
                custom_credential_loader,
                catalog,
                &catalog_id,
            )
            .await;
        }

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
        let catalog_config = get_rest_catalog(base_uri, props).await?;
        let mut catalog_client = RestCatalog::new(catalog_config);
        if let Some(loader) = custom_credential_loader {
            catalog_client = catalog_client.with_file_io_extension(loader);
        }

        let catalog_provider = IcebergCatalogProvider::try_new(
            Arc::new(catalog_client),
            namespace.map(|n| n.name().clone()),
            catalog.include.as_ref(),
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

/// Builds an `IcebergRestCatalog` from a base URI and properties.
pub async fn get_rest_catalog(
    base_uri: String,
    mut props: HashMap<String, String, std::hash::RandomState>,
) -> Result<IcebergRestCatalog> {
    props.insert(REST_CATALOG_PROP_URI.to_string(), base_uri);
    RestCatalogBuilder::default()
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

pub fn parse_hadoop_table_url(
    url: &str,
    warehouse_uri: Option<&str>,
) -> Result<(String, Namespace, String)> {
    // There's no definite position for a root namespace in Hadoop, so all we can do is validate the URL and return the base URI.
    // If an optional root is provided, it will be used as the warehouse root.
    let parsed = Url::parse(url).context(UrlParseSnafu)?;

    match parsed.scheme() {
        "file" | "s3a" => {} // OK
        other => {
            return InvalidSchemeSnafu {
                scheme: other.to_string(),
            }
            .fail();
        }
    }

    let count = parsed
        .path_segments()
        .map(std::iter::Iterator::count)
        .context(UrlParseNoSourceSnafu)?;

    let table_name = parsed
        .path_segments()
        .and_then(std::iter::Iterator::last)
        .context(UrlParseNoSourceSnafu)?;

    // Set initial namespace - this falls through if warehouse URI is not provided
    let namespace_ident = parsed
        .path_segments()
        .and_then(|mut segments| {
            segments
                .nth(count - 2)
                .map(|s| NamespaceIdent::new(s.to_string()))
        })
        .context(MissingNamespaceSnafu)?;

    let nodes = parsed
        .path_segments()
        .map(|segments| segments.take(count - 1).collect::<Vec<_>>())
        .context(UrlParseNoSourceSnafu)?;

    let warehouse_leaves = nodes
        .clone()
        .iter()
        .map(ToString::to_string)
        .take(count - 2)
        .collect::<Vec<_>>()
        .join("/");

    let mut base_uri = if let Some(host) = parsed.host_str() {
        format!("{}://{host}/{warehouse_leaves}", parsed.scheme())
    } else {
        // nodes includes the inferred namespace, which needs to be excluded from the inferred base URI
        format!("{}://{warehouse_leaves}", parsed.scheme())
    };

    let mut namespace = Namespace::new(namespace_ident);

    if let Some(warehouse_uri) = warehouse_uri {
        base_uri = warehouse_uri.to_string();

        let warehouse_uri = Url::parse(warehouse_uri).context(UrlParseSnafu)?;
        if warehouse_uri.scheme() != parsed.scheme() {
            return InvalidSchemeSnafu {
                scheme: warehouse_uri.scheme().to_string(),
            }
            .fail();
        }

        // inverse union of the nodes with the warehouse URI paths gives any namespace segments
        let warehouse_segments: Vec<_> = warehouse_uri
            .path_segments()
            .map(Iterator::collect::<Vec<_>>)
            .unwrap_or_default();

        let namespace_segments: Vec<_> = nodes
            .iter()
            .filter(|segment| !warehouse_segments.contains(segment))
            .map(ToString::to_string)
            .collect();

        if !namespace_segments.is_empty() {
            let namespace_ident = NamespaceIdent::from_vec(namespace_segments)
                .map_err(|_| Error::MissingNamespace)?;
            namespace = Namespace::new(namespace_ident);
        }
    }

    Ok((base_uri, namespace, table_name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hadoop_table_url() {
        let url = "s3a://my-bucket/my-prefix/warehouse/spiceai_sandbox/my_table";
        let (base_uri, namespace, table_name) =
            parse_hadoop_table_url(url, Some("s3a://my-bucket/my-prefix/warehouse"))
                .expect("Failed to parse Hadoop table URL");
        assert_eq!(base_uri, "s3a://my-bucket/my-prefix/warehouse");
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");

        let url = "file:///my/local/path/to/warehouse/spiceai_sandbox/my_table";
        let (base_uri, namespace, table_name) =
            parse_hadoop_table_url(url, Some("file:///my/local/path/to/warehouse"))
                .expect("Failed to parse Hadoop table URL");
        assert_eq!(base_uri, "file:///my/local/path/to/warehouse");
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");

        // should infer the base URI when no warehouse is provided
        let url = "s3a://my-bucket/my-prefix/warehouse/spiceai_sandbox/my_table";
        let (base_uri, namespace, table_name) =
            parse_hadoop_table_url(url, None).expect("Failed to parse Hadoop table URL");
        assert_eq!(base_uri, "s3a://my-bucket/my-prefix/warehouse");
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");

        let url = "file://my-bucket/my-prefix/warehouse/spiceai_sandbox/my_table";
        let (base_uri, namespace, table_name) =
            parse_hadoop_table_url(url, None).expect("Failed to parse Hadoop table URL");
        assert_eq!(base_uri, "file://my-bucket/my-prefix/warehouse");
        assert_eq!(namespace.name().to_url_string().as_str(), "spiceai_sandbox");
        assert_eq!(table_name, "my_table");

        // should support nested namespaces when a warehouse URI is provided
        let url = "s3a://my-bucket/my-prefix/warehouse/spiceai_sandbox/nested/my_table";
        let (base_uri, namespace, table_name) =
            parse_hadoop_table_url(url, Some("s3a://my-bucket/my-prefix/warehouse"))
                .expect("Failed to parse Hadoop table URL");
        assert_eq!(base_uri, "s3a://my-bucket/my-prefix/warehouse");
        assert_eq!(
            namespace.name().to_string().as_str(),
            "spiceai_sandbox.nested",
        );
        assert_eq!(table_name, "my_table");

        // should deny unknown schemes, or schemes from warehouses that don't match
        let url = "ftp://my-bucket/my-prefix/warehouse/spiceai_sandbox/my_table";
        let result = parse_hadoop_table_url(url, Some("ftp://my-bucket/my-prefix/warehouse"));
        assert!(result.is_err());

        let url = "s3a://my-bucket/my-prefix/warehouse/spiceai_sandbox/my_table";
        let result = parse_hadoop_table_url(url, Some("file:///my/local/path/to/warehouse"));
        assert!(result.is_err());
    }

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
        let result = parse_catalog_url(url);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_host() {
        let url = "https:///v1/namespaces/spiceai_sandbox";
        let result = parse_catalog_url(url);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_namespace_segment() {
        let url = "https://my.iceberg.com/v1/";
        let result = parse_catalog_url(url);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_namespace_segment() {
        let url = "https://my.iceberg.com/v1/namespaces";
        let result = parse_catalog_url(url);
        assert!(result.is_ok());
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
}
