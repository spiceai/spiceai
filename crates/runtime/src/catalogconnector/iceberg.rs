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

use super::{CatalogConnector, ConnectorComponent, ParameterSpec, Parameters};
use crate::{
    component::catalog::Catalog, dataconnector::ConnectorParams,
    http::v1::iceberg::namespace::Namespace as HttpNamespace, Runtime,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use iceberg::{Namespace, NamespaceIdent};
use iceberg_catalog_rest::RestCatalogConfig;
use snafu::prelude::*;
use std::{any::Any, collections::HashMap, sync::Arc};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid URL scheme '{}'. Must be http or https", scheme))]
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
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("token").secret().description(
        "The personal access token used to authenticate against the Iceberg REST Catalog API.",
    ),
    // S3 storage options
    ParameterSpec::connector("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),
    // Azure storage options
    ParameterSpec::connector("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),
    // GCS storage options
    ParameterSpec::connector("google_service_account")
        .description("Filesystem path to the Google service account JSON key file.")
        .secret(),
];

#[async_trait]
impl CatalogConnector for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "iceberg".into(),
                    message: "A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>.\nFor details, visit: https://docs.spiceai.org/components/catalogs/iceberg#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        let (catalog_config, namespace) = match parse_catalog_url(catalog_id.as_str()) {
            Ok(result) => result,
            Err(e) => {
                return Err(super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: format!("A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>.\nFor details, visit: https://docs.spiceai.org/components/catalogs/iceberg#from\n{e}"),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                });
            }
        };

        todo!();

        //Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
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
///   RestCatalogConfig { uri: "https://my.iceberg.com", props: {} },
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
///   RestCatalogConfig { uri: "https://my.iceberg.com", props: {"prefix": "my_prefix"} },
///   Namespace { name: "spiceai_sandbox", properties: {} }
/// )
/// ```
pub fn parse_catalog_url(url: &str) -> Result<(RestCatalogConfig, Option<Namespace>)> {
    // Parse the URL
    let parsed = Url::parse(url).context(UrlParseSnafu)?;

    // Validate scheme
    match parsed.scheme() {
        "http" | "https" => {} // OK
        other => {
            return InvalidSchemeSnafu {
                scheme: other.to_string(),
            }
            .fail()
        }
    }

    // Build the base URI (scheme://host[:port])
    let host = parsed.host_str().context(MissingHostSnafu)?;

    let port_part = match parsed.port() {
        Some(port) => format!(":{port}"),
        None => String::new(),
    };
    let base_uri = format!("{}://{}{}", parsed.scheme(), host, port_part);

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

    // Find the "namespaces" segment
    let namespaces_idx = segments
        .iter()
        .position(|seg| *seg == "namespaces")
        .context(MissingNamespacesSegmentSnafu)?;

    if namespaces_idx <= v1_idx {
        return InvalidSegmentOrderSnafu.fail();
    }

    let mut namespace: Option<Namespace> = None;
    if namespaces_idx + 1 < segments.len() {
        // The namespace name is the segment immediately after "namespaces"
        let namespace_name = HttpNamespace::from_encoded(segments[namespaces_idx + 1]);
        let Ok(namespace_name) = NamespaceIdent::from_vec(namespace_name.parts) else {
            unreachable!(
        "NamespaceIdent::from_vec never fails if namespace_name.parts has at least one part"
    )
        };
        namespace = Some(Namespace::new(namespace_name));
    }

    // Everything between "v1" and "namespaces" is considered the prefix
    let prefix_segments = &segments[v1_idx + 1..namespaces_idx];
    let prefix = prefix_segments.join("/");

    // Build up the catalog properties
    let mut props = HashMap::new();
    if !prefix.is_empty() {
        props.insert("prefix".to_string(), prefix);
    }

    // Return the RestCatalogConfig + Namespace
    Ok((
        RestCatalogConfig::builder()
            .uri(base_uri)
            .props(props)
            .build(),
        namespace,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_catalog_url_no_prefix() {
        let url = "https://my.iceberg.com/v1/namespaces/spiceai_sandbox";
        let (_, namespace) = parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(
            namespace
                .clone()
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "spiceai_sandbox"
        );
        assert!(namespace
            .expect("Namespace is None")
            .properties()
            .is_empty());
    }

    #[test]
    fn test_parse_catalog_url_with_prefix() {
        let url = "https://my.iceberg.com/v1/my_prefix/namespaces/spiceai_sandbox";
        let (_, namespace) = parse_catalog_url(url).expect("Failed to parse catalog URL");
        assert_eq!(
            namespace
                .clone()
                .expect("Namespace is None")
                .name()
                .to_url_string()
                .as_str(),
            "spiceai_sandbox"
        );
        assert!(namespace
            .expect("Namespace is None")
            .properties()
            .is_empty());
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
        assert!(result.expect("Failed to parse catalog URL").1.is_none());
    }
}
