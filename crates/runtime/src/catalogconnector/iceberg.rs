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
    component::catalog::Catalog, dataconnector::ConnectorParams,
    http::v1::iceberg::namespace::Namespace as HttpNamespace, Runtime,
};
use async_trait::async_trait;
use data_components::{
    iceberg::{catalog::RestCatalog, provider::IcebergCatalogProvider},
    RefreshableCatalogProvider,
};
use iceberg::{Namespace, NamespaceIdent};
use iceberg_catalog_rest::RestCatalogConfig;
use ns_lookup::verify_ns_lookup_and_tcp_connect;
use secrecy::ExposeSecret;
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

    #[snafu(display("Failed to connect to the S3 endpoint at '{url}'.\nVerify the S3 endpoint is accessible and try again."))]
    FailedToConnectS3Endpoint { url: String },
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
    ParameterSpec::connector("token")
        .secret()
        .description("Bearer token value to use for Authorization header."),
    ParameterSpec::connector("oauth2_credential")
        .secret()
        .description(
            "Credential to use for OAuth2 client credential flow when initializing the catalog. Separated by a colon as <client_id>:<client_secret>.",
        ),
    ParameterSpec::connector("oauth2_token_url")
        .description("The URL to use for OAuth2 token endpoint."),
    ParameterSpec::connector("oauth2_scope")
        .description(
            "The scope to use for OAuth2 token endpoint (default: catalog).",
        )
        .default("catalog"),
    ParameterSpec::connector("oauth2_server_url")
        .description("URL of the OAuth2 server tokens endpoint."),
    ParameterSpec::connector("sigv4_enabled")
        .description("Enable SigV4 authentication for the catalog (for connecting to AWS Glue)."),
    ParameterSpec::connector("signing_region")
        .description("The region to use when signing the request for SigV4. Defaults to the region in the catalog URL if available."),
    ParameterSpec::connector("signing_name")
        .description("The name to use when signing the request for SigV4.")
        .default("glue"),
    // S3 storage options
    ParameterSpec::connector("s3_endpoint")
        .description(
            "Configure an alternative endpoint for the S3 service. This can be any s3-compatible object storage service. i.e. Minio, Cloudflare R2, etc.",
        )
        .secret(),
    ParameterSpec::connector("s3_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("s3_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("s3_session_token")
        .description("Configure the static session token used for S3 storage.")
        .secret(),
    ParameterSpec::connector("s3_region")
        .description("The AWS S3 region to use.")
        .secret(),
    ParameterSpec::connector("s3_role_session_name")
        .description("An optional identifier for the assumed role session for auditing purposes.")
        .secret(),
    ParameterSpec::connector("s3_role_arn")
        .description("The Amazon Resource Name (ARN) of the role to assume. If provided instead of s3_access_key_id and s3_secret_access_key, temporary credentials will be fetched by assuming this role")
        .secret(),
    ParameterSpec::connector("s3_connect_timeout")
        .description("Configure socket connection timeout, in seconds (default: 60).")
];

/// Maps a Spice parameter name to an Iceberg property name.
fn map_param_name_to_iceberg_prop(param_name: &str) -> Option<String> {
    match param_name {
        "token" => Some("token".to_string()),
        "oauth2_credential" => Some("credential".to_string()),
        "oauth2_server_url" => Some("oauth2-server-uri".to_string()),
        "oauth2_scope" => Some("scope".to_string()),
        "s3_endpoint" => Some("s3.endpoint".to_string()),
        "s3_access_key_id" => Some("s3.access-key-id".to_string()),
        "s3_secret_access_key" => Some("s3.secret-access-key".to_string()),
        "s3_session_token" => Some("s3.session-token".to_string()),
        "s3_region" => Some("s3.region".to_string()),
        "s3_role_session_name" => Some("client.assume-role.session-name".to_string()),
        "s3_role_arn" => Some("client.assume-role.arn".to_string()),
        "sigv4_enabled" => Some("rest.sigv4-enabled".to_string()),
        "signing_region" => Some("rest.signing-region".to_string()),
        "signing_name" => Some("rest.signing-name".to_string()),
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
        _runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "iceberg".into(),
                    message: "A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>.\nFor details, visit: https://spiceai.org/docs/components/catalogs/iceberg#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        let (base_uri, mut props, namespace) = match parse_catalog_url(catalog_id.as_str()) {
            Ok(result) => result,
            Err(e) => {
                return Err(super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: format!("A Catalog Path is required for Iceberg in the format of: http://<host_and_port>/v1/namespaces/<namespace>.\nFor details, visit: https://spiceai.org/docs/components/catalogs/iceberg#from\n{e}"),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                });
            }
        };

        for (key, value) in &self.params {
            if let Some(prop) = map_param_name_to_iceberg_prop(key.as_str()) {
                props.insert(prop, value.expose_secret().to_string());
            }
        }

        if let Some(endpoint) = props.get("s3.endpoint") {
            verify_s3_endpoint(endpoint)
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "iceberg".into(),
                    message: e.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?;
        }

        let catalog_config = RestCatalogConfig::builder()
            .uri(base_uri)
            .props(props)
            .build();

        let catalog_client = RestCatalog::new(catalog_config);

        let catalog_provider = IcebergCatalogProvider::try_new(
            Arc::new(catalog_client),
            namespace.map(|n| n.name().clone()),
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

async fn verify_s3_endpoint(endpoint: &str) -> Result<()> {
    let url = Url::parse(endpoint).context(UrlParseSnafu)?;
    let host = url.host_str().context(MissingHostSnafu)?;
    let port = url.port().unwrap_or_else(|| {
        if url.scheme() == "http" {
            80
        } else if url.scheme() == "https" {
            443
        } else {
            return 0;
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
        base_uri.push_str(&format!("/{prefix_path}"));
    }

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

    // Auto-detect AWS Glue URLs and set signing region, name, and SigV4 enabled
    if let Some(host_str) = parsed.host_str() {
        if host_str.starts_with("glue.") && host_str.ends_with(".amazonaws.com") {
            if let Some(region) = host_str
                .strip_prefix("glue.")
                .and_then(|s| s.strip_suffix(".amazonaws.com"))
            {
                props.insert("rest.signing-region".to_string(), region.to_string());
                props.insert("rest.signing-name".to_string(), "glue".to_string());
                props.insert("rest.sigv4-enabled".to_string(), "true".to_string());
            }
        }
    }

    // Return the Base URI + Properties + Namespace
    Ok((base_uri, props, namespace))
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
        assert!(namespace
            .expect("Namespace is None")
            .properties()
            .is_empty());
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
        assert!(namespace
            .expect("Namespace is None")
            .properties()
            .is_empty());
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
}
