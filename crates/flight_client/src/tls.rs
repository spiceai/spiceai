/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use base64::{Engine as _, engine::general_purpose};
use snafu::prelude::*;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load all system TLS certificates: {errors:?}"))]
    FailedToLoadCerts {
        errors: Vec<rustls_native_certs::Error>,
    },

    #[snafu(display("Unable to convert PEMs to string: {source}"))]
    FailedToConvertPems { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to connect to endpoint: {source}"))]
    UnableToConnectToEndpoint { source: tonic::transport::Error },

    #[snafu(display("IO error: {source}"))]
    Io { source: std::io::Error },

    #[snafu(display("CA certificate file not found: {path}"))]
    CaCertificateFileNotFound { path: String },

    #[snafu(display("Failed to read CA certificate file '{path}': {source}"))]
    FailedToReadCaCertificate {
        path: String,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// # Errors
///
/// Will return `Err` if:
///     - `rustls_native_certs` could not load native certificates.
///     - It couldn't convert the PEMs to a string.
pub fn system_tls_certificate() -> Result<tonic::transport::Certificate> {
    // Load root certificates found in the platform's native certificate store.
    let cert_result = rustls_native_certs::load_native_certs();
    if !cert_result.errors.is_empty() {
        return Err(Error::FailedToLoadCerts {
            errors: cert_result.errors,
        });
    }

    let mut pem = Vec::new();
    for cert in cert_result.certs {
        pem.write_all(b"-----BEGIN CERTIFICATE-----\n")
            .context(IoSnafu)?;
        pem.write_all(general_purpose::STANDARD.encode(cert.as_ref()).as_bytes())
            .context(IoSnafu)?;
        pem.write_all(b"\n-----END CERTIFICATE-----\n")
            .context(IoSnafu)?;
    }

    Ok(tonic::transport::Certificate::from_pem(pem))
}

/// Loads a CA certificate from a file path.
///
/// The certificate file should be in PEM format.
///
/// # Errors
///
/// Will return `Err` if:
///     - The file does not exist.
///     - The file could not be read.
pub async fn load_ca_certificate_from_file(path: &Path) -> Result<tonic::transport::Certificate> {
    if !path.exists() {
        return Err(Error::CaCertificateFileNotFound {
            path: path.display().to_string(),
        });
    }

    let pem = tokio::fs::read(path)
        .await
        .context(FailedToReadCaCertificateSnafu {
            path: path.display().to_string(),
        })?;

    Ok(tonic::transport::Certificate::from_pem(pem))
}

/// TLS endpoint information extracted from a URL.
struct TlsEndpointInfo {
    /// The endpoint URL normalized for tonic (using https:// scheme).
    normalized_url: String,
    /// The hostname for TLS certificate verification.
    domain_name: String,
}

/// Extracts TLS endpoint information from a URL.
///
/// Handles `https://` and `grpc+tls://` schemes. For `grpc+tls://`, the scheme
/// is normalized to `https://` since tonic's `Endpoint` only recognizes standard
/// HTTP schemes.
///
/// Returns `None` if the URL doesn't use a TLS scheme or if parsing fails.
fn extract_tls_endpoint_info(endpoint_str: &str) -> Option<TlsEndpointInfo> {
    let tls_prefixes = ["https://", "grpc+tls://"];

    for prefix in &tls_prefixes {
        if endpoint_str.starts_with(prefix) {
            // Normalize grpc+tls:// to https:// for tonic compatibility
            let normalized_url = if *prefix == "grpc+tls://" {
                endpoint_str.replacen("grpc+tls://", "https://", 1)
            } else {
                endpoint_str.to_string()
            };

            if let Ok(url) = Url::parse(&normalized_url)
                && let Some(host) = url.host_str()
            {
                return Some(TlsEndpointInfo {
                    normalized_url,
                    domain_name: host.to_string(),
                });
            }
        }
    }

    None
}

/// Creates a new TLS-enabled Flight channel.
///
/// If `ca_certificate_path` is provided, the certificate from that file will be used
/// for server verification. Otherwise, system certificates will be loaded.
///
/// # Errors
///
/// Will return `Err` if:
///    - It couldn't connect to the endpoint.
///    - It couldn't load the TLS certificate (either from file or system).
pub async fn new_tls_flight_channel(
    endpoint_str: &str,
    ca_certificate_path: Option<&Path>,
) -> Result<Channel> {
    if let Some(tls_info) = extract_tls_endpoint_info(endpoint_str) {
        // Use the normalized URL (https://) for tonic compatibility
        let mut endpoint =
            Endpoint::from_str(&tls_info.normalized_url).context(UnableToConnectToEndpointSnafu)?;

        let cert = if let Some(ca_path) = ca_certificate_path {
            load_ca_certificate_from_file(ca_path).await?
        } else {
            system_tls_certificate()?
        };

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(cert)
            .domain_name(tls_info.domain_name);
        endpoint = endpoint
            .tls_config(tls_config)
            .context(UnableToConnectToEndpointSnafu)?;

        endpoint
            .connect()
            .await
            .context(UnableToConnectToEndpointSnafu)
    } else {
        // Non-TLS endpoint, connect without TLS config
        Endpoint::from_str(endpoint_str)
            .context(UnableToConnectToEndpointSnafu)?
            .connect()
            .await
            .context(UnableToConnectToEndpointSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tls_endpoint_info_grpc_tls_with_port() {
        let info = extract_tls_endpoint_info("grpc+tls://localhost:31337")
            .expect("should parse grpc+tls URL with port");
        assert_eq!(info.normalized_url, "https://localhost:31337");
        assert_eq!(info.domain_name, "localhost");
    }

    #[test]
    fn test_extract_tls_endpoint_info_grpc_tls_without_port() {
        let info = extract_tls_endpoint_info("grpc+tls://example.com")
            .expect("should parse grpc+tls URL without port");
        assert_eq!(info.normalized_url, "https://example.com");
        assert_eq!(info.domain_name, "example.com");
    }

    #[test]
    fn test_extract_tls_endpoint_info_https_with_port() {
        let info = extract_tls_endpoint_info("https://myhost.example.com:443")
            .expect("should parse https URL with port");
        assert_eq!(info.normalized_url, "https://myhost.example.com:443");
        assert_eq!(info.domain_name, "myhost.example.com");
    }

    #[test]
    fn test_extract_tls_endpoint_info_https_without_port() {
        let info = extract_tls_endpoint_info("https://secure.example.org")
            .expect("should parse https URL without port");
        assert_eq!(info.normalized_url, "https://secure.example.org");
        assert_eq!(info.domain_name, "secure.example.org");
    }

    #[test]
    fn test_extract_tls_endpoint_info_https_with_path() {
        let info = extract_tls_endpoint_info("https://api.example.com:8443/v1/flight")
            .expect("should parse https URL with path");
        assert_eq!(
            info.normalized_url,
            "https://api.example.com:8443/v1/flight"
        );
        assert_eq!(info.domain_name, "api.example.com");
    }

    #[test]
    fn test_extract_tls_endpoint_info_http_returns_none() {
        assert!(extract_tls_endpoint_info("http://localhost:8080").is_none());
    }

    #[test]
    fn test_extract_tls_endpoint_info_grpc_plaintext_returns_none() {
        assert!(extract_tls_endpoint_info("grpc://localhost:50051").is_none());
    }

    #[test]
    fn test_extract_tls_endpoint_info_ipv4_address() {
        let info = extract_tls_endpoint_info("grpc+tls://192.168.1.100:31337")
            .expect("should parse grpc+tls URL with IPv4 address");
        assert_eq!(info.normalized_url, "https://192.168.1.100:31337");
        assert_eq!(info.domain_name, "192.168.1.100");
    }

    #[test]
    fn test_extract_tls_endpoint_info_ipv6_address() {
        let info = extract_tls_endpoint_info("https://[::1]:8443")
            .expect("should parse https URL with IPv6 address");
        assert_eq!(info.normalized_url, "https://[::1]:8443");
        assert_eq!(info.domain_name, "[::1]");
    }

    #[test]
    fn test_extract_tls_endpoint_info_empty_string() {
        assert!(extract_tls_endpoint_info("").is_none());
    }

    #[test]
    fn test_extract_tls_endpoint_info_invalid_url() {
        assert!(extract_tls_endpoint_info("grpc+tls://").is_none());
    }

    #[tokio::test]
    async fn test_load_ca_certificate_from_file_not_found() {
        let result = load_ca_certificate_from_file(Path::new("/nonexistent/path/ca.pem")).await;
        assert!(result.is_err());
        let err = result.expect_err("should return error for non-existent file");
        assert!(
            matches!(err, Error::CaCertificateFileNotFound { .. }),
            "expected CaCertificateFileNotFound error, got: {err:?}"
        );
    }
}
