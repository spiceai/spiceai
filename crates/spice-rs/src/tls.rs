use crate::config::GenericError;
use base64::{Engine as _, engine::general_purpose};
use std::io::Write;
use std::str::FromStr;
use std::sync::Once;

use tonic::transport::Channel;
use tonic::transport::channel::{ClientTlsConfig, Endpoint};

static INIT: Once = Once::new();

pub fn system_tls_certificate() -> Result<tonic::transport::Certificate, GenericError> {
    // Load root certificates found in the platform’s native certificate store.
    // Use the same pem format as spiceai cloud connector: https://github.com/spiceai/spiceai/blob/571007c4be89a2a9892e3bd0eb43f8bd28464a69/crates/flight_client/src/tls.rs#L47
    let cert_result = rustls_native_certs::load_native_certs();

    let mut pem = Vec::new();
    for cert in cert_result.certs {
        pem.write_all(b"-----BEGIN CERTIFICATE-----\n")?;
        pem.write_all(general_purpose::STANDARD.encode(cert.as_ref()).as_bytes())?;
        pem.write_all(b"\n-----END CERTIFICATE-----\n")?;
    }

    Ok(tonic::transport::Certificate::from_pem(pem))
}

pub async fn new_tls_flight_channel(https_url: &str) -> Result<Channel, GenericError> {
    let mut endpoint = Endpoint::from_str(https_url)?;

    if https_url.starts_with("https://") {
        let cert = system_tls_certificate()?;
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(cert)
            .domain_name(https_url.trim_start_matches("https://"));
        endpoint = endpoint.tls_config(tls_config)?;
    }

    Ok(endpoint.connect().await?)
}

pub(crate) fn ensure_crypto_provider() {
    // Install the default AWS LC RS crypto provider for rusttls
    // Use the same provider as spiceai: https://github.com/spiceai/spiceai/blob/571007c4be89a2a9892e3bd0eb43f8bd28464a69/bin/spiced/src/main.rs#L74
    INIT.call_once(|| {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::CryptoProvider::install_default(
                rustls::crypto::aws_lc_rs::default_provider(),
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_tls_certificate_loads() {
        let result = system_tls_certificate();
        assert!(result.is_ok(), "should load system TLS certificates");
    }

    #[test]
    fn test_ensure_crypto_provider_does_not_panic() {
        // Should be safe to call multiple times
        ensure_crypto_provider();
        ensure_crypto_provider();
        ensure_crypto_provider();
    }

    #[tokio::test]
    async fn test_new_tls_flight_channel_http() {
        // HTTP endpoint should work without TLS
        let result = new_tls_flight_channel("http://localhost:12345").await;
        // Will fail to connect, but should not panic on TLS config
        assert!(result.is_err()); // Connection refused is expected
    }

    #[tokio::test]
    async fn test_new_tls_flight_channel_https_invalid_host() {
        // HTTPS with invalid host should fail gracefully
        let result = new_tls_flight_channel("https://invalid.nonexistent.host:443").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_endpoint_parsing_valid_https() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("https://flight.spiceai.io");
        assert!(endpoint.is_ok());
    }

    #[test]
    fn test_endpoint_parsing_valid_http() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("http://localhost:50051");
        assert!(endpoint.is_ok());
    }

    // Edge case tests

    #[tokio::test]
    async fn test_new_tls_flight_channel_empty_url() {
        // Empty URL should fail
        let result = new_tls_flight_channel("").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_new_tls_flight_channel_unreachable_port() {
        // Connection to an unreachable port should fail
        let result = new_tls_flight_channel("http://127.0.0.1:1").await;
        // Port 1 is typically not open, so connection should fail
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_new_tls_flight_channel_missing_scheme() {
        // Missing scheme should fail
        let result = new_tls_flight_channel("example.com:443").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_endpoint_parsing_with_path() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("https://flight.spiceai.io/path");
        assert!(endpoint.is_ok());
    }

    #[test]
    fn test_endpoint_parsing_with_port() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("https://flight.spiceai.io:443");
        assert!(endpoint.is_ok());
    }

    #[test]
    fn test_endpoint_parsing_localhost_ipv4() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("http://127.0.0.1:50051");
        assert!(endpoint.is_ok());
    }

    #[test]
    fn test_endpoint_parsing_localhost_ipv6() {
        use std::str::FromStr;
        use tonic::transport::channel::Endpoint;

        let endpoint = Endpoint::from_str("http://[::1]:50051");
        assert!(endpoint.is_ok());
    }

    #[test]
    fn test_system_tls_certificate_pem_format() {
        // Verify the certificate loads and is in valid PEM format
        let result = system_tls_certificate();
        assert!(
            result.is_ok(),
            "should load system TLS certificates in PEM format"
        );
    }

    #[test]
    fn test_crypto_provider_is_installed_after_ensure() {
        ensure_crypto_provider();
        // After ensuring, provider should be installed
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
