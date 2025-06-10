use crate::config::GenericError;
use base64::{engine::general_purpose, Engine as _};
use std::io::Write;
use std::str::FromStr;
use std::sync::Once;

use tonic::transport::channel::{ClientTlsConfig, Endpoint};
use tonic::transport::Channel;

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
