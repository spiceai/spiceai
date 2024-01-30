use snafu::prelude::*;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load system TLS certificate"))]
    FailedToLoadCerts { source: std::io::Error },

    #[snafu(display("Unable to convert PEMs to string"))]
    FailedToConvertPems { source: std::string::FromUtf8Error },

    #[snafu(display("Unable to connect to endpoint"))]
    UnableToConnectToEndpoint { source: tonic::transport::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn system_tls_certificate() -> Result<tonic::transport::Certificate> {
    // Load root certificates found in the platform’s native certificate store.
    let certs = rustls_native_certs::load_native_certs().context(FailedToLoadCertsSnafu)?;

    let concatenated_pems = certs
        .iter()
        .filter_map(|cert| {
            let mut buf = &cert.0[..];
            rustls_pemfile::certs(&mut buf).ok()?.pop()
        })
        .map(String::from_utf8)
        .collect::<Result<String, _>>()
        .context(FailedToConvertPemsSnafu)?;

    Ok(tonic::transport::Certificate::from_pem(concatenated_pems))
}

pub async fn new_tls_flight_channel(https_url: &str) -> Result<Channel> {
    let mut endpoint = Endpoint::from_str(https_url).context(UnableToConnectToEndpointSnafu)?;

    if https_url.starts_with("https://") {
        let cert = system_tls_certificate()?;
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(cert)
            .domain_name(https_url.trim_start_matches("https://"));
        endpoint = endpoint
            .tls_config(tls_config)
            .context(UnableToConnectToEndpointSnafu)?;
    }

    endpoint
        .connect()
        .await
        .context(UnableToConnectToEndpointSnafu)
}
